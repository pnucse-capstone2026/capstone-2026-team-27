"""
Decision Agent
==============
node_contracts.md Step 3 기준.

입력 (읽는 필드):
  - anomaly_type, resource_type, resource_id, classification_reasoning, raw_metrics

출력 (채우는 필드):
  - candidate_actions, selected_action, risk_level,
    requires_approval, decision_reasoning, target_instance_type

설계:
  - NoAction은 항상 룰 기반 더미 값 (0.0, 0.0, 1.0)
  - EC2: saving_rate는 AWS Trusted Advisor 공식(CPU 평균 기준)으로 항상 계산.
         impact_score / stability_score는 항상 LLM(Gemini)에게 위임.
  - Lambda / S3 / AutoScaling / RDS: saving_rate / impact_score / stability_score
    세 가지 모두 항상 LLM에게 위임.
  - LLM 미설정(GEMINI_API_KEY 없음) 시에도 동작해야 하므로,
    LLM 호출은 항상 try/except로 감싸고 실패 시 룰 기반 fallback으로 전환한다.
  - 환각 방어(보고서 4.1절):
      1) 모든 점수를 [0.0, 1.0]로 클램핑
      2) JSON 파싱 실패 시 최대 N회 재시도, 끝까지 실패하면 NoAction 쪽으로
         점수가 떨어지도록 saving_rate=0.0 / impact_score=1.0 / stability_score=0.0 처리
      3) action은 ALLOWED_ACTIONS 표 밖의 값이 나올 수 없음
         (LLM에게 액션 후보 자체를 만들게 하지 않고, 미리 정의된 액션에 대한
          점수만 추정하게 했기 때문에 구조적으로 막혀 있음)
      4) temperature=0.1로 고정
"""

from __future__ import annotations

import json
import os
import logging

from schema.state import (
    PipelineState,
    CandidateAction,
    ALLOWED_ACTIONS,
    resolve_risk_level,
)

logger = logging.getLogger(__name__)

# 1. 설정값
# JSON 파싱 재시도 최대 횟수
MAX_LLM_RETRIES = 2

# EC2 Resize 시 기본 목표 인스턴스 타입 (Decision Agent가 별도 로직 없으면 이 값을 채움)
DEFAULT_TARGET_INSTANCE_TYPE = "t3.small"

# 룰 기반 기본 점수 테이블 (action -> (saving_rate, impact_score, stability_score))
# NoAction의 기본값이자, LLM 미설정/완전 실패 시 fallback 테이블로도 쓰인다.
RULE_BASED_SCORE_TABLE: dict[str, tuple[float, float, float]] = {
    "NoAction":      (0.0, 0.0, 1.0),
    "Stop":          (0.9, 0.1, 0.8),
    "Stop+Schedule": (0.7, 0.2, 0.8),
    "Resize":        (0.5, 0.3, 0.7),
    "Throttle":      (0.4, 0.2, 0.8),
    "Block":         (0.6, 0.4, 0.6),
    "ScaleDown":     (0.6, 0.3, 0.7),
}

# 2. 헬퍼 함수들
def _clamp01(value: float) -> float:
    """
    입력: value (임의의 숫자/문자열)
    출력: [0.0, 1.0] 범위로 강제 클램핑된 float (환각 방어 1차 방어선)
    """
    try:
        v = float(value)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, min(1.0, v))


def _get_llm():
    """
    입력: 없음 (환경변수 GEMINI_API_KEY 사용)
    출력: LangChain Gemini 클라이언트, 미설정/미설치 시 None
    (호출부에서 None이면 룰 기반으로 fallback)
    """
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        logger.warning("GEMINI_API_KEY 미설정 — 룰 기반으로만 동작합니다.")
        return None
    try:
        from langchain_google_genai import ChatGoogleGenerativeAI
    except ImportError:
        logger.warning("langchain_google_genai 미설치 — 룰 기반으로만 동작합니다.")
        return None

    return ChatGoogleGenerativeAI(
        model="gemini-2.5-flash",
        google_api_key=api_key,
        temperature=0.1,  # temperature 고정으로 일관성 확보
    )


def _ec2_saving_rate(raw_metrics: dict) -> float:
    """
    입력: raw_metrics (EC2Metrics, cpu_utilization 리스트 포함)
    출력: saving_rate [0.0, 1.0]

    AWS Trusted Advisor 공식 기준:
      - cpu_avg <= 10% → 0.9 (Low Utilization, 낭비 확실)
      - cpu_avg >= 90% → 0.1 (High Utilization, 과부하 — Stop하면 위험)
      - 그 사이       → 선형 보간
    """
    cpu_values = raw_metrics.get("cpu_utilization", [])
    if not cpu_values:
        logger.warning("cpu_utilization 데이터 없음 — saving_rate=0.0 처리")
        return 0.0

    cpu_avg = sum(cpu_values) / len(cpu_values)

    if cpu_avg <= 10:
        return 0.9
    if cpu_avg >= 90:
        return 0.1
    return _clamp01(0.9 - (cpu_avg - 10) / (90 - 10) * 0.8)


def _build_impact_stability_prompt(
    action: str, anomaly_type: str, resource_type: str, raw_metrics: dict
) -> str:
    """
    입력: action, anomaly_type, resource_type, raw_metrics
    출력: EC2 전용 impact_score/stability_score 추정 프롬프트 문자열
    (saving_rate는 공식으로 이미 계산했으므로 LLM에는 요청하지 않는다)
    """
    return f"""다음 클라우드 리소스 이상 상황에서 '{action}' 액션을 실행했을 때의
        서비스 영향도(impact_score)와 시스템 안정성(stability_score)만 추정해줘.

        리소스 타입: {resource_type}
        이상 유형: {anomaly_type}
        최근 수집 지표: {json.dumps(raw_metrics, ensure_ascii=False)}

        아래 JSON 형식으로만 답해. 설명, 마크다운, 다른 텍스트는 절대 포함하지 마:
        {{"impact_score": <0.0~1.0 사이 숫자>, "stability_score": <0.0~1.0 사이 숫자>}}
        """


def _build_full_score_prompt(
    action: str, anomaly_type: str, resource_type: str, raw_metrics: dict
) -> str:
    """
    입력: action, anomaly_type, resource_type, raw_metrics
    출력: Lambda/S3/AutoScaling/RDS 전용 saving_rate/impact_score/stability_score
          전체 추정 프롬프트 문자열
    """
    return f"""다음 클라우드 리소스 이상 상황에서 '{action}' 액션을 실행했을 때의
        예상 비용 절감률(saving_rate), 서비스 영향도(impact_score),
        시스템 안정성(stability_score)을 추정해줘.

        리소스 타입: {resource_type}
        이상 유형: {anomaly_type}
        최근 수집 지표: {json.dumps(raw_metrics, ensure_ascii=False)}

        아래 JSON 형식으로만 답해. 설명, 마크다운, 다른 텍스트는 절대 포함하지 마:
        {{"saving_rate": <0.0~1.0 사이 숫자>, "impact_score": <0.0~1.0 사이 숫자>, "stability_score": <0.0~1.0 사이 숫자>}}
        """


def _parse_llm_json(raw_text: str) -> dict | None:
    """
    입력: raw_text (LLM 원문 응답)
    출력: 파싱된 dict, 실패 시 None (마크다운 코드블록 등 흔한 노이즈 제거 후 시도)
    """
    cleaned = raw_text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`")
        cleaned = cleaned.removeprefix("json").strip()
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        return None


def _invoke_llm_with_retry(llm, prompt: str, action: str) -> dict | None:
    """
    입력: llm 클라이언트, prompt 문자열, action(로그 식별용)
    출력: 파싱된 JSON dict, 모든 재시도(MAX_LLM_RETRIES+1회) 실패 시 None
    """
    from langchain_core.messages import HumanMessage

    for attempt in range(MAX_LLM_RETRIES + 1):
        try:
            response = llm.invoke([HumanMessage(content=prompt)])
            parsed = _parse_llm_json(response.content)
            if parsed is None:
                logger.warning(
                    "LLM 응답 JSON 파싱 실패 (action=%s, attempt=%d): %r",
                    action, attempt, response.content,
                )
                continue
            return parsed
        except Exception as exc:  # noqa: BLE001 - LLM 호출은 광범위하게 방어
            logger.warning(
                "LLM 호출 실패 (action=%s, attempt=%d): %s", action, attempt, exc
            )

    logger.error("LLM 응답을 끝내 파싱하지 못함 (action=%s)", action)
    return None


def _estimate_impact_stability_with_llm(
    action: str, anomaly_type: str, resource_type: str, raw_metrics: dict
) -> tuple[float, float]:
    """
    입력: action, anomaly_type, resource_type, raw_metrics
    출력: (impact_score, stability_score) — EC2 전용, saving_rate는 포함하지 않음

    LLM 미설정 시 룰 기반 테이블로 fallback.
    재시도 끝까지 실패 시 NoAction 쪽으로 유도하도록 (impact=1.0, stability=0.0) 처리.
    """
    llm = _get_llm()
    if llm is None:
        _, impact, stability = RULE_BASED_SCORE_TABLE.get(action, (0.0, 0.0, 1.0))
        return impact, stability

    prompt = _build_impact_stability_prompt(action, anomaly_type, resource_type, raw_metrics)
    parsed = _invoke_llm_with_retry(llm, prompt, action)
    if parsed is None:
        return 1.0, 0.0

    impact = _clamp01(parsed.get("impact_score", 1.0))
    stability = _clamp01(parsed.get("stability_score", 0.0))
    return impact, stability


def _estimate_full_scores_with_llm(
    action: str, anomaly_type: str, resource_type: str, raw_metrics: dict
) -> tuple[float, float, float]:
    """
    입력: action, anomaly_type, resource_type, raw_metrics
    출력: (saving_rate, impact_score, stability_score) — Lambda/S3/AutoScaling/RDS 전용

    LLM 미설정 시 룰 기반 테이블로 fallback.
    재시도 끝까지 실패 시 NoAction 쪽으로 유도하도록 (0.0, 1.0, 0.0) 처리.
    """
    llm = _get_llm()
    if llm is None:
        return RULE_BASED_SCORE_TABLE.get(action, (0.0, 0.0, 1.0))

    prompt = _build_full_score_prompt(action, anomaly_type, resource_type, raw_metrics)
    parsed = _invoke_llm_with_retry(llm, prompt, action)
    if parsed is None:
        return 0.0, 1.0, 0.0

    saving = _clamp01(parsed.get("saving_rate", 0.0))
    impact = _clamp01(parsed.get("impact_score", 1.0))
    stability = _clamp01(parsed.get("stability_score", 0.0))
    return saving, impact, stability


def _score_components(action: str, state: PipelineState) -> tuple[float, float, float]:
    """
    입력: action, state (resource_type/anomaly_type/raw_metrics 사용)
    출력: 액션 1개에 대한 (saving_rate, impact_score, stability_score)
    """
    if action == "NoAction":
        return RULE_BASED_SCORE_TABLE["NoAction"]

    resource_type = state["resource_type"]
    anomaly_type = state["anomaly_type"]
    raw_metrics = state.get("raw_metrics", {})

    if resource_type == "EC2":
        saving_rate = _ec2_saving_rate(raw_metrics)
        impact_score, stability_score = _estimate_impact_stability_with_llm(
            action, anomaly_type, resource_type, raw_metrics
        )
        return saving_rate, impact_score, stability_score

    # Lambda / S3 / AutoScaling / RDS: 세 점수 모두 LLM 위임
    return _estimate_full_scores_with_llm(action, anomaly_type, resource_type, raw_metrics)


# 3. 핵심 함수
def decision_node(state: PipelineState) -> PipelineState:
    """
    입력: state["anomaly_type"], state["resource_type"], state["raw_metrics"]
    출력: state["candidate_actions"], state["selected_action"], state["risk_level"],
          state["requires_approval"], state["decision_reasoning"], state["target_instance_type"]
    """
    anomaly_type = state["anomaly_type"]
    resource_type = state["resource_type"]

    # 환각 방어 3: anomaly_type이 허용 표에 없으면 NoAction만 허용
    allowed_actions = ALLOWED_ACTIONS.get(anomaly_type)
    if not allowed_actions:
        allowed_actions = ["NoAction"]

    candidates: list[CandidateAction] = []
    for action in allowed_actions:
        saving_rate, impact_score, stability_score = _score_components(action, state)
        score = 0.5 * saving_rate - 0.3 * impact_score + 0.2 * stability_score
        candidates.append(
            {
                "action": action,  # type: ignore[typeddict-item]
                "saving_rate": saving_rate,
                "impact_score": impact_score,
                "stability_score": stability_score,
                "score": score,
            }
        )

    selected = max(candidates, key=lambda c: c["score"])
    risk = resolve_risk_level(anomaly_type=anomaly_type, selected_action=selected["action"])

    scoring_desc = (
        "공식 기반 saving_rate + LLM impact/stability"
        if resource_type == "EC2"
        else "LLM 전체 점수 추정"
    )

    state["candidate_actions"] = candidates
    state["selected_action"] = selected["action"]
    state["risk_level"] = risk
    state["requires_approval"] = risk in ("MED", "HIGH")
    state["target_instance_type"] = (
        DEFAULT_TARGET_INSTANCE_TYPE if selected["action"] == "Resize" else None
    )
    state["decision_reasoning"] = (
        f"{scoring_desc}로 후보 {len(candidates)}개 중 '{selected['action']}' 선택 "
        f"(score={selected['score']:.2f}, risk={risk})"
    )
    return state
