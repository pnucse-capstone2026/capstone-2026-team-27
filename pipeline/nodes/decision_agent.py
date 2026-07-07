"""
Decision Agent
==============
node_contracts.md Step 3 기준.

입력 (읽는 필드):
  - anomaly_type, resource_type, resource_id, classification_reasoning,
    anomaly_score_zscore, raw_metrics

출력 (채우는 필드):
  - candidate_actions, selected_action, risk_level,
    requires_approval, decision_reasoning

설계:
  - 명확한 케이스 → 룰 기반 점수 테이블 (LLM 호출 없음, 비용/지연 절약)
  - 애매한 케이스(anomaly_score가 임계값 근처) → LLM(Gemini)에게 saving_rate /
    impact_score / stability_score 추정 위임
  - LLM 미설정(GEMINI_API_KEY 없음) 시에도 동작해야 하므로,
    LLM 호출은 항상 try/except로 감싸고 실패 시 룰 기반 fallback으로 전환한다.
  - 환각 방어(보고서 4.1절):
      1) saving_rate/impact_score/stability_score를 [0.0, 1.0]로 클램핑
      2) JSON 파싱 실패 시 최대 N회 재시도, 끝까지 실패하면 saving_rate=0.0 처리
         (자동으로 NoAction 쪽으로 점수가 떨어지도록 유도)
      3) action은 ALLOWED_ACTIONS 표 밖의 값이 나올 수 없음
         (LLM에게 액션 후보 자체를 만들게 하지 않고, 미리 정의된 액션에 대한
          점수만 추정하게 했기 때문에 구조적으로 막혀 있음)
      4) temperature=0.1로 고정
"""

# 애매한 구간에서만 LLM 호출
# rule-based 점수는 항상 동작
# 환각 방어 4가지

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
# 애매하다고 판단하는 z-score 구간 (임계값 3.0 근처)
AMBIGUOUS_ZSCORE_LOW = 2.5
AMBIGUOUS_ZSCORE_HIGH = 3.5

# JSON 파싱 재시도 최대 횟수
MAX_LLM_RETRIES = 2

# 룰 기반 기본 점수 테이블 (action -> (saving_rate, impact_score, stability_score))
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
    """0.0 ~ 1.0 범위로 강제 클램핑 (환각 방어 1차 방어선)."""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, min(1.0, v))


def _is_ambiguous(state: PipelineState) -> bool:
    """anomaly_score_zscore가 임계값(3.0) 근처면 모호한 케이스로 판단."""
    z = state.get("anomaly_score_zscore")
    if z is None:
        return False
    return AMBIGUOUS_ZSCORE_LOW < z < AMBIGUOUS_ZSCORE_HIGH


def _get_llm():
    """
    LangChain Gemini 클라이언트 생성.
    GEMINI_API_KEY가 없거나 langchain_google_genai가 설치되지 않으면 None 리턴
    (호출부에서 None이면 룰 기반으로 fallback).
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

    # langchain
    return ChatGoogleGenerativeAI(
        model="gemini-2.5-flash",
        google_api_key=api_key,
        temperature=0.1,  # temperature 고정으로 일관성 확보
    )

# prompt 생성
def _build_prompt(action: str, anomaly_type: str, raw_metrics: dict) -> str:
    return f"""다음 클라우드 리소스 이상 상황에서 '{action}' 액션을 실행했을 때의
        예상 비용 절감률(saving_rate)만 추정해줘.

        이상 유형: {anomaly_type}
        최근 수집 지표: {json.dumps(raw_metrics, ensure_ascii=False)}

        아래 JSON 형식으로만 답해. 설명, 마크다운, 다른 텍스트는 절대 포함하지 마:
        {{"saving_rate": <0.0~1.0 사이 숫자>}}
        """


def _parse_llm_json(raw_text: str) -> dict | None:
    """LLM 응답에서 JSON 파싱. 마크다운 코드블록 등 흔한 노이즈 제거."""
    cleaned = raw_text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`")
        cleaned = cleaned.removeprefix("json").strip()
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        return None


def _estimate_scores_with_llm(
    action: str, anomaly_type: str, raw_metrics: dict
) -> tuple[float, float, float]:
    """
    모호한 케이스에서 LLM에게 점수 추정을 위임.
    실패(LLM 없음 / JSON 파싱 실패 / 예외) 시 룰 기반 테이블로 fallback.
    """
    llm = _get_llm()
    if llm is None:
        return RULE_BASED_SCORE_TABLE.get(action, (0.0, 0.0, 1.0))

    from langchain_core.messages import HumanMessage

    prompt = _build_prompt(action, anomaly_type, raw_metrics)

    for attempt in range(MAX_LLM_RETRIES + 1):
        try:
            # LLM 호출
            response = llm.invoke([HumanMessage(content=prompt)])
            parsed = _parse_llm_json(response.content)
            if parsed is None:
                logger.warning(
                    "LLM 응답 JSON 파싱 실패 (action=%s, attempt=%d): %r",
                    action, attempt, response.content,
                )
                continue

            saving = _clamp01(parsed.get("saving_rate", 0.0))
            _, impact, stability = RULE_BASED_SCORE_TABLE.get(action, (0.0, 1.0, 0.0))
            return saving, impact, stability

        except Exception as exc:  # noqa: BLE001 - LLM 호출은 광범위하게 방어
            logger.warning(
                "LLM 호출 실패 (action=%s, attempt=%d): %s", action, attempt, exc
            )

    # 모든 재시도 실패 → 보고서 4.1절: saving_rate=0.0으로 떨어뜨려 NoAction 유도
    logger.error(
        "LLM 응답을 끝내 파싱하지 못함 (action=%s) → saving_rate=0.0 처리", action
    )
    return 0.0, 1.0, 0.0


def _score_components(
    action: str, state: PipelineState, ambiguous: bool
) -> tuple[float, float, float]:
    """액션 1개에 대한 (saving_rate, impact_score, stability_score) 계산."""
    if not ambiguous:
        return RULE_BASED_SCORE_TABLE.get(action, (0.0, 0.0, 1.0))
    return _estimate_scores_with_llm(
        action=action,
        anomaly_type=state["anomaly_type"],
        raw_metrics=state.get("raw_metrics", {}),
    )

# 3. 핵심 함수
def decision_node(state: PipelineState) -> PipelineState:
    anomaly_type = state["anomaly_type"]

    # 환각 방어 3: anomaly_type이 허용 표에 없으면 NoAction만 허용
    allowed_actions = ALLOWED_ACTIONS.get(anomaly_type)
    if not allowed_actions:
        allowed_actions = ["NoAction"]

    ambiguous = _is_ambiguous(state)

    candidates: list[CandidateAction] = []
    for action in allowed_actions:
        saving_rate, impact_score, stability_score = _score_components(
            action, state, ambiguous
        )
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

    state["candidate_actions"] = candidates
    state["selected_action"] = selected["action"]
    state["risk_level"] = risk
    state["requires_approval"] = risk in ("MED", "HIGH")
    state["decision_reasoning"] = (
        f"{'LLM 추정' if ambiguous else '룰 기반'} 점수로 "
        f"후보 {len(candidates)}개 중 '{selected['action']}' 선택 "
        f"(score={selected['score']:.2f}, risk={risk})"
    )
    return state
