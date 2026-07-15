"""
Decision Agent
==============
node_contracts.md Step 3 기준.

입력 (읽는 필드):
  - anomaly_type, resource_type, resource_id, classification_reasoning, raw_metrics

출력 (채우는 필드):
  - candidate_actions, selected_action, risk_level,
    requires_approval, decision_reasoning, target_instance_type

설계 (saving_rate 산정 방식 — 2026-07 중간보고 시점 개선):
  - saving_rate는 "LLM이 추정하는 값"이 아니라 raw_metrics["cost"] 시계열로부터
    결정론적으로 계산하는 것을 기본으로 한다. 액션별 계산 방식:
      * Stop           : 리소스를 완전히 멈추므로 현재 평균 비용의 100%가 절감된다고 본다.
      * Stop+Schedule  : "업무시간 외 절반 정도 꺼둔다"는 단순화된 가정(50%)을 적용한다.
      * Resize         : EC2_HOURLY_PRICE_USD 정적 단가표에서 현재 평균 비용과 가장
                         가까운 tier를 "현재 인스턴스 타입"으로 역추정하고, 한 단계
                         저렴한 tier로 다운사이즈했을 때의 단가 차이로 계산한다.
      * Throttle/ScaleDown : 비용을 0으로 만드는 게 아니라 "급증분만 깎는" 액션이므로,
                         cost 윈도우를 기준선(앞쪽)과 최근 급증 구간(뒤쪽)으로 나눠
                         그 차이(초과분)를 절감 가능액으로 본다.
      * Block          : 목적이 비용 절감이 아니라 보안 위협 차단이므로 saving_rate를
                         인위적으로 만들지 않고 0.0으로 고정한다. Block의 실행 여부는
                         impact_score/stability_score와 risk_level 승인 게이트로 판단한다.
      * NoAction       : 항상 (0.0, 0.0, 1.0) 룰 기반 더미 값.
  - cost 데이터가 없거나 너무 짧아 위 결정론적 계산이 불가능한 예외 상황에서만
    LLM에게 saving_rate 추정을 맡긴다 (_estimate_saving_rate_with_llm).
  - impact_score / stability_score는 정량화가 어려운 값이라 리소스 타입에 관계없이
    항상 LLM(Gemini)에게 위임한다 (기존과 동일, EC2 전용이 아니라 전 리소스 공통으로 통일).
  - 각 후보 액션에는 saving_rate(비율, [0,1]) 외에 estimated_saving_usd(시간당 USD
    절감 예상액)도 함께 기록한다. LLM fallback으로 산정된 saving_rate는 근거 있는
    금액을 만들어낼 수 없으므로 estimated_saving_usd=0.0으로 둔다.
  - LLM 미설정(GEMINI_API_KEY 없음) 시에도 동작해야 하므로,
    LLM 호출은 항상 try/except로 감싸고 실패 시 룰 기반 fallback으로 전환한다.
  - 환각 방어(보고서 4.1절):
      1) 모든 점수를 [0.0, 1.0]로 클램핑
      2) JSON 파싱 실패 시 최대 N회 재시도, 끝까지 실패하면 NoAction 쪽으로
         점수가 떨어지도록 impact_score=1.0 / stability_score=0.0 처리
      3) action은 ALLOWED_ACTIONS 표 밖의 값이 나올 수 없음
         (LLM에게 액션 후보 자체를 만들게 하지 않고, 미리 정의된 액션에 대한
          점수만 추정하게 했기 때문에 구조적으로 막혀 있음)
      4) temperature=0.1로 고정

한계 / 다음 단계 과제:
  - EC2_HOURLY_PRICE_USD는 ap-northeast-2 리전 온디맨드 기준 정적 근사치이며
    실제로는 AWS Pricing API로 대체해야 한다.
  - Resize의 "현재 인스턴스 타입"은 실제 타입을 조회하는 것이 아니라 cost 평균으로
    역추정한 근사치다. AWS 연동 후에는 Action Agent의 스냅샷처럼 실제 타입을
    그대로 사용하는 방향으로 교체해야 한다.
  - Stop+Schedule의 50% 가정은 실제 스케줄 정책(오프 시간 비율)이 정해지면
    정교화해야 한다.
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

# EC2 Resize 시 기본 목표 인스턴스 타입 (cost 데이터가 없어 역추정이 불가능할 때만 사용)
DEFAULT_TARGET_INSTANCE_TYPE = "t3.small"

# EC2 온디맨드 시간당 단가(USD) — ap-northeast-2(서울) 리전, 2026-07 기준 근사치.
# 가격 오름차순으로 정렬되어 있어야 "한 단계 다운사이즈" 로직이 성립한다.
# 실제 운영에서는 AWS Pricing API로 대체해야 하는 캡스톤 스코프의 정적 참고 테이블이다.
EC2_HOURLY_PRICE_USD: list[tuple[str, float]] = [
    ("t3.micro", 0.0104),
    ("t3.small", 0.0210),
    ("t3.medium", 0.0420),
    ("t3.large", 0.0830),
    ("t3.xlarge", 0.1660),
]

# Stop+Schedule 절감률 가정치: "업무시간 외(야간/주말)에 절반 정도 꺼둔다"는
# 단순화된 가정. 실제 스케줄 정책이 정해지면 이 상수를 정교화해야 한다.
STOP_SCHEDULE_DUTY_CYCLE_ASSUMPTION = 0.5

# Throttle/ScaleDown의 "급증분 대비 기준선" 계산에 필요한 최소 cost 데이터 포인트 수.
# 이보다 적으면 급증 구간과 평상 구간을 나눌 수 없어 LLM/규칙 기반 fallback으로 넘어간다.
MIN_COST_POINTS_FOR_TREND = 4

# 위 구간을 나눌 때 "최근 급증 구간"으로 볼 뒤쪽 포인트 비율/최소 개수
RECENT_SPIKE_WINDOW_RATIO = 0.2
MIN_RECENT_SPIKE_POINTS = 3

# 룰 기반 기본 점수 테이블 (action -> (saving_rate, impact_score, stability_score))
# NoAction의 기본값이자, LLM 미설정/완전 실패 시 fallback 테이블로도 쓰인다.
# (saving_rate 항목은 이제 cost 결정론적 계산이 불가능한 예외 상황에서만 참조된다)
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


def _mean(values: list[float]) -> float:
    """입력: values 출력: 산술 평균, 빈 리스트면 0.0"""
    return sum(values) / len(values) if values else 0.0


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


# ── saving_rate 결정론적 계산 (cost 시계열 기반) ─────────────────────────────

def _cost_based_full_removal_saving(
    raw_metrics: dict, fraction: float
) -> tuple[float, float] | None:
    """
    입력: raw_metrics (cost 리스트 포함), fraction (제거되는 비율, 0~1)
    출력: (saving_rate, estimated_saving_usd), cost 데이터 없으면 None

    Stop(fraction=1.0)/Stop+Schedule(fraction=듀티사이클 가정치)에 공통 사용.
    "현재 평균 비용 × fraction"을 절감 예상액으로, fraction 자체를
    saving_rate(현재 지출 대비 제거되는 비율)로 사용한다.
    """
    cost_values = raw_metrics.get("cost", [])
    if not cost_values:
        return None

    avg_cost = _mean(cost_values)
    estimated_saving_usd = round(avg_cost * fraction, 6)
    saving_rate = _clamp01(fraction)
    return saving_rate, estimated_saving_usd


def _ec2_resize_saving(raw_metrics: dict) -> tuple[float, float, str]:
    """
    입력: raw_metrics (EC2Metrics, cost 리스트 포함)
    출력: (saving_rate, estimated_saving_usd, target_instance_type)

    cost 평균을 EC2_HOURLY_PRICE_USD 표에서 가장 가까운 단가와 매칭해
    "현재 인스턴스 타입"을 역추정하고, 한 단계 저렴한 타입으로 Resize했을 때의
    절감액을 계산한다. (raw_metrics에 실제 instance_type이 없어 cost로
    역추정하는 근사치이며, AWS 연동 후에는 실제 타입을 그대로 쓰는 방향으로
    교체해야 한다 — 파일 상단 "한계" 참고)
    """
    cost_values = raw_metrics.get("cost", [])
    if not cost_values:
        return 0.0, 0.0, DEFAULT_TARGET_INSTANCE_TYPE

    avg_cost = _mean(cost_values)

    # 평균 비용과 가장 가까운 단가 tier를 "현재 타입"으로 역추정
    current_idx = min(
        range(len(EC2_HOURLY_PRICE_USD)),
        key=lambda i: abs(EC2_HOURLY_PRICE_USD[i][1] - avg_cost),
    )
    current_type, current_price = EC2_HOURLY_PRICE_USD[current_idx]

    if current_idx == 0 or current_price <= 0:
        # 이미 최저 tier로 추정됨 → 더 다운사이즈해도 절감 없음
        return 0.0, 0.0, current_type

    target_type, target_price = EC2_HOURLY_PRICE_USD[current_idx - 1]

    saving_rate = _clamp01((current_price - target_price) / current_price)
    estimated_saving_usd = round(current_price - target_price, 6)
    return saving_rate, estimated_saving_usd, target_type


def _trend_based_partial_saving(raw_metrics: dict) -> tuple[float, float] | None:
    """
    입력: raw_metrics (cost 리스트 포함)
    출력: (saving_rate, estimated_saving_usd), 데이터 부족 시 None

    Throttle/ScaleDown처럼 "완전히 끄는 게 아니라 급증분만 깎는" 액션에 사용.
    cost 윈도우를 앞쪽(기준선)과 뒤쪽(최근 급증 구간)으로 나눠, 급증 구간
    평균이 기준선 평균보다 얼마나 높은지를 "제거 가능한 초과분"으로 본다.
    """
    cost_values = raw_metrics.get("cost", [])
    if len(cost_values) < MIN_COST_POINTS_FOR_TREND:
        return None

    recent_n = max(MIN_RECENT_SPIKE_POINTS, int(len(cost_values) * RECENT_SPIKE_WINDOW_RATIO))
    recent_n = min(recent_n, len(cost_values) - 1)  # 기준선에 최소 1개는 남겨야 함

    baseline_values = cost_values[:-recent_n]
    recent_values = cost_values[-recent_n:]

    baseline_avg = _mean(baseline_values)
    recent_avg = _mean(recent_values)

    if recent_avg <= 0:
        return 0.0, 0.0

    excess = max(0.0, recent_avg - baseline_avg)
    saving_rate = _clamp01(excess / recent_avg)
    estimated_saving_usd = round(excess, 6)
    return saving_rate, estimated_saving_usd


def _build_saving_rate_only_prompt(
    action: str, anomaly_type: str, resource_type: str, raw_metrics: dict
) -> str:
    """
    입력: action, anomaly_type, resource_type, raw_metrics
    출력: saving_rate 전용 추정 프롬프트 문자열

    cost 데이터가 부족해 결정론적 계산이 불가능한 예외 상황에서만 사용된다.
    """
    return f"""다음 클라우드 리소스 이상 상황에서 '{action}' 액션을 실행했을 때의
        예상 비용 절감률(saving_rate)만 추정해줘. 비용 데이터가 충분하지 않아
        수치 계산이 어려운 상황이니, 리소스 타입과 이상 유형을 참고해서 보수적으로
        추정해줘.

        리소스 타입: {resource_type}
        이상 유형: {anomaly_type}
        최근 수집 지표: {json.dumps(raw_metrics, ensure_ascii=False)}

        아래 JSON 형식으로만 답해. 설명, 마크다운, 다른 텍스트는 절대 포함하지 마:
        {{"saving_rate": <0.0~1.0 사이 숫자>}}
        """


def _build_impact_stability_prompt(
    action: str, anomaly_type: str, resource_type: str, raw_metrics: dict
) -> str:
    """
    입력: action, anomaly_type, resource_type, raw_metrics
    출력: impact_score/stability_score 추정 프롬프트 문자열 (리소스 타입 공통)
    (saving_rate는 cost 데이터로 이미 결정론적으로 계산했으므로 LLM에는 요청하지 않는다)
    """
    return f"""다음 클라우드 리소스 이상 상황에서 '{action}' 액션을 실행했을 때의
        서비스 영향도(impact_score)와 시스템 안정성(stability_score)만 추정해줘.

        리소스 타입: {resource_type}
        이상 유형: {anomaly_type}
        최근 수집 지표: {json.dumps(raw_metrics, ensure_ascii=False)}

        아래 JSON 형식으로만 답해. 설명, 마크다운, 다른 텍스트는 절대 포함하지 마:
        {{"impact_score": <0.0~1.0 사이 숫자>, "stability_score": <0.0~1.0 사이 숫자>}}
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


def _estimate_saving_rate_with_llm(
    action: str, anomaly_type: str, resource_type: str, raw_metrics: dict
) -> float:
    """
    입력: action, anomaly_type, resource_type, raw_metrics
    출력: saving_rate — cost 데이터 부족 등으로 결정론적 계산이 불가능할 때만
          호출되는 예외 경로. LLM 미설정/실패 시 RULE_BASED_SCORE_TABLE로 fallback.
    (estimated_saving_usd는 이 경로에서는 근거 있는 금액을 만들 수 없으므로
     호출부에서 항상 0.0으로 둔다)
    """
    llm = _get_llm()
    if llm is None:
        saving, _, _ = RULE_BASED_SCORE_TABLE.get(action, (0.0, 0.0, 1.0))
        return saving

    prompt = _build_saving_rate_only_prompt(action, anomaly_type, resource_type, raw_metrics)
    parsed = _invoke_llm_with_retry(llm, prompt, action)
    if parsed is None:
        saving, _, _ = RULE_BASED_SCORE_TABLE.get(action, (0.0, 0.0, 1.0))
        return saving
    return _clamp01(parsed.get("saving_rate", 0.0))


def _estimate_impact_stability_with_llm(
    action: str, anomaly_type: str, resource_type: str, raw_metrics: dict
) -> tuple[float, float]:
    """
    입력: action, anomaly_type, resource_type, raw_metrics
    출력: (impact_score, stability_score) — 리소스 타입 공통(EC2 전용이 아님)

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


def _score_components(action: str, state: PipelineState) -> tuple[float, float, float, float]:
    """
    입력: action, state (resource_type/anomaly_type/raw_metrics 사용)
    출력: 액션 1개에 대한 (saving_rate, impact_score, stability_score, estimated_saving_usd)
    """
    if action == "NoAction":
        saving, impact, stability = RULE_BASED_SCORE_TABLE["NoAction"]
        return saving, impact, stability, 0.0

    resource_type = state["resource_type"]
    anomaly_type = state["anomaly_type"]
    raw_metrics = state.get("raw_metrics", {})

    estimated_saving_usd = 0.0

    if action == "Block":
        # 보안 조치이지 비용 절감 조치가 아니므로 saving_rate를 인위적으로
        # 산정하지 않고 0으로 고정한다 (파일 상단 설계 설명 참고).
        saving_rate = 0.0
    elif action in ("Stop", "Stop+Schedule"):
        fraction = 1.0 if action == "Stop" else STOP_SCHEDULE_DUTY_CYCLE_ASSUMPTION
        result = _cost_based_full_removal_saving(raw_metrics, fraction)
        if result is not None:
            saving_rate, estimated_saving_usd = result
        else:
            saving_rate = _estimate_saving_rate_with_llm(
                action, anomaly_type, resource_type, raw_metrics
            )
    elif action == "Resize":
        saving_rate, estimated_saving_usd, _ = _ec2_resize_saving(raw_metrics)
    elif action in ("Throttle", "ScaleDown"):
        result = _trend_based_partial_saving(raw_metrics)
        if result is not None:
            saving_rate, estimated_saving_usd = result
        else:
            saving_rate = _estimate_saving_rate_with_llm(
                action, anomaly_type, resource_type, raw_metrics
            )
    else:
        saving_rate = _estimate_saving_rate_with_llm(
            action, anomaly_type, resource_type, raw_metrics
        )

    impact_score, stability_score = _estimate_impact_stability_with_llm(
        action, anomaly_type, resource_type, raw_metrics
    )

    return saving_rate, impact_score, stability_score, estimated_saving_usd


# 3. 핵심 함수
def decision_node(state: PipelineState) -> PipelineState:
    """
    입력: state["anomaly_type"], state["resource_type"], state["raw_metrics"]
    출력: state["candidate_actions"], state["selected_action"], state["risk_level"],
          state["requires_approval"], state["decision_reasoning"], state["target_instance_type"]
    """
    anomaly_type = state["anomaly_type"]
    resource_type = state["resource_type"]
    raw_metrics = state.get("raw_metrics", {})

    # 환각 방어 3: anomaly_type이 허용 표에 없으면 NoAction만 허용
    allowed_actions = ALLOWED_ACTIONS.get(anomaly_type)
    if not allowed_actions:
        allowed_actions = ["NoAction"]

    candidates: list[CandidateAction] = []
    for action in allowed_actions:
        saving_rate, impact_score, stability_score, estimated_saving_usd = _score_components(
            action, state
        )
        score = 0.5 * saving_rate - 0.3 * impact_score + 0.2 * stability_score
        candidates.append(
            {
                "action": action,  # type: ignore[typeddict-item]
                "saving_rate": saving_rate,
                "impact_score": impact_score,
                "stability_score": stability_score,
                "score": score,
                "estimated_saving_usd": estimated_saving_usd,
            }
        )

    selected = max(candidates, key=lambda c: c["score"])
    risk = resolve_risk_level(anomaly_type=anomaly_type, selected_action=selected["action"])

    # Resize가 선택된 경우에만 cost 기반으로 역추정한 목표 인스턴스 타입을 채운다
    # (saving_rate 계산과 동일한 로직 재사용, EC2 전용).
    target_instance_type = None
    if selected["action"] == "Resize":
        _, _, target_instance_type = _ec2_resize_saving(raw_metrics)

    state["candidate_actions"] = candidates
    state["selected_action"] = selected["action"]
    state["risk_level"] = risk
    state["requires_approval"] = risk in ("MED", "HIGH")
    state["target_instance_type"] = target_instance_type
    state["decision_reasoning"] = (
        f"cost 데이터 기반 saving_rate 계산 + LLM impact/stability 추정으로 "
        f"후보 {len(candidates)}개 중 '{selected['action']}' 선택 "
        f"(score={selected['score']:.2f}, risk={risk}, "
        f"estimated_saving_usd={selected['estimated_saving_usd']:.4f}/hr)"
    )
    return state
