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

# [ADDED] 액션 선택을 LLM에게 맡기기 위한 boto3 공식 API 스펙 테이블.
# ALLOWED_ACTIONS로 허용된 액션에 한해서만 이 스펙이 프롬프트에 포함된다.
BOTO3_SPEC: dict[str, dict] = {
    "Stop": {
        "api": "ec2.stop_instances(InstanceIds=['string'], Force=True|False)",
        "description": "EC2 인스턴스를 즉시 중지. stopping → stopped 상태로 전환.",
        "cost_effect": "EC2 컴퓨팅 비용 100% 제거 (EBS 스토리지 비용은 유지됨)",
        "side_effects": "서비스 완전 중단. 재시작 전까지 접근 불가.",
        "reversible": True,
        "exceptions": ["InvalidInstanceID.NotFound", "IncorrectInstanceState"],
    },
    "Stop+Schedule": {
        "api": "ec2.stop_instances() + scheduler.create_schedule(ScheduleExpression='cron(0 9 ? * MON-FRI *)')",
        "description": "즉시 중지 후 EventBridge Scheduler로 업무시간 재시작 등록.",
        "cost_effect": "업무시간 외 운영 중단으로 평균 50% 비용 절감 (9~18시 운영 가정)",
        "side_effects": "야간/주말 서비스 중단. 스케줄 등록 실패 시 재시작 안 됨.",
        "reversible": True,
        "exceptions": ["InvalidInstanceID.NotFound", "ConflictException(scheduler)"],
    },
    "Resize": {
        "api": "ec2.stop_instances() → ec2.modify_instance_attribute(InstanceId, InstanceType={'Value': 'string'}) → ec2.start_instances()",
        "description": "인스턴스 타입 다운사이즈. 반드시 stopped 상태에서만 실행 가능.",
        "cost_effect": "단가 차이만큼 절감. t3.medium → t3.small: 약 50% 절감.",
        "side_effects": "재시작 필요 (다운타임 발생). 일부 타입 간 변경 불가.",
        "reversible": True,
        "exceptions": ["IncorrectInstanceState", "InvalidParameterValue(타입 변경 불가)"],
    },
    "Throttle": {
        "api": "lambda.put_function_concurrency(FunctionName='string', ReservedConcurrentExecutions=123)",
        "description": "Lambda 함수의 최대 동시 실행 수를 예약. 기본값 10으로 제한.",
        "cost_effect": "급증분(초과 호출)만 차단. 정상 범위 내 비용은 유지.",
        "side_effects": "ReservedConcurrentExecutions=0 설정 시 함수 완전 중단. 계정 전체에 최소 100개 미예약 슬롯 필요.",
        "reversible": True,
        "exceptions": ["InvalidParameterValueException", "ResourceNotFoundException", "TooManyRequestsException"],
    },
    "ScaleDown": {
        "api": "autoscaling.update_auto_scaling_group(AutoScalingGroupName='string', MaxSize=123, MinSize=123, DesiredCapacity=123)",
        "description": "AutoScaling 그룹의 최대 인스턴스 수 제한. 기본 MaxSize=2.",
        "cost_effect": "급증 인스턴스 수 제한으로 초과 비용 차단.",
        "side_effects": "MaxSize를 현재 실행 수보다 낮추면 즉시 termination 발생. 트래픽 처리 용량 감소.",
        "reversible": True,
        "exceptions": ["ResourceContentionFault", "ScalingActivityInProgressFault"],
    },
    "Block": {
        "api": "s3.put_public_access_block(Bucket='string', PublicAccessBlockConfiguration={'BlockPublicAcls': True, 'IgnorePublicAcls': True, 'BlockPublicPolicy': True, 'RestrictPublicBuckets': True})",
        "description": "S3 버킷 퍼블릭 접근 완전 차단. 4개 설정 모두 True.",
        "cost_effect": "비용 절감 목적이 아닌 보안 위협 차단 목적. saving_rate=0.0 고정.",
        "side_effects": "퍼블릭 접근이 필요한 정상 사용자도 차단됨.",
        "reversible": True,
        "exceptions": ["NoSuchBucket"],
    },
    "NoAction": {
        "api": None,
        "description": "API 호출 없음. 이상 탐지됐으나 현재 조치 불필요 또는 위험도 낮음.",
        "cost_effect": "없음.",
        "side_effects": "없음. 모니터링만 지속.",
        "reversible": True,
        "exceptions": [],
    },
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


def _trend_based_partial_saving(raw_metrics: dict) -> tuple[float, float, float] | None:
    """
    입력: raw_metrics (cost 리스트 포함)
    출력: (saving_rate, estimated_saving_usd, recent_avg), 데이터 부족 시 None

    Throttle/ScaleDown처럼 "완전히 끄는 게 아니라 급증분만 깎는" 액션에 사용.
    cost 윈도우를 앞쪽(기준선)과 뒤쪽(최근 급증 구간)으로 나눠, 급증 구간
    평균이 기준선 평균보다 얼마나 높은지를 "제거 가능한 초과분"으로 본다.

    recent_avg(최근 급증 구간 평균)도 함께 반환하는 이유: 이 액션들의
    estimated_saving_usd는 raw_metrics 전체 평균이 아니라 이 recent_avg를
    기준으로 계산됐기 때문에, decision_node에서 "before -> after 비용"을
    표시할 때도 전체 평균이 아니라 이 recent_avg를 "before"로 써야
    절감액과 앞뒤가 맞는다 (전체 평균을 쓰면 급증 이전 구간에 희석되어
    after 비용이 0 밑으로 내려가는 문제가 있었음).
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
        return 0.0, 0.0, recent_avg

    excess = max(0.0, recent_avg - baseline_avg)
    saving_rate = _clamp01(excess / recent_avg)
    estimated_saving_usd = round(excess, 6)
    return saving_rate, estimated_saving_usd, recent_avg


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


# [REMOVED] _build_impact_stability_prompt() — 후보별 impact/stability 점수 공식이
# 폐기되면서 더 이상 필요 없음. 대신 LLM이 boto3 스펙을 직접 보고 액션 1개를
# 선택하는 _build_action_selection_prompt()로 교체됨.


# [ADDED] LLM이 boto3 스펙을 근거로 액션을 직접 선택하도록 만드는 프롬프트.
def _build_action_selection_prompt(
    allowed_actions: list[str],
    anomaly_type: str,
    resource_type: str,
    raw_metrics: dict,
) -> str:
    """
    입력: allowed_actions (anomaly_type 기준 허용 액션 목록),
          anomaly_type, resource_type, raw_metrics
    출력: LLM에게 넘길 액션 선택 프롬프트
    """
    spec_text = ""
    for action in allowed_actions:
        spec = BOTO3_SPEC.get(action, {})
        spec_text += f"""
[{action}]
  API: {spec.get('api', 'N/A')}
  설명: {spec.get('description', '')}
  비용 효과: {spec.get('cost_effect', '')}
  부작용: {spec.get('side_effects', '')}
  복구 가능: {spec.get('reversible', True)}
"""

    # [ADDED] 액션별로 이미 결정론적으로 계산된 saving_rate/estimated_saving_usd를
    # LLM에게 그대로 제공한다 (LLM이 비용 수치를 다시 추정하지 않도록 하기 위함).
    # _score_components()는 dict에서 resource_type/anomaly_type/raw_metrics 세
    # 키만 읽으므로, 전체 PipelineState 대신 이 세 값만 담은 최소 dict로 충분하다.
    pseudo_state = {
        "resource_type": resource_type,
        "anomaly_type": anomaly_type,
        "raw_metrics": raw_metrics,
    }
    action_scores = {}
    for action in allowed_actions:
        s_rate, _, _, s_usd = _score_components(action, pseudo_state)  # type: ignore[arg-type]
        action_scores[action] = {
            "saving_rate": round(s_rate, 3),
            "estimated_saving_usd_per_hr": round(s_usd, 4),
        }

    metrics_summary = {}
    for key in ["cpu_utilization", "network_in", "cost", "invocation_count",
                "error_count", "group_in_service_instances", "bytes_downloaded"]:
        values = raw_metrics.get(key, [])
        if values:
            metrics_summary[key] = {"mean": round(_mean(values), 4), "n": len(values)}

    return f"""당신은 AWS FinOps 전문가입니다.
클라우드 리소스 이상 탐지 후 실행할 최적 액션 1개를 선택해주세요.

## 현재 상황
- 리소스 타입: {resource_type}
- 이상 유형: {anomaly_type}
- 핵심 지표 요약: {json.dumps(metrics_summary, ensure_ascii=False)}

## 액션별 사전 계산된 비용 절감 수치 (cost 시계열 기반)
{json.dumps(action_scores, ensure_ascii=False)}

## 실행 가능한 액션 및 boto3 API 스펙 (가용성 판단용)
{spec_text}

## 선택 기준
1. 비용 절감 수치와 가용성 부작용을 함께 고려할 것
2. 서비스 가용성을 최우선으로 보호할 것
3. 이상 유형에 맞는 액션을 선택할 것
4. 부작용이 최소화되는 방향으로 보수적으로 판단할 것

아래 JSON 형식으로만 응답하세요. 설명, 마크다운, 다른 텍스트 절대 포함 금지:
{{"action": "<액션명>", "reason": "<선택 이유 한 문장>"}}
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


# [REMOVED] _estimate_impact_stability_with_llm() — 후보별 impact/stability 점수
# 추정이 폐기되면서 더 이상 필요 없음. 대신 LLM이 boto3 스펙을 보고 액션을
# 직접 하나 선택하는 _select_action_with_llm()으로 교체됨.


# [ADDED] LLM이 boto3 스펙을 보고 액션 1개를 직접 추천하도록 하는 함수.
def _select_action_with_llm(
    allowed_actions: list[str],
    anomaly_type: str,
    resource_type: str,
    raw_metrics: dict,
) -> tuple[str, str]:
    """
    입력: allowed_actions, anomaly_type, resource_type, raw_metrics
    출력: (selected_action, reason)

    LLM이 boto3 스펙을 보고 직접 액션 1개를 추천.
    실패 시 RULE_BASED_SCORE_TABLE에서 가장 높은 saving_rate 액션으로 fallback.
    """
    llm = _get_llm()
    if llm is None:
        return _rule_based_fallback_action(allowed_actions), "LLM 미설정 - 룰 기반 선택"

    prompt = _build_action_selection_prompt(
        allowed_actions, anomaly_type, resource_type, raw_metrics
    )
    parsed = _invoke_llm_with_retry(llm, prompt, "action_selection")

    if parsed is None:
        return _rule_based_fallback_action(allowed_actions), "LLM 실패 - 룰 기반 선택"

    llm_action = parsed.get("action", "NoAction")
    reason = parsed.get("reason", "")

    # 환각 방어: ALLOWED_ACTIONS 범위 밖이면 fallback
    if llm_action not in allowed_actions:
        logger.warning(
            "LLM이 허용되지 않은 액션 추천: %s (allowed: %s)", llm_action, allowed_actions
        )
        return _rule_based_fallback_action(allowed_actions), "허용 범위 초과 - 룰 기반 선택"

    return llm_action, reason


def _rule_based_fallback_action(allowed_actions: list[str]) -> str:
    """
    입력: allowed_actions
    출력: RULE_BASED_SCORE_TABLE에서 saving_rate 가장 높은 허용 액션
    """
    return max(
        allowed_actions,
        key=lambda a: RULE_BASED_SCORE_TABLE.get(a, (0.0, 0.0, 1.0))[0],
    )


def _score_components(action: str, state: PipelineState) -> tuple[float, float, float, float]:
    """
    입력: action, state (resource_type/anomaly_type/raw_metrics 사용)
    출력: 액션 1개에 대한 (saving_rate, impact_score, stability_score, estimated_saving_usd)

    impact_score/stability_score는 [ADDED] 액션 선택이 LLM의 boto3 스펙 기반
    직접 선택(_select_action_with_llm)으로 바뀌면서 더 이상 추정하지 않고
    0.0 고정값으로 둔다 (CandidateAction 스키마 호환을 위해 필드는 유지).
    """
    if action == "NoAction":
        saving, _, _ = RULE_BASED_SCORE_TABLE["NoAction"]
        return saving, 0.0, 0.0, 0.0

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
            saving_rate, estimated_saving_usd, _ = result
        else:
            saving_rate = _estimate_saving_rate_with_llm(
                action, anomaly_type, resource_type, raw_metrics
            )
    else:
        saving_rate = _estimate_saving_rate_with_llm(
            action, anomaly_type, resource_type, raw_metrics
        )

    return saving_rate, 0.0, 0.0, estimated_saving_usd


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

    # [REMOVED] 기존 후보별 LLM impact/stability 추정 + 점수 공식 기반 선택
    # candidates: list[CandidateAction] = []
    # for action in allowed_actions:
    #     saving_rate, impact_score, stability_score, estimated_saving_usd = _score_components(...)
    #     score = 0.5 * saving_rate - 0.3 * impact_score + 0.2 * stability_score
    #     candidates.append(...)
    # selected = max(candidates, key=lambda c: c["score"])

    # [ADDED] LLM이 boto3 스펙을 보고 액션을 직접 선택
    selected_action, llm_reason = _select_action_with_llm(
        allowed_actions, anomaly_type, resource_type, raw_metrics
    )

    # saving_rate / estimated_saving_usd는 기존 결정론적 계산 그대로 유지
    saving_rate, _, _, estimated_saving_usd = _score_components(selected_action, state)

    # candidate_actions는 allowed_actions 전체에 대해 saving_rate만 채워서 기록
    candidates: list[CandidateAction] = []
    for action in allowed_actions:
        s_rate, _, _, s_usd = _score_components(action, state)
        candidates.append(
            {
                "action": action,  # type: ignore[typeddict-item]
                "saving_rate": s_rate,
                "impact_score": 0.0,   # LLM 추정 제거됨
                "stability_score": 0.0,
                "score": s_rate,       # score 필드는 saving_rate로 대체
                "estimated_saving_usd": s_usd,
            }
        )

    selected = next(c for c in candidates if c["action"] == selected_action)
    risk = resolve_risk_level(anomaly_type=anomaly_type, selected_action=selected["action"])

    # Resize가 선택된 경우에만 cost 기반으로 역추정한 목표 인스턴스 타입을 채운다
    # (saving_rate 계산과 동일한 로직 재사용, EC2 전용).
    target_instance_type = None
    if selected["action"] == "Resize":
        _, _, target_instance_type = _ec2_resize_saving(raw_metrics)

    # [ADDED] "비용이 얼마에서 얼마로 줄었는지"를 decision_reasoning에서 바로 확인할 수 있도록
    # 현재 비용(before)과 절감 적용 후 예상 비용(after)을 함께 계산한다.
    # Throttle/ScaleDown은 estimated_saving_usd 자체가 raw_metrics 전체 평균이 아니라
    # "최근 급증 구간 평균 대비 기준선" 기준으로 계산되므로(_trend_based_partial_saving),
    # before 비용도 전체 평균이 아니라 같은 최근 급증 구간 평균을 써야 앞뒤가 맞는다
    # (target_instance_type을 위해 _ec2_resize_saving을 다시 부르는 것과 같은 패턴).
    if selected["action"] in ("Throttle", "ScaleDown"):
        trend_result = _trend_based_partial_saving(raw_metrics)
        current_cost_usd = trend_result[2] if trend_result is not None else _mean(raw_metrics.get("cost", []))
    else:
        current_cost_usd = _mean(raw_metrics.get("cost", []))
    after_cost_usd = max(0.0, current_cost_usd - estimated_saving_usd)

    state["candidate_actions"] = candidates
    state["selected_action"] = selected["action"]
    state["risk_level"] = risk
    state["requires_approval"] = risk in ("MED", "HIGH")
    state["target_instance_type"] = target_instance_type
    state["decision_reasoning"] = (
        f"LLM boto3 스펙 기반 선택: '{selected_action}' - {llm_reason} "
        f"(risk={risk}, cost {current_cost_usd:.4f} -> {after_cost_usd:.4f} USD/hr, "
        f"절감액={estimated_saving_usd:.4f}/hr)"
    )
    return state
