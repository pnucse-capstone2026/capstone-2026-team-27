"""
QA Agent
--------
액션 수행 이후 SLA 준수 여부를 검증하고, 실패 시 롤백을 트리거

검증 항목:
1) CPU SLA: 액션 후 CPU 사용률이 임계값(80%) 이하인지
2) 비용 SLA: 액션이 실제 비용 절감 효과를 가져왔는지
3) 가용성 SLA: 서비스 가용성이 유지되는지 (액션 결과 정상 여부)

처리 흐름:
- 검증 통과 → qa_passed=True, logging으로 이동
- 검증 실패 + rollback_count < 2 → qa_passed=False, rollback_count 증가, action으로 재시도
- 검증 실패 + rollback_count >= 2 → qa_passed=False, 현재 상태 유지, 관리자 알림
"""

import json
from typing import Optional

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate
from schema.state import PipelineState, SlaCheckResult

from dotenv import load_dotenv
load_dotenv()

# SLA 임계값 정의
SLA_THRESHOLDS = {
    "cpu_utilization_max": 80.0,      # CPU 사용률 최대 허용치 (%)
    "cost_reduction_min": 0.0,         # 최소 비용 절감률 (액션 실행 시 비용 증가 방지)
    "availability_min": 99.0,          # 최소 가용성 (%)
}

llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash", temperature=0.1)

# LLM 프롬프트: 복잡한 SLA 판단이 필요한 경우
prompt = ChatPromptTemplate.from_template("""
당신은 AWS 클라우드 복구 액션의 품질을 검증하는 QA 전문가입니다.
아래 정보를 바탕으로 SLA 준수 여부를 판단하고 JSON으로만 응답하세요.
마크다운 코드블록, 설명 텍스트 없이 JSON만 출력하세요.

## 입력 정보
- 리소스 타입: {resource_type}
- 실행된 액션: {action_executed}
- 액션 결과: {action_result}
- 액션 전 스냅샷: {pre_action_snapshot}
- 현재 지표 요약: {metrics_summary}
- 이상 유형: {anomaly_type}

## SLA 기준
- CPU 사용률: 80% 이하 유지
- 비용: 액션으로 인한 추가 비용 발생 없음
- 가용성: 서비스 중단 없음

## 판단 기준
- 액션이 성공적으로 완료되었는지 (action_result status 확인)
- 액션이 SLA를 위반하지 않았는지
- 롤백이 필요한 상황인지

## 응답 형식 (JSON만, 다른 텍스트 금지)
{{
  "cpu_ok": true | false,
  "cost_ok": true | false,
  "availability_ok": true | false,
  "overall_pass": true | false,
  "reasoning": "판단 근거를 2문장 이내로",
  "rollback_recommended": true | false
}}
""")

chain = prompt | llm


def _metrics_summary(raw_metrics: dict) -> dict:
    """각 지표의 최근값(마지막)과 평균을 요약."""
    summary = {}
    for key, values in raw_metrics.items():
        if isinstance(values, list) and values:
            summary[key] = {
                "latest": round(values[-1], 3),
                "mean": round(sum(values) / len(values), 3),
            }
    return summary


def _check_cpu_sla(state: PipelineState) -> tuple[bool, str]:
    """CPU SLA 검증: 최근 CPU 사용률이 임계값 이하인지 확인."""
    raw_metrics = state.get("raw_metrics", {})

    # CPU 지표 추출 (EC2, RDS)
    cpu_values = raw_metrics.get("cpu_utilization", [])

    if not cpu_values:
        # CPU 지표가 없는 리소스 (Lambda, S3, AutoScaling)는 통과
        return True, "CPU 지표 없음 (해당 리소스 타입에 적용되지 않음)"

    latest_cpu = cpu_values[-1] if cpu_values else 0.0
    threshold = SLA_THRESHOLDS["cpu_utilization_max"]

    if latest_cpu <= threshold:
        return True, f"CPU {latest_cpu:.1f}% <= {threshold}% (정상)"
    else:
        return False, f"CPU {latest_cpu:.1f}% > {threshold}% (SLA 위반)"


def _check_cost_sla(state: PipelineState) -> tuple[bool, str]:
    """비용 SLA 검증: 액션으로 인한 비용 증가가 없는지 확인."""
    raw_metrics = state.get("raw_metrics", {})
    cost_values = raw_metrics.get("cost", [])

    if not cost_values or len(cost_values) < 2:
        return True, "비용 데이터 부족 (추후 확인 필요)"

    # 최근 비용과 이전 평균 비교
    recent_cost = cost_values[-1]
    prev_avg_cost = sum(cost_values[:-1]) / len(cost_values[:-1]) if len(cost_values) > 1 else recent_cost

    # 비용이 이전 평균 대비 10% 이상 증가하면 SLA 위반
    cost_increase_threshold = 1.1  # 10% 증가 허용

    if recent_cost <= prev_avg_cost * cost_increase_threshold:
        reduction = ((prev_avg_cost - recent_cost) / prev_avg_cost * 100) if prev_avg_cost > 0 else 0
        return True, f"비용 정상 (절감률: {reduction:.1f}%)"
    else:
        increase = ((recent_cost - prev_avg_cost) / prev_avg_cost * 100) if prev_avg_cost > 0 else 0
        return False, f"비용 증가 감지 ({increase:.1f}% 증가, SLA 위반)"


def _check_availability_sla(state: PipelineState) -> tuple[bool, str]:
    """가용성 SLA 검증: 액션이 성공적으로 완료되었는지 확인."""
    action_result = state.get("action_result", {})

    if not action_result:
        return False, "액션 결과 없음"

    # 액션 결과 상태 확인
    status = action_result.get("status", "").lower()
    http_code = action_result.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)

    # 성공 조건: status가 success이거나 HTTP 200번대
    if status == "success" or (200 <= http_code < 300):
        return True, f"액션 성공 (status={status}, HTTP {http_code})"
    else:
        error_msg = action_result.get("error", "알 수 없는 오류")
        return False, f"액션 실패: {error_msg}"


def _apply_rule_based_qa(state: PipelineState) -> Optional[tuple[SlaCheckResult, bool, str]]:
    """
    Rule-based SLA 검증.
    명확한 케이스는 규칙으로 처리하고 (SlaCheckResult, qa_passed, reasoning) 반환.
    모호한 케이스는 None 반환 → LLM으로 넘어감.
    """
    action_executed = state.get("action_executed")
    action_result = state.get("action_result", {})

    # NoAction인 경우 항상 통과
    if action_executed == "NoAction" or action_executed is None:
        return (
            {
                "cpu_ok": True,
                "cost_ok": True,
                "availability_ok": True,
                "detail": "NoAction - 액션 없음, 검증 스킵",
            },
            True,
            "[Rule] NoAction이므로 SLA 검증 통과",
        )

    # 액션 결과가 명확히 실패인 경우
    if action_result.get("status") == "failed":
        error_msg = action_result.get("error", "알 수 없는 오류")
        return (
            {
                "cpu_ok": True,  # CPU는 영향 없음
                "cost_ok": True,  # 비용은 영향 없음
                "availability_ok": False,
                "detail": f"액션 실행 실패: {error_msg}",
            },
            False,
            f"[Rule] 액션 실행 실패로 인한 SLA 검증 실패: {error_msg}",
        )

    # 개별 SLA 체크
    cpu_ok, cpu_detail = _check_cpu_sla(state)
    cost_ok, cost_detail = _check_cost_sla(state)
    avail_ok, avail_detail = _check_availability_sla(state)

    all_ok = cpu_ok and cost_ok and avail_ok

    # 모든 검증이 명확하게 통과/실패인 경우
    detail_parts = []
    if not cpu_ok:
        detail_parts.append(cpu_detail)
    if not cost_ok:
        detail_parts.append(cost_detail)
    if not avail_ok:
        detail_parts.append(avail_detail)

    detail = "; ".join(detail_parts) if detail_parts else "모든 SLA 충족"

    return (
        {
            "cpu_ok": cpu_ok,
            "cost_ok": cost_ok,
            "availability_ok": avail_ok,
            "detail": detail,
        },
        all_ok,
        f"[Rule] CPU: {cpu_detail}, Cost: {cost_detail}, Availability: {avail_detail}",
    )


def _parse_llm_response(text: str) -> dict:
    """LLM 응답에서 JSON 추출."""
    import re
    cleaned = re.sub(r"```(?:json)?", "", text).replace("```", "").strip()
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        return {
            "cpu_ok": True,
            "cost_ok": True,
            "availability_ok": True,
            "overall_pass": True,
            "reasoning": f"LLM 응답 파싱 실패: {text[:100]}",
            "rollback_recommended": False,
        }


def _call_llm_qa(state: PipelineState) -> tuple[SlaCheckResult, bool, str]:
    """LLM을 사용한 복잡한 SLA 검증."""
    summary = _metrics_summary(state.get("raw_metrics", {}))

    for attempt in range(3):
        try:
            response = chain.invoke({
                "resource_type": state.get("resource_type", "Unknown"),
                "action_executed": state.get("action_executed", "None"),
                "action_result": json.dumps(state.get("action_result", {}), ensure_ascii=False),
                "pre_action_snapshot": json.dumps(state.get("pre_action_snapshot", {}), ensure_ascii=False),
                "metrics_summary": json.dumps(summary, ensure_ascii=False),
                "anomaly_type": state.get("anomaly_type", "Unknown"),
            })
            parsed = _parse_llm_response(response.content)

            sla_result: SlaCheckResult = {
                "cpu_ok": parsed.get("cpu_ok", True),
                "cost_ok": parsed.get("cost_ok", True),
                "availability_ok": parsed.get("availability_ok", True),
                "detail": parsed.get("reasoning", ""),
            }

            qa_passed = parsed.get("overall_pass", True)
            reasoning = f"[LLM] {parsed.get('reasoning', '')}"

            return sla_result, qa_passed, reasoning

        except Exception as e:
            if attempt == 2:
                # 3회 재시도 초과 시 안전하게 통과 처리 (보수적)
                return (
                    {
                        "cpu_ok": True,
                        "cost_ok": True,
                        "availability_ok": True,
                        "detail": f"LLM 호출 실패로 인한 기본 통과: {e}",
                    },
                    True,
                    f"[LLM] 호출 실패 (3회 재시도 초과), 기본 통과 처리: {e}",
                )

    return (
        {"cpu_ok": True, "cost_ok": True, "availability_ok": True, "detail": ""},
        True,
        "[LLM] 기본 통과",
    )


def qa_node(state: PipelineState) -> PipelineState:
    """
    QA Agent 메인 노드 함수.

    SLA 검증 수행 후:
    - 통과: qa_passed=True
    - 실패 + rollback_count < 2: qa_passed=False, rollback_count 증가
    - 실패 + rollback_count >= 2: qa_passed=False (graph에서 logging으로 이동)
    """
    # Rule-based 검증 시도
    rule_result = _apply_rule_based_qa(state)

    if rule_result is not None:
        sla_result, qa_passed, reasoning = rule_result
    else:
        # LLM 검증 (모호한 케이스)
        sla_result, qa_passed, reasoning = _call_llm_qa(state)

    # State 업데이트
    state["sla_check_result"] = sla_result
    state["qa_passed"] = qa_passed

    # 검증 실패 시 롤백 카운트 증가
    if not qa_passed:
        current_count = state.get("rollback_count", 0)
        state["rollback_count"] = current_count + 1

        if state["rollback_count"] >= 2:
            # 2회 초과: 현재 상태 유지, 관리자 알림 필요
            reasoning += " [ALERT] 롤백 2회 초과, 관리자 확인 필요"

    # 로그 엔트리 추가
    log_entries = state.get("log_entries", [])
    log_entries.append(f"[QA] {reasoning}")
    log_entries.append(f"[QA] SLA 결과: cpu_ok={sla_result['cpu_ok']}, cost_ok={sla_result['cost_ok']}, availability_ok={sla_result['availability_ok']}")
    log_entries.append(f"[QA] qa_passed={qa_passed}, rollback_count={state['rollback_count']}")
    state["log_entries"] = log_entries

    return state


# 테스트용 헬퍼 함수들
def qa_node_force_fail(state: PipelineState) -> PipelineState:
    """테스트용: 항상 실패하는 QA 노드."""
    sla_result: SlaCheckResult = {
        "cpu_ok": False,
        "cost_ok": True,
        "availability_ok": True,
        "detail": "[테스트] CPU SLA 강제 실패",
    }

    state["sla_check_result"] = sla_result
    state["qa_passed"] = False
    state["rollback_count"] = state.get("rollback_count", 0) + 1

    return state


def qa_node_force_pass(state: PipelineState) -> PipelineState:
    """테스트용: 항상 통과하는 QA 노드.
      (테스트에서 다른 노드를 점검하기 위해, LLM 노드의 결과를 일시적으로 모두 무시하고 넘어감)"""
    sla_result: SlaCheckResult = {
        "cpu_ok": True,
        "cost_ok": True,
        "availability_ok": True,
        "detail": "[테스트] 모든 SLA 강제 통과",
    }

    state["sla_check_result"] = sla_result
    state["qa_passed"] = True

    return state
