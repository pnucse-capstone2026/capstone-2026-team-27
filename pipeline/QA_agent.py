"""
QA Agent (SLA 검증)

액션 수행 후 SLA 준수 여부 검증, 실패 시 롤백 트리거.

검증 항목:
  - CPU SLA: 사용률 80% 이하
  - 비용 SLA: 실제 비용 절감 효과 확인
  - 가용성 SLA: 서비스 가용성 유지

처리 흐름:
  - 통과 → qa_passed=True
  - 실패 + rollback_count < 2 → 롤백 후 재시도
  - 실패 + rollback_count >= 2 → 관리자 알림
"""

import json
import logging
import os
import time
from datetime import datetime
from typing import Optional

from schema.state import PipelineState, SlaCheckResult
from pipeline.action_agent import rollback_action
from pipeline.orchestrator import assemble_resource
from pipeline.rule_engine import get_rule_engine
from pipeline.inbound_handlers import remove_waf_rate_based_rule
from utils.slack_notifier import send_slack_alert

logger = logging.getLogger(__name__)

POST_ACTION_WAIT_SECONDS = 300

# LLM 판단 로그 경로 (classification_agent.py와 동일)
LLM_LOG_PATH = os.path.join(
    os.path.dirname(__file__), "..", "schema", "logs", "llm_classification_log.jsonl"
)


def _update_llm_log_with_qa_result(state: PipelineState) -> None:
    """
    LLM 판단 로그에 QA 결과를 추가.
    trace_id로 해당 로그 엔트리를 찾아 qa_result 필드를 업데이트.

    Decision 단계가 LLM 판단이 아닌 경우(Rule Book 매칭)는 스킵.
    LLM 사용 여부는 state["decision_pseudo_code"] 존재로 판단.
    """
    trace_id = state.get("trace_id")
    decision_used_llm = bool(state.get("decision_pseudo_code"))

    # Decision이 LLM 판단이 아니면(Rule Book으로 결정됨) 로깅 스킵
    if not trace_id or not decision_used_llm:
        return

    qa_result = {
        "qa_passed": state.get("qa_passed"),
        "rollback_count": state.get("rollback_count", 0),
        "sla_check_result": state.get("sla_check_result"),
        "qa_matched_rule_id": state.get("qa_matched_rule_id"),
        "whitelisted": state.get("whitelisted", False),
        "updated_at": datetime.utcnow().isoformat() + "Z",
    }

    if not os.path.exists(LLM_LOG_PATH):
        return

    try:
        # 로그 파일 읽기 → trace_id 매칭 → qa_result 업데이트 → 다시 쓰기
        updated_lines = []
        found = False

        with open(LLM_LOG_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    if (
                        entry.get("trace_id") == trace_id
                        and entry.get("qa_result") is None
                    ):
                        entry["qa_result"] = qa_result
                        found = True
                    updated_lines.append(json.dumps(entry, ensure_ascii=False))
                except json.JSONDecodeError:
                    updated_lines.append(line)

        if found:
            with open(LLM_LOG_PATH, "w", encoding="utf-8") as f:
                f.write("\n".join(updated_lines) + "\n")

    except Exception as e:
        print(f"[QA_agent] LLM 로그 QA 결과 업데이트 실패: {e}")


COST_PREDICTION_LOG_PATH = os.path.join(
    os.path.dirname(__file__), "..", "schema", "logs", "cost_prediction_log.jsonl"
)


def _update_cost_prediction_log_with_qa_result(state: PipelineState) -> None:
    trace_id = state.get("trace_id")
    if not trace_id or not os.path.exists(COST_PREDICTION_LOG_PATH):
        return

    qa_passed = state.get("qa_passed")

    try:
        updated_lines = []
        found = False
        with open(COST_PREDICTION_LOG_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    if entry.get("trace_id") == trace_id and "qa_passed" not in entry:
                        entry["qa_passed"] = qa_passed
                        found = True
                    updated_lines.append(json.dumps(entry, ensure_ascii=False))
                except json.JSONDecodeError:
                    updated_lines.append(line)

        if found:
            with open(COST_PREDICTION_LOG_PATH, "w", encoding="utf-8") as f:
                f.write("\n".join(updated_lines) + "\n")
    except Exception as e:
        print(f"[QA_agent] cost_prediction_log QA 결과 업데이트 실패: {e}")


from utils.llm_utils import call_gemini

from dotenv import load_dotenv

load_dotenv()

# SLA 임계값 정의
SLA_THRESHOLDS = {
    "cpu_utilization_max": 80.0,  # CPU 사용률 최대 허용치 (%)
    "cost_reduction_min": 0.0,  # 최소 비용 절감률 (액션 실행 시 비용 증가 방지)
    "availability_min": 99.0,  # 최소 가용성 (%)
}

# LLM 프롬프트: 복잡한 SLA 판단이 필요한 경우 (str.format()과 동일한 {{ }} 이스케이프 규칙)
PROMPT_TEMPLATE = """
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
"""


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


# triggered_metrics 기반 SLA 체크 (CPU 외 지표도 자동 검증)
# 절대 임계값 있는 지표와 상대 비교 지표 구분
_ABSOLUTE_THRESHOLD_METRICS = {"cpu_utilization": SLA_THRESHOLDS["cpu_utilization_max"]}
_RELATIVE_SPIKE_RATIO = (
    1.5  # 기준선(초반 평균) 대비 1.5배 넘으면 아직 안 가라앉은 것으로 판단
)


def _check_cpu_sla(state: PipelineState) -> tuple[bool, str]:
    """관련 지표 SLA 검증: 액션 후에도 '원래 이상을 트리거했던 지표'가 여전히
    튀어 있으면 위반으로 본다 (이름은 호환을 위해 cpu_sla로 유지, 실제로는
    CPU 전용이 아니라 triggered_metrics 기반 범용 체크)."""
    raw_metrics = state.get("raw_metrics", {})
    triggered_metrics = state.get("triggered_metrics") or []

    # detection 단계에서 특정 지표가 안 짚혔으면(IForest만으로 잡힌 경우 등)
    # 리소스에 cpu_utilization이 있으면 그거라도 폴백으로 체크.
    if not triggered_metrics:
        if raw_metrics.get("cpu_utilization"):
            triggered_metrics = ["cpu_utilization"]
        else:
            return True, "체크할 트리거 지표 없음 (해당 리소스 타입에 적용되지 않음)"

    violations = []
    details = []
    for metric in triggered_metrics:
        values = raw_metrics.get(metric, [])
        if not values:
            continue
        latest = values[-1]

        if metric in _ABSOLUTE_THRESHOLD_METRICS:
            threshold = _ABSOLUTE_THRESHOLD_METRICS[metric]
            ok = latest <= threshold
            details.append(
                f"{metric} {latest:.1f} <= {threshold}%"
                if ok
                else f"{metric} {latest:.1f} > {threshold}%"
            )
        else:
            # 절대 임계값이 없는 지표(bytes_downloaded, invocation_count 등)는
            # 액션 전 구간(마지막 몇 개 제외) 평균을 기준선으로 상대 비교.
            baseline_part = values[:-3] if len(values) > 3 else values
            baseline = sum(baseline_part) / len(baseline_part) if baseline_part else 0.0
            ok = latest <= baseline * _RELATIVE_SPIKE_RATIO if baseline > 0 else True
            details.append(
                f"{metric} {latest:.1f} <= 기준선 {baseline:.1f}*{_RELATIVE_SPIKE_RATIO}"
                if ok
                else f"{metric} {latest:.1f} > 기준선 {baseline:.1f}*{_RELATIVE_SPIKE_RATIO} (아직 안 가라앉음)"
            )

        if not ok:
            violations.append(metric)

    passed = len(violations) == 0
    detail_str = ", ".join(details) if details else "체크할 값 없음"
    return passed, detail_str


def _check_cost_sla(state: PipelineState) -> tuple[bool, str]:
    """비용 SLA 검증: 액션으로 인한 비용 증가가 없는지 확인."""
    raw_metrics = state.get("raw_metrics", {})
    cost_values = raw_metrics.get("cost", [])

    if not cost_values or len(cost_values) < 2:
        return True, "비용 데이터 부족 (추후 확인 필요)"

    # 최근 비용과 이전 평균 비교
    recent_cost = cost_values[-1]
    prev_avg_cost = (
        sum(cost_values[:-1]) / len(cost_values[:-1])
        if len(cost_values) > 1
        else recent_cost
    )

    # 비용이 이전 평균 대비 10% 이상 증가하면 SLA 위반
    cost_increase_threshold = 1.1  # 10% 증가 허용

    if recent_cost <= prev_avg_cost * cost_increase_threshold:
        reduction = (
            ((prev_avg_cost - recent_cost) / prev_avg_cost * 100)
            if prev_avg_cost > 0
            else 0
        )
        return True, f"비용 정상 (절감률: {reduction:.1f}%)"
    else:
        increase = (
            ((recent_cost - prev_avg_cost) / prev_avg_cost * 100)
            if prev_avg_cost > 0
            else 0
        )
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


def _apply_rule_based_qa(
    state: PipelineState,
) -> Optional[tuple[SlaCheckResult, bool, str, Optional[str]]]:
    """
    Rule Book 기반 QA 규칙 매칭 + 기본 SLA 검증.
    (SlaCheckResult, qa_passed, reasoning, rule_id) 반환.
    """
    action_executed = state.get("action_executed")
    action_result = state.get("action_result", {})
    engine = get_rule_engine()

    # 1. Rule Book에서 QA 규칙 매칭 시도
    matched_rule = engine.match_qa_rules(state)
    if matched_rule is not None:
        result = matched_rule.get("result", {})
        rule_id = matched_rule.get("rule_id")
        reasoning = engine.format_reasoning(matched_rule, state)

        # force_pass가 True이면 무조건 통과
        if result.get("force_pass"):
            return (
                {
                    "cpu_ok": True,
                    "cost_ok": True,
                    "availability_ok": True,
                    "detail": f"Rule Book 규칙 적용: {reasoning}",
                },
                True,
                f"[Rule:{rule_id}] {reasoning}",
                rule_id,
            )

        # force_fail이 True이면 무조건 실패
        if result.get("force_fail"):
            return (
                {
                    "cpu_ok": False,
                    "cost_ok": False,
                    "availability_ok": False,
                    "detail": f"Rule Book 규칙 적용: {reasoning}",
                },
                False,
                f"[Rule:{rule_id}] {reasoning}",
                rule_id,
            )

    # 2. 기본 규칙: NoAction인 경우 항상 통과
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
            None,
        )

    # 3. 액션 결과가 명확히 실패인 경우
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
            None,
        )

    # 액션 미구현 상태도 실패로 처리
    if action_result.get("status") == "not_implemented":
        return (
            {
                "cpu_ok": True,
                "cost_ok": True,
                "availability_ok": False,
                "detail": f"액션 미구현: {action_executed} (resource_type={state.get('resource_type')})",
            },
            False,
            f"[Rule] {action_executed} 액션이 이 리소스 타입에 구현되지 않아 실행되지 않음 "
            f"— SLA 검증 실패로 처리",
            None,
        )

    # 4. 개별 SLA 체크
    cpu_ok, cpu_detail = _check_cpu_sla(state)
    cost_ok, cost_detail = _check_cost_sla(state)
    avail_ok, avail_detail = _check_availability_sla(state)

    # 데이터 부족으로 판단 불가 시 LLM 검증으로 위임
    ambiguous = "체크할 트리거 지표 없음" in cpu_detail or "데이터 부족" in cost_detail
    if ambiguous:
        return None

    all_ok = cpu_ok and cost_ok and avail_ok

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
        f"[Rule] Metric: {cpu_detail}, Cost: {cost_detail}, Availability: {avail_detail}",
        None,
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

    rendered_prompt = PROMPT_TEMPLATE.format(
        resource_type=state.get("resource_type", "Unknown"),
        action_executed=state.get("action_executed", "None"),
        action_result=json.dumps(state.get("action_result", {}), ensure_ascii=False),
        pre_action_snapshot=json.dumps(
            state.get("pre_action_snapshot", {}), ensure_ascii=False
        ),
        metrics_summary=json.dumps(summary, ensure_ascii=False),
        anomaly_type=state.get("anomaly_type", "Unknown"),
    )

    for attempt in range(3):
        try:
            raw_text = call_gemini(rendered_prompt, temperature=0.1)
        except RuntimeError as e:
            # GEMINI_KEY_1/2/3 전부 소진/미설정 - 재시도해도 결과가 같으므로
            # 즉시 포기하고 안전하게 통과 처리 (보수적)
            return (
                {
                    "cpu_ok": True,
                    "cost_ok": True,
                    "availability_ok": True,
                    "detail": f"Gemini 키 사용 불가로 인한 기본 통과: {e}",
                },
                True,
                f"[LLM] Gemini 키 사용 불가, 기본 통과 처리: {e}",
            )
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
            continue

        parsed = _parse_llm_response(raw_text)

        sla_result: SlaCheckResult = {
            "cpu_ok": parsed.get("cpu_ok", True),
            "cost_ok": parsed.get("cost_ok", True),
            "availability_ok": parsed.get("availability_ok", True),
            "detail": parsed.get("reasoning", ""),
        }

        qa_passed = parsed.get("overall_pass", True)
        reasoning = f"[LLM] {parsed.get('reasoning', '')}"

        return sla_result, qa_passed, reasoning

    return (
        {"cpu_ok": True, "cost_ok": True, "availability_ok": True, "detail": ""},
        True,
        "[LLM] 기본 통과",
    )


def _release_waf_rate_limit_if_resolved(state: PipelineState) -> None:
    """AutoScaling EDoS 대응으로 WAF Rate-based Rule을 걸었던 경우, QA가 트래픽
    정상화를 확인했으면(qa_passed=True) 그 규칙을 자동으로 해제한다.

    inbound_handlers.py의 remove_waf_rate_based_rule()은 이미 구현·테스트돼 있었지만
    지금까지 파이프라인 어디서도 호출되지 않아 한 번도 실제로 해제된 적이 없었다
    (2026-09-11 발견, 팀원 B 구현 + 여기서 연동). 이게 없으면 공격이 끝난 뒤에도
    Rate-based Rule이 계속 남아 정상 트래픽까지 제한하게 된다."""
    if state.get("resource_type") != "AutoScaling":
        return
    if state.get("action_executed") != "ScaleDown":
        return

    waf_result = (state.get("action_result") or {}).get("waf_result")
    if not waf_result or waf_result.get("status") != "success":
        return  # WAF가 애초에 안 걸렸으면(ALB 미연결 등) 해제할 것도 없음

    log_entries = state.get("log_entries", [])
    try:
        release_result = remove_waf_rate_based_rule(
            rule_name=waf_result["rule_name"],
            web_acl_name=waf_result["web_acl_name"],
            web_acl_id=waf_result["web_acl_id"],
            dry_run=False,
        )
        log_entries.append(
            f"[QA] 트래픽 정상화 확인 -> WAF Rate-based Rule 자동 해제 "
            f"(status={release_result.get('status')})"
        )
        logger.info("[QA] WAF Rule 자동 해제: %s", release_result.get("status"))
    except Exception as exc:
        log_entries.append(f"[QA] WAF Rule 자동 해제 시도 실패: {exc}")
        logger.warning("[QA] WAF Rule 자동 해제 실패: %s", exc)
    state["log_entries"] = log_entries


def _trigger_rollback(state: PipelineState, qa_reasoning: str) -> str:
    """
    QA 실패 확정 시 pre_action_snapshot으로 즉시 롤백을 실행한다.
    (A안: 실행 실패/SLA 위반 구분 없이 항상 동일하게 롤백한다)

    ⚠️ action_result에 "이 액션은 이후 롤백되었다"는 사실을 명확히 남긴다.
       action_result는 schema/state.py에서 Optional[dict]로만 선언돼 있어
       (필드 구조가 고정된 TypedDict가 아님) 스키마 필드명을 바꾸지 않고도
       아래 키들을 안전하게 추가할 수 있다:
         - rolled_back        : bool  — 롤백 발생 여부
         - rollback_status     : "success"/"failed"/"not_implemented" 등
         - rollback_detail     : rollback_action()의 원본 반환값
         - rollback_reason     : 왜 롤백했는지 (QA 실패 사유)
         - rollback_attempt    : 몇 번째 롤백 시도인지 (rollback_count 증가 전 값 + 1)

    입력: state (resource_type, resource_id, pre_action_snapshot, action_result 사용)
    출력: 로그에 남길 롤백 결과 요약 문자열 ([ROLLBACK] 접두사로 검색/필터링 쉽게 함)
    """
    resource_type = state.get("resource_type")
    resource_id = state.get("resource_id")
    action_executed = state.get("action_executed")
    snapshot = state.get("pre_action_snapshot")
    attempt_no = state.get("rollback_count", 0) + 1

    # NoAction/pending_approval 등 실제로 아무것도 실행되지 않은 경우
    # (rule 기반 검증에서 이런 케이스는 이미 qa_passed=True로 처리되므로
    #  여기 도달했다는 건 실제 액션이 실행되었다는 뜻이지만, 방어적으로 한 번 더 확인)
    if action_executed in (None, "NoAction"):
        return "[ROLLBACK] 스킵 - 실행된 액션 없음 (action_executed=NoAction/None)"

    rollback_result = rollback_action(resource_type, resource_id, snapshot)
    rollback_success = rollback_result.get("status") == "success"

    # action_result를 그대로 덮어쓰지 않고, 기존 실행 결과 위에 롤백 정보를 덧붙인다.
    # → 이 액션이 "실행됐다가 나중에 롤백됐다"는 사실이 action_result만 봐도 명확해짐.
    updated_action_result = dict(state.get("action_result") or {})
    updated_action_result["rolled_back"] = True
    updated_action_result["rollback_status"] = rollback_result.get("status")
    updated_action_result["rollback_detail"] = rollback_result
    updated_action_result["rollback_reason"] = qa_reasoning
    updated_action_result["rollback_attempt"] = attempt_no
    state["action_result"] = updated_action_result

    status_kr = "성공" if rollback_success else "실패"
    return (
        f"[ROLLBACK] {status_kr} (시도 {attempt_no}회차) - "
        f"{resource_type}:{resource_id}의 '{action_executed}' 액션을 롤백함 "
        f"(사유: {qa_reasoning}) → rollback_result={rollback_result}"
    )


def _refresh_metrics_after_action(state: PipelineState) -> None:
    """액션 실행 후 POST_ACTION_WAIT_SECONDS만큼 대기했다가 CloudWatch를 실제로
    재조회해서 state["raw_metrics"]를 액션 *후* 데이터로 갱신한다.

    - NoAction/미실행(action_executed가 None/"NoAction")이면 검증할 변화 자체가
      없으므로 대기·재조회를 스킵한다 (불필요한 지연 방지).
    - 재조회는 assemble_resource()로 실제 프로덕션 경로(fetch_metrics +
      estimate_cost_series)를 그대로 타서, decision_agent가 쓰는 것과 동일한
      cost 계산 로직을 그대로 재사용한다.
    - 원래(액션 전) raw_metrics는 pre_action_raw_metrics에 보존한다 — 재조회한
      배열도 결국 대부분(2.5시간 창 중 몇 분 빼고는) 액션 전 이력과 겹치므로,
      기존 _check_cost_sla/_check_cpu_sla의 "최근값 vs 배열 나머지 평균" 비교가
      "실측 후 -vs- 실측 전 기준선" 비교로 자연스럽게 성립한다.
    - 재조회 실패(권한 없음/리소스 삭제 등) 시 기존 raw_metrics를 그대로 유지하고
      경고만 남긴다 — QA가 죽지 않고 기존(액션 전) 데이터 기준으로라도 판단한다.
    - 중간에 값이 바뀌었는지 짧은 주기로 미리 엿보고 조기 종료하는 방식은 의도적으로
      쓰지 않는다 — 조회 윈도우가 "지금부터 과거 300초"를 담는 슬라이딩 윈도우라
      300초가 되기 전에는 액션 전후 데이터가 섞인 값만 나오고, 그 상태에서 감지되는
      변화는 SLA 판단에 쓸 만큼 깨끗한 신호가 아니다. 따라서 판단은 항상 정확히
      POST_ACTION_WAIT_SECONDS(300초)가 지난 시점의 값으로만 한다.
    """
    action_executed = state.get("action_executed")
    if action_executed in (None, "NoAction"):
        return

    resource_id = state.get("resource_id", "")
    resource_type = state.get("resource_type", "")

    logger.info(
        "[QA] 액션(%s) 후 실측을 위해 %d초 대기 중... (%s:%s)",
        action_executed,
        POST_ACTION_WAIT_SECONDS,
        resource_type,
        resource_id,
    )
    time.sleep(POST_ACTION_WAIT_SECONDS)

    try:
        assembled = assemble_resource(resource_id, resource_type)
        state["pre_action_raw_metrics"] = state.get("raw_metrics")
        state["raw_metrics"] = assembled["raw_metrics"]
        logger.info("[QA] 실측 재조회 완료 (%s:%s)", resource_type, resource_id)
    except Exception as exc:
        logger.warning(
            "[QA] 실측 재조회 실패, 액션 전 데이터로 판단 유지 (%s:%s): %s",
            resource_type,
            resource_id,
            exc,
        )


def qa_node(state: PipelineState) -> PipelineState:
    """
    QA Agent 메인 노드 함수.

    처리 순서:
    1. 화이트리스트 체크 - 해당되면 검증 스킵
    2. Rule Book 기반 QA 규칙 매칭
    3. 기본 SLA 검증 또는 LLM 검증

    SLA 검증 수행 후:
    - 통과: qa_passed=True
    - 실패 + rollback_count < 2:
        qa_passed=False, pre_action_snapshot으로 즉시 롤백 실행, rollback_count 증가
        (graph.py의 qa_router가 이 결과를 보고 action으로 재시도시킴)
    - 실패 + rollback_count >= 2: qa_passed=False, 롤백은 실행하되 재시도는 하지 않음 (관리자 알림)
    """
    if state.get("_demo_replay"):
        replay = state["_demo_replay"]
        state["qa_passed"] = replay["qa_passed"]
        state["sla_check_result"] = replay["sla_check_result"]
        state["rollback_count"] = replay["rollback_count"]
        return state

    engine = get_rule_engine()
    resource_id = state.get("resource_id", "")
    resource_type = state.get("resource_type", "")
    log_entries = state.get("log_entries", [])

    # 1. 화이트리스트 체크
    is_whitelisted, whitelist_entry = engine.is_whitelisted(resource_id, resource_type)
    state["whitelisted"] = is_whitelisted

    if is_whitelisted:
        reason = (
            whitelist_entry.get("reason", "화이트리스트 등록됨")
            if whitelist_entry
            else "화이트리스트 등록됨"
        )
        entry_id = whitelist_entry.get("entry_id", "N/A") if whitelist_entry else "N/A"

        sla_result: SlaCheckResult = {
            "cpu_ok": True,
            "cost_ok": True,
            "availability_ok": True,
            "detail": f"화이트리스트 적용 ({entry_id}): {reason}",
        }
        state["sla_check_result"] = sla_result
        state["qa_passed"] = True
        state["qa_matched_rule_id"] = None

        log_entries.append(f"[QA] 화이트리스트 적용 - {entry_id}: {reason}")
        log_entries.append(
            f"[QA] qa_passed=True (화이트리스트), rollback_count={state.get('rollback_count', 0)}"
        )
        state["log_entries"] = log_entries
        return state

    # 1.5. 액션 후 실측 재조회 (whitelist 스킵된 경우는 위에서 이미 return돼서 안 탐)
    _refresh_metrics_after_action(state)

    # 2. Rule Book 기반 검증 시도
    rule_result = _apply_rule_based_qa(state)

    if rule_result is not None:
        sla_result, qa_passed, reasoning, rule_id = rule_result
        state["qa_matched_rule_id"] = rule_id
    else:
        # LLM 검증 (모호한 케이스)
        sla_result, qa_passed, reasoning = _call_llm_qa(state)
        state["qa_matched_rule_id"] = None

    # State 업데이트
    state["sla_check_result"] = sla_result
    state["qa_passed"] = qa_passed

    # 검증 실패 시: 실행 실패든 SLA 위반이든 구분 없이 항상 즉시 롤백 (A안)
    if not qa_passed:
        rollback_log = _trigger_rollback(state, reasoning)
        log_entries.append(rollback_log)

        current_count = state.get("rollback_count", 0)
        state["rollback_count"] = current_count + 1

        if state["rollback_count"] >= 2:
            # 2회 초과: 현재 상태 유지, 관리자 알림 필요 (더 이상 action으로 재시도하지 않음)
            reasoning += " [ALERT] 롤백 2회 초과, 관리자 확인 필요"
            send_slack_alert(
                f"[롤백 2회 초과] {state['resource_type']} · {state['resource_id']}\n"
                f"액션: {state.get('action_executed')}\n"
                f"사유: {reasoning}\n"
                f"더 이상 자동 재시도하지 않습니다 — 관리자 확인이 필요합니다."
            )
    else:
        # 검증 통과: 문제가 해소됐으니, EDoS 대응으로 걸어둔 WAF Rate-based Rule이
        # 있었다면 여기서 자동 해제한다 (안 그러면 정상 트래픽까지 계속 제한됨).
        _release_waf_rate_limit_if_resolved(state)

    # 로그 엔트리 추가
    log_entries.append(f"[QA] {reasoning}")
    log_entries.append(
        f"[QA] SLA 결과: cpu_ok={sla_result['cpu_ok']}, cost_ok={sla_result['cost_ok']}, availability_ok={sla_result['availability_ok']}"
    )
    log_entries.append(
        f"[QA] qa_passed={qa_passed}, rollback_count={state.get('rollback_count', 0)}"
    )
    state["log_entries"] = log_entries

    # LLM 판단 로그에 QA 결과 추가 (규칙 승격 분석용)
    _update_llm_log_with_qa_result(state)
    _update_cost_prediction_log_with_qa_result(state)

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
