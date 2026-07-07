"""
더미 노드 모음 — 실제 로직 없이 State 필드만 채워서 파이프라인 흐름 검증용.
각 노드는 자신이 담당하는 필드만 채운다 (node_contracts.md 기준).
"""
from schema.state import PipelineState

from schema.state import (
    PipelineState,
    CandidateAction,
    SlaCheckResult,
    EC2Snapshot,
    resolve_risk_level,
)


def detection_node(state: PipelineState) -> PipelineState:
    state["anomaly_flag"]          = True
    state["anomaly_score_zscore"]  = 3.5
    state["anomaly_score_iforest"] = 0.7
    state["triggered_metrics"]     = ["cpu_utilization", "cost"]
    return state


def classification_node(state: PipelineState) -> PipelineState:
    state["anomaly_type"]              = "cost_spike"
    state["classification_reasoning"]  = "더미: cpu_utilization + cost 동시 급등 → 트래픽 폭증으로 판단"
    state["interim_action_taken"]      = None
    return state


def decision_node(state: PipelineState) -> PipelineState:
    candidates: list[CandidateAction] = [
        {
            "action":          "ScaleDown",
            "saving_rate":     0.6,
            "impact_score":    0.3,
            "stability_score": 0.7,
            "score":           0.6 * 0.5 - 0.3 * 0.3 + 0.7 * 0.2,  # 0.35
        },
        {
            "action":          "Throttle",
            "saving_rate":     0.4,
            "impact_score":    0.2,
            "stability_score": 0.8,
            "score":           0.4 * 0.5 - 0.2 * 0.3 + 0.8 * 0.2,  # 0.30
        },
    ]
    selected = max(candidates, key=lambda c: c["score"])

    risk = resolve_risk_level(
        anomaly_type=state["anomaly_type"],
        selected_action=selected["action"],
    )

    state["candidate_actions"]  = candidates
    state["selected_action"]    = selected["action"]
    state["risk_level"]         = risk
    state["requires_approval"]  = risk in ("MED", "HIGH")
    state["decision_reasoning"] = f"더미: 후보 {len(candidates)}개 중 score 최고 → {selected['action']} 선택 (risk={risk})"
    return state


def action_node(state: PipelineState) -> PipelineState:
    snapshot: EC2Snapshot = {
        "instance_type":      "t3.medium",
        "state":              "running",
        "security_group_ids": ["sg-0abc1234"],
    }
    state["pre_action_snapshot"] = snapshot
    state["action_executed"]     = state["selected_action"]
    state["action_result"]       = {"status": "success", "ResponseMetadata": {"HTTPStatusCode": 200}}
    return state


def qa_node(state: PipelineState) -> PipelineState:
    sla: SlaCheckResult = {
        "cpu_ok":          True,
        "cost_ok":         True,
        "availability_ok": True,
        "detail":          "",
    }
    state["qa_passed"]        = True
    state["sla_check_result"] = sla
    return state


def qa_node_fail(state: PipelineState) -> PipelineState:
    """롤백 루프 검증용 — qa_passed=False 강제."""
    sla: SlaCheckResult = {
        "cpu_ok":          False,
        "cost_ok":         True,
        "availability_ok": True,
        "detail":          "더미: CPU SLA 미달 (임계값 80% 초과)",
    }
    state["qa_passed"]        = False
    state["sla_check_result"] = sla
    state["rollback_count"]   = state["rollback_count"] + 1
    return state


def logging_node(state: PipelineState) -> PipelineState:
    entries = state.get("log_entries", [])
    entries.append(f"[detection]       anomaly_flag={state['anomaly_flag']}, triggered={state['triggered_metrics']}")
    entries.append(f"[classification]  anomaly_type={state['anomaly_type']}, interim={state['interim_action_taken']}")
    entries.append(f"[decision]        selected={state['selected_action']}, risk={state['risk_level']}, approval={state['requires_approval']}")
    entries.append(f"[action]          executed={state['action_executed']}, result={state['action_result']}")
    entries.append(f"[qa]              passed={state['qa_passed']}, rollback_count={state['rollback_count']}")
    entries.append("파이프라인 완료")
    state["log_entries"] = entries
    return state
