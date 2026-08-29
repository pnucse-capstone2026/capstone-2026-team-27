"""
no_langgraph/raw_python/decision_action.py  (담당: 개발자 C)

개발자 C가 알아야 하는 것: state.anomaly_type (B가 채움) + 자기가 채워야 할
selected_action/action_result. rollback_action도 여기 둔다 — "방금 실행한
것을 되돌리는" 책임은 애초에 실행한 사람(C)이 가장 잘 알기 때문이다
(QA는 "통과했는지"만 판단하지 "어떻게 되돌리는지"는 모른다).

⚠️ 데모용 run_action은 성공/실패를 resource_id 문자열로 흉내낸다 — 재시도
   루프가 실제로 도는 걸 재현성 있게 보여주기 위한 장치일 뿐, 실제 SLA
   판단 로직이 아니다.
     - resource_id에 "FAIL"이 들어있으면 항상 실패 (재시도 소진 시나리오)
     - resource_id에 "RECOVER1"이 들어있으면 1번 롤백 후에는 성공
       (재시도로 복구되는 시나리오)
     - 그 외에는 항상 성공 (재시도 없이 끝나는 시나리오)
"""

from no_langgraph.raw_python.state import PipelineState

ALLOWED_ACTIONS = {
    "cost_inefficiency": "Stop",
    "cost_spike": "Throttle",
    "risk_security": "Block",
}


def run_decision(state: PipelineState) -> PipelineState:
    state.selected_action = ALLOWED_ACTIONS.get(state.anomaly_type, "NoAction")
    state.risk_level = "LOW"
    return state


def run_action(state: PipelineState) -> PipelineState:
    """더미 액션 실행."""
    state.pre_action_snapshot = {"prior_state": "running"}

    if "FAIL" in state.resource_id:
        success = False
    elif "RECOVER1" in state.resource_id:
        success = state.rollback_count >= 1
    else:
        success = True

    state.action_executed = state.selected_action
    state.action_result = (
        {"status": "success"} if success else {"status": "failed", "error": "더미 실패"}
    )
    return state


def rollback_action(state: PipelineState) -> None:
    """실행했던 액션을 되돌린다 (더미: 스냅샷으로 복원했다고 가정하고 로그만 남김)."""
    state.log_entries.append(
        f"[ROLLBACK] '{state.action_executed}' 되돌림 (스냅샷={state.pre_action_snapshot})"
    )
