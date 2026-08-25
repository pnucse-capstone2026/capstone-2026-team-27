"""
no_langgraph/raw_python/logging_stage.py

담당자가 없다. 요구사항에는 개발자 A=Detection, B=Classification+QA,
C=Decision+Action만 명시돼 있고 Logging은 누구 몫인지 정해져 있지 않다.
실제로는 이런 "명시적 주인이 없는 조각"이 꼭 하나씩 생기고, 대개 마지막에
전체를 조립하는 사람(통합 담당자)이 떠맡게 된다 — DESIGN_NOTES.md
"3명 동시 개발 시 충돌 지점" 참고.
"""

from no_langgraph.raw_python.state import PipelineState


def run_logging(state: PipelineState) -> PipelineState:
    state.log_entries.append(
        f"[LOG] resource={state.resource_id} anomaly={state.anomaly_flag} "
        f"type={state.anomaly_type} action={state.action_executed} "
        f"qa_passed={state.qa_passed} rollback_count={state.rollback_count}"
    )
    for entry in state.log_entries:
        print(entry)
    return state
