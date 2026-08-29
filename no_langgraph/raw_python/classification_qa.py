"""
no_langgraph/raw_python/classification_qa.py  (담당: 개발자 B)

개발자 B가 알아야 하는 것:
  - run_classification: state.triggered_metrics (A가 채움) + 자기가 채워야
    할 anomaly_type.
  - run_qa: state.action_result (C가 채움) + 자기가 채워야 할 qa_passed.

B는 A의 탐지 알고리즘 내부나 C의 boto3 호출 방식을 몰라도 된다. 다만
action_result 딕셔너리 안에 어떤 키가 들어있는지는 C와 미리 합의해야 한다
(여기서는 {"status": "success"/"failed"} 하나만 약속).
"""

from no_langgraph.raw_python.state import PipelineState


def run_classification(state: PipelineState) -> PipelineState:
    """더미 분류: cost만 튀었으면 cost_inefficiency, 그 외는 cost_spike."""
    if "cost" in state.triggered_metrics:
        state.anomaly_type = "cost_inefficiency"
        state.classification_reasoning = "cost 지표만 단독 이상"
    else:
        state.anomaly_type = "cost_spike"
        state.classification_reasoning = "기타 지표 이상"
    return state


def run_qa(state: PipelineState) -> PipelineState:
    """
    더미 SLA 검증: action_result.status가 success면 통과.
    실제로는 CPU/비용/가용성을 따로 확인하겠지만, 이 데모의 초점은 재시도
    흐름 자체이므로 판정 로직은 최대한 단순하게 뒀다.
    """
    result = state.action_result or {}
    state.qa_passed = result.get("status") == "success"
    return state
