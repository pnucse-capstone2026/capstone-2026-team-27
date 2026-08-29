"""
no_langgraph/raw_python/detection.py  (담당: 개발자 A)

개발자 A가 알아야 하는 것: state.py의 PipelineState 중 입력 필드
(resource_id, resource_type, raw_metrics)와 자기가 채워야 하는 Step 1 필드뿐.
Classification/Decision/Action/QA/Logging이 내부적으로 뭘 하는지 몰라도,
심지어 orchestrator가 이 함수를 정확히 언제 호출하는지 몰라도 이 파일을
짤 수 있다.

실제 프로덕션 알고리즘(Isolation Forest 등)은 이 데모의 초점이 아니므로
Z-score 하나만 더미로 구현했다.
"""

from no_langgraph.raw_python.state import PipelineState

Z_SCORE_THRESHOLD = 3.0


def run_detection(state: PipelineState) -> PipelineState:
    """cost 시계열의 Z-score가 임계값을 넘으면 이상으로 판단하는 더미 탐지."""
    cost = state.raw_metrics.get("cost", [])

    if len(cost) >= 2:
        mean = sum(cost) / len(cost)
        variance = sum((c - mean) ** 2 for c in cost) / len(cost)
        std = variance ** 0.5
        max_z = max(abs(c - mean) / (std + 1e-9) for c in cost)
    else:
        max_z = 0.0

    triggered = ["cost"] if max_z > Z_SCORE_THRESHOLD else []

    state.anomaly_score_zscore = round(max_z, 3)
    state.triggered_metrics = triggered
    state.anomaly_flag = bool(triggered)
    return state
