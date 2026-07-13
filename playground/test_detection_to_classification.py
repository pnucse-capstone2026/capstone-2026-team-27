# test_detection_to_classification.py (langgraph_study/ 루트에 만들기)
from pipeline.detection_agent import detection_node
from pipeline.classification_agent import classification_node

# Detection 돌리기
state = {
    "resource_id":   "i-0testdummy",
    "resource_type": "EC2",
    "raw_metrics": {
        "cpu_utilization": [90.0] * 30,
        "network_in":      [1024.0] * 30,
        "network_out":     [512.0] * 30,
        "cost":            [5.0] * 30,
    },
    "timestamp":             "2026-06-23T00:00:00",
    "anomaly_flag":          False,
    "anomaly_score_zscore":  None,
    "anomaly_score_iforest": None,
    "triggered_metrics":     [],
    # 나머지 필드 초기값
    "anomaly_type": None, "classification_reasoning": None,
    "interim_action_taken": None, "candidate_actions": [],
    "selected_action": None, "risk_level": None,
    "requires_approval": False, "decision_reasoning": None,
    "pre_action_snapshot": None, "action_executed": None,
    "action_result": None, "qa_passed": None,
    "sla_check_result": None, "rollback_count": 0, "log_entries": [],
}

# 1단계: Detection 실행
state = detection_node(state)
print("=== Detection 출력 ===")
print("anomaly_flag:", state["anomaly_flag"])
print("anomaly_score_zscore:", state["anomaly_score_zscore"])
print("triggered_metrics:", state["triggered_metrics"])

# 2단계: Classification에 그대로 넘기기
state = classification_node(state)
print("\n=== Classification 출력 ===")
print("anomaly_type:", state["anomaly_type"])
print("classification_reasoning:", state["classification_reasoning"])