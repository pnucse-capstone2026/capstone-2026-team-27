# playground/test_classification.py
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.classification_agent import classification_node

# ── 테스트용 State 만들기 ──────────────────────────────────────────────────

def make_state(resource_type, triggered_metrics, raw_metrics):
    return {
        "resource_id":             "i-0test",
        "resource_type":           resource_type,
        "raw_metrics":             raw_metrics,
        "timestamp":               "2026-06-23T00:00:00",
        "anomaly_flag":            True,
        "anomaly_score_zscore":    3.5,
        "anomaly_score_iforest":   0.7,
        "triggered_metrics":       triggered_metrics,
        "anomaly_type":            None,
        "classification_reasoning": None,
        "interim_action_taken":    None,
        "candidate_actions":       [],
        "selected_action":         None,
        "risk_level":              None,
        "requires_approval":       False,
        "decision_reasoning":      None,
        "pre_action_snapshot":     None,
        "action_executed":         None,
        "action_result":           None,
        "qa_passed":               None,
        "sla_check_result":        None,
        "rollback_count":          0,
        "log_entries":             [],
    }

# ── 케이스별 테스트 ────────────────────────────────────────────────────────

cases = [
    # (설명, resource_type, triggered_metrics, raw_metrics)
    (
        "EC2 cost만 단독 이상 → cost_inefficiency (Rule)",
        "EC2",
        ["cost"],
        {"cpu_utilization": [10.0]*30, "network_in": [1000.0]*30,
         "network_out": [500.0]*30, "cost": [0.05]*30},
    ),
    (
        "Lambda 호출+에러 동시 급증 → cost_spike (Rule)",
        "Lambda",
        ["invocation_count", "error_count"],
        {"invocation_count": [100.0]*30, "error_count": [50.0]*30,
         "duration_avg": [200.0]*30, "cost": [0.01]*30},
    ),
    (
        "S3 bytes_downloaded 단독 → risk_security (Rule)",
        "S3",
        ["bytes_downloaded"],
        {"number_of_requests": [100.0]*30, "bytes_downloaded": [9999999.0]*30,
         "cost": [0.02]*30},
    ),
    (
        "AutoScaling 인스턴스 수 2배 급증 → risk_security (Rule)",
        "AutoScaling",
        ["group_desired_capacity"],
        {"group_desired_capacity":     [2.0]*28 + [2.0, 10.0],  # 마지막에 급증
         "group_in_service_instances": [2.0]*30,
         "cost": [0.1]*30},
    ),
]

for desc, resource_type, triggered_metrics, raw_metrics in cases:
    state = make_state(resource_type, triggered_metrics, raw_metrics)
    result = classification_node(state)
    print(f"\n[ {desc} ]")
    print(f"  anomaly_type   : {result['anomaly_type']}")
    print(f"  reasoning      : {result['classification_reasoning']}")
    print(f"  interim_action : {result['interim_action_taken']}")

from dotenv import load_dotenv
load_dotenv()

print("\n[ EC2 cpu+network 동시 이상 → LLM 판단 ]")
state = make_state(
    resource_type="EC2",
    triggered_metrics=["cpu_utilization", "network_in"],
    raw_metrics={
        "cpu_utilization": [10.0]*28 + [10.0, 92.0],   # 마지막에 급등
        "network_in":      [1000.0]*28 + [1000.0, 98000.0],
        "network_out":     [500.0]*30,
        "cost":            [0.01]*30,
    }
)
result = classification_node(state)
print(f"  anomaly_type   : {result['anomaly_type']}")
print(f"  reasoning      : {result['classification_reasoning']}")
print(f"  interim_action : {result['interim_action_taken']}")