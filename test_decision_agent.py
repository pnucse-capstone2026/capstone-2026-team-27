# test_decision.py

import os

from pipeline.nodes.decision_agent import decision_node  
from dotenv import load_dotenv

load_dotenv()

# state = {
#     "resource_id": "i-0testdummy",
#     "resource_type": "EC2",
#     "anomaly_type": "cost_inefficiency",
#     "anomaly_score_zscore": 5.0,
#     "raw_metrics": {
#         "cpu_utilization": [3.0] * 30,
#         "cost": [1.2] * 30,
#     },
# }
state = {
    "resource_id": "i-0testdummy",
    "resource_type": "EC2",
    "anomaly_type": "cost_inefficiency",

    # 애매한 구간 -> LLM 사용
    "anomaly_score_zscore": 3.0,

    "raw_metrics": {
        "cpu_utilization": [
            7.5, 6.9, 7.2, 8.1, 6.8,
            7.0, 7.4, 6.7, 7.3, 7.1,
            7.0, 6.9, 7.5, 7.2, 6.8,
            7.1, 7.3, 7.0, 6.9, 7.2,
            7.1, 7.0, 7.4, 6.8, 7.2,
            7.1, 7.0, 7.3, 6.9, 7.2
        ],
        "cost": [
            1.20, 1.22, 1.19, 1.21, 1.23,
            1.20, 1.24, 1.22, 1.21, 1.20,
            1.23, 1.22, 1.21, 1.20, 1.24,
            1.23, 1.21, 1.22, 1.20, 1.23,
            1.21, 1.22, 1.20, 1.23, 1.21,
            1.22, 1.20, 1.23, 1.21, 1.22
        ],
    },
}

result = decision_node(state)
print("=" * 60)
print("Selected Action :", result["selected_action"])
print("Risk Level      :", result["risk_level"])
print("Requires Approval :", result["requires_approval"])
print("Reason          :", result["decision_reasoning"])
print("\nCandidate Actions")
for c in result["candidate_actions"]:
    print(
        f"{c['action']:<15}"
        f" saving={c['saving_rate']:.2f}"
        f" impact={c['impact_score']:.2f}"
        f" stability={c['stability_score']:.2f}"
        f" score={c['score']:.3f}"
    )