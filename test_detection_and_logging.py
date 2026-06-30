"""
스모크 테스트: detection_agent.py + logging_agent.py 가 실제로 동작하는지 확인.

[사전 준비]
  1. 로컬 PostgreSQL이 떠 있고, PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD
     환경변수가 "이 스크립트를 실행할 같은 터미널 세션"에 설정돼 있어야 함.
     (export는 터미널을 새로 열면 사라지니, export 한 그 터미널에서 바로 실행)
  2. 필요한 패키지 설치:
       pip install scikit-learn numpy psycopg2-binary --break-system-packages
     (가상환경 쓰고 있으면 --break-system-packages는 빼도 됨)

[실행 위치]
  pipeline/, schema/ 폴더가 보이는 프로젝트 루트에서:
       python test_pipeline_smoke.py
"""

import random

from dotenv import load_dotenv

from pipeline.detection_agent import detection_node
from pipeline.logging_agent import logging_node

load_dotenv()  # .env 파일에서 PGHOST 등 환경변수 로드 (run_dummy_pipeline.py와 동일한 방식)

random.seed(42)

# ── 1) 가짜 EC2 지표 데이터 생성 (cost에 마지막 시점 스파이크) ────────────────
cpu = [50 + random.uniform(-2, 2) for _ in range(30)]
network_in = [1000 + random.uniform(-50, 50) for _ in range(30)]
network_out = [800 + random.uniform(-50, 50) for _ in range(30)]
cost = [2.0 + random.uniform(-0.1, 0.1) for _ in range(29)] + [9.5]  # 마지막에 스파이크

state = {
    "resource_id": "i-test-0001",
    "resource_type": "EC2",
    "raw_metrics": {
        "cpu_utilization": cpu,
        "network_in": network_in,
        "network_out": network_out,
        "cost": cost,
    },
    "timestamp": "2026-06-29T10:00:00Z",

    # 아래는 detection 이전 단계라 비워둠 (실제 그래프에선 각 노드가 채움)
    "anomaly_flag": False,
    "anomaly_score_zscore": None,
    "anomaly_score_iforest": None,
    "triggered_metrics": [],
    "anomaly_type": None,
    "classification_reasoning": None,
    "interim_action_taken": None,
    "candidate_actions": [],
    "selected_action": None,
    "risk_level": None,
    "requires_approval": False,
    "decision_reasoning": None,
    "pre_action_snapshot": None,
    "action_executed": None,
    "action_result": None,
    "qa_passed": None,
    "sla_check_result": None,
    "rollback_count": 0,
    "log_entries": [],
}

print("=" * 60)
print("1) detection_node 실행")
print("=" * 60)
state = detection_node(state)
print(f"anomaly_flag          : {state['anomaly_flag']}")
print(f"anomaly_score_zscore  : {state['anomaly_score_zscore']}")
print(f"anomaly_score_iforest : {state['anomaly_score_iforest']}")
print(f"triggered_metrics     : {state['triggered_metrics']}")

print()
print("=" * 60)
print("2) logging_node 실행 (PostgreSQL에 실제로 INSERT)")
print("=" * 60)
state = logging_node(state)  # 여기서 DB 연결 실패하면 에러 메시지로 원인 확인 가능
print("DB 적재 완료 ✅")
print()
print("state['log_entries'] 요약:")
for line in state["log_entries"]:
    print(" ", line)

print()
print("=" * 60)
print("DB에 진짜 쌓였는지 psql로 직접 확인하려면 (다른 터미널에서):")
print("=" * 60)
print("""
psql -U postgres -d cloud_anomaly_agent -c \\
  "SELECT resource_id, resource_type, anomaly_flag, selected_action, status FROM agent_runs ORDER BY finished_at DESC LIMIT 5;"

psql -U postgres -d cloud_anomaly_agent -c \\
  "SELECT step_name, status FROM agent_steps ORDER BY logged_at DESC LIMIT 5;"

# (db 이름/user가 .env에 설정한 값과 다르면 그에 맞게 바꿔서 입력하세요)
""")
