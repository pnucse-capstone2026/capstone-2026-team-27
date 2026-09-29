"""
Settings 임시 캐시
==================
settings_state는 api/routers/settings.py가 매 요청마다 config/decision_policy의 실제
값으로 덮어쓰는 캐시일 뿐이다 (여기 적힌 초기값은 서버가 막 뜬 순간에만 잠깐 보일 수
있는 자리표시자 — 실제 응답 전에 항상 _sync_real_values()로 덮어써짐).

이 파일에 원래 있던 rule_book/whitelist/pipeline_nodes mock은 전부 제거했다 —
Rule Book/화이트리스트는 schema/rules/*.json 실파일로, 파이프라인 노드 상태는
config/pipeline_live_status.py + Postgres agent_steps로 완전히 대체됐고, 아무 데서도
더 이상 참조하지 않는 죽은 코드였다.
"""

from __future__ import annotations

settings_state: dict = {
    "priority_weight": 30,
    "polling_interval": 5,
    "llm_cost_limit": 5,
    "resources": {
        "EC2": True,
        "Lambda": True,
        "S3": False,
        "RDS": True,
        "AutoScaling": True,
    },
}

pipeline_stats: dict = {
    # 아래 값들은 api/routers/status.py가 매 요청마다 실제 값
    # (Postgres agent_runs, checkpointer, schema/rules/*.json)으로 덮어씀
    "anomaly_detected": 0,
    "anomaly_completed": 0,
    "anomaly_failed": 0,
    "pending_approvals": 0,
    "active_rules": 0,
}
