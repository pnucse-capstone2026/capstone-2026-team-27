"""
Mock 데이터 저장소
==================
지금은 DB 없이 전역 변수로 상태를 들고 있는다. 각 라우터가 이 모듈의 리스트/딕셔너리를
직접 읽고 쓴다.

승인 큐(approval_queue)는 더 이상 여기 없다 — api/graph_runtime.py가 LangGraph
checkpointer(Postgres)에서 interrupt된 thread 목록을 직접 조회한다 (api/routers/approvals.py).
LLM 로그(llm_logs)도 더 이상 여기 없다 — api/routers/logs.py가
schema/logs/llm_classification_log.jsonl을 직접 읽는다.

나머지는 아직 mock이고, 실제 연동 시 교체될 지점이다:
  - rule_book / whitelist -> schema/rules/*.json (RuleEngine 로드 대상) 또는 DB 테이블
  - settings_state  -> config/decision_policy 테이블
"""

from __future__ import annotations

import itertools
from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))


def now_iso() -> str:
    return datetime.now(KST).isoformat()


_whitelist_id_counter = itertools.count(1)  # 관리자 웹에서 추가 시 wl1부터 시작

rule_book: list[dict] = [
    {
        "id": "CLF-001",
        "target": "AutoScaling",
        "condition": "desired_capacity > mean×2",
        "result": "risk_security",
        "source": "human",
        "enabled": True,
    },
    {
        "id": "CLF-002",
        "target": "EC2",
        "condition": "avg(cpu_utilization) < 5%  (30min)",
        "result": "cost_inefficiency",
        "source": "human",
        "enabled": True,
    },
    {
        "id": "CLF-003",
        "target": "Lambda",
        "condition": "invocation_count.spike AND error_count.spike",
        "result": "cost_spike",
        "source": "llm",
        "enabled": True,
    },
    {
        "id": "CLF-004",
        "target": "*",
        "condition": "resource_id IN whitelist",
        "result": "force_pass",
        "source": "human",
        "enabled": False,
    },
]

_rule_id_counter = itertools.count(len(rule_book) + 1)

# 화이트리스트는 관리자 웹사이트에서 추가/삭제 (코드에서 자동 제어하지 않음)
whitelist: list[dict] = []

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
    # (Postgres agent_runs, checkpointer)으로 덮어씀
    "anomaly_detected": 0,
    "anomaly_completed": 0,
    "anomaly_failed": 0,
    "pending_approvals": 0,
    "active_rules": sum(1 for r in rule_book if r["enabled"]),
}

pipeline_nodes: dict = {
    "detection": "success",
    "classification": "success",
    "decision": "running",
    "recovery": "idle",
    "logging": "idle",
}


def next_rule_id() -> str:
    return f"CLF-{next(_rule_id_counter):03d}"


def next_whitelist_id() -> str:
    return f"wl{next(_whitelist_id_counter)}"
