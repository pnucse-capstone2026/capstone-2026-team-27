"""
대시보드 "최근 탐지" 목록 — 승인 대기 중(checkpointer)인 것과 
이미 끝난 실행(Postgres agent_runs)을 시간순으로 합쳐서 보여준다.

상태 표시 규칙:
  - 승인 대기 중         -> 예상 절감액 ($/hr)
  - status='completed'  -> "처리 완료"
  - 그 외(실패)          -> "실패"
"""

from __future__ import annotations

import psycopg2
import psycopg2.extras
from fastapi import APIRouter

from api import graph_runtime
from api.pg import connection_params

router = APIRouter(prefix="/recent-detections", tags=["recent"])


def _pending_items() -> list[dict]:
    items = []
    for pending in graph_runtime.list_pending_approvals():
        interrupt = pending["interrupt"]
        selected_action = interrupt.get("selected_action")

        estimated_saving = 0.0
        for candidate in interrupt.get("candidate_actions") or []:
            if candidate.get("action") == selected_action:
                estimated_saving = candidate.get("estimated_saving_usd", 0.0)
                break

        items.append(
            {
                "id": pending["thread_id"],
                "severity": interrupt.get("risk_level"),
                "action": selected_action,
                "resource_type": interrupt.get("resource_type"),
                "resource_id": interrupt.get("resource_id"),
                "timestamp": pending["created_at"],
                "display": {"type": "saving", "value": estimated_saving},
            }
        )
    return items


def _finished_items(limit: int) -> list[dict]:
    try:
        conn = psycopg2.connect(**connection_params())
    except Exception:
        return []

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT to_regclass('public.agent_runs')")
            if cur.fetchone()["to_regclass"] is None:
                return []

            cur.execute(
                """
                SELECT resource_id, resource_type, selected_action, risk_level, status, finished_at
                FROM agent_runs
                WHERE anomaly_flag = true
                ORDER BY finished_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    items = []
    for row in rows:
        display = (
            {"type": "status", "value": "처리 완료"}
            if row["status"] == "completed"
            else {"type": "status", "value": "실패"}
        )
        items.append(
            {
                "id": f"run-{row['resource_id']}-{row['finished_at'].isoformat()}",
                "severity": row["risk_level"],
                "action": row["selected_action"],
                "resource_type": row["resource_type"],
                "resource_id": row["resource_id"],
                "timestamp": row["finished_at"].isoformat(),
                "display": display,
            }
        )
    return items


@router.get("")
def get_recent_detections(limit: int = 5):
    items = _pending_items() + _finished_items(limit)
    items.sort(key=lambda i: i["timestamp"], reverse=True)
    return items[:limit]
