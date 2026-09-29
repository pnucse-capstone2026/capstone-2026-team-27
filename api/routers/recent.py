"""
대시보드 "최근 탐지" 목록 — 승인 대기 중(checkpointer)인 것과
이미 끝난 실행(Postgres agent_runs)을 시간순으로 합쳐서 보여준다.

상태 표시 규칙:
  - 승인 대기 중         -> 예상 절감액 ($/hr)
  - status='completed'  -> "조치 완료"
  - 그 외(실패)          -> "실패"
"""

from __future__ import annotations

import psycopg2
import psycopg2.extras
from fastapi import APIRouter

from api import graph_runtime
from api.pg import connection_params

router = APIRouter(prefix="/recent-detections", tags=["recent"])


def _format_usd_per_hour(value: float) -> str:
    """일반적인 값은 소수점 2자리로 충분하지만, S3처럼 트래픽이 미미한 테스트
    리소스는 실제 절감액이 $0.0000003/hr처럼 2자리에서 그냥 0으로 뭉개진다.
    0이 아닌 값은 유효숫자가 보일 때까지 소수점 자리수를 늘린다
    (frontend/src/format.js의 formatUsdPerHour와 동일한 규칙)."""
    if not value:
        return "0.00"
    if abs(value) >= 0.01:
        return f"{value:.2f}"

    decimals = 2
    while decimals < 20 and round(value, decimals) == 0:
        decimals += 1
    return f"{value:.{min(decimals + 1, 20)}f}"


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
                SELECT resource_id, resource_type, selected_action, risk_level, status,
                       finished_at, estimated_saving_usd
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
        saving = row.get("estimated_saving_usd")
        if row["status"] != "completed":
            display = {"type": "status", "value": "실패"}
        elif saving:
            display = {
                "type": "status",
                "value": f"조치 완료 (예상 절감 ${_format_usd_per_hour(saving)}/hr)",
            }
        else:
            display = {"type": "status", "value": "조치 완료"}
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
