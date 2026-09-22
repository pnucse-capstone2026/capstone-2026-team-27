from __future__ import annotations

import psycopg2
import psycopg2.extras
from fastapi import APIRouter

from api.pg import connection_params
from pipeline.live_events import scenario_label, action_label, completion_action_label

router = APIRouter(prefix="/notifications", tags=["notifications"])


def _build_message(row: dict) -> dict:
    scenario = scenario_label(
        row["resource_type"], row["anomaly_type"], row["ec2_utilization_band"]
    )
    action = action_label(row["selected_action"])

    if row["event_type"] == "decision":
        if row["requires_approval"]:
            text = f"🔍 {scenario}! 확인이 필요합니다"
        else:
            text = f"🔍 {scenario}! 자동으로 {action}합니다"
    else:
        if row["qa_passed"]:
            if row["selected_action"] == "NoAction":
                text = "✅ 정상 범위로 판단되어 추가 조치 없이 지켜봅니다"
            else:
                label = completion_action_label(
                    row["selected_action"], row["anomaly_type"]
                )
                text = f"✅ {label} 완료! QA 검증까지 통과했습니다"
        else:
            text = "↩️ QA가 효과 없음을 확인해 원래대로 되돌렸습니다"

    return {
        "event_id": row["event_id"],
        "event_type": row["event_type"],
        "resource_id": row["resource_id"],
        "text": text,
        "requires_approval": bool(row["requires_approval"])
        if row["event_type"] == "decision"
        else False,
        "created_at": row["created_at"].isoformat() if row["created_at"] else None,
    }


@router.get("/recent")
def get_recent_notifications(after_id: int = 0, limit: int = 20):
    try:
        conn = psycopg2.connect(**connection_params())
    except Exception:
        return {"events": [], "latest_id": after_id}

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT to_regclass('public.pipeline_events')")
            if cur.fetchone()["to_regclass"] is None:
                return {"events": [], "latest_id": after_id}

            cur.execute(
                """
                SELECT event_id, event_type, resource_id, resource_type, anomaly_type,
                       ec2_utilization_band, selected_action, risk_level,
                       requires_approval, qa_passed, rollback_count, created_at
                FROM pipeline_events
                WHERE event_id > %s
                ORDER BY event_id ASC
                LIMIT %s
                """,
                (after_id, limit),
            )
            rows = cur.fetchall()

            cur.execute(
                "SELECT COALESCE(MAX(event_id), %s) AS max_id FROM pipeline_events",
                (after_id,),
            )
            db_latest_id = cur.fetchone()["max_id"]
    finally:
        conn.close()

    events = [_build_message(dict(r)) for r in rows]
    latest_id = events[-1]["event_id"] if events else after_id
    return {"events": events, "latest_id": latest_id, "db_latest_id": db_latest_id}
