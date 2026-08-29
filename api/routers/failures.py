"""
처리 실패 목록 — Postgres agent_runs/agent_steps에서 직접 조회한다.

LLM 로그(schema/logs/*.jsonl)는 classification/decision 단계에서 LLM을 실제로
호출했을 때만 남는 기록이라, 룰 기반으로 처리된 실행이나 QA/액션 단계에서 실패한
건은 거기 안 나온다. 실패 여부는 LLM 호출과 무관하게 agent_runs.status로 판단해야
하므로 별도 엔드포인트로 분리했다.

"""

from __future__ import annotations

import psycopg2
import psycopg2.extras
from fastapi import APIRouter

from api.pg import connection_params

router = APIRouter(prefix="/failures", tags=["failures"])

_FAILURE_STATUSES = ("failed_qa", "rollback_exhausted")


@router.get("")
def get_failures():
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
                SELECT
                    r.run_id,
                    r.resource_id,
                    r.resource_type,
                    r.anomaly_type,
                    r.selected_action,
                    r.risk_level,
                    r.status,
                    r.rollback_count,
                    r.finished_at,
                    s.output AS qa_output
                FROM agent_runs r
                LEFT JOIN agent_steps s
                    ON s.run_id = r.run_id AND s.step_name = 'qa'
                WHERE r.status = ANY(%s)
                ORDER BY r.finished_at DESC
                """,
                (list(_FAILURE_STATUSES),),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    failures = []
    for row in rows:
        qa_output = row.get("qa_output") or {}
        sla = qa_output.get("sla_check_result") or {}
        failures.append(
            {
                "id": str(row["run_id"]),
                "resource_id": row["resource_id"],
                "resource_type": row["resource_type"],
                "anomaly_type": row["anomaly_type"],
                "selected_action": row["selected_action"],
                "risk_level": row["risk_level"],
                "status": row["status"],
                "rollback_count": row["rollback_count"],
                "timestamp": row["finished_at"].isoformat() if row["finished_at"] else None,
                "sla_detail": sla.get("detail"),
                "cpu_ok": sla.get("cpu_ok"),
                "cost_ok": sla.get("cost_ok"),
                "availability_ok": sla.get("availability_ok"),
            }
        )
    return failures
