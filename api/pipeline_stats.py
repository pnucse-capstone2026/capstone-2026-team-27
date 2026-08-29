"""
대시보드 통계 — Logging Agent가 Postgres에 쌓아둔 agent_runs 테이블에서 조회한다.
pipeline/logging_agent.py와 같은 접속 정보(.env의 PG*)를 쓴다.

카드들의 정의 (서로 겹치지 않도록 대시보드에서 합의된 기준):
  - anomaly_detected : anomaly_flag=true인 것 (성공/실패 무관, "문제가 있었다"는 사실만)
  - anomaly_completed: anomaly_flag=true AND status='completed'인 것
                        (이상이 있었고, 액션까지 실행해서 QA도 통과한 것만)
  - anomaly_failed   : anomaly_flag=true AND status IN ('failed_qa', 'rollback_exhausted')인 것
                        (QA를 통과 못했거나 롤백을 2번 다 실패한 것)
  - 승인 대기는 agent_runs와 무관 (checkpointer에서 실시간 조회, api/graph_runtime.py)

  anomaly_detected = 승인 대기 + anomaly_completed + anomaly_failed 로 딱 맞아떨어진다.
"""

from __future__ import annotations

import psycopg2

from api.pg import connection_params


def get_run_counts() -> dict:
    """agent_runs 테이블이 아직 없으면(파이프라인이 한 번도 안 돈 로컬 환경) 전부 0."""
    empty = {"anomaly_detected": 0, "anomaly_completed": 0, "anomaly_failed": 0}

    try:
        conn = psycopg2.connect(**connection_params())
    except Exception:
        return empty

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.agent_runs')")
            if cur.fetchone()[0] is None:
                return empty

            cur.execute(
                """
                SELECT
                    count(*) FILTER (WHERE anomaly_flag = true),
                    count(*) FILTER (WHERE anomaly_flag = true AND status = 'completed'),
                    count(*) FILTER (WHERE anomaly_flag = true AND status IN ('failed_qa', 'rollback_exhausted'))
                FROM agent_runs
                """
            )
            anomaly_detected, anomaly_completed, anomaly_failed = cur.fetchone()
            return {
                "anomaly_detected": anomaly_detected,
                "anomaly_completed": anomaly_completed,
                "anomaly_failed": anomaly_failed,
            }
    finally:
        conn.close()
