"""Postgres 접속 정보 — pipeline/logging_agent.py와 같은 .env PG* 값을 쓴다."""

from __future__ import annotations

import os


def connection_params() -> dict[str, str]:
    return {
        "host": os.environ.get("PGHOST", "localhost"),
        "port": os.environ.get("PGPORT", "5432"),
        "dbname": os.environ.get("PGDATABASE", "cloud_anomaly_agent"),
        "user": os.environ.get("PGUSER", "postgres"),
        "password": os.environ.get("PGPASSWORD", ""),
    }
