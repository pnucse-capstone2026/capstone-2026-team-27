"""
Postgres Checkpointer
=====================
docker-compose.yml의 postgres 컨테이너를 LangGraph checkpointer 저장소로도 재사용한다. 
`PostgresSaver.setup()`이 필요한 테이블(checkpoints, checkpoint_writes 등)을
같은 DB 안에 알아서 만든다.
"""

from __future__ import annotations

import os

from langgraph.checkpoint.postgres import PostgresSaver


def _conn_string() -> str:
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    dbname = os.getenv("PGDATABASE", "postgres")
    user = os.getenv("PGUSER", "postgres")
    password = os.getenv("PGPASSWORD", "")
    return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"


def get_postgres_checkpointer():
    """
    PostgresSaver 컨텍스트 매니저를 반환한다.
    """
    return PostgresSaver.from_conn_string(_conn_string())
