"""
관리자 계정 — 기존 Postgres(체크포인터/로깅이 사용하는 DB)에 테이블 하나만 추가한다.
비밀번호는 bcrypt로 해시해서만 저장한다.
"""

from __future__ import annotations

import bcrypt
import psycopg2

from api.pg import connection_params

_DDL = """
CREATE TABLE IF NOT EXISTS admin_users (
    id            SERIAL PRIMARY KEY,
    username      TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def ensure_table() -> None:
    conn = psycopg2.connect(**connection_params())
    try:
        with conn.cursor() as cur:
            cur.execute(_DDL)
        conn.commit()
    finally:
        conn.close()


def create_user(username: str, password: str) -> None:
    """이미 있는 username이면 비밀번호를 덮어쓴다 (초기 계정 세팅용)"""
    password_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
    conn = psycopg2.connect(**connection_params())
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO admin_users (username, password_hash)
                VALUES (%s, %s)
                ON CONFLICT (username) DO UPDATE SET password_hash = EXCLUDED.password_hash
                """,
                (username, password_hash),
            )
        conn.commit()
    finally:
        conn.close()


def verify_user(username: str, password: str) -> bool:
    conn = psycopg2.connect(**connection_params())
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT password_hash FROM admin_users WHERE username = %s", (username,))
            row = cur.fetchone()
    finally:
        conn.close()

    if row is None:
        return False
    return bcrypt.checkpw(password.encode("utf-8"), row[0].encode("utf-8"))
