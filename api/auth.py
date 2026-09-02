"""
관리자 대시보드 접근 제어 — ID/PW 로그인 + 세션 토큰.
========================================================
계정은 api/admin_users.py의 Postgres 테이블(admin_users)에 bcrypt 해시로 저장된다.
로그인 성공 시 랜덤 토큰을 발급하고, 그 이후 모든 요청은 X-Admin-Key 헤더에 이
토큰을 실어서 보내야 한다 (api/routers/auth.py가 발급, 여기서 검증).

세션은 이 FastAPI 프로세스 메모리에만 있다 — 서버 재시작하면 전원 다시 로그인해야
함. 이 프로젝트 규모(관리자 소수, 캡스톤)엔 Redis 같은 별도 세션 저장소까지는
과해서 이렇게 뒀다. 나중에 여러 인스턴스로 확장하게 되면 그때 옮기면 됨.
"""

from __future__ import annotations

import secrets
from datetime import datetime, timedelta, timezone

from fastapi import Header, HTTPException

SESSION_TTL = timedelta(hours=12)

# {token: {"username": str, "expires_at": datetime}}
_sessions: dict[str, dict] = {}

# ── 로그인 시도 제한 (brute-force 방지) ──────────────────────────────────────
# 계정이 admin1/admin2/admin3처럼 아이디==비밀번호인 단순한 값이라, 제한이
# 없으면 자동화된 무차별 대입으로 순식간에 뚫린다. IP가 아니라 username별로
# 세는 이유: 이 프로젝트는 계정이 3개로 고정돼 있어서(회원가입 없음) username
# 기준으로 잠그는 게 더 확실하다 — IP 기준이면 여러 IP로 우회 가능하지만
# username 기준은 그 계정 자체를 못 건드리게 막는다.
MAX_LOGIN_ATTEMPTS = 5
LOCKOUT_DURATION = timedelta(minutes=5)

# ⚠️ 계정을 돌려가며 시도하면(id1 5번 -> id2 5번 -> ...) username별 제한만으로는
# 시스템 전체 시도 횟수가 계정 수만큼 늘어나는 허점이 있다. "전체 합산 잠금"으로
# 막아볼 수도 있지만, 그러면 아무나 아무 계정에나 몇 번 틀리기만 해도 관리자
# 전원이 한꺼번에 잠기는 더 큰 문제(셀프 DoS)가 생겨서 채택하지 않았다 — 이
# 프로젝트처럼 승인 대기 같은 시간 민감한 작업이 있는 경우 특히 위험하다.
# username별 제한만으로 남겨두는 게 트레이드오프상 낫다는 판단.

# {username: {"failed_count": int, "locked_until": datetime | None}}
_login_attempts: dict[str, dict] = {}


def is_locked_out(username: str) -> tuple[bool, int]:
    """(잠겨있는지, 남은 시간(초)) 반환."""
    record = _login_attempts.get(username)
    if record is None or record.get("locked_until") is None:
        return False, 0

    remaining = (record["locked_until"] - datetime.now(timezone.utc)).total_seconds()
    if remaining <= 0:
        # 잠금 시간이 지났으면 초기화하고 다시 시도 허용
        _login_attempts.pop(username, None)
        return False, 0

    return True, int(remaining)


def record_failed_login(username: str) -> None:
    record = _login_attempts.setdefault(username, {"failed_count": 0, "locked_until": None})
    record["failed_count"] += 1
    if record["failed_count"] >= MAX_LOGIN_ATTEMPTS:
        record["locked_until"] = datetime.now(timezone.utc) + LOCKOUT_DURATION


def record_successful_login(username: str) -> None:
    _login_attempts.pop(username, None)


def create_session(username: str) -> str:
    token = secrets.token_urlsafe(32)
    _sessions[token] = {
        "username": username,
        "expires_at": datetime.now(timezone.utc) + SESSION_TTL,
    }
    return token


def invalidate_session(token: str) -> None:
    _sessions.pop(token, None)


def verify_session_token(x_admin_key: str = Header(default="")) -> str:
    """반환값: 로그인한 username (라우터에서 "누가 했는지" 기록하고 싶을 때 쓸 수 있음)"""
    session = _sessions.get(x_admin_key)
    if session is None:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    if datetime.now(timezone.utc) > session["expires_at"]:
        _sessions.pop(x_admin_key, None)
        raise HTTPException(status_code=401, detail="세션이 만료됐습니다. 다시 로그인해주세요.")

    return session["username"]
