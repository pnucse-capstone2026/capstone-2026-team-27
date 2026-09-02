from __future__ import annotations

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel

from api import admin_users, auth

router = APIRouter(prefix="/auth", tags=["auth"])


class LoginRequest(BaseModel):
    username: str
    password: str


@router.post("/login")
def login(body: LoginRequest):
    locked, remaining_seconds = auth.is_locked_out(body.username)
    if locked:
        raise HTTPException(
            status_code=429,
            detail={
                "message": f"로그인 시도가 너무 많습니다. {remaining_seconds}초 후 다시 시도해주세요.",
                "retry_after_seconds": remaining_seconds,
            },
        )

    if not admin_users.verify_user(body.username, body.password):
        auth.record_failed_login(body.username)
        raise HTTPException(status_code=401, detail="아이디 또는 비밀번호가 올바르지 않습니다.")

    auth.record_successful_login(body.username)
    token = auth.create_session(body.username)
    return {"token": token, "username": body.username}


@router.post("/logout")
def logout(x_admin_key: str = Header(default="")):
    auth.invalidate_session(x_admin_key)
    return {"status": "ok"}
