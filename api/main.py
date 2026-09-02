"""
관리자 제어판 백엔드
======================
approvals.py는 실제 LangGraph checkpointer(Postgres)에 연결되어 있어,
approval_gate에서 interrupt()로 멈춘 thread를 조회/재개한다 (api/graph_runtime.py).

인증: ID/PW 로그인 (api/admin_users.py — PostgreSQL, bcrypt 해시) + 세션 토큰

"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api import admin_users, graph_runtime
from api.auth import verify_session_token
from api.routers import approvals, auth, failures, logs, promotions, recent, rules, settings, status, whitelist


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Postgres 연결 시도 (실패해도 기본 API는 동작) — admin_users 테이블 생성도
    # 이 안에 포함시켜서, DB가 안 떠 있을 때 로그인 테이블 생성 실패로 서버 전체가
    # 죽지 않고 graph_runtime과 동일하게 "승인/로그인 기능 제외 기본 API만 동작"으로 대응한다.
    try:
        admin_users.ensure_table()  # admin_users 테이블 없으면 생성 (계정 자체는 별도 시딩 스크립트로)
        graph_runtime.start()  # Postgres 연결 + 승인 그래프 준비
    except Exception as e:
        print(f"[WARNING] graph_runtime/admin_users 시작 실패 (Postgres 미연결): {e}")
        print("[WARNING] 승인/로그인 기능 제외하고 기본 API만 동작합니다.")
    try:
        yield
    finally:
        try:
            graph_runtime.stop()
        except Exception:
            pass


app = FastAPI(title="Cloud Anomaly Agent - Admin API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 로그인/로그아웃은 인증 없이 접근 가능해야 함
app.include_router(auth.router)

# 나머지는 전부 로그인 세션 토큰 필요
_protected = Depends(verify_session_token)
app.include_router(status.router, dependencies=[_protected])
app.include_router(approvals.router, dependencies=[_protected])
app.include_router(rules.router, dependencies=[_protected])
app.include_router(whitelist.router, dependencies=[_protected])
app.include_router(promotions.router, dependencies=[_protected])
app.include_router(logs.router, dependencies=[_protected])
app.include_router(failures.router, dependencies=[_protected])
app.include_router(recent.router, dependencies=[_protected])
app.include_router(settings.router, dependencies=[_protected])


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
