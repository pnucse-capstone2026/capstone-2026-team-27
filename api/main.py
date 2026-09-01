"""
관리자 제어판 백엔드
======================
approvals.py는 실제 LangGraph checkpointer(Postgres)에 연결되어 있어,
approval_gate에서 interrupt()로 멈춘 thread를 조회/재개한다 (api/graph_runtime.py).
나머지 라우터(rules/whitelist/logs/settings)는 아직 api/store.py의 mock 데이터를 쓴다.
실제 연동 지점은 각 api/routers/*.py 파일의 # TODO 주석 참고.

실행 (프로젝트 루트에서, docker-compose postgres가 떠 있어야 함):
    pip install -r api/requirements.txt
    docker compose up -d postgres
    python -m api.main
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api import graph_runtime
from api.routers import approvals, failures, logs, promotions, recent, rules, settings, status, whitelist


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Postgres 연결 시도 (실패해도 기본 API는 동작)
    try:
        graph_runtime.start()
    except Exception as e:
        print(f"[WARNING] graph_runtime 시작 실패 (Postgres 미연결): {e}")
        print("[WARNING] 승인 기능 제외하고 기본 API만 동작합니다.")
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

app.include_router(status.router)
app.include_router(approvals.router)
app.include_router(rules.router)
app.include_router(whitelist.router)
app.include_router(promotions.router)
app.include_router(logs.router)
app.include_router(failures.router)
app.include_router(recent.router)
app.include_router(settings.router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
