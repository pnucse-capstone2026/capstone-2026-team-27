"""
LangGraph 연동 런타임
======================
FastAPI 프로세스가 살아있는 동안 계속 열어두는 Postgres checkpointer + 승인 그래프.
FastAPI의 lifespan에서 start()/stop()을 호출한다 (api/main.py 참고).

주의: 파이프라인 실행(detection ~ decision)은 이 API가 직접 트리거하지 않는다. 
이 API는 이미 실행 중이다가 approval_gate에서
멈춘 thread들을 조회하고 재개하는 역할만 한다.
"""

from __future__ import annotations

from pipeline.checkpointer import get_postgres_checkpointer
from pipeline.graph import build_approval_graph

_checkpointer_cm = None
_checkpointer = None
approval_app = None


def start() -> None:
    """FastAPI startup에서 호출. Postgres 커넥션을 열고 승인 그래프를 컴파일해둔다."""
    global _checkpointer_cm, _checkpointer, approval_app

    _checkpointer_cm = get_postgres_checkpointer()
    _checkpointer = _checkpointer_cm.__enter__()
    _checkpointer.setup()
    approval_app = build_approval_graph(_checkpointer)


def stop() -> None:
    """FastAPI shutdown에서 호출. 커넥션 정리."""
    global _checkpointer_cm, _checkpointer, approval_app

    if _checkpointer_cm is not None:
        _checkpointer_cm.__exit__(None, None, None)
    _checkpointer_cm = None
    _checkpointer = None
    approval_app = None


def list_pending_approvals() -> list[dict]:
    """
    checkpointer에 저장된 모든 thread를 훑어서, approval_gate의 interrupt()에서
    멈춰있는 thread만 골라 반환한다. thread_id별로 가장 최근 체크포인트만 본다.
    """
    if approval_app is None:
        raise RuntimeError("graph_runtime.start()가 먼저 호출돼야 함")

    seen_threads: set[str] = set()
    pending: list[dict] = []

    # .list()가 반환하는 이터레이터는 커넥션의 커서를 열어둔 채로 유지되므로,
    # 다 소진하기 전에 같은 커넥션으로 get_state()를 또 호출하면(중첩 커서) 멈춘다.
    # 그래서 먼저 list()로 통째로 뽑아 이터레이터를 닫아버린 다음에 순회해야 한다.
    all_checkpoints = list(approval_app.checkpointer.list(None))

    for checkpoint_tuple in all_checkpoints:
        thread_id = checkpoint_tuple.config["configurable"]["thread_id"]
        if thread_id in seen_threads:
            continue
        seen_threads.add(thread_id)

        snapshot = approval_app.get_state({"configurable": {"thread_id": thread_id}})
        if snapshot.interrupts:
            pending.append(
                {
                    "thread_id": thread_id,
                    "interrupt": snapshot.interrupts[0].value,
                    "values": snapshot.values,
                    "created_at": snapshot.created_at,
                }
            )

    return pending
