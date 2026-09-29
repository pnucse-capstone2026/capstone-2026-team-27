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

import threading

from pipeline.checkpointer import get_postgres_checkpointer
from pipeline.graph import build_approval_graph

_checkpointer_cm = None
_checkpointer = None
approval_app = None

# 승인/거부 클릭 → 체크포인트가 실제로 "더 이상 interrupt 아님"으로 갱신되기까지
# 짬이 있는데(백그라운드 스레드가 별도 커넥션으로 app.invoke()를 막 시작한 시점),
# 그 사이 GET /queue 폴링이 checkpointer를 통해 다시 읽으면 여전히 대기 중으로
# 보여서 큐에 재등장한다 — 사용자가 그걸 보고 다시 클릭하면 같은 thread_id에
# 재개 스레드가 중복으로 뜨는 문제까지 이어졌다(실측 확인, 2026-09-28).
# DB 반영 지연과 무관하게 즉시/정확하게 "지금 처리 중"을 알 수 있도록, 이
# API 프로세스 안에서 직접 추적한다.
_resuming_thread_ids: set[str] = set()
_resuming_lock = threading.Lock()

# approval_app이 물고 있는 psycopg2 커넥션 하나를 GET /status(list_pending_approvals),
# GET /queue, POST /queue/{id}/approve가 전부 공유해서 쓴다 — psycopg2 커넥션은
# 스레드 세이프하지 않은데, FastAPI의 sync 엔드포인트는 스레드풀에서 동시에 여러
# 개가 실행되므로 두 요청이 동시에 이 커넥션을 건드리면 무한 대기/충돌이 난다.
# 실측으로 확인된 증상: GET /status가 GET /queue와 겹치면 응답이 영영 안 오고
# 웹 대시보드가 "불러오는 중..."에서 멈춤(2026-09-28). 이 커넥션을 쓰는 모든
# 진입점이 아래 락 하나를 공유해서 서로 겹치지 않게 직렬화한다.
_connection_lock = threading.Lock()


def mark_resuming(thread_id: str) -> bool:
    """thread_id를 '지금 재개 처리 중'으로 표시. 이미 처리 중이었으면 False."""
    with _resuming_lock:
        if thread_id in _resuming_thread_ids:
            return False
        _resuming_thread_ids.add(thread_id)
        return True


def unmark_resuming(thread_id: str) -> None:
    """백그라운드 재개가 끝나면(성공/실패 무관) 반드시 호출 — 안 그러면 그
    thread_id가 영영 큐에 다시 안 뜬다."""
    with _resuming_lock:
        _resuming_thread_ids.discard(thread_id)


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


_LIST_CHECKPOINT_LIMIT = 300


def list_pending_approvals() -> list[dict]:
    """
    checkpointer에 저장된 최근 체크포인트를 훑어서, approval_gate의 interrupt()에서
    멈춰있는 thread만 골라 반환한다. thread_id별로 가장 최근 체크포인트만 본다.
    """
    if approval_app is None:
        raise RuntimeError("graph_runtime.start()가 먼저 호출돼야 함")

    seen_threads: set[str] = set()
    pending: list[dict] = []

    with _connection_lock:
        # .list()가 반환하는 이터레이터는 커넥션의 커서를 열어둔 채로 유지되므로,
        # 다 소진하기 전에 같은 커넥션으로 get_state()를 또 호출하면(중첩 커서) 멈춘다.
        # 그래서 먼저 list()로 통째로 뽑아 이터레이터를 닫아버린 다음에 순회해야 한다.
        #
        # limit 없이 전체 체크포인트(세션 내내 쌓여 1000개 넘게 감)를 매번 훑고
        # thread_id마다 get_state()까지 또 부르니 호출 한 번에 몇 초씩 걸렸다 —
        # 5초 폴링과 겹치면 요청이 쌓여서 웹 대시보드가 통째로 멈춰 보이는 원인이었다
        # (2026-09-28 실측 확인). checkpoint_id는 시간순 정렬되는 UUID라
        # ORDER BY checkpoint_id DESC로 최근 것부터 오므로, 지금 실제로 대기 중인
        # thread는 반드시 이 범위 안에 있다 — 아주 오래(이 한도를 넘길 만큼) 방치된
        # 승인 대기만 놓칠 수 있는데, 이 프로젝트 사용 패턴상 그런 경우는 없다.
        all_checkpoints = list(
            approval_app.checkpointer.list(None, limit=_LIST_CHECKPOINT_LIMIT)
        )

        for checkpoint_tuple in all_checkpoints:
            thread_id = checkpoint_tuple.config["configurable"]["thread_id"]
            if thread_id in seen_threads:
                continue
            seen_threads.add(thread_id)

            # 이미 승인/거부돼서 백그라운드로 처리 중인 건 체크포인트 반영을
            # 기다리지 않고 즉시 큐에서 제외한다(위 _resuming_thread_ids 주석 참고).
            if thread_id in _resuming_thread_ids:
                continue

            snapshot = approval_app.get_state(
                {"configurable": {"thread_id": thread_id}}
            )
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


def get_state(thread_id: str):
    """approval_app.get_state()를 락으로 보호해서 호출하는 헬퍼 — 위
    _connection_lock 주석 참고. 이 커넥션을 건드리는 곳은 전부 이 락을 거쳐야
    한다(직접 approval_app.get_state()를 호출하면 다른 동시 요청과 충돌할 수 있음)."""
    if approval_app is None:
        raise RuntimeError("graph_runtime.start()가 먼저 호출돼야 함")
    with _connection_lock:
        return approval_app.get_state({"configurable": {"thread_id": thread_id}})
