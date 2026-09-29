"""
승인 대기 큐 — 실제 LangGraph checkpointer(Postgres)에 저장된,
approval_gate에서 interrupt()로 멈춰있는 thread들을 조회/재개한다.

thread_id를 그대로 프론트가 쓰는 "id" 필드로 노출한다.

승인 요청에서 action → QA(300초 재검증) → logging까지 동기로 실행해
응답이 5분 이상 블로킹되고 브라우저 타임아웃 및 승인 큐 재등장 문제가 발생했다.
따라서 승인(interrupt 해제)만 즉시 처리해 응답하고, 실제 실행(action~logging)은
백그라운드 스레드에서 별도의 Postgres 커넥션과 그래프 인스턴스로 실행한다.
기존 approval_app의 커넥션을 공유하면 GET /queue 폴링과
동시 접근 시 psycopg 스레드 안정성 문제가 발생할 수 있으므로,
실행 시마다 새 커넥션을 생성한다.
"""

from __future__ import annotations

import logging
import threading

from fastapi import APIRouter, HTTPException
from langgraph.types import Command

from api import graph_runtime
from pipeline.checkpointer import get_postgres_checkpointer
from pipeline.graph import build_approval_graph

router = APIRouter(prefix="/queue", tags=["approvals"])
logger = logging.getLogger("api.approvals")


def _to_queue_item(pending: dict) -> dict:
    interrupt = pending["interrupt"]
    selected_action = interrupt.get("selected_action")

    estimated_saving = 0.0
    for candidate in interrupt.get("candidate_actions") or []:
        if candidate.get("action") == selected_action:
            estimated_saving = candidate.get("estimated_saving_usd", 0.0)
            break

    return {
        "id": pending["thread_id"],
        "severity": interrupt.get("risk_level"),
        "action": selected_action,
        "resource_type": interrupt.get("resource_type"),
        "resource_id": interrupt.get("resource_id"),
        "timestamp": pending["created_at"],
        "reason": interrupt.get("decision_reasoning"),
        "estimated_saving": estimated_saving,
        "pseudo_code": interrupt.get("decision_pseudo_code") or "",
    }


@router.get("")
def get_queue():
    return [_to_queue_item(p) for p in graph_runtime.list_pending_approvals()]


def _run_resume_in_background(thread_id: str, approved: bool) -> None:
    """action~QA(300초 실측 대기)~logging을 API 요청과 분리된 스레드에서 실행.
    graph_runtime.approval_app의 커넥션을 건드리지 않도록 이 스레드 전용의
    새 checkpointer/그래프 인스턴스를 연다."""
    config = {"configurable": {"thread_id": thread_id}}
    try:
        with get_postgres_checkpointer() as checkpointer:
            checkpointer.setup()
            app = build_approval_graph(checkpointer)
            app.invoke(Command(resume={"approved": approved}), config)
    except Exception:
        logger.exception("[approvals] 백그라운드 재개 실패 (thread_id=%s)", thread_id)
    finally:
        # 반드시 풀어줘야 실패한 케이스도 재시도(다시 승인 클릭) 가능해진다.
        graph_runtime.unmark_resuming(thread_id)


def _resume(thread_id: str, approved: bool) -> dict:
    # 이미 처리 중인 thread_id면(체크포인트가 아직 interrupt 해제로 안 보여도)
    # 중복으로 재개 스레드를 또 띄우지 않고 그대로 "처리 중" 응답만 준다 —
    # 중복 클릭/폴링 재등장으로 같은 thread에 재개가 2번 겹쳐 뜨던 문제 방지.
    if not graph_runtime.mark_resuming(thread_id):
        return {
            "id": thread_id,
            "status": "approved" if approved else "rejected",
            "processing": True,
        }

    # graph_runtime.approval_app.get_state()를 직접 부르지 않고 락으로 보호된
    # 헬퍼를 거친다 — 안 그러면 이 호출이 동시에 들어온 GET /status(마찬가지로
    # 이 커넥션을 쓰는 list_pending_approvals)와 충돌해 응답이 안 온다.
    snapshot = graph_runtime.get_state(thread_id)
    if not snapshot.interrupts:
        graph_runtime.unmark_resuming(thread_id)
        raise HTTPException(status_code=404, detail="승인 대기 중인 thread가 아님")

    thread = threading.Thread(
        target=_run_resume_in_background, args=(thread_id, approved), daemon=True
    )
    thread.start()

    return {
        "id": thread_id,
        "status": "approved" if approved else "rejected",
        "processing": True,
    }


@router.post("/{item_id}/approve")
def approve_queue_item(item_id: str):
    return _resume(item_id, approved=True)


@router.post("/{item_id}/reject")
def reject_queue_item(item_id: str):
    return _resume(item_id, approved=False)
