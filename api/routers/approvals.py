"""
승인 대기 큐 — 실제 LangGraph checkpointer(Postgres)에 저장된, 
approval_gate에서 interrupt()로 멈춰있는 thread들을 조회/재개한다.

thread_id를 그대로 프론트가 쓰는 "id" 필드로 노출한다.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from langgraph.types import Command

from api import graph_runtime

router = APIRouter(prefix="/queue", tags=["approvals"])


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


def _resume(thread_id: str, approved: bool) -> dict:
    config = {"configurable": {"thread_id": thread_id}}

    snapshot = graph_runtime.approval_app.get_state(config)
    if not snapshot.interrupts:
        raise HTTPException(status_code=404, detail="승인 대기 중인 thread가 아님")

    result = graph_runtime.approval_app.invoke(Command(resume={"approved": approved}), config)
    return {
        "id": thread_id,
        "status": "approved" if approved else "rejected",
        "action_executed": result.get("action_executed"),
        "action_result": result.get("action_result"),
    }


@router.post("/{item_id}/approve")
def approve_queue_item(item_id: str):
    return _resume(item_id, approved=True)


@router.post("/{item_id}/reject")
def reject_queue_item(item_id: str):
    return _resume(item_id, approved=False)
