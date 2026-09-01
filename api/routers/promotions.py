"""
승인 대기 중인 Rule 승격 후보 관리 API
"""
from fastapi import APIRouter, HTTPException

from pipeline.rule_promoter import (
    load_pending_promotions,
    approve_pending_rule,
    reject_pending_rule,
)
from pipeline.decision_pseudocode_promoter import (
    approve_pending_decision_rule,
    reject_pending_decision_rule,
)
from pipeline.rule_engine import reload_rules

router = APIRouter(prefix="/promotions", tags=["promotions"])


@router.get("")
def get_pending_promotions():
    """승인 대기 중인 모든 규칙 승격 후보 조회."""
    pending = load_pending_promotions()
    return {
        "classification": pending.get("classification", []),
        "decision": pending.get("decision", []),
        "total": len(pending.get("classification", [])) + len(pending.get("decision", [])),
    }


@router.post("/{pending_id}/approve")
def approve_promotion(pending_id: str):
    """승인 대기 중인 규칙을 승인하여 Rule Book에 추가."""
    # Classification 규칙인지 Decision 규칙인지 판단
    if pending_id.startswith("pending-clf-"):
        result = approve_pending_rule(pending_id)
        rule_type = "classification"
    elif pending_id.startswith("pending-dec-"):
        result = approve_pending_decision_rule(pending_id)
        rule_type = "decision"
    else:
        raise HTTPException(status_code=400, detail="Invalid pending_id format")

    if result is None:
        raise HTTPException(status_code=404, detail="Pending promotion not found")

    # RuleEngine 리로드
    reload_rules()

    return {
        "status": "approved",
        "rule_type": rule_type,
        "rule": result,
    }


@router.post("/{pending_id}/reject")
def reject_promotion(pending_id: str):
    """승인 대기 중인 규칙을 거부 (대기 큐에서 제거)."""
    # Classification 규칙인지 Decision 규칙인지 판단
    if pending_id.startswith("pending-clf-"):
        success = reject_pending_rule(pending_id)
        rule_type = "classification"
    elif pending_id.startswith("pending-dec-"):
        success = reject_pending_decision_rule(pending_id)
        rule_type = "decision"
    else:
        raise HTTPException(status_code=400, detail="Invalid pending_id format")

    if not success:
        raise HTTPException(status_code=404, detail="Pending promotion not found")

    return {
        "status": "rejected",
        "rule_type": rule_type,
        "pending_id": pending_id,
    }
