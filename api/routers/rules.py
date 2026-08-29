from fastapi import APIRouter, HTTPException

from api import store
from api.schemas import RuleCreate

router = APIRouter(prefix="/rules", tags=["rules"])


@router.get("")
def get_rules():
    return store.rule_book


@router.post("")
def create_rule(rule: RuleCreate):
    # TODO: schema/rules/classification_rules.json (또는 DB)에 실제로 반영하고
    #       RuleEngine.load_rules()가 재로드하도록 연결
    new_rule = {"id": store.next_rule_id(), **rule.model_dump()}
    store.rule_book.append(new_rule)
    return new_rule


@router.delete("/{rule_id}")
def delete_rule(rule_id: str):
    rule = next((r for r in store.rule_book if r["id"] == rule_id), None)
    if rule is None:
        raise HTTPException(status_code=404, detail="rule not found")
    store.rule_book.remove(rule)
    return {"id": rule_id, "status": "deleted"}


@router.patch("/{rule_id}/toggle")
def toggle_rule(rule_id: str):
    rule = next((r for r in store.rule_book if r["id"] == rule_id), None)
    if rule is None:
        raise HTTPException(status_code=404, detail="rule not found")
    rule["enabled"] = not rule["enabled"]
    return rule
