"""
Rule Book 관리 API - 실제 파일 연동
"""
import json
import os
from datetime import datetime, timezone
from fastapi import APIRouter, HTTPException

from api.schemas import RuleCreate
from pipeline.rule_engine import get_rule_engine, reload_rules

router = APIRouter(prefix="/rules", tags=["rules"])

# 파일 경로
RULES_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "schema", "rules")
CLASSIFICATION_RULES_PATH = os.path.join(RULES_DIR, "classification_rules.json")
DECISION_RULES_PATH = os.path.join(RULES_DIR, "decision_rules.json")


def _load_rules(path: str) -> list[dict]:
    """규칙 파일 로드."""
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _save_rules(path: str, rules: list[dict]) -> None:
    """규칙 파일 저장."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rules, f, ensure_ascii=False, indent=2)


def _get_next_rule_id(rules: list[dict], prefix: str) -> str:
    """다음 규칙 ID 생성."""
    max_num = 0
    for rule in rules:
        rule_id = rule.get("rule_id", "")
        if rule_id.startswith(prefix):
            try:
                num = int(rule_id.replace(f"{prefix}-", ""))
                max_num = max(max_num, num)
            except ValueError:
                continue
    return f"{prefix}-{max_num + 1:03d}"


@router.get("")
def get_rules():
    """모든 규칙 조회 (Classification + Decision)."""
    clf_rules = _load_rules(CLASSIFICATION_RULES_PATH)
    dec_rules = _load_rules(DECISION_RULES_PATH)
    return {
        "classification": clf_rules,
        "decision": dec_rules,
        "total": len(clf_rules) + len(dec_rules),
    }


@router.get("/classification")
def get_classification_rules():
    """Classification 규칙만 조회."""
    return _load_rules(CLASSIFICATION_RULES_PATH)


@router.get("/decision")
def get_decision_rules():
    """Decision 규칙만 조회."""
    return _load_rules(DECISION_RULES_PATH)


@router.post("/classification")
def create_classification_rule(rule: RuleCreate):
    """Classification 규칙 생성."""
    rules = _load_rules(CLASSIFICATION_RULES_PATH)
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    new_rule = {
        "rule_id": _get_next_rule_id(rules, "CLF"),
        "rule_type": "classification",
        "created_at": now_iso,
        "updated_at": now_iso,
        "author": "admin",
        "enabled": True,
        **rule.model_dump(),
    }
    rules.append(new_rule)
    _save_rules(CLASSIFICATION_RULES_PATH, rules)
    reload_rules()

    return new_rule


@router.post("/decision")
def create_decision_rule(rule: RuleCreate):
    """Decision 규칙 생성."""
    rules = _load_rules(DECISION_RULES_PATH)
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    new_rule = {
        "rule_id": _get_next_rule_id(rules, "DEC"),
        "rule_type": "decision",
        "created_at": now_iso,
        "updated_at": now_iso,
        "author": "admin",
        "enabled": True,
        **rule.model_dump(),
    }
    rules.append(new_rule)
    _save_rules(DECISION_RULES_PATH, rules)
    reload_rules()

    return new_rule


@router.delete("/{rule_id}")
def delete_rule(rule_id: str):
    """규칙 삭제."""
    # Classification 또는 Decision 판단
    if rule_id.startswith("CLF-"):
        path = CLASSIFICATION_RULES_PATH
    elif rule_id.startswith("DEC-"):
        path = DECISION_RULES_PATH
    else:
        raise HTTPException(status_code=400, detail="Invalid rule_id format")

    rules = _load_rules(path)
    rule = next((r for r in rules if r.get("rule_id") == rule_id), None)

    if rule is None:
        raise HTTPException(status_code=404, detail="Rule not found")

    rules.remove(rule)
    _save_rules(path, rules)
    reload_rules()

    return {"rule_id": rule_id, "status": "deleted"}


@router.patch("/{rule_id}/toggle")
def toggle_rule(rule_id: str):
    """규칙 활성화/비활성화 토글."""
    # Classification 또는 Decision 판단
    if rule_id.startswith("CLF-"):
        path = CLASSIFICATION_RULES_PATH
    elif rule_id.startswith("DEC-"):
        path = DECISION_RULES_PATH
    else:
        raise HTTPException(status_code=400, detail="Invalid rule_id format")

    rules = _load_rules(path)
    rule = next((r for r in rules if r.get("rule_id") == rule_id), None)

    if rule is None:
        raise HTTPException(status_code=404, detail="Rule not found")

    rule["enabled"] = not rule.get("enabled", True)
    rule["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    _save_rules(path, rules)
    reload_rules()

    return rule
