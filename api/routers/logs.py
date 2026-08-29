"""
LLM 판단 로그 — 두 agent가 각자 기록하는 JSONL 파일을 합쳐서 보여준다.
  - schema/logs/llm_classification_log.jsonl : classification_agent.py
    (이상 유형이 뭔지 판단 — pseudo_code 없음)
  - schema/logs/llm_decision_log.jsonl       : decision_agent.py
    (어떤 액션을 선택할지 판단 — pseudo_code 있음)

두 파일 사이엔 같은 파이프라인 실행임을 보장하는 공식 키(run_id)가 없다. 
그래서 같은 resource_id + 시간이 가까운 것(GROUPING_WINDOW 이내)을 같은 실행으로 간주해서
하나의 카드로 묶는다 (근사치 매칭). 
실제 파이프라인은 분류→결정이 같은 실행 안에서 몇 초~1분 안에 일어나므로 이 정도 창이면 충분하고, 
decision 로깅 기능이 없던 과거 로그는 
자연스럽게 짝 없이 classification 단독 카드로 남는다.

"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta

from fastapi import APIRouter

router = APIRouter(prefix="/logs", tags=["logs"])

_LOGS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "schema", "logs")
_CLASSIFICATION_LOG_PATH = os.path.join(_LOGS_DIR, "llm_classification_log.jsonl")
_DECISION_LOG_PATH = os.path.join(_LOGS_DIR, "llm_decision_log.jsonl")

# classification과 decision 로그를 "같은 실행"으로 묶어줄 최대 시간 간격
GROUPING_WINDOW = timedelta(minutes=10)


def _read_jsonl(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []

    entries = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return entries


def _parse_ts(timestamp: str) -> datetime:
    return datetime.fromisoformat(timestamp.replace("Z", "+00:00"))


def _classification_summary(entry: dict) -> dict:
    output = entry.get("output", {})
    return {
        "id": entry.get("trace_id"),
        "timestamp": entry.get("logged_at"),
        "source": "rule" if entry.get("matched_rule_id") else "llm",
        "reasoning": output.get("reasoning"),
    }


def _decision_summary(entry: dict) -> dict:
    output = entry.get("output", {})
    return {
        "id": entry.get("trace_id"),
        "timestamp": entry.get("logged_at"),
        "source": "llm" if output.get("used_llm") else "rule",
        "reasoning": output.get("reason"),
        "pseudo_code": output.get("pseudo_code") or "",
    }


@router.get("")
def get_logs():
    classification_entries = _read_jsonl(_CLASSIFICATION_LOG_PATH)
    decision_entries = _read_jsonl(_DECISION_LOG_PATH)

    # resource_id별로 decision 후보들을 시간순 정렬해서 준비 (가까운 것부터 매칭)
    decisions_by_resource: dict[str, list[dict]] = {}
    for d in decision_entries:
        resource_id = d.get("input", {}).get("resource_id")
        decisions_by_resource.setdefault(resource_id, []).append(d)
    for group in decisions_by_resource.values():
        group.sort(key=lambda d: _parse_ts(d["logged_at"]))

    used_decision_ids: set[str] = set()
    items: list[dict] = []

    for c in classification_entries:
        c_input = c.get("input", {})
        resource_id = c_input.get("resource_id")
        c_time = _parse_ts(c["logged_at"])

        match = None
        for d in decisions_by_resource.get(resource_id, []):
            if d.get("trace_id") in used_decision_ids:
                continue
            d_time = _parse_ts(d["logged_at"])
            if c_time <= d_time <= c_time + GROUPING_WINDOW:
                match = d
                break

        if match:
            used_decision_ids.add(match["trace_id"])
            items.append(
                {
                    "id": f"{c['trace_id']}+{match['trace_id']}",
                    "grouped": True,
                    "resource_type": c_input.get("resource_type"),
                    "resource_id": resource_id,
                    "timestamp": match["logged_at"],  # 더 늦게 끝난 시점 기준으로 정렬
                    "classification": _classification_summary(c),
                    "decision": _decision_summary(match),
                }
            )
        else:
            items.append(
                {
                    "id": c["trace_id"],
                    "grouped": False,
                    "stage": "classification",
                    "resource_type": c_input.get("resource_type"),
                    "resource_id": resource_id,
                    "timestamp": c["logged_at"],
                    "source": _classification_summary(c)["source"],
                    "reasoning": _classification_summary(c)["reasoning"],
                    "pseudo_code": "",
                }
            )

    # 짝 못 찾은 decision 로그는 단독 카드로
    for d in decision_entries:
        if d.get("trace_id") in used_decision_ids:
            continue
        d_input = d.get("input", {})
        summary = _decision_summary(d)
        items.append(
            {
                "id": d["trace_id"],
                "grouped": False,
                "stage": "decision",
                "resource_type": d_input.get("resource_type"),
                "resource_id": d_input.get("resource_id"),
                "timestamp": d["logged_at"],
                "source": summary["source"],
                "reasoning": summary["reasoning"],
                "pseudo_code": summary["pseudo_code"],
            }
        )

    return sorted(items, key=lambda item: item["timestamp"], reverse=True)
