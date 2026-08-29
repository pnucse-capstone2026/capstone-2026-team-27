"""
LLM 사용량(비용) 추적
=====================
call_gemini()을 부르는 모든 agent(classification/decision/QA)가 공통으로 거치는
곳에서, 오늘 하루 누적 비용을 파일에 기록하고 config/decision_policy.py의
llm_cost_limit과 비교한다.

여러 프로세스(파이프라인 워커 여러 개가 동시에 돌 수도 있음)가 같은 파일을
건드릴 수 있으니 값 하나짜리 JSON이라도 read-modify-write 사이 레이스가 있을 수
있다 — 이 프로젝트 스코프(캡스톤, 단일 워커 가정)에서는 무시할 수 있는 수준이라
파일 락은 걸지 않는다.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))

_USAGE_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "llm_usage.json")


def _today_str() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")


def _read_usage() -> dict:
    try:
        with open(_USAGE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"date": _today_str(), "spent_usd": 0.0}

    if data.get("date") != _today_str():
        return {"date": _today_str(), "spent_usd": 0.0}
    return data


def get_today_spent_usd() -> float:
    return _read_usage().get("spent_usd", 0.0)


def add_cost(usd: float) -> None:
    """오늘 누적 비용에 usd만큼 더해서 저장."""
    usage = _read_usage()
    usage["spent_usd"] = usage.get("spent_usd", 0.0) + max(0.0, usd)
    os.makedirs(os.path.dirname(_USAGE_PATH), exist_ok=True)
    with open(_USAGE_PATH, "w", encoding="utf-8") as f:
        json.dump(usage, f, ensure_ascii=False, indent=2)


def is_over_limit(limit_usd: float) -> bool:
    return get_today_spent_usd() >= limit_usd
