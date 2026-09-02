"""
Pipeline Live Status
====================

run_full_pipeline.py가 LangGraph app.stream()으로 
노드가 끝날 때마다 이 파일을 갱신하고, 
api/routers/status.py가 이 파일이 최근에 갱신됐다면 실시간 값을, 
오래됐으면(프로세스가 안 돌고 있다면) 과거 실행 기록 기반 추정값을 대신 쓴다.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone

_STATUS_PATH = os.path.join(os.path.dirname(__file__), "pipeline_live_status.json")

# 이 시간(초)보다 오래 안 갱신됐으면 "더 이상 실시간 정보가 아니다"로 간주한다.
# 노드 하나 실행에 LLM 호출 포함해도 보통 몇 초 안쪽이라, 30초면 충분히 여유 있다.
FRESHNESS_SECONDS = 30

STEP_NAMES = ["detection", "classification", "decision", "action", "qa", "logging"]


def initial_nodes() -> dict:
    """새 리소스 처리를 시작할 때 쓸 초기 상태 — detection만 running, 나머진 idle."""
    nodes = {name: "idle" for name in STEP_NAMES}
    nodes["detection"] = "running"
    return nodes


def write(nodes: dict, resource_id: str | None, resource_type: str | None) -> None:
    payload = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "resource_id": resource_id,
        "resource_type": resource_type,
        "nodes": nodes,
    }
    try:
        with open(_STATUS_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
    except OSError:
        pass  # 상태 표시는 부가 기능 — 파일 쓰기 실패로 파이프라인 자체가 죽으면 안 됨


def clear() -> None:
    """루프 사이클 사이 대기 중일 때 등, "지금 아무것도 안 돈다"를 명시적으로 남기고 싶을 때."""
    write({name: "idle" for name in STEP_NAMES}, None, None)


def read_if_fresh() -> dict | None:
    """파일이 있고 FRESHNESS_SECONDS 이내에 갱신됐으면 payload 반환, 아니면 None."""
    try:
        with open(_STATUS_PATH, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None

    try:
        updated_at = datetime.fromisoformat(payload["updated_at"])
    except (KeyError, ValueError):
        return None

    age_seconds = (datetime.now(timezone.utc) - updated_at).total_seconds()
    if age_seconds > FRESHNESS_SECONDS:
        return None

    return payload
