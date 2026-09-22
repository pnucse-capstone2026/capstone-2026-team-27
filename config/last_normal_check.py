from __future__ import annotations

import json
import os
from datetime import datetime, timezone

_STATUS_PATH = os.path.join(os.path.dirname(__file__), "last_normal_check.json")


def write() -> None:
    payload = {"checked_at": datetime.now(timezone.utc).isoformat()}
    try:
        with open(_STATUS_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
    except OSError:
        pass


def read() -> str | None:
    try:
        with open(_STATUS_PATH, "r", encoding="utf-8") as f:
            payload = json.load(f)
        return payload.get("checked_at")
    except (FileNotFoundError, json.JSONDecodeError):
        return None
