"""
Whitelist 관리 API - 실제 파일 연동
"""
import json
import os
from datetime import datetime, timezone
from fastapi import APIRouter, HTTPException

from api.schemas import WhitelistCreate
from pipeline.rule_engine import reload_rules

router = APIRouter(prefix="/whitelist", tags=["whitelist"])

# 파일 경로
WHITELIST_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "schema", "rules", "whitelist.json"
)


def _load_whitelist() -> list[dict]:
    """화이트리스트 파일 로드."""
    if not os.path.exists(WHITELIST_PATH):
        return []
    with open(WHITELIST_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _save_whitelist(entries: list[dict]) -> None:
    """화이트리스트 파일 저장."""
    with open(WHITELIST_PATH, "w", encoding="utf-8") as f:
        json.dump(entries, f, ensure_ascii=False, indent=2)


def _get_next_entry_id(entries: list[dict]) -> str:
    """다음 엔트리 ID 생성."""
    max_num = 0
    for entry in entries:
        entry_id = entry.get("entry_id", "")
        if entry_id.startswith("WL-"):
            try:
                num = int(entry_id.replace("WL-", ""))
                max_num = max(max_num, num)
            except ValueError:
                continue
    return f"WL-{max_num + 1:03d}"


@router.get("")
def get_whitelist():
    """화이트리스트 전체 조회."""
    return _load_whitelist()


@router.post("")
def create_whitelist_entry(entry: WhitelistCreate):
    """화이트리스트 엔트리 생성."""
    entries = _load_whitelist()
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    new_entry = {
        "entry_id": _get_next_entry_id(entries),
        "created_at": now_iso,
        "created_by": "admin",
        **entry.model_dump(),
    }
    entries.append(new_entry)
    _save_whitelist(entries)
    reload_rules()

    return new_entry


@router.delete("/{entry_id}")
def delete_whitelist_entry(entry_id: str):
    """화이트리스트 엔트리 삭제."""
    entries = _load_whitelist()
    entry = next((e for e in entries if e.get("entry_id") == entry_id), None)

    if entry is None:
        raise HTTPException(status_code=404, detail="Whitelist entry not found")

    entries.remove(entry)
    _save_whitelist(entries)
    reload_rules()

    return {"entry_id": entry_id, "status": "deleted"}


@router.patch("/{entry_id}")
def update_whitelist_entry(entry_id: str, update: WhitelistCreate):
    """화이트리스트 엔트리 수정."""
    entries = _load_whitelist()
    entry = next((e for e in entries if e.get("entry_id") == entry_id), None)

    if entry is None:
        raise HTTPException(status_code=404, detail="Whitelist entry not found")

    # 업데이트
    entry.update(update.model_dump())
    _save_whitelist(entries)
    reload_rules()

    return entry
