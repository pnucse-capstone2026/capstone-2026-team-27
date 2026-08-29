from fastapi import APIRouter, HTTPException

from api import store
from api.schemas import WhitelistCreate

router = APIRouter(prefix="/whitelist", tags=["whitelist"])


@router.get("")
def get_whitelist():
    return store.whitelist


@router.post("")
def create_whitelist_entry(entry: WhitelistCreate):
    # TODO: schema/rules/whitelist.json (또는 DB)에 반영, RuleEngine.load_rules() 재로드
    new_entry = {
        "id": store.next_whitelist_id(),
        "created_at": store.now_iso(),
        **entry.model_dump(),
    }
    store.whitelist.append(new_entry)
    return new_entry


@router.delete("/{entry_id}")
def delete_whitelist_entry(entry_id: str):
    entry = next((w for w in store.whitelist if w["id"] == entry_id), None)
    if entry is None:
        raise HTTPException(status_code=404, detail="whitelist entry not found")
    store.whitelist.remove(entry)
    return {"id": entry_id, "status": "deleted"}
