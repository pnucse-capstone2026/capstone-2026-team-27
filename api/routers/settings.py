from pathlib import Path

import yaml
from fastapi import APIRouter, HTTPException

from api import pipeline_process, store
from api.schemas import SettingsUpdate
from config import decision_policy
from utils.llm_usage_tracker import get_today_spent_usd

router = APIRouter(prefix="/settings", tags=["settings"])

_EXPORT_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "decision_policy_export.yaml"


def _sync_real_values() -> None:
    # priority_weight/llm_cost_limit/polling_interval/resources는 모두
    # config/decision_policy.json이 실제 출처 (decision_agent/utils.llm_utils,
    # run_scheduler.py, playground/run_full_pipeline.py가 이걸 읽음).
    store.settings_state["priority_weight"] = decision_policy.get_priority_weight()
    store.settings_state["llm_cost_limit"] = decision_policy.get_llm_cost_limit()
    store.settings_state["llm_cost_spent_today"] = round(get_today_spent_usd(), 4)
    store.settings_state["polling_interval"] = decision_policy.get_polling_interval_minutes()
    store.settings_state["resources"] = decision_policy.get_resources()
    # 파이프라인이 실행 중이면 웹에서 설정을 못 바꾸게 잠가야 하므로(실행 중 변경은
    # "지금 도는 사이클"과 "다음 사이클"이 서로 다른 설정을 쓰게 되는 혼선을 만든다),
    # 프론트가 폼을 비활성화할 수 있도록 실행 상태도 같이 내려준다.
    store.settings_state["pipeline_running"] = pipeline_process.is_running()


@router.get("")
def get_settings():
    _sync_real_values()
    return store.settings_state


@router.patch("")
def update_settings(update: SettingsUpdate):
    if pipeline_process.is_running():
        raise HTTPException(
            status_code=409,
            detail="파이프라인 실행 중에는 설정을 변경할 수 없습니다. 먼저 종료하세요.",
        )

    data = update.model_dump(exclude_unset=True)

    if "priority_weight" in data and data["priority_weight"] is not None:
        decision_policy.set_priority_weight(data.pop("priority_weight"))

    if "llm_cost_limit" in data and data["llm_cost_limit"] is not None:
        decision_policy.set_llm_cost_limit(data.pop("llm_cost_limit"))

    if "polling_interval" in data and data["polling_interval"] is not None:
        decision_policy.set_polling_interval_minutes(data.pop("polling_interval"))

    if "resources" in data and data["resources"] is not None:
        decision_policy.set_resources(data.pop("resources"))

    store.settings_state.update(data)
    _sync_real_values()
    return store.settings_state


@router.post("/export")
def export_settings_yaml():
    """"저장" 버튼 - 현재 설정값(config/decision_policy.json의 실제 값)을
    YAML 스냅샷으로 남긴다. decision_policy.json 자체가 이미 정본(source of truth)이라
    이 파일은 그걸 그대로 복제한 배포/보관용 스냅샷일 뿐이다."""
    _sync_real_values()
    payload = {
        "priority_weight": store.settings_state["priority_weight"],
        "polling_interval": store.settings_state["polling_interval"],
        "llm_cost_limit": store.settings_state["llm_cost_limit"],
        "resources": store.settings_state["resources"],
    }
    with open(_EXPORT_PATH, "w", encoding="utf-8") as f:
        yaml.safe_dump(payload, f, allow_unicode=True, sort_keys=False)
    return {"path": str(_EXPORT_PATH), "content": payload}
