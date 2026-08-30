from fastapi import APIRouter

from api import store
from api.schemas import SettingsUpdate
from config import decision_policy
from utils.llm_usage_tracker import get_today_spent_usd

router = APIRouter(prefix="/settings", tags=["settings"])


def _sync_real_values() -> None:
    # priority_weight/llm_cost_limit/polling_interval/resources는 모두
    # config/decision_policy.json이 실제 출처 (decision_agent/utils.llm_utils,
    # run_scheduler.py, playground/run_full_pipeline.py가 이걸 읽음).
    store.settings_state["priority_weight"] = decision_policy.get_priority_weight()
    store.settings_state["llm_cost_limit"] = decision_policy.get_llm_cost_limit()
    store.settings_state["llm_cost_spent_today"] = round(get_today_spent_usd(), 4)
    store.settings_state["polling_interval"] = decision_policy.get_polling_interval_minutes()
    store.settings_state["resources"] = decision_policy.get_resources()


@router.get("")
def get_settings():
    _sync_real_values()
    return store.settings_state


@router.patch("")
def update_settings(update: SettingsUpdate):
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
