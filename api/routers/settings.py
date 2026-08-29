from fastapi import APIRouter

from api import store
from api.schemas import SettingsUpdate
from config import decision_policy
from utils.llm_usage_tracker import get_today_spent_usd

router = APIRouter(prefix="/settings", tags=["settings"])


def _sync_real_values() -> None:
    # priority_weight/llm_cost_limit은 config/decision_policy.json이 실제 출처
    # (decision_agent/utils.llm_utils가 이걸 읽음). polling_interval/resources는 아직 mock.
    store.settings_state["priority_weight"] = decision_policy.get_priority_weight()
    store.settings_state["llm_cost_limit"] = decision_policy.get_llm_cost_limit()
    store.settings_state["llm_cost_spent_today"] = round(get_today_spent_usd(), 4)


@router.get("")
def get_settings():
    _sync_real_values()
    return store.settings_state


@router.patch("")
def update_settings(update: SettingsUpdate):
    # TODO: polling_interval / resources는 아직 실제 반영 지점이 없음
    data = update.model_dump(exclude_unset=True)

    if "priority_weight" in data and data["priority_weight"] is not None:
        decision_policy.set_priority_weight(data.pop("priority_weight"))

    if "llm_cost_limit" in data and data["llm_cost_limit"] is not None:
        decision_policy.set_llm_cost_limit(data.pop("llm_cost_limit"))

    if "resources" in data and data["resources"] is not None:
        store.settings_state["resources"].update(data.pop("resources"))

    store.settings_state.update(data)
    _sync_real_values()
    return store.settings_state
