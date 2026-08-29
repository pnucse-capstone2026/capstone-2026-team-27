"""API 요청 바디 스키마 (Pydantic)"""

from __future__ import annotations

from pydantic import BaseModel


class RuleCreate(BaseModel):
    target: str
    condition: str
    result: str
    source: str = "human"
    enabled: bool = True


class WhitelistCreate(BaseModel):
    pattern: str
    resource_type: str | None = None
    reason: str = ""
    expires_at: str | None = None


class SettingsUpdate(BaseModel):
    priority_weight: int | None = None
    polling_interval: int | None = None
    llm_cost_limit: float | None = None
    resources: dict[str, bool] | None = None
