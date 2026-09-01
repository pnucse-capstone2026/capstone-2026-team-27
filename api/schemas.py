"""API 요청 바디 스키마 (Pydantic)"""

from __future__ import annotations

from typing import Any
from pydantic import BaseModel


class RuleCreate(BaseModel):
    """규칙 생성 요청 스키마 (Classification/Decision 공통)."""
    description: str
    resource_types: list[str]
    conditions: dict[str, Any]
    result: dict[str, Any]
    priority: int = 50
    rationale: str = ""


class WhitelistCreate(BaseModel):
    """화이트리스트 엔트리 생성 요청 스키마."""
    resource_id: str  # glob 패턴 (예: "i-0abc123*", "dev-*")
    resource_type: str | None = None  # null이면 모든 타입
    reason: str = ""
    expires_at: str | None = None  # ISO 형식, null이면 무기한


class SettingsUpdate(BaseModel):
    """설정 업데이트 요청 스키마."""
    priority_weight: int | None = None
    polling_interval: int | None = None
    llm_cost_limit: float | None = None
    resources: dict[str, bool] | None = None
