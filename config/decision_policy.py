"""
Decision Policy
===============
관리자가 조정하는 설정값을 JSON 파일 하나로 영속화한다:
  - priority_weight (0~100): 가용성 ↔ 비용 절감 우선순위
  - llm_cost_limit ($/일): LLM 호출에 하루 최대 얼마까지 쓸지 상한

decision_agent.py 등은 별도 프로세스(파이프라인 워커)에서 돌고, admin API(FastAPI)는
또 다른 프로세스에서 돈다. 두 프로세스가 값을 공유하려면 메모리 변수로는 안 되고
DB나 파일 같은 공유 저장소가 필요하다. 이 프로젝트는 이미 schema/rules/*.json을
RuleEngine이 파일로 읽는 방식을 쓰고 있어서, 같은 패턴으로 파일을 택했다
"""

from __future__ import annotations

import json
import os

_POLICY_PATH = os.path.join(os.path.dirname(__file__), "decision_policy.json")

DEFAULT_PRIORITY_WEIGHT = 30

# README에 기록된 이 프로젝트 Gemini 키의 실측 무료 티어 한도(하루 20회, 공식 문서엔
# 계정별 배정이라 고정 수치가 없어 이 값이 유일한 근거)를, 실제 decision_agent
# 프롬프트 크기로 측정한 호출당 비용(2026-08 기준 input=964/output=1485 토큰,
# gemini-2.5-flash 단가 input $0.30/output $2.50 per 1M 토큰 → 호출당 약 $0.004)에
# 곱해서 산정했다: 0.004 * 20회 ≈ $0.08.
# 무료 티어 안에서는 실제로 $0 청구되지만, 유료로 전환됐을 때 "지금까지 정상 운영
# 범위였던 호출량"을 그대로 유지하는 감각의 기본값이다.
DEFAULT_LLM_COST_LIMIT_USD = 0.08


def _read_policy() -> dict:
    try:
        with open(_POLICY_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _write_policy(updates: dict) -> None:
    """기존 값은 유지하고 updates만 덮어써서 저장 (다른 키를 날리지 않도록)."""
    data = _read_policy()
    data.update(updates)
    with open(_POLICY_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_priority_weight() -> int:
    """0(가용성 최우선) ~ 100(비용 절감 최우선). 파일/키가 없으면 기본값."""
    value = _read_policy().get("priority_weight", DEFAULT_PRIORITY_WEIGHT)
    return max(0, min(100, int(value)))


def set_priority_weight(value: int) -> None:
    _write_policy({"priority_weight": max(0, min(100, int(value)))})


def get_llm_cost_limit() -> float:
    """하루 LLM 비용 상한 (USD). 파일/키가 없으면 기본값."""
    value = _read_policy().get("llm_cost_limit", DEFAULT_LLM_COST_LIMIT_USD)
    return max(0.0, float(value))


def set_llm_cost_limit(value: float) -> None:
    _write_policy({"llm_cost_limit": max(0.0, float(value))})
