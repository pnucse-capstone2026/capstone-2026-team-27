"""
Decision Policy
===============
관리자가 조정하는 설정값을 JSON 파일 하나로 영속화한다:
  - priority_weight (0~100): 가용성 ↔ 비용 절감 우선순위
  - llm_cost_limit ($/일): LLM 호출에 하루 최대 얼마까지 쓸지 상한

decision_agent.py 등은 별도 프로세스(파이프라인 워커)에서 돌고, 
admin API(FastAPI)는 또 다른 프로세스에서 돈다. 
두 프로세스가 값을 공유하려면 메모리 변수로는 안 되고
DB나 파일 같은 공유 저장소가 필요하다. 
이 프로젝트는 이미 schema/rules/*.json을 RuleEngine이 파일로 읽는 방식을 쓰고 있어서, 
같은 패턴으로 파일을 택했다
"""

from __future__ import annotations

import json
import os

_POLICY_PATH = os.path.join(os.path.dirname(__file__), "decision_policy.json")

DEFAULT_PRIORITY_WEIGHT = 30

DEFAULT_POLLING_INTERVAL_MINUTES = 5

# Detection Agent 담당자 요청: 5분보다 짧게 폴링하면 탐지 모델 학습(윈도우/베이스라인
# 계산)에 영향을 줘서, 값이 뭐가 들어오든 서버에서 무조건 5 밑으로는 안 내려가게 막는다.
MIN_POLLING_INTERVAL_MINUTES = 5

DEFAULT_RESOURCES = {
    "EC2": True,
    "Lambda": True,
    "S3": False,
    "RDS": True,
    "AutoScaling": True,
}

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


def get_polling_interval_minutes() -> int:
    """탐지 사이클 폴링 주기 (분). 파일/키가 없으면 기본값. 최소 5분 보장."""
    value = _read_policy().get("polling_interval", DEFAULT_POLLING_INTERVAL_MINUTES)
    return max(MIN_POLLING_INTERVAL_MINUTES, int(value))


def set_polling_interval_minutes(value: int) -> None:
    _write_policy({"polling_interval": max(MIN_POLLING_INTERVAL_MINUTES, int(value))})


def get_resources() -> dict[str, bool]:
    """리소스 타입별 탐지 활성화 여부. 파일/키가 없으면 기본값."""
    stored = _read_policy().get("resources", {})
    resources = dict(DEFAULT_RESOURCES)
    resources.update(stored)
    return resources


def set_resources(patch: dict[str, bool]) -> None:
    """주어진 리소스 타입만 갱신 (나머지는 기존 값 유지)."""
    resources = get_resources()
    resources.update(patch)
    _write_policy({"resources": resources})


def get_enabled_resource_types() -> list[str]:
    """활성화된(True) 리소스 타입만 리스트로. discover_all_resources()/
    run_detection_cycle()의 resource_types 인자로 바로 넘길 수 있는 형태."""
    return [rtype for rtype, enabled in get_resources().items() if enabled]
