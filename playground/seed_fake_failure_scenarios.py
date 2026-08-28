"""
playground/seed_fake_failure_scenarios.py

⚠️ 대시보드 데모/테스트 전용 — 실제 AWS 호출도, 실제 LLM 호출도 하지 않는다.
playground/seed_fake_action_scenario.py와 동일한 방식: 그래프를 돌리지 않고
"이미 실패로 끝난" state를 직접 만들어 logging_node()에만 넘긴다.

"자동 조치 실패 이력" 테이블(grafana/provisioning/dashboards/detection-detail.json,
id=22)의 "실패 원인" CASE 분기 2가지를 각각 재현한다:
  1) 실행 자체가 실패 (action_result.status == "failed") → error 메시지 그대로 표시
  2) 실행은 성공했지만 QA 실패로 롤백됨 (action_result.rolled_back == True)
     → "QA 실패로 롤백됨"으로 표시

[실행 방법]
  프로젝트 루트에서: python playground/seed_fake_failure_scenarios.py
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from datetime import datetime, timezone

from pipeline.detection_agent import _build_initial_state
from pipeline.logging_agent import logging_node


def _base_state(resource_id: str, resource_type: str, raw_metrics: dict) -> dict:
    state = _build_initial_state({
        "resource_id": resource_id,
        "resource_type": resource_type,
        "raw_metrics": raw_metrics,
    })
    state["timestamp"] = datetime.now(timezone.utc).isoformat()
    state["anomaly_flag"] = True
    state["anomaly_score_zscore"] = 0.0
    state["anomaly_score_iforest"] = 0.9
    state["triggered_metrics"] = []
    return state


# ── 시나리오 1: 실행 자체 실패 (예: 존재하지 않는 인스턴스) ─────────────────────
state1 = _base_state(
    "i-0demo-fail-scenario",
    "EC2",
    {
        "cpu_utilization": [1.0] * 30,
        "network_in":      [5.0] * 30,
        "network_out":     [5.0] * 30,
        "cost":            [0.013] * 30,
    },
)
state1["anomaly_type"] = "cost_inefficiency"
state1["classification_reasoning"] = "[DEMO] 좀비 리소스 패턴."
state1["candidate_actions"] = [
    {"action": "Stop", "saving_rate": 1.0, "impact_score": 0.0, "stability_score": 0.0,
     "score": 1.0, "estimated_saving_usd": 0.013},
]
state1["selected_action"] = "Stop"
state1["risk_level"] = "LOW"
state1["requires_approval"] = False
state1["decision_reasoning"] = "[DEMO] 규칙 기반 선택: 'Stop' (risk=LOW)"
state1["pre_action_snapshot"] = None  # 스냅샷 조회 단계에서부터 실패했다고 가정
state1["action_executed"] = "Stop"
state1["action_result"] = {
    "status": "failed",
    "error": "An error occurred (InvalidInstanceID.NotFound) when calling the "
             "StopInstances operation: The instance ID 'i-0demo-fail-scenario' does not exist",
}
state1["qa_passed"] = False
state1["sla_check_result"] = {"status": "skipped", "reason": "action failed before execution"}
state1["rollback_count"] = 0

result1 = logging_node(state1)
print("[시나리오 1: 실행 실패]")
print("  action_result:", result1.get("action_result"))


# ── 시나리오 2: 실행은 성공했지만 QA 실패로 롤백 ─────────────────────────────
state2 = _base_state(
    "i-0demo-rollback-scenario",
    "Lambda",
    {
        "invocation_count": [50000.0] * 30,
        "error_count":      [200.0] * 30,
        "duration_avg":     [3000.0] * 30,
        "cost":             [20.0] * 30,
    },
)
state2["anomaly_type"] = "cost_spike"
state2["classification_reasoning"] = "[DEMO] invocation_count 급증 패턴."
state2["candidate_actions"] = [
    {"action": "Throttle", "saving_rate": 0.6, "impact_score": 0.0, "stability_score": 0.0,
     "score": 0.6, "estimated_saving_usd": 0.45},
]
state2["selected_action"] = "Throttle"
state2["risk_level"] = "LOW"
state2["requires_approval"] = False
state2["decision_reasoning"] = "[DEMO] 규칙 기반 선택: 'Throttle' (risk=LOW)"
state2["pre_action_snapshot"] = {"reserved_concurrency": -1}
state2["action_executed"] = "Throttle"
state2["action_result"] = {
    "status": "success",
    "reserved_concurrency": 10,
    "rolled_back": True,
    "rollback_reason": "SLA 체크 실패 — Throttle 이후 오류율이 오히려 상승",
}
state2["qa_passed"] = False
state2["sla_check_result"] = {"status": "fail", "reason": "error_rate 상승 감지"}
state2["rollback_count"] = 1

result2 = logging_node(state2)
print("[시나리오 2: QA 실패 후 롤백]")
print("  action_result:", result2.get("action_result"))
