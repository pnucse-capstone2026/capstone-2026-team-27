"""
playground/seed_fake_action_scenario.py

⚠️ 대시보드 데모/테스트 전용 — 실제 AWS 호출도, 실제 LLM 호출도 하지 않는다.

detection → classification → decision → action → qa 전체 그래프를 돌리지 않고,
"액션까지 전부 성공한 것처럼" 이미 완성된 state를 직접 만들어서 logging_node()에만
넘긴다. action_log에는 실제로 action_executed가 채워진 실행만 기록되는데(risk_level=LOW로
승인 없이 바로 실행된 경우), 지금까지 쌓인 실제/테스트 실행은 전부 MED/HIGH(승인 대기)라
action_log가 비어있어서 "시나리오별 비용 절감액" 등 action_log 기반 패널이 비어보임 —
그걸 확인해보기 위한 가짜 데이터 1건을 심는다.

[실행 방법]
  프로젝트 루트에서: python playground/seed_fake_action_scenario.py
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

resource = {
    "resource_id": "i-0demo-zombie-scenario",
    "resource_type": "EC2",
    "raw_metrics": {
        # 좀비 리소스: CPU/네트워크는 거의 안 쓰는데 cost는 계속 나가는 패턴
        "cpu_utilization": [1.0] * 30,
        "network_in":      [5.0] * 30,
        "network_out":     [5.0] * 30,
        "cost":            [0.013] * 30,  # t3.micro 시간당 단가 그대로 계속 나감
    },
}

state = _build_initial_state(resource)
state["timestamp"] = datetime.now(timezone.utc).isoformat()

# ── detection (가짜) ──────────────────────────────────────────────────────────
state["anomaly_flag"] = True
state["anomaly_score_zscore"] = 0.0
state["anomaly_score_iforest"] = 0.87
state["triggered_metrics"] = []  # IForest 단독 탐지로 가정 (다변량 이상)

# ── classification (가짜) ─────────────────────────────────────────────────────
state["anomaly_type"] = "cost_inefficiency"
state["classification_reasoning"] = "[DEMO] CPU/네트워크 사용률이 거의 0인데 인스턴스가 계속 켜져 있어 비용만 발생 (좀비 리소스)."
state["interim_action_taken"] = None

# ── decision (가짜) ────────────────────────────────────────────────────────────
estimated_saving_usd = 0.013  # cost 전체를 절감(Stop)
state["candidate_actions"] = [
    {
        "action": "Stop",
        "saving_rate": 1.0,
        "impact_score": 0.0,
        "stability_score": 0.0,
        "score": 1.0,
        "estimated_saving_usd": estimated_saving_usd,
    },
    {
        "action": "NoAction",
        "saving_rate": 0.0,
        "impact_score": 0.0,
        "stability_score": 0.0,
        "score": 0.0,
        "estimated_saving_usd": 0.0,
    },
]
state["selected_action"] = "Stop"
state["risk_level"] = "LOW"           # LOW라야 승인 없이 바로 "실행"된 것으로 기록됨
state["requires_approval"] = False
state["decision_reasoning"] = (
    "[DEMO] 규칙 기반 선택: 'Stop' (risk=LOW, cost 0.0130 -> 0.0000 USD/hr, 절감액=0.0130/hr)"
)

# ── action (가짜 — 실제 boto3 호출 없음) ────────────────────────────────────────
state["pre_action_snapshot"] = {"instance_type": "t3.micro", "state": "running"}
state["action_executed"] = "Stop"
state["action_result"] = {"status": "success", "raw": []}  # 실제 EC2 stop_instances 응답 형태만 흉내

# ── qa (가짜) ──────────────────────────────────────────────────────────────────
state["qa_passed"] = True
state["sla_check_result"] = {"status": "ok"}
state["rollback_count"] = 0

result = logging_node(state)
print("action_executed:", result.get("action_executed"))
print("action_result:", result.get("action_result"))
print("log_entries:", result.get("log_entries"))
