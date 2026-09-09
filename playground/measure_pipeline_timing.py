"""
playground/measure_pipeline_timing.py

파이프라인 전체(Detection -> Classification -> Decision -> Action -> QA -> Logging)를
실제 리소스 하나에 처음부터 끝까지 돌려서 단계별 + 총 소요시간을 실측한다.

⚠️ QA_agent.py에 액션 후 5분 대기+실측 재조회(POST_ACTION_WAIT_SECONDS)가 추가된
이후로는 "액션이 실제로 실행되는 케이스"의 전체 파이프라인 시간이 예전(초 단위)과
완전히 달라진다 — 이 스크립트가 그 실제 소요시간을 잰다.

⚠️ risk_level=HIGH인 액션(예: S3 Block)은 원래 requires_approval=True라서
action_node가 실제 실행을 안 하고 "pending_approval"로 멈춘다. 이러면 Action/QA
단계 시간을 잴 수가 없으므로, 이 스크립트는 **타이밍 측정 목적으로만** 승인
게이트를 우회한다(state["requires_approval"]=False로 직접 덮어씀 — action_agent.py나
approval_gate 자체를 바꾸는 게 아니라 이 스크립트의 로컬 state에서만). 실제 운영
파이프라인은 그대로 승인을 요구한다.

[실행 방법]
  python playground/measure_pipeline_timing.py --resource-id detection-trial-anomaly-0 --resource-type S3

[생성 파일]
  playground/eval_outputs/pipeline_timing_{날짜}.json
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

from pipeline.orchestrator import assemble_resource
from pipeline.detection_agent import detection_node
from pipeline.classification_agent import classification_node
from pipeline.decision_agent import decision_node
from pipeline.action_agent import action_node
from pipeline.QA_agent import qa_node
from pipeline.logging_agent import logging_node

RESULT_DIR = PROJECT_ROOT / "playground" / "eval_outputs"


def _build_initial_state(resource_id: str, resource_type: str) -> dict:
    assembled = assemble_resource(resource_id, resource_type)
    return {
        "trace_id": None,
        "resource_id": resource_id,
        "resource_type": resource_type,
        "raw_metrics": assembled["raw_metrics"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "resource_age_seconds": None,
        "anomaly_flag": False,
        "anomaly_score_zscore": None,
        "anomaly_score_iforest": None,
        "triggered_metrics": [],
        "anomaly_type": None,
        "classification_reasoning": None,
        "interim_action_taken": None,
        "matched_rule_id": None,
        "candidate_actions": [],
        "selected_action": None,
        "risk_level": None,
        "requires_approval": False,
        "decision_reasoning": None,
        "target_instance_type": None,
        "pre_action_snapshot": None,
        "action_executed": None,
        "action_result": None,
        "qa_passed": None,
        "sla_check_result": None,
        "rollback_count": 0,
        "qa_matched_rule_id": None,
        "whitelisted": False,
        "log_entries": [],
    }


def measure(resource_id: str, resource_type: str, bypass_approval_for_timing: bool) -> dict:
    timings: dict[str, float] = {}
    t_total_start = time.time()

    state = _build_initial_state(resource_id, resource_type)

    t0 = time.time()
    state = detection_node(state)
    timings["detection"] = time.time() - t0
    print(f"[detection] {timings['detection']:.3f}s -> anomaly_flag={state['anomaly_flag']}")

    if not state["anomaly_flag"]:
        timings["total"] = time.time() - t_total_start
        print("이상 없음(anomaly_flag=False) — 여기서 파이프라인 종료 (정상 판정 경로).")
        return {"resource_id": resource_id, "resource_type": resource_type,
                "anomaly_flag": False, "timings": timings}

    t0 = time.time()
    state = classification_node(state)
    timings["classification"] = time.time() - t0
    print(f"[classification] {timings['classification']:.3f}s -> anomaly_type={state['anomaly_type']}")

    t0 = time.time()
    state = decision_node(state)
    timings["decision"] = time.time() - t0
    print(f"[decision] {timings['decision']:.3f}s -> action={state['selected_action']}, "
          f"risk={state['risk_level']}, requires_approval={state['requires_approval']}")

    approval_bypassed = False
    if state["requires_approval"] and bypass_approval_for_timing:
        print("[!] requires_approval=True — 타이밍 측정 목적으로만 승인 게이트 우회함 "
              "(실제 운영에서는 여기서 사람 승인을 기다려야 함, 그 대기시간은 무한정이라 측정 불가)")
        state["requires_approval"] = False
        approval_bypassed = True
    elif state["requires_approval"]:
        timings["total"] = time.time() - t_total_start
        print("requires_approval=True — 승인 대기 상태. --bypass-approval 없이는 "
              "여기서 더 진행 안 함 (실제 운영과 동일한 동작).")
        return {"resource_id": resource_id, "resource_type": resource_type,
                "anomaly_flag": True, "selected_action": state["selected_action"],
                "stopped_at": "approval_gate", "timings": timings}

    t0 = time.time()
    state = action_node(state)
    timings["action"] = time.time() - t0
    print(f"[action] {timings['action']:.3f}s -> action_executed={state['action_executed']}, "
          f"result={state['action_result']}")

    t0 = time.time()
    state = qa_node(state)
    timings["qa"] = time.time() - t0
    print(f"[qa] {timings['qa']:.3f}s -> qa_passed={state['qa_passed']} "
          f"(POST_ACTION_WAIT_SECONDS 대기 포함된 실제 시간)")

    t0 = time.time()
    try:
        state = logging_node(state)
    except Exception as exc:
        print(f"[logging] 실패(DB 미연결 등, 타이밍엔 영향 없음): {exc}")
    timings["logging"] = time.time() - t0
    print(f"[logging] {timings['logging']:.3f}s")

    timings["total"] = time.time() - t_total_start

    return {
        "resource_id": resource_id,
        "resource_type": resource_type,
        "anomaly_flag": True,
        "anomaly_type": state["anomaly_type"],
        "selected_action": state["selected_action"],
        "risk_level": state["risk_level"],
        "approval_bypassed_for_timing": approval_bypassed,
        "qa_passed": state["qa_passed"],
        "timings": timings,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resource-id", required=True)
    parser.add_argument("--resource-type", required=True,
                         choices=["EC2", "Lambda", "S3", "RDS", "AutoScaling"])
    parser.add_argument("--bypass-approval", action="store_true",
                         help="requires_approval=True여도 타이밍 측정을 위해 강제로 넘어감 "
                              "(실제 액션이 실행됨 — 테스트 리소스에서만 쓸 것)")
    args = parser.parse_args()

    result = measure(args.resource_id, args.resource_type, args.bypass_approval)

    print("\n=== 단계별 소요시간 ===")
    for step, t in result.get("timings", {}).items():
        print(f"  {step}: {t:.3f}s" if step != "total" else f"  --------\n  총합: {t:.3f}s")

    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")
    out_path = RESULT_DIR / f"pipeline_timing_{date_str}.json"

    existing = []
    if out_path.exists():
        with open(out_path, encoding="utf-8") as f:
            existing = json.load(f)
    existing.append({**result, "measured_at": datetime.now(timezone.utc).isoformat()})
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False, indent=2)
    print(f"\n결과 저장: {out_path}")


if __name__ == "__main__":
    main()
