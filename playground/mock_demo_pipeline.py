from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env")

import uuid

from langgraph.types import Command

from config import pipeline_live_status
from pipeline.detection_agent import _build_initial_state
from pipeline.classification_agent import classification_node
from pipeline.decision_agent import decision_node
from pipeline.logging_agent import logging_node
from pipeline.live_events import (
    emit_decision_event,
    emit_qa_event,
    scenario_label,
    action_label,
)
from pipeline.checkpointer import get_postgres_checkpointer
import pipeline.graph as graph_module
from pipeline.graph import build_approval_graph
from api import pipeline_process

EVAL_DIR = PROJECT_ROOT / "playground" / "eval_outputs"
TEAM_DIR = PROJECT_ROOT / "playground" / "team_results"

_DEMO_HOURLY_RATE_USD = 0.0104


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _ensure_cost(
    raw_metrics: dict, period_seconds: int = 300, cost_proxy_metric: str | None = None
) -> dict:
    if "cost" in raw_metrics:
        return raw_metrics
    n_points = len(next(iter(raw_metrics.values())))
    per_point = _DEMO_HOURLY_RATE_USD * (period_seconds / 3600)

    if cost_proxy_metric and cost_proxy_metric in raw_metrics:
        proxy = raw_metrics[cost_proxy_metric]
        baseline = min(v for v in proxy if v > 0) if any(v > 0 for v in proxy) else 1.0
        cost = [per_point * (v / baseline) for v in proxy]
    else:
        cost = [per_point] * n_points
    return {**raw_metrics, "cost": cost}


def _base_state(resource_id: str, resource_type: str) -> dict:
    state = _build_initial_state(
        {
            "resource_id": resource_id,
            "resource_type": resource_type,
            "raw_metrics": {},
        }
    )
    state["ec2_utilization_band"] = None
    return state


def _build_scenarios() -> list[dict]:
    scenarios = []

    s3_src = _load(
        EVAL_DIR
        / "s3_repeated_trial__window2.5h_objsize50kb_n8-5_scriptv5_20260910.json"
    )
    s3_trial = s3_src["anomaly_trials"][0]
    scenarios.append(
        {
            "key": "s3",
            "title": "S3 다운로드 폭증",
            "live": True,
            "resource_id": s3_trial["after"]["resource_id"],
            "resource_type": "S3",
            "raw_metrics": s3_trial["after"]["raw_metrics"],
            "resource_age_seconds": None,
            "replay": {
                "action_executed": "Block",
                "action_result": {"status": "success", "action": "Block"},
                "qa_passed": True,
                "sla_check_result": {
                    "cpu_ok": True,
                    "cost_ok": True,
                    "availability_ok": True,
                    "detail": "실측 재생 (3x_real_pipeline_20260910_025234.json)",
                },
                "rollback_count": 0,
            },
            "source_note": "s3_repeated_trial__window2.5h_objsize50kb_n8-5_scriptv5_20260910.json",
        }
    )

    ec2_over_src = _load(EVAL_DIR / "ec2_overprovision_repeated_trial__converted.json")
    ec2_over_trial = next(
        t
        for t in ec2_over_src["anomaly_trials"]
        if t["after"]["resource_id"] == "i-043ad8f8ff784936e"
    )
    scenarios.append(
        {
            "key": "ec2_over",
            "title": "EC2 오버프로비저닝",
            "live": True,
            "resource_id": ec2_over_trial["after"]["resource_id"],
            "resource_type": "EC2",
            "raw_metrics": ec2_over_trial["after"]["raw_metrics"],
            "resource_age_seconds": ec2_over_trial["after"].get("resource_age_seconds"),
            "measured_at": ec2_over_trial["after"].get("measured_at"),
            "anomaly_type_replay": "cost_inefficiency",
            "decision_replay": {
                "selected_action": "Resize",
                "risk_level": "MED",
                "requires_approval": True,
            },
            "replay": {
                "action_executed": "Resize",
                "action_result": {"status": "success", "new_instance_type": "t3.micro"},
                "qa_passed": True,
                "sla_check_result": {
                    "cpu_ok": True,
                    "cost_ok": True,
                    "availability_ok": True,
                    "detail": "실측 재생 (batch_pipeline_replay__EC2_20260912_134432.json)",
                },
                "rollback_count": 0,
            },
            "source_note": "ec2_overprovision_repeated_trial__converted.json",
        }
    )

    lambda_src = _load(
        TEAM_DIR / "lambda_throttle" / "clean_verification_20260914.json"
    )
    lambda_trial = next(t for t in lambda_src["trials"] if t.get("label") == "anomaly")
    scenarios.append(
        {
            "key": "lambda",
            "title": "함수 재시도 폭증 (Lambda)",
            "live": False,
            "resource_id": lambda_trial["resource_id"],
            "resource_type": "Lambda",
            "replay_full": {
                "anomaly_flag": True,
                "anomaly_score_zscore": lambda_trial.get("anomaly_score_zscore"),
                "anomaly_score_iforest": lambda_trial.get("anomaly_score_iforest"),
                "triggered_metrics": lambda_trial.get("triggered_metrics") or [],
                "anomaly_type": lambda_trial["anomaly_type"],
                "selected_action": lambda_trial["selected_action"],
                "risk_level": lambda_trial["risk_level"],
                "requires_approval": lambda_trial["risk_level"] in ("MED", "HIGH"),
            },
            "replay": {
                "action_executed": lambda_trial["selected_action"],
                "action_result": {
                    "status": "success",
                    "action": lambda_trial["selected_action"],
                },
                "qa_passed": True,
                "sla_check_result": {
                    "cpu_ok": True,
                    "cost_ok": True,
                    "availability_ok": True,
                    "detail": "실측 재생 (team_results/lambda_throttle/clean_verification_20260914.json)",
                },
                "rollback_count": 0,
            },
            "source_note": "team_results/lambda_throttle/clean_verification_20260914.json",
        }
    )

    edos_src = _load(
        EVAL_DIR / "autoscaling_edos_traffic_trial__n5-8_scriptv4_20260915.json"
    )
    edos_trial = next(
        t
        for t in edos_src["trials"]
        if t.get("resource") == "detection-traffic-asg-anomaly-3"
    )
    pr = edos_trial["pipeline_result"]
    scenarios.append(
        {
            "key": "edos",
            "title": "EDoS 공격 감지 (AutoScaling)",
            "live": True,
            "resource_id": edos_trial["resource"],
            "resource_type": "AutoScaling",
            "raw_metrics": edos_trial["after"]["raw_metrics"],
            "resource_age_seconds": None,
            "cost_proxy_metric": "request_count",
            "anomaly_type_replay": pr.get("anomaly_type"),
            "decision_replay": {
                "selected_action": pr.get("selected_action"),
                "risk_level": pr.get("risk_level"),
                "requires_approval": pr.get("requires_approval"),
            },
            "replay": {
                "action_executed": pr.get("action_executed"),
                "action_result": pr.get("action_result"),
                "qa_passed": pr.get("qa_passed"),
                "sla_check_result": {
                    "cpu_ok": True,
                    "cost_ok": True,
                    "availability_ok": True,
                    "detail": "실측 재생 - ScaleDown + WAF 연동 모두 성공 "
                    "(autoscaling_edos_traffic_trial__n5-8_scriptv4_20260915.json, "
                    "resource=detection-traffic-asg-anomaly-3)",
                },
                "rollback_count": pr.get("rollback_count", 0),
            },
            "source_note": "autoscaling_edos_traffic_trial__n5-8_scriptv4_20260915.json (anomaly-3)",
        }
    )

    zombie_src = _load(
        EVAL_DIR / "ec2_zombie_replay_trial__n8-5_scriptv1_20260910.json"
    )
    zombie_trial = zombie_src["anomaly_trials"][0]
    scenarios.append(
        {
            "key": "zombie",
            "title": "좀비 리소스 발견 (EC2)",
            "live": True,
            "resource_id": zombie_trial["after"]["resource_id"],
            "resource_type": "EC2",
            "raw_metrics": zombie_trial["after"]["raw_metrics"],
            "resource_age_seconds": zombie_trial.get("resource_age_seconds"),
            "measured_at": zombie_trial.get("measured_at"),
            "anomaly_type_replay": "cost_inefficiency",
            "decision_replay": {
                "selected_action": "Stop",
                "risk_level": "LOW",
                "requires_approval": False,
            },
            "replay": {
                "action_executed": "Stop",
                "action_result": {"status": "success", "action": "Stop"},
                "qa_passed": True,
                "sla_check_result": {
                    "cpu_ok": True,
                    "cost_ok": True,
                    "availability_ok": True,
                    "detail": "실측 재생 (EC2 좀비 v3 클린 재실행 100% 결과 반영, "
                    "playground/eval_outputs/logs/ec2_zombie_v3_batch_pipeline_console_20260914_194909.log)",
                },
                "rollback_count": 0,
            },
            "source_note": "ec2_zombie_replay_trial__n8-5_scriptv1_20260910.json",
        }
    )

    return scenarios


def _wrap_with_overrides(real_fn, overrides: dict | None):
    if not overrides:
        return real_fn

    def wrapped(state):
        new_state = real_fn(state)
        new_state.update(overrides)
        return new_state

    return wrapped


_REPLAY_CLASSIFICATION_RULES = {
    "cost_inefficiency": (
        "CLF-004",
        "비용 지표만 단독 이상, 성능 지표 정상 -> 좀비 리소스 또는 오버프로비저닝",
    ),
    "risk_security": (
        "CLF-001",
        "ALB 요청 수(request_count)가 평균 대비 5.0배 이상 급증하고 최근 구간에도 "
        "계속 유지됨 -> EDoS 의심 (순간 스파이크였다면 단순 인기 폭증으로 간주)",
    ),
}


def _skip_classification(anomaly_type: str):
    rule_id, reasoning = _REPLAY_CLASSIFICATION_RULES.get(anomaly_type, (None, None))

    def node(state):
        new_state = dict(state)
        new_state["anomaly_type"] = anomaly_type
        new_state["matched_rule_id"] = rule_id
        new_state["classification_reasoning"] = (
            f"[Rule:{rule_id}] {reasoning}" if rule_id else "실측 재생"
        )
        new_state["interim_action_taken"] = None
        return new_state

    return node


REALISTIC_NODE_SECONDS = {
    "detection": 0.3,
    "classification": 2.0,
    "decision": 0.6,
    "action": 1.0,
    "qa": 4.0,
    "logging": 0.2,
}


def _track_stream(
    app,
    input_or_command,
    config: dict,
    nodes: dict,
    resource_id: str,
    resource_type: str,
    time_scale: float,
) -> None:
    for chunk in app.stream(input_or_command, config, stream_mode="updates"):
        updated = [n for n in chunk if n in nodes]
        for node_name in updated:
            nodes[node_name] = "success"
        upcoming = app.get_state(config).next
        for node_name in upcoming:
            if node_name in nodes and nodes[node_name] == "idle":
                nodes[node_name] = "running"
        pipeline_live_status.write(nodes, resource_id, resource_type)
        if updated:
            delay = (
                max(REALISTIC_NODE_SECONDS.get(n, 0.3) for n in updated) * time_scale
            )
            if delay:
                time.sleep(delay)


def _print_summary(state: dict) -> None:
    print(
        f"  결과: anomaly_type={state.get('anomaly_type')} action={state.get('selected_action')} "
        f"risk={state.get('risk_level')} action_result={(state.get('action_result') or {}).get('status')} "
        f"qa_passed={state.get('qa_passed')} rollback_count={state.get('rollback_count')}"
    )


def run_scenario(sc: dict, checkpointer, time_scale: float, stage_pause: float) -> None:
    print(f"\n=== {sc['title']} ({datetime.now(timezone.utc).isoformat()}) ===")
    print(f"    출처: {sc['source_note']}")

    resource_id, resource_type = sc["resource_id"], sc["resource_type"]
    nodes = pipeline_live_status.initial_nodes()
    pipeline_live_status.write(nodes, resource_id, resource_type)

    if sc["live"]:
        raw_metrics = _ensure_cost(
            sc["raw_metrics"], cost_proxy_metric=sc.get("cost_proxy_metric")
        )
        initial_state = _build_initial_state(
            {
                "resource_id": resource_id,
                "resource_type": resource_type,
                "raw_metrics": raw_metrics,
                "resource_age_seconds": sc.get("resource_age_seconds"),
            }
        )
        initial_state["_demo_replay"] = sc["replay"]

        if sc.get("anomaly_type_replay"):
            graph_module.classification_node = _skip_classification(
                sc["anomaly_type_replay"]
            )
        else:
            graph_module.classification_node = classification_node
        graph_module.decision_node = _wrap_with_overrides(
            decision_node, sc.get("decision_replay")
        )
        graph_module.notify_gate_node = lambda state: state
        approval_app = build_approval_graph(checkpointer)

        thread_id = f"mock-demo-{resource_type}-{resource_id}-{uuid.uuid4().hex[:8]}"
        config = {"configurable": {"thread_id": thread_id}}

        _track_stream(
            approval_app,
            initial_state,
            config,
            nodes,
            resource_id,
            resource_type,
            time_scale,
        )
        snapshot = approval_app.get_state(config)
        state = dict(snapshot.values)

        if not state.get("anomaly_flag"):
            print(
                "  이상 없음으로 판정 - 데모 데이터가 실측과 달라졌을 수 있음, 건너뜀"
            )
            return

        if snapshot.next:
            print(
                f"  [approval_gate] 실제 승인 대기열에 등록됨 - action={state.get('selected_action')} "
                f"risk={state.get('risk_level')} thread_id={thread_id}"
            )
            print(
                "  웹 제어판 '승인 대기' 탭에서 사람이 직접 승인/거부할 때까지 대기합니다 "
                "(자동 승인 없음 - Ctrl+C로 중단 가능)..."
            )
            while True:
                time.sleep(1.0)
                snapshot = approval_app.get_state(config)
                if not snapshot.next:
                    break
            state = dict(snapshot.values)
            for key in ("action", "qa", "logging"):
                nodes[key] = "success"
            pipeline_live_status.write(nodes, resource_id, resource_type)
        else:
            label = scenario_label(
                state.get("resource_type"),
                state.get("anomaly_type"),
                state.get("ec2_utilization_band"),
            )
            print(f"  토스트: 발견 - {label} (LOW 위험도라 승인 없이 자동 진행)")
    else:
        state = _base_state(resource_id, resource_type)
        state.update(sc["replay_full"])
        for key in ("detection", "classification", "decision"):
            time.sleep(REALISTIC_NODE_SECONDS[key] * time_scale)
            nodes[key] = "success"
            pipeline_live_status.write(nodes, resource_id, resource_type)
        emit_decision_event(state)
        label = scenario_label(
            state.get("resource_type"),
            state.get("anomaly_type"),
            state.get("ec2_utilization_band"),
        )
        print(
            f"  [재생] {label} action={state['selected_action']} risk={state['risk_level']}"
        )
        time.sleep(stage_pause)

        nodes["action"] = "running"
        pipeline_live_status.write(nodes, resource_id, resource_type)
        time.sleep(REALISTIC_NODE_SECONDS["action"] * time_scale)
        replay = sc["replay"]
        state["action_executed"] = replay["action_executed"]
        state["action_result"] = replay["action_result"]
        nodes["action"] = "success"
        pipeline_live_status.write(nodes, resource_id, resource_type)

        nodes["qa"] = "running"
        pipeline_live_status.write(nodes, resource_id, resource_type)
        time.sleep(REALISTIC_NODE_SECONDS["qa"] * time_scale)
        state["qa_passed"] = replay["qa_passed"]
        state["sla_check_result"] = replay["sla_check_result"]
        state["rollback_count"] = replay["rollback_count"]
        nodes["qa"] = "success"
        pipeline_live_status.write(nodes, resource_id, resource_type)
        emit_qa_event(state)

        nodes["logging"] = "running"
        pipeline_live_status.write(nodes, resource_id, resource_type)
        try:
            logging_node(state)
        except Exception as exc:
            print(f"  logging 실패(데모엔 영향 없음): {exc}")
        nodes["logging"] = "success"
        pipeline_live_status.write(nodes, resource_id, resource_type)

    action_txt = action_label(state.get("selected_action"))
    print(f"  토스트: {'완료' if state.get('qa_passed') else '롤백'} - {action_txt}")
    _print_summary(state)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--time-scale",
        type=float,
        default=2.5,
        help="노드별 지연(REALISTIC_NODE_SECONDS, 실측 비율 압축값)에 곱하는 배율 - "
        "1.0이면 detection 0.3s/classification 2s/decision 0.6s/action 1s/"
        "qa 4s/logging 0.2s. 기본값 2.5는 나레이션 길이에 맞춰 시나리오 하나가 "
        "너무 순식간에 끝나 보이지 않도록 여유를 둔 값(전체 약 20초/시나리오) - "
        "더 빠르게 보이려면 1.0, 더 느긋하게는 3~4 등",
    )
    parser.add_argument(
        "--stage-pause",
        type=float,
        default=2.5,
        help="토스트가 뜬 뒤 다음 단계로 넘어가기 전 대기(초) - Lambda(재생 전용)"
        "에서만 쓰인다. 승인이 필요한 시나리오는 이제 실제 승인/거부가"
        "있을 때까지 무기한 대기하므로 이 값과 무관하다",
    )
    parser.add_argument(
        "--scenario-pause",
        type=float,
        default=8.0,
        help="시나리오 사이 대기(초) - 기본값 8초는 토스트 자동소멸 시간"
        "(frontend/src/components/Toast.jsx의 AUTO_DISMISS_REMOVE_MS=7500ms)"
        "보다 살짝 길게 잡아서, 이전 시나리오의 '완료' 토스트가 화면에서"
        "완전히 사라진 뒤에 다음 시나리오가 시작되게 한다",
    )
    parser.add_argument(
        "--only",
        type=str,
        default=None,
        help="쉼표로 구분된 시나리오 key만 순서대로 재생 "
        "(s3, ec2_over, lambda, edos, zombie). "
        "예: --only zombie,edos  (짧은 시연용 - LLM 분류 호출이 없어 "
        "타이밍이 안정적인 두 시나리오만 고른 것)",
    )
    args = parser.parse_args()

    all_scenarios = {sc["key"]: sc for sc in _build_scenarios()}
    if args.only:
        keys = [k.strip() for k in args.only.split(",") if k.strip()]
        unknown = [k for k in keys if k not in all_scenarios]
        if unknown:
            raise SystemExit(
                f"알 수 없는 시나리오 key: {unknown} (가능한 값: {list(all_scenarios)})"
            )
        scenarios = [all_scenarios[k] for k in keys]
    else:
        scenarios = list(all_scenarios.values())

    print(
        f"총 {len(scenarios)}개 시나리오를 순서대로 재생합니다 "
        f"(action/qa는 실시간 AWS 호출 없음, 승인 필요 건은 실제 승인 대기열에 등록됨)."
    )

    with get_postgres_checkpointer() as checkpointer:
        checkpointer.setup()
        for sc in scenarios:
            pipeline_live_status.clear()
            pipeline_process._write_state(
                os.getpid(), datetime.now(timezone.utc).isoformat()
            )
            try:
                run_scenario(sc, checkpointer, args.time_scale, args.stage_pause)
            finally:
                pipeline_process._write_state(None, None)
            time.sleep(args.scenario_pause)
    pipeline_live_status.clear()
    print("\n=== 데모 재생 종료 ===")


if __name__ == "__main__":
    main()
