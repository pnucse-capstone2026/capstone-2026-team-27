"""
playground/live_demo/common.py

실시간 부스 시연용 공통 유틸리티.

mock_demo_pipeline.py(전부 재생)와 다르게, 여기서는 탐지 입력값(raw_metrics)만
과거 실측 데이터로 재생하고 분류~로깅까지는 전부 진짜 함수를 그대로 태운다
(_demo_replay 우회 없음 - action_node/qa_node가 실제 boto3를 호출한다).

각 시나리오 스크립트(zombie_live.py 등)는 이 모듈의 run_live_scenario()만
부르면 되고, 결과는 실제 Postgres(pipeline_events/agent_runs)에 그대로
쌓이므로 웹 제어판/Grafana에는 추가 배선 없이 실시간 반영된다.
"""

from __future__ import annotations

import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env")

from config import pipeline_live_status, last_normal_check
from api import pipeline_process
from pipeline.detection_agent import _build_initial_state
from pipeline.checkpointer import get_postgres_checkpointer
from pipeline.graph import build_approval_graph
from pipeline import cost_estimator

LOG_ROOT = Path(__file__).parent / "logs"


def _track_stream(
    app,
    input_or_command,
    config: dict,
    nodes: dict,
    resource_id: str,
    resource_type: str,
) -> None:
    """실제 그래프를 스트리밍하면서, 노드가 끝날 때마다 웹 제어판 '파이프라인
    에이전트 상태' 위젯에 즉시 반영한다(연출용 인위적 지연 없음 - 실제 소요
    시간 그대로)."""
    for chunk in app.stream(input_or_command, config, stream_mode="updates"):
        updated = [n for n in chunk if n in nodes]
        for node_name in updated:
            nodes[node_name] = "success"
        upcoming = app.get_state(config).next
        for node_name in upcoming:
            if node_name in nodes and nodes[node_name] == "idle":
                nodes[node_name] = "running"
        pipeline_live_status.write(nodes, resource_id, resource_type)


def _estimate_resource_cost_usd(
    resource_type: str, resource_id: str, state: dict
) -> float | None:
    """실측 비용(USD) 추정 - 리소스 타입별 cost_estimator 함수를 그대로 호출.
    실패(리소스 조회 불가 등)해도 시연 자체는 죽지 않도록 None을 반환한다."""
    try:
        if resource_type == "EC2":
            instance_type = cost_estimator._get_ec2_instance_type(resource_id)
            return cost_estimator.estimate_ec2_cost(instance_type, hours=1.0)
        if resource_type == "Lambda":
            metrics = state.get("raw_metrics", {})
            invocations = sum(metrics.get("invocation_count", [0])) or 0
            duration = (metrics.get("duration_avg") or [0])[-1]
            memory_mb = cost_estimator._get_lambda_memory_mb(resource_id)
            return cost_estimator.estimate_lambda_cost(invocations, duration, memory_mb)
        if resource_type == "S3":
            metrics = state.get("raw_metrics", {})
            bytes_downloaded = sum(metrics.get("bytes_downloaded", [0])) or 0
            get_requests = sum(metrics.get("number_of_requests", [0])) or 0
            return cost_estimator.estimate_s3_cost(
                storage_gb=0.0,
                get_requests=get_requests,
                put_requests=0.0,
                period_fraction_of_month=1.0 / (24 * 30),
                bytes_downloaded=bytes_downloaded,
            )
        if resource_type == "AutoScaling":
            instance_type = cost_estimator._get_asg_instance_type(resource_id)
            capacity = (
                state.get("raw_metrics", {}).get("group_desired_capacity") or [1]
            )[-1]
            return cost_estimator.estimate_autoscaling_cost(
                instance_type, capacity, hours=1.0
            )
    except Exception as exc:
        print(f"  [경고] 비용 추정 실패({resource_type}:{resource_id}): {exc}")
        return None
    return None


def run_live_scenario(
    scenario_key: str,
    resource_id: str,
    resource_type: str,
    raw_metrics: dict,
    resource_age_seconds: float | None = None,
) -> dict:
    """탐지 입력만 재생하고 나머지(분류~로깅)는 전부 실제로 돌리는 시나리오 1건 실행.

    반환값 + 로그 파일에 elapsed_seconds(단계별 소요시간)와
    estimated_cost_usd(실측 기반 비용 추정치)를 같이 남긴다.
    """
    nodes = pipeline_live_status.initial_nodes()
    pipeline_live_status.write(nodes, resource_id, resource_type)
    pipeline_process._write_state(
        __import__("os").getpid(), datetime.now(timezone.utc).isoformat()
    )

    run_started_at = datetime.now(timezone.utc)
    initial_state = _build_initial_state(
        {
            "resource_id": resource_id,
            "resource_type": resource_type,
            "raw_metrics": raw_metrics,
            "resource_age_seconds": resource_age_seconds,
        }
    )

    try:
        with get_postgres_checkpointer() as checkpointer:
            checkpointer.setup()
            approval_app = build_approval_graph(checkpointer)

            thread_id = (
                f"live-demo-{resource_type}-{resource_id}-{uuid.uuid4().hex[:8]}"
            )
            config = {"configurable": {"thread_id": thread_id}}

            _track_stream(
                approval_app, initial_state, config, nodes, resource_id, resource_type
            )
            snapshot = approval_app.get_state(config)
            state = dict(snapshot.values)

            if snapshot.next:
                print(
                    f"  [approval_gate] 실제 승인 대기열에 등록됨 - action={state.get('selected_action')} "
                    f"risk={state.get('risk_level')} thread_id={thread_id}"
                )
                print(
                    "  웹 제어판 '승인 대기' 탭에서 사람이 직접 승인/거부할 때까지 대기합니다..."
                )
                import time as _time

                while True:
                    _time.sleep(1.0)
                    # 승인 대기 중에도 노드 상태가 안 바뀌어서 write()가 호출 안 되면
                    # FRESHNESS_SECONDS(30초) 넘게 갱신이 없어 웹 대시보드가 "지금
                    # 실행 중"이 아니라 과거 실행 기록(폴백)으로 잘못 표시된다 — 매
                    # 반복마다 updated_at만이라도 다시 찍는다.
                    #
                    # 승인은 api/routers/approvals.py가 별도 백그라운드 스레드에서
                    # 처리한다(5분 넘게 블로킹되는 걸 피하려고) — 즉 승인 이후
                    # action~QA 실행은 "이 프로세스"가 아니라 API 서버 쪽에서 일어나고,
                    # 거기서도(qa_agent.py) 같은 파일에 실제 진행 상황(action=success,
                    # qa=running 등)을 쓴다. 여기서 무조건 이 루프가 들고 있는 stale한
                    # nodes(전부 idle)로 덮어쓰면, API 서버가 방금 쓴 진짜 진행 상황을
                    # 1초마다 도로 idle로 되돌려버린다 — 그래서 같은 resource_id에 대해
                    # 이미 더 최신 진행 상황이 쓰여 있으면 그걸 그대로 유지하고 시각만
                    # 갱신한다.
                    current = pipeline_live_status.read_if_fresh()
                    write_nodes = (
                        current["nodes"]
                        if current and current.get("resource_id") == resource_id
                        else nodes
                    )
                    pipeline_live_status.write(write_nodes, resource_id, resource_type)
                    snapshot = approval_app.get_state(config)
                    if not snapshot.next:
                        break
                state = dict(snapshot.values)
                for key in ("action", "qa", "logging"):
                    nodes[key] = "success"
                pipeline_live_status.write(nodes, resource_id, resource_type)
    finally:
        pipeline_process._write_state(None, None)
        pipeline_live_status.clear()

    run_ended_at = datetime.now(timezone.utc)
    elapsed_seconds_total = (run_ended_at - run_started_at).total_seconds()
    step_timings_ms = state.get("step_timings") or {}
    estimated_cost_usd = _estimate_resource_cost_usd(resource_type, resource_id, state)

    result = {
        "scenario_key": scenario_key,
        "resource_id": resource_id,
        "resource_type": resource_type,
        "run_started_at": run_started_at.isoformat(),
        "run_ended_at": run_ended_at.isoformat(),
        "elapsed_seconds_total": round(elapsed_seconds_total, 2),
        "step_timings_ms": step_timings_ms,
        "estimated_cost_usd": estimated_cost_usd,
        # [2026-09-28] real_demo.common.py와 동일한 이유로 추가 — 판정 점수 자체가
        # 없으면 "왜 탐지가 안 됐는지" 나중에 재현/디버깅이 불가능하다.
        "anomaly_flag": state.get("anomaly_flag"),
        "anomaly_score_zscore": state.get("anomaly_score_zscore"),
        "anomaly_score_iforest": state.get("anomaly_score_iforest"),
        "triggered_metrics": state.get("triggered_metrics"),
        "ec2_utilization_band": state.get("ec2_utilization_band"),
        "anomaly_type": state.get("anomaly_type"),
        "selected_action": state.get("selected_action"),
        "risk_level": state.get("risk_level"),
        "requires_approval": state.get("requires_approval"),
        "action_result": state.get("action_result"),
        "qa_passed": state.get("qa_passed"),
        "rollback_count": state.get("rollback_count"),
    }

    # 웹 제어판 사이드바의 "비용 정상 (OO 기준)"은 이 시나리오가 QA까지 실제로
    # 통과했을 때만 갱신한다 — 탐지 자체가 안 됐거나 액션/QA가 실패한 실행까지
    # "정상"으로 표시하면 오해를 준다는 요청(2026-09-29).
    if result.get("qa_passed") is True:
        last_normal_check.write()

    # [2026-09-28] real_demo.common.py와 동일 — 파일명만 보고 실패 사례를 바로
    # 골라낼 수 있게 "_fail" 접미사를 붙인다.
    action_result = result.get("action_result")
    action_failed = bool(action_result) and action_result.get("status") != "success"
    is_fail = (
        not result.get("anomaly_flag")
        or result.get("selected_action") == "NoAction"
        or result.get("qa_passed") is False
        or action_failed
    )
    suffix = "_fail" if is_fail else ""

    log_dir = LOG_ROOT / scenario_key
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = run_started_at.strftime("%Y%m%d_%H%M%S")
    log_path = log_dir / f"{scenario_key}_{ts}{suffix}.json"
    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n=== {scenario_key} {'실패' if is_fail else '완료'} ===")
    print(
        f"  action={result['selected_action']} risk={result['risk_level']} "
        f"qa_passed={result['qa_passed']} elapsed={result['elapsed_seconds_total']}s "
        f"cost=${result['estimated_cost_usd']}"
    )
    print(f"  로그 저장: {log_path}")

    return result
