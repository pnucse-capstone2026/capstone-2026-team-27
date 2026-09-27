"""
playground/real_demo/common.py

"실연동" 데모용 공통 유틸리티 — playground/live_demo/common.py와 달리 탐지 입력값
(raw_metrics)도 재생이 아니라 실시간 CloudWatch에서 그대로 가져온다
(pipeline/orchestrator.py의 assemble_resource()를 그대로 재사용 — 이게 실제
운영 코드(run_detection_cycle)가 지표를 조립하는 방식과 100% 동일).

⚠️ 웜업 필수: assemble_resource()는 n_points=30 × period_seconds=300초 =
2.5시간치 CloudWatch 데이터포인트를 요구한다. 리소스를 방금 띄웠다면 그 구간이
전부 채워질 때까지(=리소스가 최소 2.5시간 전에 이미 떠서 이상 패턴을 유발 중이어야)
탐지 자체가 정상 동작하지 않는다 — 각 시나리오 스크립트 실행 전에 반드시
해당 리소스를 미리(2.5시간 이상) 띄워둬야 한다.

각 시나리오 스크립트(zombie_real.py 등)는 이 모듈의 run_real_scenario()만
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

from config import pipeline_live_status
from api import pipeline_process
from pipeline.detection_agent import _build_initial_state
from pipeline.checkpointer import get_postgres_checkpointer
from pipeline.graph import build_approval_graph
from pipeline.orchestrator import assemble_resource
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
    """live_demo/common.py의 동명 함수와 동일 — 실제 그래프 스트리밍 중 노드 완료를
    웹 제어판 상태 위젯에 그대로 반영(인위적 지연 없음)."""
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
    """live_demo/common.py와 동일한 실측 기반 비용 추정 로직."""
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


def run_real_scenario(
    scenario_key: str,
    resource_id: str,
    resource_type: str,
    n_points: int = 30,
    period_seconds: int = 300,
    extra_metrics_fetcher=None,
) -> dict:
    """탐지 입력까지 실시간 CloudWatch에서 조립해 전부 실제로 돌리는 시나리오 1건 실행.

    live_demo.common.run_live_scenario()와 동일한 구조지만, raw_metrics를 인자로
    받는 대신 assemble_resource()로 지금 이 순간의 실측 지표를 직접 가져온다.

    extra_metrics_fetcher: assemble_resource()가 못 채우는 지표를 보충해야 할 때
    쓰는 훅(예: AutoScaling EDoS는 ALB RequestCount가 detection에 필수인데
    pipeline/cloudwatch_client.fetch_metrics는 group_desired_capacity/
    group_in_service_instances만 가져와서 따로 병합해야 함). 인자 없이 호출해서
    {metric_name: [values...]} 형태의 dict를 반환하면 raw_metrics에 병합한다.
    """
    print(
        f"[{scenario_key}] 실시간 CloudWatch 지표 조회 중 (resource_id={resource_id}, "
        f"resource_type={resource_type})..."
    )
    resource = assemble_resource(resource_id, resource_type, n_points, period_seconds)
    raw_metrics = resource["raw_metrics"]
    if extra_metrics_fetcher is not None:
        raw_metrics = dict(raw_metrics)
        raw_metrics.update(extra_metrics_fetcher())
    resource_age_seconds = resource.get("resource_age_seconds")

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
                f"real-demo-{resource_type}-{resource_id}-{uuid.uuid4().hex[:8]}"
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
        "anomaly_type": state.get("anomaly_type"),
        "selected_action": state.get("selected_action"),
        "risk_level": state.get("risk_level"),
        "requires_approval": state.get("requires_approval"),
        "action_result": state.get("action_result"),
        "qa_passed": state.get("qa_passed"),
        "rollback_count": state.get("rollback_count"),
        # ⚠️ measure_pipeline_timing.py의 measure()는 state["raw_metrics"]를 내부적으로
        # 갖고 있으면서도 반환값에는 안 담아서(설계 누락 — 원래 목적이 타이밍 측정이라
        # 재생용 보관을 염두에 안 둠), sysy04의 lambda_throttle_* 로그에 raw_metrics가
        # 전혀 없는 문제가 있었다(2026-09-27 확인). 이 실행에서 실제로 조회된 지표를
        # 여기서 반드시 같이 저장해서, live_demo가 나중에 이 로그를 그대로 재생 소스로
        # 쓸 수 있게 한다.
        # QA_agent.qa_node가 액션 후 raw_metrics를 실측 재조회값으로 덮어쓰고 원래
        # (액션 전, 이상이 실제로 트리거된) 값은 pre_action_raw_metrics에 보존하므로
        # (QA_agent.py:708-709), 재생 소스로는 그 원본을 우선 쓴다.
        "raw_metrics": state.get("pre_action_raw_metrics") or state.get("raw_metrics"),
        "resource_age_seconds": state.get("resource_age_seconds"),
    }

    log_dir = LOG_ROOT / scenario_key
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = run_started_at.strftime("%Y%m%d_%H%M%S")
    log_path = log_dir / f"{scenario_key}_{ts}.json"
    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n=== {scenario_key} (실연동) 완료 ===")
    print(
        f"  action={result['selected_action']} risk={result['risk_level']} "
        f"qa_passed={result['qa_passed']} elapsed={result['elapsed_seconds_total']}s "
        f"cost=${result['estimated_cost_usd']}"
    )
    print(f"  로그 저장: {log_path}")

    return result
