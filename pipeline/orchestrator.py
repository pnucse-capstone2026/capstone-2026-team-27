"""
pipeline/orchestrator.py

Phase E: 오케스트레이션 — 디스커버리(C) → 지표/비용 수집(A+B) → 순차 스캔(Phase 0).

⚠️ 안전 설계: run_detection_cycle()은 탐지만 하고 끝난다. 실제 액션(Stop/Throttle 등)은
   자동 실행하지 않는다 — anomaly_flag=True인 리소스 목록만 반환하고, 그걸 실제
   app.invoke()로 이어서 액션까지 실행할지는 호출부(사람)가 명시적으로 결정해야 한다.
"""

from __future__ import annotations

from typing import Iterator, Optional

from pipeline.cloudwatch_client import fetch_metrics
from pipeline.cost_estimator import estimate_cost_series
from pipeline.detection_agent import scan_resources_sequential
from pipeline.resource_discovery import discover_all_resources
from schema.state import PipelineState


def assemble_resource(
    resource_id: str,
    resource_type: str,
    n_points: int = 30,
    period_seconds: int = 300,
) -> dict:
    """CloudWatch 지표(Phase A) + 비용 추정(Phase B)을 합쳐서
    scan_resources_sequential이 받는 {resource_id, resource_type, raw_metrics} 형태로 조립."""
    usage_metrics = fetch_metrics(resource_type, resource_id, n_points=n_points, period_seconds=period_seconds)
    cost_series = estimate_cost_series(resource_type, resource_id, usage_metrics, period_seconds=period_seconds)

    raw_metrics = dict(usage_metrics)
    raw_metrics["cost"] = cost_series

    return {
        "resource_id": resource_id,
        "resource_type": resource_type,
        "raw_metrics": raw_metrics,
    }


def run_detection_cycle(
    tag_key: str = "Detection",
    tag_value: str = "true",
    resource_types: Optional[list[str]] = None,
    n_points: int = 30,
    period_seconds: int = 300,
) -> list[PipelineState]:
    """
    1회 탐지 사이클: 디스커버리 → 지표/비용 수집 → 순차 탐지.
    반환값은 anomaly_flag=True인 리소스들의 PipelineState 리스트 (액션은 실행 안 됨).
    지표 수집에 실패한 리소스는 건너뛰고 로그만 남긴다 (한 리소스 오류로 전체가 죽지 않게).
    """
    discovered = discover_all_resources(tag_key, tag_value, resource_types)

    resource_list: list[dict] = []
    for r in discovered:
        try:
            resource_list.append(
                assemble_resource(r["resource_id"], r["resource_type"], n_points, period_seconds)
            )
        except Exception as exc:
            print(f"[orchestrator] {r['resource_type']} {r['resource_id']} 지표 수집 실패: {exc}")
            continue

    return list(scan_resources_sequential(resource_list))


def run_detection_cycle_streaming(
    tag_key: str = "Detection",
    tag_value: str = "true",
    resource_types: Optional[list[str]] = None,
    n_points: int = 30,
    period_seconds: int = 300,
) -> Iterator[PipelineState]:
    """run_detection_cycle()과 동일하지만 제너레이터 — 이상이 발견되는 즉시 하나씩 넘겨준다
    (Phase 0의 scan_resources_sequential 특성을 그대로 유지, 다 모았다가 몰아주지 않음)."""
    discovered = discover_all_resources(tag_key, tag_value, resource_types)

    def _resources():
        for r in discovered:
            try:
                yield assemble_resource(r["resource_id"], r["resource_type"], n_points, period_seconds)
            except Exception as exc:
                print(f"[orchestrator] {r['resource_type']} {r['resource_id']} 지표 수집 실패: {exc}")
                continue

    yield from scan_resources_sequential(_resources())
