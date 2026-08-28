"""
playground/test_phase0_scan_resources.py

Phase 0 (여러 리소스 순차 스캔 디스패처) 검증 스크립트.
pipeline/detection_agent.py의 scan_resources_sequential(), _build_initial_state() 대상.

[실행 방법]
  프로젝트 루트에서: python playground/test_phase0_scan_resources.py

[검증 항목]
  1. 정상 리소스만 있으면 아무것도 안 나옴 (빈 결과)
  2. 정상/이상이 섞여 있으면 이상 리소스만, 입력 순서 그대로 하나씩 나옴
  3. 제너레이터라 "필요할 때마다 하나씩" 처리됨 (전체를 미리 다 처리하고 몰아주는 게 아님)
  4. _build_initial_state가 PipelineState 필드를 빠짐없이 채움
  5. timestamp 생략 시 자동으로 채워짐
"""

import os
import shutil
import sys
import typing
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# 모델 캐시를 테스트 전용 임시 디렉토리로 격리
MODEL_DIR = str(PROJECT_ROOT / ".tmp_models_phase0_test")
os.environ["PIPELINE_MODEL_DIR"] = MODEL_DIR


def _reset_model_cache():
    if os.path.exists(MODEL_DIR):
        shutil.rmtree(MODEL_DIR)


_reset_model_cache()

from schema.state import PipelineState
from pipeline.detection_agent import scan_resources_sequential, _build_initial_state


# ── 공통 더미 리소스 ─────────────────────────────────────────────────────────

def _normal_ec2(resource_id: str) -> dict:
    return {
        "resource_id": resource_id,
        "resource_type": "EC2",
        "raw_metrics": {
            "cpu_utilization": [50.0] * 30,
            "network_in":      [1000.0] * 30,
            "network_out":     [800.0] * 30,
            "cost":            [2.0] * 30,
        },
    }


def _spike_ec2(resource_id: str) -> dict:
    # detection_node의 알림 판단은 지속성 체크(최근 PERSISTENCE_WINDOW_POINTS=3개가
    # 전부 임계값을 넘어야 트리거)라서, 마지막 1개만 튀우면 더 이상 안 잡힌다 —
    # 최근 3개를 전부 스파이크시켜야 함 (pipeline/detection_agent.py의
    # _zscore_check_persistent 참고).
    return {
        "resource_id": resource_id,
        "resource_type": "EC2",
        "raw_metrics": {
            "cpu_utilization": [50.0] * 27 + [95.0] * 3,
            "network_in":      [1000.0] * 27 + [50000.0] * 3,
            "network_out":     [800.0] * 30,
            "cost":            [2.0] * 27 + [20.0] * 3,
        },
    }


def _spike_lambda(resource_id: str) -> dict:
    # EC2와 동일한 이유로 최근 3개를 전부 스파이크시킴.
    return {
        "resource_id": resource_id,
        "resource_type": "Lambda",
        "raw_metrics": {
            "invocation_count": [100.0] * 27 + [50000.0] * 3,
            "error_count":      [1.0] * 30,
            "duration_avg":     [200.0] * 30,
            "cost":             [0.1] * 27 + [20.0] * 3,
        },
    }


# ── 1. 정상 리소스만 있으면 빈 결과 ────────────────────────────────────────────

def test_all_normal_yields_nothing():
    _reset_model_cache()
    resources = [_normal_ec2("i-normal-1"), _normal_ec2("i-normal-2")]
    results = list(scan_resources_sequential(resources))
    assert results == [], f"정상 리소스만 있는데 뭔가 나옴: {[r['resource_id'] for r in results]}"
    print("✅ [1] 정상 리소스만 있으면 빈 결과")


# ── 2. 이상 리소스만, 입력 순서 그대로 하나씩 ──────────────────────────────────

def test_only_anomalies_in_order():
    _reset_model_cache()
    resources = [
        _normal_ec2("i-normal-1"),
        _spike_ec2("i-spike-2"),
        _spike_lambda("func-spike-3"),
        _normal_ec2("i-normal-4"),
        _spike_ec2("i-spike-5"),
    ]
    results = list(scan_resources_sequential(resources))
    ids = [r["resource_id"] for r in results]

    assert ids == ["i-spike-2", "func-spike-3", "i-spike-5"], f"순서/필터링 불일치: {ids}"
    assert all(r["anomaly_flag"] for r in results), "yield된 결과는 전부 anomaly_flag=True여야 함"
    print("✅ [2] 정상은 스킵, 이상 리소스만 입력 순서 그대로 방출:", ids)


# ── 3. 제너레이터 = 스트리밍 (미리 다 처리하고 몰아주는 게 아님) ────────────────

def test_lazy_streaming_not_batched():
    _reset_model_cache()

    processed_order = []
    resources_raw = [
        ("i-spike-1", _spike_ec2),
        ("i-normal-2", _normal_ec2),
        ("i-spike-3", _spike_ec2),
        ("i-normal-4", _normal_ec2),
    ]

    def tracking_resources():
        """제너레이터가 실제로 하나씩 당겨쓰는지 side-effect로 추적."""
        for rid, builder in resources_raw:
            processed_order.append(rid)   # detection_node에 넘기기 직전에 기록
            yield builder(rid)

    gen = scan_resources_sequential(tracking_resources())

    # 아직 아무것도 consume 안 했으면, 내부에서 미리 처리된 것도 없어야 함
    assert processed_order == [], f"아직 next() 호출 전인데 벌써 처리됨(배치 처리 의심): {processed_order}"

    first = next(gen)
    assert first["resource_id"] == "i-spike-1"
    # 첫 이상 리소스를 얻기 위해 딱 1개만 처리했어야 함 (뒤에 남은 3개를 미리 안 봄)
    assert processed_order == ["i-spike-1"], f"스트리밍이 아니라 미리 여러 개 처리함: {processed_order}"

    second = next(gen)
    assert second["resource_id"] == "i-spike-3"
    # 두 번째 이상을 찾으려고 중간의 정상(i-normal-2)까지만 추가로 처리됐어야 함
    assert processed_order == ["i-spike-1", "i-normal-2", "i-spike-3"], (
        f"기대와 다른 처리 순서: {processed_order}"
    )

    remaining = list(gen)
    assert remaining == [], "남은 건 정상 리소스뿐이라 더 나오면 안 됨"
    assert processed_order == ["i-spike-1", "i-normal-2", "i-spike-3", "i-normal-4"]

    print("✅ [3] 제너레이터가 실제로 하나씩 지연 평가(streaming)됨 — 배치 처리 아님")


# ── 4. _build_initial_state가 PipelineState 필드를 빠짐없이 채움 ──────────────

def test_build_initial_state_covers_all_fields():
    resource = {
        "resource_id": "i-abc",
        "resource_type": "EC2",
        "raw_metrics": {"cost": [1.0, 2.0]},
        "timestamp": "2026-01-01T00:00:00Z",
    }
    state = _build_initial_state(resource)

    expected_keys = set(typing.get_type_hints(PipelineState).keys())
    actual_keys = set(state.keys())

    missing = expected_keys - actual_keys
    extra = actual_keys - expected_keys
    assert not missing, f"PipelineState에 있는데 안 채워진 필드: {missing}"
    assert not extra, f"PipelineState에 없는 엉뚱한 필드: {extra}"

    assert state["resource_id"] == "i-abc"
    assert state["rollback_count"] == 0
    assert state["whitelisted"] is False
    assert state["log_entries"] == []
    print(f"✅ [4] PipelineState {len(expected_keys)}개 필드 전부 채워짐 (누락/여분 없음)")


# ── 5. timestamp 생략 시 자동으로 채워짐 ──────────────────────────────────────

def test_timestamp_defaults_when_missing():
    resource = {
        "resource_id": "i-no-ts",
        "resource_type": "EC2",
        "raw_metrics": {"cost": [1.0]},
        # timestamp 없음
    }
    state = _build_initial_state(resource)
    assert state["timestamp"], "timestamp가 비어있으면 안 됨"
    assert "T" in state["timestamp"], f"ISO 8601 형식이 아님: {state['timestamp']}"
    print("✅ [5] timestamp 생략 시 자동으로 현재 시각 채워짐:", state["timestamp"])


if __name__ == "__main__":
    test_all_normal_yields_nothing()
    test_only_anomalies_in_order()
    test_lazy_streaming_not_batched()
    test_build_initial_state_covers_all_fields()
    test_timestamp_defaults_when_missing()

    _reset_model_cache()
    print("\n✅ Phase 0 전체 테스트 통과")
