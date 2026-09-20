"""AutoScaling IForest 학습 버퍼 워밍업.

v6 실험(2026-09-13) 사후분석 결과, request_count 필드를 스키마에 추가하면서 예전
mock 버퍼(33열)와 새 스키마(35열)의 shape이 안 맞아 AutoScaling 학습 버퍼가 사실상
리셋됐고, 그 뒤로 정상 윈도우가 딱 1개만 채택된 채(콜드스타트 상태) 방치돼 있었다.
윈도우 1개로만 학습된 IsolationForest는 분산이 거의 없는 데이터에 과적합돼서 그
다음부터 들어오는 정상 윈도우조차 "이상하다"고 오판해 버퍼 채택 자체를 계속
거부하는 자기강화적 폐쇄 루프에 빠진다(detection_agent.py:392-397 주석에 있는
"창 1개 학습 = 정상 32.7% 오탐" 문제와 동일 메커니즘).

이 스크립트는 실제 실험을 시작하기 전에, 여러 개의 현실적인(약간의 지터가 있는)
정상 베이스라인 윈도우를 buffer_by_type["AutoScaling"]에 직접 채워넣고 그걸로
모델을 한 번 재학습시켜서, 실험 시작 시점에 이미 "정상 범위의 폭"을 어느 정도
학습한 상태로 출발하게 한다. detection_node가 실제로 쓰는 함수(build_unified_
feature_matrix, _fit_and_cache_unified, _save_training_buffer)를 그대로
재사용해서, 워밍업된 결과가 production 경로와 100% 동일한 형식으로 저장되게
한다.

사용법:
    python playground/warmup_autoscaling_iforest_buffer.py
"""
from __future__ import annotations

import random
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import numpy as np

from pipeline.detection_agent import (
    ALL_METRICS,
    build_unified_feature_matrix,
    _fit_and_cache_unified,
    _load_training_buffer,
    _save_training_buffer,
    MAX_WINDOWS_PER_TYPE,
)

N_WARMUP_WINDOWS = 8
WINDOW_POINTS = 30
BASELINE_REQUEST_COUNT_MEAN = 291.0
BASELINE_REQUEST_COUNT_JITTER = 6.0  # 실측 baseline이 289~293 사이였던 것 참고


def _make_baseline_window(seed: int) -> np.ndarray:
    rng = random.Random(seed)
    request_count = [
        BASELINE_REQUEST_COUNT_MEAN + rng.uniform(-BASELINE_REQUEST_COUNT_JITTER, BASELINE_REQUEST_COUNT_JITTER)
        for _ in range(WINDOW_POINTS)
    ]
    metrics = {
        "group_desired_capacity": [1.0] * WINDOW_POINTS,
        "group_in_service_instances": [1.0] * WINDOW_POINTS,
        "request_count": request_count,
        "cost": [0.5 + rng.uniform(-0.02, 0.02) for _ in range(WINDOW_POINTS)],
    }
    return build_unified_feature_matrix("AutoScaling", metrics)


def main() -> None:
    buffer_by_type, _pending = _load_training_buffer()

    windows = [_make_baseline_window(seed=i) for i in range(N_WARMUP_WINDOWS)]
    buffer_by_type["AutoScaling"] = windows[-MAX_WINDOWS_PER_TYPE:]
    _save_training_buffer(buffer_by_type, 0)

    combined = np.vstack([np.vstack(v) for v in buffer_by_type.values() if v])
    _fit_and_cache_unified(combined)

    print(f"AutoScaling 워밍업 완료: {len(windows)}개 정상 윈도우로 재학습")
    print(f"전체 버퍼 타입별 윈도우 수: { {k: len(v) for k, v in buffer_by_type.items()} }")
    print(f"통합 학습 행렬 shape: {combined.shape} (feature 열 수={len(ALL_METRICS) * 2 + 5})")


if __name__ == "__main__":
    main()
