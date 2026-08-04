import numpy as np
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pipeline.detection_agent import (
    build_unified_feature_matrix,
    ALL_METRICS,
    RESOURCE_TYPES,
)

def test_shape_and_mask():
    metrics = {
        "cost": [1.0, 2.0, 1.5],
        "network_in": [100.0, 120.0, 90.0],
        "network_out": [50.0, 55.0, 48.0],
        "cpu_utilization": [30.0, 35.0, 32.0],
    }
    X = build_unified_feature_matrix("EC2", metrics)

    # 행 개수 = 시점 개수(3), 열 개수 = (지표수*2) + 리소스타입수
    expected_cols = len(ALL_METRICS) * 2 + len(RESOURCE_TYPES)
    assert X.shape == (3, expected_cols), f"실제 shape: {X.shape}, 기대값: (3, {expected_cols})"

    print("✅ shape 통과:", X.shape)

test_shape_and_mask()

def test_mask_values():
    metrics = {
        "cost": [1.0, 2.0],
        "network_in": [100.0, 120.0],
        "network_out": [50.0, 55.0],
        "cpu_utilization": [30.0, 35.0],
    }
    X = build_unified_feature_matrix("EC2", metrics)

    # ALL_METRICS에서 각 지표의 컬럼 위치 찾기 (값 컬럼은 idx*2, 마스크는 idx*2+1)
    for i, metric_name in enumerate(ALL_METRICS):
        value_col = X[:, i * 2]
        mask_col = X[:, i * 2 + 1]
        has_it = metric_name in metrics
        expected_mask = 1.0 if has_it else 0.0
        assert (mask_col == expected_mask).all(), (
            f"{metric_name}: 마스크가 예상과 다름 (있음={has_it}, 실제 마스크={mask_col})"
        )
        if has_it:
            assert np.allclose(value_col, metrics[metric_name]), f"{metric_name} 값 불일치"

    print("✅ 마스크 검증 통과")

test_mask_values()

def test_one_hot():
    metrics = {"cost": [1.0, 2.0]}
    X_ec2 = build_unified_feature_matrix("EC2", metrics)
    X_lambda = build_unified_feature_matrix("Lambda", metrics)

    onehot_start = len(ALL_METRICS) * 2  # one-hot 컬럼이 시작되는 위치

    ec2_onehot = X_ec2[0, onehot_start:]
    lambda_onehot = X_lambda[0, onehot_start:]

    print("EC2 one-hot:   ", ec2_onehot)
    print("Lambda one-hot:", lambda_onehot)

    assert ec2_onehot[RESOURCE_TYPES.index("EC2")] == 1.0
    assert lambda_onehot[RESOURCE_TYPES.index("Lambda")] == 1.0
    assert ec2_onehot.sum() == 1.0  # 딱 하나만 켜져야 함
    assert lambda_onehot.sum() == 1.0

    print("✅ one-hot 검증 통과")

test_one_hot()

def test_module_loads_without_assertion_error():
    # 이 함수가 에러 없이 import를 마쳤다면, state.py와 불일치가 없다는 뜻
    import pipeline.detection_agent  # noqa
    print("✅ RESOURCE_TYPES 일치성 검증 통과 (import 성공)")

test_module_loads_without_assertion_error()

import shutil
from pipeline.detection_agent import (
    detection_node,
    IFOREST_MODEL_DIR,
    IFOREST_UNIFIED_MODEL_NAME,
)

def test_end_to_end_detection_node():

    # 캐시 초기화 (이전 테스트 잔여물 제거)
    if os.path.exists(IFOREST_MODEL_DIR):
        shutil.rmtree(IFOREST_MODEL_DIR)

    # MIN_POINTS_FOR_IFOREST(=5) 이상의 데이터로 정상 케이스 구성
    fake_state = {
        "resource_id": "i-12345",
        "resource_type": "EC2",
        "raw_metrics": {
            "cost": [1.0, 1.1, 1.05, 1.2, 1.1, 15.0],  # 마지막 값이 이상치
            "network_in": [100.0, 105.0, 98.0, 110.0, 102.0, 500.0],
            "network_out": [50.0, 52.0, 49.0, 53.0, 51.0, 55.0],
            "cpu_utilization": [30.0, 32.0, 31.0, 33.0, 30.0, 31.0],
        },
        "timestamp": "2026-08-04T00:00:00Z",
        "anomaly_flag": False,
        "anomaly_score_zscore": None,
        "anomaly_score_iforest": None,
        "triggered_metrics": [],
    }

    result = detection_node(fake_state)

    print("anomaly_flag:", result["anomaly_flag"])
    print("zscore:", result["anomaly_score_zscore"])
    print("iforest:", result["anomaly_score_iforest"])
    print("triggered_metrics:", result["triggered_metrics"])

    # 캐시 파일이 통합 이름으로 생겼는지 확인
    expected_path = os.path.join(IFOREST_MODEL_DIR, f"iforest_{IFOREST_UNIFIED_MODEL_NAME}.pkl")
    assert os.path.exists(expected_path), "통합 모델 캐시 파일이 안 생김"
    print("✅ 통합 모델 캐시 파일 생성 확인:", expected_path)

    # 극단적 스파이크(cost=15.0)가 들어갔으니 최소 zscore는 트리거되어야 함
    assert result["anomaly_flag"] is True
    print("✅ end-to-end 통과")

test_end_to_end_detection_node()

def test_multiple_resource_types_share_cache():
    lambda_state = {
        "resource_id": "func-abc",
        "resource_type": "Lambda",
        "raw_metrics": {
            "invocation_count": [10, 12, 11, 13, 10, 12],
            "error_count": [0, 0, 1, 0, 0, 0],
            "duration_avg": [120.0, 130.0, 125.0, 128.0, 122.0, 121.0],
            "cost": [0.5, 0.55, 0.52, 0.53, 0.51, 0.5],
        },
        "timestamp": "2026-08-04T00:05:00Z",
        "anomaly_flag": False,
        "anomaly_score_zscore": None,
        "anomaly_score_iforest": None,
        "triggered_metrics": [],
    }

    result = detection_node(lambda_state)
    print("Lambda 결과:", result["anomaly_flag"], result["anomaly_score_iforest"])

    # 캐시 파일이 여전히 하나뿐인지 (iforest_EC2.pkl 같은 게 안 생겼는지)
    files = os.listdir(IFOREST_MODEL_DIR)
    print("현재 모델 캐시 파일들:", files)
    assert files == [f"iforest_{IFOREST_UNIFIED_MODEL_NAME}.pkl"], (
        f"통합 모델이 아니라 리소스별 파일이 따로 생김: {files}"
    )
    print("✅ 통합 캐시 재사용 확인")

test_multiple_resource_types_share_cache()

from pipeline.detection_agent import (
    _get_or_train_iforest,
    build_unified_feature_matrix,
    benchmark_iforest_inference,
)

def test_inference_time():
    metrics = {
        "cost": [1.0] * 30,
        "network_in": [100.0] * 30,
        "network_out": [50.0] * 30,
        "cpu_utilization": [30.0] * 30,
    }
    model = _get_or_train_iforest("EC2", metrics)
    X = build_unified_feature_matrix("EC2", metrics)

    result = benchmark_iforest_inference(model, X, n_runs=100)
    print("추론 시간 벤치마크:", result)

    assert result["max_sec"] < 1.0, f"1초 초과! {result['max_sec']}초"
    print("✅ 추론 시간 1초 미만 확인")

test_inference_time()