import numpy as np
import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

# ⚠️ 이 테스트는 아래에서 shutil.rmtree(IFOREST_MODEL_DIR)로 모델 캐시를 반복적으로
# 지운다. PIPELINE_MODEL_DIR을 지정 안 하면 pipeline/detection_agent.py의
# IFOREST_MODEL_DIR이 프로덕션과 동일한 "models/" 디렉터리를 가리켜서, 실 AWS
# 파일럿(run_full_pipeline.py 등)이 쌓아온 학습 버퍼가 이 테스트 실행 한 번에 통째로
# 날아가는 사고가 있었다 — validate_real_aws_buffer.py처럼 테스트 전용 디렉터리로
# 격리한다. import 시점에 IFOREST_MODEL_DIR이 확정되므로 pipeline.detection_agent를
# import하기 전에 반드시 설정해야 한다.
os.environ.setdefault(
    "PIPELINE_MODEL_DIR", os.path.join(PROJECT_ROOT, ".test_models_detection_agent")
)

from pipeline.detection_agent import (
    build_unified_feature_matrix,
    ALL_METRICS,
    RESOURCE_TYPES,
    DERIVED_FEATURE_NAMES,
)

def test_shape_and_mask():
    metrics = {
        "cost": [1.0, 2.0, 1.5],
        "network_in": [100.0, 120.0, 90.0],
        "network_out": [50.0, 55.0, 48.0],
        "cpu_utilization": [30.0, 35.0, 32.0],
    }
    X = build_unified_feature_matrix("EC2", metrics)

    # 행 개수 = 시점 개수(3), 열 개수 = (지표수*2) + 파생feature수(ec2_idle_flag/
    # lambda_error_rate, 2026-09-12 추가) + 리소스타입수
    expected_cols = len(ALL_METRICS) * 2 + len(DERIVED_FEATURE_NAMES) + len(RESOURCE_TYPES)
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

    onehot_start = len(ALL_METRICS) * 2 + len(DERIVED_FEATURE_NAMES)  # one-hot 컬럼이 시작되는 위치

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

    # detection_node는 이제 최근 PERSISTENCE_WINDOW_POINTS(기본 3)개 시점이 연속으로
    # 임계값을 넘어야 트리거하므로(지속성 체크), 마지막 1개만 스파이크인 데이터로는
    # 더 이상 안 잡힘 — 실제 운영 윈도우 크기(30포인트)에 맞춰 마지막 3개가 지속적으로
    # 스파이크인 데이터로 구성 (n이 너무 작으면 스파이크가 평균/표준편차 자체를
    # 끌어올려서 z-score가 잘 안 오르는 문제도 있음 — 페이지 4/persistence 관련 논의 참고)
    n_normal = 27
    normal_metrics = {
        "cost":            [1.0 + 0.02 * ((i % 5) - 2) for i in range(30)],
        "network_in":      [100.0 + 2 * ((i % 5) - 2) for i in range(30)],
        "network_out":     [50.0 + 1 * ((i % 5) - 2) for i in range(30)],
        "cpu_utilization": [30.0 + 1 * ((i % 5) - 2) for i in range(30)],
    }
    fake_state = {
        "resource_id": "i-12345",
        "resource_type": "EC2",
        "raw_metrics": {
            "cost":            [1.0 + 0.02 * ((i % 5) - 2) for i in range(n_normal)] + [15.0, 15.2, 14.8],
            "network_in":      [100.0 + 2 * ((i % 5) - 2) for i in range(n_normal)] + [500.0, 480.0, 510.0],
            "network_out":     [50.0 + 1 * ((i % 5) - 2) for i in range(n_normal)] + [50.0, 51.0, 49.0],
            "cpu_utilization": [30.0 + 1 * ((i % 5) - 2) for i in range(n_normal)] + [31.0, 30.0, 30.0],
        },
        "timestamp": "2026-08-04T00:00:00Z",
        "anomaly_flag": False,
        "anomaly_score_zscore": None,
        "anomaly_score_iforest": None,
        "triggered_metrics": [],
    }

    # 콜드스타트 시드는 이제 그 순간 이상해 보이면 거부되므로(실 AWS 파일럿에서 발견한
    # 버퍼 자기오염 문제 수정 — 아래 test_cold_start_seed_rejects_contaminated_window 참고),
    # 먼저 정상 데이터로 한 번 시드해서 모델을 만들어둔 뒤에 스파이크 데이터를 넣는다.
    detection_node({
        "resource_id": "i-12345", "resource_type": "EC2", "raw_metrics": normal_metrics,
        "timestamp": "2026-08-04T00:00:00Z", "anomaly_flag": False,
        "anomaly_score_zscore": None, "anomaly_score_iforest": None, "triggered_metrics": [],
    })

    result = detection_node(fake_state)

    print("anomaly_flag:", result["anomaly_flag"])
    print("zscore:", result["anomaly_score_zscore"])
    print("iforest:", result["anomaly_score_iforest"])
    print("triggered_metrics:", result["triggered_metrics"])

    # 캐시 파일이 통합 이름으로 생겼는지 확인
    expected_path = os.path.join(IFOREST_MODEL_DIR, f"iforest_{IFOREST_UNIFIED_MODEL_NAME}.pkl")
    assert os.path.exists(expected_path), "통합 모델 캐시 파일이 안 생김"
    print("✅ 통합 모델 캐시 파일 생성 확인:", expected_path)

    # 3개 시점 연속 스파이크(지속성 조건 충족)가 들어갔으니 트리거되어야 함
    assert result["anomaly_flag"] is True
    print("✅ end-to-end 통과")

test_end_to_end_detection_node()


def test_cold_start_seed_rejects_contaminated_window():
    """실 AWS 파일럿 테스트에서 발견한 버퍼 자기오염 사고 재현 + 수정 검증.

    사고: 부하 테스트로 Lambda를 반복 호출하던 중, 하필 그 순간에 콜드스타트가
    겹쳐서 "이상해 보이는 윈도우"가 아무 검사 없이 그대로 시드 모델로 확정
    학습됐다 — 그 뒤로 모델이 "이 정도 부하는 정상"이라고 계속 오판. IForest
    점수는 모델이 없어 콜드스타트 시점엔 원천적으로 못 보지만, Z-score는 모델
    없이도 계산 가능하므로 최소한 이거라도 걸어서, 첫 윈도우 자체가 이미
    이상해 보이면 시드를 거부하고 다음 사이클에 재시도하도록 고쳤다.
    """
    if os.path.exists(IFOREST_MODEL_DIR):
        shutil.rmtree(IFOREST_MODEL_DIR)

    # 정상(0)이 대부분이고 마지막 몇 개만 튄, 딱 사고 재현 상황과 같은 윈도우
    contaminated = {
        "invocation_count": [0.0] * 24 + [50.0, 109.0, 141.0, 100.0, 110.0, 137.0],
        "error_count":      [0.0] * 30,
        "duration_avg":     [0.0] * 24 + [0.5, 1.2, 2.3, 1.0, 1.1, 1.8],
        "cost":             [0.0] * 24 + [1e-5] * 6,
    }
    normal = {
        "invocation_count": [0.0] * 30,
        "error_count":      [0.0] * 30,
        "duration_avg":     [0.0] * 30,
        "cost":              [0.0] * 30,
    }

    # 오염된 윈도우로 여러 번 시도해도 시드가 절대 확정되면 안 됨
    for i in range(5):
        result = detection_node({
            "resource_id": "func-pilot", "resource_type": "Lambda", "raw_metrics": contaminated,
            "timestamp": "2026-08-29T00:00:00Z", "anomaly_flag": False,
            "anomaly_score_zscore": None, "anomaly_score_iforest": None, "triggered_metrics": [],
        })
        assert result["anomaly_score_iforest"] == 0.0, (
            f"{i+1}번째 시도에서 IForest가 오염된 윈도우로 시드됨 (콜드스타트 게이팅 실패)"
        )
    expected_path = os.path.join(IFOREST_MODEL_DIR, f"iforest_{IFOREST_UNIFIED_MODEL_NAME}.pkl")
    assert not os.path.exists(expected_path), "오염된 윈도우인데도 모델 캐시 파일이 생성됨"
    print("✅ 오염된 윈도우 5회 시도 전부 시드 거부 확인 (캐시 파일 미생성)")

    # 정상 데이터가 들어오면 그제서야 시드되어야 함
    detection_node({
        "resource_id": "func-pilot", "resource_type": "Lambda", "raw_metrics": normal,
        "timestamp": "2026-08-29T00:05:00Z", "anomaly_flag": False,
        "anomaly_score_zscore": None, "anomaly_score_iforest": None, "triggered_metrics": [],
    })
    assert os.path.exists(expected_path), "정상 윈도우인데도 시드가 안 됨"
    print("✅ 정상 윈도우가 들어오자 정상적으로 시드됨")

    if os.path.exists(IFOREST_MODEL_DIR):
        shutil.rmtree(IFOREST_MODEL_DIR)

test_cold_start_seed_rejects_contaminated_window()


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

    # 캐시 파일이 리소스 타입별로 따로(iforest_EC2.pkl 등) 안 생기고, 통합 모델
    # 파일 + 학습 버퍼(어떤 리소스 타입을 학습에 반영했는지 추적용) 2개만 있어야 함
    files = sorted(os.listdir(IFOREST_MODEL_DIR))
    print("현재 모델 캐시 파일들:", files)
    expected = sorted([
        f"iforest_{IFOREST_UNIFIED_MODEL_NAME}.pkl",
        "iforest_unified_train_buffer.pkl",
    ])
    assert files == expected, (
        f"통합 모델이 아니라 리소스별 파일이 따로 생김: {files}"
    )
    print("✅ 통합 캐시 재사용 확인 (리소스 타입별 파일 없음)")

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

from pipeline.detection_agent import (
    _zscore_check,
    _zscore_check_persistent,
    PERSISTENCE_WINDOW_POINTS,
)


def test_zscore_persistent_ignores_past_spike():
    """윈도우 중간에 스파이크가 있었지만 최근(마지막 PERSISTENCE_WINDOW_POINTS개)은
    정상으로 돌아온 경우:
    - window-max(_zscore_check, 학습 버퍼 채택 판단용)는 과거 스파이크 때문에 여전히 트리거되어야 함
    - persistent(_zscore_check_persistent, detection_node 알림 판단용)는 트리거되면 안 됨
    """
    # index 6에서만 스파이크, 나머지는 전부 평상시 수준 (n=12 — n이 너무 작으면
    # 스파이크 값을 아무리 키워도 window-max z가 이론적 상한(sqrt(n-1))에 막혀
    # threshold를 못 넘음. n=12일 때 상한은 sqrt(11)=3.32로 충분히 여유 있음)
    values = [1.0, 1.05, 1.1, 0.95, 1.0, 1.02, 100.0, 0.98, 1.03, 1.0, 1.01, 0.99]

    window_max_z, window_max_triggered = _zscore_check(values)
    persistent_z, persistent_triggered = _zscore_check_persistent(values)

    print("window-max:", window_max_z, window_max_triggered)
    print("persistent:", persistent_z, persistent_triggered)

    assert window_max_triggered is True, "window-max 방식은 과거 스파이크로 여전히 트리거되어야 함"
    assert persistent_triggered is False, "persistent 방식은 값이 회복되면 트리거되면 안 됨"
    assert persistent_z < window_max_z, "persistent z는 window-max z보다 작아야 함"

    print("✅ 스파이크 회복 후 persistent 방식만 알림이 해제됨을 확인")

test_zscore_persistent_ignores_past_spike()


def test_zscore_persistent_ignores_single_blip():
    """지속성 체크의 핵심 동작: 최근 시점 중 딱 1개(마지막)만 튄 순간적 노이즈는
    트리거되면 안 됨 — 최근 PERSISTENCE_WINDOW_POINTS개가 '전부' 넘어야 하므로,
    마지막 1개만 넘는 건 부족하다."""
    values = [1.0, 1.05, 0.98, 1.02, 1.0, 1.01, 0.99, 1.03, 50.0]  # 마지막 값만 스파이크

    z, triggered = _zscore_check_persistent(values)
    print(f"single blip (k={PERSISTENCE_WINDOW_POINTS}):", z, triggered)

    assert triggered is False, "마지막 1개 시점만 튄 노이즈는 지속성 체크에서 걸러져야 함"
    print("✅ 순간적인 노이즈 튐 한 번은 트리거되지 않음을 확인 (flapping 방지)")

test_zscore_persistent_ignores_single_blip()


def test_zscore_persistent_triggers_on_sustained_spike():
    """최근 PERSISTENCE_WINDOW_POINTS개 시점이 전부 지속적으로 이상이면 트리거되어야 함
    (회귀 방지용 — 지속성 체크를 도입하면서 "진짜 지속되는 이상"까지 못 잡게 되면 안 됨).
    실제 운영 윈도우 크기(30포인트)에 맞춰 구성 — n이 작으면 스파이크가 평균/표준편차
    자체를 끌어올려서 z-score가 잘 안 오르는 문제가 있음(마스킹 효과)."""
    n_normal = 27
    values = [1.0 + 0.02 * ((i % 5) - 2) for i in range(n_normal)] + [15.0, 15.2, 14.8]

    z, triggered = _zscore_check_persistent(values)
    print(f"sustained spike (k={PERSISTENCE_WINDOW_POINTS}):", z, triggered)

    assert triggered is True, "최근 3개 시점이 계속 이상이면 트리거되어야 함"
    print("✅ 지속되는 이상은 여전히 잡힘을 확인")

test_zscore_persistent_triggers_on_sustained_spike()


def test_detection_node_clears_after_spike_recovers():
    """detection_node 통합 테스트: 윈도우 중간에 cost 스파이크가 있었지만 최근 값들은
    정상으로 회복된 상황 → triggered_metrics에 'cost'가 없어야 함 (예전 window-max
    방식이면 여기서 트리거됨). IForest도 트리거 안 되도록 다른 지표는 전부 평탄하게 구성."""
    if os.path.exists(IFOREST_MODEL_DIR):
        shutil.rmtree(IFOREST_MODEL_DIR)

    fake_state = {
        "resource_id": "i-recovered",
        "resource_type": "EC2",
        "raw_metrics": {
            # index 3에서만 스파이크(50.0), 이후 6개 포인트는 전부 평상시 수준으로 회복
            "cost":              [1.0, 1.05, 1.1, 50.0, 1.02, 0.98, 1.03, 1.0, 1.01, 0.99],
            "network_in":        [100.0, 102.0, 98.0, 101.0, 99.0, 100.0, 101.0, 100.0, 99.0, 100.0],
            "network_out":       [50.0, 51.0, 49.0, 50.0, 50.0, 51.0, 49.0, 50.0, 50.0, 50.0],
            "cpu_utilization":   [30.0, 31.0, 30.0, 31.0, 30.0, 30.0, 31.0, 30.0, 30.0, 31.0],
        },
        "timestamp": "2026-08-28T00:00:00Z",
        "anomaly_flag": False,
        "anomaly_score_zscore": None,
        "anomaly_score_iforest": None,
        "triggered_metrics": [],
    }

    result = detection_node(fake_state)
    print("triggered_metrics:", result["triggered_metrics"])
    print("anomaly_flag:", result["anomaly_flag"])
    print("zscore:", result["anomaly_score_zscore"], "iforest:", result["anomaly_score_iforest"])

    assert "cost" not in result["triggered_metrics"], (
        "값이 회복됐는데도 과거 스파이크 때문에 cost가 여전히 트리거됨 (window-max로 되돌아간 회귀)"
    )
    print("✅ 과거 스파이크가 윈도우에 남아있어도 값이 회복되면 더 이상 트리거되지 않음을 확인")

    # 캐시 정리 (다음 테스트 실행에 영향 안 주도록)
    if os.path.exists(IFOREST_MODEL_DIR):
        shutil.rmtree(IFOREST_MODEL_DIR)

test_detection_node_clears_after_spike_recovers()


from pipeline.detection_agent import (
    _low_utilization_check,
    EC2_IDLE_CPU_THRESHOLD_PCT,
    EC2_IDLE_NETWORK_IO_BYTES_THRESHOLD,
    EC2_IDLE_TARGET_METRICS,
)


def test_low_utilization_check_flags_flat_idle_ec2():
    """시나리오 1 (좀비 EC2): 윈도우 내내 CPU/네트워크 둘 다 낮고 변동이 없는 케이스.
    z-score는 window 내부 평균/표준편차 기준이라 이런 무변동 저사용 패턴을 못 잡는데
    (z≈0), 이 절대임계값 체크는 window 전체의 peak CPU/합산 network I/O를 직접
    보므로 잡아야 한다."""
    metrics = {
        "cpu_utilization": [1.5] * 30,
        "network_in":      [500.0] * 30,
        "network_out":     [300.0] * 30,
        "cost":             [0.05] * 30,
    }
    triggered, is_idle = _low_utilization_check("EC2", metrics)

    assert is_idle is True, "CPU/네트워크 둘 다 임계값 이하로 지속되면 유휴로 판정되어야 함"
    assert set(triggered) == set(EC2_IDLE_TARGET_METRICS), (
        f"트리거 시 cpu_utilization/network_in/network_out이 함께 반환되어야 함: {triggered}"
    )
    print("✅ 완전 평평한 저사용률 EC2 윈도우 → 유휴 판정 확인")

test_low_utilization_check_flags_flat_idle_ec2()


def test_low_utilization_check_requires_both_cpu_and_network_low():
    """AWS Compute Optimizer 기준은 CPU AND network I/O 둘 다 낮아야 idle이다.
    CPU만 낮고 network_in/out은 정상 범위(예: 트래픽만 통과시키는 인스턴스)면
    idle이 아니어야 한다 — 이걸 false negative로 오인하지 않도록 회귀 방지용으로
    남겨둔다 (시나리오 1 케이스 D 논의 참고)."""
    metrics = {
        "cpu_utilization": [2.0] * 30,       # 임계값(5%) 이하
        "network_in":      [50000.0] * 30,   # 임계값을 훨씬 초과하는 정상 트래픽
        "network_out":     [30000.0] * 30,
    }
    triggered, is_idle = _low_utilization_check("EC2", metrics)

    assert is_idle is False, "CPU만 낮고 network는 정상이면 idle로 판정되면 안 됨(AND 조건)"
    assert triggered == []
    print("✅ CPU만 낮고 network는 정상인 케이스는 idle로 판정되지 않음을 확인 (AND 시맨틱스)")

test_low_utilization_check_requires_both_cpu_and_network_low()


def test_low_utilization_check_boundary_network_io():
    """network I/O 합계가 임계값과 정확히 같으면(<=) 트리거되고, 아주 조금이라도
    넘으면 트리거되지 않아야 한다 (경계값 검증)."""
    n = 30
    cpu_safe = [1.0] * n  # cpu는 임계값과 충분히 떨어뜨려서 network만 경계 테스트

    at_boundary = {
        "cpu_utilization": cpu_safe,
        "network_in": [EC2_IDLE_NETWORK_IO_BYTES_THRESHOLD / n] * n,
        "network_out": [0.0] * n,
    }
    _, is_idle_at = _low_utilization_check("EC2", at_boundary)
    assert is_idle_at is True, "network I/O 합계가 임계값과 정확히 같으면 트리거되어야 함"

    over_boundary = {
        "cpu_utilization": cpu_safe,
        "network_in": [(EC2_IDLE_NETWORK_IO_BYTES_THRESHOLD + n) / n] * n,  # 총합 임계값 + n bytes
        "network_out": [0.0] * n,
    }
    _, is_idle_over = _low_utilization_check("EC2", over_boundary)
    assert is_idle_over is False, "network I/O 합계가 임계값을 조금이라도 넘으면 트리거되면 안 됨"

    print("✅ network I/O 임계값 경계(<=) 동작 확인")

test_low_utilization_check_boundary_network_io()


def test_low_utilization_check_boundary_cpu():
    """peak CPU가 임계값과 정확히 같으면(<=) 트리거되고, 아주 조금이라도 넘으면
    트리거되지 않아야 한다."""
    n = 30
    network_safe = {"network_in": [0.0] * n, "network_out": [0.0] * n}  # network는 충분히 낮게 고정

    at_boundary = {"cpu_utilization": [EC2_IDLE_CPU_THRESHOLD_PCT] * n, **network_safe}
    _, is_idle_at = _low_utilization_check("EC2", at_boundary)
    assert is_idle_at is True, "peak CPU가 임계값과 정확히 같으면 트리거되어야 함"

    over_boundary = {
        "cpu_utilization": [EC2_IDLE_CPU_THRESHOLD_PCT] * (n - 1) + [EC2_IDLE_CPU_THRESHOLD_PCT + 0.01],
        **network_safe,
    }
    _, is_idle_over = _low_utilization_check("EC2", over_boundary)
    assert is_idle_over is False, "peak CPU가 임계값을 조금이라도 넘으면 트리거되면 안 됨"

    print("✅ CPU 임계값 경계(<=) 동작 확인")

test_low_utilization_check_boundary_cpu()


def test_low_utilization_check_scoped_to_ec2_only():
    """이번 범위는 EC2 전용 — RDS는 cpu_utilization 필드가 있어도(schema/state.py의
    RDSMetrics 참고) network_in/network_out이 없어 어차피 조건을 못 채우지만,
    resource_type 게이트 자체도 명시적으로 확인해 향후 회귀를 방지한다."""
    rds_metrics = {
        "cpu_utilization": [1.0] * 30,
        "database_connections": [0.0] * 30,
        "read_iops": [0.0] * 30,
        "write_iops": [0.0] * 30,
    }
    triggered, is_idle = _low_utilization_check("RDS", rds_metrics)
    assert (triggered, is_idle) == ([], False), "RDS는 이번 범위에서 제외되어야 함"
    print("✅ RDS는 이번 범위에서 제외됨을 확인 (resource_type 게이트)")

test_low_utilization_check_scoped_to_ec2_only()


def test_detection_node_flags_idle_ec2_via_absolute_threshold():
    """detection_node 통합 테스트: 완전 평평한 저사용률 윈도우를 넣었을 때
    anomaly_flag=True가 되고, triggered_metrics에 이번에 추가한 세 지표가
    포함되는지 확인 (z-score/IForest 콜드스타트 값과 무관하게 절대임계값
    체크만으로도 탐지가 성립함을 검증)."""
    if os.path.exists(IFOREST_MODEL_DIR):
        shutil.rmtree(IFOREST_MODEL_DIR)

    fake_state = {
        "resource_id": "i-zombie",
        "resource_type": "EC2",
        "raw_metrics": {
            "cpu_utilization": [1.5] * 30,
            "network_in":      [500.0] * 30,
            "network_out":     [300.0] * 30,
            "cost":             [0.05] * 30,
        },
        "timestamp": "2026-09-05T00:00:00Z",
        "anomaly_flag": False,
        "anomaly_score_zscore": None,
        "anomaly_score_iforest": None,
        "triggered_metrics": [],
    }

    result = detection_node(fake_state)
    print("triggered_metrics:", result["triggered_metrics"])
    print("anomaly_flag:", result["anomaly_flag"])

    assert result["anomaly_flag"] is True, "저사용률 EC2는 탐지되어야 함"
    assert set(EC2_IDLE_TARGET_METRICS) <= set(result["triggered_metrics"]), (
        "triggered_metrics에 cpu_utilization/network_in/network_out이 포함되어야 함"
    )
    print("✅ detection_node가 절대임계값 체크로 저사용률 EC2를 탐지함을 확인")

    if os.path.exists(IFOREST_MODEL_DIR):
        shutil.rmtree(IFOREST_MODEL_DIR)

test_detection_node_flags_idle_ec2_via_absolute_threshold()


_EC2_IDLE_WINDOW_SECONDS = 30 * 300  # n_points × period_seconds 기본값 = 2.5시간(=9000초)


def test_low_utilization_check_holds_judgment_for_young_instance():
    """2026-09-05 실 AWS 테스트에서 발견: 막 생성된 EC2는 윈도우 대부분이 진짜
    유휴가 아니라 '아직 이력이 없어서 0으로 채워진' 값이라, 부팅 트래픽마저 작았다면
    즉시 좀비로 오판될 수 있다. 나이가 윈도우 길이보다 어리면 판단을 보류해야 한다."""
    idle_looking_metrics = {
        "cpu_utilization": [0.0] * 30,
        "network_in":      [0.0] * 30,
        "network_out":     [0.0] * 30,
    }
    young_age = _EC2_IDLE_WINDOW_SECONDS - 1  # 윈도우 길이보다 1초 어림

    triggered, is_idle = _low_utilization_check("EC2", idle_looking_metrics, young_age)

    assert (triggered, is_idle) == ([], False), (
        "생성된 지 윈도우 길이도 안 된 인스턴스는 지표가 idle처럼 보여도 판단을 보류해야 함"
    )
    print("✅ 신생 인스턴스(윈도우 길이 미만)는 idle 판단이 보류됨을 확인")

test_low_utilization_check_holds_judgment_for_young_instance()


def test_low_utilization_check_still_triggers_for_mature_instance():
    """가드가 진짜 좀비까지 막아버리면 안 됨 — 나이가 윈도우 길이 이상이면
    (즉 가드에 안 걸리면) 기존처럼 정상적으로 트리거되어야 한다 (회귀 방지)."""
    idle_metrics = {
        "cpu_utilization": [1.5] * 30,
        "network_in":      [500.0] * 30,
        "network_out":     [300.0] * 30,
    }
    mature_age = _EC2_IDLE_WINDOW_SECONDS  # 정확히 윈도우 길이 = 판단 허용 경계

    triggered, is_idle = _low_utilization_check("EC2", idle_metrics, mature_age)

    assert is_idle is True, "나이가 윈도우 길이 이상이면 가드에 안 걸리고 정상 판단돼야 함"
    assert set(triggered) == set(EC2_IDLE_TARGET_METRICS)
    print("✅ 나이가 윈도우 길이 이상인 진짜 유휴 인스턴스는 여전히 잡힘을 확인")

test_low_utilization_check_still_triggers_for_mature_instance()


def test_low_utilization_check_unknown_age_keeps_old_behavior():
    """resource_age_seconds를 안 주면(None, 기본값) 가드가 작동하지 않고 기존
    동작(오늘 이전까지의 동작) 그대로 유지되어야 한다 — 하위호환 확인.
    실제로 오늘 실 AWS 테스트 인스턴스가 이 경로를 탔다(당시엔 나이 정보 자체가 없었음)."""
    idle_metrics = {
        "cpu_utilization": [1.5] * 30,
        "network_in":      [500.0] * 30,
        "network_out":     [300.0] * 30,
    }
    triggered, is_idle = _low_utilization_check("EC2", idle_metrics)  # resource_age_seconds 생략

    assert is_idle is True, "나이 정보가 없으면(None) 가드 없이 기존처럼 판단해야 함"
    assert set(triggered) == set(EC2_IDLE_TARGET_METRICS)
    print("✅ 나이 정보 없음(None)이면 하위호환대로 기존 판단 로직이 그대로 적용됨을 확인")

test_low_utilization_check_unknown_age_keeps_old_behavior()


from pipeline.detection_agent import (
    _lambda_error_rate_check,
    LAMBDA_ERROR_RATE_THRESHOLD,
    LAMBDA_ERROR_RATE_MIN_INVOCATIONS,
)


def _lambda_metrics(invocation_last3, error_last3):
    """최근 3개 포인트만 의미 있고 나머지 27개는 평상시 수준(정상)으로 채운
    Lambda raw_metrics. 지속성 체크가 '최근 k개'만 보므로 앞부분은 무관."""
    n_normal = 27
    return {
        "invocation_count": [20.0] * n_normal + invocation_last3,
        "error_count":      [1.0] * n_normal + error_last3,
        "duration_avg":     [100.0] * 30,
    }


def test_lambda_error_rate_check_flags_sustained_surge():
    """시나리오 4 핵심 케이스: 최근 3포인트 연속 invocation>=10 & error_rate>=50%."""
    metrics = _lambda_metrics(
        invocation_last3=[20.0, 20.0, 20.0],
        error_last3=[10.0, 12.0, 15.0],  # 50%, 60%, 75%
    )
    triggered, is_surge = _lambda_error_rate_check("Lambda", metrics)

    assert is_surge is True
    assert set(triggered) == {"error_count", "invocation_count"}
    print("✅ 최근 3포인트 연속 error_rate>=50% & invocation>=10 -> 트리거 확인")

test_lambda_error_rate_check_flags_sustained_surge()


def test_lambda_error_rate_check_boundary():
    """error_rate가 정확히 임계값이면 트리거, 아주 조금이라도 못 미치면 트리거 안 됨 (경계값).
    LAMBDA_ERROR_RATE_THRESHOLD를 하드코딩하지 않고 상수를 그대로 참조해서, 나중에
    임계값이 조정돼도 이 테스트가 깨지지 않게 한다 (EC2 경계값 테스트와 동일 패턴)."""
    inv = LAMBDA_ERROR_RATE_MIN_INVOCATIONS
    at_err = inv * LAMBDA_ERROR_RATE_THRESHOLD  # 정확히 임계값 비율

    at_boundary = _lambda_metrics(
        invocation_last3=[inv, inv, inv],
        error_last3=[at_err, at_err, at_err],
    )
    _, is_surge_at = _lambda_error_rate_check("Lambda", at_boundary)
    assert is_surge_at is True, "error_rate가 임계값과 정확히 같으면(>=) 트리거되어야 함"

    under_boundary = _lambda_metrics(
        invocation_last3=[inv, inv, inv],
        error_last3=[at_err - 0.01, at_err - 0.01, at_err - 0.01],
    )
    _, is_surge_under = _lambda_error_rate_check("Lambda", under_boundary)
    assert is_surge_under is False, "error_rate가 임계값에 조금이라도 못 미치면 트리거되면 안 됨"

    print(f"✅ error_rate {LAMBDA_ERROR_RATE_THRESHOLD:.0%} 경계(>=) 동작 확인")

test_lambda_error_rate_check_boundary()


def test_lambda_error_rate_check_min_invocation_gate():
    """호출수가 게이트(LAMBDA_ERROR_RATE_MIN_INVOCATIONS) 미만이면 error_rate가 100%여도
    트리거 안 됨 -- 노이즈 방지 게이트가 실제로 작동하는지 확인 (0으로 나누기 회귀 방지도 겸함)."""
    under_gate = LAMBDA_ERROR_RATE_MIN_INVOCATIONS - 1
    metrics = _lambda_metrics(
        invocation_last3=[under_gate, under_gate, under_gate],
        error_last3=[under_gate, under_gate, under_gate],  # 100%, 그러나 호출수가 게이트 미달
    )
    triggered, is_surge = _lambda_error_rate_check("Lambda", metrics)

    assert is_surge is False, "최소 호출수 게이트 미달이면 error_rate가 100%여도 트리거되면 안 됨"
    assert triggered == []
    print(f"✅ 최소 호출수 게이트(포인트당 {LAMBDA_ERROR_RATE_MIN_INVOCATIONS}건) 미달 시 트리거 안 됨을 확인")

test_lambda_error_rate_check_min_invocation_gate()


def test_lambda_error_rate_check_ignores_single_blip():
    """최근 3포인트 중 1개만 폭증하고 나머지 2개는 정상이면(지속성 미충족) 트리거 안 됨
    -- z-score의 _zscore_check_persistent와 동일한 '순간 노이즈 무시' 철학 회귀 방지."""
    metrics = _lambda_metrics(
        invocation_last3=[20.0, 20.0, 20.0],
        error_last3=[1.0, 1.0, 15.0],  # 5%, 5%, 75% -- 마지막 1개만 폭증
    )
    _, is_surge = _lambda_error_rate_check("Lambda", metrics)

    assert is_surge is False, "최근 3포인트 중 1개만 튄 순간적 노이즈는 트리거되면 안 됨"
    print("✅ 순간적 노이즈(최근 3포인트 중 1개만 폭증)는 트리거 안 됨을 확인")

test_lambda_error_rate_check_ignores_single_blip()


def test_lambda_error_rate_check_scoped_to_lambda_only():
    """Lambda 전용 -- 다른 리소스 타입(우연히 같은 필드명을 가진 경우는 없지만
    resource_type 게이트 자체를 명시적으로 확인)은 항상 제외."""
    metrics = _lambda_metrics(
        invocation_last3=[20.0, 20.0, 20.0],
        error_last3=[15.0, 15.0, 15.0],
    )
    triggered, is_surge = _lambda_error_rate_check("EC2", metrics)
    assert (triggered, is_surge) == ([], False)
    print("✅ Lambda 외 리소스 타입은 항상 제외됨을 확인")

test_lambda_error_rate_check_scoped_to_lambda_only()


def test_detection_node_flags_lambda_error_surge_and_activates_clf002():
    """detection_node 통합 테스트 + CLF-002가 실제로 살아나는지까지 확인.
    CLF-002(schema/rules/classification_rules.json)는 오늘 이전까지 triggered_metrics에
    error_count가 절대 안 들어가서 죽어있던 규칙이었다 -- 이번 체크가 그걸 살리는 게
    핵심 목표이므로, rule_engine.py까지 이어서 실제 매칭을 확인한다."""
    if os.path.exists(IFOREST_MODEL_DIR):
        shutil.rmtree(IFOREST_MODEL_DIR)

    fake_state = {
        "resource_id": "func-retry-storm",
        "resource_type": "Lambda",
        "raw_metrics": _lambda_metrics(
            invocation_last3=[20.0, 20.0, 20.0],
            error_last3=[15.0, 16.0, 18.0],  # 75%, 80%, 90%
        ),
        "timestamp": "2026-09-05T00:00:00Z",
        "anomaly_flag": False,
        "anomaly_score_zscore": None,
        "anomaly_score_iforest": None,
        "triggered_metrics": [],
    }

    result = detection_node(fake_state)
    print("triggered_metrics:", result["triggered_metrics"])
    print("anomaly_flag:", result["anomaly_flag"])

    assert result["anomaly_flag"] is True
    assert {"error_count", "invocation_count"} <= set(result["triggered_metrics"])

    from pipeline.rule_engine import RuleEngine
    engine = RuleEngine()
    matched = engine.match_classification_rules(result)
    assert matched is not None, "CLF-002가 매칭되어야 하는데 아무 규칙도 안 잡힘"
    assert matched["rule_id"] == "CLF-002", f"CLF-002가 아니라 {matched['rule_id']}가 매칭됨"
    assert matched["result"]["anomaly_type"] == "cost_spike"
    print(f"✅ CLF-002 활성화 확인 -- 매칭된 규칙: {matched['rule_id']} ({matched['description']})")

    if os.path.exists(IFOREST_MODEL_DIR):
        shutil.rmtree(IFOREST_MODEL_DIR)

test_detection_node_flags_lambda_error_surge_and_activates_clf002()