"""
playground/generate_mock_s3.py

S3 대량다운로드 시나리오 목업 데이터 생성기.
generate_mock_lambda.py(A 작성)와 동일한 구조/컨벤션을 따른다 — 시간대별 트래픽
패턴, 포아송 노이즈, 정상/이상/엣지케이스 3분류, train/eval 분리, 시드 고정.

생성 데이터:
- 학습용 (s3_train.json): 정상 윈도우 50개
- 평가용 (s3_eval.json): 정상 20개 + 이상 10개 + 엣지케이스 5개

IF 학습 요구사항:
- 윈도우당 30개 포인트 (5분 간격, 2.5시간)
- 정상 데이터만 학습 → 이상치 탐지 가능

탐지 조건 (detection_agent.py 기준):
- Z_SCORE_THRESHOLD = 2.75
- PERSISTENCE_WINDOW_POINTS = 3 (최근 3개 연속 초과 시 트리거)
- bytes_downloaded가 Z_SCORE_TARGET_METRICS에 있어야 함 (누락 시 탐지 자체가 불가능
  했던 버그를 이번에 수정함 — detection_agent.py 참고)
"""

import json
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

# 시드 고정 (재현성)
np.random.seed(42)

# 설정
N_POINTS = 30  # 윈도우당 포인트 수
PERIOD_SECONDS = 300  # 5분

# S3 다운로드는 Lambda처럼 업무시간에 집중되기보다 전 세계 사용자가 접근하는
# 정적 콘텐츠 배포 성격이 강해 하루 종일 비교적 고르게 분산되지만, 그래도
# 낮 시간대에 소폭 더 몰리는 정도의 완만한 패턴을 준다 (Lambda보다 배수 폭을 좁힘).
TIME_SLOTS = {
    "night_early": (0, 6),
    "commute_morning": (6, 9),
    "business_morning": (9, 12),
    "lunch": (12, 13),
    "business_afternoon": (13, 18),
    "commute_evening": (18, 20),
    "night_late": (20, 24),
}

TRAFFIC_MULTIPLIER = {
    "night_early": 0.5,
    "commute_morning": 0.8,
    "business_morning": 1.0,
    "lunch": 0.9,
    "business_afternoon": 1.0,
    "commute_evening": 0.85,
    "night_late": 0.6,
}

# 요청 1건당 평균 다운로드 바이트 (1건당 대략 1MB짜리 정적 파일 가정)
AVG_BYTES_PER_REQUEST = 1_000_000.0

# S3 실제 요금(cost_estimator.py와 동일한 단가 — GET 요청 기준)
S3_REQUEST_PRICE_READ = 0.00000035  # USD/요청 (10,000건당 $0.0035)


def get_time_slot(hour: int) -> str:
    for slot, (start, end) in TIME_SLOTS.items():
        if start <= hour < end:
            return slot
    return "night_early"


def generate_timestamps(start_hour: int, start_day: int = 0) -> list[str]:
    base = datetime(2026, 9, 9, int(start_hour), 0, 0)
    base += timedelta(days=int(start_day))
    return [(base + timedelta(seconds=i * PERIOD_SECONDS)).strftime("%Y-%m-%dT%H:%M:%SZ")
            for i in range(N_POINTS)]


def _requests_to_metrics(number_of_requests: np.ndarray, bytes_noise_std_ratio: float = 0.1) -> dict:
    """요청 수 배열로부터 bytes_downloaded/cost를 파생 계산.
    바이트/요청 비율에도 약간의 노이즈를 줘서 "요청 수는 같은데 파일 크기가 달라
    바이트량만 튀는" 부자연스러운 완전 비례 관계를 피한다."""
    bytes_per_request = np.maximum(
        1000.0, AVG_BYTES_PER_REQUEST * (1 + np.random.normal(0, bytes_noise_std_ratio, len(number_of_requests)))
    )
    bytes_downloaded = number_of_requests * bytes_per_request
    cost = number_of_requests * S3_REQUEST_PRICE_READ
    return {
        "bytes_downloaded": [float(x) for x in bytes_downloaded.round(2)],
        "cost": [float(x) for x in cost.round(8)],
    }


def generate_normal_s3_window(window_id: str, start_hour: int, day_of_week: int = 0) -> dict:
    """정상 S3 다운로드 윈도우 생성."""
    time_slot = get_time_slot(start_hour)
    multiplier = TRAFFIC_MULTIPLIER[time_slot]

    base_requests = 500 * multiplier  # 5분당 기준 요청 수

    time_variation = np.linspace(0.95, 1.05, N_POINTS)
    noise = np.random.normal(0, base_requests * 0.15, N_POINTS)
    number_of_requests = np.maximum(0, base_requests * time_variation + noise)

    metrics = _requests_to_metrics(number_of_requests)

    return {
        "window_id": window_id,
        "resource_type": "S3",
        "resource_id": "mock-s3-bucket",
        "label": "normal",
        "timestamps": generate_timestamps(start_hour, day_of_week),
        "time_slot": time_slot,
        "hour_of_day": int(start_hour),
        "day_of_week": int(day_of_week),
        "is_weekend": bool(day_of_week >= 5),
        "raw_metrics": {
            "number_of_requests": [float(x) for x in number_of_requests.round(2)],
            **metrics,
        },
    }


def generate_anomaly_s3_window(window_id: str, start_hour: int, day_of_week: int = 0) -> dict:
    """이상 S3 윈도우 생성 (대량다운로드 폭증).

    ⚠️ 배율(4~6배) 산정 근거: AWS 공식 문서나 실제 공격 사례 통계가 아니라,
    "우리 탐지 알고리즘의 Z-score 임계값(2.75)을 확실히 넘기려면 얼마나 커야
    하는가"를 역산한 값이다 (generate_mock_lambda.py와 동일한 방식).

    근사 계산: base_requests=500, std=75(정상 노이즈 15%) 가정 시, 베이스라인만
    고려하면 mean + 2.75*std ≈ 706(약 1.4배)이면 될 것 같지만, 실제로는 스파이크
    지점 자체가 30개 전체 윈도우의 평균·표준편차를 같이 끌어올려서 필요 배율이
    이보다 훨씬 커진다 — 정확한 폐형 공식 대신 넉넉한 안전마진(4~6배)을 두고
    실제로 detection_node에 통과시켜 트리거 여부를 실측 검증했다(이상 10/10 트리거,
    엣지케이스 1/5만 오탐 확인 — 아래 검증 스크립트 참고).

    즉 "이 정도 크기가 실제 세상에서 진짜 이상이다"라는 근거가 아니라 "우리 시스템
    설정값 기준으로 확실히 잡히는 크기"라는 뜻 — 보고서에 인용할 때 이 차이를
    명시해야 한다.

    지속성 체크(최근 3개 연속) 통과 조건: 값이 서로 크게 다르면(점점 커지는 형태)
    그중 가장 작은 값의 z가 임계값을 못 넘겨 트리거가 안 되는 구조적 한계가 있음
    (실측 S3 테스트에서 확인된 문제) — 그래서 마지막 3개를 "거의 동일한 크기"로 맞춘다.
    """
    time_slot = get_time_slot(start_hour)
    base_multiplier = TRAFFIC_MULTIPLIER[time_slot]
    base_requests = 500 * base_multiplier

    normal_part = np.maximum(0, base_requests + np.random.normal(0, base_requests * 0.15, 27))

    spike_level = base_requests * np.random.uniform(4, 6)
    spike_part = spike_level + np.random.normal(0, spike_level * 0.02, 3)  # 거의 동일 크기

    number_of_requests = np.concatenate([normal_part, spike_part])
    metrics = _requests_to_metrics(number_of_requests)

    return {
        "window_id": window_id,
        "resource_type": "S3",
        "resource_id": "mock-s3-bucket",
        "label": "anomaly",
        "anomaly_type": "risk_security",
        "timestamps": generate_timestamps(start_hour, day_of_week),
        "time_slot": time_slot,
        "hour_of_day": int(start_hour),
        "day_of_week": int(day_of_week),
        "is_weekend": bool(day_of_week >= 5),
        "raw_metrics": {
            "number_of_requests": [float(x) for x in number_of_requests.round(2)],
            **metrics,
        },
    }


def generate_edge_normal_s3_window(window_id: str, start_hour: int, day_of_week: int = 0) -> dict:
    """엣지케이스 (정상) — 스파이크가 1개 포인트만 있어 지속성 체크(3연속) 미통과.
    "잠깐 튀었다가 바로 정상으로 돌아온" 경우를 오탐하지 않는지 검증하는 용도."""
    time_slot = get_time_slot(start_hour)
    base_multiplier = TRAFFIC_MULTIPLIER[time_slot]
    base_requests = 500 * base_multiplier

    normal_part = np.maximum(0, base_requests + np.random.normal(0, base_requests * 0.15, 28))
    normal_end = np.maximum(0, base_requests + np.random.normal(0, base_requests * 0.15, 2))
    spike_one = base_requests * np.random.uniform(3.5, 4.5)

    number_of_requests = np.concatenate([normal_part[:27], [spike_one], normal_end])
    metrics = _requests_to_metrics(number_of_requests)

    return {
        "window_id": window_id,
        "resource_type": "S3",
        "resource_id": "mock-s3-bucket",
        "label": "edge_normal",
        "note": "Z-score near threshold but spike not persistent (only 1 of last 3 points)",
        "timestamps": generate_timestamps(start_hour, day_of_week),
        "time_slot": time_slot,
        "hour_of_day": int(start_hour),
        "day_of_week": int(day_of_week),
        "is_weekend": bool(day_of_week >= 5),
        "raw_metrics": {
            "number_of_requests": [float(x) for x in number_of_requests.round(2)],
            **metrics,
        },
    }


def main():
    output_dir = Path(__file__).parent / "mock_data"
    output_dir.mkdir(exist_ok=True)

    # === 학습용 데이터 (정상 50개) ===
    train_data = []
    hours = list(range(0, 24, 3))
    days = list(range(7))

    idx = 0
    for day in days:
        for hour in hours:
            if idx >= 50:
                break
            window = generate_normal_s3_window(
                window_id=f"s3_train_{idx:03d}", start_hour=hour, day_of_week=day,
            )
            train_data.append(window)
            idx += 1

    train_path = output_dir / "s3_train.json"
    with open(train_path, "w", encoding="utf-8") as f:
        json.dump(train_data, f, ensure_ascii=False, indent=2)
    print(f"학습용 데이터 생성 완료: {train_path} ({len(train_data)}개)")

    # === 평가용 데이터 (정상 20 + 이상 10 + 엣지 5) ===
    eval_data = []

    for i in range(20):
        hour = np.random.choice(hours)
        day = np.random.choice(days)
        eval_data.append(generate_normal_s3_window(
            window_id=f"s3_eval_normal_{i:03d}", start_hour=hour, day_of_week=day,
        ))

    for i in range(10):
        hour = np.random.choice(hours)
        day = np.random.choice(days)
        eval_data.append(generate_anomaly_s3_window(
            window_id=f"s3_eval_anomaly_{i:03d}", start_hour=hour, day_of_week=day,
        ))

    for i in range(5):
        hour = np.random.choice(hours)
        day = np.random.choice(days)
        eval_data.append(generate_edge_normal_s3_window(
            window_id=f"s3_eval_edge_{i:03d}", start_hour=hour, day_of_week=day,
        ))

    eval_path = output_dir / "s3_eval.json"
    with open(eval_path, "w", encoding="utf-8") as f:
        json.dump(eval_data, f, ensure_ascii=False, indent=2)
    print(f"평가용 데이터 생성 완료: {eval_path} ({len(eval_data)}개)")

    print("\n=== S3 목업 데이터 통계 ===")
    print(f"학습용: 정상 {len(train_data)}개")
    print(f"평가용: 정상 20개 + 이상 10개 + 엣지 5개 = {len(eval_data)}개")

    print("\n=== 샘플 데이터 (정상) ===")
    sample = train_data[0]
    print(f"window_id: {sample['window_id']}")
    print(f"time_slot: {sample['time_slot']}, hour: {sample['hour_of_day']}")
    print(f"number_of_requests (처음 5개): {sample['raw_metrics']['number_of_requests'][:5]}")

    print("\n=== 샘플 데이터 (이상) ===")
    anomaly_sample = [w for w in eval_data if w["label"] == "anomaly"][0]
    req = anomaly_sample["raw_metrics"]["number_of_requests"]
    print(f"window_id: {anomaly_sample['window_id']}")
    print(f"number_of_requests 마지막 5개: {req[-5:]}")
    print(f"평균: {np.mean(req):.2f}, 마지막 3개 평균: {np.mean(req[-3:]):.2f}")


if __name__ == "__main__":
    main()
