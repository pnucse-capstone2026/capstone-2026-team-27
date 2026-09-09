"""
playground/generate_mock_lambda.py

Lambda 폭증 시나리오 목업 데이터 생성기

생성 데이터:
- 학습용 (lambda_train.json): 정상 윈도우 50개
- 평가용 (lambda_eval.json): 정상 20개 + 이상 10개 + 엣지케이스 5개

IF 학습 요구사항:
- 윈도우당 30개 포인트 (5분 간격, 2.5시간)
- 정상 데이터만 학습 → 이상치 탐지 가능

탐지 조건 (detection_agent.py 기준):
- Z_SCORE_THRESHOLD = 2.75
- PERSISTENCE_WINDOW_POINTS = 3 (최근 3개 연속 초과 시 트리거)
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

# 시간대 정의
TIME_SLOTS = {
    "night_early": (0, 6),      # 새벽
    "commute_morning": (6, 9),  # 출근
    "business_morning": (9, 12), # 오전 업무
    "lunch": (12, 13),          # 점심
    "business_afternoon": (13, 18), # 오후 업무
    "commute_evening": (18, 20), # 퇴근
    "night_late": (20, 24),     # 저녁/야간
}

# 시간대별 트래픽 배수
TRAFFIC_MULTIPLIER = {
    "night_early": 0.15,
    "commute_morning": 0.6,
    "business_morning": 1.0,
    "lunch": 0.7,
    "business_afternoon": 1.0,
    "commute_evening": 0.5,
    "night_late": 0.3,
}


def get_time_slot(hour: int) -> str:
    """시간(0~23)에 해당하는 시간대 반환"""
    for slot, (start, end) in TIME_SLOTS.items():
        if start <= hour < end:
            return slot
    return "night_early"


def generate_timestamps(start_hour: int, start_day: int = 0) -> list[str]:
    """30개 포인트의 타임스탬프 생성"""
    base = datetime(2026, 9, 9, int(start_hour), 0, 0)  # 월요일 기준
    base += timedelta(days=int(start_day))
    return [(base + timedelta(seconds=i * PERIOD_SECONDS)).strftime("%Y-%m-%dT%H:%M:%SZ")
            for i in range(N_POINTS)]


def generate_normal_lambda_window(
    window_id: str,
    start_hour: int,
    day_of_week: int = 0,
) -> dict:
    """정상 Lambda 윈도우 생성"""

    time_slot = get_time_slot(start_hour)
    multiplier = TRAFFIC_MULTIPLIER[time_slot]

    # 기본 값 (업무시간 기준)
    base_invocation = 150
    base_error_rate = 0.02  # 2% 에러율
    base_duration = 120  # ms

    # 시간대별 조정
    invocation_mean = base_invocation * multiplier

    # 30개 포인트 생성 (시간에 따른 미세 변동 포함)
    # 윈도우 내에서도 시간이 흐르면서 약간의 변동
    time_variation = np.linspace(0.95, 1.05, N_POINTS)  # 5% 내외 변동
    noise = np.random.normal(0, invocation_mean * 0.15, N_POINTS)  # 15% 노이즈

    invocation_count = np.maximum(0, invocation_mean * time_variation + noise)

    # 에러 수 (호출 수에 비례 + 랜덤)
    error_count = np.random.poisson(invocation_count * base_error_rate).astype(float)

    # 실행 시간 (정상 범위 내 변동)
    duration_avg = base_duration + np.random.normal(0, 15, N_POINTS)
    duration_avg = np.maximum(50, duration_avg)  # 최소 50ms

    # 비용 계산 (Lambda 비용 공식 근사)
    # $0.0000002/요청 + $0.0000166667/GB-초 (128MB = 0.125GB)
    cost_per_request = 0.0000002
    cost_per_gb_sec = 0.0000166667
    memory_gb = 0.125

    cost = (invocation_count * cost_per_request +
            invocation_count * (duration_avg / 1000) * memory_gb * cost_per_gb_sec)

    return {
        "window_id": window_id,
        "resource_type": "Lambda",
        "resource_id": "mock-lambda-function",
        "label": "normal",
        "timestamps": generate_timestamps(start_hour, day_of_week),
        "time_slot": time_slot,
        "hour_of_day": int(start_hour),
        "day_of_week": int(day_of_week),
        "is_weekend": bool(day_of_week >= 5),
        "raw_metrics": {
            "invocation_count": [float(x) for x in invocation_count.round(2)],
            "error_count": [float(x) for x in error_count],
            "duration_avg": [float(x) for x in duration_avg.round(2)],
            "cost": [float(x) for x in cost.round(8)],
        }
    }


def generate_anomaly_lambda_window(
    window_id: str,
    start_hour: int,
    day_of_week: int = 0,
) -> dict:
    """이상 Lambda 윈도우 생성 (폭증)

    Z-score > 2.75 조건을 만족하도록:
    - 최근 3개 포인트가 모두 평균 대비 크게 높음
    - 평균 150, 표준편차 30 기준 → 2.75σ = 82.5 → 232.5 이상
    - 안전하게 3배 이상(450+)으로 설정
    """

    time_slot = get_time_slot(start_hour)
    base_multiplier = TRAFFIC_MULTIPLIER[time_slot]

    base_invocation = 150 * base_multiplier

    # 앞 27개는 정상, 마지막 3개는 폭증 (지속성 체크 통과)
    normal_part = np.maximum(0, base_invocation + np.random.normal(0, base_invocation * 0.15, 27))

    # 폭증 부분: 평균의 4~6배 (Z-score 확실히 초과)
    spike_multiplier = np.random.uniform(4, 6, 3)
    spike_part = base_invocation * spike_multiplier

    invocation_count = np.concatenate([normal_part, spike_part])

    # 에러도 폭증 시 증가 가능
    error_rate = np.concatenate([
        np.full(27, 0.02),
        np.full(3, 0.05)  # 폭증 시 에러율 증가
    ])
    error_count = np.random.poisson(invocation_count * error_rate)

    # 실행 시간 (폭증 시 약간 증가)
    duration_normal = 120 + np.random.normal(0, 15, 27)
    duration_spike = 150 + np.random.normal(0, 20, 3)  # 폭증 시 지연
    duration_avg = np.concatenate([duration_normal, duration_spike])
    duration_avg = np.maximum(50, duration_avg)

    # 비용
    cost_per_request = 0.0000002
    cost_per_gb_sec = 0.0000166667
    memory_gb = 0.125
    cost = (invocation_count * cost_per_request +
            invocation_count * (duration_avg / 1000) * memory_gb * cost_per_gb_sec)

    return {
        "window_id": window_id,
        "resource_type": "Lambda",
        "resource_id": "mock-lambda-function",
        "label": "anomaly",
        "anomaly_type": "cost_spike",
        "timestamps": generate_timestamps(start_hour, day_of_week),
        "time_slot": time_slot,
        "hour_of_day": int(start_hour),
        "day_of_week": int(day_of_week),
        "is_weekend": bool(day_of_week >= 5),
        "raw_metrics": {
            "invocation_count": [float(x) for x in invocation_count.round(2)],
            "error_count": [float(x) for x in error_count],
            "duration_avg": [float(x) for x in duration_avg.round(2)],
            "cost": [float(x) for x in cost.round(8)],
        }
    }


def generate_edge_normal_lambda_window(
    window_id: str,
    start_hour: int,
    day_of_week: int = 0,
) -> dict:
    """엣지케이스 (정상) Lambda 윈도우 생성

    임계값 근처지만 정상인 데이터:
    - Z-score가 2.5 정도 (2.75 미만)
    - 또는 스파이크가 1~2개 포인트만 (지속성 체크 미통과)
    """

    time_slot = get_time_slot(start_hour)
    base_multiplier = TRAFFIC_MULTIPLIER[time_slot]

    base_invocation = 150 * base_multiplier

    # 케이스 1: Z-score가 임계값 바로 아래 (2.5σ 정도)
    # 2.5σ = 평균 + 2.5 * 표준편차
    normal_part = np.maximum(0, base_invocation + np.random.normal(0, base_invocation * 0.15, 27))

    # 마지막 3개: 높지만 임계값 미만 (2.5배 정도)
    edge_multiplier = np.random.uniform(2.3, 2.7, 3)
    edge_part = base_invocation * edge_multiplier

    invocation_count = np.concatenate([normal_part, edge_part])

    # 에러 (정상 수준)
    error_count = np.random.poisson(invocation_count * 0.02)

    # 실행 시간
    duration_avg = 120 + np.random.normal(0, 15, N_POINTS)
    duration_avg = np.maximum(50, duration_avg)

    # 비용
    cost_per_request = 0.0000002
    cost_per_gb_sec = 0.0000166667
    memory_gb = 0.125
    cost = (invocation_count * cost_per_request +
            invocation_count * (duration_avg / 1000) * memory_gb * cost_per_gb_sec)

    return {
        "window_id": window_id,
        "resource_type": "Lambda",
        "resource_id": "mock-lambda-function",
        "label": "edge_normal",
        "note": "Z-score near threshold but below 2.75, or spike not persistent",
        "timestamps": generate_timestamps(start_hour, day_of_week),
        "time_slot": time_slot,
        "hour_of_day": int(start_hour),
        "day_of_week": int(day_of_week),
        "is_weekend": bool(day_of_week >= 5),
        "raw_metrics": {
            "invocation_count": [float(x) for x in invocation_count.round(2)],
            "error_count": [float(x) for x in error_count],
            "duration_avg": [float(x) for x in duration_avg.round(2)],
            "cost": [float(x) for x in cost.round(8)],
        }
    }


def main():
    output_dir = Path(__file__).parent / "mock_data"
    output_dir.mkdir(exist_ok=True)

    # === 학습용 데이터 (정상 50개) ===
    train_data = []
    hours = list(range(0, 24, 3))  # 0, 3, 6, 9, 12, 15, 18, 21
    days = list(range(7))  # 월~일

    idx = 0
    for day in days:
        for hour in hours:
            if idx >= 50:
                break
            window = generate_normal_lambda_window(
                window_id=f"lambda_train_{idx:03d}",
                start_hour=hour,
                day_of_week=day,
            )
            train_data.append(window)
            idx += 1

    train_path = output_dir / "lambda_train.json"
    with open(train_path, "w", encoding="utf-8") as f:
        json.dump(train_data, f, ensure_ascii=False, indent=2)
    print(f"학습용 데이터 생성 완료: {train_path} ({len(train_data)}개)")

    # === 평가용 데이터 (정상 20 + 이상 10 + 엣지 5) ===
    eval_data = []

    # 정상 20개
    for i in range(20):
        hour = np.random.choice(hours)
        day = np.random.choice(days)
        window = generate_normal_lambda_window(
            window_id=f"lambda_eval_normal_{i:03d}",
            start_hour=hour,
            day_of_week=day,
        )
        eval_data.append(window)

    # 이상 10개
    for i in range(10):
        hour = np.random.choice(hours)
        day = np.random.choice(days)
        window = generate_anomaly_lambda_window(
            window_id=f"lambda_eval_anomaly_{i:03d}",
            start_hour=hour,
            day_of_week=day,
        )
        eval_data.append(window)

    # 엣지케이스 5개
    for i in range(5):
        hour = np.random.choice(hours)
        day = np.random.choice(days)
        window = generate_edge_normal_lambda_window(
            window_id=f"lambda_eval_edge_{i:03d}",
            start_hour=hour,
            day_of_week=day,
        )
        eval_data.append(window)

    eval_path = output_dir / "lambda_eval.json"
    with open(eval_path, "w", encoding="utf-8") as f:
        json.dump(eval_data, f, ensure_ascii=False, indent=2)
    print(f"평가용 데이터 생성 완료: {eval_path} ({len(eval_data)}개)")

    # 통계 출력
    print("\n=== Lambda 목업 데이터 통계 ===")
    print(f"학습용: 정상 {len(train_data)}개")
    print(f"평가용: 정상 20개 + 이상 10개 + 엣지 5개 = {len(eval_data)}개")

    # 샘플 출력
    print("\n=== 샘플 데이터 (정상) ===")
    sample = train_data[0]
    print(f"window_id: {sample['window_id']}")
    print(f"time_slot: {sample['time_slot']}, hour: {sample['hour_of_day']}")
    print(f"invocation_count (처음 5개): {sample['raw_metrics']['invocation_count'][:5]}")

    print("\n=== 샘플 데이터 (이상) ===")
    anomaly_sample = [w for w in eval_data if w['label'] == 'anomaly'][0]
    inv = anomaly_sample['raw_metrics']['invocation_count']
    print(f"window_id: {anomaly_sample['window_id']}")
    print(f"invocation_count 마지막 5개: {inv[-5:]}")
    print(f"평균: {np.mean(inv):.2f}, 마지막 3개 평균: {np.mean(inv[-3:]):.2f}")


if __name__ == "__main__":
    main()
