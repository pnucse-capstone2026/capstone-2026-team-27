"""
playground/generate_mock_autoscaling.py

EDoS 의심 (AutoScaling) 시나리오 목업 데이터 생성기

생성 데이터:
- 학습용 (autoscaling_train.json): 정상 윈도우 50개
- 평가용 (autoscaling_eval.json): 정상 20개 + 이상 10개 + 엣지케이스 5개

IF 학습 요구사항:
- 윈도우당 30개 포인트 (5분 간격, 2.5시간)
- 정상 데이터만 학습 → 이상치 탐지 가능

EDoS 탐지 조건 (classification_rules.json CLF-001):
- group_desired_capacity > mean × 2.0
- anomaly_type: risk_security
"""

import json
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

# 시드 고정 (재현성)
np.random.seed(43)  # Lambda와 다른 시드

# 설정
N_POINTS = 30  # 윈도우당 포인트 수
PERIOD_SECONDS = 300  # 5분

# 시간대 정의
TIME_SLOTS = {
    "night_early": (0, 6),
    "commute_morning": (6, 9),
    "business_morning": (9, 12),
    "lunch": (12, 13),
    "business_afternoon": (13, 18),
    "commute_evening": (18, 20),
    "night_late": (20, 24),
}

# 시간대별 인스턴스 수 배수 (야간에는 적게 유지)
CAPACITY_MULTIPLIER = {
    "night_early": 0.5,
    "commute_morning": 0.8,
    "business_morning": 1.0,
    "lunch": 0.9,
    "business_afternoon": 1.0,
    "commute_evening": 0.7,
    "night_late": 0.6,
}


def get_time_slot(hour: int) -> str:
    """시간(0~23)에 해당하는 시간대 반환"""
    for slot, (start, end) in TIME_SLOTS.items():
        if start <= hour < end:
            return slot
    return "night_early"


def generate_timestamps(start_hour: int, start_day: int = 0) -> list[str]:
    """30개 포인트의 타임스탬프 생성"""
    base = datetime(2026, 9, 9, int(start_hour), 0, 0)
    base += timedelta(days=int(start_day))
    return [(base + timedelta(seconds=i * PERIOD_SECONDS)).strftime("%Y-%m-%dT%H:%M:%SZ")
            for i in range(N_POINTS)]


def generate_normal_autoscaling_window(
    window_id: str,
    start_hour: int,
    day_of_week: int = 0,
) -> dict:
    """정상 AutoScaling 윈도우 생성

    AutoScaling은 인스턴스 수가 천천히 변하므로,
    정상 상태에서는 desired_capacity가 거의 일정하게 유지됨.
    """

    time_slot = get_time_slot(start_hour)
    multiplier = CAPACITY_MULTIPLIER[time_slot]

    # 기본 인스턴스 수 (업무시간 기준 평균 4대)
    base_capacity = 4
    target_capacity = base_capacity * multiplier

    # 정상 상태: 거의 일정 + 약간의 변동 (±1 정도)
    # AutoScaling은 급격한 변화 없이 점진적으로 조정됨
    capacity_variation = np.random.choice([-1, 0, 0, 0, 1], N_POINTS)
    capacity_base = np.full(N_POINTS, round(target_capacity))

    # 점진적 변화 시뮬레이션 (급격한 변화 없음)
    group_desired_capacity = np.clip(capacity_base + capacity_variation, 1, 8).astype(float)

    # in_service는 desired와 거의 동일 (±1 지연)
    service_delay = np.random.choice([0, 0, 1], N_POINTS)
    group_in_service_instances = np.clip(group_desired_capacity - service_delay, 1, 8)

    # 비용 계산 (EC2 인스턴스 시간당 비용 근사, t3.medium 기준)
    # $0.0416/hour = $0.000694/minute = $0.00347/5분
    cost_per_instance_per_period = 0.00347
    cost = group_in_service_instances * cost_per_instance_per_period

    return {
        "window_id": window_id,
        "resource_type": "AutoScaling",
        "resource_id": "mock-asg-group",
        "label": "normal",
        "timestamps": generate_timestamps(start_hour, day_of_week),
        "time_slot": time_slot,
        "hour_of_day": int(start_hour),
        "day_of_week": int(day_of_week),
        "is_weekend": bool(day_of_week >= 5),
        "raw_metrics": {
            "group_desired_capacity": [float(x) for x in group_desired_capacity],
            "group_in_service_instances": [float(x) for x in group_in_service_instances],
            "cost": [float(x) for x in cost.round(6)],
        }
    }


def generate_anomaly_autoscaling_window(
    window_id: str,
    start_hour: int,
    day_of_week: int = 0,
) -> dict:
    """이상 AutoScaling 윈도우 생성 (EDoS 의심)

    EDoS 탐지 조건 (CLF-001):
    - group_desired_capacity > mean × 2.0

    또한 지속성 체크 (PERSISTENCE_WINDOW_POINTS = 3):
    - 최근 3개 포인트가 연속으로 임계값 초과

    평균 4대 기준 → 8대 이상이면 탐지
    안전하게 10~15대로 설정
    """

    time_slot = get_time_slot(start_hour)
    base_multiplier = CAPACITY_MULTIPLIER[time_slot]

    base_capacity = 4 * base_multiplier

    # 앞 27개는 정상 (평균 ~4대)
    normal_capacity = np.clip(
        base_capacity + np.random.choice([-1, 0, 0, 0, 1], 27),
        1, 6
    ).astype(float)

    # 마지막 3개는 EDoS 스파이크 (평균의 2.5~4배)
    # 평균 4 × 2.5 = 10, 평균 4 × 4 = 16
    spike_capacity = np.random.uniform(10, 16, 3)

    group_desired_capacity = np.concatenate([normal_capacity, spike_capacity])

    # in_service도 급증 (공격으로 인해 실제로 인스턴스가 늘어남)
    group_in_service_instances = group_desired_capacity - np.random.choice([0, 1], N_POINTS)
    group_in_service_instances = np.maximum(1, group_in_service_instances)

    # 비용 급증
    cost_per_instance_per_period = 0.00347
    cost = group_in_service_instances * cost_per_instance_per_period

    return {
        "window_id": window_id,
        "resource_type": "AutoScaling",
        "resource_id": "mock-asg-group",
        "label": "anomaly",
        "anomaly_type": "risk_security",
        "attack_type": "EDoS",
        "timestamps": generate_timestamps(start_hour, day_of_week),
        "time_slot": time_slot,
        "hour_of_day": int(start_hour),
        "day_of_week": int(day_of_week),
        "is_weekend": bool(day_of_week >= 5),
        "raw_metrics": {
            "group_desired_capacity": [float(x) for x in group_desired_capacity.round(1)],
            "group_in_service_instances": [float(x) for x in group_in_service_instances.round(1)],
            "cost": [float(x) for x in cost.round(6)],
        }
    }


def generate_edge_normal_autoscaling_window(
    window_id: str,
    start_hour: int,
    day_of_week: int = 0,
) -> dict:
    """엣지케이스 (정상) AutoScaling 윈도우 생성

    임계값 근처지만 정상인 데이터:
    - capacity가 평균의 1.8배 (2배 미만)
    - 또는 스파이크가 1~2개 포인트만 (지속성 체크 미통과)
    """

    time_slot = get_time_slot(start_hour)
    base_multiplier = CAPACITY_MULTIPLIER[time_slot]

    base_capacity = 4 * base_multiplier

    # 정상 부분
    normal_capacity = np.clip(
        base_capacity + np.random.choice([-1, 0, 0, 0, 1], 27),
        1, 6
    ).astype(float)

    # 엣지 케이스: 평균의 1.7~1.9배 (2배 미만)
    # 평균 4 × 1.8 = 7.2
    edge_capacity = base_capacity * np.random.uniform(1.7, 1.9, 3)

    group_desired_capacity = np.concatenate([normal_capacity, edge_capacity])

    group_in_service_instances = group_desired_capacity - np.random.choice([0, 1], N_POINTS)
    group_in_service_instances = np.maximum(1, group_in_service_instances)

    cost_per_instance_per_period = 0.00347
    cost = group_in_service_instances * cost_per_instance_per_period

    return {
        "window_id": window_id,
        "resource_type": "AutoScaling",
        "resource_id": "mock-asg-group",
        "label": "edge_normal",
        "note": "capacity increased but below 2x threshold (1.7-1.9x)",
        "timestamps": generate_timestamps(start_hour, day_of_week),
        "time_slot": time_slot,
        "hour_of_day": int(start_hour),
        "day_of_week": int(day_of_week),
        "is_weekend": bool(day_of_week >= 5),
        "raw_metrics": {
            "group_desired_capacity": [float(x) for x in group_desired_capacity.round(1)],
            "group_in_service_instances": [float(x) for x in group_in_service_instances.round(1)],
            "cost": [float(x) for x in cost.round(6)],
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
            window = generate_normal_autoscaling_window(
                window_id=f"autoscaling_train_{idx:03d}",
                start_hour=hour,
                day_of_week=day,
            )
            train_data.append(window)
            idx += 1

    train_path = output_dir / "autoscaling_train.json"
    with open(train_path, "w", encoding="utf-8") as f:
        json.dump(train_data, f, ensure_ascii=False, indent=2)
    print(f"학습용 데이터 생성 완료: {train_path} ({len(train_data)}개)")

    # === 평가용 데이터 (정상 20 + 이상 10 + 엣지 5) ===
    eval_data = []

    # 정상 20개
    for i in range(20):
        hour = np.random.choice(hours)
        day = np.random.choice(days)
        window = generate_normal_autoscaling_window(
            window_id=f"autoscaling_eval_normal_{i:03d}",
            start_hour=hour,
            day_of_week=day,
        )
        eval_data.append(window)

    # 이상 10개 (EDoS)
    for i in range(10):
        hour = np.random.choice(hours)
        day = np.random.choice(days)
        window = generate_anomaly_autoscaling_window(
            window_id=f"autoscaling_eval_anomaly_{i:03d}",
            start_hour=hour,
            day_of_week=day,
        )
        eval_data.append(window)

    # 엣지케이스 5개
    for i in range(5):
        hour = np.random.choice(hours)
        day = np.random.choice(days)
        window = generate_edge_normal_autoscaling_window(
            window_id=f"autoscaling_eval_edge_{i:03d}",
            start_hour=hour,
            day_of_week=day,
        )
        eval_data.append(window)

    eval_path = output_dir / "autoscaling_eval.json"
    with open(eval_path, "w", encoding="utf-8") as f:
        json.dump(eval_data, f, ensure_ascii=False, indent=2)
    print(f"평가용 데이터 생성 완료: {eval_path} ({len(eval_data)}개)")

    # 통계 출력
    print("\n=== AutoScaling (EDoS) 목업 데이터 통계 ===")
    print(f"학습용: 정상 {len(train_data)}개")
    print(f"평가용: 정상 20개 + 이상(EDoS) 10개 + 엣지 5개 = {len(eval_data)}개")

    # 샘플 출력
    print("\n=== 샘플 데이터 (정상) ===")
    sample = train_data[0]
    print(f"window_id: {sample['window_id']}")
    print(f"time_slot: {sample['time_slot']}, hour: {sample['hour_of_day']}")
    capacity = sample['raw_metrics']['group_desired_capacity']
    print(f"group_desired_capacity: mean={np.mean(capacity):.2f}, range=[{min(capacity)}, {max(capacity)}]")

    print("\n=== 샘플 데이터 (EDoS 이상) ===")
    anomaly_sample = [w for w in eval_data if w['label'] == 'anomaly'][0]
    capacity = anomaly_sample['raw_metrics']['group_desired_capacity']
    print(f"window_id: {anomaly_sample['window_id']}")
    print(f"group_desired_capacity 마지막 5개: {capacity[-5:]}")
    print(f"전체 평균: {np.mean(capacity):.2f}, 마지막 3개 평균: {np.mean(capacity[-3:]):.2f}")
    print(f"스파이크 배수: {np.mean(capacity[-3:]) / np.mean(capacity[:27]):.2f}x")


if __name__ == "__main__":
    main()
