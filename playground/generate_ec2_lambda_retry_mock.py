"""
친구가 만든 mock 데이터(lambda_train.json / lambda_eval.json / autoscaling_*)와
동일한 스키마로, 담당 시나리오(EC2 좀비 인스턴스, Lambda 에러 재시도 폭증) 전용
mock 데이터를 생성한다.

생성 파일:
  - ec2_train.json          : label="normal" 50개
  - ec2_eval.json            : normal 20 + anomaly(좀비) 10 + edge_normal 5
  - lambda_eval_retry.json   : normal 20 + anomaly(재시도 폭증) 10 + edge_normal 5
    (친구의 lambda_eval.json anomaly_type="cost_spike"는 실측 결과 마지막 3포인트
    error_rate가 4.9~6.7%로 50% 문턱과 무관한 "호출량/비용 폭증" 시나리오였음 —
    처음엔 담당 체크용 정상 데이터를 기존 lambda_train.json/lambda_eval.json에서
    재사용했는데, 그중 일부가 실제로 모델 학습에 쓰인 윈도우라 오탐률 재검증 때
    "학습에 쓰인 데이터로 채점"하는 무효한 측정이 나온 적이 있어 — 다른 eval
    파일들과 동일하게 이 파일 안에 전용 normal 20개를 따로 생성하는 것으로 변경.)

각 윈도우는 생성 직후 실제 pipeline/detection_agent.py의 체크 함수로 재검증해서
의도한 라벨과 실제 판정이 일치하는지 assert한다 — "설계상 이래야 한다"가 아니라
"실제로 이렇게 판정된다"를 보장한다.

[실행 방법] 프로젝트 루트에서: python playground/generate_ec2_lambda_retry_mock.py
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import numpy as np

import pipeline.detection_agent as da

N = 30                    # 윈도우당 포인트 수
PERIOD_SECONDS = 300       # 5분
OUT_DIR = PROJECT_ROOT / "playground" / "mock_data"

# 친구 파일과 동일한 3시간 간격 슬롯 순환 (하루 8슬롯)
_SLOT_HOURS = [0, 3, 6, 9, 12, 15, 18, 21]
_SLOT_NAMES = {
    0: "night_early",
    3: "night_early",
    6: "commute_morning",
    9: "business_morning",
    12: "lunch",
    15: "business_afternoon",
    18: "commute_evening",
    21: "night_late",
}
_BASE_DATE = datetime(2026, 9, 9, tzinfo=timezone.utc)  # 친구 파일과 동일한 시작일


def _time_fields(window_index: int) -> dict:
    """window_index(0부터)를 8슬롯/하루 순환에 맞춰 timestamps/time_slot/
    hour_of_day/day_of_week/is_weekend으로 변환. 친구 파일의 날짜/요일 패턴과
    동일한 방식(day_of_week 0~4=평일, 5~6=주말)."""
    day_offset = window_index // len(_SLOT_HOURS)
    slot_idx = window_index % len(_SLOT_HOURS)
    hour = _SLOT_HOURS[slot_idx]
    start = _BASE_DATE + timedelta(days=day_offset, hours=hour)
    timestamps = [
        (start + timedelta(seconds=PERIOD_SECONDS * i)).strftime("%Y-%m-%dT%H:%M:%SZ")
        for i in range(N)
    ]
    day_of_week = day_offset % 7
    return {
        "timestamps": timestamps,
        "time_slot": _SLOT_NAMES[hour],
        "hour_of_day": hour,
        "day_of_week": day_of_week,
        "is_weekend": day_of_week in (5, 6),
    }


def _window(window_id: str, resource_type: str, resource_id: str, label: str,
            window_index: int, raw_metrics: dict, **extra) -> dict:
    w = {
        "window_id": window_id,
        "resource_type": resource_type,
        "resource_id": resource_id,
        "label": label,
        **_time_fields(window_index),
        "raw_metrics": raw_metrics,
    }
    w.update(extra)
    return w


# ══════════════════════════════════════════════════════════════════
# EC2
# ══════════════════════════════════════════════════════════════════

def _ec2_network_in_pattern(rng: np.random.Generator) -> np.ndarray:
    """실측 검증된 패턴(validate_real_aws_buffer.py 근거, seed_mock_iforest_buffer.py와
    동일 로직 재사용): 20~40분 주기로 9K/16K/23K bytes 다단계 반복."""
    tiers = [9000.0, 16000.0, 23000.0]
    values = np.zeros(N)
    i = 0
    while i < N:
        cycle_len = rng.integers(4, 9)
        tier = tiers[rng.integers(0, len(tiers))]
        span = min(cycle_len, N - i)
        values[i:i + span] = tier + rng.uniform(-tier * 0.05, tier * 0.05, size=span)
        i += span
    return values


def make_ec2_normal(rng: np.random.Generator) -> dict:
    cpu_level = rng.choice([30.0, 45.0, 60.0])
    cpu = cpu_level + rng.uniform(-cpu_level * 0.08, cpu_level * 0.08, size=N)
    network_in = _ec2_network_in_pattern(rng)
    network_out = np.clip(network_in * rng.uniform(0.55, 0.75) + rng.uniform(-200, 200, size=N), 0, None)
    cost = np.full(N, 0.05)
    return {
        "cpu_utilization": cpu.tolist(),
        "network_in": network_in.tolist(),
        "network_out": network_out.tolist(),
        "cost": cost.tolist(),
    }


def make_ec2_zombie_clear(rng: np.random.Generator) -> dict:
    """명백한 좀비: peak_cpu 1~3%, network I/O 합산 거의 0."""
    cpu = rng.uniform(0.5, 3.0, size=N)
    network_in = rng.uniform(10, 60, size=N)
    network_out = rng.uniform(5, 40, size=N)
    cost = np.full(N, 0.05)
    return {
        "cpu_utilization": cpu.tolist(),
        "network_in": network_in.tolist(),
        "network_out": network_out.tolist(),
        "cost": cost.tolist(),
    }


def make_ec2_zombie_boundary(rng: np.random.Generator) -> dict:
    """경계 좀비: peak_cpu가 4.7~5.0(임계값 이하) 사이, network I/O 합산이
    임계값의 90~99.5% 사이가 되도록 설계."""
    threshold = da.EC2_IDLE_NETWORK_IO_BYTES_THRESHOLD
    peak = rng.uniform(4.7, 5.0)
    cpu = rng.uniform(0.5, peak, size=N)
    cpu[rng.integers(0, N)] = peak  # peak가 실제로 윈도우에 등장하도록 보장
    target_sum = threshold * rng.uniform(0.90, 0.995)
    in_ratio = rng.uniform(0.55, 0.7)
    per_point_in = (target_sum * in_ratio) / N
    per_point_out = (target_sum * (1 - in_ratio)) / N
    network_in = per_point_in + rng.uniform(-per_point_in * 0.05, per_point_in * 0.05, size=N)
    network_out = per_point_out + rng.uniform(-per_point_out * 0.05, per_point_out * 0.05, size=N)
    cost = np.full(N, 0.05)
    return {
        "cpu_utilization": cpu.tolist(),
        "network_in": network_in.tolist(),
        "network_out": network_out.tolist(),
        "cost": cost.tolist(),
    }


def make_ec2_edge_cases(rng: np.random.Generator) -> list[dict]:
    threshold = da.EC2_IDLE_NETWORK_IO_BYTES_THRESHOLD
    age_hours_sec = da._EC2_IDLE_WINDOW_HOURS * 3600  # 9000초

    cases = []

    # 1) 신생 인스턴스: 나이가 가드 임계값보다 훨씬 어림. 지표만 보면 좀비.
    cpu = rng.uniform(0.5, 2.5, size=N)
    network_in = rng.uniform(10, 60, size=N)
    network_out = rng.uniform(5, 40, size=N)
    cases.append(({
        "cpu_utilization": cpu.tolist(), "network_in": network_in.tolist(),
        "network_out": network_out.tolist(), "cost": [0.05] * N,
    }, {"resource_age_seconds": 1800.0,
        "note": "생성된 지 30분밖에 안 된 신생 인스턴스 - 지표만 보면 좀비지만 나이가드로 판단 보류돼야 함"}))

    # 2) 나이는 충분(가드 무관), 윈도우 중 딱 1포인트만 CPU가 임계값을 넘음(정상 크론잡 등)
    cpu = rng.uniform(0.5, 3.0, size=N)
    cpu[15] = 15.0
    network_in = rng.uniform(10, 60, size=N)
    network_out = rng.uniform(5, 40, size=N)
    cases.append(({
        "cpu_utilization": cpu.tolist(), "network_in": network_in.tolist(),
        "network_out": network_out.tolist(), "cost": [0.05] * N,
    }, {"resource_age_seconds": 20000.0,
        "note": "나머지는 유휴지만 중간에 정상적인 짧은 크론잡 스파이크(CPU 15%) 1회 - peak 조건 불충족으로 정상 처리돼야 함"}))

    # 3) CPU는 계속 낮지만 network I/O 합산이 임계값을 살짝 초과 (가벼운 모니터링 하트비트 등)
    cpu = rng.uniform(0.5, 2.5, size=N)
    target_sum = threshold * 1.05
    per_point = target_sum / N / 2
    network_in = per_point + rng.uniform(-per_point * 0.05, per_point * 0.05, size=N)
    network_out = per_point + rng.uniform(-per_point * 0.05, per_point * 0.05, size=N)
    cases.append(({
        "cpu_utilization": cpu.tolist(), "network_in": network_in.tolist(),
        "network_out": network_out.tolist(), "cost": [0.05] * N,
    }, {"resource_age_seconds": 50000.0,
        "note": "CPU는 유휴 수준이지만 network I/O 합산이 임계값을 살짝 초과(모니터링 하트비트 등) - AND 조건이라 정상 처리돼야 함"}))

    # 4) peak CPU가 임계값을 아주 살짝 초과(5.01%), network는 거의 0
    cpu = rng.uniform(0.5, 3.0, size=N)
    cpu[10] = 5.01
    network_in = rng.uniform(10, 60, size=N)
    network_out = rng.uniform(5, 40, size=N)
    cases.append(({
        "cpu_utilization": cpu.tolist(), "network_in": network_in.tolist(),
        "network_out": network_out.tolist(), "cost": [0.05] * N,
    }, {"resource_age_seconds": 40000.0,
        "note": "peak CPU가 임계값(5.0%)을 0.01%p 초과 - 경계 바로 바깥이라 정상 처리돼야 함"}))

    # 5) 나이가 가드 임계값 바로 아래(8999초, 1초 차이) - 가드 경계 자체를 테스트
    cpu = rng.uniform(0.5, 2.0, size=N)
    network_in = rng.uniform(10, 60, size=N)
    network_out = rng.uniform(5, 40, size=N)
    cases.append(({
        "cpu_utilization": cpu.tolist(), "network_in": network_in.tolist(),
        "network_out": network_out.tolist(), "cost": [0.05] * N,
    }, {"resource_age_seconds": age_hours_sec - 1.0,
        "note": "나이가 가드 임계값(9000초)보다 1초 어림 - 지표는 완전 좀비지만 가드 경계상 판단 보류돼야 함"}))

    return cases


def build_ec2_files(rng: np.random.Generator):
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # ── ec2_train.json ──
    train = []
    for i in range(50):
        train.append(_window(f"ec2_train_{i:03d}", "EC2", "mock-ec2-instance", "normal",
                              i, make_ec2_normal(rng)))

    # ── ec2_eval.json ──
    eval_windows = []
    for i in range(20):
        eval_windows.append(_window(f"ec2_eval_normal_{i:03d}", "EC2", "mock-ec2-instance", "normal",
                                     i, make_ec2_normal(rng)))

    anomaly_idx = 0
    for i in range(6):
        eval_windows.append(_window(f"ec2_eval_anomaly_{anomaly_idx:03d}", "EC2", "mock-ec2-instance", "anomaly",
                                     20 + anomaly_idx, make_ec2_zombie_clear(rng),
                                     anomaly_type="idle_zombie"))
        anomaly_idx += 1
    for i in range(4):
        eval_windows.append(_window(f"ec2_eval_anomaly_{anomaly_idx:03d}", "EC2", "mock-ec2-instance", "anomaly",
                                     20 + anomaly_idx, make_ec2_zombie_boundary(rng),
                                     anomaly_type="idle_zombie_boundary"))
        anomaly_idx += 1

    edge_cases = make_ec2_edge_cases(rng)
    for i, (metrics, extra) in enumerate(edge_cases):
        eval_windows.append(_window(f"ec2_eval_edge_{i:03d}", "EC2", "mock-ec2-instance", "edge_normal",
                                     30 + i, metrics, **extra))

    return train, eval_windows


# ══════════════════════════════════════════════════════════════════
# Lambda 재시도 폭증
# ══════════════════════════════════════════════════════════════════

def make_lambda_normal(rng: np.random.Generator) -> dict:
    """정상 Lambda: 팀원 lambda_train.json과 같은 스타일(호출량 3단계 x 낮은
    베이스라인 에러율). 30포인트 전부 연속적인 노이즈로 생성 - edge_normal
    설계에서 발견한 "꼬리만 인위적으로 일정한 값" 문제(IForest가 절대 수치
    급변으로 오탐)를 피하려고 끝부분도 나머지와 같은 방식으로 흔든다."""
    inv_level = rng.choice([20.0, 80.0, 150.0])
    invocation = np.clip(inv_level + rng.uniform(-inv_level * 0.25, inv_level * 0.25, size=N), 1, None)
    error_rate = rng.uniform(0.0, 0.03, size=N)
    error = np.round(invocation * error_rate)
    duration = rng.uniform(90, 200, size=N)
    cost = invocation * 0.0000002 + (duration / 1000) * (128 / 1024) * invocation * 0.0000166667
    return {
        "invocation_count": invocation.tolist(),
        "error_count": error.tolist(),
        "duration_avg": duration.tolist(),
        "cost": cost.tolist(),
    }


def make_lambda_retry_anomaly(rng: np.random.Generator) -> dict:
    """마지막 3포인트: invocation>=10 AND error_rate>=50% 동시 만족.
    앞부분은 정상 베이스라인(에러율 낮음)."""
    baseline_inv = rng.uniform(30, 80, size=N - 3)
    baseline_err = np.round(baseline_inv * rng.uniform(0.01, 0.03, size=N - 3))
    surge_inv = rng.uniform(15, 60, size=3)
    surge_err_rate = rng.uniform(0.55, 0.9, size=3)
    surge_err = np.round(surge_inv * surge_err_rate)
    # 게이트(inv>=10)를 확실히 만족시키기 위해 최소값 보정
    surge_inv = np.maximum(surge_inv, 12.0)

    invocation = np.concatenate([baseline_inv, surge_inv])
    error = np.concatenate([baseline_err, surge_err])
    duration = rng.uniform(90, 200, size=N)
    cost = invocation * 0.0000002 + (duration / 1000) * (128 / 1024) * invocation * 0.0000166667
    return {
        "invocation_count": invocation.tolist(),
        "error_count": error.tolist(),
        "duration_avg": duration.tolist(),
        "cost": cost.tolist(),
    }


def make_lambda_edge_cases(rng: np.random.Generator) -> list[dict]:
    cases = []

    # 1) error_rate 45%(임계값 바로 아래) 3연속, invocation은 충분
    baseline_inv = rng.uniform(30, 80, size=N - 3)
    baseline_err = np.round(baseline_inv * rng.uniform(0.01, 0.03, size=N - 3))
    tail_inv = np.full(3, 40.0)
    tail_err = np.round(tail_inv * 0.45)
    invocation = np.concatenate([baseline_inv, tail_inv])
    error = np.concatenate([baseline_err, tail_err])
    cases.append(({
        "invocation_count": invocation.tolist(), "error_count": error.tolist(),
        "duration_avg": rng.uniform(90, 200, size=N).tolist(),
        "cost": (invocation * 0.00003).tolist(),
    }, {"note": "마지막 3포인트 error_rate=45% - 50% 문턱 바로 아래라 정상 처리돼야 함"}))

    # 2) invocation=8(게이트 10 미만), error_rate는 높음(90%)
    baseline_inv = rng.uniform(30, 80, size=N - 3)
    baseline_err = np.round(baseline_inv * rng.uniform(0.01, 0.03, size=N - 3))
    tail_inv = np.full(3, 8.0)
    tail_err = np.round(tail_inv * 0.9)
    invocation = np.concatenate([baseline_inv, tail_inv])
    error = np.concatenate([baseline_err, tail_err])
    cases.append(({
        "invocation_count": invocation.tolist(), "error_count": error.tolist(),
        "duration_avg": rng.uniform(90, 200, size=N).tolist(),
        "cost": (invocation * 0.00003).tolist(),
    }, {"note": "마지막 3포인트 invocation=8(최소 호출수 게이트 10 미만) - error_rate는 90%지만 게이트에서 걸러져 정상 처리돼야 함"}))

    # 3) 최근 3개 중 가장 오래된(첫)포인트만 정상으로 낮아서 지속성 조건 깨짐
    baseline_inv = rng.uniform(30, 80, size=N - 3)
    baseline_err = np.round(baseline_inv * rng.uniform(0.01, 0.03, size=N - 3))
    tail_inv = np.array([40.0, 40.0, 40.0])
    tail_err = np.array([8.0, 30.0, 32.0])  # 0.2, 0.75, 0.8 - 첫 포인트만 미달
    invocation = np.concatenate([baseline_inv, tail_inv])
    error = np.concatenate([baseline_err, tail_err])
    cases.append(({
        "invocation_count": invocation.tolist(), "error_count": error.tolist(),
        "duration_avg": rng.uniform(90, 200, size=N).tolist(),
        "cost": (invocation * 0.00003).tolist(),
    }, {"note": "최근 3포인트 중 가장 오래된 포인트의 error_rate만 20%(미달) - 3개 전부 조건이라 정상 처리돼야 함"}))

    # 4) 최근 3개 중 가장 최신(마지막) 포인트가 정상으로 회복돼서 지속성 조건 깨짐
    baseline_inv = rng.uniform(30, 80, size=N - 3)
    baseline_err = np.round(baseline_inv * rng.uniform(0.01, 0.03, size=N - 3))
    tail_inv = np.array([40.0, 40.0, 40.0])
    tail_err = np.array([32.0, 30.0, 8.0])  # 0.8, 0.75, 0.2 - 마지막 포인트만 회복
    invocation = np.concatenate([baseline_inv, tail_inv])
    error = np.concatenate([baseline_err, tail_err])
    cases.append(({
        "invocation_count": invocation.tolist(), "error_count": error.tolist(),
        "duration_avg": rng.uniform(90, 200, size=N).tolist(),
        "cost": (invocation * 0.00003).tolist(),
    }, {"note": "최근 3포인트 중 가장 최신 포인트가 error_rate 20%로 회복 - 직전까지 높았어도 정상 처리돼야 함"}))

    # 5) 대용량 서비스, error_rate 49.9%(임계값 바로 아래, 물량과 무관하게 비율만 판단됨을 검증)
    baseline_inv = rng.uniform(500, 900, size=N - 3)
    baseline_err = np.round(baseline_inv * rng.uniform(0.01, 0.03, size=N - 3))
    tail_inv = np.full(3, 1000.0)
    tail_err = np.round(tail_inv * 0.499)
    invocation = np.concatenate([baseline_inv, tail_inv])
    error = np.concatenate([baseline_err, tail_err])
    cases.append(({
        "invocation_count": invocation.tolist(), "error_count": error.tolist(),
        "duration_avg": rng.uniform(90, 200, size=N).tolist(),
        "cost": (invocation * 0.00003).tolist(),
    }, {"note": "대용량(호출 1000회) 서비스에서 error_rate=49.9% - 문턱은 비율 기준이라 물량과 무관하게 정상 처리돼야 함"}))

    return cases


def build_lambda_retry_file(rng: np.random.Generator):
    windows = []
    for i in range(20):
        windows.append(_window(f"lambda_eval_retry_normal_{i:03d}", "Lambda", "mock-lambda-function", "normal",
                                i, make_lambda_normal(rng)))
    for i in range(10):
        windows.append(_window(f"lambda_eval_retry_anomaly_{i:03d}", "Lambda", "mock-lambda-function", "anomaly",
                                20 + i, make_lambda_retry_anomaly(rng),
                                anomaly_type="error_retry_surge"))
    edge_cases = make_lambda_edge_cases(rng)
    for i, (metrics, extra) in enumerate(edge_cases):
        windows.append(_window(f"lambda_eval_retry_edge_{i:03d}", "Lambda", "mock-lambda-function", "edge_normal",
                                30 + i, metrics, **extra))
    return windows


# ══════════════════════════════════════════════════════════════════
# 검증: 실제 체크 함수로 라벨-판정 일치 확인
# ══════════════════════════════════════════════════════════════════

def verify_ec2(windows: list[dict]):
    for w in windows:
        _, is_idle, _ = da._low_utilization_check(
            "EC2", w["raw_metrics"], w.get("resource_age_seconds")
        )
        expect_idle = (w["label"] == "anomaly")
        assert is_idle == expect_idle, (
            f"{w['window_id']}: label={w['label']} 인데 is_idle={is_idle} "
            f"(peak_cpu={max(w['raw_metrics']['cpu_utilization']):.3f}, "
            f"io_sum={sum(w['raw_metrics']['network_in'])+sum(w['raw_metrics']['network_out']):.1f}, "
            f"age={w.get('resource_age_seconds')})"
        )


def verify_lambda(windows: list[dict]):
    for w in windows:
        _, is_surge = da._lambda_error_rate_check("Lambda", w["raw_metrics"])
        expect_surge = (w["label"] == "anomaly")
        assert is_surge == expect_surge, (
            f"{w['window_id']}: label={w['label']} 인데 is_surge={is_surge}"
        )


def main():
    rng = np.random.default_rng(2026)

    train, ec2_eval = build_ec2_files(rng)
    lambda_retry = build_lambda_retry_file(rng)

    verify_ec2(ec2_eval)
    verify_lambda(lambda_retry)
    print("검증 통과: 모든 윈도우의 라벨이 실제 체크 함수 판정과 일치합니다.")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "ec2_train.json").write_text(json.dumps(train, indent=2, ensure_ascii=False), encoding="utf-8")
    (OUT_DIR / "ec2_eval.json").write_text(json.dumps(ec2_eval, indent=2, ensure_ascii=False), encoding="utf-8")
    (OUT_DIR / "lambda_eval_retry.json").write_text(json.dumps(lambda_retry, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"ec2_train.json: {len(train)}개")
    print(f"ec2_eval.json: {len(ec2_eval)}개 "
          f"(normal={sum(1 for w in ec2_eval if w['label']=='normal')}, "
          f"anomaly={sum(1 for w in ec2_eval if w['label']=='anomaly')}, "
          f"edge_normal={sum(1 for w in ec2_eval if w['label']=='edge_normal')})")
    print(f"lambda_eval_retry.json: {len(lambda_retry)}개 "
          f"(normal={sum(1 for w in lambda_retry if w['label']=='normal')}, "
          f"anomaly={sum(1 for w in lambda_retry if w['label']=='anomaly')}, "
          f"edge_normal={sum(1 for w in lambda_retry if w['label']=='edge_normal')})")
    print(f"\n저장 위치: {OUT_DIR}")


if __name__ == "__main__":
    main()
