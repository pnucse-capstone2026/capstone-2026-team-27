"""
playground/seed_mock_iforest_buffer.py

Stage 3(실측 63일치 재생) 대신 채택한 대안: CloudWatch API 호출 없이,
리소스 타입별 "정상 시나리오" mock 데이터로 IForest 학습 버퍼와
iforest_unified.pkl을 미리 채워서 콜드스타트/사전학습 즉석 시딩 과정
자체를 건너뛴다.

이 스크립트는 1회성 오프라인 셋업용이다 — detection_agent.py의 런타임
로직은 전혀 안 바뀌고, models/ 아래 두 캐시 파일만 새로 만든다.
직후 상태는 pipeline/detection_agent.py의 MOCK_SEED_BUFFER_FROZEN=True로
동결돼 있어서, 정상 경로(실데이터가 FIFO로 mock을 밀어내며 자연 교체)로
전환하려면 그 플래그만 False로 바꾸면 된다(코드 변경 불필요, 이미 구현됨).

각 리소스 타입의 "정상" 정의 — 감으로 고르지 않고 아래 근거를 따름:
  - EC2: cpu_utilization은 유휴(<5%, 별도 절대임계값 탐지 대상)도
    포화(~85%+)도 아닌 중간대(30/45/60%) 여러 수준을 윈도우마다 무작위로
    섞어서 "정상 범위의 폭"을 반영. network_in/out은 실측 검증
    (playground/validate_real_aws_buffer.py)에서 확인된 실제 패턴 —
    20~40분 주기로 9K/16K/23K bytes 다단계로 반복 튀는 특성을 그대로
    재현한다(추측이 아니라 실측 근거).
  - Lambda: invocation_count는 저/중/고 트래픽 수준을 섞고, error_count는
    낮은 베이스라인 에러율(0~2%)만 반영 — 우리가 만든 재시도 폭증
    임계값(50%)과 확실히 구분되는 수준.
  - S3: number_of_requests/bytes_downloaded는 서로 비례하게(요청당 평균
    다운로드량 일정) 생성 — 실제 트래픽 패턴의 기본 상관관계를 반영.
  - RDS: database_connections를 0 근처가 아니라 항상 유의미하게 활성
    상태(5/15/30)로 유지 — 0에 가까우면 그 자체로 유휴(별도 이상 유형)
    이므로 "정상"의 정의에서 명시적으로 제외.
  - AutoScaling: group_in_service_instances가 desired_capacity와 거의
    항상 일치하도록(정상 ASG는 스케일링 지연이 드묾) 생성.
  - 비용(cost) 필드: EC2/RDS/AutoScaling은 실제로 가동시간 기반 정액
    과금이라(cost_estimator.py 확인) 윈도우 내내 고정값, Lambda/S3는
    사용량 비례 과금이라 그 윈도우의 실제 사용량에 연동해서 생성 —
    실제 pipeline/cost_estimator.py의 계산 방식과 일치시킴.

[실행 방법]
  프로젝트 루트에서: python playground/seed_mock_iforest_buffer.py
  (실행 전 반드시 models/*.pkl 백업 확인 — 기존 캐시를 덮어씀)
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import numpy as np

import pipeline.detection_agent as da

N = 30                 # 윈도우당 포인트 수 (실제 파이프라인과 동일)
WINDOWS_PER_TYPE = 30   # MAX_WINDOWS_PER_TYPE과 동일 — Phase 5에서 검증된 상한 그대로 재사용
RANDOM_SEED = 2026


def _noisy(rng, base, noise_frac=0.03, size=N):
    """base 기준 ±noise_frac 비율의 균등 노이즈. 실측 EC2 데이터에서 관찰된
    수준(급격한 변동 아닌 잔노이즈)을 흉내낸다."""
    noise = base * noise_frac
    return base + rng.uniform(-noise, noise, size=size)


def _ec2_network_in_pattern(rng):
    """실측 확인된 패턴(validate_real_aws_buffer.py): 20~40분 주기로
    9K/16K/23K bytes 다단계로 반복 튀는 network_in. period_seconds=300초
    기준 20~40분=4~8포인트 주기."""
    tiers = [9000.0, 16000.0, 23000.0]
    values = np.zeros(N)
    i = 0
    while i < N:
        cycle_len = rng.integers(4, 9)  # 4~8포인트 = 20~40분
        tier = tiers[rng.integers(0, len(tiers))]
        span = min(cycle_len, N - i)
        values[i:i + span] = tier + rng.uniform(-tier * 0.05, tier * 0.05, size=span)
        i += span
    return values


def make_ec2_window(rng):
    cpu_level = rng.choice([30.0, 45.0, 60.0])  # 유휴(<5%)도 포화(~85%+)도 아닌 정상 가동 범위
    cpu = _noisy(rng, cpu_level, noise_frac=0.08)
    network_in = _ec2_network_in_pattern(rng)
    network_out = network_in * rng.uniform(0.55, 0.75) + rng.uniform(-200, 200, size=N)
    network_out = np.clip(network_out, 0, None)
    cost = np.full(N, 0.05)  # EC2는 가동시간 기반 정액과금 — 윈도우 내내 고정 (cost_estimator.py와 일치)
    return {
        "cpu_utilization": cpu.tolist(),
        "network_in": network_in.tolist(),
        "network_out": network_out.tolist(),
        "cost": cost.tolist(),
    }


def make_lambda_window(rng):
    invocation_level = rng.choice([10.0, 50.0, 150.0])
    invocation = np.clip(_noisy(rng, invocation_level, noise_frac=0.15), 1, None)
    error_rate = rng.uniform(0.0, 0.02)  # 0~2% 베이스라인 에러율 — 재시도 폭증 임계값(50%)과 확실히 구분
    error = np.round(invocation * error_rate)
    duration = _noisy(rng, rng.choice([100.0, 200.0, 300.0]), noise_frac=0.1)
    cost = invocation * 0.0000002 + (duration / 1000) * (128 / 1024) * invocation * 0.0000166667
    # 2026-09-12 추가 — throttle_count/async_event_age. F-1 실측(정상 시나리오는
    # 동시성 여유가 있어 스로틀이 거의 발생 안 함) 근거로 baseline은 0에 가깝게
    # 유지 — 재시도 폭증 임계값(THROTTLE_RATE_THRESHOLD=0.4)과 확실히 구분.
    # mask=0(값 없음)이 아니라 "실측은 됐고 값이 0"으로 채워야, 실제 운영에서
    # 항상 조회되는 지표(cloudwatch_client.py가 매 윈도우 fetch)와 스키마가
    # 일치한다 — 없으면 mock 학습 데이터가 이 두 컬럼을 전부 mask=0으로 배워서
    # 실제 운영(mask=1, value~0)과 어긋남.
    throttle_count = np.round(np.clip(_noisy(rng, 0.2, noise_frac=1.0), 0, None))
    async_event_age = np.clip(_noisy(rng, 50.0, noise_frac=0.5), 0, None)  # ms, 큐 대기 거의 없음
    return {
        "invocation_count": invocation.tolist(),
        "error_count": error.tolist(),
        "duration_avg": duration.tolist(),
        "cost": cost.tolist(),
        "throttle_count": throttle_count.tolist(),
        "async_event_age": async_event_age.tolist(),
    }


def make_s3_window(rng):
    requests_level = rng.choice([100.0, 500.0, 1000.0])
    requests = np.clip(_noisy(rng, requests_level, noise_frac=0.1), 1, None)
    bytes_per_request = rng.uniform(1500.0, 2500.0)  # 요청당 평균 다운로드량 — 요청수에 비례
    bytes_downloaded = requests * bytes_per_request
    cost = requests * 0.00000035  # 조회 비용 위주 (cost_estimator.py의 S3_REQUEST_PRICE_READ)
    return {
        "number_of_requests": requests.tolist(),
        "bytes_downloaded": bytes_downloaded.tolist(),
        "cost": cost.tolist(),
    }


def make_rds_window(rng):
    cpu_level = rng.choice([20.0, 40.0, 55.0])
    cpu = _noisy(rng, cpu_level, noise_frac=0.08)
    connections_level = rng.choice([5.0, 15.0, 30.0])  # 0 근처는 유휴(별도 이상 유형)라 제외
    connections = np.clip(_noisy(rng, connections_level, noise_frac=0.15), 1, None)
    read_iops = _noisy(rng, connections_level * 6.0, noise_frac=0.15)
    write_iops = _noisy(rng, connections_level * 4.0, noise_frac=0.15)
    cost = np.full(N, 0.08)  # RDS도 가동시간 기반 정액과금
    return {
        "cpu_utilization": cpu.tolist(),
        "database_connections": connections.tolist(),
        "read_iops": read_iops.tolist(),
        "write_iops": write_iops.tolist(),
        "cost": cost.tolist(),
    }


def make_autoscaling_window(rng):
    desired_level = rng.choice([2.0, 3.0, 5.0])
    desired = np.full(N, desired_level)
    # 정상 ASG는 desired와 실제 실행 인스턴스 수가 거의 항상 일치 (스케일링 지연은 드묾)
    in_service = desired.copy()
    lag_points = rng.integers(0, 2)  # 0~1개 포인트만 아주 드물게 살짝 어긋남
    if lag_points:
        idx = rng.integers(0, N)
        in_service[idx] = max(0, desired_level - 1)
    cost = np.full(N, 0.05) * desired  # 인스턴스당 정액 × 대수
    return {
        "group_desired_capacity": desired.tolist(),
        "group_in_service_instances": in_service.tolist(),
        "cost": cost.tolist(),
    }


GENERATORS = {
    "EC2": make_ec2_window,
    "Lambda": make_lambda_window,
    "S3": make_s3_window,
    "RDS": make_rds_window,
    "AutoScaling": make_autoscaling_window,
}


def main() -> None:
    rng = np.random.default_rng(RANDOM_SEED)

    buffer_by_type: dict[str, list[np.ndarray]] = {}
    for resource_type, gen in GENERATORS.items():
        windows = []
        for _ in range(WINDOWS_PER_TYPE):
            metrics = gen(rng)
            X = da.build_unified_feature_matrix(resource_type, metrics)
            windows.append(X)
        buffer_by_type[resource_type] = windows
        print(f"  {resource_type}: {len(windows)}개 mock 윈도우 생성")

    combined = np.vstack([np.vstack(v) for v in buffer_by_type.values() if v])
    print(f"\n총 {combined.shape[0]}행으로 iforest_unified.pkl 학습 중...")
    da._fit_and_cache_unified(combined)
    da._save_training_buffer(buffer_by_type, pending_count=0)

    print(f"\n완료: {da.IFOREST_MODEL_DIR}/iforest_unified.pkl, "
          f"{da.IFOREST_MODEL_DIR}/iforest_unified_train_buffer.pkl 갱신됨")
    print("MOCK_SEED_BUFFER_FROZEN=True 상태이므로 실데이터로 자연 교체는 아직 비활성.")


if __name__ == "__main__":
    main()
