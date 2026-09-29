"""
playground/real_demo/s3_real.py

S3 대량다운로드(요청 폭증) 시나리오 "완전 실연동" 데모.
playground/s3_repeated_trial.py(계정 이전과 무관 — SSM/IAM 인스턴스 프로파일을
전혀 안 써서 그대로 재사용 가능)의 버킷 준비 + 실제 GET 트래픽 생성 로직을
그대로 가져오고, 마지막 "파이프라인 실행"만 real_demo.common.run_real_scenario로
바꿨다(예전 detect()는 detection_node만 직접 호출 — 분류/결정/액션/QA/승인·웹
제어판 반영이 전혀 없었음).

[실행 방법]
  1) 버킷 준비(Request Metrics 활성화, 최소 30분~1시간 워밍업 권장):
     python playground/real_demo/s3_real.py --setup
  2) 실제 GET 트래픽 생성 + 실연동 파이프라인 실행
     (2.5시간 창 전체에 실시간으로 트래픽을 쏴야 해서 총 ~2.5시간+수 분):
     python playground/real_demo/s3_real.py --run --profile anomaly
     python playground/real_demo/s3_real.py --run --profile edge   # 엣지케이스(완만한 폭증)
  3) 종료 후 정리(반드시 실행):
     python playground/real_demo/s3_real.py --teardown
"""

from __future__ import annotations

import argparse
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

PLAYGROUND_ROOT = Path(__file__).resolve().parent.parent
if str(PLAYGROUND_ROOT) not in sys.path:
    sys.path.insert(0, str(PLAYGROUND_ROOT))

import boto3

from s3_repeated_trial import (
    AWS_REGION,
    NORMAL_BASE_REQUESTS_PER_WINDOW,
    _anomaly_bucket_name,
    _ensure_bucket_with_metrics,
)

from common import run_real_scenario

BUCKET_NAME = _anomaly_bucket_name(0)
OBJECT_SIZE_BYTES = 50_000  # s3_repeated_trial.py v5 기준(50KB)과 동일

# extreme: 예전 스크립트의 anomaly 시행과 동일(base * 4~6배 폭증)
# edge: [2026-09-27 추가] 훨씬 완만한 증가(base * 1.5~2배)에서도 탐지되는지 별도 확인
SPIKE_MULTIPLIER_RANGE = {
    "anomaly": (4.0, 6.0),
    "edge": (1.5, 2.0),
}


def setup() -> None:
    s3 = boto3.client("s3", region_name=AWS_REGION)
    _ensure_bucket_with_metrics(s3, BUCKET_NAME)
    print(
        "버킷 준비 완료. Request Metrics는 활성화 직후 바로 반영 안 될 수 있어 "
        "최소 30분~1시간 워밍업 후 --run을 권장합니다."
    )


def teardown() -> None:
    s3 = boto3.client("s3", region_name=AWS_REGION)
    try:
        objs = s3.list_objects_v2(Bucket=BUCKET_NAME).get("Contents", [])
        if objs:
            s3.delete_objects(
                Bucket=BUCKET_NAME,
                Delete={"Objects": [{"Key": o["Key"]} for o in objs]},
            )
        s3.delete_bucket(Bucket=BUCKET_NAME)
        print(f"버킷 삭제 완료: {BUCKET_NAME}")
    except Exception as exc:
        print(f"버킷 삭제 실패(이미 없을 수 있음): {exc}")


def run(
    profile: str, n_points: int = 30, period_seconds: int = 300, spike_periods: int = 3
) -> None:
    """s3_repeated_trial.run_anomaly_trial()과 동일한 실시간 트래픽 생성 —
    앞쪽 (n_points-spike_periods)개 구간은 평상시 수준, 마지막 spike_periods개
    구간만 폭증시켜서 실제 2.5시간 창을 실시간으로 채운다."""
    s3 = boto3.client("s3", region_name=AWS_REGION)
    key = "s3_real_demo_load.bin"
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=os.urandom(OBJECT_SIZE_BYTES))

    base = NORMAL_BASE_REQUESTS_PER_WINDOW
    lo, hi = SPIKE_MULTIPLIER_RANGE[profile]
    spike_level = base * random.uniform(lo, hi)
    print(
        f"[s3] profile={profile} 평상시 ~{round(base)}건/구간 x {n_points - spike_periods}개, "
        f"마지막 {spike_periods}개는 폭증 ~{round(spike_level)}건/구간"
    )

    def _get_once() -> None:
        try:
            s3.get_object(Bucket=BUCKET_NAME, Key=key)["Body"].read()
        except Exception:
            pass

    for period_idx in range(n_points):
        period_start = time.time()
        is_spike = period_idx >= (n_points - spike_periods)
        if is_spike:
            n_requests = max(1, round(spike_level * random.uniform(0.98, 1.02)))
        else:
            n_requests = max(1, round(base * random.uniform(0.85, 1.15)))
        # [2026-09-28] 순차 for-loop로 한 건씩 쏘면 스파이크 구간(2000건+)이 300초
        # 안에 다 안 끝나서 뒤 구간까지 밀리고, 특히 마지막 구간이 잘려나가 지속성
        # 체크가 실패하는 문제가 실측으로 확인됨(edos_real.py와 동일한 원인) —
        # 스레드풀로 동시에 쏴서 300초 안에 확실히 끝나게 한다.
        with ThreadPoolExecutor(max_workers=64) as pool:
            list(pool.map(lambda _: _get_once(), range(n_requests)))
        elapsed = time.time() - period_start
        remaining = period_seconds - elapsed
        print(
            f"  구간 {period_idx + 1}/{n_points}({'SPIKE' if is_spike else '정상'}): "
            f"{n_requests}건 완료 ({elapsed:.1f}s), {max(0, remaining):.1f}s 대기"
        )
        if remaining > 0:
            time.sleep(remaining)

    run_real_scenario(
        scenario_key="s3",
        resource_id=BUCKET_NAME,
        resource_type="S3",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--setup", action="store_true")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--teardown", action="store_true")
    parser.add_argument("--profile", choices=["anomaly", "edge"], default="anomaly")
    args = parser.parse_args()

    if args.setup:
        setup()
    elif args.run:
        run(args.profile)
    elif args.teardown:
        teardown()
    else:
        parser.error("--setup / --run / --teardown 중 하나를 지정하세요")


if __name__ == "__main__":
    main()
