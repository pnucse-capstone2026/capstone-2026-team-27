"""
playground/real_demo/edos_real.py

AutoScaling EDoS 시나리오 "완전 실연동" 데모.
playground/autoscaling_edos_traffic_trial.py(실측 검증까지 끝난 스크립트)의
ALB/ASG 인프라 구성 + 실제 트래픽 생성 로직은 전부 그대로 재사용하고,
마지막 "파이프라인 실행" 부분만 바꿨다:

  - 예전 스크립트(run_full_pipeline_from_metrics): 노드를 직접 순서대로 호출,
    승인 게이트는 --bypass-approval로 강제 통과(측정 전용) 또는 그냥 멈춤,
    웹 제어판 상태 위젯도 안 움직임.
  - 이 스크립트(real_demo.common.run_real_scenario): 실제 LangGraph
    checkpointer/그래프를 그대로 태워서, 웹 제어판 '승인 대기' 탭에 실제로
    올라가고 사람이 승인하면 재개되며, 노드 진행 상황도 실시간 반영된다.

[실행 방법]
  1) 인프라 준비(ALB+ASG 1대, 헬스체크까지 2~3분):
     python playground/real_demo/edos_real.py --setup
  2) 실제 트래픽 생성 + 실연동 파이프라인 실행
     (baseline 150분 + spike 35분 + 지표 반영 대기 5분 = 총 ~3.2시간):
     python playground/real_demo/edos_real.py --run --profile extreme_spike
     python playground/real_demo/edos_real.py --run --profile edge   # 엣지케이스(완만한 증가)
  3) 종료 후 정리(반드시 실행):
     python playground/real_demo/edos_real.py --teardown
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

PLAYGROUND_ROOT = Path(__file__).resolve().parent.parent
if str(PLAYGROUND_ROOT) not in sys.path:
    sys.path.insert(0, str(PLAYGROUND_ROOT))

import boto3
import requests

from autoscaling_edos_traffic_trial import (
    ALB_NAME,
    AWS_REGION,
    BASELINE_RPS,
    EDGE_SPIKE_RPS,
    SPIKE_RPS,
    RESULT_DIR,
    _alb_dimension_value,
    _asg_name,
    _fetch_alb_request_count,
    _tg_dimension_value,
    setup_all,
    teardown_all,
)

from common import run_real_scenario


def _concurrent_traffic_loop(url: str, rps_getter, stop_event: threading.Event) -> None:
    """autoscaling_edos_traffic_trial._traffic_loop()는 요청 하나를 보내고
    응답이 올 때까지 기다린 뒤에야 다음 요청을 보내는 단일 스레드 구조라, 목표
    rps가 높아지면(엣지 10배) 응답 지연 때문에 실제 달성 처리량이 목표에
    한참 못 미치고 시간이 지날수록 흔들리는 문제가 있었다(2026-09-28 real_demo
    실측에서 발견 — 스파이크 첫 구간만 반짝 튀고 이후 급격히 가라앉음).
    스레드풀로 요청을 쏘고 응답을 기다리지 않고 바로 다음 요청을 쏴서, 응답
    지연과 무관하게 목표 rps를 그대로 유지한다."""
    session = requests.Session()

    def _fire() -> None:
        try:
            session.get(url, timeout=3)
        except Exception:
            pass

    with ThreadPoolExecutor(max_workers=64) as pool:
        while not stop_event.is_set():
            rps = max(0.1, rps_getter())
            interval = 1.0 / rps
            pool.submit(_fire)
            stop_event.wait(interval)


MANIFEST_PATH = RESULT_DIR / "autoscaling_edos_traffic_manifest.json"
ASG_NAME = _asg_name("anomaly", 0)


def setup() -> None:
    setup_all(n_anomaly=1, n_normal=0)


def teardown() -> None:
    teardown_all(n_anomaly=1, n_normal=0)


def run(
    profile: str, baseline_minutes: int, spike_minutes: int, metric_wait_sec: int
) -> None:
    with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
        manifest = json.load(f)
    alb_dns = manifest["alb_dns"]
    port = manifest["ports"][ASG_NAME]

    elb = boto3.client("elbv2", region_name=AWS_REGION)
    alb_arn = elb.describe_load_balancers(Names=[ALB_NAME])["LoadBalancers"][0][
        "LoadBalancerArn"
    ]
    alb_id_suffix = _alb_dimension_value(alb_arn)
    tg = elb.describe_target_groups(Names=["tg-anomaly-0"])["TargetGroups"][0]
    tg_id_suffix = _tg_dimension_value(tg["TargetGroupArn"])

    spike_rps = EDGE_SPIKE_RPS if profile == "edge" else SPIKE_RPS
    url = f"http://{alb_dns}:{port}/"
    current_rps = [BASELINE_RPS]
    stop_event = threading.Event()
    traffic_thread = threading.Thread(
        target=_concurrent_traffic_loop,
        args=(url, lambda: current_rps[0], stop_event),
        daemon=True,
    )
    traffic_thread.start()

    try:
        print(f"[edos] baseline rps={BASELINE_RPS}로 {baseline_minutes}분 유지...")
        time.sleep(baseline_minutes * 60)

        print(
            f"[edos] profile={profile} rps={spike_rps}로 폭증, {spike_minutes}분 유지..."
        )
        current_rps[0] = spike_rps
        time.sleep(spike_minutes * 60)

        print(f"[edos] 메트릭 반영 대기 {metric_wait_sec}초...")
        time.sleep(metric_wait_sec)
    finally:
        stop_event.set()
        traffic_thread.join(timeout=5)

    run_real_scenario(
        scenario_key="edos",
        resource_id=ASG_NAME,
        resource_type="AutoScaling",
        extra_metrics_fetcher=lambda: {
            "request_count": _fetch_alb_request_count(
                alb_id_suffix, tg_id_suffix, datetime.now(timezone.utc)
            )
        },
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--setup", action="store_true")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--teardown", action="store_true")
    parser.add_argument(
        "--profile", choices=["extreme_spike", "edge"], default="extreme_spike"
    )
    parser.add_argument("--baseline-minutes", type=int, default=150)
    parser.add_argument("--spike-minutes", type=int, default=35)
    parser.add_argument("--metric-wait-sec", type=int, default=300)
    args = parser.parse_args()

    if args.setup:
        setup()
    elif args.run:
        run(
            args.profile,
            args.baseline_minutes,
            args.spike_minutes,
            args.metric_wait_sec,
        )
    elif args.teardown:
        teardown()
    else:
        parser.error("--setup / --run / --teardown 중 하나를 지정하세요")


if __name__ == "__main__":
    main()
