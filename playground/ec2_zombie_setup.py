"""
playground/ec2_zombie_setup.py

EC2 좀비(완전 유휴) 시나리오의 실 AWS 인스턴스를 준비한다.
ec2_overprovision_setup.py의 인스턴스 생성/SSM 로직을 그대로 재사용하되,
anomaly 그룹에는 SSM 부하를 아예 안 걸어서 자연스럽게 CPU 0%대(진짜 좀비)가
나오게 한다(오버프로비저닝 스크립트 자체 docstring에 있던 설명: "좀비는 인스턴스를
그냥 방치하면 자연스럽게 CPU 0%대가 나와서 '유발'이 필요 없었다").

anomaly 5대: 부하 없음(방치) — 진짜 좀비
normal   8대: 목표 45% 부하 — 정상 사용 중인 리소스

[실행 방법]
  1) 준비(2.5시간 뒤 측정 가능, 즉시 반환):
     python playground/ec2_zombie_setup.py --setup
  2) 2.5시간 뒤 측정+전체 파이프라인(탐지~QA) 실행:
     python playground/ec2_lambda_repeated_trial.py --scenario ec2 --run \
       --manifest playground/eval_outputs/ec2_zombie_manifest_<timestamp>.json
  3) 종료 후 정리(반드시 실행):
     python playground/ec2_zombie_setup.py --teardown --instance-ids <콤마구분>
"""
from __future__ import annotations

import sys
sys.stdout.reconfigure(encoding="utf-8")

import argparse
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3

from ec2_overprovision_setup import (
    N_ANOMALY, N_NORMAL, NORMAL_TARGET_CPU_PCT, WINDOW_SECONDS, N_VCPU,
    _launch_instances, _wait_ssm_online, _duty_cycle_command, teardown,
)

SCRIPT_VERSION = "1"
RESULT_DIR = Path(__file__).parent / "eval_outputs"


def setup() -> None:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    name_prefix = f"detection-test-ec2-zombie-{ts}"

    anomaly_ids = _launch_instances("anomaly", N_ANOMALY, name_prefix)
    normal_ids = _launch_instances("normal", N_NORMAL, name_prefix)
    all_ids = anomaly_ids + normal_ids

    print("running 상태 대기...")
    ec2 = boto3.client("ec2")
    ec2.get_waiter("instance_running").wait(InstanceIds=all_ids)
    desc = ec2.describe_instances(InstanceIds=all_ids)
    launch_times = {
        i["InstanceId"]: i["LaunchTime"].astimezone(timezone.utc).isoformat()
        for r in desc["Reservations"] for i in r["Instances"]
    }

    # normal 그룹만 SSM 부하 필요 (anomaly=좀비는 그냥 방치)
    print("SSM 등록 대기(최대 5분, 부팅+에이전트 기동 시간 필요)...")
    time.sleep(60)
    _wait_ssm_online(normal_ids)

    ssm = boto3.client("ssm")
    for iid in normal_ids:
        cmds = _duty_cycle_command(NORMAL_TARGET_CPU_PCT, WINDOW_SECONDS, N_VCPU)
        ssm.send_command(InstanceIds=[iid], DocumentName="AWS-RunShellScript", Parameters={"commands": cmds})
        print(f"[normal {iid}] 목표 CPU {NORMAL_TARGET_CPU_PCT}% 부하 시작 ({WINDOW_SECONDS}초)")
    for iid in anomaly_ids:
        print(f"[anomaly {iid}] 부하 없음 — 방치(좀비)")

    check_earliest = datetime.now(timezone.utc) + timedelta(seconds=WINDOW_SECONDS)
    manifest = {
        "script_version": SCRIPT_VERSION,
        "scenario": "ec2_zombie",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "check_earliest_utc": check_earliest.isoformat(),
        "normal_target_cpu_pct": NORMAL_TARGET_CPU_PCT,
        "instances": [
            {"instance_id": iid, "true_label": "anomaly", "launch_time_utc": launch_times[iid],
             "profile": "idle_zombie"}
            for iid in anomaly_ids
        ] + [
            {"instance_id": iid, "true_label": "normal", "launch_time_utc": launch_times[iid],
             "profile": f"target_cpu_{NORMAL_TARGET_CPU_PCT}pct"}
            for iid in normal_ids
        ],
    }
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    manifest_path = RESULT_DIR / f"ec2_zombie_manifest_{ts}.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    print(f"\n매니페스트 저장: {manifest_path}")
    print(f"측정 가능 시점(2.5시간 뒤): {check_earliest.isoformat()}")
    print(f"\n측정 명령:\n  python playground/ec2_lambda_repeated_trial.py --scenario ec2 --run --manifest {manifest_path}")
    print(f"\n종료 후 정리(반드시 실행):\n  python playground/ec2_zombie_setup.py --teardown --instance-ids {','.join(all_ids)}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--setup", action="store_true")
    parser.add_argument("--teardown", action="store_true")
    parser.add_argument("--instance-ids", type=str)
    args = parser.parse_args()

    if args.setup:
        setup()
    elif args.teardown:
        if not args.instance_ids:
            parser.error("--teardown에는 --instance-ids가 필요합니다")
        teardown(args.instance_ids.split(","))
    else:
        parser.error("--setup 또는 --teardown 중 하나를 지정하세요")


if __name__ == "__main__":
    main()
