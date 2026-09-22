"""
playground/ec2_zombie_edge_setup.py

EC2 좀비(유휴) 시나리오 v2 — 경계(edge) 케이스를 포함한 재실험.

[v1(ec2_zombie_setup.py) 대비 변경점]
1. 2026-09-14 첫 실행에서 normal(목표 45%) 그룹의 SSM duty-cycle 부하가 실제로는
   걸리지 않아(원인 미확정 — systemd-run 명령 자체는 SSM상 Success였으나 그 이후
   실제로 부하가 지속됐는지는 인스턴스가 이미 종료돼 사후 확인 불가) anomaly/normal이
   구분되지 않는 문제가 있었다. 이번엔 부하 시작 후 약 12~15분 뒤 CloudWatch로
   중간 점검(--verify)해서 target별로 CPU가 실제로 벌어지고 있는지 먼저 확인한다.
2. 같은 실행에서 완전 유휴 인스턴스도 idle 판정 network I/O 조건에 걸려 좀비로
   탐지되지 못하는 버그를 발견했다(SSM 에이전트 자체 하트비트 트래픽이 기존
   5MB/day 임계값을 초과) — detection_agent.py의 임계값을 10MB/day로 상향 수정함.
3. 순수 0% vs 45%만 테스트하던 v1과 달리, EC2_IDLE_CPU_THRESHOLD_PCT(5.0%) 경계
   근처의 케이스를 추가해 경계 판정 성능까지 함께 검증한다.

[그룹 구성 — 13대]
  true_zombie  3대: 목표 0%(부하 없음, 완전 유휴)         true_label=anomaly
  edge_zombie  2대: 목표 4%(임계값 5% 바로 아래, 여전히 좀비 판정 되어야 함) true_label=anomaly
  true_normal  6대: 목표 45%(확실한 정상 사용)             true_label=normal
  edge_normal  2대: 목표 7%(임계값 5% 바로 위, 좀비로 오판되면 안 됨)        true_label=normal

[실행 방법]
  1) 준비 + 부하 시작(즉시 반환):
     python playground/ec2_zombie_edge_setup.py --setup
  2) 부하 시작 12~15분 후 중간 점검(부하가 실제로 걸렸는지 CloudWatch로 확인):
     python playground/ec2_zombie_edge_setup.py --verify --manifest <manifest 경로>
     (verify 결과가 이상하면 --resend-load 로 해당 그룹에 SSM 명령 재전송)
  3) 측정 가능 시점(2.5시간 후) 전체 파이프라인 측정:
     python playground/ec2_lambda_repeated_trial.py --scenario ec2 --run --manifest <manifest 경로>
  4) 종료 후 정리(반드시 실행):
     python playground/ec2_zombie_edge_setup.py --teardown --instance-ids <콤마구분>
"""

from __future__ import annotations

import sys

sys.stdout.reconfigure(encoding="utf-8")

import argparse
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ec2_overprovision_setup import (
    WINDOW_SECONDS,
    N_VCPU,
    _launch_instances,
    _wait_ssm_online,
    _duty_cycle_command,
    teardown,
)

import boto3

from _runner_tag import runner_suffix

SCRIPT_VERSION = "2"
RESULT_DIR = Path(__file__).parent / "eval_outputs"

GROUPS = {
    "true_zombie": {"n": 3, "target_cpu_pct": 0.0, "true_label": "anomaly"},
    "edge_zombie": {"n": 2, "target_cpu_pct": 4.0, "true_label": "anomaly"},
    "true_normal": {"n": 6, "target_cpu_pct": 45.0, "true_label": "normal"},
    "edge_normal": {"n": 2, "target_cpu_pct": 7.0, "true_label": "normal"},
}


def setup() -> None:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S") + runner_suffix()
    name_prefix = f"detection-test-ec2-zombie-edge-{ts}"

    group_ids: dict[str, list[str]] = {}
    for group, spec in GROUPS.items():
        group_ids[group] = _launch_instances(group, spec["n"], name_prefix)
    all_ids = [iid for ids in group_ids.values() for iid in ids]

    print("running 상태 대기...")
    ec2 = boto3.client("ec2")
    ec2.get_waiter("instance_running").wait(InstanceIds=all_ids)
    desc = ec2.describe_instances(InstanceIds=all_ids)
    launch_times = {
        i["InstanceId"]: i["LaunchTime"].astimezone(timezone.utc).isoformat()
        for r in desc["Reservations"]
        for i in r["Instances"]
    }

    print("SSM 등록 대기(최대 5분, 부팅+에이전트 기동 시간 필요)...")
    time.sleep(60)
    _wait_ssm_online(all_ids)

    ssm = boto3.client("ssm")
    load_started_at = datetime.now(timezone.utc)
    for group, spec in GROUPS.items():
        target = spec["target_cpu_pct"]
        for iid in group_ids[group]:
            if target <= 0.0:
                print(f"[{group} {iid}] 부하 없음 - 방치(완전 유휴)")
                continue
            cmds = _duty_cycle_command(target, WINDOW_SECONDS, N_VCPU)
            ssm.send_command(
                InstanceIds=[iid],
                DocumentName="AWS-RunShellScript",
                Parameters={"commands": cmds},
            )
            print(f"[{group} {iid}] 목표 CPU {target}% 부하 시작 ({WINDOW_SECONDS}초)")

    check_earliest = datetime.now(timezone.utc) + timedelta(seconds=WINDOW_SECONDS)
    verify_earliest = load_started_at + timedelta(seconds=900)  # 15분 후 중간점검 가능
    manifest = {
        "script_version": SCRIPT_VERSION,
        "scenario": "ec2_zombie_edge",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "load_started_at_utc": load_started_at.isoformat(),
        "verify_earliest_utc": verify_earliest.isoformat(),
        "check_earliest_utc": check_earliest.isoformat(),
        "groups": {g: spec["target_cpu_pct"] for g, spec in GROUPS.items()},
        "instances": [
            {
                "instance_id": iid,
                "true_label": spec["true_label"],
                "launch_time_utc": launch_times[iid],
                "profile": f"{group}_target_cpu_{spec['target_cpu_pct']}pct",
                "group": group,
            }
            for group, spec in GROUPS.items()
            for iid in group_ids[group]
        ],
    }
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    manifest_path = RESULT_DIR / f"ec2_zombie_edge_manifest_{ts}.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    print(f"\n매니페스트 저장: {manifest_path}")
    print(f"중간점검 가능 시점(15분 후): {verify_earliest.isoformat()}")
    print(f"측정 가능 시점(2.5시간 뒤): {check_earliest.isoformat()}")
    print(
        f"\n중간점검 명령:\n  python playground/ec2_zombie_edge_setup.py --verify --manifest {manifest_path}"
    )
    print(
        f"\n측정 명령:\n  python playground/ec2_lambda_repeated_trial.py --scenario ec2 --run --manifest {manifest_path}"
    )
    print(
        f"\n종료 후 정리(반드시 실행):\n  python playground/ec2_zombie_edge_setup.py --teardown --instance-ids {','.join(all_ids)}"
    )


def verify(manifest_path: str) -> None:
    """부하 시작 후 15분 뒤, 각 그룹의 CPU가 실제로 목표에 맞게 벌어지고 있는지 확인.
    5분 단위 CloudWatch 포인트가 최소 2~3개는 쌓여야 의미가 있어 15분 이상 기다린 후 호출한다."""
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    cw = boto3.client("cloudwatch")
    end = datetime.now(timezone.utc)
    start = end - timedelta(seconds=900)

    by_group: dict[str, list[float]] = {}
    for inst in manifest["instances"]:
        resp = cw.get_metric_data(
            MetricDataQueries=[
                {
                    "Id": "m0",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/EC2",
                            "MetricName": "CPUUtilization",
                            "Dimensions": [
                                {"Name": "InstanceId", "Value": inst["instance_id"]}
                            ],
                        },
                        "Period": 300,
                        "Stat": "Average",
                    },
                    "ReturnData": True,
                }
            ],
            StartTime=start,
            EndTime=end,
            ScanBy="TimestampAscending",
        )
        values = resp["MetricDataResults"][0]["Values"]
        group = inst["group"]
        by_group.setdefault(group, []).extend(values)
        print(
            f"  [{group} {inst['instance_id']}] 최근 CPU 포인트: {[round(v, 2) for v in values]}"
        )

    print("\n=== 그룹별 평균 CPU (지난 15분) ===")
    ok = True
    for group, spec in GROUPS.items():
        vals = by_group.get(group, [])
        avg = sum(vals) / len(vals) if vals else None
        target = spec["target_cpu_pct"]
        print(f"  {group}: 평균={avg}, 목표={target}%, 포인트수={len(vals)}")
        if avg is None or (target > 0 and avg < target * 0.3):
            print(f"    ⚠️ 목표({target}%) 대비 너무 낮음 — 부하 미작동 의심")
            ok = False
    if ok:
        print("\n모든 그룹이 목표 방향으로 CPU가 형성되고 있음 — 계속 진행 가능")
    else:
        print(
            "\n일부 그룹의 부하가 걸리지 않은 것으로 보임 — --resend-load로 재전송 필요"
        )


def resend_load(manifest_path: str, groups: list[str]) -> None:
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    ssm = boto3.client("ssm")
    remaining = manifest["check_earliest_utc"]
    remaining_sec = int(
        (datetime.fromisoformat(remaining) - datetime.now(timezone.utc)).total_seconds()
    )
    remaining_sec = max(remaining_sec, 300)
    for inst in manifest["instances"]:
        if inst["group"] not in groups:
            continue
        target = GROUPS[inst["group"]]["target_cpu_pct"]
        if target <= 0.0:
            continue
        cmds = _duty_cycle_command(target, remaining_sec, N_VCPU)
        ssm.send_command(
            InstanceIds=[inst["instance_id"]],
            DocumentName="AWS-RunShellScript",
            Parameters={"commands": cmds},
        )
        print(
            f"[재전송][{inst['group']} {inst['instance_id']}] 목표 {target}% ({remaining_sec}초 남음)"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--setup", action="store_true")
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--resend-load", action="store_true")
    parser.add_argument(
        "--groups", type=str, help="--resend-load 대상 그룹, 콤마구분(기본: 전체)"
    )
    parser.add_argument("--teardown", action="store_true")
    parser.add_argument("--manifest", type=str)
    parser.add_argument("--instance-ids", type=str)
    args = parser.parse_args()

    if args.setup:
        setup()
    elif args.verify:
        if not args.manifest:
            parser.error("--verify에는 --manifest가 필요합니다")
        verify(args.manifest)
    elif args.resend_load:
        if not args.manifest:
            parser.error("--resend-load에는 --manifest가 필요합니다")
        groups = args.groups.split(",") if args.groups else list(GROUPS.keys())
        resend_load(args.manifest, groups)
    elif args.teardown:
        if not args.instance_ids:
            parser.error("--teardown에는 --instance-ids가 필요합니다")
        teardown(args.instance_ids.split(","))
    else:
        parser.error(
            "--setup, --verify, --resend-load, --teardown 중 하나를 지정하세요"
        )


if __name__ == "__main__":
    main()
