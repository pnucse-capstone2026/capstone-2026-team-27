"""
playground/ec2_overprovision_setup.py

EC2 오버프로비저닝 시나리오(peak CPU 5~20%)의 실 AWS 인스턴스를 준비한다.
detection_agent.py의 EC2_OVERPROVISION_CPU_THRESHOLD_PCT(20.0) 도입에 맞춰,
좀비(완전 유휴) 실험과 구분되는 새 실측이 필요해서 만들었다.

[좀비 실험과 다른 점]
좀비는 인스턴스를 그냥 방치하면 자연스럽게 CPU 0%대가 나와서 "유발"이 필요 없었다.
오버프로비저닝은 "쓰긴 쓰는데 낮게 쓰는" 상태를 인위적으로 만들어야 하므로, SSM으로
인스턴스 안에서 duty-cycle(바쁨/휴식 반복) 방식의 CPU 부하를 목표 %로 맞춰 튼다.
phase_g_real_world_validation.py의 SSM 부하(yes 프로세스로 100% 밀어붙이는 방식)는
퍼센트 제어가 안 돼서 그대로 못 쓰고, 이 스크립트는 duty-cycle 방식으로 직접 구현한다.
    목표 CPU% = busy_sec / (busy_sec + idle_sec), vCPU 개수만큼 병렬 실행
    (모든 vCPU가 같은 비율로 돌면 CloudWatch의 전체 평균 CPUUtilization도 그 %에 수렴)

t3.micro가 아니라 t3.small을 쓴다 — t3.micro는 EC2_HOURLY_PRICE_USD 표의 최저 tier라
Resize를 실행해도 절감액이 항상 $0으로 나온다(이미 좀비 실험에서 확인된 문제).

anomaly 5대: 목표 12% (좀비 임계값 5%보다 위, 오버프로비저닝 임계값 20%보다 아래)
normal   8대: 목표 45% (오버프로비저닝 임계값을 확실히 넘겨서 경계 근처 애매함 방지)

측정 자체(_low_utilization_check)는 network I/O도 AND 조건으로 보므로, 이 스크립트는
network_in/out을 낮게(부트스트랩 수준) 유지한다 — CPU 부하만 발생시키고 네트워크
트래픽은 만들지 않으므로 자연히 낮게 나온다.

[실행 방법]
  1) 준비 + 부하 시작 (2.5시간 지속, 즉시 반환):
     python playground/ec2_overprovision_setup.py --setup
  2) 2.5시간 뒤 측정 (매니페스트 자동 생성됨):
     python playground/ec2_lambda_repeated_trial.py --scenario ec2 --run \
       --manifest playground/eval_outputs/ec2_overprovision_manifest_<timestamp>.json
  3) 종료 후 정리(비용 방지, 반드시 실행):
     python playground/ec2_overprovision_setup.py --teardown --tag-suffix <timestamp>

[생성 파일]
  playground/eval_outputs/ec2_overprovision_manifest_{timestamp}.json
"""

from __future__ import annotations

SCRIPT_VERSION = "1"

import argparse
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

import boto3

RESULT_DIR = PROJECT_ROOT / "playground" / "eval_outputs"

# 좀비 실험 인스턴스와 동일한 AMI/보안그룹/서브넷 재사용 (SSM agent 설치 확인된 조합)
AMI_ID = "ami-08d82cf148c92fcc3"
SECURITY_GROUP_IDS = ["sg-01eb420b11ed6706e"]
SUBNET_ID = "subnet-08c39c64faa7c9364"
INSTANCE_TYPE = "t3.small"   # t3.micro는 최저 tier라 Resize 절감액이 항상 0 — 반드시 한 단계 위
IAM_INSTANCE_PROFILE = "detection-test-ec2-ssm-role"
N_VCPU = 2  # t3.small

N_ANOMALY = 5
N_NORMAL = 8
ANOMALY_TARGET_CPU_PCT = 12.0   # 5% < x <= 20% (오버프로비저닝 밴드)
NORMAL_TARGET_CPU_PCT = 45.0    # 20%를 확실히 넘겨 경계 근처 애매함 방지

WINDOW_POINTS = 30
PERIOD_SECONDS = 300
WINDOW_SECONDS = WINDOW_POINTS * PERIOD_SECONDS  # 9000s = 2.5h — detection_agent의 나이가드와 동일

DUTY_CYCLE_PERIOD_SEC = 1.0  # busy+idle 합이 이 값이 되도록 (짧을수록 CPU% 변동이 매끈함)


def _duty_cycle_command(target_pct: float, duration_sec: int, n_vcpu: int) -> list[str]:
    """vCPU 개수만큼 병렬로 busy/idle 반복 루프를 백그라운드 실행하는 셸 명령 목록.

    yes를 target_pct/100 비율만큼 돌리고 나머지는 sleep — 평균 CPU%가 target_pct에 수렴한다.
    stress-ng 등 별도 설치 없이 AL2023 기본 셸만으로 동작하도록 설계."""
    busy = round(DUTY_CYCLE_PERIOD_SEC * target_pct / 100, 3)
    idle = round(DUTY_CYCLE_PERIOD_SEC - busy, 3)
    loop = (
        f'END=$(( $(date +%s) + {duration_sec} )); '
        f'while [ $(date +%s) -lt $END ]; do '
        f'timeout {busy} yes > /dev/null 2>&1; sleep {idle}; '
        f'done'
    )
    return [f'nohup bash -c "{loop}" >/dev/null 2>&1 &' for _ in range(n_vcpu)]


def _launch_instances(label: str, n: int, name_prefix: str) -> list[str]:
    ec2 = boto3.client("ec2")
    resp = ec2.run_instances(
        ImageId=AMI_ID,
        InstanceType=INSTANCE_TYPE,
        MinCount=n,
        MaxCount=n,
        SecurityGroupIds=SECURITY_GROUP_IDS,
        SubnetId=SUBNET_ID,
        IamInstanceProfile={"Name": IAM_INSTANCE_PROFILE},
        TagSpecifications=[{
            "ResourceType": "instance",
            "Tags": [
                {"Key": "Name", "Value": f"{name_prefix}-{label}"},
                {"Key": "detection-test", "Value": "ec2-overprovision"},
                {"Key": "true_label", "Value": label},
            ],
        }],
    )
    ids = [i["InstanceId"] for i in resp["Instances"]]
    print(f"[{label}] {n}대 launch 요청: {ids}")
    return ids


def _wait_ssm_online(instance_ids: list[str], timeout_sec: int = 300) -> None:
    ssm = boto3.client("ssm")
    deadline = time.time() + timeout_sec
    pending = set(instance_ids)
    while pending and time.time() < deadline:
        info = ssm.describe_instance_information(
            Filters=[{"Key": "InstanceIds", "Values": list(pending)}]
        )
        online = {i["InstanceId"] for i in info["InstanceInformationList"] if i["PingStatus"] == "Online"}
        pending -= online
        if online:
            print(f"  SSM 온라인 확인: {sorted(online)}")
        if pending:
            time.sleep(10)
    if pending:
        print(f"⚠️ SSM 미등록 상태로 타임아웃됨(재부팅 필요할 수 있음): {sorted(pending)}")


def setup() -> None:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    name_prefix = f"detection-test-ec2-overprovision-{ts}"

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

    print("SSM 등록 대기(최대 5분, 부팅+에이전트 기동 시간 필요)...")
    time.sleep(60)  # 최소 부팅 시간 확보 후 폴링 시작
    _wait_ssm_online(all_ids)

    ssm = boto3.client("ssm")
    for iid in anomaly_ids:
        cmds = _duty_cycle_command(ANOMALY_TARGET_CPU_PCT, WINDOW_SECONDS, N_VCPU)
        ssm.send_command(InstanceIds=[iid], DocumentName="AWS-RunShellScript", Parameters={"commands": cmds})
        print(f"[anomaly {iid}] 목표 CPU {ANOMALY_TARGET_CPU_PCT}% 부하 시작 ({WINDOW_SECONDS}초)")
    for iid in normal_ids:
        cmds = _duty_cycle_command(NORMAL_TARGET_CPU_PCT, WINDOW_SECONDS, N_VCPU)
        ssm.send_command(InstanceIds=[iid], DocumentName="AWS-RunShellScript", Parameters={"commands": cmds})
        print(f"[normal {iid}] 목표 CPU {NORMAL_TARGET_CPU_PCT}% 부하 시작 ({WINDOW_SECONDS}초)")

    check_earliest = datetime.now(timezone.utc) + timedelta(seconds=WINDOW_SECONDS)
    manifest = {
        "script_version": SCRIPT_VERSION,
        "scenario": "ec2_overprovision",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "check_earliest_utc": check_earliest.isoformat(),
        "anomaly_target_cpu_pct": ANOMALY_TARGET_CPU_PCT,
        "normal_target_cpu_pct": NORMAL_TARGET_CPU_PCT,
        "instances": [
            {"instance_id": iid, "true_label": "anomaly", "launch_time_utc": launch_times[iid],
             "profile": f"target_cpu_{ANOMALY_TARGET_CPU_PCT}pct"}
            for iid in anomaly_ids
        ] + [
            {"instance_id": iid, "true_label": "normal", "launch_time_utc": launch_times[iid],
             "profile": f"target_cpu_{NORMAL_TARGET_CPU_PCT}pct"}
            for iid in normal_ids
        ],
    }
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    manifest_path = RESULT_DIR / f"ec2_overprovision_manifest_{ts}.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    print(f"\n매니페스트 저장: {manifest_path}")
    print(f"측정 가능 시점(2.5시간 뒤): {check_earliest.isoformat()}")
    print(f"\n측정 명령:\n  python playground/ec2_lambda_repeated_trial.py --scenario ec2 --run --manifest {manifest_path}")
    print(f"\n종료 후 정리(반드시 실행):\n  python playground/ec2_overprovision_setup.py --teardown --instance-ids {','.join(all_ids)}")


def teardown(instance_ids: list[str]) -> None:
    ec2 = boto3.client("ec2")
    print(f"종료 요청: {instance_ids}")
    ec2.terminate_instances(InstanceIds=instance_ids)
    print("terminate 요청 완료 (반영까지 수 분 소요될 수 있음)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--setup", action="store_true", help="인스턴스 생성 + 부하 시작 + 매니페스트 작성")
    parser.add_argument("--teardown", action="store_true", help="인스턴스 종료")
    parser.add_argument("--instance-ids", type=str, help="--teardown용, 콤마로 구분된 인스턴스 ID 목록")
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
