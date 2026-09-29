"""
playground/real_demo/zombie_real.py

EC2 좀비(완전 유휴) 시나리오 "완전 실연동" 데모.
ec2_zombie_edge_setup.py(경계 케이스 포함, 계정 이전 후 User Data로 재작성된
가장 최근 버전)의 인스턴스 프로비저닝 로직을 그대로 가져오고, 마지막 "파이프라인
실행"만 real_demo.common.run_real_scenario로 바꿨다.

⚠️ 알려진 블로커(2026-09 확인, 미해결): 이 계정은 EC2 인스턴스가 20초~8분 내
원인불명으로 자동종료되는 문제가 있다 — 친구(계정 소유자)는 본인이 뭘 설정한 적
없다고 함, 원인 확인 중(CloudTrail/Budget Actions/EventBridge/Config 등 루트
계정으로 점검 예정). 이 스크립트는 코드는 완성해뒀지만 그 문제가 해결되기 전엔
--setup 이후 인스턴스가 2.5시간을 못 버티고 사라질 가능성이 높다.

[실행 방법]
  1) 인스턴스 준비(즉시 반환, User Data로 부팅 시 부하 자동 시작):
     python playground/real_demo/zombie_real.py --setup --profile true   # 완전 유휴(0%)
     python playground/real_demo/zombie_real.py --setup --profile edge  # 경계(4%, 5% 임계값 바로 아래)
  2) 2.5시간 뒤(나이가드 통과 + 부하 누적) 실연동 파이프라인 실행:
     python playground/real_demo/zombie_real.py --run
  3) 종료 후 정리(반드시 실행):
     python playground/real_demo/zombie_real.py --teardown
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

PLAYGROUND_ROOT = Path(__file__).resolve().parent.parent
if str(PLAYGROUND_ROOT) not in sys.path:
    sys.path.insert(0, str(PLAYGROUND_ROOT))

import boto3

from ec2_overprovision_setup import (
    WINDOW_SECONDS,
    N_VCPU,
    _launch_instances,
    _user_data_script,
    teardown as _teardown_instances,
)

from common import run_real_scenario

MANIFEST_PATH = Path(__file__).parent / ".zombie_real_manifest.json"

# true: ec2_zombie_edge_setup.py의 true_zombie(목표 0%, 완전 유휴)
# edge: ec2_zombie_edge_setup.py의 edge_zombie(목표 4%, 임계값 5% 바로 아래 — 여전히 좀비 판정 되어야 함)
TARGET_CPU_PCT = {"true": 0.0, "edge": 4.0}


def setup(profile: str) -> None:
    target = TARGET_CPU_PCT[profile]
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    name_prefix = f"real-demo-zombie-{profile}-{ts}"
    user_data = (
        _user_data_script(target, WINDOW_SECONDS, N_VCPU) if target > 0.0 else None
    )

    instance_ids = _launch_instances(profile, 1, name_prefix, user_data=user_data)
    instance_id = instance_ids[0]

    ec2 = boto3.client("ec2")
    ec2.get_waiter("instance_running").wait(InstanceIds=[instance_id])

    check_earliest = datetime.now(timezone.utc) + timedelta(seconds=WINDOW_SECONDS)
    MANIFEST_PATH.write_text(
        json.dumps(
            {
                "instance_id": instance_id,
                "profile": profile,
                "target_cpu_pct": target,
                "check_earliest_utc": check_earliest.isoformat(),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(
        f"[zombie_real] 인스턴스 준비 완료: {instance_id} (profile={profile}, 목표 CPU {target}%)"
    )
    print(f"측정 가능 시점(2.5시간 뒤): {check_earliest.isoformat()}")


def teardown() -> None:
    if not MANIFEST_PATH.exists():
        print("매니페스트 없음 — 정리할 인스턴스 없음")
        return
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    _teardown_instances([manifest["instance_id"]])
    MANIFEST_PATH.unlink()
    print(f"인스턴스 종료 완료: {manifest['instance_id']}")


def run() -> None:
    if not MANIFEST_PATH.exists():
        raise SystemExit("--setup을 먼저 실행하세요")
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    check_earliest = datetime.fromisoformat(manifest["check_earliest_utc"])
    wait_sec = (check_earliest - datetime.now(timezone.utc)).total_seconds()
    if wait_sec > 0:
        print(f"[zombie_real] 나이가드/부하누적 대기 {wait_sec:.0f}초...")
        time.sleep(wait_sec)

    run_real_scenario(
        scenario_key="zombie",
        resource_id=manifest["instance_id"],
        resource_type="EC2",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--setup", action="store_true")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--teardown", action="store_true")
    parser.add_argument("--profile", choices=["true", "edge"], default="true")
    args = parser.parse_args()

    if args.setup:
        setup(args.profile)
    elif args.run:
        run()
    elif args.teardown:
        teardown()
    else:
        parser.error("--setup / --run / --teardown 중 하나를 지정하세요")


if __name__ == "__main__":
    main()
