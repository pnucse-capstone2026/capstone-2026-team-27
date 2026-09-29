"""
playground/real_demo/ec2_over_real.py

EC2 오버프로비저닝(저활용 지속) 시나리오 "완전 실연동" 데모.
ec2_overprovision_setup.py(계정 이전 후 User Data로 재작성된 가장 최근 버전)의
인스턴스 프로비저닝 로직을 그대로 가져오고, 마지막 "파이프라인 실행"만
real_demo.common.run_real_scenario로 바꿨다.

⚠️ zombie_real.py와 동일한 EC2 자동종료 블로커 있음(미해결) — 코드는 완성해뒀지만
실제 실행 검증은 그 문제가 풀린 뒤로 미룬다.

⚠️ "edge" 프로파일은 기존 팀 스크립트에 없어서 이번에 새로 추가함(2026-09-27) —
오버프로비저닝 밴드(5%초과~20%이하)의 상단 경계(19%, 20% 임계값 바로 아래)에서도
탐지되는지 확인하기 위함. anomaly(12%)는 ec2_overprovision_setup.py의 기존 값.

[실행 방법]
  1) 인스턴스 준비(즉시 반환, User Data로 부팅 시 부하 자동 시작):
     python playground/real_demo/ec2_over_real.py --setup --profile anomaly  # 목표 12%
     python playground/real_demo/ec2_over_real.py --setup --profile edge    # 목표 19%(경계)
  2) 2.5시간 뒤(나이가드 통과 + 부하 누적) 실연동 파이프라인 실행:
     python playground/real_demo/ec2_over_real.py --run
  3) 종료 후 정리(반드시 실행):
     python playground/real_demo/ec2_over_real.py --teardown
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
    ANOMALY_TARGET_CPU_PCT,
    WINDOW_SECONDS,
    N_VCPU,
    _launch_instances,
    _user_data_script,
    teardown as _teardown_instances,
)

from common import run_real_scenario

MANIFEST_PATH = Path(__file__).parent / ".ec2_over_real_manifest.json"

TARGET_CPU_PCT = {"anomaly": ANOMALY_TARGET_CPU_PCT, "edge": 19.0}


def setup(profile: str) -> None:
    target = TARGET_CPU_PCT[profile]
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    name_prefix = f"real-demo-ec2-over-{profile}-{ts}"
    user_data = _user_data_script(target, WINDOW_SECONDS, N_VCPU)

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
        f"[ec2_over_real] 인스턴스 준비 완료: {instance_id} (profile={profile}, 목표 CPU {target}%)"
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
        print(f"[ec2_over_real] 나이가드/부하누적 대기 {wait_sec:.0f}초...")
        time.sleep(wait_sec)

    run_real_scenario(
        scenario_key="ec2_over",
        resource_id=manifest["instance_id"],
        resource_type="EC2",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--setup", action="store_true")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--teardown", action="store_true")
    parser.add_argument("--profile", choices=["anomaly", "edge"], default="anomaly")
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
