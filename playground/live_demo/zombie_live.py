"""
playground/live_demo/zombie_live.py

EC2 좀비(완전 유휴) 시나리오 실시간 데모.
탐지 입력(raw_metrics)은 가장 최근 성공했던 실측 결과 파일에서 재생하고,
분류~로깅은 실제 파이프라인 그대로 실행한다.

소스: playground/eval_outputs/ec2_zombie_repeated_trial__n8-5_scriptv1_20260914__soyoung.json
      (2026-09-14 실측, trials[0], profile=idle_zombie, detected_production=True)

⚠️ 2026-09-27 발견(s3_live.py 등과 동일한 문제): 계정 이전 전 실측 데이터라
resource_id(인스턴스 ID)가 지금 계정엔 없다 — action_node의 stop_instances가
InvalidInstanceID.NotFound로 죽는다. 그래서 지금 계정에 실제 EC2 인스턴스를 하나
새로 띄우고, **그 실제 instance_id로 resource_id를 바꿔치기**해서 action이 걸릴
진짜 대상이 있게 한다. raw_metrics/resource_age_seconds는 그대로 재생 데이터를
쓴다 — 새 인스턴스의 진짜 나이(방금 떠서 2.5시간 나이가드 미달)를 쓰면 EC2 유휴
판정 자체가 보류되므로, 재생된(가드 통과하는) 나이값을 유지해야 한다.

⚠️ 알려진 블로커: 이 계정은 EC2 인스턴스가 20초~8분 내 원인불명으로 자동종료되는
문제가 있어(2026-09 확인, 미해결) 이 스크립트가 지금 당장 끝까지 성공한다는
보장이 없다 — 코드는 완성해두되 실제 실행 검증은 그 문제가 풀린 뒤로 미룬다.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

PLAYGROUND_ROOT = Path(__file__).resolve().parent.parent
if str(PLAYGROUND_ROOT) not in sys.path:
    sys.path.insert(0, str(PLAYGROUND_ROOT))

import boto3

from common import run_live_scenario

SOURCE_FILE = (
    Path(__file__).parent.parent
    / "eval_outputs"
    / "ec2_zombie_repeated_trial__n8-5_scriptv1_20260914__soyoung.json"
)


def _launch_real_zombie_instance() -> str:
    from ec2_overprovision_setup import _launch_instances

    ids = _launch_instances("anomaly", 1, "live-demo-zombie")
    instance_id = ids[0]
    ec2 = boto3.client("ec2", region_name="ap-northeast-2")
    ec2.get_waiter("instance_running").wait(InstanceIds=[instance_id])
    print(f"[zombie_live] 실제 인스턴스 준비 완료(방치 상태): {instance_id}")
    return instance_id


def main() -> None:
    data = json.load(open(SOURCE_FILE, encoding="utf-8"))
    trial = next(t for t in data["trials"] if t["label"] == "anomaly")
    after = trial["after"]

    real_resource_id = _launch_real_zombie_instance()

    run_live_scenario(
        scenario_key="zombie",
        resource_id=real_resource_id,
        resource_type="EC2",
        raw_metrics=after["raw_metrics"],
        resource_age_seconds=after.get("resource_age_seconds"),
    )


if __name__ == "__main__":
    main()
