"""
playground/live_demo/ec2_over_live.py

EC2 오버프로비저닝(저활용 지속) 시나리오 실시간 데모.
탐지 입력(raw_metrics)은 가장 최근 성공했던 실측 결과 파일에서 재생하고,
분류~로깅은 실제 파이프라인 그대로 실행한다.

소스: playground/eval_outputs/ec2_overprovision_repeated_trial__converted__soyoung.json
      (anomaly_trials[0], detected=True)

⚠️ 2026-09-27 발견(zombie_live.py와 동일한 문제·동일한 대응): resource_id를 지금
계정에 실제로 띄운 인스턴스로 바꿔치기한다 — Resize 액션은 실제
describe_instances로 현재 인스턴스 타입을 조회해 다운사이즈 대상을 정하므로
(decision_agent._ec2_resize_saving), 진짜 인스턴스가 있어야 한다. raw_metrics/
resource_age_seconds는 재생 데이터를 그대로 써서 나이가드/탐지 판정을 유지한다.

⚠️ 알려진 블로커: zombie_live.py와 동일 — EC2 자동종료 이슈(미해결) 때문에
코드만 완성해두고 실제 실행 검증은 보류한다.
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
    / "ec2_overprovision_repeated_trial__converted__soyoung.json"
)


def _launch_real_overprovisioned_instance() -> str:
    from ec2_overprovision_setup import _launch_instances

    ids = _launch_instances("anomaly", 1, "live-demo-ec2-over")
    instance_id = ids[0]
    ec2 = boto3.client("ec2", region_name="ap-northeast-2")
    ec2.get_waiter("instance_running").wait(InstanceIds=[instance_id])
    print(f"[ec2_over_live] 실제 인스턴스 준비 완료: {instance_id}")
    return instance_id


def main() -> None:
    data = json.load(open(SOURCE_FILE, encoding="utf-8"))
    trial = data["anomaly_trials"][0]
    after = trial["after"]

    real_resource_id = _launch_real_overprovisioned_instance()

    run_live_scenario(
        scenario_key="ec2_over",
        resource_id=real_resource_id,
        resource_type="EC2",
        raw_metrics=after["raw_metrics"],
        resource_age_seconds=after.get("resource_age_seconds"),
    )


if __name__ == "__main__":
    main()
