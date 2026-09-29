"""
playground/live_demo/ec2_over_live.py

EC2 오버프로비저닝(저활용 지속) 시나리오 실시간 데모.
탐지 입력(raw_metrics)은 가장 최근 성공했던 실측 결과 파일에서 재생하고,
분류~로깅은 실제 파이프라인 그대로 실행한다.

소스: playground/real_demo/logs/ec2_over/ec2_over_20260928_170104.json
      (오늘 real_demo ec2_over_real.py --run 실측 결과 — anomaly_flag=True,
       ec2_utilization_band=overprovisioned, action=Resize, risk=MED,
       qa_passed=True까지 전부 성공한 실행. 예전엔 ec2_overprovision_repeated_trial
       ...json(계정 이전 전 캡처)을 썼는데, duty-cycle 부하 스크립트의 셸
       이스케이프 버그(ec2_overprovision_setup.py, 2026-09-28 수정) 때문에 그
       데이터의 CPU가 항상 0%대로 찍혀 있었다 — 이제 버그를 고치고 재측정해
       목표치인 12%대가 정확히 나온 새 데이터로 교체함.

⚠️ 2026-09-27 발견(zombie_live.py와 동일한 문제·동일한 대응): 이 raw_metrics는
다른(예전) 계정/실행에서 실측된 데이터라 그 안의 resource_id(인스턴스 ID)가
지금 계정엔 없다 — Resize 액션은 실제 describe_instances로 현재 인스턴스
타입을 조회해 다운사이즈 대상을 정하므로(decision_agent._ec2_resize_saving),
진짜 인스턴스가 있어야 한다. 그래서 지금 계정에 실제 EC2 인스턴스를 하나
새로 띄우고, **그 실제 instance_id로 resource_id를 바꿔치기**한다.
raw_metrics/resource_age_seconds는 재생 데이터를 그대로 써서 나이가드/탐지
판정을 유지한다.

[2026-09-28 확인] zombie_live.py와 동일 — EC2 자동종료 블로커는 IAM 인스턴스
프로파일 미부착으로 해소 확인됨.
"""

from __future__ import annotations

import argparse
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
    / "real_demo"
    / "logs"
    / "ec2_over"
    / "ec2_over_20260928_170104.json"
)

# [2026-09-28 ADDED] zombie_live.py와 동일한 이유 — 매번 새 인스턴스라 --teardown이
# 뭘 지울지 알려면 방금 만든 instance_id를 파일로 남겨둬야 한다.
MANIFEST_PATH = Path(__file__).parent / ".ec2_over_live_manifest.json"


def _launch_real_overprovisioned_instance() -> str:
    from ec2_overprovision_setup import _launch_instances

    ids = _launch_instances("anomaly", 1, "live-demo-ec2-over")
    instance_id = ids[0]
    ec2 = boto3.client("ec2", region_name="ap-northeast-2")
    ec2.get_waiter("instance_running").wait(InstanceIds=[instance_id])
    print(f"[ec2_over_live] 실제 인스턴스 준비 완료: {instance_id}")
    MANIFEST_PATH.write_text(
        json.dumps({"instance_id": instance_id}, ensure_ascii=False), encoding="utf-8"
    )
    return instance_id


def teardown() -> None:
    if not MANIFEST_PATH.exists():
        print("[ec2_over_live] 매니페스트 없음 — 정리할 인스턴스 없음")
        return
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    instance_id = manifest["instance_id"]
    ec2 = boto3.client("ec2", region_name="ap-northeast-2")
    ec2.terminate_instances(InstanceIds=[instance_id])
    MANIFEST_PATH.unlink()
    print(f"[ec2_over_live] 인스턴스 종료 요청 완료: {instance_id}")


def run() -> None:
    data = json.load(open(SOURCE_FILE, encoding="utf-8"))

    real_resource_id = _launch_real_overprovisioned_instance()

    run_live_scenario(
        scenario_key="ec2_over",
        resource_id=real_resource_id,
        resource_type="EC2",
        raw_metrics=data["raw_metrics"],
        resource_age_seconds=data.get("resource_age_seconds"),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--teardown", action="store_true")
    args = parser.parse_args()

    if args.teardown:
        teardown()
    else:
        run()


if __name__ == "__main__":
    main()
