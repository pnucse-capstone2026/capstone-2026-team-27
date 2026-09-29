"""
playground/live_demo/zombie_live.py

EC2 좀비(완전 유휴) 시나리오 실시간 데모.
탐지 입력(raw_metrics)은 가장 최근 성공했던 실측 결과 파일에서 재생하고,
분류~로깅은 실제 파이프라인 그대로 실행한다.

소스: playground/real_demo/logs/zombie/zombie_20260928_134904.json
      (오늘 real_demo zombie_real.py --run 실측 결과 — anomaly_flag=True,
       action=Stop, risk=LOW, qa_passed=True까지 전부 성공한 실행. 예전엔
       2026-09-14 캡처(ec2_zombie_repeated_trial...json)를 썼는데, 그 데이터는
       오늘 모델 재학습 이후 네트워크 I/O가 임계값을 살짝 넘어 좀비 판정이 안 됨
       — raw_metrics 자체가 유효한 최신 성공 사례로 교체함.)

⚠️ 2026-09-27 발견(s3_live.py 등과 동일한 문제): 이 raw_metrics는 다른(예전) 계정/
실행에서 실측된 데이터라 그 안의 resource_id(인스턴스 ID)가 지금 이 프로세스가
보는 계정엔 없다 — action_node의 stop_instances가 InvalidInstanceID.NotFound로
죽는다. 그래서 지금 계정에 실제 EC2 인스턴스를 하나 새로 띄우고, **그 실제
instance_id로 resource_id를 바꿔치기**해서 action이 걸릴 진짜 대상이 있게 한다.
raw_metrics/resource_age_seconds는 그대로 재생 데이터를 쓴다 — 새 인스턴스의
진짜 나이(방금 떠서 2.5시간 나이가드 미달)를 쓰면 EC2 유휴 판정 자체가
보류되므로, 재생된(가드 통과하는) 나이값을 유지해야 한다.

[2026-09-28 확인] 예전엔 "EC2 인스턴스가 20초~8분 내 원인불명으로 자동종료"되는
블로커가 있어 실행 검증을 보류했었는데, IAM 인스턴스 프로파일을 붙이지 않고
띄우면(_launch_instances가 이미 그렇게 함) 종료되지 않는다는 게 실측으로
확인됐다(이 계정의 보안 자동화가 IAM 역할 붙은 EC2만 타겟하는 것으로 추정).
실제로 이 스크립트를 돌려 인스턴스가 끝까지 살아있음을 확인함 — 블로커 해소.
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
    / "zombie"
    / "zombie_20260928_134904.json"
)

# [2026-09-28 ADDED] 매번 새 인스턴스를 띄우기 때문에(고정된 리소스 이름이 아님)
# --teardown이 뭘 지울지 알려면 방금 만든 instance_id를 파일로 남겨둬야 한다.
MANIFEST_PATH = Path(__file__).parent / ".zombie_live_manifest.json"


def _launch_real_zombie_instance() -> str:
    from ec2_overprovision_setup import _launch_instances

    ids = _launch_instances("anomaly", 1, "live-demo-zombie")
    instance_id = ids[0]
    ec2 = boto3.client("ec2", region_name="ap-northeast-2")
    ec2.get_waiter("instance_running").wait(InstanceIds=[instance_id])
    print(f"[zombie_live] 실제 인스턴스 준비 완료(방치 상태): {instance_id}")
    MANIFEST_PATH.write_text(
        json.dumps({"instance_id": instance_id}, ensure_ascii=False), encoding="utf-8"
    )
    return instance_id


def teardown() -> None:
    if not MANIFEST_PATH.exists():
        print("[zombie_live] 매니페스트 없음 — 정리할 인스턴스 없음")
        return
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    instance_id = manifest["instance_id"]
    ec2 = boto3.client("ec2", region_name="ap-northeast-2")
    ec2.terminate_instances(InstanceIds=[instance_id])
    MANIFEST_PATH.unlink()
    print(f"[zombie_live] 인스턴스 종료 요청 완료: {instance_id}")


def run() -> None:
    data = json.load(open(SOURCE_FILE, encoding="utf-8"))

    real_resource_id = _launch_real_zombie_instance()

    run_live_scenario(
        scenario_key="zombie",
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
