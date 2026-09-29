"""
playground/live_demo/s3_live.py

S3 요청 폭증(스크래핑/과도한 다운로드) 시나리오 실시간 데모.
탐지 입력(raw_metrics)은 가장 최근 성공했던 실측 결과 파일에서 재생하고,
분류~로깅은 실제 파이프라인 그대로 실행한다.

소스: playground/team_results/s3/repeated_trial.json
      (= s3_repeated_trial__window2.5h_objsize50kb_n8-5_scriptv5_20260910.json,
       anomaly_trials[0], detected=True)

⚠️ 2026-09-27 발견: 이 raw_metrics는 계정 이전 전(예전 AWS 계정)에 실측된 데이터라,
그 안의 resource_id(버킷 이름)가 지금 계정엔 존재하지 않는다 — 탐지/분류/결정은
순수 계산이라 문제없이 통과했지만, action_node가 실제로 그 버킷에 boto3 호출을
하면서 NoSuchBucket으로 죽었다(웹 제어판에서 승인 후 액션이 안 되는 것처럼 보인
원인). 그래서 run_live_scenario 호출 전에 같은 이름의 버킷을 지금 계정에 실제로
만들어둔다 — 트래픽은 필요 없고 action(Block)이 실제로 걸릴 대상만 있으면 된다.
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

from s3_repeated_trial import AWS_REGION, _ensure_bucket_with_metrics

from common import run_live_scenario

SOURCE_FILE = (
    Path(__file__).parent.parent / "team_results" / "s3" / "repeated_trial.json"
)


def _resource_id() -> str:
    data = json.load(open(SOURCE_FILE, encoding="utf-8"))
    return data["anomaly_trials"][0]["after"]["resource_id"]


def teardown() -> None:
    resource_id = _resource_id()
    s3 = boto3.client("s3", region_name=AWS_REGION)
    try:
        objs = s3.list_objects_v2(Bucket=resource_id).get("Contents", [])
        if objs:
            s3.delete_objects(
                Bucket=resource_id,
                Delete={"Objects": [{"Key": o["Key"]} for o in objs]},
            )
        s3.delete_bucket(Bucket=resource_id)
        print(f"[s3_live] 버킷 삭제 완료: {resource_id}")
    except Exception as exc:
        print(f"[s3_live] 버킷 삭제 실패(이미 없을 수 있음): {exc}")


def run() -> None:
    data = json.load(open(SOURCE_FILE, encoding="utf-8"))
    trial = data["anomaly_trials"][0]
    after = trial["after"]
    resource_id = after["resource_id"]

    s3 = boto3.client("s3", region_name=AWS_REGION)
    _ensure_bucket_with_metrics(s3, resource_id)
    print(f"[s3_live] 버킷 존재 확인/생성 완료: {resource_id}")

    run_live_scenario(
        scenario_key="s3",
        resource_id=resource_id,
        resource_type="S3",
        raw_metrics=after["raw_metrics"],
        resource_age_seconds=None,
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
