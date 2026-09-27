"""
playground/live_demo/edos_live.py

AutoScaling EDoS(요청 폭증으로 인한 자동확장 남용) 시나리오 실시간 데모.
탐지 입력(raw_metrics)은 가장 최근 성공했던 실측 결과 파일에서 재생하고,
분류~로깅은 실제 파이프라인 그대로 실행한다.

소스: playground/eval_outputs/autoscaling_edos_traffic_trial__n5-8_scriptv4_20260915__soyoung.json
      (2026-09-15 실측, trials[0], profile=extreme_spike, label=anomaly, after 구간 사용)

⚠️ 2026-09-27 발견(s3_live.py/lambda_live.py와 동일한 문제): 계정 이전 전 실측
데이터라 resource_id(ASG 이름)가 지금 계정엔 없다. 게다가 EDoS의 액션(ScaleDown
+WAF)은 ASG에 실제로 연결된 ALB/Target Group까지 있어야 하므로, action(Block)만
필요했던 S3/Lambda보다 준비가 더 필요하다 — autoscaling_edos_traffic_trial.py의
setup_all()로 ALB+ASG(anomaly 1개)를 실제로 구성해둔다(이미 있으면 스킵).
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

from autoscaling_edos_traffic_trial import AWS_REGION, setup_all

from common import run_live_scenario

SOURCE_FILE = (
    Path(__file__).parent.parent
    / "eval_outputs"
    / "autoscaling_edos_traffic_trial__n5-8_scriptv4_20260915__soyoung.json"
)


def _ensure_asg_exists(asg_name: str) -> None:
    asg = boto3.client("autoscaling", region_name=AWS_REGION)
    existing = asg.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])[
        "AutoScalingGroups"
    ]
    if existing:
        print(f"[edos_live] ASG 이미 존재: {asg_name}")
        return
    print(f"[edos_live] ASG 없음 — ALB+ASG 신규 프로비저닝 (2~3분 소요)")
    setup_all(n_anomaly=1, n_normal=0)


def main() -> None:
    data = json.load(open(SOURCE_FILE, encoding="utf-8"))
    trial = next(t for t in data["trials"] if t["label"] == "anomaly")
    after = trial["after"]
    resource_id = after["resource_id"]

    _ensure_asg_exists(resource_id)

    run_live_scenario(
        scenario_key="edos",
        resource_id=resource_id,
        resource_type="AutoScaling",
        raw_metrics=after["raw_metrics"],
        resource_age_seconds=after.get("resource_age_seconds"),
    )


if __name__ == "__main__":
    main()
