"""
playground/live_demo/edos_live.py

AutoScaling EDoS(요청 폭증으로 인한 자동확장 남용) 시나리오 실시간 데모.
탐지 입력(raw_metrics)은 가장 최근 성공했던 실측 결과 파일에서 재생하고,
분류~로깅은 실제 파이프라인 그대로 실행한다.

소스: playground/real_demo/logs/edos/edos_20260928_185815.json
      (오늘 real_demo edos_real.py --run 실측 결과 — anomaly_flag=True(IForest
       점수 1.0), action=ScaleDown, risk=HIGH, qa_passed=True까지 전부 성공한
       실행. 예전엔 2026-09-15 캡처 데이터를 썼는데, AutoScaling 학습 시드
       (playground/mock_data/autoscaling_train.json)에 request_count 메트릭
       자체가 없어서 IForest가 이 지표를 아예 학습 못 해(윈도우 전체가 동일
       점수로 나오는 퇴화 케이스) 탐지가 안 됐었다 — 시드에 request_count
       정상 baseline을 채워넣고(2026-09-28) 재학습한 뒤 재측정한 데이터로
       교체함. WAF Rate-based Rule 연결도 이번에 같이 수정된 재시도 로직
       (inbound_handlers.py, ALB 생성 직후 propagation 지연 대응)으로 정상
       작동 확인됨.

⚠️ 2026-09-27 발견(s3_live.py/lambda_live.py와 동일한 문제): 이 raw_metrics는
다른(예전) 계정/실행에서 실측된 데이터라 그 안의 resource_id(ASG 이름)가
지금 계정엔 없다. 게다가 EDoS의 액션(ScaleDown+WAF)은 ASG에 실제로 연결된
ALB/Target Group까지 있어야 하므로, action(Block)만 필요했던 S3/Lambda보다
준비가 더 필요하다 — autoscaling_edos_traffic_trial.py의 setup_all()로
ALB+ASG(anomaly 1개)를 실제로 구성해둔다(이미 있으면 스킵).
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

from autoscaling_edos_traffic_trial import AWS_REGION, setup_all, teardown_all

from common import run_live_scenario

SOURCE_FILE = (
    Path(__file__).parent.parent
    / "real_demo"
    / "logs"
    / "edos"
    / "edos_20260928_185815.json"
)


# [2026-09-29 ADDED] 공용 실험 스크립트(autoscaling_edos_traffic_trial.py)는
# CAPACITY=1로 MaxSize까지 고정해뒀다 — "트래픽 지표만 단독 검증"하려는 의도된
# 설계라 실측 검증(real_demo)에는 그대로 둬야 한다. 하지만 그러면 decision_agent의
# EDoS 회피 비용 추정(_edos_avoided_scaling_cost, MaxSize-DesiredCapacity 기반)이
# 항상 0으로 나와 시연 효과가 없다 — live_demo 전용으로만 여기서 MaxSize에 여유를
# 둬서 "공격을 막아 확장을 회피했다"는 그림이 실제로 나오게 한다.
LIVE_DEMO_ASG_MAX_SIZE = 4


def _ensure_asg_exists(asg_name: str) -> None:
    asg = boto3.client("autoscaling", region_name=AWS_REGION)
    existing = asg.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])[
        "AutoScalingGroups"
    ]
    if not existing:
        print(f"[edos_live] ASG 없음 — ALB+ASG 신규 프로비저닝 (2~3분 소요)")
        setup_all(n_anomaly=1, n_normal=0)
    else:
        print(f"[edos_live] ASG 이미 존재: {asg_name}")

    current_max = existing[0]["MaxSize"] if existing else None
    if current_max is None or current_max < LIVE_DEMO_ASG_MAX_SIZE:
        asg.update_auto_scaling_group(
            AutoScalingGroupName=asg_name, MaxSize=LIVE_DEMO_ASG_MAX_SIZE
        )
        print(
            f"[edos_live] MaxSize를 {LIVE_DEMO_ASG_MAX_SIZE}로 확장 "
            "(EDoS 회피 비용 추정을 위해 live_demo 전용으로만 적용, DesiredCapacity는 그대로 유지)"
        )


def teardown() -> None:
    """⚠️ 이 ALB+ASG(detection-traffic-asg-anomaly-0)는 real_demo/edos_real.py와
    이름이 완전히 같아서 같은 리소스를 공유한다 — real_demo edos가 아직 돌고
    있는 중이면 이 teardown이 그 실행을 같이 망가뜨린다. real_demo edos가
    확실히 끝나고 그쪽도 teardown할 시점에만 호출할 것."""
    teardown_all(n_anomaly=1, n_normal=0)


def run() -> None:
    data = json.load(open(SOURCE_FILE, encoding="utf-8"))
    resource_id = data["resource_id"]

    _ensure_asg_exists(resource_id)

    run_live_scenario(
        scenario_key="edos",
        resource_id=resource_id,
        resource_type="AutoScaling",
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
