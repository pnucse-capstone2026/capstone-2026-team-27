"""
playground/live_demo/lambda_live.py

Lambda 재시도 폭증(에러율 급증) 시나리오 실시간 데모.
탐지 입력(raw_metrics)은 가장 최근 성공했던 실측 결과 파일에서 재생하고,
분류~로깅은 실제 파이프라인 그대로 실행한다.

소스: playground/team_results/lambda/repeated_trial.json
      (= lambda_retry_repeated_trial__window2.5h_spike3p_teammate_n8-5_scriptv2_20260910.json,
       anomaly_trials[0], anomaly_flag=True)

주: lambda_throttle 계열 결과 파일들은 raw_metrics 자체가 저장되어 있지 않아
    (탐지 재생용 소스로 쓸 수 없음) team_results/lambda 쪽(재시도 폭증 메커니즘)을 사용한다.

⚠️ 2026-09-27 발견(s3_live.py와 동일한 문제): 이 raw_metrics는 계정 이전 전
실측 데이터라 그 안의 resource_id(함수 이름)가 지금 계정엔 없다 — action_node가
실제로 put_function_concurrency를 호출하면서 ResourceNotFoundException으로
죽는다. run_live_scenario 호출 전에 같은 이름의 Lambda 함수를 지금 계정에
실제로 만들어둔다 — 트래픽은 필요 없고 action(Throttle)이 걸릴 대상만 있으면 된다.
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

from lambda_retry_trial import (
    AWS_REGION,
    _create_lambda_zip,
    _ensure_lambda_function,
    _get_or_create_lambda_role,
)

from common import run_live_scenario

SOURCE_FILE = (
    Path(__file__).parent.parent / "team_results" / "lambda" / "repeated_trial.json"
)


def main() -> None:
    data = json.load(open(SOURCE_FILE, encoding="utf-8"))
    trial = data["anomaly_trials"][0]
    after = trial["after"]
    resource_id = after["resource_id"]

    iam = boto3.client("iam", region_name=AWS_REGION)
    lam = boto3.client("lambda", region_name=AWS_REGION)
    role_arn = _get_or_create_lambda_role(iam)
    _ensure_lambda_function(lam, resource_id, role_arn, _create_lambda_zip())
    print(f"[lambda_live] 함수 존재 확인/생성 완료: {resource_id}")

    run_live_scenario(
        scenario_key="lambda",
        resource_id=resource_id,
        resource_type="Lambda",
        raw_metrics=after["raw_metrics"],
        resource_age_seconds=None,
    )


if __name__ == "__main__":
    main()
