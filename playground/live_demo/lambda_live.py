"""
playground/live_demo/lambda_live.py

Lambda 스로틀(동시성 급증) 시나리오 실시간 데모.
탐지 입력(raw_metrics)은 가장 최근 성공했던 실측 결과 파일에서 재생하고,
분류~로깅은 실제 파이프라인 그대로 실행한다.

소스: playground/real_demo/logs/lambda/lambda_20260928_151235_fail.json
      (오늘 real_demo lambda_real.py --run 실측 결과, API 서버 재시작 + 모델 재학습
       이후 재시도 — 탐지(cost_spike)→결정(Throttle, MED)→액션까지는 전부 정상.
       액션 단계에서 계정 동시성 한도(10) 때문에 요청한 5는 여전히 거부되지만,
       inbound_handlers.py의 fallback으로 0(완전 차단)까지 내려가서 API 호출
       자체는 성공(action_result.status=success, degraded_to_full_block=True).
       다만 QA가 "완전 차단은 가용성 SLA 위반"으로 정확히 판단해 롤백시켜서
       qa_passed=False로 끝남 — 이건 버그가 아니라 안전장치가 의도대로 작동한
       것이다. 이 계정에서는 Lambda 동시성 증설(현재 CASE_OPENED, 대기 중)이
       승인되기 전까지는 Throttle이 구조적으로 "성공"까지 못 가고 항상 이
       fallback→롤백 경로를 탄다. 증설되면 5가 그대로 통과해 QA도 통과할 것으로
       예상되며, 그때 이 SOURCE_FILE을 다시 갈아끼워야 한다.
       예전엔 team_results/lambda/repeated_trial.json(재시도 폭증 메커니즘)을 대신
       썼었는데, "스로틀" 메커니즘 raw_metrics가 그동안 저장된 적이 없어서였다.

⚠️ 2026-09-27 발견(s3_live.py와 동일한 문제): 이 raw_metrics는 다른(예전) 계정에서
실측된 데이터라 그 안의 resource_id(함수 이름)가 지금 계정엔 없다 — action_node가
실제로 put_function_concurrency를 호출하면서 ResourceNotFoundException으로
죽는다. run_live_scenario 호출 전에 같은 이름의 Lambda 함수를 지금 계정에
실제로 만들어둔다 — 트래픽은 필요 없고 action(Throttle)이 걸릴 대상만 있으면 된다.
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

from lambda_retry_trial import (
    AWS_REGION,
    _create_lambda_zip,
    _ensure_lambda_function,
    _get_or_create_lambda_role,
)

from common import run_live_scenario

SOURCE_FILE = (
    Path(__file__).parent.parent
    / "real_demo"
    / "logs"
    / "lambda"
    / "lambda_20260928_151235_fail.json"
)


def teardown() -> None:
    data = json.load(open(SOURCE_FILE, encoding="utf-8"))
    resource_id = data["resource_id"]
    lam = boto3.client("lambda", region_name=AWS_REGION)
    try:
        lam.delete_function(FunctionName=resource_id)
        print(f"[lambda_live] 함수 삭제 완료: {resource_id}")
    except Exception as exc:
        print(f"[lambda_live] 함수 삭제 실패(이미 없을 수 있음): {exc}")


def run() -> None:
    data = json.load(open(SOURCE_FILE, encoding="utf-8"))
    resource_id = data["resource_id"]

    iam = boto3.client("iam", region_name=AWS_REGION)
    lam = boto3.client("lambda", region_name=AWS_REGION)
    role_arn = _get_or_create_lambda_role(iam)
    _ensure_lambda_function(lam, resource_id, role_arn, _create_lambda_zip())
    print(f"[lambda_live] 함수 존재 확인/생성 완료: {resource_id}")

    run_live_scenario(
        scenario_key="lambda",
        resource_id=resource_id,
        resource_type="Lambda",
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
