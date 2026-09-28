"""
playground/real_demo/lambda_real.py

Lambda 스로틀(동시성 제한 + 버스트 호출로 재시도/에러 유발) 시나리오
"완전 실연동" 데모. sysy04의 playground/lambda_throttle_clean_verification.py
(가장 최근에 실측 검증까지 끝난 버전 — anomaly는 ReservedConcurrentExecutions로
동시성을 제한해두고 버스트 호출을 걸어 Throttle/재시도를 유발)의 함수 생성 +
워밍업(3시간, 30포인트 창이 콜드스타트로 왜곡되지 않게) + 버스트 호출 로직을
그대로 가져오고, 마지막 "파이프라인 실행"만 real_demo.common.run_real_scenario로
바꿨다(예전 measure()는 detection_node~decision_node만 직접 호출하고
bypass_approval_for_timing=True로 승인을 강제 우회 — 액션/QA/웹 제어판 반영이
전혀 없었음).

⚠️ IAM_ROLE_ARN이 예전 계정(268140507066)에 하드코딩돼 있어서(계정 이전으로
무효) lambda_retry_trial.py의 _get_or_create_lambda_role()로 현재 계정에
역할을 새로 만든다.

[실행 방법]
  1) 함수 준비 + 3시간 베이스라인 워밍업(콜드스타트 왜곡 방지):
     python playground/real_demo/lambda_real.py --setup
  2) 버스트 호출(동시성 제한으로 Throttle 유발) + 실연동 파이프라인 실행:
     python playground/real_demo/lambda_real.py --run --profile severe
     python playground/real_demo/lambda_real.py --run --profile moderate  # 엣지케이스(완만한 제한)
  3) 종료 후 정리(반드시 실행):
     python playground/real_demo/lambda_real.py --teardown
"""

from __future__ import annotations

import argparse
import io
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

PLAYGROUND_ROOT = Path(__file__).resolve().parent.parent
if str(PLAYGROUND_ROOT) not in sys.path:
    sys.path.insert(0, str(PLAYGROUND_ROOT))

import boto3

from lambda_retry_trial import AWS_REGION, _get_or_create_lambda_role

from common import run_real_scenario

FUNCTION_NAME = "detection-test-lambda-throttle-demo"

LAMBDA_HANDLER_CODE = (
    "def handler(event, context):\n"
    '    if isinstance(event, dict) and event.get("force_error"):\n'
    '        raise Exception("lambda_real: intentional error for retry-storm test")\n'
    '    return {"statusCode": 200, "body": "ok"}\n'
)

# severe: sysy04 스크립트의 anomaly-2/3(concurrency=1) 프로파일과 동일
# moderate: [2026-09-27 추가] 엣지케이스 — anomaly-4/5(concurrency=2, 덜 극단적) 프로파일
CONCURRENCY_BY_PROFILE = {"severe": 1, "moderate": 2}
BURST_SIZE = 40
N_BURSTS = 3
BURST_INTERVAL_SEC = 300
MAX_EVENT_AGE_SEC = 60
POST_BURST_WAIT_SEC = 180
INVOKE_POOL_SIZE = 40

WARMUP_DURATION_SEC = 10800  # 3시간
WARMUP_PERIOD_SEC = 300
WARMUP_CALLS_PER_TICK = 2


def _lambda_client():
    return boto3.client("lambda", region_name=AWS_REGION)


def setup() -> None:
    iam = boto3.client("iam", region_name=AWS_REGION)
    role_arn = _get_or_create_lambda_role(iam)
    lam = _lambda_client()

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", LAMBDA_HANDLER_CODE)
    zip_bytes = buf.getvalue()

    try:
        lam.get_function(FunctionName=FUNCTION_NAME)
        print(f"[{FUNCTION_NAME}] 이미 존재 — 스킵")
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(
            FunctionName=FUNCTION_NAME,
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.handler",
            Code={"ZipFile": zip_bytes},
            Timeout=3,
            MemorySize=128,
            Architectures=["x86_64"],
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=FUNCTION_NAME)
        print(f"[{FUNCTION_NAME}] 생성 완료")

    print(f"3시간 베이스라인 워밍업 시작 (30포인트 창이 콜드스타트로 왜곡되지 않게)...")
    n_ticks = WARMUP_DURATION_SEC // WARMUP_PERIOD_SEC
    with ThreadPoolExecutor(max_workers=INVOKE_POOL_SIZE) as pool:
        for i in range(n_ticks):
            futures = [
                pool.submit(
                    lam.invoke,
                    FunctionName=FUNCTION_NAME,
                    InvocationType="Event",
                    Payload=b"{}",
                )
                for _ in range(WARMUP_CALLS_PER_TICK)
            ]
            for f in futures:
                f.result()
            print(f"  워밍업 tick {i + 1}/{n_ticks} 완료")
            if i < n_ticks - 1:
                time.sleep(WARMUP_PERIOD_SEC)
    print("워밍업 완료")


def teardown() -> None:
    lam = _lambda_client()
    try:
        lam.delete_function(FunctionName=FUNCTION_NAME)
        print(f"Lambda 함수 삭제 완료: {FUNCTION_NAME}")
    except Exception as exc:
        print(f"Lambda 함수 삭제 실패(이미 없을 수 있음): {exc}")


def run(profile: str) -> None:
    lam = _lambda_client()
    concurrency = CONCURRENCY_BY_PROFILE[profile]

    lam.put_function_event_invoke_config(
        FunctionName=FUNCTION_NAME,
        MaximumEventAgeInSeconds=MAX_EVENT_AGE_SEC,
        MaximumRetryAttempts=2,
    )
    try:
        lam.put_function_concurrency(
            FunctionName=FUNCTION_NAME, ReservedConcurrentExecutions=concurrency
        )
        print(
            f"[{FUNCTION_NAME}] profile={profile} concurrency={concurrency}로 제한, 버스트 시작"
        )
    except lam.exceptions.InvalidParameterValueException as exc:
        # [2026-09-28] 이 계정의 Lambda 계정 전체 동시실행 한도가 10개뿐이라(AWS가
        # UnreservedConcurrentExecutions를 항상 최소 10개 남기게 강제) 1~2개조차 예약이
        # 안 된다 — 대신 버스트(40개)가 계정 한도(10개) 자체를 넘겨서 예약 없이도
        # 자연스럽게 스로틀이 걸린다(오히려 이 계정 환경에 더 맞는 방식).
        print(
            f"[{FUNCTION_NAME}] 동시성 예약 실패({exc}) — 예약 없이 버스트만으로 진행 "
            f"(계정 전체 한도 10개 < 버스트 {BURST_SIZE}개라 자연 스로틀 예상)"
        )
    time.sleep(3)

    with ThreadPoolExecutor(max_workers=INVOKE_POOL_SIZE) as invoke_pool:
        for b in range(N_BURSTS):
            futures = [
                invoke_pool.submit(
                    lam.invoke,
                    FunctionName=FUNCTION_NAME,
                    InvocationType="Event",
                    Payload=b"{}",
                )
                for _ in range(BURST_SIZE)
            ]
            ok = sum(1 for f in futures if _safe_result(f))
            print(f"  버스트 {b + 1}/{N_BURSTS} 완료: 성공 {ok}/{BURST_SIZE}")
            if b < N_BURSTS - 1:
                time.sleep(BURST_INTERVAL_SEC)

    print(f"버스트 완료, 지표 반영 대기 {POST_BURST_WAIT_SEC}초...")
    time.sleep(POST_BURST_WAIT_SEC)

    try:
        lam.delete_function_concurrency(FunctionName=FUNCTION_NAME)
        lam.delete_function_event_invoke_config(FunctionName=FUNCTION_NAME)
    except Exception:
        pass

    run_real_scenario(
        scenario_key="lambda",
        resource_id=FUNCTION_NAME,
        resource_type="Lambda",
    )


def _safe_result(future) -> bool:
    try:
        future.result()
        return True
    except Exception:
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--setup", action="store_true")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--teardown", action="store_true")
    parser.add_argument("--profile", choices=["severe", "moderate"], default="severe")
    args = parser.parse_args()

    if args.setup:
        setup()
    elif args.run:
        run(args.profile)
    elif args.teardown:
        teardown()
    else:
        parser.error("--setup / --run / --teardown 중 하나를 지정하세요")


if __name__ == "__main__":
    main()
