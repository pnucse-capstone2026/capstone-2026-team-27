"""
playground/test_live_lambda_spike.py

Lambda 호출 폭증 실연동 테스트 (통합 자동화)

파일 하나만 실행하면 전체 테스트가 자동으로 진행됩니다:
  1. Lambda 함수 존재 여부 확인 (없으면 자동 생성)
  2. 기준선 생성 (100회 호출)
  3. CloudWatch 메트릭 반영 대기
  4. 스파이크 생성 (1000회 × 3회, 5분 간격) - 지속성 체크 통과용
  5. 파이프라인 실행 및 이상 감지 확인
  6. Throttle 적용 확인
  7. 롤백 및 원상복구

사전 조건:
  - .env에 AWS 자격증명 설정
  - PostgreSQL 체크포인터 연결 가능

실행:
  python playground/test_live_lambda_spike.py              # 전체 테스트 (약 20분)
  python playground/test_live_lambda_spike.py --quick      # 빠른 테스트 (1회 스파이크, 감지 안될 수 있음)
  python playground/test_live_lambda_spike.py --dry-run    # 실제 Throttle 없이 테스트
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

import boto3

from pipeline.action_agent import take_snapshot, rollback_action
from pipeline.orchestrator import assemble_resource
from pipeline.detection_agent import _build_initial_state
from pipeline.graph import build_approval_graph
from pipeline.checkpointer import get_postgres_checkpointer


# ── 설정 ──────────────────────────────────────────────────────────────────────
FUNCTION_NAME = os.getenv("LAMBDA_FUNCTION_NAME", "detection-test-lambda")
BASELINE_INVOKE_COUNT = 100       # 기준선 호출 횟수
SPIKE_INVOKE_COUNT = 3000         # 스파이크 호출 횟수 (Z-score 임계값 2.75 통과 위해 증가)
SPIKE_ROUNDS = 3                  # 스파이크 반복 횟수 (지속성 체크 통과용)
METRIC_WAIT_SECONDS = 300         # CloudWatch 메트릭 반영 대기 (5분)
INVOKE_CONCURRENCY = 10           # 동시 호출 스레드 수


def print_header(title: str) -> None:
    print(f"\n{'='*70}")
    print(f" {title}")
    print('='*70)


def print_step(step: int, total: int, title: str) -> None:
    print(f"\n[{step}/{total}] {title}")
    print("-" * 50)


def countdown(seconds: int, message: str = "대기 중") -> None:
    """카운트다운 표시"""
    print(f"  {message}... ({seconds}초)")
    for remaining in range(seconds, 0, -30):
        time.sleep(min(30, remaining))
        if remaining > 30:
            print(f"    {remaining - 30}초 남음...")
    print("  완료!")


def check_aws_connection() -> dict:
    """AWS 연결 상태 확인"""
    try:
        sts = boto3.client("sts")
        identity = sts.get_caller_identity()
        return {
            "connected": True,
            "account": identity["Account"],
            "user": identity["Arn"].split("/")[-1],
        }
    except Exception as e:
        return {"connected": False, "error": str(e)}


def ensure_lambda_function() -> dict:
    """Lambda 함수 존재 확인, 없으면 생성"""
    lambda_client = boto3.client("lambda")

    # 함수 존재 확인
    try:
        response = lambda_client.get_function(FunctionName=FUNCTION_NAME)
        return {
            "exists": True,
            "created": False,
            "arn": response["Configuration"]["FunctionArn"],
        }
    except lambda_client.exceptions.ResourceNotFoundException:
        pass

    # IAM 역할 확인/생성
    iam = boto3.client("iam")
    role_name = "detection-lambda-role"

    try:
        role = iam.get_role(RoleName=role_name)
        role_arn = role["Role"]["Arn"]
    except iam.exceptions.NoSuchEntityException:
        # 역할 생성
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }]
        }
        role = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
        )
        role_arn = role["Role"]["Arn"]

        # 기본 실행 정책 연결
        iam.attach_role_policy(
            RoleName=role_name,
            PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
        )
        print("  IAM 역할 생성 완료, 전파 대기 (10초)...")
        time.sleep(10)

    # Lambda 함수 코드
    function_code = '''
import json
import time

def lambda_handler(event, context):
    time.sleep(0.1)
    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Hello from detection-test-lambda!", "event": event})
    }
'''

    # ZIP 파일 생성
    import zipfile
    import io

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", function_code)
    zip_buffer.seek(0)

    # Lambda 함수 생성
    response = lambda_client.create_function(
        FunctionName=FUNCTION_NAME,
        Runtime="python3.12",
        Role=role_arn,
        Handler="lambda_function.lambda_handler",
        Code={"ZipFile": zip_buffer.read()},
        Timeout=30,
        MemorySize=128,
        Tags={"Detection": "true"},
    )

    return {
        "exists": True,
        "created": True,
        "arn": response["FunctionArn"],
    }


def invoke_lambda_burst(count: int) -> dict:
    """Lambda 함수 대량 호출"""
    lambda_client = boto3.client("lambda")
    payload = json.dumps({"test": "spike"}).encode()

    results = {"success": 0, "error": 0, "throttled": 0}

    def invoke_once(_):
        try:
            resp = lambda_client.invoke(
                FunctionName=FUNCTION_NAME,
                InvocationType="Event",
                Payload=payload,
            )
            return "success" if resp.get("StatusCode") == 202 else "error"
        except lambda_client.exceptions.TooManyRequestsException:
            return "throttled"
        except Exception:
            return "error"

    print(f"  호출 시작: {count}회 (동시성 {INVOKE_CONCURRENCY})")
    start = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=INVOKE_CONCURRENCY) as executor:
        futures = [executor.submit(invoke_once, i) for i in range(count)]
        for future in concurrent.futures.as_completed(futures):
            results[future.result()] += 1

    elapsed = time.time() - start
    print(f"  완료: {elapsed:.1f}초 소요")
    print(f"  결과: 성공 {results['success']}, 오류 {results['error']}, 쓰로틀 {results['throttled']}")

    return results


def run_pipeline(dry_run: bool = False) -> dict:
    """파이프라인 실행"""
    resource = assemble_resource(FUNCTION_NAME, "Lambda")
    initial_state = _build_initial_state(resource)

    if dry_run:
        initial_state["dry_run"] = True

    # 메트릭 출력
    print("  수집된 메트릭:")
    raw_metrics = initial_state.get("raw_metrics", {})
    for key, values in raw_metrics.items():
        if isinstance(values, list) and values:
            recent = values[-5:] if len(values) >= 5 else values
            print(f"    {key}: {[round(v, 2) for v in recent]}")

    thread_id = f"test-spike-{FUNCTION_NAME}-{uuid.uuid4()}"
    config = {"configurable": {"thread_id": thread_id}}

    print("  파이프라인 실행 중...")
    with get_postgres_checkpointer() as checkpointer:
        checkpointer.setup()
        app = build_approval_graph(checkpointer)

        for chunk in app.stream(initial_state, config, stream_mode="updates"):
            for node_name in chunk:
                print(f"    -> {node_name} 완료")

        final_state = dict(app.get_state(config).values)
        interrupted = bool(app.get_state(config).next)

    return {
        "thread_id": thread_id,
        "state": final_state,
        "interrupted": interrupted,
    }


def run_full_test(quick: bool = False, dry_run: bool = False) -> dict:
    """전체 테스트 실행"""
    results = {
        "start_time": datetime.now(timezone.utc).isoformat(),
        "steps": [],
        "success": False,
    }

    total_steps = 5 if quick else 7

    # ── Step 1: 환경 확인 ──
    print_step(1, total_steps, "환경 확인")

    aws_info = check_aws_connection()
    if not aws_info["connected"]:
        print(f"  AWS 연결 실패: {aws_info['error']}")
        return results

    print(f"  AWS Account: {aws_info['account']}")
    print(f"  IAM User: {aws_info['user']}")

    lambda_info = ensure_lambda_function()
    if lambda_info["created"]:
        print(f"  Lambda 함수 생성됨: {FUNCTION_NAME}")
    else:
        print(f"  Lambda 함수 확인됨: {FUNCTION_NAME}")

    results["steps"].append({"name": "환경 확인", "status": "success"})

    # ── Step 2: 초기 스냅샷 ──
    print_step(2, total_steps, "초기 스냅샷 저장")

    try:
        initial_snapshot = take_snapshot("Lambda", FUNCTION_NAME)
        print(f"  현재 동시성 설정: {initial_snapshot}")
        results["initial_snapshot"] = initial_snapshot
    except Exception as e:
        print(f"  스냅샷 저장 실패: {e}")
        initial_snapshot = None

    results["steps"].append({"name": "초기 스냅샷", "status": "success"})

    # ── Step 3: 기준선 생성 ──
    print_step(3, total_steps, f"기준선 생성 ({BASELINE_INVOKE_COUNT}회 호출)")

    baseline_result = invoke_lambda_burst(BASELINE_INVOKE_COUNT)
    results["steps"].append({"name": "기준선 생성", "invoke_result": baseline_result})

    if quick:
        # 빠른 테스트: 1회 스파이크만
        print_step(4, total_steps, "스파이크 생성 (빠른 모드)")

        countdown(60, "CloudWatch 메트릭 반영 대기")
        spike_result = invoke_lambda_burst(SPIKE_INVOKE_COUNT)
        results["steps"].append({"name": "스파이크 생성", "invoke_result": spike_result})

        countdown(60, "메트릭 반영 대기")

        print_step(5, total_steps, "파이프라인 실행")
        pipeline_result = run_pipeline(dry_run)

    else:
        # 전체 테스트: 3회 연속 스파이크 (지속성 체크 통과)
        print_step(4, total_steps, f"스파이크 생성 ({SPIKE_INVOKE_COUNT}회 x {SPIKE_ROUNDS}라운드)")

        spike_results = []
        for round_num in range(1, SPIKE_ROUNDS + 1):
            print(f"\n  === 라운드 {round_num}/{SPIKE_ROUNDS} ===")

            if round_num > 1:
                countdown(METRIC_WAIT_SECONDS, f"라운드 {round_num} 대기")
            else:
                countdown(60, "CloudWatch 메트릭 반영 대기")

            spike_result = invoke_lambda_burst(SPIKE_INVOKE_COUNT)
            spike_results.append(spike_result)

        results["steps"].append({"name": "스파이크 생성", "rounds": spike_results})

        # 마지막 메트릭 반영 대기
        print_step(5, total_steps, "메트릭 반영 대기")
        countdown(60, "최종 메트릭 반영 대기")

        print_step(6, total_steps, "파이프라인 실행")
        pipeline_result = run_pipeline(dry_run)

    state = pipeline_result["state"]
    results["pipeline_result"] = {
        "anomaly_flag": state.get("anomaly_flag"),
        "anomaly_type": state.get("anomaly_type"),
        "selected_action": state.get("selected_action"),
        "risk_level": state.get("risk_level"),
        "action_executed": state.get("action_executed"),
        "action_result": state.get("action_result"),
        "qa_passed": state.get("qa_passed"),
    }

    # 결과 요약 출력
    print("\n  [Detection 결과]")
    print(f"    이상 감지: {state.get('anomaly_flag', False)}")
    print(f"    이상 유형: {state.get('anomaly_type', 'N/A')}")
    print(f"    선택된 액션: {state.get('selected_action', 'N/A')}")
    print(f"    위험 수준: {state.get('risk_level', 'N/A')}")
    print(f"    실행된 액션: {state.get('action_executed', 'N/A')}")
    print(f"    QA 통과: {state.get('qa_passed', 'N/A')}")

    results["steps"].append({"name": "파이프라인 실행", "status": "success"})

    # ── Step 6/7: Throttle 확인 및 롤백 ──
    step_num = 5 if quick else 7
    print_step(step_num, total_steps, "Throttle 확인 및 롤백")

    try:
        current_snapshot = take_snapshot("Lambda", FUNCTION_NAME)
        print(f"  현재 동시성 설정: {current_snapshot}")

        if initial_snapshot and current_snapshot != initial_snapshot:
            print("  -> Throttle 적용됨!")
            results["throttle_applied"] = True
        else:
            print("  -> 동시성 설정 변경 없음")
            results["throttle_applied"] = False

        # 롤백
        if initial_snapshot:
            rollback_result = rollback_action("Lambda", FUNCTION_NAME, initial_snapshot)
            print(f"  롤백 결과: {rollback_result['status']}")

            final_snapshot = take_snapshot("Lambda", FUNCTION_NAME)
            print(f"  최종 동시성 설정: {final_snapshot}")
    except Exception as e:
        print(f"  오류: {e}")

    results["steps"].append({"name": "롤백", "status": "success"})

    # ── 최종 결과 ──
    results["end_time"] = datetime.now(timezone.utc).isoformat()
    results["success"] = True

    return results


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--quick", action="store_true",
        help="빠른 테스트 (1회 스파이크, 지속성 체크 미통과 가능)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="실제 Throttle 없이 테스트"
    )
    args = parser.parse_args()

    print_header("Lambda 호출 폭증 실연동 테스트")

    if args.quick:
        print("  모드: 빠른 테스트 (약 3분)")
        print("  주의: 지속성 체크로 인해 이상 감지가 안 될 수 있음")
    else:
        print("  모드: 전체 테스트 (약 20분)")
        print("  3회 연속 스파이크로 지속성 체크 통과 시도")

    print(f"  Dry Run: {args.dry_run}")
    print(f"  시작 시간: {datetime.now(timezone.utc).isoformat()}")

    try:
        results = run_full_test(quick=args.quick, dry_run=args.dry_run)
    except KeyboardInterrupt:
        print("\n\n테스트 중단됨 (Ctrl+C)")
        return
    except Exception as e:
        print(f"\n\n테스트 실패: {e}")
        import traceback
        traceback.print_exc()
        return

    # 최종 결과 출력
    print_header("테스트 완료")

    if results.get("pipeline_result"):
        pr = results["pipeline_result"]
        detected = pr.get("anomaly_flag", False)

        if detected:
            print("  결과: 이상 감지 성공!")
            print(f"    - 유형: {pr.get('anomaly_type')}")
            print(f"    - 액션: {pr.get('action_executed')}")
            print(f"    - QA: {pr.get('qa_passed')}")
        else:
            print("  결과: 이상 미감지")
            if args.quick:
                print("    -> --quick 모드에서는 지속성 체크로 인해 미감지될 수 있음")
                print("    -> 전체 테스트(--quick 없이)를 실행해보세요")

    print(f"\n  종료 시간: {results.get('end_time', 'N/A')}")


if __name__ == "__main__":
    main()
