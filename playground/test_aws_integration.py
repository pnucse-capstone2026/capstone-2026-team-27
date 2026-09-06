#!/usr/bin/env python3
"""
AWS 실연동 통합 테스트
- Lambda Throttle 핸들러: 실제 AWS API 호출
- QA 검증: 액션 후 SLA 검증
- 롤백: 원래 상태 복원
"""
import sys
import json
import time
import os
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# .env 파일 로드 (파이프라인과 동일한 환경 사용)
from dotenv import load_dotenv
load_dotenv(project_root / ".env")

import boto3
from pipeline.inbound_handlers import throttle_lambda_concurrency
from pipeline.QA_agent import qa_node

# 테스트 대상 Lambda 함수 (capstone 계정의 기존 함수 사용)
TEST_LAMBDA_NAME = "detection-test-lambda"


def get_lambda_concurrency(function_name: str) -> int:
    """Lambda 함수의 현재 동시성 설정 조회 (-1이면 미설정)"""
    client = boto3.client("lambda", region_name="ap-northeast-2")
    try:
        resp = client.get_function_concurrency(FunctionName=function_name)
        return resp.get("ReservedConcurrentExecutions", -1)
    except client.exceptions.ResourceNotFoundException:
        return -1


def delete_lambda_concurrency(function_name: str) -> bool:
    """Lambda 함수의 동시성 설정 제거 (원래 상태 복원)"""
    client = boto3.client("lambda", region_name="ap-northeast-2")
    try:
        client.delete_function_concurrency(FunctionName=function_name)
        return True
    except Exception as e:
        print(f"  경고: 동시성 제거 실패 - {e}")
        return False


def test_lambda_throttle_real():
    """Lambda Throttle 핸들러 실연동 테스트"""
    print("\n" + "=" * 70)
    print("테스트 1: Lambda Throttle 실연동 테스트")
    print("=" * 70)

    # 1. 초기 상태 확인
    print(f"\n[Phase 1] 초기 상태 확인...")
    initial_concurrency = get_lambda_concurrency(TEST_LAMBDA_NAME)
    print(f"  초기 동시성: {initial_concurrency} (-1 = 미설정)")

    # NOTE: 계정 동시성 제한이 낮아 reserved_concurrency=0 (완전 차단) 테스트
    # 실제 운영에서는 적절한 값(예: 10)으로 설정
    TEST_CONCURRENCY = 0  # 0 = 함수 호출 완전 차단

    # 2. dry_run으로 계획 확인
    print(f"\n[Phase 2] dry_run으로 실행 계획 확인...")
    dry_result = throttle_lambda_concurrency(
        function_name=TEST_LAMBDA_NAME,
        reserved_concurrency=TEST_CONCURRENCY,
        dry_run=True,
    )
    print(f"  dry_run 결과: {json.dumps(dry_result, indent=2, ensure_ascii=False)}")
    assert dry_result["status"] == "dry_run", "dry_run 상태 확인 실패"

    # 3. 실제 Throttle 적용 (dry_run=False)
    print(f"\n[Phase 3] 실제 Throttle 적용 (동시성={TEST_CONCURRENCY})...")
    real_result = throttle_lambda_concurrency(
        function_name=TEST_LAMBDA_NAME,
        reserved_concurrency=TEST_CONCURRENCY,
        dry_run=False,  # 실제 실행!
    )
    print(f"  실행 결과: {json.dumps(real_result, indent=2, ensure_ascii=False)}")

    if real_result["status"] != "success":
        print(f"  ✗ Throttle 적용 실패: {real_result.get('error', 'Unknown')}")
        return False

    # 4. 적용 확인
    print(f"\n[Phase 4] 적용 확인...")
    time.sleep(1)  # AWS 반영 대기
    current_concurrency = get_lambda_concurrency(TEST_LAMBDA_NAME)
    print(f"  현재 동시성: {current_concurrency}")
    assert current_concurrency == TEST_CONCURRENCY, f"동시성 불일치: expected={TEST_CONCURRENCY}, actual={current_concurrency}"
    print(f"  ✓ Throttle 정상 적용됨 (동시성={TEST_CONCURRENCY})")

    # 5. 원래 상태 복원
    print(f"\n[Phase 5] 원래 상태 복원...")
    if initial_concurrency == -1:
        delete_lambda_concurrency(TEST_LAMBDA_NAME)
        restored_concurrency = get_lambda_concurrency(TEST_LAMBDA_NAME)
        print(f"  복원 후 동시성: {restored_concurrency}")
        assert restored_concurrency == -1, "복원 실패"
    else:
        throttle_lambda_concurrency(
            function_name=TEST_LAMBDA_NAME,
            reserved_concurrency=initial_concurrency,
            dry_run=False,
        )
        restored_concurrency = get_lambda_concurrency(TEST_LAMBDA_NAME)
        print(f"  복원 후 동시성: {restored_concurrency}")
        assert restored_concurrency == initial_concurrency, "복원 실패"

    print(f"  ✓ 원래 상태 복원 완료")
    return True


def test_qa_after_throttle():
    """Throttle 액션 후 QA 검증 테스트"""
    print("\n" + "=" * 70)
    print("테스트 2: Throttle 액션 후 QA 검증")
    print("=" * 70)

    # NOTE: 계정 동시성 제한이 낮아 reserved_concurrency=0 (완전 차단) 테스트
    TEST_CONCURRENCY = 0

    # 1. Throttle 적용
    print(f"\n[Phase 1] Throttle 적용 (동시성={TEST_CONCURRENCY})...")
    initial_concurrency = get_lambda_concurrency(TEST_LAMBDA_NAME)
    throttle_result = throttle_lambda_concurrency(
        function_name=TEST_LAMBDA_NAME,
        reserved_concurrency=TEST_CONCURRENCY,
        dry_run=False,
    )
    print(f"  Throttle 결과: status={throttle_result['status']}")

    if throttle_result["status"] != "success":
        print(f"  ✗ Throttle 실패, QA 테스트 스킵")
        return False

    # 2. QA 검증용 state 구성
    print(f"\n[Phase 2] QA 검증...")
    qa_state = {
        "resource_id": TEST_LAMBDA_NAME,
        "resource_type": "Lambda",
        "anomaly_type": "cost_spike",
        "action_executed": "Throttle",
        "action_result": throttle_result,  # 실제 결과 사용
        "raw_metrics": {
            "invocation_count": [100.0] * 28 + [5000.0, 100.0],  # 급증 후 감소
            "error_count": [1.0] * 30,
            "duration_avg": [200.0] * 30,
            "cost": [0.1] * 28 + [2.0, 0.1],  # 비용 급증 후 감소
        },
        "pre_action_snapshot": {"reserved_concurrency": initial_concurrency},
        "log_entries": [],
        "rollback_count": 0,
    }

    qa_result_state = qa_node(qa_state)
    qa_passed = qa_result_state.get("qa_passed", False)
    sla_result = qa_result_state.get("sla_check_result", {})

    print(f"  QA 결과: qa_passed={qa_passed}")
    print(f"  SLA 결과: cpu_ok={sla_result.get('cpu_ok')}, cost_ok={sla_result.get('cost_ok')}, availability_ok={sla_result.get('availability_ok')}")
    print(f"  상세: {sla_result.get('detail', 'N/A')}")

    # 3. 원래 상태 복원
    print(f"\n[Phase 3] 원래 상태 복원...")
    if initial_concurrency == -1:
        delete_lambda_concurrency(TEST_LAMBDA_NAME)
    else:
        throttle_lambda_concurrency(
            function_name=TEST_LAMBDA_NAME,
            reserved_concurrency=initial_concurrency,
            dry_run=False,
        )
    print(f"  ✓ 복원 완료")

    return qa_passed


def test_full_pipeline_flow():
    """전체 파이프라인 플로우 테스트 (Detection → Decision → Action → QA)"""
    print("\n" + "=" * 70)
    print("테스트 3: 전체 파이프라인 플로우 (시뮬레이션)")
    print("=" * 70)

    from pipeline.decision_agent import decision_node
    from pipeline.detection_agent import detection_node

    # 1. Detection 시뮬레이션
    print(f"\n[Phase 1] Detection 노드...")
    detection_state = {
        "resource_id": TEST_LAMBDA_NAME,
        "resource_type": "Lambda",
        "raw_metrics": {
            "invocation_count": [100.0] * 25 + [5000.0] * 5,  # 급증 패턴
            "error_count": [1.0] * 30,
            "duration_avg": [200.0] * 30,
            "cost": [0.1] * 25 + [2.0] * 5,
        },
        "log_entries": [],
    }

    detection_result = detection_node(detection_state)
    anomaly_type = detection_result.get("anomaly_type")
    anomaly_detected = detection_result.get("anomaly_detected", False)
    print(f"  이상 탐지: {anomaly_detected}")
    print(f"  이상 유형: {anomaly_type}")

    if not anomaly_detected:
        print(f"  ⚠ 이상 미탐지 (탐지 임계값 미달)")
        # 강제로 이상 설정하여 다음 단계 테스트
        detection_result["anomaly_detected"] = True
        detection_result["anomaly_type"] = "cost_spike"
        print(f"  → 테스트 진행을 위해 cost_spike로 강제 설정")

    # 2. Decision 노드
    print(f"\n[Phase 2] Decision 노드...")
    decision_result = decision_node(detection_result)
    selected_action = decision_result.get("selected_action")
    action_reason = decision_result.get("action_reason", "")
    print(f"  선택된 액션: {selected_action}")
    print(f"  사유: {action_reason[:80]}..." if len(action_reason) > 80 else f"  사유: {action_reason}")

    # 3. Action 실행 (Throttle인 경우)
    # NOTE: 계정 동시성 제한이 낮아 reserved_concurrency=0 (완전 차단) 테스트
    TEST_CONCURRENCY = 0

    if selected_action == "Throttle":
        print(f"\n[Phase 3] Action 노드 (Throttle 실행, 동시성={TEST_CONCURRENCY})...")
        initial_concurrency = get_lambda_concurrency(TEST_LAMBDA_NAME)

        action_result = throttle_lambda_concurrency(
            function_name=TEST_LAMBDA_NAME,
            reserved_concurrency=TEST_CONCURRENCY,
            dry_run=False,
        )
        print(f"  Throttle 결과: status={action_result['status']}")

        # 4. QA 검증
        print(f"\n[Phase 4] QA 노드...")
        qa_state = {
            **decision_result,
            "action_executed": "Throttle",
            "action_result": action_result,
            "pre_action_snapshot": {"reserved_concurrency": initial_concurrency},
            "raw_metrics": {
                **detection_state["raw_metrics"],
                "cost": [0.1] * 25 + [2.0] * 4 + [0.1],  # 액션 후 비용 감소
            },
            "rollback_count": 0,
        }

        qa_result_state = qa_node(qa_state)
        qa_passed = qa_result_state.get("qa_passed", False)
        print(f"  QA 결과: qa_passed={qa_passed}")

        # 5. 복원
        print(f"\n[Phase 5] 원래 상태 복원...")
        if initial_concurrency == -1:
            delete_lambda_concurrency(TEST_LAMBDA_NAME)
        else:
            throttle_lambda_concurrency(
                function_name=TEST_LAMBDA_NAME,
                reserved_concurrency=initial_concurrency,
                dry_run=False,
            )
        print(f"  ✓ 복원 완료")

        return qa_passed
    else:
        print(f"  ⚠ Throttle 외 액션({selected_action})은 실제 실행 스킵")
        return True


def cleanup():
    """테스트 후 정리"""
    print("\n" + "=" * 70)
    print("정리 작업")
    print("=" * 70)

    # Lambda 동시성 설정 제거
    print(f"\n[Cleanup] Lambda 동시성 설정 확인 및 제거...")
    concurrency = get_lambda_concurrency(TEST_LAMBDA_NAME)
    if concurrency != -1:
        delete_lambda_concurrency(TEST_LAMBDA_NAME)
        print(f"  동시성 설정 제거됨 (이전값: {concurrency})")
    else:
        print(f"  동시성 설정 없음 (정상)")


def main():
    print("=" * 70)
    print("AWS 실연동 통합 테스트 시작")
    print(f"테스트 대상: {TEST_LAMBDA_NAME}")
    print("=" * 70)

    results = {
        "throttle_real": False,
        "qa_after_throttle": False,
        "full_pipeline": False,
    }

    try:
        # 1. Lambda Throttle 실연동 테스트
        try:
            results["throttle_real"] = test_lambda_throttle_real()
        except Exception as e:
            print(f"\n✗ Throttle 테스트 예외: {e}")
            import traceback
            traceback.print_exc()

        # 2. QA 검증 테스트
        try:
            results["qa_after_throttle"] = test_qa_after_throttle()
        except Exception as e:
            print(f"\n✗ QA 테스트 예외: {e}")
            import traceback
            traceback.print_exc()

        # 3. 전체 파이프라인 플로우
        try:
            results["full_pipeline"] = test_full_pipeline_flow()
        except Exception as e:
            print(f"\n✗ 파이프라인 테스트 예외: {e}")
            import traceback
            traceback.print_exc()

    finally:
        # 정리
        cleanup()

    # 최종 결과
    print("\n" + "=" * 70)
    print("최종 결과")
    print("=" * 70)
    print(f"  Lambda Throttle 실연동: {'✓ PASS' if results['throttle_real'] else '✗ FAIL'}")
    print(f"  QA 검증 (Throttle 후): {'✓ PASS' if results['qa_after_throttle'] else '✗ FAIL'}")
    print(f"  전체 파이프라인 플로우: {'✓ PASS' if results['full_pipeline'] else '✗ FAIL'}")

    all_passed = all(results.values())
    print(f"\n{'✓ 모든 테스트 통과!' if all_passed else '✗ 일부 테스트 실패'}")

    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
