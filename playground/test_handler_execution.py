#!/usr/bin/env python3
"""
핸들러 및 QA 검증 테스트
- Throttle 핸들러: dry_run 모드로 설정 확인
- ScaleDown 핸들러: dry_run 모드로 설정 확인
- QA 검증: 액션 성공 후 QA 검증 통과 확인
"""
import sys
import json
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pipeline.inbound_handlers import (
    throttle_lambda_concurrency,
    scale_down_with_rate_limit,
)


def test_throttle_handler():
    """Lambda Throttle 핸들러 설정 테스트 (dry_run 모드)"""
    print("\n" + "=" * 60)
    print("테스트 1: Lambda Throttle 핸들러 설정 확인")
    print("=" * 60)

    function_name = "detection-test-lambda"
    reserved_concurrency = 10  # 동시성 10으로 제한

    # dry_run=True로 계획 확인
    print(f"\n[Phase 1] dry_run=True로 핸들러 설정 확인...")
    dry_result = throttle_lambda_concurrency(
        function_name=function_name,
        reserved_concurrency=reserved_concurrency,
        dry_run=True,
    )
    print(f"  결과: {json.dumps(dry_result, indent=2, ensure_ascii=False)}")

    # 검증
    assert dry_result["status"] == "dry_run", "dry_run 상태 확인 실패"
    assert dry_result["function_name"] == function_name, "function_name 불일치"
    assert dry_result["reserved_concurrency"] == reserved_concurrency, "concurrency 불일치"
    assert dry_result["would_execute"] == "put_function_concurrency", "예정 액션 불일치"

    print("  ✓ Throttle 핸들러 설정 확인 완료")
    print(f"    - 대상 함수: {dry_result['function_name']}")
    print(f"    - 설정할 동시성: {dry_result['reserved_concurrency']}")
    print(f"    - 실행할 API: {dry_result['would_execute']}")
    return True


def test_scaledown_handler():
    """AutoScaling ScaleDown 핸들러 설정 테스트 (dry_run 모드)"""
    print("\n" + "=" * 60)
    print("테스트 2: AutoScaling ScaleDown 핸들러 설정 확인")
    print("=" * 60)

    asg_name = "detection-test-asg"
    target_capacity = 2  # 최대 2로 제한

    # dry_run=True로 계획 확인
    print(f"\n[Phase 1] dry_run=True로 핸들러 설정 확인...")
    dry_result = scale_down_with_rate_limit(
        auto_scaling_group_name=asg_name,
        target_capacity=target_capacity,
        dry_run=True,
    )
    print(f"  결과: {json.dumps(dry_result, indent=2, ensure_ascii=False)}")

    # 검증
    assert dry_result["status"] == "dry_run", "dry_run 상태 확인 실패"
    assert dry_result["auto_scaling_group_name"] == asg_name, "ASG 이름 불일치"
    assert dry_result["target_capacity"] == target_capacity, "target_capacity 불일치"
    assert "update_auto_scaling_group" in dry_result["would_execute"], "예정 액션 불일치"

    print("  ✓ ScaleDown 핸들러 설정 확인 완료")
    print(f"    - 대상 ASG: {dry_result['auto_scaling_group_name']}")
    print(f"    - 목표 용량: {dry_result['target_capacity']}")
    print(f"    - 실행할 API: {dry_result['would_execute']}")
    return True


def test_qa_after_action():
    """액션 성공 시 QA 검증 통과 확인"""
    print("\n" + "=" * 60)
    print("테스트 3: 액션 실행 후 QA 검증")
    print("=" * 60)

    from pipeline.QA_agent import qa_node

    # Lambda Throttle 액션에 대한 QA 검증 (액션 성공 상태)
    # NOTE: cost 메트릭은 액션 후 비용이 감소한 것처럼 설정 (SLA 통과를 위해)
    lambda_state = {
        "resource_id": "detection-test-lambda",
        "resource_type": "Lambda",
        "anomaly_type": "cost_spike",
        "action_executed": "Throttle",
        "action_result": {
            "status": "success",
            "previous_concurrency": -1,
            "new_concurrency": 10,
            "function_name": "detection-test-lambda",
            "reserved_concurrency": 10,
        },
        "raw_metrics": {
            "invocation_count": [100.0] * 25 + [5000.0] * 4 + [100.0],  # 액션 후 감소
            "error_count": [1.0] * 30,
            "duration_avg": [200.0] * 30,
            "cost": [0.1] * 25 + [2.0] * 4 + [0.1],  # 액션 후 비용 감소
        },
        "pre_action_snapshot": {"reserved_concurrency": -1},  # 올바른 키 이름
        "log_entries": [],
        "rollback_count": 0,
    }

    print(f"\n[QA 1] Lambda Throttle 액션 검증...")
    lambda_qa_state = qa_node(lambda_state)
    print(f"  QA 결과: qa_passed={lambda_qa_state.get('qa_passed')}")
    print(f"  SLA 결과: {lambda_qa_state.get('sla_check_result', {})}")
    lambda_passed = lambda_qa_state.get("qa_passed", False)

    # AutoScaling ScaleDown 액션에 대한 QA 검증 (액션 성공 상태)
    # NOTE: cost 메트릭은 액션 후 비용이 감소한 것처럼 설정 (SLA 통과를 위해)
    asg_state = {
        "resource_id": "detection-test-asg",
        "resource_type": "AutoScaling",
        "anomaly_type": "risk_security",
        "action_executed": "ScaleDown",
        "action_result": {
            "status": "success",
            "scaledown_result": {
                "status": "success",
                "previous_max_size": 20,
                "previous_desired_capacity": 20,
            },
        },
        "raw_metrics": {
            "group_in_service_instances": [2.0] * 27 + [20.0] * 2 + [2.0],  # 액션 후 감소
            "group_desired_capacity": [2.0] * 27 + [20.0] * 2 + [2.0],  # 액션 후 감소
            "cost": [0.5] * 27 + [8.0, 8.5] + [0.5],  # 액션 후 비용 감소
        },
        "pre_action_snapshot": {"max_size": 20, "desired_capacity": 20},  # 올바른 키 이름
        "log_entries": [],
        "rollback_count": 0,
    }

    print(f"\n[QA 2] AutoScaling ScaleDown 액션 검증...")
    asg_qa_state = qa_node(asg_state)
    print(f"  QA 결과: qa_passed={asg_qa_state.get('qa_passed')}")
    print(f"  SLA 결과: {asg_qa_state.get('sla_check_result', {})}")
    asg_passed = asg_qa_state.get("qa_passed", False)

    return lambda_passed and asg_passed


def main():
    print("=" * 60)
    print("핸들러 직접 실행 테스트 시작")
    print("=" * 60)

    results = {
        "throttle": False,
        "scaledown": False,
        "qa": False,
    }

    # 1. Lambda Throttle 테스트
    try:
        results["throttle"] = test_throttle_handler()
    except Exception as e:
        print(f"\n✗ Throttle 테스트 예외: {e}")

    # 2. ScaleDown 테스트
    try:
        results["scaledown"] = test_scaledown_handler()
    except Exception as e:
        print(f"\n✗ ScaleDown 테스트 예외: {e}")

    # 3. QA 검증 테스트
    try:
        results["qa"] = test_qa_after_action()
    except Exception as e:
        print(f"\n✗ QA 테스트 예외: {e}")

    # 최종 결과
    print("\n" + "=" * 60)
    print("최종 결과")
    print("=" * 60)
    print(f"  Lambda Throttle: {'✓ PASS' if results['throttle'] else '✗ FAIL'}")
    print(f"  AutoScaling ScaleDown: {'✓ PASS' if results['scaledown'] else '✗ FAIL'}")
    print(f"  QA 검증: {'✓ PASS' if results['qa'] else '✗ FAIL'}")

    all_passed = all(results.values())
    print(f"\n{'✓ 모든 테스트 통과!' if all_passed else '✗ 일부 테스트 실패'}")

    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
