#!/usr/bin/env python3
"""
AutoScaling ScaleDown 실연동 테스트
"""
import sys
import json
import time
import os
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / ".env")

import boto3
from pipeline.inbound_handlers import scale_down_with_rate_limit
from pipeline.QA_agent import qa_node

TEST_ASG_NAME = "integration-test-asg"


def get_asg_info(asg_name: str) -> dict:
    """ASG 현재 상태 조회"""
    client = boto3.client("autoscaling", region_name="ap-northeast-2")
    resp = client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])
    if not resp["AutoScalingGroups"]:
        return {}
    asg = resp["AutoScalingGroups"][0]
    return {
        "min_size": asg["MinSize"],
        "max_size": asg["MaxSize"],
        "desired_capacity": asg["DesiredCapacity"],
    }


def update_asg(asg_name: str, max_size: int, desired_capacity: int):
    """ASG 설정 업데이트"""
    client = boto3.client("autoscaling", region_name="ap-northeast-2")
    client.update_auto_scaling_group(
        AutoScalingGroupName=asg_name,
        MaxSize=max_size,
        DesiredCapacity=desired_capacity,
    )


def test_scaledown_real():
    """AutoScaling ScaleDown 실연동 테스트"""
    print("\n" + "=" * 70)
    print("테스트: AutoScaling ScaleDown 실연동")
    print("=" * 70)

    # 1. 초기 상태 확인
    print(f"\n[Phase 1] 초기 상태 확인...")
    initial_state = get_asg_info(TEST_ASG_NAME)
    if not initial_state:
        print(f"  ✗ ASG '{TEST_ASG_NAME}' 없음")
        return False
    print(f"  초기 상태: max={initial_state['max_size']}, desired={initial_state['desired_capacity']}")

    # 2. ScaleDown 테스트를 위해 max_size를 높게 설정
    print(f"\n[Phase 2] ScaleDown 테스트 준비 (max=10 설정)...")
    update_asg(TEST_ASG_NAME, max_size=10, desired_capacity=0)
    time.sleep(1)
    before_state = get_asg_info(TEST_ASG_NAME)
    print(f"  변경 후: max={before_state['max_size']}, desired={before_state['desired_capacity']}")

    # 3. dry_run으로 계획 확인
    print(f"\n[Phase 3] dry_run으로 실행 계획 확인...")
    dry_result = scale_down_with_rate_limit(
        auto_scaling_group_name=TEST_ASG_NAME,
        target_capacity=2,
        dry_run=True,
    )
    print(f"  dry_run 결과: {json.dumps(dry_result, indent=2, ensure_ascii=False)}")
    assert dry_result["status"] == "dry_run", "dry_run 상태 확인 실패"

    # 4. 실제 ScaleDown 적용 (max_size=2로 제한)
    print(f"\n[Phase 4] 실제 ScaleDown 적용 (target_capacity=2)...")
    real_result = scale_down_with_rate_limit(
        auto_scaling_group_name=TEST_ASG_NAME,
        target_capacity=2,
        dry_run=False,  # 실제 실행!
    )
    print(f"  실행 결과: {json.dumps(real_result, indent=2, ensure_ascii=False)}")

    if real_result.get("scaledown_result", {}).get("status") != "success":
        print(f"  ✗ ScaleDown 적용 실패")
        return False

    # 5. 적용 확인
    print(f"\n[Phase 5] 적용 확인...")
    time.sleep(1)
    after_state = get_asg_info(TEST_ASG_NAME)
    print(f"  현재 상태: max={after_state['max_size']}, desired={after_state['desired_capacity']}")

    # max_size가 2로 변경되었는지 확인
    if after_state["max_size"] != 2:
        print(f"  ✗ max_size 불일치: expected=2, actual={after_state['max_size']}")
        return False
    print(f"  ✓ ScaleDown 정상 적용됨 (max_size: 10 → 2)")

    # 6. 원래 상태 복원
    print(f"\n[Phase 6] 원래 상태 복원...")
    update_asg(TEST_ASG_NAME, max_size=initial_state["max_size"], desired_capacity=initial_state["desired_capacity"])
    restored_state = get_asg_info(TEST_ASG_NAME)
    print(f"  복원 후: max={restored_state['max_size']}, desired={restored_state['desired_capacity']}")
    print(f"  ✓ 원래 상태 복원 완료")

    return True


def test_qa_after_scaledown():
    """ScaleDown 액션 후 QA 검증"""
    print("\n" + "=" * 70)
    print("테스트: ScaleDown 액션 후 QA 검증")
    print("=" * 70)

    # 1. 초기 상태 저장
    initial_state = get_asg_info(TEST_ASG_NAME)
    if not initial_state:
        print(f"  ✗ ASG 없음")
        return False

    # 2. ScaleDown 준비 및 적용
    print(f"\n[Phase 1] ScaleDown 적용...")
    update_asg(TEST_ASG_NAME, max_size=10, desired_capacity=0)
    time.sleep(1)

    scaledown_result = scale_down_with_rate_limit(
        auto_scaling_group_name=TEST_ASG_NAME,
        target_capacity=2,
        dry_run=False,
    )
    print(f"  ScaleDown 결과: status={scaledown_result.get('scaledown_result', {}).get('status')}")

    # 3. QA 검증
    print(f"\n[Phase 2] QA 검증...")
    qa_state = {
        "resource_id": TEST_ASG_NAME,
        "resource_type": "AutoScaling",
        "anomaly_type": "risk_security",
        "action_executed": "ScaleDown",
        "action_result": scaledown_result,
        "raw_metrics": {
            "group_in_service_instances": [2.0] * 27 + [10.0] * 2 + [2.0],
            "group_desired_capacity": [2.0] * 27 + [10.0] * 2 + [2.0],
            "cost": [0.5] * 27 + [5.0] * 2 + [0.5],  # 비용 감소
        },
        "pre_action_snapshot": {"max_size": 10, "desired_capacity": 0},
        "log_entries": [],
        "rollback_count": 0,
    }

    qa_result_state = qa_node(qa_state)
    qa_passed = qa_result_state.get("qa_passed", False)
    sla_result = qa_result_state.get("sla_check_result", {})

    print(f"  QA 결과: qa_passed={qa_passed}")
    print(f"  SLA: cpu_ok={sla_result.get('cpu_ok')}, cost_ok={sla_result.get('cost_ok')}, availability_ok={sla_result.get('availability_ok')}")

    # 4. 원복
    print(f"\n[Phase 3] 원래 상태 복원...")
    update_asg(TEST_ASG_NAME, max_size=initial_state["max_size"], desired_capacity=initial_state["desired_capacity"])
    print(f"  ✓ 복원 완료")

    return qa_passed


def main():
    print("=" * 70)
    print("AutoScaling ScaleDown 실연동 테스트")
    print(f"테스트 대상: {TEST_ASG_NAME}")
    print("=" * 70)

    results = {
        "scaledown_real": False,
        "qa_after_scaledown": False,
    }

    try:
        # 1. ScaleDown 실연동
        try:
            results["scaledown_real"] = test_scaledown_real()
        except Exception as e:
            print(f"\n✗ ScaleDown 테스트 예외: {e}")
            import traceback
            traceback.print_exc()

        # 2. QA 검증
        try:
            results["qa_after_scaledown"] = test_qa_after_scaledown()
        except Exception as e:
            print(f"\n✗ QA 테스트 예외: {e}")
            import traceback
            traceback.print_exc()

    finally:
        # 정리: ASG 원복 확인
        print("\n" + "=" * 70)
        print("정리 작업")
        print("=" * 70)
        state = get_asg_info(TEST_ASG_NAME)
        if state:
            print(f"  ASG 최종 상태: max={state['max_size']}, desired={state['desired_capacity']}")

    # 최종 결과
    print("\n" + "=" * 70)
    print("최종 결과")
    print("=" * 70)
    print(f"  ScaleDown 실연동: {'✓ PASS' if results['scaledown_real'] else '✗ FAIL'}")
    print(f"  QA 검증 (ScaleDown 후): {'✓ PASS' if results['qa_after_scaledown'] else '✗ FAIL'}")

    all_passed = all(results.values())
    print(f"\n{'✓ 모든 테스트 통과!' if all_passed else '✗ 일부 테스트 실패'}")

    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
