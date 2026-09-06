#!/usr/bin/env python3
"""
인바운드 규칙 실행 테스트
- Classification 규칙 → anomaly_type 분류
- Decision 규칙 → action 선택
- Action 실행 (dry_run)
"""
import sys
import json
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / ".env")

from pipeline.classification_agent import classification_node
from pipeline.decision_agent import decision_node
from pipeline.inbound_handlers import throttle_lambda_concurrency, scale_down_with_rate_limit


def test_lambda_cost_spike_to_throttle():
    """
    테스트: Lambda cost_spike → Throttle
    규칙: CLF-006 (Lambda cost+invocation_count → cost_spike)
        + DEC-002 (Lambda cost_spike → Throttle)
    """
    print("\n" + "=" * 70)
    print("테스트 1: Lambda cost_spike → Throttle")
    print("=" * 70)

    # 1. 초기 상태 (Detection 후 상태 시뮬레이션)
    state = {
        "resource_id": "detection-test-lambda",
        "resource_type": "Lambda",
        "anomaly_detected": True,
        "triggered_metrics": ["cost", "invocation_count"],  # CLF-006 조건
        "raw_metrics": {
            "invocation_count": [100.0] * 25 + [5000.0] * 5,
            "error_count": [1.0] * 30,
            "duration_avg": [200.0] * 30,
            "cost": [0.1] * 25 + [2.0] * 5,
        },
        "log_entries": [],
    }

    # 2. Classification
    print("\n[Phase 1] Classification...")
    clf_state = classification_node(state)
    anomaly_type = clf_state.get("anomaly_type")
    matched_rule = clf_state.get("classification_rule_id")
    print(f"  anomaly_type: {anomaly_type}")
    print(f"  matched_rule: {matched_rule}")

    if anomaly_type != "cost_spike":
        print(f"  ✗ 예상: cost_spike, 실제: {anomaly_type}")
        return False

    # 3. Decision
    print("\n[Phase 2] Decision...")
    dec_state = decision_node(clf_state)
    selected_action = dec_state.get("selected_action")
    decision_rule = dec_state.get("decision_rule_id")
    print(f"  selected_action: {selected_action}")
    print(f"  decision_rule: {decision_rule}")

    if selected_action != "Throttle":
        print(f"  ✗ 예상: Throttle, 실제: {selected_action}")
        return False

    # 4. Action (dry_run)
    print("\n[Phase 3] Action (dry_run)...")
    action_result = throttle_lambda_concurrency(
        function_name="detection-test-lambda",
        reserved_concurrency=10,
        dry_run=True,
    )
    print(f"  결과: {json.dumps(action_result, indent=2, ensure_ascii=False)}")

    if action_result["status"] != "dry_run":
        print(f"  ✗ dry_run 실패")
        return False

    print("\n  ✓ Lambda cost_spike → Throttle 규칙 정상 동작")
    return True


def test_autoscaling_edos_to_scaledown():
    """
    테스트: AutoScaling risk_security → ScaleDown
    규칙: CLF-001 (AutoScaling desired_capacity > 2x mean → risk_security)
        + DEC-003 (AutoScaling risk_security → ScaleDown)
    """
    print("\n" + "=" * 70)
    print("테스트 2: AutoScaling EDoS → ScaleDown")
    print("=" * 70)

    # 1. 초기 상태
    state = {
        "resource_id": "detection-test-asg",
        "resource_type": "AutoScaling",
        "anomaly_detected": True,
        "triggered_metrics": ["group_desired_capacity"],  # CLF-001 조건
        "raw_metrics": {
            "group_in_service_instances": [2.0] * 27 + [20.0] * 3,
            "group_desired_capacity": [2.0] * 27 + [20.0] * 3,  # 평균 대비 급증
            "cost": [0.5] * 27 + [8.0, 8.5, 9.0],
        },
        "log_entries": [],
    }

    # 2. Classification
    print("\n[Phase 1] Classification...")
    clf_state = classification_node(state)
    anomaly_type = clf_state.get("anomaly_type")
    matched_rule = clf_state.get("classification_rule_id")
    print(f"  anomaly_type: {anomaly_type}")
    print(f"  matched_rule: {matched_rule}")

    if anomaly_type != "risk_security":
        print(f"  ✗ 예상: risk_security, 실제: {anomaly_type}")
        return False

    # 3. Decision
    print("\n[Phase 2] Decision...")
    dec_state = decision_node(clf_state)
    selected_action = dec_state.get("selected_action")
    decision_rule = dec_state.get("decision_rule_id")
    print(f"  selected_action: {selected_action}")
    print(f"  decision_rule: {decision_rule}")

    if selected_action != "ScaleDown":
        print(f"  ✗ 예상: ScaleDown, 실제: {selected_action}")
        return False

    # 4. Action (dry_run)
    print("\n[Phase 3] Action (dry_run)...")
    action_result = scale_down_with_rate_limit(
        auto_scaling_group_name="detection-test-asg",
        target_capacity=2,
        dry_run=True,
    )
    print(f"  결과: {json.dumps(action_result, indent=2, ensure_ascii=False)}")

    if action_result["status"] != "dry_run":
        print(f"  ✗ dry_run 실패")
        return False

    print("\n  ✓ AutoScaling EDoS → ScaleDown 규칙 정상 동작")
    return True


def test_ec2_cost_inefficiency_to_resize():
    """
    테스트: EC2 cost_inefficiency → Resize
    규칙: CLF-004 (EC2 cost only → cost_inefficiency)
        + DEC-001 (EC2 cost_inefficiency → Resize)
    """
    print("\n" + "=" * 70)
    print("테스트 3: EC2 cost_inefficiency → Resize")
    print("=" * 70)

    # 1. 초기 상태
    state = {
        "resource_id": "i-test12345",
        "resource_type": "EC2",
        "anomaly_detected": True,
        "triggered_metrics": ["cost"],  # CLF-004 조건 (cost만)
        "raw_metrics": {
            "cpu_utilization": [3.0] * 30,  # 낮은 CPU (정상)
            "network_in": [100.0] * 30,
            "network_out": [80.0] * 30,
            "cost": [0.5] * 25 + [6.0] * 5,  # 비용 급증
        },
        "log_entries": [],
    }

    # 2. Classification
    print("\n[Phase 1] Classification...")
    clf_state = classification_node(state)
    anomaly_type = clf_state.get("anomaly_type")
    matched_rule = clf_state.get("classification_rule_id")
    print(f"  anomaly_type: {anomaly_type}")
    print(f"  matched_rule: {matched_rule}")

    if anomaly_type != "cost_inefficiency":
        print(f"  ✗ 예상: cost_inefficiency, 실제: {anomaly_type}")
        return False

    # 3. Decision
    print("\n[Phase 2] Decision...")
    dec_state = decision_node(clf_state)
    selected_action = dec_state.get("selected_action")
    decision_rule = dec_state.get("decision_rule_id")
    print(f"  selected_action: {selected_action}")
    print(f"  decision_rule: {decision_rule}")

    if selected_action != "Resize":
        print(f"  ✗ 예상: Resize, 실제: {selected_action}")
        return False

    print("\n  ✓ EC2 cost_inefficiency → Resize 규칙 정상 동작")
    print("  (Resize 핸들러는 EC2 인스턴스 필요하여 dry_run 스킵)")
    return True


def main():
    print("=" * 70)
    print("인바운드 규칙 실행 테스트")
    print("=" * 70)

    results = {
        "lambda_throttle": False,
        "asg_scaledown": False,
        "ec2_resize": False,
    }

    try:
        results["lambda_throttle"] = test_lambda_cost_spike_to_throttle()
    except Exception as e:
        print(f"\n✗ Lambda Throttle 테스트 예외: {e}")
        import traceback
        traceback.print_exc()

    try:
        results["asg_scaledown"] = test_autoscaling_edos_to_scaledown()
    except Exception as e:
        print(f"\n✗ ASG ScaleDown 테스트 예외: {e}")
        import traceback
        traceback.print_exc()

    try:
        results["ec2_resize"] = test_ec2_cost_inefficiency_to_resize()
    except Exception as e:
        print(f"\n✗ EC2 Resize 테스트 예외: {e}")
        import traceback
        traceback.print_exc()

    # 최종 결과
    print("\n" + "=" * 70)
    print("최종 결과")
    print("=" * 70)
    print(f"  Lambda cost_spike → Throttle:    {'✓ PASS' if results['lambda_throttle'] else '✗ FAIL'}")
    print(f"  AutoScaling EDoS → ScaleDown:    {'✓ PASS' if results['asg_scaledown'] else '✗ FAIL'}")
    print(f"  EC2 cost_inefficiency → Resize:  {'✓ PASS' if results['ec2_resize'] else '✗ FAIL'}")

    all_passed = all(results.values())
    print(f"\n{'✓ 모든 인바운드 규칙 정상 동작!' if all_passed else '✗ 일부 규칙 실패'}")

    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
