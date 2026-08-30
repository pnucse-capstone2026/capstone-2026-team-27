"""
playground/test_inbound_handlers.py

인바운드 트래픽 제어 액션 함수 단위 테스트.
dry_run=True 기준으로 테스트하여 실제 AWS API 호출 없이 동작 검증.

실행:
    python playground/test_inbound_handlers.py
    또는
    pytest playground/test_inbound_handlers.py -v
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

# ── sys.path 설정 ──────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

# 테스트 대상 모듈
from pipeline.inbound_handlers import (
    apply_waf_rate_based_rule,
    throttle_lambda_concurrency,
    scale_down_with_rate_limit,
    get_alb_arn_for_asg,
    DEFAULT_WAF_RATE_LIMIT,
    DEFAULT_LAMBDA_THROTTLE_LIMIT,
    DEFAULT_ASG_SCALEDOWN_CAPACITY,
)
from pipeline.action_agent import execute_action


# ── 테스트 헬퍼 ─────────────────────────────────────────────────────────────────

def _print_section(title: str) -> None:
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def _print_result(result: dict, prefix: str = "") -> None:
    print(f"{prefix}status: {result.get('status')}")
    for key, value in result.items():
        if key != "status":
            print(f"{prefix}{key}: {value}")


# ── dry_run 테스트 ──────────────────────────────────────────────────────────────

def test_waf_rate_based_rule_dry_run():
    """WAF Rate-based Rule 적용 (dry_run=True)"""
    _print_section("테스트: apply_waf_rate_based_rule (dry_run=True)")

    result = apply_waf_rate_based_rule(
        resource_arn="arn:aws:elasticloadbalancing:ap-northeast-2:123456789012:loadbalancer/app/my-alb/1234567890abcdef",
        rule_name="rate-limit-test",
        limit=1000,
        aggregate_key="IP",
        dry_run=True,
    )

    _print_result(result, "  ")

    assert result["status"] == "dry_run", f"Expected dry_run, got {result['status']}"
    assert result["would_execute"] == "apply_waf_rate_based_rule"
    assert result["limit"] == 1000
    assert result["rule_name"] == "rate-limit-test"
    print("\n  [PASS] WAF Rate-based Rule dry_run 테스트 통과")


def test_waf_rate_limit_validation():
    """WAF Rate-based Rule limit 검증 (100 미만 거부)"""
    _print_section("테스트: apply_waf_rate_based_rule limit 검증")

    result = apply_waf_rate_based_rule(
        resource_arn="arn:aws:elasticloadbalancing:ap-northeast-2:123456789012:loadbalancer/app/my-alb/1234567890abcdef",
        rule_name="invalid-limit-test",
        limit=50,  # AWS 최소값은 100
        dry_run=True,
    )

    _print_result(result, "  ")

    assert result["status"] == "failed", f"Expected failed, got {result['status']}"
    assert "must be >= 100" in result.get("error", "")
    print("\n  [PASS] WAF limit 검증 테스트 통과 (100 미만 거부됨)")


def test_lambda_throttle_dry_run():
    """Lambda Throttle (dry_run=True)"""
    _print_section("테스트: throttle_lambda_concurrency (dry_run=True)")

    # 기본값 테스트 (reserved_concurrency=0, 완전 차단)
    result = throttle_lambda_concurrency(
        function_name="my-test-lambda",
        dry_run=True,
    )

    _print_result(result, "  ")

    assert result["status"] == "dry_run"
    assert result["would_execute"] == "put_function_concurrency"
    assert result["reserved_concurrency"] == DEFAULT_LAMBDA_THROTTLE_LIMIT
    print(f"\n  기본값: reserved_concurrency={DEFAULT_LAMBDA_THROTTLE_LIMIT} (완전 차단)")

    # 사용자 지정 동시성 테스트
    result2 = throttle_lambda_concurrency(
        function_name="my-test-lambda",
        reserved_concurrency=50,
        dry_run=True,
    )

    print(f"\n  사용자 지정: reserved_concurrency=50")
    assert result2["reserved_concurrency"] == 50
    print("\n  [PASS] Lambda Throttle dry_run 테스트 통과")


def test_scale_down_with_rate_limit_dry_run():
    """AutoScaling ScaleDown + WAF (dry_run=True)"""
    _print_section("테스트: scale_down_with_rate_limit (dry_run=True)")

    # ALB 없이 스케일다운만
    result1 = scale_down_with_rate_limit(
        auto_scaling_group_name="my-test-asg",
        target_capacity=2,
        dry_run=True,
    )

    print("  [ALB 없음]")
    _print_result(result1, "    ")

    assert result1["status"] == "dry_run"
    assert "update_auto_scaling_group" in result1["would_execute"]
    assert "apply_waf_rate_based_rule" not in result1["would_execute"]

    # ALB 있으면 WAF도 포함
    result2 = scale_down_with_rate_limit(
        auto_scaling_group_name="my-test-asg",
        target_capacity=2,
        associated_alb_arn="arn:aws:elasticloadbalancing:ap-northeast-2:123456789012:loadbalancer/app/my-alb/1234567890abcdef",
        waf_rate_limit=500,
        dry_run=True,
    )

    print("\n  [ALB 연결됨]")
    _print_result(result2, "    ")

    assert result2["status"] == "dry_run"
    assert "update_auto_scaling_group" in result2["would_execute"]
    assert "apply_waf_rate_based_rule" in result2["would_execute"]
    assert result2["waf_rate_limit"] == 500
    print("\n  [PASS] ScaleDown + Rate Limit dry_run 테스트 통과")


def test_execute_action_dry_run_lambda():
    """execute_action을 통한 Lambda Throttle dry_run 테스트"""
    _print_section("테스트: execute_action (Lambda Throttle, dry_run=True)")

    result = execute_action(
        action="Throttle",
        resource_type="Lambda",
        resource_id="my-test-lambda",
        dry_run=True,
    )

    _print_result(result, "  ")

    assert result["status"] == "dry_run"
    print("\n  [PASS] execute_action Lambda Throttle dry_run 테스트 통과")


def test_execute_action_dry_run_autoscaling():
    """execute_action을 통한 AutoScaling ScaleDown dry_run 테스트"""
    _print_section("테스트: execute_action (AutoScaling ScaleDown, dry_run=True)")

    # WAF 없이
    result1 = execute_action(
        action="ScaleDown",
        resource_type="AutoScaling",
        resource_id="my-test-asg",
        dry_run=True,
    )

    print("  [WAF 미적용]")
    _print_result(result1, "    ")
    assert result1["status"] == "dry_run"

    # WAF 포함
    result2 = execute_action(
        action="ScaleDown",
        resource_type="AutoScaling",
        resource_id="my-test-asg",
        dry_run=True,
        apply_waf=True,
        waf_rate_limit=1000,
    )

    print("\n  [WAF 포함]")
    _print_result(result2, "    ")
    assert result2["status"] == "dry_run"
    print("\n  [PASS] execute_action AutoScaling ScaleDown dry_run 테스트 통과")


def test_execute_action_block_lambda():
    """Lambda Block 액션 테스트 (동시성 0으로 설정)"""
    _print_section("테스트: execute_action (Lambda Block, dry_run=True)")

    result = execute_action(
        action="Block",
        resource_type="Lambda",
        resource_id="my-test-lambda",
        dry_run=True,
    )

    _print_result(result, "  ")

    assert result["status"] == "dry_run"
    assert result["reserved_concurrency"] == 0, "Block은 동시성 0으로 설정해야 함"
    print("\n  [PASS] Lambda Block dry_run 테스트 통과")


def test_execute_action_block_autoscaling():
    """AutoScaling Block 액션 테스트 (ScaleDown + WAF)"""
    _print_section("테스트: execute_action (AutoScaling Block, dry_run=True)")

    result = execute_action(
        action="Block",
        resource_type="AutoScaling",
        resource_id="my-test-asg",
        dry_run=True,
    )

    _print_result(result, "  ")

    assert result["status"] == "dry_run"
    # Block은 자동으로 apply_waf=True
    print("\n  [PASS] AutoScaling Block dry_run 테스트 통과")


# ── Mock을 사용한 실제 호출 시뮬레이션 ──────────────────────────────────────────

def test_lambda_throttle_mock():
    """Lambda Throttle 실제 호출 시뮬레이션 (Mock)"""
    _print_section("테스트: throttle_lambda_concurrency (Mock, dry_run=False)")

    mock_lambda = MagicMock()
    mock_lambda.get_function_concurrency.return_value = {"ReservedConcurrentExecutions": 100}
    mock_lambda.put_function_concurrency.return_value = {"ReservedConcurrentExecutions": 10}

    with patch("pipeline.inbound_handlers._get_lambda_client", return_value=mock_lambda):
        result = throttle_lambda_concurrency(
            function_name="my-test-lambda",
            reserved_concurrency=10,
            dry_run=False,
        )

    _print_result(result, "  ")

    assert result["status"] == "success"
    assert result["previous_concurrency"] == 100
    assert result["new_concurrency"] == 10
    mock_lambda.put_function_concurrency.assert_called_once_with(
        FunctionName="my-test-lambda",
        ReservedConcurrentExecutions=10,
    )
    print("\n  [PASS] Lambda Throttle Mock 테스트 통과")


def test_scale_down_mock():
    """AutoScaling ScaleDown 실제 호출 시뮬레이션 (Mock)"""
    _print_section("테스트: scale_down_with_rate_limit (Mock, dry_run=False)")

    mock_asg = MagicMock()
    mock_asg.describe_auto_scaling_groups.return_value = {
        "AutoScalingGroups": [{
            "AutoScalingGroupName": "my-test-asg",
            "MaxSize": 10,
            "DesiredCapacity": 8,
            "TargetGroupARNs": [],
        }]
    }

    with patch("pipeline.inbound_handlers._get_autoscaling_client", return_value=mock_asg):
        result = scale_down_with_rate_limit(
            auto_scaling_group_name="my-test-asg",
            target_capacity=2,
            dry_run=False,
        )

    print("  scaledown_result:")
    if result.get("scaledown_result"):
        _print_result(result["scaledown_result"], "    ")

    assert result["status"] == "success"
    assert result["scaledown_result"]["status"] == "success"
    assert result["scaledown_result"]["new_max_size"] == 2
    assert result["scaledown_result"]["new_desired_capacity"] == 2
    mock_asg.update_auto_scaling_group.assert_called_once()
    print("\n  [PASS] AutoScaling ScaleDown Mock 테스트 통과")


# ── 메인 ────────────────────────────────────────────────────────────────────────

def run_all_tests():
    """모든 테스트 실행"""
    print("\n" + "=" * 70)
    print("  인바운드 트래픽 제어 액션 단위 테스트")
    print("  (dry_run=True 기준, 실제 AWS API 호출 없음)")
    print("=" * 70)

    tests = [
        test_waf_rate_based_rule_dry_run,
        test_waf_rate_limit_validation,
        test_lambda_throttle_dry_run,
        test_scale_down_with_rate_limit_dry_run,
        test_execute_action_dry_run_lambda,
        test_execute_action_dry_run_autoscaling,
        test_execute_action_block_lambda,
        test_execute_action_block_autoscaling,
        test_lambda_throttle_mock,
        test_scale_down_mock,
    ]

    passed = 0
    failed = 0

    for test_func in tests:
        try:
            test_func()
            passed += 1
        except AssertionError as e:
            print(f"\n  [FAIL] {test_func.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"\n  [ERROR] {test_func.__name__}: {e}")
            failed += 1

    _print_section("테스트 결과 요약")
    print(f"  통과: {passed}/{len(tests)}")
    print(f"  실패: {failed}/{len(tests)}")

    if failed == 0:
        print("\n  >>> 모든 테스트 통과!")
    else:
        print(f"\n  >>> {failed}개 테스트 실패")
        sys.exit(1)


if __name__ == "__main__":
    run_all_tests()
