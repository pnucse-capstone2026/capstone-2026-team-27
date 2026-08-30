"""
Inbound Traffic Control Handlers
================================
인바운드 트래픽을 제어하는 액션 함수들.

Security Group은 allow-only라 deny를 지원하지 않으므로,
실제 차단은 WAF Rate-based Rule 또는 NACL deny로 구현한다.

모든 함수는 dry_run=True가 기본값이며:
  - dry_run=True: 실제 API 호출 없이 "would execute" 결과 반환
  - dry_run=False: 실제 boto3 API 호출

필요 IAM 권한:
  - wafv2:GetWebACL
  - wafv2:UpdateWebACL
  - wafv2:AssociateWebACL
  - wafv2:ListResourcesForWebACL
  - lambda:PutFunctionConcurrency
  - lambda:GetFunctionConcurrency
  - autoscaling:DescribeAutoScalingGroups
  - autoscaling:UpdateAutoScalingGroup
  - elasticloadbalancing:DescribeLoadBalancers (ALB ARN 조회 시)
"""

from __future__ import annotations

import os
import logging
import time
from typing import Literal

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# 기본값 상수
DEFAULT_WAF_RATE_LIMIT = 2000  # 5분간 최대 요청 수 (AWS 최소값은 100)
DEFAULT_LAMBDA_THROTTLE_LIMIT = 0  # 동시성 0 = 완전 차단
DEFAULT_ASG_SCALEDOWN_CAPACITY = 2  # 스케일다운 목표 용량
MAX_WAF_RETRY = 2  # WAFOptimisticLockException 재시도 횟수


def _get_wafv2_client(scope: Literal["REGIONAL", "CLOUDFRONT"] = "REGIONAL"):
    """
    boto3 WAFv2 클라이언트 생성.
    scope="CLOUDFRONT"일 경우 반드시 us-east-1 리전 사용.
    """
    region = "us-east-1" if scope == "CLOUDFRONT" else os.getenv("AWS_DEFAULT_REGION")
    return boto3.client("wafv2", region_name=region)


def _get_lambda_client():
    """boto3 Lambda 클라이언트 생성."""
    return boto3.client("lambda", region_name=os.getenv("AWS_DEFAULT_REGION"))


def _get_autoscaling_client():
    """boto3 AutoScaling 클라이언트 생성."""
    return boto3.client("autoscaling", region_name=os.getenv("AWS_DEFAULT_REGION"))


def _get_elbv2_client():
    """boto3 ELBv2 클라이언트 생성 (ALB/NLB)."""
    return boto3.client("elbv2", region_name=os.getenv("AWS_DEFAULT_REGION"))


# ── WAF Rate-based Rule ─────────────────────────────────────────────────────


def apply_waf_rate_based_rule(
    resource_arn: str,
    rule_name: str,
    limit: int = DEFAULT_WAF_RATE_LIMIT,
    aggregate_key: Literal["IP", "FORWARDED_IP"] = "IP",
    web_acl_name: str | None = None,
    web_acl_id: str | None = None,
    scope: Literal["REGIONAL", "CLOUDFRONT"] = "REGIONAL",
    dry_run: bool = True,
) -> dict:
    """
    WAF Web ACL에 Rate-based Rule을 추가하여 과도한 요청을 차단.

    Args:
        resource_arn: 보호할 리소스 ARN (ALB, API Gateway, AppSync 등)
        rule_name: 생성할 Rate-based Rule 이름
        limit: 5분간 최대 허용 요청 수 (기본 2000, AWS 최소값 100)
        aggregate_key: 요청 집계 기준 ("IP" 또는 "FORWARDED_IP")
        web_acl_name: 기존 Web ACL 이름 (없으면 자동 생성)
        web_acl_id: 기존 Web ACL ID (web_acl_name과 함께 필요)
        scope: "REGIONAL" (ALB, API GW) 또는 "CLOUDFRONT"
        dry_run: True면 실제 API 호출 없이 실행 계획만 반환

    Returns:
        dict: {"status": "success"/"failed"/"dry_run", ...}

    Notes:
        - get_web_acl로 기존 규칙 + LockToken 조회
        - update_web_acl로 전체 규칙 갱신 (부분 수정 API 없음)
        - WAFOptimisticLockException 시 LockToken 재조회 후 최대 2회 재시도
        - Web ACL 미연결 시 associate_web_acl도 호출
    """
    if limit < 100:
        return {
            "status": "failed",
            "error": f"Rate limit must be >= 100 (AWS minimum), got {limit}",
        }

    result_info = {
        "resource_arn": resource_arn,
        "rule_name": rule_name,
        "limit": limit,
        "aggregate_key": aggregate_key,
        "scope": scope,
        "web_acl_name": web_acl_name,
        "web_acl_id": web_acl_id,
    }

    if dry_run:
        logger.info(
            "[DRY-RUN] WAF Rate-based Rule 적용 계획: %s (limit=%d)", rule_name, limit
        )
        return {
            "status": "dry_run",
            "would_execute": "apply_waf_rate_based_rule",
            **result_info,
        }

    waf = _get_wafv2_client(scope)

    # 1. Web ACL이 지정되지 않으면 새로 생성
    if not web_acl_name or not web_acl_id:
        return _create_web_acl_with_rate_rule(
            waf, resource_arn, rule_name, limit, aggregate_key, scope
        )

    # 2. 기존 Web ACL에 Rate-based Rule 추가
    for attempt in range(MAX_WAF_RETRY + 1):
        try:
            # 현재 Web ACL 조회 (LockToken 필수)
            get_resp = waf.get_web_acl(Name=web_acl_name, Id=web_acl_id, Scope=scope)
            web_acl = get_resp["WebACL"]
            lock_token = get_resp["LockToken"]

            # 기존 규칙에 새 Rate-based Rule 추가
            existing_rules = web_acl.get("Rules", [])

            # 같은 이름의 규칙이 있는지 확인
            if any(r["Name"] == rule_name for r in existing_rules):
                logger.info("WAF Rule '%s' 이미 존재 — 업데이트 생략", rule_name)
                return {
                    "status": "success",
                    "detail": "rule_already_exists",
                    **result_info,
                }

            # 새 규칙 생성 (Priority는 기존 규칙 중 최대값 + 1)
            max_priority = max((r["Priority"] for r in existing_rules), default=0)
            new_rule = _build_rate_based_rule(
                rule_name, limit, aggregate_key, max_priority + 1
            )
            updated_rules = existing_rules + [new_rule]

            # Web ACL 업데이트
            waf.update_web_acl(
                Name=web_acl_name,
                Id=web_acl_id,
                Scope=scope,
                LockToken=lock_token,
                DefaultAction=web_acl.get("DefaultAction", {"Allow": {}}),
                Rules=updated_rules,
                VisibilityConfig=web_acl["VisibilityConfig"],
            )

            logger.info("WAF Rate-based Rule '%s' 추가 완료", rule_name)

            # 리소스에 Web ACL 연결 확인
            _ensure_web_acl_associated(waf, web_acl["ARN"], resource_arn, scope)

            return {"status": "success", **result_info}

        except waf.exceptions.WAFOptimisticLockException:
            if attempt < MAX_WAF_RETRY:
                logger.warning(
                    "WAFOptimisticLockException 발생 — 재시도 %d/%d",
                    attempt + 1,
                    MAX_WAF_RETRY,
                )
                time.sleep(0.5)  # 짧은 대기 후 재시도
                continue
            logger.error("WAF 업데이트 실패: LockToken 충돌 (최대 재시도 초과)")
            return {"status": "failed", "error": "WAFOptimisticLockException", **result_info}

        except ClientError as exc:
            logger.error("WAF Rate-based Rule 적용 실패: %s", exc)
            return {"status": "failed", "error": str(exc), **result_info}

    return {"status": "failed", "error": "unexpected_loop_exit", **result_info}


def _create_web_acl_with_rate_rule(
    waf,
    resource_arn: str,
    rule_name: str,
    limit: int,
    aggregate_key: str,
    scope: str,
) -> dict:
    """새 Web ACL을 생성하고 Rate-based Rule을 추가한 뒤 리소스에 연결."""
    acl_name = f"auto-rate-limit-{int(time.time())}"

    try:
        rate_rule = _build_rate_based_rule(rule_name, limit, aggregate_key, priority=0)

        create_resp = waf.create_web_acl(
            Name=acl_name,
            Scope=scope,
            DefaultAction={"Allow": {}},
            Rules=[rate_rule],
            VisibilityConfig={
                "SampledRequestsEnabled": True,
                "CloudWatchMetricsEnabled": True,
                "MetricName": acl_name.replace("-", "_"),
            },
        )

        web_acl_arn = create_resp["Summary"]["ARN"]
        web_acl_id = create_resp["Summary"]["Id"]

        # 리소스에 연결
        waf.associate_web_acl(WebACLArn=web_acl_arn, ResourceArn=resource_arn)

        logger.info(
            "새 Web ACL '%s' 생성 및 리소스 연결 완료 (ARN: %s)", acl_name, web_acl_arn
        )

        return {
            "status": "success",
            "created_web_acl": True,
            "web_acl_name": acl_name,
            "web_acl_id": web_acl_id,
            "web_acl_arn": web_acl_arn,
            "resource_arn": resource_arn,
            "rule_name": rule_name,
            "limit": limit,
        }

    except ClientError as exc:
        logger.error("Web ACL 생성 실패: %s", exc)
        return {"status": "failed", "error": str(exc)}


def _build_rate_based_rule(
    name: str, limit: int, aggregate_key: str, priority: int
) -> dict:
    """WAF Rate-based Rule 구조 생성."""
    return {
        "Name": name,
        "Priority": priority,
        "Statement": {
            "RateBasedStatement": {
                "Limit": limit,
                "AggregateKeyType": aggregate_key,
            }
        },
        "Action": {"Block": {}},
        "VisibilityConfig": {
            "SampledRequestsEnabled": True,
            "CloudWatchMetricsEnabled": True,
            "MetricName": name.replace("-", "_"),
        },
    }


def _ensure_web_acl_associated(
    waf, web_acl_arn: str, resource_arn: str, scope: str
) -> None:
    """리소스에 Web ACL이 연결되어 있지 않으면 연결."""
    try:
        resource_type = _get_resource_type_from_arn(resource_arn)
        if not resource_type:
            logger.warning("알 수 없는 리소스 타입, Web ACL 연결 확인 생략: %s", resource_arn)
            return

        # 현재 연결된 리소스 목록 조회
        resp = waf.list_resources_for_web_acl(WebACLArn=web_acl_arn, ResourceType=resource_type)
        associated = resp.get("ResourceArns", [])

        if resource_arn not in associated:
            waf.associate_web_acl(WebACLArn=web_acl_arn, ResourceArn=resource_arn)
            logger.info("Web ACL을 리소스에 연결: %s", resource_arn)

    except ClientError as exc:
        logger.warning("Web ACL 연결 확인/연결 실패: %s", exc)


def _get_resource_type_from_arn(arn: str) -> str | None:
    """ARN에서 WAF 리소스 타입 추출."""
    if "elasticloadbalancing" in arn and "loadbalancer/app" in arn:
        return "APPLICATION_LOAD_BALANCER"
    if "apigateway" in arn:
        return "API_GATEWAY"
    if "appsync" in arn:
        return "APPSYNC"
    if "cognito-idp" in arn:
        return "COGNITO_USER_POOL"
    if "apprunner" in arn:
        return "APP_RUNNER_SERVICE"
    if "verified-access" in arn:
        return "VERIFIED_ACCESS_INSTANCE"
    return None


# ── Lambda Concurrency Throttle ─────────────────────────────────────────────


def throttle_lambda_concurrency(
    function_name: str,
    reserved_concurrency: int = DEFAULT_LAMBDA_THROTTLE_LIMIT,
    dry_run: bool = True,
) -> dict:
    """
    Lambda 함수의 예약 동시성을 설정하여 호출을 제한.

    Args:
        function_name: Lambda 함수 이름 또는 ARN
        reserved_concurrency: 예약 동시성 (0이면 완전 차단, 기본값 0)
        dry_run: True면 실제 API 호출 없이 실행 계획만 반환

    Returns:
        dict: {"status": "success"/"failed"/"dry_run", ...}

    Notes:
        - reserved_concurrency=0: 함수 호출 완전 차단
        - reserved_concurrency>0: 해당 수만큼만 동시 실행 허용
        - 기존 pipeline/action_agent.py의 _execute_lambda_throttle()과 호환
    """
    result_info = {
        "function_name": function_name,
        "reserved_concurrency": reserved_concurrency,
    }

    if dry_run:
        logger.info(
            "[DRY-RUN] Lambda Throttle 계획: %s (concurrency=%d)",
            function_name,
            reserved_concurrency,
        )
        return {
            "status": "dry_run",
            "would_execute": "put_function_concurrency",
            **result_info,
        }

    lambda_client = _get_lambda_client()

    try:
        # 현재 동시성 설정 조회 (스냅샷용)
        try:
            current = lambda_client.get_function_concurrency(FunctionName=function_name)
            previous_concurrency = current.get("ReservedConcurrentExecutions", -1)
        except ClientError:
            previous_concurrency = -1

        # 새 동시성 설정
        resp = lambda_client.put_function_concurrency(
            FunctionName=function_name,
            ReservedConcurrentExecutions=reserved_concurrency,
        )

        logger.info(
            "Lambda Throttle 완료: %s (이전=%d, 현재=%d)",
            function_name,
            previous_concurrency,
            reserved_concurrency,
        )

        return {
            "status": "success",
            "previous_concurrency": previous_concurrency,
            "new_concurrency": resp.get("ReservedConcurrentExecutions", reserved_concurrency),
            **result_info,
        }

    except ClientError as exc:
        logger.error("Lambda Throttle 실패 (%s): %s", function_name, exc)
        return {"status": "failed", "error": str(exc), **result_info}


# ── AutoScaling ScaleDown + WAF Rate Limit ──────────────────────────────────


def scale_down_with_rate_limit(
    auto_scaling_group_name: str,
    target_capacity: int = DEFAULT_ASG_SCALEDOWN_CAPACITY,
    associated_alb_arn: str | None = None,
    waf_rate_limit: int = DEFAULT_WAF_RATE_LIMIT,
    waf_rule_name: str | None = None,
    dry_run: bool = True,
) -> dict:
    """
    AutoScaling 그룹을 스케일다운하고, ALB가 연결되어 있으면 WAF Rate-based Rule도 적용.

    시나리오 3 (EDoS 의심) 대응용:
    - 급증한 인스턴스 수를 줄여 비용 절감
    - ALB 앞단에 WAF Rate-based Rule을 적용하여 과도한 요청 차단

    Args:
        auto_scaling_group_name: AutoScaling 그룹 이름
        target_capacity: 목표 인스턴스 수 (max_size와 desired_capacity를 이 값으로 설정)
        associated_alb_arn: 연결된 ALB ARN (있으면 WAF Rule도 적용)
        waf_rate_limit: WAF Rate-based Rule 제한 (5분간 요청 수)
        waf_rule_name: WAF Rule 이름 (없으면 자동 생성)
        dry_run: True면 실제 API 호출 없이 실행 계획만 반환

    Returns:
        dict: {"status": "success"/"failed"/"dry_run", "scaledown_result": ..., "waf_result": ...}
    """
    if waf_rule_name is None:
        waf_rule_name = f"rate-limit-{auto_scaling_group_name[:32]}"

    result_info = {
        "auto_scaling_group_name": auto_scaling_group_name,
        "target_capacity": target_capacity,
        "associated_alb_arn": associated_alb_arn,
        "waf_rate_limit": waf_rate_limit,
        "waf_rule_name": waf_rule_name,
    }

    if dry_run:
        logger.info(
            "[DRY-RUN] ScaleDown + Rate Limit 계획: %s (capacity=%d, ALB=%s)",
            auto_scaling_group_name,
            target_capacity,
            associated_alb_arn or "없음",
        )
        plan = {
            "status": "dry_run",
            "would_execute": ["update_auto_scaling_group"],
            **result_info,
        }
        if associated_alb_arn:
            plan["would_execute"].append("apply_waf_rate_based_rule")
        return plan

    asg_client = _get_autoscaling_client()

    # 1. AutoScaling ScaleDown
    scaledown_result = _execute_autoscaling_scaledown_internal(
        asg_client, auto_scaling_group_name, target_capacity
    )

    # 2. WAF Rate-based Rule 적용 (ALB가 있는 경우)
    waf_result = None
    if associated_alb_arn and scaledown_result.get("status") == "success":
        waf_result = apply_waf_rate_based_rule(
            resource_arn=associated_alb_arn,
            rule_name=waf_rule_name,
            limit=waf_rate_limit,
            dry_run=False,  # 부모 함수에서 이미 dry_run 체크함
        )

    # 최종 결과 조합
    overall_status = scaledown_result.get("status", "failed")
    if waf_result and waf_result.get("status") == "failed":
        overall_status = "partial_success"

    return {
        "status": overall_status,
        "scaledown_result": scaledown_result,
        "waf_result": waf_result,
        **result_info,
    }


def _execute_autoscaling_scaledown_internal(
    asg_client, group_name: str, target_capacity: int
) -> dict:
    """AutoScaling 그룹 스케일다운 내부 구현."""
    try:
        # 현재 상태 조회
        resp = asg_client.describe_auto_scaling_groups(AutoScalingGroupNames=[group_name])
        if not resp["AutoScalingGroups"]:
            return {"status": "failed", "error": f"ASG not found: {group_name}"}

        group = resp["AutoScalingGroups"][0]
        previous_max = group["MaxSize"]
        previous_desired = group["DesiredCapacity"]

        # 새 desired는 target_capacity와 현재 중 작은 값
        new_desired = min(previous_desired, target_capacity)

        asg_client.update_auto_scaling_group(
            AutoScalingGroupName=group_name,
            MaxSize=target_capacity,
            DesiredCapacity=new_desired,
        )

        logger.info(
            "AutoScaling ScaleDown 완료: %s (이전 max=%d, desired=%d → 현재 max=%d, desired=%d)",
            group_name,
            previous_max,
            previous_desired,
            target_capacity,
            new_desired,
        )

        return {
            "status": "success",
            "previous_max_size": previous_max,
            "previous_desired_capacity": previous_desired,
            "new_max_size": target_capacity,
            "new_desired_capacity": new_desired,
        }

    except ClientError as exc:
        logger.error("AutoScaling ScaleDown 실패 (%s): %s", group_name, exc)
        return {"status": "failed", "error": str(exc)}


# ── 유틸리티: ALB ARN 조회 ──────────────────────────────────────────────────


def get_alb_arn_for_asg(auto_scaling_group_name: str) -> str | None:
    """
    AutoScaling 그룹에 연결된 ALB ARN을 조회.
    Target Group을 통해 연결된 경우 해당 ALB ARN을 반환.

    Returns:
        ALB ARN 또는 None (연결된 ALB가 없는 경우)
    """
    try:
        asg_client = _get_autoscaling_client()
        elbv2 = _get_elbv2_client()

        # ASG의 Target Group ARN 조회
        resp = asg_client.describe_auto_scaling_groups(
            AutoScalingGroupNames=[auto_scaling_group_name]
        )
        if not resp["AutoScalingGroups"]:
            return None

        group = resp["AutoScalingGroups"][0]
        target_group_arns = group.get("TargetGroupARNs", [])

        if not target_group_arns:
            return None

        # Target Group의 Load Balancer ARN 조회
        tg_resp = elbv2.describe_target_groups(TargetGroupArns=target_group_arns[:1])
        if not tg_resp["TargetGroups"]:
            return None

        lb_arns = tg_resp["TargetGroups"][0].get("LoadBalancerArns", [])
        if not lb_arns:
            return None

        # ALB인지 확인
        lb_resp = elbv2.describe_load_balancers(LoadBalancerArns=lb_arns[:1])
        if not lb_resp["LoadBalancers"]:
            return None

        lb = lb_resp["LoadBalancers"][0]
        if lb.get("Type") == "application":
            return lb["LoadBalancerArn"]

        return None

    except ClientError as exc:
        logger.warning("ALB ARN 조회 실패 (%s): %s", auto_scaling_group_name, exc)
        return None
