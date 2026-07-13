"""
Action Agent
============
node_contracts.md Step 4 기준.

입력 (읽는 필드):
  - selected_action, resource_id, resource_type, requires_approval, target_instance_type

출력 (채우는 필드):
  - pre_action_snapshot, action_executed, action_result

설계:
  - EC2(Stop, Resize), Lambda(Throttle), AutoScaling(ScaleDown) 구현.
    S3/RDS는 NotImplementedAction 으로 표시해두고 추후 각자 확장.
  - boto3 클라이언트는 모듈 레벨에서 만들지 않고 함수 안에서 생성
    (테스트 시 monkeypatch/mock 주입하기 쉽도록).
  - 리전은 항상 환경변수 AWS_DEFAULT_REGION에서 읽는다 (하드코딩 금지).
  - NoAction이면 스냅샷 생략하고 바로 리턴.
  - requires_approval=True 인 경우, 지금 단계에서는 실제 액션을 실행하지
    않고 "pending_approval" 상태로만 표시한다.
    (60분 대기 타이머는 추후 스케줄러로 별도 구현 — 보고서 2.2.4)
  - 모든 boto3 호출은 ClientError를 잡아서 action_result에 실패 사유를 남긴다.
  - rollback_action()은 QA Agent(강지원 담당)에서 import해서 사용하는 함수.
"""

from __future__ import annotations

import os
import logging

import boto3
from botocore.exceptions import ClientError, WaiterError

from schema.state import PipelineState, EC2Snapshot, LambdaSnapshot, AutoScalingSnapshot

logger = logging.getLogger(__name__)

# Lambda Throttle 시 제한할 동시성 기본값
DEFAULT_LAMBDA_THROTTLE_LIMIT = 10

# AutoScaling ScaleDown 시 축소할 최대 인스턴스 수 기본값
DEFAULT_ASG_SCALEDOWN_MAX_SIZE = 2


def _get_ec2_client():
    """boto3 EC2 클라이언트 생성. 함수 내부에서 생성해야 테스트 시 mock 주입이 쉽다."""
    return boto3.client("ec2", region_name=os.getenv("AWS_DEFAULT_REGION"))


def _get_lambda_client():
    """boto3 Lambda 클라이언트 생성. 함수 내부에서 생성해야 테스트 시 mock 주입이 쉽다."""
    return boto3.client("lambda", region_name=os.getenv("AWS_DEFAULT_REGION"))


def _get_autoscaling_client():
    """boto3 AutoScaling 클라이언트 생성. 함수 내부에서 생성해야 테스트 시 mock 주입이 쉽다."""
    return boto3.client("autoscaling", region_name=os.getenv("AWS_DEFAULT_REGION"))


# ── 스냅샷 ────────────────────────────────────────────────────────────────────

def _take_ec2_snapshot(resource_id: str) -> EC2Snapshot:
    """
    입력: resource_id (EC2 인스턴스 ID)
    출력: EC2Snapshot (instance_type, state, security_group_ids)
    """
    ec2 = _get_ec2_client()
    resp = ec2.describe_instances(InstanceIds=[resource_id])
    instance = resp["Reservations"][0]["Instances"][0]
    snapshot: EC2Snapshot = {
        "instance_type": instance["InstanceType"],
        "state": instance["State"]["Name"],
        "security_group_ids": [sg["GroupId"] for sg in instance.get("SecurityGroups", [])],
    }
    return snapshot


def _take_lambda_snapshot(resource_id: str) -> LambdaSnapshot:
    """
    입력: resource_id (Lambda 함수명)
    출력: LambdaSnapshot (reserved_concurrency, 설정 없으면 -1)
    """
    lambda_client = _get_lambda_client()
    resp = lambda_client.get_function_concurrency(FunctionName=resource_id)
    reserved = resp.get("ReservedConcurrentExecutions")
    snapshot: LambdaSnapshot = {
        "reserved_concurrency": reserved if reserved is not None else -1,
    }
    return snapshot


def _take_autoscaling_snapshot(resource_id: str) -> AutoScalingSnapshot:
    """
    입력: resource_id (AutoScaling 그룹명)
    출력: AutoScalingSnapshot (max_size, desired_capacity)
    """
    asg_client = _get_autoscaling_client()
    resp = asg_client.describe_auto_scaling_groups(AutoScalingGroupNames=[resource_id])
    group = resp["AutoScalingGroups"][0]
    snapshot: AutoScalingSnapshot = {
        "max_size": group["MaxSize"],
        "desired_capacity": group["DesiredCapacity"],
    }
    return snapshot


def take_snapshot(resource_type: str, resource_id: str) -> dict | None:
    """
    입력: resource_type, resource_id
    출력: 리소스 타입별 스냅샷 dict. 미구현 타입은 None
    """
    if resource_type == "EC2":
        return _take_ec2_snapshot(resource_id)
    if resource_type == "Lambda":
        return _take_lambda_snapshot(resource_id)
    if resource_type == "AutoScaling":
        return _take_autoscaling_snapshot(resource_id)

    logger.warning("스냅샷 미구현 리소스 타입: %s — 빈 스냅샷 처리", resource_type)
    return None


# ── 액션 실행 ─────────────────────────────────────────────────────────────────

def _execute_ec2_stop(resource_id: str) -> dict:
    """
    입력: resource_id (EC2 인스턴스 ID)
    출력: {"status": "success"/"failed", ...}
    """
    ec2 = _get_ec2_client()
    try:
        resp = ec2.stop_instances(InstanceIds=[resource_id])
        return {"status": "success", "raw": resp.get("StoppingInstances", [])}
    except ClientError as exc:
        logger.error("EC2 Stop 실패 (%s): %s", resource_id, exc)
        return {"status": "failed", "error": str(exc)}


def _execute_ec2_resize(resource_id: str, target_instance_type: str) -> dict:
    """
    입력: resource_id (EC2 인스턴스 ID), target_instance_type (목표 인스턴스 타입)
    출력: {"status": "success"/"failed", ...}

    Resize는 running 상태에서 인스턴스 타입 변경이 불가하므로
    stop → modify → start 순서를 지킨다.
    """
    ec2 = _get_ec2_client()
    try:
        ec2.stop_instances(InstanceIds=[resource_id])

        waiter = ec2.get_waiter("instance_stopped")
        waiter.wait(InstanceIds=[resource_id])

        ec2.modify_instance_attribute(
            InstanceId=resource_id,
            InstanceType={"Value": target_instance_type},
        )

        ec2.start_instances(InstanceIds=[resource_id])

        return {"status": "success", "new_instance_type": target_instance_type}
    except WaiterError as exc:
        logger.error("EC2 Resize 중 stop 대기 실패 (%s): %s", resource_id, exc)
        return {"status": "failed", "error": f"waiter_error: {exc}"}
    except ClientError as exc:
        logger.error("EC2 Resize 실패 (%s): %s", resource_id, exc)
        return {"status": "failed", "error": str(exc)}


def _execute_lambda_throttle(resource_id: str, limit: int = DEFAULT_LAMBDA_THROTTLE_LIMIT) -> dict:
    """
    입력: resource_id (Lambda 함수명), limit (제한할 동시성, 기본 10)
    출력: {"status": "success"/"failed", ...}
    """
    lambda_client = _get_lambda_client()
    try:
        resp = lambda_client.put_function_concurrency(
            FunctionName=resource_id,
            ReservedConcurrentExecutions=limit,
        )
        return {
            "status": "success",
            "reserved_concurrency": resp.get("ReservedConcurrentExecutions", limit),
        }
    except ClientError as exc:
        logger.error("Lambda Throttle 실패 (%s): %s", resource_id, exc)
        return {"status": "failed", "error": str(exc)}


def _execute_autoscaling_scaledown(
    resource_id: str, max_size: int = DEFAULT_ASG_SCALEDOWN_MAX_SIZE
) -> dict:
    """
    입력: resource_id (AutoScaling 그룹명), max_size (축소할 최대 인스턴스 수, 기본 2)
    출력: {"status": "success"/"failed", ...}
    """
    asg_client = _get_autoscaling_client()
    try:
        current = asg_client.describe_auto_scaling_groups(AutoScalingGroupNames=[resource_id])
        current_desired = current["AutoScalingGroups"][0]["DesiredCapacity"]
        new_desired = min(current_desired, max_size)

        asg_client.update_auto_scaling_group(
            AutoScalingGroupName=resource_id,
            MaxSize=max_size,
            DesiredCapacity=new_desired,
        )
        return {"status": "success", "max_size": max_size, "desired_capacity": new_desired}
    except ClientError as exc:
        logger.error("AutoScaling ScaleDown 실패 (%s): %s", resource_id, exc)
        return {"status": "failed", "error": str(exc)}


def execute_action(
    action: str,
    resource_type: str,
    resource_id: str,
    target_instance_type: str | None = None,
) -> dict:
    """
    입력: action, resource_type, resource_id, target_instance_type(Resize 전용, 없으면 기본값 사용)
    출력: {"status": ..., ...} — 선택된 액션을 실제로 실행하는 dispatcher
    """
    if action == "NoAction":
        return {"status": "skipped"}

    if resource_type == "EC2":
        if action == "Stop":
            return _execute_ec2_stop(resource_id)
        if action == "Resize":
            return _execute_ec2_resize(resource_id, target_instance_type or "t3.small")
        # Stop+Schedule, ScaleDown, Block, Throttle 등은 추후 구현
        logger.warning("EC2 액션 미구현: %s", action)
        return {"status": "not_implemented", "action": action}

    if resource_type == "Lambda":
        if action == "Throttle":
            return _execute_lambda_throttle(resource_id)
        logger.warning("Lambda 액션 미구현: %s", action)
        return {"status": "not_implemented", "action": action}

    if resource_type == "AutoScaling":
        if action == "ScaleDown":
            return _execute_autoscaling_scaledown(resource_id)
        logger.warning("AutoScaling 액션 미구현: %s", action)
        return {"status": "not_implemented", "action": action}

    # S3 / RDS 는 추후 각자 확장
    logger.warning("리소스 타입 미구현: %s (action=%s)", resource_type, action)
    return {"status": "not_implemented", "resource_type": resource_type, "action": action}


def action_node(state: PipelineState) -> PipelineState:
    """
    입력: state["selected_action"], state["resource_type"], state["resource_id"],
          state["requires_approval"], state["target_instance_type"]
    출력: state["pre_action_snapshot"], state["action_executed"], state["action_result"]
    """
    action = state["selected_action"]
    resource_type = state["resource_type"]
    resource_id = state["resource_id"]

    if action == "NoAction":
        state["pre_action_snapshot"] = None
        state["action_executed"] = "NoAction"
        state["action_result"] = {"status": "skipped"}
        return state

    # 보고서 2.2.4: MED/HIGH는 60분 대기 — 지금 단계에서는 액션 보류만 표시
    if state.get("requires_approval"):
        state["pre_action_snapshot"] = None
        state["action_executed"] = None
        state["action_result"] = {"status": "pending_approval"}
        return state

    # 1. 액션 실행 전 스냅샷 (롤백용, 반드시 먼저 — 보고서 4.3절)
    state["pre_action_snapshot"] = take_snapshot(resource_type, resource_id)

    # 2. 실제 액션 실행
    result = execute_action(
        action, resource_type, resource_id,
        target_instance_type=state.get("target_instance_type"),
    )

    state["action_executed"] = action
    state["action_result"] = result
    return state


# ── 롤백 (QA Agent에서 import해서 사용) ──────────────────────────────────────

def _rollback_ec2(resource_id: str, snapshot: EC2Snapshot) -> dict:
    """
    입력: resource_id (EC2 인스턴스 ID), snapshot (EC2Snapshot)
    출력: {"status": "success"/"failed", ...} — 저장된 스냅샷으로 EC2 인스턴스를 복원
    """
    ec2 = _get_ec2_client()
    try:
        current = ec2.describe_instances(InstanceIds=[resource_id])
        current_instance = current["Reservations"][0]["Instances"][0]
        current_type = current_instance["InstanceType"]
        current_state = current_instance["State"]["Name"]

        if current_type != snapshot["instance_type"]:
            # 1. 현재 상태 확인 + 2. 상태별 분기 — modify_instance_attribute는
            #    stopped 상태에서만 가능하므로, pending 상태에서 곧바로
            #    instance_stopped waiter를 걸면 terminal failure가 난다.
            if current_state in ("pending", "running"):
                # pending은 아직 stop_instances를 받을 수 없으므로 running까지 대기
                ec2.get_waiter("instance_running").wait(InstanceIds=[resource_id])
                ec2.stop_instances(InstanceIds=[resource_id])
                ec2.get_waiter("instance_stopped").wait(InstanceIds=[resource_id])
            elif current_state == "stopping":
                ec2.get_waiter("instance_stopped").wait(InstanceIds=[resource_id])
            # current_state == "stopped"이면 별도 대기 없이 바로 3번으로 진행

            # 3. 타입 변경
            ec2.modify_instance_attribute(
                InstanceId=resource_id,
                InstanceType={"Value": snapshot["instance_type"]},
            )

            # 4. 스냅샷 시점 상태가 running이었으면 재시작
            if snapshot["state"] == "running":
                ec2.start_instances(InstanceIds=[resource_id])
        else:
            # 타입 변경이 필요 없는 경우 실행 상태만 복원
            if snapshot["state"] == "running":
                if current_state == "stopping":
                    ec2.get_waiter("instance_stopped").wait(InstanceIds=[resource_id])
                    current_state = "stopped"
                if current_state != "running":
                    ec2.start_instances(InstanceIds=[resource_id])
            elif snapshot["state"] == "stopped":
                ec2.stop_instances(InstanceIds=[resource_id])

        return {"status": "success"}
    except (ClientError, WaiterError) as exc:
        logger.error("EC2 롤백 실패 (%s): %s", resource_id, exc)
        return {"status": "failed", "error": str(exc)}


def _rollback_lambda(resource_id: str, snapshot: LambdaSnapshot) -> dict:
    """
    입력: resource_id (Lambda 함수명), snapshot (LambdaSnapshot)
    출력: {"status": "success"/"failed", ...}

    원래 동시성 설정이 -1(미설정)이면 delete_function_concurrency로 제거,
    아니면 원래 값으로 put_function_concurrency 복원.
    """
    lambda_client = _get_lambda_client()
    try:
        original = snapshot["reserved_concurrency"]
        if original == -1:
            lambda_client.delete_function_concurrency(FunctionName=resource_id)
        else:
            lambda_client.put_function_concurrency(
                FunctionName=resource_id,
                ReservedConcurrentExecutions=original,
            )
        return {"status": "success"}
    except ClientError as exc:
        logger.error("Lambda 롤백 실패 (%s): %s", resource_id, exc)
        return {"status": "failed", "error": str(exc)}


def _rollback_autoscaling(resource_id: str, snapshot: AutoScalingSnapshot) -> dict:
    """
    입력: resource_id (AutoScaling 그룹명), snapshot (AutoScalingSnapshot)
    출력: {"status": "success"/"failed", ...} — max_size/desired_capacity를 원복
    """
    asg_client = _get_autoscaling_client()
    try:
        asg_client.update_auto_scaling_group(
            AutoScalingGroupName=resource_id,
            MaxSize=snapshot["max_size"],
            DesiredCapacity=snapshot["desired_capacity"],
        )
        return {"status": "success"}
    except ClientError as exc:
        logger.error("AutoScaling 롤백 실패 (%s): %s", resource_id, exc)
        return {"status": "failed", "error": str(exc)}


def rollback_action(resource_type: str, resource_id: str, snapshot: dict | None) -> dict:
    """
    입력: resource_type, resource_id, snapshot (take_snapshot 결과, 없으면 None)
    출력: {"status": "success"/"failed"/"not_implemented", ...}

    QA Agent가 qa_passed=False일 때 호출.
    snapshot이 None이면(NoAction 등) 롤백할 게 없으므로 바로 success 처리.
    """
    if snapshot is None:
        return {"status": "success", "detail": "스냅샷 없음 — 롤백 불필요"}

    if resource_type == "EC2":
        return _rollback_ec2(resource_id, snapshot)  # type: ignore[arg-type]
    if resource_type == "Lambda":
        return _rollback_lambda(resource_id, snapshot)  # type: ignore[arg-type]
    if resource_type == "AutoScaling":
        return _rollback_autoscaling(resource_id, snapshot)  # type: ignore[arg-type]

    logger.warning("롤백 미구현 리소스 타입: %s", resource_type)
    return {"status": "not_implemented", "resource_type": resource_type}
