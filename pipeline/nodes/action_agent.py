"""
Action Agent
============
node_contracts.md Step 4 기준.

입력 (읽는 필드):
  - selected_action, resource_id, resource_type, requires_approval

출력 (채우는 필드):
  - pre_action_snapshot, action_executed, action_result

설계:
  - 지금은 EC2(Stop, Resize)만 구현. Lambda/S3/AutoScaling/RDS는
    NotImplementedAction 으로 표시해두고 추후 각자 확장.
  - boto3 클라이언트는 모듈 레벨에서 만들지 않고 함수 안에서 생성
    (테스트 시 monkeypatch/mock 주입하기 쉽도록).
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

from schema.state import PipelineState, EC2Snapshot

logger = logging.getLogger(__name__)


def _get_ec2_client():
    """boto3 EC2 클라이언트 생성. 함수 내부에서 생성해야 테스트 시 mock 주입이 쉽다."""
    return boto3.client("ec2", region_name=os.getenv("AWS_DEFAULT_REGION"))


# ── 스냅샷 ────────────────────────────────────────────────────────────────────

def _take_ec2_snapshot(resource_id: str) -> EC2Snapshot:
    """EC2 인스턴스의 현재 상태를 저장 (롤백용)."""
    ec2 = _get_ec2_client()
    resp = ec2.describe_instances(InstanceIds=[resource_id])
    instance = resp["Reservations"][0]["Instances"][0]
    snapshot: EC2Snapshot = {
        "instance_type": instance["InstanceType"],
        "state": instance["State"]["Name"],
        "security_group_ids": [sg["GroupId"] for sg in instance.get("SecurityGroups", [])],
    }
    return snapshot


def take_snapshot(resource_type: str, resource_id: str) -> dict | None:
    """리소스 타입별 스냅샷 dispatcher. 미구현 타입은 None 리턴."""
    if resource_type == "EC2":
        return _take_ec2_snapshot(resource_id)

    logger.warning("스냅샷 미구현 리소스 타입: %s — 빈 스냅샷 처리", resource_type)
    return None


# ── 액션 실행 ─────────────────────────────────────────────────────────────────

def _execute_ec2_stop(resource_id: str) -> dict:
    ec2 = _get_ec2_client()
    try:
        resp = ec2.stop_instances(InstanceIds=[resource_id])
        return {"status": "success", "raw": resp.get("StoppingInstances", [])}
    except ClientError as exc:
        logger.error("EC2 Stop 실패 (%s): %s", resource_id, exc)
        return {"status": "failed", "error": str(exc)}


def _execute_ec2_resize(resource_id: str, target_instance_type: str = "t3.small") -> dict:
    """
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


def execute_action(action: str, resource_type: str, resource_id: str) -> dict:
    """선택된 액션을 실제로 실행하는 dispatcher."""
    if action == "NoAction":
        return {"status": "skipped"}

    if resource_type == "EC2":
        if action == "Stop":
            return _execute_ec2_stop(resource_id)
        if action == "Resize":
            return _execute_ec2_resize(resource_id)
        # Stop+Schedule, ScaleDown, Block, Throttle 등은 추후 구현
        logger.warning("EC2 액션 미구현: %s", action)
        return {"status": "not_implemented", "action": action}

    # Lambda / S3 / RDS / AutoScaling 은 추후 각자 확장
    logger.warning("리소스 타입 미구현: %s (action=%s)", resource_type, action)
    return {"status": "not_implemented", "resource_type": resource_type, "action": action}


def action_node(state: PipelineState) -> PipelineState:
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
    result = execute_action(action, resource_type, resource_id)

    state["action_executed"] = action
    state["action_result"] = result
    return state


# ── 롤백 (QA Agent에서 import해서 사용) ──────────────────────────────────────

def _rollback_ec2(resource_id: str, snapshot: EC2Snapshot) -> dict:
    """저장된 스냅샷으로 EC2 인스턴스를 복원."""
    ec2 = _get_ec2_client()
    try:
        current = ec2.describe_instances(InstanceIds=[resource_id])
        current_instance = current["Reservations"][0]["Instances"][0]
        current_type = current_instance["InstanceType"]

        if current_type != snapshot["instance_type"]:
            ec2.stop_instances(InstanceIds=[resource_id])
            waiter = ec2.get_waiter("instance_stopped")
            waiter.wait(InstanceIds=[resource_id])
            ec2.modify_instance_attribute(
                InstanceId=resource_id,
                InstanceType={"Value": snapshot["instance_type"]},
            )

        if snapshot["state"] == "running":
            ec2.start_instances(InstanceIds=[resource_id])
        elif snapshot["state"] == "stopped":
            ec2.stop_instances(InstanceIds=[resource_id])

        return {"status": "success"}
    except (ClientError, WaiterError) as exc:
        logger.error("EC2 롤백 실패 (%s): %s", resource_id, exc)
        return {"status": "failed", "error": str(exc)}


def rollback_action(resource_type: str, resource_id: str, snapshot: dict | None) -> dict:
    """
    QA Agent가 qa_passed=False일 때 호출.
    snapshot이 None이면(NoAction 등) 롤백할 게 없으므로 바로 success 처리.
    """
    if snapshot is None:
        return {"status": "success", "detail": "스냅샷 없음 — 롤백 불필요"}

    if resource_type == "EC2":
        return _rollback_ec2(resource_id, snapshot)  # type: ignore[arg-type]

    logger.warning("롤백 미구현 리소스 타입: %s", resource_type)
    return {"status": "not_implemented", "resource_type": resource_type}
