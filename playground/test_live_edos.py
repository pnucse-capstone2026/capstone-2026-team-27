"""
playground/test_live_edos.py

EDOS(Economic Denial of Sustainability) 의심 실연동 테스트 (통합 자동화)

파일 하나만 실행하면 전체 테스트가 자동으로 진행됩니다:
  1. AutoScaling 그룹 존재 여부 확인 (없으면 자동 생성)
  2. 초기 스냅샷 저장 (현재 Desired/Min/Max)
  3. 인스턴스 급증 시뮬레이션 (Desired Capacity 증가)
  4. CloudWatch 메트릭 반영 대기
  5. 파이프라인 실행 및 이상 감지 확인
  6. ScaleDown 액션 확인 (risk_level=HIGH, 승인 필요)
  7. 롤백 및 원상복구

사전 조건:
  - .env에 AWS 자격증명 설정
  - PostgreSQL 체크포인터 연결 가능

실행:
  python playground/test_live_edos.py              # 전체 테스트 (약 15분)
  python playground/test_live_edos.py --quick      # 빠른 테스트 (1회 스파이크)
  python playground/test_live_edos.py --dry-run    # 실제 ScaleDown 없이 테스트
  python playground/test_live_edos.py --cleanup    # 리소스 정리 (ASG, LT, SG 삭제)

비용 참고:
  - t3.micro: 시간당 약 $0.0104 (ap-northeast-2)
  - 테스트 중 최대 5개 인스턴스 실행 시: 시간당 약 $0.05
"""

from __future__ import annotations

import argparse
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

from pipeline.orchestrator import assemble_resource
from pipeline.detection_agent import _build_initial_state
from pipeline.graph import build_approval_graph
from pipeline.checkpointer import get_postgres_checkpointer


# ── 설정 ──────────────────────────────────────────────────────────────────────
ASG_NAME = os.getenv("ASG_NAME", "detection-test-asg")
LAUNCH_TEMPLATE_NAME = "detection-test-lt"
SECURITY_GROUP_NAME = "detection-test-sg"

BASELINE_CAPACITY = 1          # 기준선 인스턴스 수
SPIKE_CAPACITY = 5             # 스파이크 인스턴스 수 (5배 급증)
SPIKE_ROUNDS = 3               # 스파이크 유지 라운드 (지속성 체크 통과용)
METRIC_WAIT_SECONDS = 300      # CloudWatch 메트릭 반영 대기 (5분)

# AMI ID (Amazon Linux 2023, ap-northeast-2)
DEFAULT_AMI_ID = "ami-0c2acfcb2ac4d02a0"


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


def get_default_vpc_and_subnet() -> tuple[str, str]:
    """기본 VPC와 서브넷 조회"""
    ec2 = boto3.client("ec2")

    # 기본 VPC
    vpcs = ec2.describe_vpcs(Filters=[{"Name": "is-default", "Values": ["true"]}])
    if not vpcs["Vpcs"]:
        raise Exception("기본 VPC를 찾을 수 없습니다")
    vpc_id = vpcs["Vpcs"][0]["VpcId"]

    # 서브넷
    subnets = ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])
    if not subnets["Subnets"]:
        raise Exception("서브넷을 찾을 수 없습니다")
    subnet_id = subnets["Subnets"][0]["SubnetId"]

    return vpc_id, subnet_id


def ensure_security_group(vpc_id: str) -> str:
    """보안 그룹 확인/생성"""
    ec2 = boto3.client("ec2")

    try:
        sgs = ec2.describe_security_groups(
            Filters=[{"Name": "group-name", "Values": [SECURITY_GROUP_NAME]}]
        )
        if sgs["SecurityGroups"]:
            return sgs["SecurityGroups"][0]["GroupId"]
    except Exception:
        pass

    # 생성
    response = ec2.create_security_group(
        GroupName=SECURITY_GROUP_NAME,
        Description="Detection test security group",
        VpcId=vpc_id,
    )
    return response["GroupId"]


def ensure_launch_template(sg_id: str) -> str:
    """Launch Template 확인/생성"""
    ec2 = boto3.client("ec2")

    try:
        templates = ec2.describe_launch_templates(
            LaunchTemplateNames=[LAUNCH_TEMPLATE_NAME]
        )
        if templates["LaunchTemplates"]:
            return templates["LaunchTemplates"][0]["LaunchTemplateId"]
    except Exception:
        pass

    # 생성
    response = ec2.create_launch_template(
        LaunchTemplateName=LAUNCH_TEMPLATE_NAME,
        VersionDescription="v1",
        LaunchTemplateData={
            "ImageId": DEFAULT_AMI_ID,
            "InstanceType": "t3.micro",
            "SecurityGroupIds": [sg_id],
            "TagSpecifications": [{
                "ResourceType": "instance",
                "Tags": [
                    {"Key": "Detection", "Value": "true"},
                    {"Key": "Name", "Value": "detection-test-instance"}
                ]
            }]
        }
    )
    return response["LaunchTemplate"]["LaunchTemplateId"]


def ensure_auto_scaling_group(subnet_id: str) -> dict:
    """AutoScaling 그룹 확인/생성"""
    autoscaling = boto3.client("autoscaling")

    # 존재 확인
    groups = autoscaling.describe_auto_scaling_groups(
        AutoScalingGroupNames=[ASG_NAME]
    )

    if groups["AutoScalingGroups"]:
        asg = groups["AutoScalingGroups"][0]
        return {
            "exists": True,
            "created": False,
            "name": ASG_NAME,
            "desired": asg["DesiredCapacity"],
            "min": asg["MinSize"],
            "max": asg["MaxSize"],
        }

    # 생성
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=ASG_NAME,
        LaunchTemplate={
            "LaunchTemplateName": LAUNCH_TEMPLATE_NAME,
            "Version": "$Latest"
        },
        MinSize=0,
        MaxSize=10,
        DesiredCapacity=BASELINE_CAPACITY,
        VPCZoneIdentifier=subnet_id,
        Tags=[
            {"Key": "Detection", "Value": "true", "PropagateAtLaunch": True},
            {"Key": "Name", "Value": ASG_NAME, "PropagateAtLaunch": True}
        ]
    )

    # 인스턴스 시작 대기
    print("  인스턴스 시작 대기 중 (최대 2분)...")
    for _ in range(24):
        time.sleep(5)
        groups = autoscaling.describe_auto_scaling_groups(
            AutoScalingGroupNames=[ASG_NAME]
        )
        if groups["AutoScalingGroups"]:
            instances = groups["AutoScalingGroups"][0].get("Instances", [])
            running = [i for i in instances if i["LifecycleState"] == "InService"]
            if len(running) >= BASELINE_CAPACITY:
                print(f"  인스턴스 {len(running)}개 실행 중")
                break

    return {
        "exists": True,
        "created": True,
        "name": ASG_NAME,
        "desired": BASELINE_CAPACITY,
        "min": 0,
        "max": 10,
    }


def get_asg_snapshot() -> dict:
    """AutoScaling 그룹 현재 상태 스냅샷"""
    autoscaling = boto3.client("autoscaling")

    groups = autoscaling.describe_auto_scaling_groups(
        AutoScalingGroupNames=[ASG_NAME]
    )

    if not groups["AutoScalingGroups"]:
        return None

    asg = groups["AutoScalingGroups"][0]
    instances = asg.get("Instances", [])

    return {
        "desired_capacity": asg["DesiredCapacity"],
        "min_size": asg["MinSize"],
        "max_size": asg["MaxSize"],
        "instance_count": len([i for i in instances if i["LifecycleState"] == "InService"]),
    }


def set_asg_capacity(desired: int, min_size: int = None, max_size: int = None) -> dict:
    """AutoScaling 그룹 용량 설정"""
    autoscaling = boto3.client("autoscaling")

    params = {
        "AutoScalingGroupName": ASG_NAME,
        "DesiredCapacity": desired,
    }
    if min_size is not None:
        params["MinSize"] = min_size
    if max_size is not None:
        params["MaxSize"] = max_size

    autoscaling.update_auto_scaling_group(**params)

    return {"status": "success", "desired": desired}


def wait_for_instances(target_count: int, timeout: int = 300) -> bool:
    """인스턴스가 목표 수에 도달할 때까지 대기"""
    autoscaling = boto3.client("autoscaling")
    start = time.time()

    while time.time() - start < timeout:
        groups = autoscaling.describe_auto_scaling_groups(
            AutoScalingGroupNames=[ASG_NAME]
        )
        if groups["AutoScalingGroups"]:
            instances = groups["AutoScalingGroups"][0].get("Instances", [])
            running = [i for i in instances if i["LifecycleState"] == "InService"]
            print(f"    현재 인스턴스: {len(running)}/{target_count}")
            if len(running) >= target_count:
                return True
        time.sleep(10)

    return False


def run_pipeline(dry_run: bool = False) -> dict:
    """파이프라인 실행"""
    resource = assemble_resource(ASG_NAME, "AutoScaling")
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

    thread_id = f"test-edos-{ASG_NAME}-{uuid.uuid4()}"
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


def cleanup_resources() -> None:
    """테스트 리소스 정리"""
    print_header("리소스 정리")

    autoscaling = boto3.client("autoscaling")
    ec2 = boto3.client("ec2")

    # 1. AutoScaling 그룹 삭제
    print("  AutoScaling 그룹 삭제 중...")
    try:
        # 먼저 인스턴스를 0으로
        autoscaling.update_auto_scaling_group(
            AutoScalingGroupName=ASG_NAME,
            MinSize=0,
            MaxSize=0,
            DesiredCapacity=0,
        )
        time.sleep(10)

        # 그룹 삭제
        autoscaling.delete_auto_scaling_group(
            AutoScalingGroupName=ASG_NAME,
            ForceDelete=True,
        )
        print(f"    {ASG_NAME} 삭제 완료")
    except Exception as e:
        print(f"    삭제 실패: {e}")

    # 2. Launch Template 삭제
    print("  Launch Template 삭제 중...")
    try:
        ec2.delete_launch_template(LaunchTemplateName=LAUNCH_TEMPLATE_NAME)
        print(f"    {LAUNCH_TEMPLATE_NAME} 삭제 완료")
    except Exception as e:
        print(f"    삭제 실패: {e}")

    # 3. Security Group 삭제 (잠시 대기 후)
    print("  Security Group 삭제 중 (30초 대기)...")
    time.sleep(30)
    try:
        sgs = ec2.describe_security_groups(
            Filters=[{"Name": "group-name", "Values": [SECURITY_GROUP_NAME]}]
        )
        if sgs["SecurityGroups"]:
            ec2.delete_security_group(GroupId=sgs["SecurityGroups"][0]["GroupId"])
            print(f"    {SECURITY_GROUP_NAME} 삭제 완료")
    except Exception as e:
        print(f"    삭제 실패: {e}")

    print("\n  정리 완료!")


def run_full_test(quick: bool = False, dry_run: bool = False) -> dict:
    """전체 테스트 실행"""
    results = {
        "start_time": datetime.now(timezone.utc).isoformat(),
        "steps": [],
        "success": False,
    }

    total_steps = 6 if quick else 8

    # ── Step 1: 환경 확인 ──
    print_step(1, total_steps, "환경 확인")

    aws_info = check_aws_connection()
    if not aws_info["connected"]:
        print(f"  AWS 연결 실패: {aws_info['error']}")
        return results

    print(f"  AWS Account: {aws_info['account']}")
    print(f"  IAM User: {aws_info['user']}")

    # VPC, 서브넷, 보안 그룹, Launch Template 확인/생성
    vpc_id, subnet_id = get_default_vpc_and_subnet()
    print(f"  VPC: {vpc_id}")
    print(f"  Subnet: {subnet_id}")

    sg_id = ensure_security_group(vpc_id)
    print(f"  Security Group: {sg_id}")

    lt_id = ensure_launch_template(sg_id)
    print(f"  Launch Template: {lt_id}")

    asg_info = ensure_auto_scaling_group(subnet_id)
    if asg_info["created"]:
        print(f"  AutoScaling 그룹 생성됨: {ASG_NAME}")
    else:
        print(f"  AutoScaling 그룹 확인됨: {ASG_NAME}")

    results["steps"].append({"name": "환경 확인", "status": "success"})

    # ── Step 2: 초기 스냅샷 ──
    print_step(2, total_steps, "초기 스냅샷 저장")

    initial_snapshot = get_asg_snapshot()
    print(f"  현재 상태: {initial_snapshot}")
    results["initial_snapshot"] = initial_snapshot
    results["steps"].append({"name": "초기 스냅샷", "status": "success"})

    # ── Step 3: 기준선 설정 ──
    print_step(3, total_steps, f"기준선 설정 (Desired={BASELINE_CAPACITY})")

    set_asg_capacity(BASELINE_CAPACITY, min_size=0, max_size=10)
    print(f"  Desired Capacity: {BASELINE_CAPACITY}")

    # 인스턴스 안정화 대기
    countdown(60, "기준선 메트릭 반영 대기")
    results["steps"].append({"name": "기준선 설정", "status": "success"})

    if quick:
        # 빠른 테스트: 1회 스파이크만
        print_step(4, total_steps, f"스파이크 생성 (Desired={SPIKE_CAPACITY})")

        set_asg_capacity(SPIKE_CAPACITY)
        print(f"  Desired Capacity: {SPIKE_CAPACITY}")

        print("  인스턴스 시작 대기 중...")
        wait_for_instances(SPIKE_CAPACITY, timeout=180)

        countdown(60, "CloudWatch 메트릭 반영 대기")

        print_step(5, total_steps, "파이프라인 실행")
        pipeline_result = run_pipeline(dry_run)

    else:
        # 전체 테스트: 3라운드 스파이크 (지속성 체크 통과)
        print_step(4, total_steps, f"스파이크 생성 ({SPIKE_ROUNDS}라운드)")

        for round_num in range(1, SPIKE_ROUNDS + 1):
            print(f"\n  === 라운드 {round_num}/{SPIKE_ROUNDS} ===")

            set_asg_capacity(SPIKE_CAPACITY)
            print(f"  Desired Capacity: {SPIKE_CAPACITY}")

            print("  인스턴스 시작 대기 중...")
            wait_for_instances(SPIKE_CAPACITY, timeout=180)

            if round_num < SPIKE_ROUNDS:
                countdown(METRIC_WAIT_SECONDS, f"라운드 {round_num} 메트릭 반영 대기")

        results["steps"].append({"name": "스파이크 생성", "status": "success"})

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
        "requires_approval": state.get("requires_approval"),
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
    print(f"    승인 필요: {state.get('requires_approval', False)}")
    print(f"    승인 대기 중: {pipeline_result['interrupted']}")
    print(f"    실행된 액션: {state.get('action_executed', 'N/A')}")
    print(f"    QA 통과: {state.get('qa_passed', 'N/A')}")

    results["steps"].append({"name": "파이프라인 실행", "status": "success"})

    # ── Step 7/8: 롤백 ──
    step_num = 6 if quick else 7
    print_step(step_num, total_steps, "원상복구 (롤백)")

    if initial_snapshot:
        set_asg_capacity(
            initial_snapshot["desired_capacity"],
            min_size=initial_snapshot["min_size"],
            max_size=initial_snapshot["max_size"],
        )
        print(f"  Desired Capacity 복원: {initial_snapshot['desired_capacity']}")

    # 최종 상태 확인
    final_snapshot = get_asg_snapshot()
    print(f"  최종 상태: {final_snapshot}")

    results["steps"].append({"name": "롤백", "status": "success"})
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
        help="실제 ScaleDown 없이 테스트"
    )
    parser.add_argument(
        "--cleanup", action="store_true",
        help="테스트 리소스 정리 (ASG, Launch Template, Security Group 삭제)"
    )
    args = parser.parse_args()

    if args.cleanup:
        cleanup_resources()
        return

    print_header("EDOS 의심 실연동 테스트")

    if args.quick:
        print("  모드: 빠른 테스트 (약 5분)")
        print("  주의: 지속성 체크로 인해 이상 감지가 안 될 수 있음")
    else:
        print("  모드: 전체 테스트 (약 20분)")
        print("  3라운드 스파이크로 지속성 체크 통과 시도")

    print(f"  Dry Run: {args.dry_run}")
    print(f"  AutoScaling Group: {ASG_NAME}")
    print(f"  기준선: {BASELINE_CAPACITY} → 스파이크: {SPIKE_CAPACITY} (5배 급증)")
    print(f"  시작 시간: {datetime.now(timezone.utc).isoformat()}")

    try:
        results = run_full_test(quick=args.quick, dry_run=args.dry_run)
    except KeyboardInterrupt:
        print("\n\n테스트 중단됨 (Ctrl+C)")
        print("리소스 정리가 필요하면: python playground/test_live_edos.py --cleanup")
        return
    except Exception as e:
        print(f"\n\n테스트 실패: {e}")
        import traceback
        traceback.print_exc()
        print("\n리소스 정리가 필요하면: python playground/test_live_edos.py --cleanup")
        return

    # 최종 결과 출력
    print_header("테스트 완료")

    if results.get("pipeline_result"):
        pr = results["pipeline_result"]
        detected = pr.get("anomaly_flag", False)

        if detected:
            print("  결과: 이상 감지 성공!")
            print(f"    - 유형: {pr.get('anomaly_type')}")
            print(f"    - 액션: {pr.get('selected_action')}")
            print(f"    - 위험 수준: {pr.get('risk_level')}")
            print(f"    - 승인 필요: {pr.get('requires_approval')}")

            if pr.get("anomaly_type") == "risk_security":
                print("\n  EDOS 의심 패턴 감지됨!")
        else:
            print("  결과: 이상 미감지")
            if args.quick:
                print("    -> --quick 모드에서는 지속성 체크로 인해 미감지될 수 있음")
                print("    -> 전체 테스트(--quick 없이)를 실행해보세요")

    print(f"\n  종료 시간: {results.get('end_time', 'N/A')}")
    print("\n  리소스 정리가 필요하면: python playground/test_live_edos.py --cleanup")


if __name__ == "__main__":
    main()
