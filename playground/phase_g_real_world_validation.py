"""
playground/phase_g_real_world_validation.py

Phase G: 실제 AWS 환경에서 의도적으로 이상 상황을 만들어서, 탐지 파이프라인이
실제로 잡아내는지 기록으로 남긴다 (보고서/증빙용).

절차 (EC2, Lambda, AutoScaling 각각):
  1. before : 평상시 상태에서 탐지 실행 (정상으로 나와야 함)
  2. induce : 실제로 이상 상황을 만듦
  3. after  : 이상 상황이 CloudWatch에 반영된 뒤 탐지 실행 (이상으로 잡혀야 함)
  4. restore: 원상 복구 (AutoScaling capacity, EC2 부하 프로세스는 timeout으로 자동 종료)

EC2는 SSM(Systems Manager)으로 인스턴스 안에서 CPU 부하(yes 프로세스 2개, vCPU 수만큼)를
직접 실행시켜서 이상을 만든다. SSM을 쓰려면 인스턴스에 IAM 역할이 필요한데,
detection-test-ec2-ssm-role(AmazonSSMManagedInstanceCore 정책)을 만들어서 미리 붙여놨다
(참고: 이 프로젝트에서 EC2에 자연 발생한 network_in 스파이크도 이전에 별도로 한 번
기록해뒀음 — 부팅 직후 배경 트래픽 추정, anomaly_flag=True로 정확히 탐지됨).

전 과정을 playground/eval_outputs/phase_g_real_world_result.json에 저장하고,
사람이 읽기 쉬운 리포트를 콘솔에 출력한다.

[실행 방법]
  프로젝트 루트에서: python playground/phase_g_real_world_validation.py

⚠️ 실제 AWS 리소스에 다음 작업을 수행함:
  - EC2 인스턴스 안에서 SSM으로 CPU 부하 스크립트 실행 (최대 300초, 자동 종료)
  - Lambda 함수를 연속 호출 (비용 거의 없음)
  - AutoScaling 그룹의 desired capacity를 일시적으로 올렸다가 원상 복구 (t3.micro 몇 시간 미만 추가 비용)
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import boto3

from pipeline.cloudwatch_client import fetch_metrics
from pipeline.cost_estimator import estimate_cost_series
import pipeline.detection_agent as da

RESULT_PATH = PROJECT_ROOT / "playground" / "eval_outputs" / "phase_g_real_world_result.json"

EC2_INSTANCE_ID = "i-01c2e2f11cc1e0710"
LAMBDA_FUNCTION_NAME = "detection-test-lambda"
ASG_NAME = "detection-test-asg"


def _detect(resource_type: str, resource_id: str) -> dict:
    """실제 프로덕션 함수(_iforest_score, _zscore_check)로 지금 시점 탐지 실행."""
    usage = fetch_metrics(resource_type, resource_id)
    n = len(next(iter(usage.values()))) if usage else 0

    if n < da.MIN_POINTS_FOR_IFOREST:
        return {
            "n_points": n,
            "note": f"포인트 {n}개로 최소 기준({da.MIN_POINTS_FOR_IFOREST}) 미달 — 판단 보류",
        }

    z_max = da._zscore_max(usage)
    z_triggered = z_max > da.Z_SCORE_THRESHOLD
    iforest_score = da._iforest_score(resource_type, usage)
    iforest_triggered = iforest_score > da.IFOREST_THRESHOLD
    anomaly_flag = z_triggered or iforest_triggered

    return {
        "n_points": n,
        "anomaly_flag": anomaly_flag,
        "z_max": round(z_max, 4),
        "z_triggered": z_triggered,
        "iforest_score": round(iforest_score, 4),
        "iforest_triggered": iforest_triggered,
        "last_values": {k: v[-3:] for k, v in usage.items()},
    }


def _print_stage(label: str, result: dict) -> None:
    print(f"  [{label}] {json.dumps(result, ensure_ascii=False)}")


# ══════════════════════════════════════════════════════════════════════════
# 시나리오 1: EC2 CPU 부하 (SSM으로 인스턴스 안에서 직접 실행)
# ══════════════════════════════════════════════════════════════════════════

def run_ec2_scenario(n_vcpu: int = 2, duration_sec: int = 300, wait_sec: int = 300) -> dict:
    print("\n" + "=" * 78)
    print("시나리오 1: EC2 CPU 부하 (SSM)")
    print("=" * 78)

    before = _detect("EC2", EC2_INSTANCE_ID)
    _print_stage("before", before)

    ssm = boto3.client("ssm")
    info = ssm.describe_instance_information(
        Filters=[{"Key": "InstanceIds", "Values": [EC2_INSTANCE_ID]}]
    )
    if not info["InstanceInformationList"] or info["InstanceInformationList"][0]["PingStatus"] != "Online":
        note = "SSM에 인스턴스가 등록돼 있지 않음 — IAM 역할(detection-test-ec2-ssm-role) 연결 및 재부팅 필요"
        print(f"  → {note}")
        return {"scenario": "ec2_cpu_stress", "before": before, "after": None, "note": note}

    commands = [
        f'nohup bash -c "timeout {duration_sec} yes > /dev/null &" >/dev/null 2>&1'
        for _ in range(n_vcpu)
    ]
    print(f"  → SSM으로 CPU 부하 시작 ({n_vcpu}개 프로세스, 최대 {duration_sec}초)...")
    cmd_id = ssm.send_command(
        InstanceIds=[EC2_INSTANCE_ID],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": commands},
    )["Command"]["CommandId"]
    time.sleep(5)
    invocation = ssm.get_command_invocation(CommandId=cmd_id, InstanceId=EC2_INSTANCE_ID)
    print(f"  → 명령 실행 상태: {invocation['Status']}")

    print(f"  → CloudWatch 반영 대기 ({wait_sec}초)...")
    time.sleep(wait_sec)

    after = _detect("EC2", EC2_INSTANCE_ID)
    _print_stage("after", after)

    return {"scenario": "ec2_cpu_stress", "before": before, "after": after}


# ══════════════════════════════════════════════════════════════════════════
# 시나리오 2: Lambda 호출 폭증
# ══════════════════════════════════════════════════════════════════════════

def run_lambda_scenario() -> dict:
    print("\n" + "=" * 78)
    print("시나리오 2: Lambda 호출 폭증")
    print("=" * 78)

    before = _detect("Lambda", LAMBDA_FUNCTION_NAME)
    _print_stage("before", before)

    print("  → Lambda 100회 연속 호출 중...")
    lam = boto3.client("lambda")
    for i in range(100):
        lam.invoke(FunctionName=LAMBDA_FUNCTION_NAME, InvocationType="Event", Payload=b"{}")
    print("  → 호출 완료. CloudWatch 반영 대기 (2분)...")
    time.sleep(120)

    after = _detect("Lambda", LAMBDA_FUNCTION_NAME)
    _print_stage("after", after)

    return {"scenario": "lambda_invocation_burst", "before": before, "after": after}


# ══════════════════════════════════════════════════════════════════════════
# 시나리오 3: AutoScaling desired capacity 변경
# ══════════════════════════════════════════════════════════════════════════

def run_autoscaling_scenario(wait_sec: int = 600) -> dict:
    print("\n" + "=" * 78)
    print("시나리오 3: AutoScaling desired capacity 변경 (1 → 3 → 1)")
    print("=" * 78)

    before = _detect("AutoScaling", ASG_NAME)
    _print_stage("before", before)

    asg = boto3.client("autoscaling")
    print("  → desired capacity를 3으로 변경...")
    asg.update_auto_scaling_group(AutoScalingGroupName=ASG_NAME, DesiredCapacity=3, MaxSize=3)
    print(f"  → 인스턴스 기동 및 CloudWatch 반영 대기 ({wait_sec}초, 5분 구간이 완전히 "
          f"capacity=3으로 채워질 때까지 여유있게)...")
    time.sleep(wait_sec)

    after = _detect("AutoScaling", ASG_NAME)
    _print_stage("after", after)

    print("  → 원상 복구: desired capacity를 1로 되돌림...")
    asg.update_auto_scaling_group(AutoScalingGroupName=ASG_NAME, DesiredCapacity=1, MaxSize=2)

    return {"scenario": "autoscaling_capacity_change", "before": before, "after": after}


def main() -> None:
    result = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "detection_params": {
            "z_score_threshold": da.Z_SCORE_THRESHOLD,
            "iforest_threshold": da.IFOREST_THRESHOLD,
        },
        "ec2_prior_natural_finding": {
            "note": "SSM 부하 테스트 전, 실환경에서 자연 발생한 network_in 스파이크를 별도로 이미 기록함 "
                    "(부팅 직후 배경 트래픽 추정, anomaly_flag=True로 정확히 탐지됨)",
            "resource_id": EC2_INSTANCE_ID,
            "triggered_metric": "network_in",
            "z_max": 2.9128,
            "iforest_score": 0.4522,
        },
        "scenarios": [],
    }

    result["scenarios"].append(run_ec2_scenario())
    result["scenarios"].append(run_lambda_scenario())
    result["scenarios"].append(run_autoscaling_scenario())

    RESULT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(RESULT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print("\n" + "=" * 78)
    print("전체 요약")
    print("=" * 78)
    for s in result["scenarios"]:
        if not s.get("after"):
            print(f"  {s['scenario']}: after 없음 ({s.get('note', '알 수 없는 이유')})")
            continue
        before_flag = s["before"].get("anomaly_flag", "N/A(데이터부족)")
        after_flag = s["after"].get("anomaly_flag", "N/A(데이터부족)")
        verdict = "✅ 정상 판정" if before_flag in (False, "N/A(데이터부족)") and after_flag is True else "⚠️ 확인 필요"
        print(f"  {s['scenario']}: before={before_flag} → after={after_flag}  {verdict}")

    print(f"\n저장 완료: {RESULT_PATH}")


if __name__ == "__main__":
    main()
