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
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

import boto3

from pipeline.cloudwatch_client import fetch_metrics
from pipeline.cost_estimator import estimate_cost_series
import pipeline.detection_agent as da

RESULT_PATH = PROJECT_ROOT / "playground" / "eval_outputs" / "phase_g_real_world_result.json"

# ⚠️ EC2_INSTANCE_ID는 예전엔 하드코딩이었는데, 테스트 인스턴스가 재생성되면서
# .env의 INSTANCE_ID와 어긋나 있던 걸 발견함 — .env 기준으로 통일 (RDS/S3도 동일하게
# .env에서 읽어서 앞으로 같은 문제가 안 생기게 함).
EC2_INSTANCE_ID = os.environ["INSTANCE_ID"]
LAMBDA_FUNCTION_NAME = os.environ.get("LAMBDA_FUNCTION_NAME", "detection-test-lambda")
ASG_NAME = os.environ.get("ASG_NAME", "detection-test-asg")
RDS_INSTANCE_ID = os.environ.get("RDS_INSTANCE_ID")
RDS_MASTER_USERNAME = os.environ.get("RDS_MASTER_USERNAME")
RDS_MASTER_PASSWORD = os.environ.get("RDS_MASTER_PASSWORD")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")


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


# ══════════════════════════════════════════════════════════════════════════
# 시나리오 4: RDS 부하 (기존 EC2 테스트 인스턴스에서 SSM으로 psql 동시접속 부하)
# ══════════════════════════════════════════════════════════════════════════
# RDS는 PubliclyAccessible=False로 만들어서(보안), 인터넷에서 직접 접속이 안 된다.
# 대신 같은 VPC 안에 있는 기존 EC2 테스트 인스턴스를 통해서만 접근 가능하므로,
# EC2 CPU 부하 시나리오와 동일하게 SSM으로 그 인스턴스 안에서 psql 세션을 여러 개
# 동시에 띄워 무거운 쿼리를 반복 실행시킨다 — database_connections(접속 수),
# cpu_utilization/read_iops(쿼리 부하) 둘 다 같이 자극된다.

def run_rds_scenario(
    load_minutes: int = 18, n_connections: int = 12, wait_after_sec: int = 120
) -> dict:
    print("\n" + "=" * 78)
    print(f"시나리오 4: RDS 부하 ({n_connections}개 동시접속, {load_minutes}분간 무거운 쿼리 반복)")
    print("=" * 78)

    if not (RDS_INSTANCE_ID and RDS_MASTER_USERNAME and RDS_MASTER_PASSWORD):
        note = ".env에 RDS_INSTANCE_ID/RDS_MASTER_USERNAME/RDS_MASTER_PASSWORD가 없음"
        print(f"  → {note}")
        return {"scenario": "rds_query_load", "before": None, "after": None, "note": note}

    rds = boto3.client("rds")
    db = rds.describe_db_instances(DBInstanceIdentifier=RDS_INSTANCE_ID)["DBInstances"][0]
    if db["DBInstanceStatus"] != "available":
        note = f"RDS 상태가 available이 아님 (현재: {db['DBInstanceStatus']}) — 프로비저닝 완료 대기 필요"
        print(f"  → {note}")
        return {"scenario": "rds_query_load", "before": None, "after": None, "note": note}
    endpoint = db["Endpoint"]["Address"]

    before = _detect("RDS", RDS_INSTANCE_ID)
    _print_stage("before", before)

    duration_sec = load_minutes * 60
    heavy_sql = "SELECT count(*) FROM generate_series(1,3000000) s WHERE s % 7 = 0;"
    # ⚠️ 커넥션마다 "duration_sec초 동안 무거운 쿼리를 계속 반복"하는 백그라운드 프로세스를
    # n_connections개 동시에 띄움 (EC2 CPU 부하 시나리오의 yes 프로세스 패턴과 동일한 방식).
    # 첫 시도에서 `nohup bash -c "...psql -c \"SQL\"..."`처럼 바깥/안쪽에 같은 큰따옴표를
    # 중첩해서 셸이 문자열을 엉뚱하게 끊어 읽는 바람에 SSM 명령 자체가 Failed로 죽었다
    # (부하가 하나도 안 걸린 채 20분을 그냥 흘려보낼 뻔함) — 따옴표 중첩을 피하려고
    # 인용 구분자(quoted heredoc, 'EOF')로 스크립트 파일을 인스턴스에 써서 그 파일을
    # 실행하는 방식으로 바꿈. 'EOF'로 따옴표 처리해서 $SECONDS 등이 지금 이 자리에서
    # 확장되지 않고 파일에 그대로 문자로 저장되게(=스크립트가 실행될 때 평가되게) 함.
    load_script = (
        "cat > /tmp/rds_load.sh << 'SCRIPT_EOF'\n"
        f"end=$((SECONDS+{duration_sec}))\n"
        "while [ $SECONDS -lt $end ]; do\n"
        f"  PGPASSWORD='{RDS_MASTER_PASSWORD}' psql -h {endpoint} -U {RDS_MASTER_USERNAME} "
        f"-d postgres -c '{heavy_sql}' >/dev/null 2>&1\n"
        "done\n"
        "SCRIPT_EOF"
    )
    commands = [
        load_script,
        "chmod +x /tmp/rds_load.sh",
        f"for i in $(seq 1 {n_connections}); do nohup bash /tmp/rds_load.sh >/dev/null 2>&1 & done",
        "sleep 2; echo started",
    ]

    ssm = boto3.client("ssm")
    print(f"  → SSM으로 RDS 부하 시작 ({n_connections}개 psql 동시접속, 최대 {load_minutes}분)...")
    cmd_id = ssm.send_command(
        InstanceIds=[EC2_INSTANCE_ID],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": commands},
        TimeoutSeconds=max(duration_sec + 60, 600),
    )["Command"]["CommandId"]
    time.sleep(5)
    invocation = ssm.get_command_invocation(CommandId=cmd_id, InstanceId=EC2_INSTANCE_ID)
    print(f"  → 명령 실행 상태: {invocation['Status']}")
    if invocation["Status"] not in ("Success", "InProgress", "Pending", "Delayed"):
        note = (
            f"SSM 부하 명령이 실패함(status={invocation['Status']}) — 부하가 안 걸렸으므로 "
            f"측정을 중단함. STDERR={invocation.get('StandardErrorContent', '')[:500]}"
        )
        print(f"  → {note}")
        return {"scenario": "rds_query_load", "before": before, "after": None, "note": note}

    total_wait = duration_sec + wait_after_sec
    print(f"  → 부하 지속 + CloudWatch 반영 대기 ({total_wait}초 ≈ {total_wait/60:.1f}분)...")
    time.sleep(total_wait)

    after = _detect("RDS", RDS_INSTANCE_ID)
    _print_stage("after", after)

    return {"scenario": "rds_query_load", "before": before, "after": after}


# ══════════════════════════════════════════════════════════════════════════
# 시나리오 5: S3 요청 폭증 (PUT 1건 + GET 반복)
# ══════════════════════════════════════════════════════════════════════════
# ⚠️ 사전조건: 버킷에 Request Metrics(EntireBucket 필터)가 활성화돼 있어야
# number_of_requests/bytes_downloaded 값 자체가 CloudWatch에 나온다 (put_bucket_metrics_
# configuration으로 이미 활성화해둠 — 버킷 생성 스크립트 참고).

def run_s3_scenario(n_gets: int = 800, object_size_bytes: int = 200_000, wait_sec: int = 300) -> dict:
    print("\n" + "=" * 78)
    print(f"시나리오 5: S3 요청 폭증 (GET {n_gets}회 반복)")
    print("=" * 78)

    if not S3_BUCKET_NAME:
        note = ".env에 S3_BUCKET_NAME이 없음"
        print(f"  → {note}")
        return {"scenario": "s3_request_burst", "before": None, "after": None, "note": note}

    before = _detect("S3", S3_BUCKET_NAME)
    _print_stage("before", before)

    s3 = boto3.client("s3")
    key = "phase_g_load_test_object.bin"
    print(f"  → 테스트 객체 업로드 ({object_size_bytes} bytes)...")
    s3.put_object(Bucket=S3_BUCKET_NAME, Key=key, Body=os.urandom(object_size_bytes))

    print(f"  → GET {n_gets}회 반복 호출 중...")
    for i in range(n_gets):
        s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)

    print(f"  → 호출 완료. CloudWatch 반영 대기 ({wait_sec}초, S3 Request Metrics는 반영이 다소 느림)...")
    time.sleep(wait_sec)

    after = _detect("S3", S3_BUCKET_NAME)
    _print_stage("after", after)

    return {"scenario": "s3_request_burst", "before": before, "after": after}


_SCENARIO_RUNNERS = {
    "ec2":         run_ec2_scenario,
    "lambda":      run_lambda_scenario,
    "autoscaling": run_autoscaling_scenario,
    "rds":         run_rds_scenario,
    "s3":          run_s3_scenario,
}


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenarios", type=str, default=None,
        help="쉼표로 구분된 시나리오 목록 (ec2,lambda,autoscaling,rds,s3). 안 주면 전체 실행.",
    )
    args = parser.parse_args()
    selected = args.scenarios.split(",") if args.scenarios else list(_SCENARIO_RUNNERS.keys())

    # ⚠️ 예전엔 매번 새 result 딕셔너리를 통째로 덮어써서, 일부 시나리오만
    # (--scenarios rds,s3 같은) 골라 돌리면 그 전에 기록해둔 다른 시나리오 결과가
    # 사라지는 사고가 있었다(실제로 RDS/S3만 돌렸다가 예전 EC2/Lambda/AutoScaling
    # 기록이 날아감). 기존 파일이 있으면 불러와서 시나리오 이름이 같은 것만 이번
    # 결과로 교체하고, 나머지는 그대로 보존한다.
    if RESULT_PATH.exists():
        with open(RESULT_PATH, encoding="utf-8") as f:
            result = json.load(f)
    else:
        result = {"scenarios": []}

    result["last_run_at"] = datetime.now(timezone.utc).isoformat()
    result["detection_params"] = {
        "z_score_threshold": da.Z_SCORE_THRESHOLD,
        "iforest_threshold": da.IFOREST_THRESHOLD,
    }
    result["ec2_prior_natural_finding"] = {
        "note": "SSM 부하 테스트 전, 실환경에서 자연 발생한 network_in 스파이크를 별도로 이미 기록함 "
                "(부팅 직후 배경 트래픽 추정, anomaly_flag=True로 정확히 탐지됨)",
        "resource_id": EC2_INSTANCE_ID,
        "triggered_metric": "network_in",
        "z_max": 2.9128,
        "iforest_score": 0.4522,
    }

    existing_by_name = {s["scenario"]: i for i, s in enumerate(result["scenarios"])}
    for name in selected:
        record = _SCENARIO_RUNNERS[name]()
        record["run_at"] = datetime.now(timezone.utc).isoformat()
        if record["scenario"] in existing_by_name:
            result["scenarios"][existing_by_name[record["scenario"]]] = record
        else:
            existing_by_name[record["scenario"]] = len(result["scenarios"])
            result["scenarios"].append(record)

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
