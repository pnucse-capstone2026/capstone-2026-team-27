"""
playground/autoscaling_edos_traffic_trial.py (v4)

AutoScaling EDoS 탐지를, capacity(결과)가 아니라 트래픽(원인) 지표로 다시 검증한다.

배경 (2026-09-12, 같은 날 실측 3회 - v1/v2/v3 - 전부 recall 0%):
  - z-score: masking effect(스파이크 자체가 창의 평균/표준편차를 끌어올림)
  - IForest: MOCK_SEED_BUFFER_FROZEN으로 실측 데이터를 전혀 학습 안 함 -> 해제
  - IForest: 윈도우 자기참조 정규화 -> 버퍼 기준 절대 정규화로 교체
  - 그래도 실패: group_desired_capacity가 "순간 인스턴스 개수"라는 이산적 지표라
    값의 가짓수가 극히 적어(우리 실험 규모 1~4대) 통계적 방법이 다룰 재료가 부족함.
    반면 같은 방식(z-score+IForest, 절대임계값 없음)으로 S3는 recall 100% 성공했는데,
    S3 지표(bytes_downloaded/number_of_requests)는 일정 기간 누적되는 합계라 값의
    폭이 넓고 연속값에 가까움 - 이게 진짜 차이였다.

이번 실험 설계:
  - EDoS의 진짜 원인은 "트래픽"이므로, capacity는 전부 고정(1대)하고 건드리지 않는다
    (capacity 변수를 아예 제거해서 "트래픽 지표가 단독으로 유효한가"만 순수하게 검증).
  - ALB + 리소스별 전용 Target Group(포트로 구분)을 붙이고, 인스턴스엔 user-data로
    최소 HTTP 서버(python3 -m http.server)를 띄운다.
  - 이 스크립트 자신이 각 ALB 포트에 실제 HTTP 요청을 쏜다:
    normal = 낮고 꾸준한 요청량 유지, anomaly = baseline 후 요청량을 크게 폭증.
  - ALB의 RequestCount(Sum, 누적) CloudWatch 지표를 raw로 가져와 기존 capacity
    지표와 병합해서 detect_both()에 넘긴다(schema/state.py에 request_count 추가,
    detection_agent.py Z_SCORE_TARGET_METRICS에도 추가해뒀음).

[실행 방법]
  python playground/autoscaling_edos_traffic_trial.py --setup --n-anomaly 5 --n-normal 5
  python playground/autoscaling_edos_traffic_trial.py --run --n-anomaly 5 --n-normal 5
  python playground/autoscaling_edos_traffic_trial.py --teardown --n-anomaly 5 --n-normal 5
"""

from __future__ import annotations

SCRIPT_VERSION = "4"

import argparse
import base64
import json
import logging
import os
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(PROJECT_ROOT / "playground") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "playground"))

os.environ.setdefault("AWS_PROFILE", "default")

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

import boto3
import requests

from ec2_lambda_repeated_trial import clopper_pearson_ci, compute_metrics, detect_both

# v5(2026-09-13): 탐지만 보던 것에서 전체 파이프라인(분류→결정→조치[WAF 포함]→QA)까지
# 확장 - batch_pipeline_replay.py와 동일하게 LangGraph checkpointer 없이 노드 함수를
# 직접 순서대로 호출한다(승인 게이트도 마찬가지로 --bypass-approval로 타이밍/E2E
# 검증 목적에 한해 우회).
from pipeline.detection_agent import detection_node, _build_initial_state
from pipeline.classification_agent import classification_node
from pipeline.decision_agent import decision_node
from pipeline.action_agent import action_node
from pipeline.QA_agent import qa_node
from pipeline.logging_agent import logging_node
from pipeline.cost_estimator import estimate_cost_series

# ── 설정값 ────────────────────────────────────────────────────────────────────

ASG_PREFIX = "detection-traffic-asg"
LAUNCH_TEMPLATE_NAME = "detection-traffic-lt"
ALB_NAME = "detection-traffic-alb"
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2")

CAPACITY = 1  # 이번 실험은 capacity를 아예 고정 - 트래픽 지표만 단독 검증
LISTENER_PORT_BASE = 8001  # anomaly-0..4 -> 8001..8005, normal-0..4 -> 8011..8015
NORMAL_PORT_BASE = 8011

BASELINE_RPS = 1.0      # 정상/베이스라인 구간 초당 요청 수
SPIKE_RPS = 50.0        # anomaly 구간 초당 요청 수 (폭증)

VCPU_PER_INSTANCE = 2

LOG_DIR = PROJECT_ROOT / "playground" / "eval_outputs" / "logs"
RESULT_DIR = PROJECT_ROOT / "playground" / "eval_outputs"

logger = logging.getLogger("autoscaling_edos_traffic_trial")


def _setup_logging() -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = LOG_DIR / f"autoscaling_edos_traffic_trial_{ts}.log"
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return log_path


# ── VPC / 보안그룹 / Launch Template ──────────────────────────────────────────

def _get_default_vpc_subnets() -> tuple[str, list[str]]:
    ec2 = boto3.client("ec2", region_name=AWS_REGION)
    vpcs = ec2.describe_vpcs(Filters=[{"Name": "is-default", "Values": ["true"]}])
    if not vpcs["Vpcs"]:
        raise RuntimeError("기본 VPC가 없습니다.")
    vpc_id = vpcs["Vpcs"][0]["VpcId"]
    subnets = ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])
    if not subnets["Subnets"]:
        raise RuntimeError("서브넷이 없습니다.")
    subnet_ids = [s["SubnetId"] for s in subnets["Subnets"][:3]]
    return vpc_id, subnet_ids


def _ensure_security_groups(vpc_id: str) -> tuple[str, str]:
    """(alb_sg_id, instance_sg_id) 반환. ALB SG: 8001~8020에서 인바운드 전체 허용.
    인스턴스 SG: ALB SG로부터 80포트만 허용(직접 인터넷 노출 안 함)."""
    ec2 = boto3.client("ec2", region_name=AWS_REGION)

    def _find_or_create(name: str, desc: str) -> str:
        existing = ec2.describe_security_groups(
            Filters=[{"Name": "group-name", "Values": [name]}, {"Name": "vpc-id", "Values": [vpc_id]}]
        )
        if existing["SecurityGroups"]:
            return existing["SecurityGroups"][0]["GroupId"]
        resp = ec2.create_security_group(GroupName=name, Description=desc, VpcId=vpc_id)
        return resp["GroupId"]

    alb_sg = _find_or_create("detection-traffic-alb-sg", "ALB SG for EDoS traffic trial")
    inst_sg = _find_or_create("detection-traffic-instance-sg", "Instance SG for EDoS traffic trial")

    try:
        ec2.authorize_security_group_ingress(
            GroupId=alb_sg,
            IpPermissions=[{
                "IpProtocol": "tcp", "FromPort": 8001, "ToPort": 8020,
                "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
            }],
        )
    except ec2.exceptions.ClientError as e:
        if "InvalidPermission.Duplicate" not in str(e):
            raise

    try:
        ec2.authorize_security_group_ingress(
            GroupId=inst_sg,
            IpPermissions=[{
                "IpProtocol": "tcp", "FromPort": 80, "ToPort": 80,
                "UserIdGroupPairs": [{"GroupId": alb_sg}],
            }],
        )
    except ec2.exceptions.ClientError as e:
        if "InvalidPermission.Duplicate" not in str(e):
            raise

    return alb_sg, inst_sg


def _get_latest_amazon_linux_ami() -> str:
    ec2 = boto3.client("ec2", region_name=AWS_REGION)
    response = ec2.describe_images(
        Owners=["amazon"],
        Filters=[{"Name": "name", "Values": ["al2023-ami-*-x86_64"]}, {"Name": "state", "Values": ["available"]}],
    )
    images = sorted(response["Images"], key=lambda x: x["CreationDate"], reverse=True)
    if not images:
        raise RuntimeError("Amazon Linux AMI를 찾을 수 없습니다.")
    return images[0]["ImageId"]


_USER_DATA = """#!/bin/bash
mkdir -p /var/www
echo "ok" > /var/www/index.html
cd /var/www
cat > /etc/systemd/system/simplehttp.service <<'EOF'
[Unit]
Description=Simple HTTP server for EDoS traffic trial
After=network.target

[Service]
WorkingDirectory=/var/www
ExecStart=/usr/bin/python3 -m http.server 80
Restart=always

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable --now simplehttp.service
"""


def _ensure_launch_template(ec2, instance_sg: str) -> str:
    try:
        response = ec2.describe_launch_templates(LaunchTemplateNames=[LAUNCH_TEMPLATE_NAME])
        return response["LaunchTemplates"][0]["LaunchTemplateId"]
    except ec2.exceptions.ClientError as e:
        if "InvalidLaunchTemplateName.NotFoundException" not in str(e):
            raise

    ami_id = _get_latest_amazon_linux_ami()
    user_data_b64 = base64.b64encode(_USER_DATA.encode("utf-8")).decode("ascii")
    logger.info("Launch Template 생성: %s (AMI: %s, user-data로 HTTP 서버 설치)", LAUNCH_TEMPLATE_NAME, ami_id)
    response = ec2.create_launch_template(
        LaunchTemplateName=LAUNCH_TEMPLATE_NAME,
        LaunchTemplateData={
            "ImageId": ami_id, "InstanceType": "t3.micro",
            "SecurityGroupIds": [instance_sg],
            "UserData": user_data_b64,
        },
    )
    return response["LaunchTemplate"]["LaunchTemplateId"]


# ── ALB / 타겟그룹 / 리스너 ───────────────────────────────────────────────────

# 2026-09-13 버그 수정: ALB ARN과 TargetGroup ARN은 "/"로 나뉘는 조각 수가 다르다
# (ALB: "loadbalancer/app/이름/id" 4조각, TG: "targetgroup/이름/id" 3조각). "뒤에서
# 3조각 자르기"를 둘 다 똑같이 했더니 ALB는 우연히 맞았지만 TG는 계정/리전이 포함된
# ARN 전체가 그대로 남아, CloudWatch가 이 dimension과 매칭되는 데이터를 못 찾아서
# RequestCount가 항상 0으로 조회됐다(v4 1차 실행에서 발견). ":"로 나눠 마지막
# 조각(리소스 부분)만 뽑고, LoadBalancer 차원은 "loadbalancer/" 접두어를 벗겨서
# "app/이름/id"로(CloudWatch가 이 형태를 기대함), TargetGroup 차원은 "targetgroup/"
# 접두어를 그대로 남겨서(CloudWatch가 이 형태를 기대함) 만든다.
def _alb_dimension_value(alb_arn: str) -> str:
    resource = alb_arn.split(":")[-1]  # "loadbalancer/app/이름/id"
    return resource.split("/", 1)[1]   # "app/이름/id"


def _tg_dimension_value(tg_arn: str) -> str:
    return tg_arn.split(":")[-1]  # "targetgroup/이름/id" - 그대로가 정답


def _ensure_alb(vpc_id: str, subnet_ids: list[str], alb_sg: str) -> tuple[str, str, str]:
    """(alb_arn, alb_dns, alb_id_suffix) 반환."""
    elb = boto3.client("elbv2", region_name=AWS_REGION)
    existing = elb.describe_load_balancers(Names=[ALB_NAME]) if _alb_exists(elb) else {"LoadBalancers": []}
    if existing["LoadBalancers"]:
        lb = existing["LoadBalancers"][0]
    else:
        logger.info("ALB 생성: %s", ALB_NAME)
        resp = elb.create_load_balancer(
            Name=ALB_NAME, Subnets=subnet_ids, SecurityGroups=[alb_sg],
            Scheme="internet-facing", Type="application", IpAddressType="ipv4",
        )
        lb = resp["LoadBalancers"][0]
        # ALB가 active 될 때까지 대기
        waiter = elb.get_waiter("load_balancer_available")
        waiter.wait(LoadBalancerArns=[lb["LoadBalancerArn"]])
        lb = elb.describe_load_balancers(LoadBalancerArns=[lb["LoadBalancerArn"]])["LoadBalancers"][0]

    alb_arn = lb["LoadBalancerArn"]
    alb_dns = lb["DNSName"]
    alb_id_suffix = _alb_dimension_value(alb_arn)
    return alb_arn, alb_dns, alb_id_suffix


def _alb_exists(elb) -> bool:
    try:
        elb.describe_load_balancers(Names=[ALB_NAME])
        return True
    except elb.exceptions.LoadBalancerNotFoundException:
        return False


def _ensure_target_group_and_listener(vpc_id: str, alb_arn: str, name: str, port: int) -> tuple[str, str]:
    """(target_group_arn, tg_id_suffix) 반환. 리스너(외부 port)도 같이 만든다."""
    elb = boto3.client("elbv2", region_name=AWS_REGION)
    existing = elb.describe_target_groups(Names=[name]) if _tg_exists(elb, name) else {"TargetGroups": []}
    if existing["TargetGroups"]:
        tg = existing["TargetGroups"][0]
    else:
        resp = elb.create_target_group(
            Name=name, Protocol="HTTP", Port=80, VpcId=vpc_id, TargetType="instance",
            HealthCheckPath="/", HealthCheckIntervalSeconds=10, HealthyThresholdCount=2,
        )
        tg = resp["TargetGroups"][0]
        logger.info("Target Group 생성: %s (listener port=%d)", name, port)

    tg_arn = tg["TargetGroupArn"]

    listeners = elb.describe_listeners(LoadBalancerArn=alb_arn)["Listeners"]
    if not any(l["Port"] == port for l in listeners):
        elb.create_listener(
            LoadBalancerArn=alb_arn, Protocol="HTTP", Port=port,
            DefaultActions=[{"Type": "forward", "TargetGroupArn": tg_arn}],
        )
        logger.info("리스너 생성: port=%d -> %s", port, name)

    tg_id_suffix = _tg_dimension_value(tg_arn)
    return tg_arn, tg_id_suffix


def _tg_exists(elb, name: str) -> bool:
    try:
        elb.describe_target_groups(Names=[name])
        return True
    except elb.exceptions.TargetGroupNotFoundException:
        return False


# ── ASG 준비 / 정리 ───────────────────────────────────────────────────────────

def _asg_name(prefix: str, idx: int) -> str:
    return f"{ASG_PREFIX}-{prefix}-{idx}"


def _ensure_asg(autoscaling, name: str, lt_id: str, subnet_ids: list[str], tg_arn: str) -> None:
    existing = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[name])
    if existing["AutoScalingGroups"]:
        autoscaling.update_auto_scaling_group(
            AutoScalingGroupName=name, DesiredCapacity=CAPACITY, MinSize=1, MaxSize=CAPACITY)
    else:
        logger.info("ASG 생성: %s", name)
        autoscaling.create_auto_scaling_group(
            AutoScalingGroupName=name,
            LaunchTemplate={"LaunchTemplateId": lt_id, "Version": "$Latest"},
            MinSize=1, MaxSize=CAPACITY, DesiredCapacity=CAPACITY,
            VPCZoneIdentifier=",".join(subnet_ids),
            TargetGroupARNs=[tg_arn],
            HealthCheckType="ELB", HealthCheckGracePeriod=120,
            Tags=[
                {"Key": "Name", "Value": name, "PropagateAtLaunch": True},
                {"Key": "Purpose", "Value": "detection-traffic-trial", "PropagateAtLaunch": True},
            ],
        )


def setup_all(n_anomaly: int, n_normal: int) -> None:
    vpc_id, subnet_ids = _get_default_vpc_subnets()
    alb_sg, inst_sg = _ensure_security_groups(vpc_id)
    ec2 = boto3.client("ec2", region_name=AWS_REGION)
    lt_id = _ensure_launch_template(ec2, inst_sg)
    alb_arn, alb_dns, _ = _ensure_alb(vpc_id, subnet_ids, alb_sg)
    autoscaling = boto3.client("autoscaling", region_name=AWS_REGION)

    resource_ports: dict[str, int] = {}
    for i in range(n_anomaly):
        name = _asg_name("anomaly", i)
        port = LISTENER_PORT_BASE + i
        tg_arn, _ = _ensure_target_group_and_listener(vpc_id, alb_arn, f"tg-anomaly-{i}", port)
        _ensure_asg(autoscaling, name, lt_id, subnet_ids, tg_arn)
        resource_ports[name] = port
    for i in range(n_normal):
        name = _asg_name("normal", i)
        port = NORMAL_PORT_BASE + i
        tg_arn, _ = _ensure_target_group_and_listener(vpc_id, alb_arn, f"tg-normal-{i}", port)
        _ensure_asg(autoscaling, name, lt_id, subnet_ids, tg_arn)
        resource_ports[name] = port

    manifest = {"alb_dns": alb_dns, "ports": resource_ports}
    manifest_path = RESULT_DIR / "autoscaling_edos_traffic_manifest.json"
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    logger.info("설정 완료. ALB DNS=%s, 매니페스트=%s", alb_dns, manifest_path)
    logger.info("인스턴스가 헬스체크를 통과하려면(user-data 실행 시간 포함) 2~3분 정도 걸릴 수 있습니다.")


def teardown_all(n_anomaly: int, n_normal: int) -> None:
    autoscaling = boto3.client("autoscaling", region_name=AWS_REGION)
    elb = boto3.client("elbv2", region_name=AWS_REGION)

    for label, count in (("anomaly", n_anomaly), ("normal", n_normal)):
        for i in range(count):
            name = _asg_name(label, i)
            try:
                autoscaling.update_auto_scaling_group(AutoScalingGroupName=name, MinSize=0, DesiredCapacity=0)
                autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name, ForceDelete=True)
                logger.info("ASG 삭제: %s", name)
            except Exception as exc:
                logger.warning("ASG 삭제 실패(%s): %s", name, exc)

    try:
        lbs = elb.describe_load_balancers(Names=[ALB_NAME])["LoadBalancers"]
        if lbs:
            alb_arn = lbs[0]["LoadBalancerArn"]
            for listener in elb.describe_listeners(LoadBalancerArn=alb_arn)["Listeners"]:
                elb.delete_listener(ListenerArn=listener["ListenerArn"])
            elb.delete_load_balancer(LoadBalancerArn=alb_arn)
            logger.info("ALB 삭제: %s", ALB_NAME)
    except Exception as exc:
        logger.warning("ALB 삭제 실패: %s", exc)

    time.sleep(15)  # target group은 ALB 완전 삭제 후에나 지워짐
    for label, count in (("anomaly", n_anomaly), ("normal", n_normal)):
        for i in range(count):
            tg_name = f"tg-{label}-{i}"
            try:
                tgs = elb.describe_target_groups(Names=[tg_name])["TargetGroups"]
                if tgs:
                    elb.delete_target_group(TargetGroupArn=tgs[0]["TargetGroupArn"])
                    logger.info("Target Group 삭제: %s", tg_name)
            except Exception as exc:
                logger.warning("Target Group 삭제 실패(%s): %s", tg_name, exc)


# ── 트래픽 생성 ───────────────────────────────────────────────────────────────

def _traffic_loop(url: str, rps_getter, stop_event: threading.Event, label: str, rep: int) -> None:
    """rps_getter()가 현재 목표 rps를 반환 - 실행 중에 baseline/spike로 바뀔 수 있게 함수로 받는다."""
    session = requests.Session()
    while not stop_event.is_set():
        rps = max(0.1, rps_getter())
        interval = 1.0 / rps
        try:
            session.get(url, timeout=3)
        except Exception:
            pass  # 인스턴스가 아직 헬스체크 전이거나 일시적 실패 - 트래픽 생성 자체는 계속
        stop_event.wait(interval)


# ── 메트릭 조회 ───────────────────────────────────────────────────────────────

def _fetch_alb_request_count(alb_id_suffix: str, tg_id_suffix: str, end_time: datetime,
                              n_points: int = 30, period_seconds: int = 300) -> list[float]:
    cw = boto3.client("cloudwatch", region_name=AWS_REGION)
    start_time = end_time - timedelta(seconds=n_points * period_seconds)
    resp = cw.get_metric_data(
        MetricDataQueries=[{
            "Id": "req",
            "MetricStat": {
                "Metric": {
                    "Namespace": "AWS/ApplicationELB", "MetricName": "RequestCount",
                    "Dimensions": [
                        {"Name": "LoadBalancer", "Value": alb_id_suffix},
                        {"Name": "TargetGroup", "Value": tg_id_suffix},
                    ],
                },
                "Period": period_seconds, "Stat": "Sum",
            },
            "ReturnData": True,
        }],
        StartTime=start_time, EndTime=end_time, ScanBy="TimestampAscending",
    )
    row = resp["MetricDataResults"][0]
    observed = list(zip(row["Timestamps"], row["Values"]))
    expected_times = [start_time + timedelta(seconds=i * period_seconds) for i in range(n_points)]
    half_period = period_seconds / 2
    filled = []
    for expected_ts in expected_times:
        match = next((v for ts, v in observed if abs((ts - expected_ts).total_seconds()) < half_period), 0.0)
        filled.append(match)
    return filled


def _fetch_capacity_metrics(asg_name: str, end_time: datetime,
                             n_points: int = 30, period_seconds: int = 300) -> dict[str, list[float]]:
    from pipeline.cloudwatch_client import METRIC_SPEC, _build_dimensions
    cw = boto3.client("cloudwatch", region_name=AWS_REGION)
    start_time = end_time - timedelta(seconds=n_points * period_seconds)
    metric_keys = list(METRIC_SPEC["AutoScaling"].keys())
    dimensions = _build_dimensions("AutoScaling", asg_name)
    queries = []
    for i, metric_key in enumerate(metric_keys):
        namespace, cw_metric_name, stat = METRIC_SPEC["AutoScaling"][metric_key]
        queries.append({
            "Id": f"m{i}",
            "MetricStat": {
                "Metric": {"Namespace": namespace, "MetricName": cw_metric_name, "Dimensions": dimensions},
                "Period": period_seconds, "Stat": stat,
            },
            "ReturnData": True,
        })
    resp = cw.get_metric_data(MetricDataQueries=queries, StartTime=start_time, EndTime=end_time, ScanBy="TimestampAscending")
    results_by_id = {r["Id"]: r for r in resp["MetricDataResults"]}
    expected_times = [start_time + timedelta(seconds=i * period_seconds) for i in range(n_points)]
    half_period = period_seconds / 2
    metrics: dict[str, list[float]] = {}
    for i, metric_key in enumerate(metric_keys):
        row = results_by_id.get(f"m{i}")
        if row is None or not row.get("Timestamps"):
            metrics[metric_key] = [0.0] * n_points
            continue
        observed = list(zip(row["Timestamps"], row["Values"]))
        filled = []
        for expected_ts in expected_times:
            match = next((v for ts, v in observed if abs((ts - expected_ts).total_seconds()) < half_period), 0.0)
            filled.append(match)
        metrics[metric_key] = filled
    return metrics


def detect(asg_name: str, alb_id_suffix: str, tg_id_suffix: str) -> dict:
    end_time = datetime.now(timezone.utc)
    metrics = _fetch_capacity_metrics(asg_name, end_time)
    metrics["request_count"] = _fetch_alb_request_count(alb_id_suffix, tg_id_suffix, end_time)
    return detect_both("AutoScaling", asg_name, usage=metrics)


def run_full_pipeline_from_metrics(asg_name: str, raw_metrics: dict, bypass_approval: bool) -> dict:
    """v5(2026-09-13): 이미 조회해둔 raw_metrics로 detection부터 logging까지 전체
    파이프라인을 그대로 흘린다(batch_pipeline_replay.py와 동일한 패턴 - LangGraph
    checkpointer 없이 노드를 직접 순서대로 호출). AutoScaling+risk_security+ScaleDown이면
    decision_node가 apply_waf=True를 세팅하고, action_node가 실제 WAF Rate-based Rule을
    적용한다(대상 ALB는 action_agent가 ASG의 TargetGroupARNs로 자동 조회 - 우리가
    따로 안 넘겨줘도 됨, get_alb_arn_for_asg 참고)."""
    result: dict = {}
    metrics = dict(raw_metrics)
    if "cost" not in metrics:
        metrics["cost"] = estimate_cost_series("AutoScaling", asg_name, metrics)

    state = _build_initial_state({"resource_id": asg_name, "resource_type": "AutoScaling", "raw_metrics": metrics})

    state = detection_node(state)
    result["anomaly_flag"] = bool(state["anomaly_flag"])
    result["triggered_metrics"] = state.get("triggered_metrics")
    if not state["anomaly_flag"]:
        result["stopped_at"] = "detection"
        return result

    state = classification_node(state)
    result["anomaly_type"] = state.get("anomaly_type")
    result["matched_rule_id"] = state.get("matched_rule_id")
    if not state.get("anomaly_type"):
        result["stopped_at"] = "classification"
        return result

    state = decision_node(state)
    result["selected_action"] = state.get("selected_action")
    result["risk_level"] = state.get("risk_level")
    result["requires_approval"] = bool(state.get("requires_approval"))
    result["apply_waf"] = state.get("apply_waf")

    result["approval_bypassed"] = False
    if state.get("requires_approval"):
        if bypass_approval:
            # [측정/검증 전용] risk_security는 항상 HIGH라 실제 운영에서는 승인이
            # 필요하다. 승인 대기는 무한정이라 자동 실행 중엔 측정 불가하므로,
            # WAF/QA까지 실제로 동작하는지 검증하는 목적에 한해서만 우회한다.
            state["requires_approval"] = False
            result["approval_bypassed"] = True
        else:
            result["stopped_at"] = "approval_gate"
            return result

    state = action_node(state)
    result["action_executed"] = state.get("action_executed")
    result["action_result"] = state.get("action_result")

    state = qa_node(state)
    result["qa_passed"] = state.get("qa_passed")
    result["sla_check_result"] = state.get("sla_check_result")
    result["rollback_count"] = state.get("rollback_count", 0)

    try:
        state = logging_node(state)
    except Exception as exc:
        result["logging_error"] = str(exc)

    result["stopped_at"] = None
    return result


# ── 시행 실행 ─────────────────────────────────────────────────────────────────

def run_anomaly_trial(asg_name: str, rep: int, port: int, alb_dns: str, alb_id_suffix: str, tg_id_suffix: str,
                       baseline_minutes: int, spike_minutes: int, metric_wait_sec: int,
                       bypass_approval: bool = False) -> dict:
    t0 = time.time()
    url = f"http://{alb_dns}:{port}/"
    current_rps = [BASELINE_RPS]
    stop_event = threading.Event()
    traffic_thread = threading.Thread(
        target=_traffic_loop, args=(url, lambda: current_rps[0], stop_event, "anomaly", rep), daemon=True)
    traffic_thread.start()
    logger.info("[anomaly rep=%d] %s baseline rps=%.1f로 %d분 유지", rep, asg_name, BASELINE_RPS, baseline_minutes)
    try:
        time.sleep(baseline_minutes * 60)
        before = detect(asg_name, alb_id_suffix, tg_id_suffix)
        logger.info("[anomaly rep=%d] before or_gate=%s", rep, before["production"]["or_gate"])

        current_rps[0] = SPIKE_RPS
        logger.info("[anomaly rep=%d] rps=%.1f로 폭증, %d분 유지...", rep, SPIKE_RPS, spike_minutes)
        time.sleep(spike_minutes * 60)

        logger.info("[anomaly rep=%d] 메트릭 반영 대기 %d초...", rep, metric_wait_sec)
        time.sleep(metric_wait_sec)

        after = detect(asg_name, alb_id_suffix, tg_id_suffix)
        logger.info("[anomaly rep=%d] or_gate=%s (z=%s IF=%s) request_count 마지막=%s",
                    rep, after["production"]["or_gate"], after["production"]["zscore_persistent"],
                    after["production"]["iforest_triggered"], after["raw_metrics"]["request_count"][-3:])

        pipeline_result = None
        if after["production"]["or_gate"]:
            pipeline_result = run_full_pipeline_from_metrics(asg_name, after["raw_metrics"], bypass_approval)
            logger.info(
                "[anomaly rep=%d] 파이프라인: anomaly_type=%s action=%s risk=%s approval_bypassed=%s "
                "apply_waf=%s action_result=%s qa_passed=%s",
                rep, pipeline_result.get("anomaly_type"), pipeline_result.get("selected_action"),
                pipeline_result.get("risk_level"), pipeline_result.get("approval_bypassed"),
                pipeline_result.get("apply_waf"), pipeline_result.get("action_result"),
                pipeline_result.get("qa_passed"),
            )

        return {
            "rep": rep, "resource": asg_name, "label": "anomaly",
            "before": before, "after": after,
            "detected_production": bool(after["production"]["or_gate"]),
            "detected_iforest_only": bool(after["production"]["iforest_triggered"]),
            "detected_zscore_only": bool(after["production"]["zscore_persistent"]),
            "pipeline_result": pipeline_result,
            "elapsed_sec": round(time.time() - t0, 1),
        }
    except Exception as exc:
        logger.error("[anomaly rep=%d] 실패: %s\n%s", rep, exc, traceback.format_exc())
        return {"rep": rep, "resource": asg_name, "label": "anomaly", "error": str(exc),
                "detected_production": None, "detected_iforest_only": None, "detected_zscore_only": None}
    finally:
        stop_event.set()


def run_normal_trial(asg_name: str, rep: int, port: int, alb_dns: str, alb_id_suffix: str, tg_id_suffix: str,
                      baseline_minutes: int, spike_minutes: int, metric_wait_sec: int,
                      bypass_approval: bool = False) -> dict:
    t0 = time.time()
    url = f"http://{alb_dns}:{port}/"
    stop_event = threading.Event()
    traffic_thread = threading.Thread(
        target=_traffic_loop, args=(url, lambda: BASELINE_RPS, stop_event, "normal", rep), daemon=True)
    traffic_thread.start()
    logger.info("[normal rep=%d] %s rps=%.1f 계속 유지", rep, asg_name, BASELINE_RPS)
    try:
        time.sleep(baseline_minutes * 60)
        before = detect(asg_name, alb_id_suffix, tg_id_suffix)
        logger.info("[normal rep=%d] before or_gate=%s", rep, before["production"]["or_gate"])

        time.sleep(spike_minutes * 60 + metric_wait_sec)

        after = detect(asg_name, alb_id_suffix, tg_id_suffix)
        logger.info("[normal rep=%d] or_gate=%s (z=%s IF=%s)",
                    rep, after["production"]["or_gate"], after["production"]["zscore_persistent"],
                    after["production"]["iforest_triggered"])

        pipeline_result = None
        if after["production"]["or_gate"]:  # 오탐(FP)이면 실제로 뭘 했을지도 같이 기록
            pipeline_result = run_full_pipeline_from_metrics(asg_name, after["raw_metrics"], bypass_approval)
            logger.warning("[normal rep=%d] 오탐 발생 - 파이프라인이 실제로 조치를 시도함: %s",
                            rep, pipeline_result)

        return {
            "rep": rep, "resource": asg_name, "label": "normal",
            "before": before, "after": after,
            "detected_production": bool(after["production"]["or_gate"]),
            "detected_iforest_only": bool(after["production"]["iforest_triggered"]),
            "detected_zscore_only": bool(after["production"]["zscore_persistent"]),
            "pipeline_result": pipeline_result,
            "elapsed_sec": round(time.time() - t0, 1),
        }
    except Exception as exc:
        logger.error("[normal rep=%d] 실패: %s\n%s", rep, exc, traceback.format_exc())
        return {"rep": rep, "resource": asg_name, "label": "normal", "error": str(exc),
                "detected_production": None, "detected_iforest_only": None, "detected_zscore_only": None}
    finally:
        stop_event.set()


# ── 메인 ──────────────────────────────────────────────────────────────────────

def result_filename(n_normal: int, n_anomaly: int) -> Path:
    date_str = datetime.now().strftime("%Y%m%d")
    return RESULT_DIR / f"autoscaling_edos_traffic_trial__n{n_normal}-{n_anomaly}_scriptv{SCRIPT_VERSION}_{date_str}.json"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--setup", action="store_true")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--teardown", action="store_true")
    parser.add_argument("--n-anomaly", type=int, default=5)
    parser.add_argument("--n-normal", type=int, default=5)
    parser.add_argument("--baseline-minutes", type=int, default=150)
    parser.add_argument("--spike-minutes", type=int, default=35,
                        help="anomaly의 트래픽 폭증 유지 시간(sustained_fraction 지속성 조건 고려)")
    parser.add_argument("--metric-wait-sec", type=int, default=300)
    parser.add_argument("--bypass-approval", action="store_true",
                        help="탐지 성공 시 분류/결정/조치(WAF 포함)/QA까지 전체 파이프라인을 실행하고, "
                             "risk_security(HIGH)라 걸리는 승인 게이트를 검증 목적으로만 우회한다. "
                             "안 주면 탐지까지만 하고 승인 대기 단계에서 멈춘다(액션/WAF 실행 안 됨).")
    args = parser.parse_args()

    log_path = _setup_logging()
    logger.info("로그 파일: %s (SCRIPT_VERSION=%s)", log_path, SCRIPT_VERSION)

    if args.teardown:
        teardown_all(args.n_anomaly, args.n_normal)
        return

    if args.setup:
        setup_all(args.n_anomaly, args.n_normal)
        return

    if not args.run:
        parser.print_help()
        return

    manifest_path = RESULT_DIR / "autoscaling_edos_traffic_manifest.json"
    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)
    alb_dns = manifest["alb_dns"]
    ports = manifest["ports"]

    elb = boto3.client("elbv2", region_name=AWS_REGION)
    alb_arn = elb.describe_load_balancers(Names=[ALB_NAME])["LoadBalancers"][0]["LoadBalancerArn"]
    alb_id_suffix = _alb_dimension_value(alb_arn)

    def _tg_suffix(name: str) -> str:
        tg = elb.describe_target_groups(Names=[name])["TargetGroups"][0]
        return _tg_dimension_value(tg["TargetGroupArn"])

    anomaly_asgs = [_asg_name("anomaly", i) for i in range(args.n_anomaly)]
    normal_asgs = [_asg_name("normal", i) for i in range(args.n_normal)]

    logger.info("=== anomaly %d개 + normal %d개, 총 %d개 시행 동시 병렬 시작 (트래픽 기반) ===",
                args.n_anomaly, args.n_normal, args.n_anomaly + args.n_normal)

    results = []
    total_workers = args.n_anomaly + args.n_normal
    with ThreadPoolExecutor(max_workers=total_workers) as executor:
        futures = []
        for i in range(args.n_anomaly):
            name = anomaly_asgs[i]
            futures.append(executor.submit(
                run_anomaly_trial, name, i, ports[name], alb_dns, alb_id_suffix, _tg_suffix(f"tg-anomaly-{i}"),
                args.baseline_minutes, args.spike_minutes, args.metric_wait_sec, args.bypass_approval))
        for i in range(args.n_normal):
            name = normal_asgs[i]
            futures.append(executor.submit(
                run_normal_trial, name, i, ports[name], alb_dns, alb_id_suffix, _tg_suffix(f"tg-normal-{i}"),
                args.baseline_minutes, args.spike_minutes, args.metric_wait_sec, args.bypass_approval))
        for future in as_completed(futures):
            results.append(future.result())

    results.sort(key=lambda r: (r["label"], r["rep"]))

    metrics = {
        "production(detection_node 실제 방식)": compute_metrics(results, "detected_production"),
        "iforest_only": compute_metrics(results, "detected_iforest_only"),
        "zscore_only": compute_metrics(results, "detected_zscore_only"),
    }
    for name, m in metrics.items():
        c = m["confusion_matrix"]
        logger.info("[%s] TP=%d TN=%d FP=%d FN=%d / accuracy=%s recall=%s",
                    name, c["TP"], c["TN"], c["FP"], c["FN"],
                    f"{m['accuracy']:.1%}" if m["accuracy"] is not None else "N/A",
                    f"{m['recall']:.1%}" if m["recall"] is not None else "N/A")
        if m["recall_ci_95_clopper_pearson"]:
            lo, hi = m["recall_ci_95_clopper_pearson"]
            logger.info("    recall 95%% CI = [%.1f%%, %.1f%%]", lo * 100, hi * 100)

    out_path = result_filename(args.n_normal, args.n_anomaly)
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({
            "script_version": SCRIPT_VERSION,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "params": {
                "baseline_minutes": args.baseline_minutes, "spike_minutes": args.spike_minutes,
                "baseline_rps": BASELINE_RPS, "spike_rps": SPIKE_RPS,
                "n_normal": args.n_normal, "n_anomaly": args.n_anomaly,
            },
            "metrics": metrics,
            "trials": results,
        }, f, ensure_ascii=False, indent=2)
    logger.info("결과 저장: %s", out_path)
    logger.warning("테스트 후 반드시 정리하세요: --teardown --n-anomaly %d --n-normal %d", args.n_anomaly, args.n_normal)


if __name__ == "__main__":
    main()
