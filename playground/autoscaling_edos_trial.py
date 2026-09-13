"""
playground/autoscaling_edos_trial.py

AutoScaling EDoS 시나리오를 anomaly n회 + normal n회 반복 실행해서
TP/TN/FP/FN, accuracy, recall(+ Clopper-Pearson 95% CI)을 계산한다.

설계:
- AutoScaling Group을 여러 개(N_ANOMALY + N_NORMAL) 생성해서 병렬로 테스트한다.
  같은 ASG에 여러 시행을 걸면 CloudWatch 지표가 합쳐져서 독립 시행이 안 된다.
- anomaly 시행: desired capacity를 급격히 증가시켜 EDoS 시뮬레이션
- normal 시행: 현재 capacity 유지하며 정상 상태 확인
- 탐지 기준: detection_node가 실제로 쓰는 방식(production) + 옛 방식(teammate_compat)
  둘 다 기록

[실행 방법]
  1단계(ASG 준비, 최초 1회):
    python playground/autoscaling_edos_trial.py --setup --n-anomaly 5 --n-normal 8
  2단계(반복 실험 실행):
    python playground/autoscaling_edos_trial.py --run --n-anomaly 5 --n-normal 8
  3단계(정리 - 반드시 실행):
    python playground/autoscaling_edos_trial.py --teardown --n-anomaly 5 --n-normal 8

[생성 파일]
  - 결과: playground/eval_outputs/autoscaling_edos_trial__capacity{N}_wait{W}s_n{정상}-{이상}_scriptv{V}_{YYYYMMDD}.json
  - 로그: playground/eval_outputs/logs/autoscaling_edos_trial_{timestamp}.log
"""

from __future__ import annotations

SCRIPT_VERSION = "3"
# v3 (2026-09-12): recall 0% 실측 후 원인 3가지(z-score masking effect,
#     MOCK_SEED_BUFFER_FROZEN으로 IForest가 실측 데이터를 전혀 학습 안 함, 정상
#     baseline이 완벽히 flat이라 IsolationForest가 배울 분산이 없음)를 진단하고,
#     세 번째 원인을 합성 노이즈 시뮬레이션(playground/analyze_iforest_edos_real_data.py)
#     으로 검증(recall 0%->100%)한 뒤, 실제 AWS 재실험으로 다시 확인하기 위해:
#   (1) normal/anomaly baseline capacity를 1로 고정하지 않고 1<->2로 주기적으로
#       흔들어(jitter) 진짜 자연스러운 변동을 만든다 (ASG max_size도 그만큼 올림).
#   (2) MOCK_SEED_BUFFER_FROZEN=False로 전환(별도 커밋, detection_agent.py)해서
#       IForest가 이번엔 실측 데이터로 진짜 학습하게 한다.
# v1 (2026-09-09): 최초 작성.
# v2 (2026-09-09): 아래 7가지 수정.
#   (1) 판정 로직 교체 — _zscore_max/_iforest_score(옛 phase_g 헬퍼)는 detection_node와
#       다르다. phase_g는 2026-08-25 작성이고, 그 뒤 08-28 persistence, 09-08 절대임계값
#       체크가 detection_node에 들어갔지만 그 헬퍼엔 반영되지 않았다. production 방식으로
#       바꾸고 옛 방식은 teammate_compat으로 함께 기록(과거 수치 비교용).
#   (2) cost 지표 반영 — fetch는 group_desired_capacity/group_in_service_instances만 준다.
#       그런데 Z_SCORE_TARGET_METRICS 중 AutoScaling이 가진 지표는 cost 하나뿐이라,
#       cost가 없으면 z-score가 평가할 지표가 0개가 되어 완전히 무력화된다.
#       estimate_cost_series로 채운다(IForest의 cost feature mask 불일치도 함께 해소).
#   (3) enable_metrics_collection 추가 — 이걸 안 켜면 GroupDesiredCapacity/
#       GroupInServiceInstances가 CloudWatch에 아예 안 올라가서 전부 0으로 조회된다
#       (S3의 Request Metrics와 같은 성격).
#   (4) 베이스라인 워밍업(--baseline-minutes) 추가 — ASG 생성 직후엔 창(30포인트=2.5시간)의
#       앞부분이 0으로 채워져서, 어떤 변화든 "0에서 급증"으로 보여 탐지율이 부풀려진다.
#   (5) vCPU 쿼터 + 프리티어 사전 검증 — 기본값(anomaly capacity 5 x 5개 + normal 8개)은
#       33대 = 66vCPU로 기본 쿼터(32)를 훨씬 넘는다. 넘으면 ASG는 조용히 launch만
#       실패해서(update_auto_scaling_group은 성공 반환) desired만 오르고 in_service는
#       안 오르는 "다른 패턴"이 측정된다.
#   (6) --teardown 추가 — v1은 정리 경로가 없어서 ASG가 capacity=1로 영구 잔존한다
#       (13대 x 24시간이면 프리티어 750시간을 약 2.4일 만에 소진).
#   (7) capacity 복구를 try/finally로 — v1은 detect에서 예외가 나면 capacity가 올라간
#       채로 남는다. 그리고 스파이크가 실제로 반영됐는지(in_service 도달 여부) 검증해서
#       결과에 기록한다.

import argparse
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
    sys.stdout.reconfigure(encoding="utf-8")  # Windows 콘솔 기본 cp949가 em-dash 등에서 죽는 문제 방지

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(PROJECT_ROOT / "playground") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "playground"))

os.environ.setdefault("AWS_PROFILE", "default")

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

import boto3

from pipeline.cloudwatch_client import METRIC_SPEC, _build_dimensions
# 판정 로직·통계는 다른 반복시행 스크립트와 공유한다(한 곳에서만 관리)
from ec2_lambda_repeated_trial import clopper_pearson_ci, compute_metrics, detect_both

# ── 설정값 ────────────────────────────────────────────────────────────────────

ASG_PREFIX = "detection-trial-asg"
LAUNCH_TEMPLATE_NAME = "detection-trial-lt"
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2")

NORMAL_CAPACITY = 1  # 하위호환용 별칭 - 기존 로그 문구/필드에서 그대로 참조
# v3: 정상 상태를 1로 완전 고정하지 않고 1<->2로 흔들어(jitter) 실제 자연스러운
# 변동을 만든다 - 2026-09-12 실측/시뮬레이션으로 확인: capacity가 완벽히 flat이면
# IsolationForest가 학습할 분산이 없어서 스파이크를 걸러내지 못한다.
NORMAL_CAPACITY_LOW = 1
NORMAL_CAPACITY_HIGH = 2
JITTER_INTERVAL_SEC = 1200  # 20분마다 low<->high 토글 (150분 baseline에 약 7회)
# v1 기본값은 5였으나 쿼터/프리티어를 넘겨서 3으로 낮춤 (--anomaly-capacity로 조정 가능)
DEFAULT_ANOMALY_CAPACITY = 3

VCPU_PER_INSTANCE = 2   # t3.micro
EBS_GB_PER_INSTANCE = 8  # AL2023 기본 루트 볼륨
FREE_TIER_EBS_GB = 30
FREE_TIER_INSTANCE_HOURS = 750

LOG_DIR = PROJECT_ROOT / "playground" / "eval_outputs" / "logs"
RESULT_DIR = PROJECT_ROOT / "playground" / "eval_outputs"

logger = logging.getLogger("autoscaling_edos_trial")


def _setup_logging() -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = LOG_DIR / f"autoscaling_edos_trial_{ts}.log"

    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(fmt)
    logger.addHandler(console_handler)
    return log_path


# ── 쿼터 / 프리티어 사전 검증 ──────────────────────────────────────────────────

def check_capacity_budget(n_anomaly: int, n_normal: int, anomaly_capacity: int,
                           vcpu_limit: int, hours_estimate: float,
                           skip_quota_check: bool = False) -> None:
    """예상 최대 동시 인스턴스로 vCPU 쿼터와 프리티어(EBS 30GB, 750 인스턴스-시간)를 검증.

    ⚠️ 쿼터를 넘기면 ASG는 조용히 실패한다 — update_auto_scaling_group/set_desired_capacity는
    즉시 성공을 반환하고 실패는 ASG의 scaling activities에만 남는다. 그러면 desired만 오르고
    in_service는 안 올라서, mock이 학습한 "둘 다 급증" 패턴이 아닌 다른 신호를 측정하게 된다.
    """
    ec2 = boto3.client("ec2", region_name=AWS_REGION)
    current = ec2.describe_instances(
        Filters=[{"Name": "instance-state-name", "Values": ["pending", "running"]}])
    # 이번 트라이얼의 ASG가 이미 --setup으로 baseline capacity(1)만큼 띄워둔 인스턴스는
    # peak_instances 계산식(n_anomaly*anomaly_capacity + n_normal*NORMAL_CAPACITY)에
    # 이미 포함되어 있다. 여기서 또 더하면 이중 계산되어 실제보다 부풀려진 쿼터 초과로
    # 오판한다(실측 2026-09-12: 실제 46vCPU인데 72vCPU로 잘못 계산됨) -> 우리 트라이얼
    # 태그(Purpose=detection-trial)가 붙은 인스턴스는 "기존"에서 제외한다.
    current_instances = sum(
        1 for r in current["Reservations"] for inst in r["Instances"]
        if not any(t.get("Key") == "Purpose" and t.get("Value") == "detection-trial"
                   for t in inst.get("Tags", []))
    )

    # v3: normal/anomaly baseline이 jitter로 최대 NORMAL_CAPACITY_HIGH까지 올라갈 수
    # 있으므로 최악의 경우(모든 ASG가 동시에 high인 순간)를 기준으로 잡는다.
    peak_instances = current_instances + n_normal * NORMAL_CAPACITY_HIGH + n_anomaly * anomaly_capacity
    peak_vcpu = peak_instances * VCPU_PER_INSTANCE
    peak_ebs = peak_instances * EBS_GB_PER_INSTANCE
    instance_hours = (n_normal * NORMAL_CAPACITY_HIGH + n_anomaly * anomaly_capacity) * hours_estimate

    logger.info("예상 최대 동시 인스턴스 %d대 (기존 %d + 이번 %d)",
                peak_instances, current_instances, peak_instances - current_instances)
    logger.info("  vCPU: %d (쿼터 %d)", peak_vcpu, vcpu_limit)
    logger.info("  EBS: %dGB (프리티어 %dGB) %s", peak_ebs, FREE_TIER_EBS_GB,
                "-> 초과분은 시간비례 과금" if peak_ebs > FREE_TIER_EBS_GB else "-> 프리티어 내")
    logger.info("  인스턴스-시간: 약 %.1f (프리티어 월 %d시간)", instance_hours, FREE_TIER_INSTANCE_HOURS)
    logger.warning("프리티어 EC2/EBS는 계정 생성 후 12개월만 적용됨 - 12개월 초과 계정이면 "
                    "위 한도가 없고 전부 과금됨. Billing 콘솔에서 확인할 것.")

    if peak_vcpu > vcpu_limit:
        msg = (f"예상 최대 vCPU {peak_vcpu}가 쿼터 {vcpu_limit}를 초과합니다. "
               f"--anomaly-capacity 축소, --n-anomaly/--n-normal 축소, 기존 인스턴스 정리, "
               f"또는 쿼터 증설 중 하나가 필요합니다.")
        if skip_quota_check:
            logger.warning("%s (--skip-quota-check 지정으로 계속 진행 - launch 실패 시 "
                            "desired만 오르고 in_service는 안 오르는 왜곡된 측정이 될 수 있음)", msg)
        else:
            raise RuntimeError(msg)


# ── Launch Template / ASG 준비 ────────────────────────────────────────────────

def _asg_name(prefix: str, idx: int) -> str:
    return f"{ASG_PREFIX}-{prefix}-{idx}"


def _get_default_vpc_subnets() -> tuple[str, str]:
    """기본 VPC와 서브넷 ID들(콤마 구분). v1은 서브넷 1개만 써서 특정 AZ 용량 부족 시
    launch가 통째로 실패했는데, 여러 AZ를 주면 그 위험이 준다."""
    ec2 = boto3.client("ec2", region_name=AWS_REGION)
    vpcs = ec2.describe_vpcs(Filters=[{"Name": "is-default", "Values": ["true"]}])
    if not vpcs["Vpcs"]:
        raise RuntimeError("기본 VPC가 없습니다. VPC를 먼저 생성하세요.")
    vpc_id = vpcs["Vpcs"][0]["VpcId"]

    subnets = ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])
    if not subnets["Subnets"]:
        raise RuntimeError("서브넷이 없습니다.")
    subnet_ids = ",".join(s["SubnetId"] for s in subnets["Subnets"][:3])
    return vpc_id, subnet_ids


def _get_latest_amazon_linux_ami() -> str:
    ec2 = boto3.client("ec2", region_name=AWS_REGION)
    response = ec2.describe_images(
        Owners=["amazon"],
        Filters=[
            {"Name": "name", "Values": ["al2023-ami-*-x86_64"]},
            {"Name": "state", "Values": ["available"]},
        ],
    )
    images = sorted(response["Images"], key=lambda x: x["CreationDate"], reverse=True)
    if not images:
        raise RuntimeError("Amazon Linux AMI를 찾을 수 없습니다.")
    return images[0]["ImageId"]


def _ensure_launch_template(ec2) -> str:
    try:
        response = ec2.describe_launch_templates(LaunchTemplateNames=[LAUNCH_TEMPLATE_NAME])
        lt_id = response["LaunchTemplates"][0]["LaunchTemplateId"]
        logger.info("Launch Template 이미 존재: %s", LAUNCH_TEMPLATE_NAME)
        return lt_id
    except ec2.exceptions.ClientError as e:
        if "InvalidLaunchTemplateName.NotFoundException" not in str(e):
            raise

    ami_id = _get_latest_amazon_linux_ami()
    logger.info("Launch Template 생성: %s (AMI: %s)", LAUNCH_TEMPLATE_NAME, ami_id)
    response = ec2.create_launch_template(
        LaunchTemplateName=LAUNCH_TEMPLATE_NAME,
        LaunchTemplateData={"ImageId": ami_id, "InstanceType": "t3.micro"},
    )
    return response["LaunchTemplate"]["LaunchTemplateId"]


def _ensure_asg(autoscaling, name: str, lt_id: str, subnet_ids: str,
                 desired: int, max_size: int) -> None:
    existing = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[name])
    if existing["AutoScalingGroups"]:
        logger.info("ASG 이미 존재: %s (capacity=%d로 업데이트)", name, desired)
        autoscaling.update_auto_scaling_group(
            AutoScalingGroupName=name, DesiredCapacity=desired, MinSize=0, MaxSize=max_size)
    else:
        logger.info("ASG 생성: %s (desired=%d, max=%d)", name, desired, max_size)
        autoscaling.create_auto_scaling_group(
            AutoScalingGroupName=name,
            LaunchTemplate={"LaunchTemplateId": lt_id, "Version": "$Latest"},
            MinSize=0, MaxSize=max_size, DesiredCapacity=desired,
            VPCZoneIdentifier=subnet_ids,
            Tags=[
                {"Key": "Name", "Value": name, "PropagateAtLaunch": True},
                {"Key": "Purpose", "Value": "detection-trial", "PropagateAtLaunch": True},
            ],
        )

    # ⚠️ 이걸 안 켜면 GroupDesiredCapacity/GroupInServiceInstances가 CloudWatch에
    # 아예 안 올라간다 (S3의 Request Metrics와 같은 성격). v1에 빠져 있었다.
    autoscaling.enable_metrics_collection(AutoScalingGroupName=name, Granularity="1Minute")
    logger.info("group metrics collection 활성화: %s", name)


def setup_asgs(n_anomaly: int, n_normal: int, anomaly_capacity: int) -> tuple[list[str], list[str]]:
    ec2 = boto3.client("ec2", region_name=AWS_REGION)
    autoscaling = boto3.client("autoscaling", region_name=AWS_REGION)

    _, subnet_ids = _get_default_vpc_subnets()
    lt_id = _ensure_launch_template(ec2)

    anomaly_names = [_asg_name("anomaly", i) for i in range(n_anomaly)]
    normal_names = [_asg_name("normal", i) for i in range(n_normal)]

    # anomaly ASG는 baseline 구간에도 1<->2 jitter를 하므로 max_size가 최소
    # NORMAL_CAPACITY_HIGH는 돼야 하고, 스파이크 때는 anomaly_capacity까지 가야 한다.
    anomaly_max = max(anomaly_capacity, NORMAL_CAPACITY_HIGH)
    for name in anomaly_names:
        _ensure_asg(autoscaling, name, lt_id, subnet_ids, NORMAL_CAPACITY_LOW, max_size=anomaly_max)
    for name in normal_names:
        _ensure_asg(autoscaling, name, lt_id, subnet_ids, NORMAL_CAPACITY_LOW, max_size=NORMAL_CAPACITY_HIGH)

    logger.info("ASG 준비 완료: anomaly=%d개, normal=%d개 (모두 초기 capacity=%d, jitter로 %d~%d 사이 변동 예정)",
                n_anomaly, n_normal, NORMAL_CAPACITY_LOW, NORMAL_CAPACITY_LOW, NORMAL_CAPACITY_HIGH)
    logger.warning("생성 직후에는 CloudWatch 창(30포인트=2.5시간)의 앞부분이 0으로 채워진다. "
                    "--run의 --baseline-minutes 만큼 평상시 용량 이력을 쌓은 뒤 스파이크를 걸어야 "
                    "'0에서 급증'이 아닌 실제 EDoS 패턴을 측정하게 된다.")
    return anomaly_names, normal_names


def teardown_asgs(n_anomaly: int, n_normal: int) -> None:
    """ASG를 용량 0으로 내리고 삭제한다(인스턴스도 함께 정리). v1에 없던 경로 —
    안 지우면 capacity=1짜리 ASG가 계속 남아 프리티어를 소진한다."""
    autoscaling = boto3.client("autoscaling", region_name=AWS_REGION)
    for label, count in (("anomaly", n_anomaly), ("normal", n_normal)):
        for i in range(count):
            name = _asg_name(label, i)
            try:
                autoscaling.update_auto_scaling_group(
                    AutoScalingGroupName=name, MinSize=0, DesiredCapacity=0)
                autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name, ForceDelete=True)
                logger.info("ASG 삭제: %s", name)
            except Exception as exc:
                logger.warning("ASG 삭제 실패(%s): %s", name, exc)


# ── 메트릭 조회 / 판정 ────────────────────────────────────────────────────────

def _fetch_metrics_at(resource_type: str, resource_id: str, end_time: datetime,
                       n_points: int = 30, period_seconds: int = 300) -> dict[str, list[float]]:
    cw = boto3.client("cloudwatch", region_name=AWS_REGION)
    start_time = end_time - timedelta(seconds=n_points * period_seconds)

    metric_keys = list(METRIC_SPEC[resource_type].keys())
    dimensions = _build_dimensions(resource_type, resource_id)

    queries = []
    for i, metric_key in enumerate(metric_keys):
        namespace, cw_metric_name, stat = METRIC_SPEC[resource_type][metric_key]
        queries.append({
            "Id": f"m{i}",
            "MetricStat": {
                "Metric": {"Namespace": namespace, "MetricName": cw_metric_name, "Dimensions": dimensions},
                "Period": period_seconds,
                "Stat": stat,
            },
            "ReturnData": True,
        })

    response = cw.get_metric_data(
        MetricDataQueries=queries, StartTime=start_time, EndTime=end_time,
        ScanBy="TimestampAscending",
    )
    results_by_id = {r["Id"]: r for r in response["MetricDataResults"]}

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


def detect(resource_type: str, resource_id: str, end_time: datetime | None = None) -> dict:
    """production(detection_node 실제 방식)과 teammate_compat(옛 방식)을 모두 계산.
    cost는 detect_both가 estimate_cost_series로 채운다 — AutoScaling은 z-score 대상
    지표가 cost 하나뿐이라 이게 없으면 z-score가 아무것도 평가하지 못한다."""
    if end_time is None:
        return detect_both(resource_type, resource_id)
    usage = _fetch_metrics_at(resource_type, resource_id, end_time)
    return detect_both(resource_type, resource_id, usage=usage)


def _jitter_loop(asg_name: str, stop_event: threading.Event, label: str, rep: int) -> None:
    """정상(또는 anomaly baseline) 상태에 자연스러운 소폭 변동을 준다 - capacity를
    NORMAL_CAPACITY_LOW<->HIGH로 JITTER_INTERVAL_SEC마다 토글.

    2026-09-12 발견: capacity가 완벽히 flat이면 z-score/IForest 둘 다 실패한다
    (z-score는 masking effect, IForest는 학습할 분산 자체가 없어서). 합성 노이즈
    시뮬레이션(analyze_iforest_edos_real_data.py --inject-noise-std)으로 "약간의
    변동만 있으면 IForest가 recall 100%까지 개선된다"를 확인했고, 이번엔 그걸
    실제 AWS에서 재현하기 위해 진짜로 이렇게 흔든다(시뮬레이션이 아니라 실측).
    """
    autoscaling = boto3.client("autoscaling", region_name=AWS_REGION)
    current = NORMAL_CAPACITY_LOW
    while not stop_event.wait(JITTER_INTERVAL_SEC):
        current = NORMAL_CAPACITY_HIGH if current == NORMAL_CAPACITY_LOW else NORMAL_CAPACITY_LOW
        try:
            autoscaling.set_desired_capacity(AutoScalingGroupName=asg_name, DesiredCapacity=current)
            logger.info("[%s rep=%d] jitter: %s capacity -> %d", label, rep, asg_name, current)
        except Exception as exc:
            logger.warning("[%s rep=%d] jitter 실패(%s): %s", label, rep, asg_name, exc)


def _in_service_count(autoscaling, asg_name: str) -> int:
    groups = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])
    if not groups["AutoScalingGroups"]:
        return 0
    return sum(1 for i in groups["AutoScalingGroups"][0]["Instances"]
               if i.get("LifecycleState") == "InService")


# ── 시행 실행 ─────────────────────────────────────────────────────────────────

def run_anomaly_trial(asg_name: str, rep: int, anomaly_capacity: int, baseline_minutes: int,
                       startup_wait_sec: int, metric_wait_sec: int, sustain_minutes: int = 30) -> dict:
    t0 = time.time()
    autoscaling = boto3.client("autoscaling", region_name=AWS_REGION)
    spiked = False
    logger.info("[anomaly rep=%d] asg=%s 베이스라인 %d분(jitter %d<->%d 적용) 유지 후 capacity ->%d",
                rep, asg_name, baseline_minutes, NORMAL_CAPACITY_LOW, NORMAL_CAPACITY_HIGH, anomaly_capacity)

    jitter_stop = threading.Event()
    jitter_thread = threading.Thread(
        target=_jitter_loop, args=(asg_name, jitter_stop, "anomaly-baseline", rep), daemon=True)
    jitter_thread.start()
    try:
        time.sleep(baseline_minutes * 60)

        # 스파이크 전에 jitter부터 멈춰야 한다 - 안 그러면 jitter 스레드가 스파이크
        # 직후에 capacity를 low/high로 되돌려버려 스파이크가 씻겨나갈 수 있다.
        jitter_stop.set()
        jitter_thread.join(timeout=5)

        before = detect("AutoScaling", asg_name)
        logger.info("[anomaly rep=%d] before production.or_gate=%s", rep, before["production"]["or_gate"])

        autoscaling.set_desired_capacity(AutoScalingGroupName=asg_name,
                                          DesiredCapacity=anomaly_capacity)
        spiked = True
        logger.info("[anomaly rep=%d] capacity=%d 설정, 인스턴스 기동 대기 %d초...",
                    rep, anomaly_capacity, startup_wait_sec)
        time.sleep(startup_wait_sec)

        # 스파이크가 실제로 반영됐는지 확인 (쿼터에 걸려 조용히 실패하는 경우 감지)
        actual_in_service = _in_service_count(autoscaling, asg_name)
        spike_realized = actual_in_service >= anomaly_capacity
        if not spike_realized:
            logger.warning("[anomaly rep=%d] in_service=%d로 목표 %d 미달 - launch가 실패했을 수 있음"
                            "(쿼터/용량). 이 시행은 의도한 패턴이 아닐 수 있으니 해석 주의.",
                            rep, actual_in_service, anomaly_capacity)

        # classification_rules.json CLF-001의 sustained_fraction 조건(최근 구간 최소
        # 3포인트=15분의 60% 이상이 계속 높아야 함)을 만족하려면 startup_wait_sec만으로는
        # 부족하다(2026-09-11 발견 — 기존 10분으로는 CloudWatch 포인트가 1~2개뿐이라
        # 여유 없이 경계에 걸림). 측정 전 sustain_minutes만큼 더 유지해서 최근 구간에
        # 확실히 여러 포인트가 쌓이게 한다.
        logger.info("[anomaly rep=%d] 지속성 확보를 위해 %d분 추가 유지...", rep, sustain_minutes)
        time.sleep(sustain_minutes * 60)

        logger.info("[anomaly rep=%d] 메트릭 반영 대기 %d초...", rep, metric_wait_sec)
        time.sleep(metric_wait_sec)

        after = detect("AutoScaling", asg_name)
        logger.info("[anomaly rep=%d] production.or_gate=%s (z=%s, IF=%s) / teammate=%s",
                    rep, after["production"]["or_gate"], after["production"]["zscore_persistent"],
                    after["production"]["iforest_triggered"], after["teammate_compat"]["anomaly_flag"])

        return {
            "rep": rep, "resource": asg_name, "label": "anomaly",
            "capacity_before": NORMAL_CAPACITY_LOW, "capacity_after": anomaly_capacity,
            "in_service_after_spike": actual_in_service, "spike_realized": spike_realized,
            "before": before, "after": after,
            "detected_production": bool(after["production"]["or_gate"]),
            "detected_teammate_compat": bool(after["teammate_compat"]["anomaly_flag"]),
            "detected_iforest_only": bool(after["production"]["iforest_triggered"]),
            "detected_zscore_only": bool(after["production"]["zscore_persistent"]),
            "elapsed_sec": round(time.time() - t0, 1),
        }
    except Exception as exc:
        logger.error("[anomaly rep=%d] 실패: %s\n%s", rep, exc, traceback.format_exc())
        return {"rep": rep, "resource": asg_name, "label": "anomaly", "error": str(exc),
                "detected_production": None, "detected_teammate_compat": None,
                "detected_iforest_only": None, "detected_zscore_only": None}
    finally:
        # baseline 도중 예외가 나서 위에서 jitter_stop.set()을 못 거쳤을 경우 대비
        # (Event.set()은 이미 set된 상태에 다시 호출해도 안전하다).
        jitter_stop.set()
        jitter_thread.join(timeout=5)
        # v1은 예외 시 capacity가 올라간 채로 남았다
        if spiked:
            try:
                autoscaling.set_desired_capacity(AutoScalingGroupName=asg_name,
                                                  DesiredCapacity=NORMAL_CAPACITY_LOW)
                logger.info("[anomaly rep=%d] capacity=%d로 복구", rep, NORMAL_CAPACITY_LOW)
            except Exception as exc:
                logger.error("[anomaly rep=%d] capacity 복구 실패(수동 확인 필요): %s", rep, exc)


def run_normal_trial(asg_name: str, rep: int, baseline_minutes: int,
                      startup_wait_sec: int, metric_wait_sec: int) -> dict:
    """normal 시행: capacity를 low<->high로 jitter하며 정상 상태를 유지한다. anomaly와
    같은 시각에 측정되도록 총 대기 시간을 맞춘다(v1은 normal이 훨씬 이른 시점에 측정돼서
    두 그룹의 측정 시각이 어긋났다)."""
    t0 = time.time()
    logger.info("[normal rep=%d] asg=%s capacity %d<->%d jitter 유지 (총 %d분 후 측정)",
                rep, asg_name, NORMAL_CAPACITY_LOW, NORMAL_CAPACITY_HIGH,
                baseline_minutes + (startup_wait_sec + metric_wait_sec) // 60)

    jitter_stop = threading.Event()
    jitter_thread = threading.Thread(
        target=_jitter_loop, args=(asg_name, jitter_stop, "normal", rep), daemon=True)
    jitter_thread.start()
    try:
        time.sleep(baseline_minutes * 60)
        before = detect("AutoScaling", asg_name)
        logger.info("[normal rep=%d] before production.or_gate=%s", rep, before["production"]["or_gate"])

        time.sleep(startup_wait_sec + metric_wait_sec)

        after = detect("AutoScaling", asg_name)
        logger.info("[normal rep=%d] production.or_gate=%s (z=%s, IF=%s) / teammate=%s",
                    rep, after["production"]["or_gate"], after["production"]["zscore_persistent"],
                    after["production"]["iforest_triggered"], after["teammate_compat"]["anomaly_flag"])

        return {
            "rep": rep, "resource": asg_name, "label": "normal", "capacity": NORMAL_CAPACITY_LOW,
            "before": before, "after": after,
            "detected_production": bool(after["production"]["or_gate"]),
            "detected_teammate_compat": bool(after["teammate_compat"]["anomaly_flag"]),
            "detected_iforest_only": bool(after["production"]["iforest_triggered"]),
            "detected_zscore_only": bool(after["production"]["zscore_persistent"]),
            "elapsed_sec": round(time.time() - t0, 1),
        }
    except Exception as exc:
        logger.error("[normal rep=%d] 실패: %s\n%s", rep, exc, traceback.format_exc())
        return {"rep": rep, "resource": asg_name, "label": "normal", "error": str(exc),
                "detected_production": None, "detected_teammate_compat": None,
                "detected_iforest_only": None, "detected_zscore_only": None}
    finally:
        jitter_stop.set()
        jitter_thread.join(timeout=5)
        # 마지막 상태가 low든 high든 상관없이(정상 범위 안이므로) teardown 전까지는
        # 그대로 둬도 안전 - 굳이 강제로 low로 되돌리지 않는다.


# ── 메인 ────────────────────────────────────────────────────────────────────

def result_filename(anomaly_capacity: int, wait_sec: int, n_normal: int, n_anomaly: int) -> Path:
    date_str = datetime.now().strftime("%Y%m%d")
    name = (f"autoscaling_edos_trial__capacity{anomaly_capacity}_wait{wait_sec}s_"
            f"n{n_normal}-{n_anomaly}_scriptv{SCRIPT_VERSION}_{date_str}.json")
    return RESULT_DIR / name


def main() -> None:
    parser = argparse.ArgumentParser(description="AutoScaling EDoS 반복 시행 실험")
    parser.add_argument("--setup", action="store_true", help="ASG 생성 + group metrics 활성화")
    parser.add_argument("--run", action="store_true", help="실험 실행")
    parser.add_argument("--teardown", action="store_true", help="ASG 삭제(반드시 실행)")
    parser.add_argument("--n-anomaly", type=int, default=5)
    parser.add_argument("--n-normal", type=int, default=8)
    parser.add_argument("--anomaly-capacity", type=int, default=DEFAULT_ANOMALY_CAPACITY,
                        help="anomaly 시 capacity (기본 3 - 쿼터/프리티어 고려)")
    parser.add_argument("--baseline-minutes", type=int, default=150,
                        help="평상시 용량 이력을 쌓는 시간. 기본 150분(창 전체). 줄이면 창 앞부분이 "
                             "0으로 채워져 탐지율이 낙관적으로 왜곡됨")
    parser.add_argument("--startup-wait-sec", type=int, default=600, help="인스턴스 기동 대기(초)")
    parser.add_argument("--metric-wait-sec", type=int, default=300, help="CloudWatch 반영 대기(초)")
    parser.add_argument("--sustain-minutes", type=int, default=30,
                        help="스파이크 상태를 측정 전까지 추가로 유지할 시간(분) - "
                             "CLF-001의 sustained_fraction 조건(최근 구간 지속성) 충족용")
    parser.add_argument("--vcpu-limit", type=int, default=32, help="계정 vCPU 쿼터(L-1216C47A)")
    parser.add_argument("--skip-quota-check", action="store_true",
                        help="쿼터 초과여도 진행(권장하지 않음 - launch가 조용히 실패해 왜곡된 측정이 됨)")
    args = parser.parse_args()

    log_path = _setup_logging()
    logger.info("로그 파일: %s (SCRIPT_VERSION=%s)", log_path, SCRIPT_VERSION)

    if args.baseline_minutes < 150:
        logger.warning("--baseline-minutes=%d 는 창 전체(150분)보다 짧습니다. 창 앞부분이 0으로 "
                        "채워져 탐지율이 실제보다 좋게 나올 수 있으니 보고서에 명시할 것.",
                        args.baseline_minutes)

    if args.teardown:
        teardown_asgs(args.n_anomaly, args.n_normal)
        return

    hours_estimate = (args.baseline_minutes * 60 + args.startup_wait_sec
                       + args.sustain_minutes * 60 + args.metric_wait_sec) / 3600

    if args.setup:
        check_capacity_budget(args.n_anomaly, args.n_normal, args.anomaly_capacity,
                               args.vcpu_limit, hours_estimate, args.skip_quota_check)
        setup_asgs(args.n_anomaly, args.n_normal, args.anomaly_capacity)
        return

    if not args.run:
        parser.print_help()
        return

    check_capacity_budget(args.n_anomaly, args.n_normal, args.anomaly_capacity,
                           args.vcpu_limit, hours_estimate, args.skip_quota_check)

    anomaly_asgs = [_asg_name("anomaly", i) for i in range(args.n_anomaly)]
    normal_asgs = [_asg_name("normal", i) for i in range(args.n_normal)]

    total_workers = args.n_anomaly + args.n_normal
    logger.info("=== anomaly %d개 + normal %d개, 총 %d개 시행 동시 병렬 시작 ===",
                args.n_anomaly, args.n_normal, total_workers)

    results = []
    with ThreadPoolExecutor(max_workers=total_workers) as executor:
        futures = []
        for i in range(args.n_anomaly):
            futures.append(executor.submit(
                run_anomaly_trial, anomaly_asgs[i], i, args.anomaly_capacity,
                args.baseline_minutes, args.startup_wait_sec, args.metric_wait_sec,
                args.sustain_minutes))
        for i in range(args.n_normal):
            futures.append(executor.submit(
                run_normal_trial, normal_asgs[i], i,
                args.baseline_minutes, args.startup_wait_sec, args.metric_wait_sec))
        for future in as_completed(futures):
            results.append(future.result())

    results.sort(key=lambda r: (r["label"], r["rep"]))

    metrics = {
        "production(detection_node 실제 방식)": compute_metrics(results, "detected_production"),
        "iforest_only(persistence 적용)": compute_metrics(results, "detected_iforest_only"),
        "zscore_only(persistence 적용)": compute_metrics(results, "detected_zscore_only"),
        "teammate_compat(v1까지의 방식)": compute_metrics(results, "detected_teammate_compat"),
    }
    for name, m in metrics.items():
        c = m["confusion_matrix"]
        logger.info("[%s] TP=%d TN=%d FP=%d FN=%d / accuracy=%s recall=%s FPR=%s",
                    name, c["TP"], c["TN"], c["FP"], c["FN"],
                    f"{m['accuracy']:.1%}" if m["accuracy"] is not None else "N/A",
                    f"{m['recall']:.1%}" if m["recall"] is not None else "N/A",
                    f"{m['false_positive_rate']:.1%}" if m["false_positive_rate"] is not None else "N/A")
        if m["recall_ci_95_clopper_pearson"]:
            lo, hi = m["recall_ci_95_clopper_pearson"]
            logger.info("    recall 95%% CI(Clopper-Pearson) = [%.1f%%, %.1f%%]", lo * 100, hi * 100)

    unrealized = [r for r in results if r.get("label") == "anomaly" and r.get("spike_realized") is False]
    if unrealized:
        logger.warning("스파이크가 목표 용량에 도달하지 못한 anomaly 시행 %d건 - 해당 시행은 "
                        "'desired만 급증, in_service 평탄'이라 의도한 패턴과 다름: %s",
                        len(unrealized), [r["resource"] for r in unrealized])

    out_path = result_filename(args.anomaly_capacity, args.metric_wait_sec,
                                args.n_normal, args.n_anomaly)
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "script_version": SCRIPT_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "params": {
            "anomaly_capacity": args.anomaly_capacity,
            "baseline_minutes": args.baseline_minutes,
            "startup_wait_sec": args.startup_wait_sec,
            "metric_wait_sec": args.metric_wait_sec,
            "n_normal": args.n_normal, "n_anomaly": args.n_anomaly,
        },
        "metrics": metrics,
        "trials": results,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info("결과 저장: %s", out_path)
    logger.warning("테스트 후 반드시 정리하세요: --teardown --n-anomaly %d --n-normal %d",
                    args.n_anomaly, args.n_normal)


if __name__ == "__main__":
    main()
