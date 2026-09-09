"""
playground/lambda_spike_edos_repeated_trial.py

Lambda 호출량/비용 폭증(cost_spike)과 AutoScaling EDoS 의심 시나리오를
anomaly n회 + normal n회 반복 실행해서 TP/TN/FP/FN, accuracy, recall
(+ Clopper-Pearson 95% CI)을 계산한다.

s3_repeated_trial.py / ec2_lambda_repeated_trial.py와 같은 설계를 따른다:
- 시행마다 독립된 리소스(함수/ASG)를 쓴다. 같은 리소스에 여러 시행을 걸면
  CloudWatch 지표가 합쳐져서 "n번의 독립 시행"이 아니라 "1번의 n배 시행"이 된다.
- normal 시행도 침묵이 아니라 실제 평상시 수준 트래픽/용량을 유지한다.
- 시행마다 ±노이즈를 줘서 동일 반복이 되지 않게 한다.
- 판정은 teammate_compat(옛 phase_g 방식) / production(detection_node 실제 방식)
  둘 다 기록한다. 판정 로직은 ec2_lambda_repeated_trial.detect_both()에 일원화.

⚠️ 두 시나리오 모두 절대임계값 체크가 없다(EC2 유휴·Lambda 재시도폭증과 달리).
   따라서 z-score와 IForest가 유일한 탐지 수단이고, production과 iforest_only의
   차이는 z-score 기여분뿐이다.

⚠️ 워밍업(베이스라인 히스토리)이 결과를 좌우한다:
   탐지는 최근 30포인트(2.5시간) 창을 본다. 리소스를 새로 만들면 생성 이전 구간이
   CloudWatch에서 0으로 채워져서(cloudwatch_client 주석 참고), 어떤 트래픽이든
   "0에서 갑자기 튀어오른 것"처럼 보여 탐지율이 실제보다 좋게 나온다.
   그래서 --baseline-minutes 기본값을 150분(=창 전체)으로 뒀다. 짧게 줄이면
   결과가 낙관적으로 왜곡되므로, 줄일 경우 보고서에 그 사실을 명시해야 한다.

⚠️ EDoS는 실제 EC2 인스턴스가 뜬다(비용 + vCPU 쿼터):
   ASG 13개 × 베이스라인 용량 + anomaly 스파이크만큼 인스턴스가 동시에 존재한다.
   --vcpu-limit(기본 32, 계정 쿼터 L-1216C47A 값)로 예상 최대 vCPU를 사전 계산해서
   초과하면 실행을 거부한다. t3.micro=2vCPU 기준이므로 파라미터를 반드시 확인할 것.

[실행 방법]
  Lambda 폭증:
    python playground/lambda_spike_edos_repeated_trial.py --scenario lambda_spike --setup
    python playground/lambda_spike_edos_repeated_trial.py --scenario lambda_spike --run
  EDoS:
    python playground/lambda_spike_edos_repeated_trial.py --scenario edos --setup
    python playground/lambda_spike_edos_repeated_trial.py --scenario edos --run
    (테스트 후 정리: --scenario edos --teardown)

[생성 파일]
  - 결과: playground/eval_outputs/{scenario}_repeated_trial__n{정상}-{이상}_scriptv{V}_{YYYYMMDD}.json
  - 로그: playground/eval_outputs/logs/{scenario}_repeated_trial_{timestamp}.log
"""

from __future__ import annotations

SCRIPT_VERSION = "1"
# v1 (2026-09-09): 최초 작성 — Lambda cost_spike / AutoScaling EDoS 두 시나리오.
#   판정은 detect_both()(teammate_compat + production) 재사용, Clopper-Pearson CI,
#   EDoS vCPU 쿼터 사전 검증 + 용량 원상복구, 베이스라인 워밍업 경고.

import argparse
import json
import logging
import os
import random
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(PROJECT_ROOT / "playground") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "playground"))

os.environ.setdefault("AWS_PROFILE", "default")

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

import boto3

from ec2_lambda_repeated_trial import compute_metrics, detect_both

AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2")
LOG_DIR = PROJECT_ROOT / "playground" / "eval_outputs" / "logs"
RESULT_DIR = PROJECT_ROOT / "playground" / "eval_outputs"

# ── Lambda 폭증 설정 ─────────────────────────────────────────────────────────
# 재시도폭증 테스트용 함수와 분리한다 — 그쪽은 최근 히스토리에 에러가 많아서
# 호출량 폭증 시나리오의 베이스라인으로 쓰면 오염된다.
SPIKE_FN_PREFIX = "detection-test-spike"
LAMBDA_EXEC_ROLE = "arn:aws:iam::268140507066:role/detection-test-lambda-role"
LAMBDA_HANDLER_CODE = (
    b'def handler(event, context):\n'
    b'    return {"statusCode": 200, "body": "detection-test-spike ok"}\n'
)

# 평상시 호출 간격(초). 물량 수준을 다르게 해서 그룹 내 복제본이 되지 않게 한다.
LAMBDA_NORMAL_INTERVALS = [30, 30, 15, 15, 8, 8, 60, 20]
# anomaly는 평상시 대비 몇 배로 폭증시킬지(함수별로 다르게)
LAMBDA_SPIKE_MULTIPLIERS = [5.0, 6.5, 8.0, 5.5, 7.0]
LAMBDA_ANOMALY_BASE_INTERVAL = 20

# ── EDoS 설정 ───────────────────────────────────────────────────────────────
EDOS_ASG_PREFIX = "detection-trial-edos"
EDOS_LAUNCH_TEMPLATE_ID = os.environ.get("EDOS_LAUNCH_TEMPLATE_ID", "lt-00595beff0b8428b6")
# 평상시 용량(정상 ASG) / anomaly ASG의 베이스라인·스파이크 용량
EDOS_NORMAL_CAPACITY = 1
EDOS_ANOMALY_BASE_CAPACITY = 1
EDOS_SPIKE_CAPACITIES = [3, 4, 3, 4, 3]  # anomaly ASG별로 다르게
VCPU_PER_INSTANCE = 2  # t3.micro

logger = logging.getLogger("lambda_spike_edos_repeated_trial")


def _setup_logging(scenario: str) -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = LOG_DIR / f"{scenario}_repeated_trial_{ts}.log"
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG); fh.setFormatter(fmt); logger.addHandler(fh)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO); ch.setFormatter(fmt); logger.addHandler(ch)
    return log_path


# ══════════════════════════════════════════════════════════════════════════
# Lambda 호출량 폭증 (cost_spike)
# ══════════════════════════════════════════════════════════════════════════

def _spike_fn_name(label: str, idx: int) -> str:
    return f"{SPIKE_FN_PREFIX}-{label}-{idx}"


def setup_lambda_spike(n_anomaly: int, n_normal: int) -> list[str]:
    """전용 함수들을 생성한다(이미 있으면 스킵)."""
    import io
    import zipfile

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", LAMBDA_HANDLER_CODE.decode())
    zip_bytes = buf.getvalue()

    lam = boto3.client("lambda", region_name=AWS_REGION)
    names = ([_spike_fn_name("anomaly", i) for i in range(n_anomaly)]
             + [_spike_fn_name("normal", i) for i in range(n_normal)])
    for name in names:
        try:
            lam.get_function(FunctionName=name)
            logger.info("함수 이미 존재: %s (생성 스킵)", name)
            continue
        except lam.exceptions.ResourceNotFoundException:
            pass
        lam.create_function(
            FunctionName=name, Runtime="python3.12", Role=LAMBDA_EXEC_ROLE,
            Handler="lambda_function.handler", Code={"ZipFile": zip_bytes},
            Timeout=3, MemorySize=128,
        )
        logger.info("함수 생성: %s", name)

    logger.warning(
        "생성 직후에는 CloudWatch 히스토리가 비어 있어(0으로 채워짐) 어떤 트래픽도 "
        "'0에서 급증'으로 보인다. --run 전에 --baseline-minutes 만큼 평상시 트래픽을 "
        "쌓는 과정이 run에 포함돼 있지만, 그 시간을 줄이면 결과가 낙관적으로 왜곡된다."
    )
    return names


def run_lambda_spike_trial(function_name: str, label: str, normal_interval: int,
                            spike_multiplier: float, baseline_minutes: int,
                            spike_minutes: int, wait_sec: int,
                            jitter_ratio: float = 0.15) -> dict:
    """평상시 트래픽을 baseline_minutes 만큼 쌓은 뒤, anomaly면 spike_minutes 동안
    호출량을 spike_multiplier배로 올린다. normal은 계속 평상시 수준을 유지한다."""
    t0 = time.time()
    lam = boto3.client("lambda", region_name=AWS_REGION)

    interval = max(2, round(normal_interval * random.uniform(1 - jitter_ratio, 1 + jitter_ratio)))
    logger.info("[%s %s] 베이스라인 %d분 (간격 %ds)", label, function_name, baseline_minutes, interval)
    try:
        n_base = 0
        deadline = time.time() + baseline_minutes * 60
        while time.time() < deadline:
            try:
                lam.invoke(FunctionName=function_name, InvocationType="Event", Payload=b"{}")
                n_base += 1
            except Exception as exc:
                logger.warning("[%s] invoke 실패: %s", function_name, exc)
            time.sleep(interval)

        before = detect_both("Lambda", function_name)
        logger.info("[%s %s] 베이스라인 완료 (%d회). before production.or_gate=%s",
                    label, function_name, n_base, before["production"]["or_gate"])

        n_spike = 0
        if label == "anomaly":
            mult = spike_multiplier * random.uniform(1 - jitter_ratio, 1 + jitter_ratio)
            spike_interval = max(1, round(interval / mult))
            logger.info("[%s %s] 폭증 구간 %d분 (간격 %ds -> %ds, %.1f배)",
                        label, function_name, spike_minutes, interval, spike_interval, mult)
            deadline = time.time() + spike_minutes * 60
            while time.time() < deadline:
                try:
                    lam.invoke(FunctionName=function_name, InvocationType="Event", Payload=b"{}")
                    n_spike += 1
                except Exception as exc:
                    logger.warning("[%s] invoke 실패: %s", function_name, exc)
                time.sleep(spike_interval)
        else:
            # normal은 같은 시간 동안 평상시 수준을 계속 유지 (침묵이 아니라 정상 트래픽)
            logger.info("[%s %s] 평상시 유지 구간 %d분", label, function_name, spike_minutes)
            deadline = time.time() + spike_minutes * 60
            while time.time() < deadline:
                try:
                    lam.invoke(FunctionName=function_name, InvocationType="Event", Payload=b"{}")
                    n_spike += 1
                except Exception as exc:
                    logger.warning("[%s] invoke 실패: %s", function_name, exc)
                time.sleep(interval)

        logger.info("[%s %s] 호출 종료(폭증/유지 %d회), CloudWatch 반영 %d초 대기...",
                    label, function_name, n_spike, wait_sec)
        time.sleep(wait_sec)

        after = detect_both("Lambda", function_name)
        logger.info("[%s %s] after production.or_gate=%s (z=%s, IF=%s) / teammate=%s, %.1f분",
                    label, function_name, after["production"]["or_gate"],
                    after["production"]["zscore_persistent"], after["production"]["iforest_triggered"],
                    after["teammate_compat"]["anomaly_flag"], (time.time() - t0) / 60)

        return {"resource": function_name, "label": label,
                "normal_interval_sec": interval, "spike_multiplier": spike_multiplier if label == "anomaly" else None,
                "n_baseline_calls": n_base, "n_phase2_calls": n_spike,
                "before": before, "after": after,
                "detected_production": bool(after["production"]["or_gate"]),
                "detected_teammate_compat": bool(after["teammate_compat"]["anomaly_flag"]),
                "detected_iforest_only": bool(after["production"]["iforest_triggered"]),
                "detected_zscore_only": bool(after["production"]["zscore_persistent"]),
                "elapsed_sec": round(time.time() - t0, 1)}
    except Exception as exc:
        logger.error("[%s] 실패: %s\n%s", function_name, exc, traceback.format_exc())
        return {"resource": function_name, "label": label, "error": str(exc),
                "detected_production": None, "detected_teammate_compat": None,
                "detected_iforest_only": None, "detected_zscore_only": None}


# ══════════════════════════════════════════════════════════════════════════
# AutoScaling EDoS
# ══════════════════════════════════════════════════════════════════════════

def _asg_name(label: str, idx: int) -> str:
    return f"{EDOS_ASG_PREFIX}-{label}-{idx}"


def check_edos_quota(n_anomaly: int, n_normal: int, vcpu_limit: int) -> None:
    """예상 최대 동시 vCPU를 계산해서 쿼터를 넘으면 실행을 거부한다."""
    ec2 = boto3.client("ec2", region_name=AWS_REGION)
    current = ec2.describe_instances(
        Filters=[{"Name": "instance-state-name", "Values": ["pending", "running"]}]
    )
    current_instances = sum(len(r["Instances"]) for r in current["Reservations"])
    current_vcpu = current_instances * VCPU_PER_INSTANCE

    peak_instances = (n_normal * EDOS_NORMAL_CAPACITY
                      + sum(EDOS_SPIKE_CAPACITIES[:n_anomaly]))
    peak_vcpu = peak_instances * VCPU_PER_INSTANCE
    total = current_vcpu + peak_vcpu

    logger.info("vCPU 예상: 현재 %d대(%dvCPU) + 이번 실험 최대 %d대(%dvCPU) = %dvCPU (한도 %d)",
                current_instances, current_vcpu, peak_instances, peak_vcpu, total, vcpu_limit)
    if total > vcpu_limit:
        raise RuntimeError(
            f"예상 최대 vCPU {total}가 한도 {vcpu_limit}를 초과합니다. "
            f"다음 중 하나로 조정하세요: (1) 기존 인스턴스 정리, "
            f"(2) EDOS_SPIKE_CAPACITIES/EDOS_NORMAL_CAPACITY 축소, "
            f"(3) --n-anomaly/--n-normal 축소, (4) 1vCPU 인스턴스 타입 사용, "
            f"(5) 쿼터 증설 요청."
        )


def setup_edos(n_anomaly: int, n_normal: int) -> list[str]:
    """ASG를 생성하고 group metrics collection을 켠다.
    ⚠️ group metrics collection을 안 켜면 GroupDesiredCapacity/GroupInServiceInstances가
    CloudWatch에 아예 안 올라간다(S3의 Request Metrics와 같은 성격)."""
    asg = boto3.client("autoscaling", region_name=AWS_REGION)
    ec2 = boto3.client("ec2", region_name=AWS_REGION)
    subnets = ec2.describe_subnets(Filters=[{"Name": "default-for-az", "Values": ["true"]}])
    subnet_ids = ",".join(s["SubnetId"] for s in subnets["Subnets"][:2])

    names = []
    for label, count in (("anomaly", n_anomaly), ("normal", n_normal)):
        for i in range(count):
            name = _asg_name(label, i)
            names.append(name)
            base = EDOS_ANOMALY_BASE_CAPACITY if label == "anomaly" else EDOS_NORMAL_CAPACITY
            max_size = max(EDOS_SPIKE_CAPACITIES) if label == "anomaly" else EDOS_NORMAL_CAPACITY
            existing = asg.describe_auto_scaling_groups(AutoScalingGroupNames=[name])
            if existing["AutoScalingGroups"]:
                logger.info("ASG 이미 존재: %s (생성 스킵)", name)
            else:
                asg.create_auto_scaling_group(
                    AutoScalingGroupName=name,
                    LaunchTemplate={"LaunchTemplateId": EDOS_LAUNCH_TEMPLATE_ID, "Version": "$Latest"},
                    MinSize=0, MaxSize=max_size, DesiredCapacity=base,
                    VPCZoneIdentifier=subnet_ids,
                    Tags=[{"Key": "Name", "Value": name, "PropagateAtLaunch": True},
                          {"Key": "TrueLabel", "Value": label, "PropagateAtLaunch": False}],
                )
                logger.info("ASG 생성: %s (base=%d, max=%d)", name, base, max_size)
            asg.enable_metrics_collection(AutoScalingGroupName=name, Granularity="1Minute")
            logger.info("group metrics collection 활성화: %s", name)

    logger.warning(
        "ASG 생성 직후에는 CloudWatch 히스토리가 비어 있다(0으로 채워짐). "
        "--baseline-minutes 만큼 기다려서 평상시 용량 이력을 쌓은 뒤 스파이크를 걸어야 "
        "'0에서 급증'이 아닌 실제 EDoS 패턴이 된다."
    )
    return names


def teardown_edos(n_anomaly: int, n_normal: int) -> None:
    """ASG를 용량 0으로 내리고 삭제한다(인스턴스도 함께 정리됨)."""
    asg = boto3.client("autoscaling", region_name=AWS_REGION)
    for label, count in (("anomaly", n_anomaly), ("normal", n_normal)):
        for i in range(count):
            name = _asg_name(label, i)
            try:
                asg.update_auto_scaling_group(AutoScalingGroupName=name, MinSize=0, DesiredCapacity=0)
                asg.delete_auto_scaling_group(AutoScalingGroupName=name, ForceDelete=True)
                logger.info("ASG 삭제: %s", name)
            except Exception as exc:
                logger.warning("ASG 삭제 실패(%s): %s", name, exc)


def run_edos_trial(asg_name: str, label: str, spike_capacity: int, baseline_minutes: int,
                    spike_minutes: int, wait_sec: int) -> dict:
    """베이스라인 용량을 baseline_minutes 유지한 뒤, anomaly면 용량을 spike_capacity로
    올리고 spike_minutes 유지 후 원상복구한다."""
    t0 = time.time()
    asg = boto3.client("autoscaling", region_name=AWS_REGION)
    base = EDOS_ANOMALY_BASE_CAPACITY if label == "anomaly" else EDOS_NORMAL_CAPACITY
    spiked = False
    try:
        logger.info("[%s %s] 베이스라인 용량 %d로 %d분 유지", label, asg_name, base, baseline_minutes)
        time.sleep(baseline_minutes * 60)

        before = detect_both("AutoScaling", asg_name)
        logger.info("[%s %s] before production.or_gate=%s", label, asg_name, before["production"]["or_gate"])

        if label == "anomaly":
            logger.info("[%s %s] 용량 %d -> %d 로 스파이크", label, asg_name, base, spike_capacity)
            asg.update_auto_scaling_group(AutoScalingGroupName=asg_name, DesiredCapacity=spike_capacity)
            spiked = True
        else:
            logger.info("[%s %s] 용량 변경 없음(평상시 유지)", label, asg_name)

        logger.info("[%s %s] %d분 유지 후 CloudWatch 반영 %d초 대기...",
                    label, asg_name, spike_minutes, wait_sec)
        time.sleep(spike_minutes * 60 + wait_sec)

        after = detect_both("AutoScaling", asg_name)
        logger.info("[%s %s] after production.or_gate=%s (z=%s, IF=%s) / teammate=%s, %.1f분",
                    label, asg_name, after["production"]["or_gate"],
                    after["production"]["zscore_persistent"], after["production"]["iforest_triggered"],
                    after["teammate_compat"]["anomaly_flag"], (time.time() - t0) / 60)

        return {"resource": asg_name, "label": label,
                "base_capacity": base, "spike_capacity": spike_capacity if label == "anomaly" else None,
                "before": before, "after": after,
                "detected_production": bool(after["production"]["or_gate"]),
                "detected_teammate_compat": bool(after["teammate_compat"]["anomaly_flag"]),
                "detected_iforest_only": bool(after["production"]["iforest_triggered"]),
                "detected_zscore_only": bool(after["production"]["zscore_persistent"]),
                "elapsed_sec": round(time.time() - t0, 1)}
    except Exception as exc:
        logger.error("[%s] 실패: %s\n%s", asg_name, exc, traceback.format_exc())
        return {"resource": asg_name, "label": label, "error": str(exc),
                "detected_production": None, "detected_teammate_compat": None,
                "detected_iforest_only": None, "detected_zscore_only": None}
    finally:
        if spiked:
            try:
                asg.update_auto_scaling_group(AutoScalingGroupName=asg_name, DesiredCapacity=base)
                logger.info("[%s] 용량 원상복구: %d", asg_name, base)
            except Exception as exc:
                logger.error("[%s] 용량 원상복구 실패(수동 확인 필요): %s", asg_name, exc)


# ══════════════════════════════════════════════════════════════════════════

def result_filename(scenario: str, n_normal: int, n_anomaly: int) -> Path:
    date_str = datetime.now().strftime("%Y%m%d")
    return RESULT_DIR / (f"{scenario}_repeated_trial__n{n_normal}-{n_anomaly}_"
                          f"scriptv{SCRIPT_VERSION}_{date_str}.json")


def main() -> None:
    parser = argparse.ArgumentParser(description="Lambda 호출량 폭증 / AutoScaling EDoS 반복 시행 실험")
    parser.add_argument("--scenario", choices=["lambda_spike", "edos"], required=True)
    parser.add_argument("--setup", action="store_true", help="리소스 생성 + 지표 수집 활성화")
    parser.add_argument("--run", action="store_true", help="반복 실험 실행")
    parser.add_argument("--teardown", action="store_true", help="(edos 전용) ASG 정리")
    parser.add_argument("--n-anomaly", type=int, default=5)
    parser.add_argument("--n-normal", type=int, default=8)
    parser.add_argument("--baseline-minutes", type=int, default=150,
                        help="평상시 이력을 쌓는 시간. 기본 150분(창 전체). 줄이면 결과가 낙관적으로 왜곡됨")
    parser.add_argument("--spike-minutes", type=int, default=15,
                        help="폭증/유지 구간 길이. persistence(최근 3포인트=15분) 요건상 15분 이상 권장")
    parser.add_argument("--wait-sec", type=int, default=180, help="CloudWatch 반영 대기")
    parser.add_argument("--vcpu-limit", type=int, default=32, help="(edos) 계정 vCPU 쿼터")
    args = parser.parse_args()

    log_path = _setup_logging(args.scenario)
    logger.info("로그: %s (SCRIPT_VERSION=%s, scenario=%s)", log_path, SCRIPT_VERSION, args.scenario)

    if args.spike_minutes < 15:
        logger.warning("--spike-minutes=%d 는 persistence 요건(최근 3포인트=15분)보다 짧아 "
                        "진짜 이상도 트리거되지 않을 수 있음", args.spike_minutes)
    if args.baseline_minutes < 150:
        logger.warning("--baseline-minutes=%d 는 창 전체(150분)보다 짧습니다. 창의 앞부분이 "
                        "0으로 채워져서 탐지율이 실제보다 좋게 나올 수 있으니 보고서에 명시할 것",
                        args.baseline_minutes)

    if args.teardown:
        if args.scenario != "edos":
            parser.error("--teardown 은 --scenario edos 에서만 사용")
        teardown_edos(args.n_anomaly, args.n_normal)
        return

    if args.setup:
        if args.scenario == "lambda_spike":
            names = setup_lambda_spike(args.n_anomaly, args.n_normal)
        else:
            check_edos_quota(args.n_anomaly, args.n_normal, args.vcpu_limit)
            names = setup_edos(args.n_anomaly, args.n_normal)
        logger.info("준비 완료: %s", names)
        return

    if not args.run:
        parser.print_help()
        return

    if args.scenario == "edos":
        check_edos_quota(args.n_anomaly, args.n_normal, args.vcpu_limit)

    results = []
    if args.scenario == "lambda_spike":
        jobs = ([(_spike_fn_name("anomaly", i), "anomaly", LAMBDA_ANOMALY_BASE_INTERVAL,
                  LAMBDA_SPIKE_MULTIPLIERS[i % len(LAMBDA_SPIKE_MULTIPLIERS)])
                 for i in range(args.n_anomaly)]
                + [(_spike_fn_name("normal", i), "normal",
                    LAMBDA_NORMAL_INTERVALS[i % len(LAMBDA_NORMAL_INTERVALS)], 0.0)
                   for i in range(args.n_normal)])
        logger.info("=== Lambda 폭증: %d개 시행 동시 병렬 (베이스라인 %d분 + 폭증 %d분) ===",
                    len(jobs), args.baseline_minutes, args.spike_minutes)
        with ThreadPoolExecutor(max_workers=len(jobs)) as ex:
            futures = [ex.submit(run_lambda_spike_trial, fn, label, iv, mult,
                                  args.baseline_minutes, args.spike_minutes, args.wait_sec)
                       for fn, label, iv, mult in jobs]
            for fut in as_completed(futures):
                results.append(fut.result())
    else:
        jobs = ([(_asg_name("anomaly", i), "anomaly", EDOS_SPIKE_CAPACITIES[i % len(EDOS_SPIKE_CAPACITIES)])
                 for i in range(args.n_anomaly)]
                + [(_asg_name("normal", i), "normal", 0) for i in range(args.n_normal)])
        logger.info("=== EDoS: %d개 시행 동시 병렬 (베이스라인 %d분 + 스파이크 %d분) ===",
                    len(jobs), args.baseline_minutes, args.spike_minutes)
        with ThreadPoolExecutor(max_workers=len(jobs)) as ex:
            futures = [ex.submit(run_edos_trial, name, label, cap,
                                  args.baseline_minutes, args.spike_minutes, args.wait_sec)
                       for name, label, cap in jobs]
            for fut in as_completed(futures):
                results.append(fut.result())

    results.sort(key=lambda r: (r["label"], r["resource"]))

    metrics = {
        "production(detection_node 실제 방식)": compute_metrics(results, "detected_production"),
        "iforest_only(persistence 적용)": compute_metrics(results, "detected_iforest_only"),
        "zscore_only(persistence 적용)": compute_metrics(results, "detected_zscore_only"),
        "teammate_compat(낡은 헬퍼 방식)": compute_metrics(results, "detected_teammate_compat"),
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

    out_path = result_filename(args.scenario, args.n_normal, args.n_anomaly)
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({
            "script_version": SCRIPT_VERSION, "scenario": args.scenario,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "params": {"baseline_minutes": args.baseline_minutes, "spike_minutes": args.spike_minutes,
                        "wait_sec": args.wait_sec, "n_normal": args.n_normal, "n_anomaly": args.n_anomaly},
            "metrics": metrics, "trials": results,
        }, f, ensure_ascii=False, indent=2)
    logger.info("결과 저장: %s", out_path)
    if args.scenario == "edos":
        logger.warning("EDoS 테스트 후 반드시 정리하세요: --scenario edos --teardown")


if __name__ == "__main__":
    main()
