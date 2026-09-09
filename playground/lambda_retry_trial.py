"""
playground/lambda_retry_trial.py

Lambda 재시도폭증 시나리오를 anomaly n회 + normal n회 반복 실행해서
TP/TN/FP/FN, accuracy, recall(+ Clopper-Pearson 95% CI)을 계산한다.

설계:
- Lambda 함수를 여러 개(N_ANOMALY + N_NORMAL) 생성해서 병렬로 테스트한다.
  같은 함수에 여러 시행을 동시에 걸면 CloudWatch 지표가 하나로 합쳐지기 때문.
- anomaly 시행: 에러율 50% 이상으로 호출 (LAMBDA_ERROR_RATE_THRESHOLD 기준)
- normal 시행: 에러 없이 정상 호출
- 탐지 기준: detection_agent.py의 _lambda_error_rate_check + z-score/IForest 앙상블

[실행 방법]
  1단계(Lambda 함수 준비, 최초 1회):
    python playground/lambda_retry_trial.py --setup --n-anomaly 5 --n-normal 8
  2단계(반복 실험 실행):
    python playground/lambda_retry_trial.py --run --n-anomaly 5 --n-normal 8

[생성 파일]
  - 결과: playground/eval_outputs/lambda_retry_trial__invocations{N}_errorrate{R}_wait{W}s_n{정상}-{이상}_scriptv{V}_{YYYYMMDD}.json
  - 로그: playground/eval_outputs/logs/lambda_retry_trial_{timestamp}.log
"""

from __future__ import annotations

SCRIPT_VERSION = "1"

import argparse
import json
import logging
import os
import sys
import time
import traceback
import zipfile
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

import random
import boto3
from scipy.stats import beta as _beta_dist

import pipeline.detection_agent as da
from pipeline.cloudwatch_client import METRIC_SPEC, _build_dimensions

# ── 설정값 ────────────────────────────────────────────────────────────────────

LAMBDA_PREFIX = "detection-trial-lambda"
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2")

# detection_agent.py 기준
# - LAMBDA_ERROR_RATE_THRESHOLD = 0.5 (50%)
# - LAMBDA_ERROR_RATE_MIN_INVOCATIONS = 10
ANOMALY_ERROR_RATE = 0.6  # 60% 에러율로 anomaly 유발 (50% 임계값 초과)
NORMAL_ERROR_RATE = 0.05  # 5% 에러율로 normal 유지

# 5분당 호출 횟수 (30포인트 윈도우 중 최근 k개에서 측정)
INVOCATIONS_PER_MINUTE = 20  # 분당 20회 → 5분에 100회

LOG_DIR = PROJECT_ROOT / "playground" / "eval_outputs" / "logs"
RESULT_DIR = PROJECT_ROOT / "playground" / "eval_outputs"

logger = logging.getLogger("lambda_retry_trial")


def _setup_logging() -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = LOG_DIR / f"lambda_retry_trial_{ts}.log"

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


# ── Lambda 함수 코드 ──────────────────────────────────────────────────────────

LAMBDA_CODE = '''
import json

def handler(event, context):
    if event.get("fail"):
        raise Exception("Intentional error for testing")
    return {"statusCode": 200, "body": json.dumps("OK")}
'''


def _create_lambda_zip() -> bytes:
    """Lambda 함수 코드를 zip으로 패키징"""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('lambda_function.py', LAMBDA_CODE)
    return buffer.getvalue()


# ── Lambda 함수 준비 ──────────────────────────────────────────────────────────

def _lambda_function_name(prefix: str, idx: int) -> str:
    return f"{LAMBDA_PREFIX}-{prefix}-{idx}"


def _get_or_create_lambda_role(iam) -> str:
    """Lambda 실행용 IAM 역할 생성 또는 기존 역할 ARN 반환"""
    role_name = "detection-trial-lambda-role"

    try:
        response = iam.get_role(RoleName=role_name)
        logger.info("IAM 역할 이미 존재: %s", role_name)
        return response['Role']['Arn']
    except iam.exceptions.NoSuchEntityException:
        pass

    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]
    }

    response = iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps(trust_policy),
        Description="Role for detection trial Lambda functions"
    )
    role_arn = response['Role']['Arn']

    # 기본 실행 정책 연결
    iam.attach_role_policy(
        RoleName=role_name,
        PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
    )

    logger.info("IAM 역할 생성: %s (10초 대기)", role_name)
    time.sleep(10)  # IAM 역할 전파 대기

    return role_arn


def _ensure_lambda_function(lam, name: str, role_arn: str, zip_bytes: bytes) -> None:
    """Lambda 함수 생성 또는 업데이트"""
    try:
        lam.get_function(FunctionName=name)
        logger.info("Lambda 함수 이미 존재: %s (업데이트)", name)
        lam.update_function_code(FunctionName=name, ZipFile=zip_bytes)
    except lam.exceptions.ResourceNotFoundException:
        logger.info("Lambda 함수 생성: %s", name)
        lam.create_function(
            FunctionName=name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.handler",
            Code={"ZipFile": zip_bytes},
            Timeout=30,
            MemorySize=128,
        )
        # 함수 활성화 대기
        waiter = lam.get_waiter('function_active')
        waiter.wait(FunctionName=name)


def setup_lambda_functions(n_anomaly: int, n_normal: int) -> tuple[list[str], list[str]]:
    """anomaly n_anomaly개 + normal n_normal개 Lambda 함수 생성"""
    iam = boto3.client("iam", region_name=AWS_REGION)
    lam = boto3.client("lambda", region_name=AWS_REGION)

    role_arn = _get_or_create_lambda_role(iam)
    zip_bytes = _create_lambda_zip()

    anomaly_names = [_lambda_function_name("anomaly", i) for i in range(n_anomaly)]
    normal_names = [_lambda_function_name("normal", i) for i in range(n_normal)]

    for name in anomaly_names + normal_names:
        _ensure_lambda_function(lam, name, role_arn, zip_bytes)

    logger.info("Lambda 함수 준비 완료: anomaly=%d개, normal=%d개", n_anomaly, n_normal)
    return anomaly_names, normal_names


# ── 메트릭 조회 ───────────────────────────────────────────────────────────────

def _fetch_metrics_at(resource_type: str, resource_id: str, end_time: datetime,
                       n_points: int = 30, period_seconds: int = 300) -> dict[str, list[float]]:
    """CloudWatch에서 메트릭 조회"""
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
    """detection_agent 로직으로 판정"""
    end_time = end_time or datetime.now(timezone.utc)
    usage = _fetch_metrics_at(resource_type, resource_id, end_time)
    n = len(next(iter(usage.values()))) if usage else 0

    result = {
        "resource_id": resource_id,
        "end_time": end_time.isoformat(),
        "n_points": n,
        "raw_metrics": usage,
    }

    if n < da.MIN_POINTS_FOR_IFOREST:
        result["note"] = f"포인트 {n}개로 최소 기준({da.MIN_POINTS_FOR_IFOREST}) 미달"
        result["anomaly_flag"] = False
        return result

    # Lambda 에러 재시도 폭증 체크
    error_metrics, error_triggered = da._lambda_error_rate_check(resource_type, usage)

    # z-score / IForest
    z_max = da._zscore_max(usage)
    z_triggered = z_max > da.Z_SCORE_THRESHOLD
    iforest_score = da._iforest_score(resource_type, usage)
    iforest_triggered = iforest_score > da.IFOREST_THRESHOLD

    result.update({
        "z_max": round(z_max, 4),
        "z_triggered": z_triggered,
        "iforest_score": round(iforest_score, 4),
        "iforest_triggered": iforest_triggered,
        "error_rate_triggered": error_triggered,
        "anomaly_flag": z_triggered or iforest_triggered or error_triggered,
    })
    return result


# ── 시행 실행 ─────────────────────────────────────────────────────────────────

def _invoke_lambda(lam, function_name: str, fail: bool) -> bool:
    """Lambda 함수 호출. 성공 여부 반환."""
    try:
        response = lam.invoke(
            FunctionName=function_name,
            InvocationType="RequestResponse",
            Payload=json.dumps({"fail": fail}),
        )
        return response.get("StatusCode") == 200 and not response.get("FunctionError")
    except Exception:
        return False


def run_anomaly_trial(function_name: str, rep: int, duration_minutes: int,
                       invocations_per_minute: int, error_rate: float, wait_sec: int) -> dict:
    """anomaly 시행: 높은 에러율로 호출"""
    t0 = time.time()
    logger.info("[anomaly rep=%d] function=%s 시작 (duration=%d분, inv/min=%d, error_rate=%.0f%%)",
                rep, function_name, duration_minutes, invocations_per_minute, error_rate * 100)

    try:
        lam = boto3.client("lambda", region_name=AWS_REGION)

        total_invocations = 0
        total_errors = 0

        for minute in range(duration_minutes):
            for _ in range(invocations_per_minute):
                should_fail = random.random() < error_rate
                success = _invoke_lambda(lam, function_name, fail=should_fail)
                total_invocations += 1
                if should_fail or not success:
                    total_errors += 1

            if minute < duration_minutes - 1:
                time.sleep(60)  # 다음 분까지 대기

        logger.info("[anomaly rep=%d] 호출 완료: %d회 (에러 %d회), %d초 대기 중...",
                    rep, total_invocations, total_errors, wait_sec)
        time.sleep(wait_sec)

        after = detect("Lambda", function_name)
        logger.info("[anomaly rep=%d] anomaly_flag=%s (z_max=%s, iforest=%s, error_rate_triggered=%s)",
                    rep, after.get("anomaly_flag"), after.get("z_max"),
                    after.get("iforest_score"), after.get("error_rate_triggered"))

        return {
            "rep": rep,
            "function_name": function_name,
            "label": "anomaly",
            "total_invocations": total_invocations,
            "total_errors": total_errors,
            "actual_error_rate": total_errors / total_invocations if total_invocations else 0,
            "after": after,
            "detected": bool(after.get("anomaly_flag")),
            "elapsed_sec": time.time() - t0,
        }
    except Exception as exc:
        logger.error("[anomaly rep=%d] 실패: %s\n%s", rep, exc, traceback.format_exc())
        return {"rep": rep, "function_name": function_name, "label": "anomaly",
                "error": str(exc), "detected": None}


def run_normal_trial(function_name: str, rep: int, duration_minutes: int,
                      invocations_per_minute: int, error_rate: float, wait_sec: int) -> dict:
    """normal 시행: 낮은 에러율로 정상 호출"""
    t0 = time.time()
    logger.info("[normal rep=%d] function=%s 시작 (duration=%d분, inv/min=%d, error_rate=%.0f%%)",
                rep, function_name, duration_minutes, invocations_per_minute, error_rate * 100)

    try:
        lam = boto3.client("lambda", region_name=AWS_REGION)

        total_invocations = 0
        total_errors = 0

        for minute in range(duration_minutes):
            for _ in range(invocations_per_minute):
                should_fail = random.random() < error_rate
                success = _invoke_lambda(lam, function_name, fail=should_fail)
                total_invocations += 1
                if should_fail or not success:
                    total_errors += 1

            if minute < duration_minutes - 1:
                time.sleep(60)

        logger.info("[normal rep=%d] 호출 완료: %d회 (에러 %d회), %d초 대기 중...",
                    rep, total_invocations, total_errors, wait_sec)
        time.sleep(wait_sec)

        after = detect("Lambda", function_name)
        logger.info("[normal rep=%d] anomaly_flag=%s (z_max=%s, iforest=%s, error_rate_triggered=%s)",
                    rep, after.get("anomaly_flag"), after.get("z_max"),
                    after.get("iforest_score"), after.get("error_rate_triggered"))

        return {
            "rep": rep,
            "function_name": function_name,
            "label": "normal",
            "total_invocations": total_invocations,
            "total_errors": total_errors,
            "actual_error_rate": total_errors / total_invocations if total_invocations else 0,
            "after": after,
            "detected": bool(after.get("anomaly_flag")),
            "elapsed_sec": time.time() - t0,
        }
    except Exception as exc:
        logger.error("[normal rep=%d] 실패: %s\n%s", rep, exc, traceback.format_exc())
        return {"rep": rep, "function_name": function_name, "label": "normal",
                "error": str(exc), "detected": None}


# ── 통계 계산 ────────────────────────────────────────────────────────────────

def clopper_pearson_ci(successes: int, n: int, confidence: float = 0.95) -> tuple[float, float]:
    if n == 0:
        return (0.0, 1.0)
    alpha = 1 - confidence
    lower = 0.0 if successes == 0 else _beta_dist.ppf(alpha / 2, successes, n - successes + 1)
    upper = 1.0 if successes == n else _beta_dist.ppf(1 - alpha / 2, successes + 1, n - successes)
    return (float(lower), float(upper))


def compute_confusion_metrics(anomaly_results: list[dict], normal_results: list[dict]) -> dict:
    tp = sum(1 for r in anomaly_results if r.get("detected") is True)
    fn = sum(1 for r in anomaly_results if r.get("detected") is False)
    fp = sum(1 for r in normal_results if r.get("detected") is True)
    tn = sum(1 for r in normal_results if r.get("detected") is False)

    total = tp + fn + fp + tn
    accuracy = (tp + tn) / total if total else None
    recall = tp / (tp + fn) if (tp + fn) else None
    precision = tp / (tp + fp) if (tp + fp) else None
    fpr = fp / (fp + tn) if (fp + tn) else None

    n_anomaly = tp + fn
    n_normal = tn + fp

    return {
        "confusion_matrix": {"TP": tp, "FN": fn, "FP": fp, "TN": tn},
        "accuracy": accuracy,
        "accuracy_ci_95_clopper_pearson": list(clopper_pearson_ci(tp + tn, total)) if total else None,
        "recall": recall,
        "recall_ci_95_clopper_pearson": list(clopper_pearson_ci(tp, n_anomaly)) if n_anomaly else None,
        "precision": precision,
        "false_positive_rate": fpr,
        "fpr_ci_95_clopper_pearson": list(clopper_pearson_ci(fp, n_normal)) if n_normal else None,
    }


# ── 메인 ────────────────────────────────────────────────────────────────────

def result_filename(invocations_per_min: int, error_rate: float, wait_sec: int,
                    n_normal: int, n_anomaly: int) -> Path:
    date_str = datetime.now().strftime("%Y%m%d")
    error_pct = int(error_rate * 100)
    name = (f"lambda_retry_trial__invpm{invocations_per_min}_errrate{error_pct}_wait{wait_sec}s_"
            f"n{n_normal}-{n_anomaly}_scriptv{SCRIPT_VERSION}_{date_str}.json")
    return RESULT_DIR / name


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--setup", action="store_true", help="Lambda 함수 생성")
    parser.add_argument("--run", action="store_true", help="실험 실행")
    parser.add_argument("--n-anomaly", type=int, default=5)
    parser.add_argument("--n-normal", type=int, default=8)
    parser.add_argument("--duration-minutes", type=int, default=5, help="각 시행당 호출 지속 시간(분)")
    parser.add_argument("--invocations-per-minute", type=int, default=20)
    parser.add_argument("--anomaly-error-rate", type=float, default=0.6)
    parser.add_argument("--normal-error-rate", type=float, default=0.05)
    parser.add_argument("--wait-sec", type=int, default=300, help="CloudWatch 반영 대기 시간")
    args = parser.parse_args()

    log_path = _setup_logging()
    logger.info("로그 파일: %s (SCRIPT_VERSION=%s)", log_path, SCRIPT_VERSION)

    if args.setup:
        setup_lambda_functions(args.n_anomaly, args.n_normal)
        return

    if not args.run:
        parser.print_help()
        return

    anomaly_functions = [_lambda_function_name("anomaly", i) for i in range(args.n_anomaly)]
    normal_functions = [_lambda_function_name("normal", i) for i in range(args.n_normal)]

    total_workers = args.n_anomaly + args.n_normal
    logger.info("=== anomaly %d개 + normal %d개, 총 %d개 시행 동시 병렬 시작 ===",
                args.n_anomaly, args.n_normal, total_workers)

    anomaly_results, normal_results = [], []
    with ThreadPoolExecutor(max_workers=total_workers) as executor:
        futures = {}
        for i in range(args.n_anomaly):
            fut = executor.submit(
                run_anomaly_trial, anomaly_functions[i], i,
                args.duration_minutes, args.invocations_per_minute,
                args.anomaly_error_rate, args.wait_sec
            )
            futures[fut] = ("anomaly", i)
        for i in range(args.n_normal):
            fut = executor.submit(
                run_normal_trial, normal_functions[i], i,
                args.duration_minutes, args.invocations_per_minute,
                args.normal_error_rate, args.wait_sec
            )
            futures[fut] = ("normal", i)

        for future in as_completed(futures):
            label, _ = futures[future]
            result = future.result()
            (anomaly_results if label == "anomaly" else normal_results).append(result)

    anomaly_results.sort(key=lambda r: r["rep"])
    normal_results.sort(key=lambda r: r["rep"])

    metrics = compute_confusion_metrics(anomaly_results, normal_results)
    logger.info("=== 결과 ===\n%s", json.dumps(metrics, ensure_ascii=False, indent=2))

    out_path = result_filename(args.invocations_per_minute, args.anomaly_error_rate,
                                args.wait_sec, args.n_normal, args.n_anomaly)
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "script_version": SCRIPT_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "params": {
            "duration_minutes": args.duration_minutes,
            "invocations_per_minute": args.invocations_per_minute,
            "anomaly_error_rate": args.anomaly_error_rate,
            "normal_error_rate": args.normal_error_rate,
            "wait_sec": args.wait_sec,
            "n_normal": args.n_normal,
            "n_anomaly": args.n_anomaly,
        },
        "metrics": metrics,
        "anomaly_trials": anomaly_results,
        "normal_trials": normal_results,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info("결과 저장: %s", out_path)


if __name__ == "__main__":
    main()
