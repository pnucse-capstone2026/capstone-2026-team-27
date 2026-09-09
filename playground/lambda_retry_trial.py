"""
playground/lambda_retry_trial.py

Lambda 재시도폭증 시나리오를 anomaly n회 + normal n회 반복 실행해서
TP/TN/FP/FN, accuracy, recall(+ Clopper-Pearson 95% CI)을 계산한다.

설계:
- Lambda 함수를 여러 개(N_ANOMALY + N_NORMAL) 생성해서 병렬로 테스트한다.
  같은 함수에 여러 시행을 동시에 걸면 CloudWatch 지표가 하나로 합쳐지기 때문.
- anomaly 시행: 에러율 50% 이상으로 호출 (LAMBDA_ERROR_RATE_THRESHOLD 기준)
- normal 시행: 평상시 수준의 낮은 베이스라인 에러율로 정상 호출
- 탐지 기준: detection_node가 실제로 쓰는 방식(production) + 옛 방식(teammate_compat)
  둘 다 기록

[실행 방법]
  1단계(Lambda 함수 준비, 최초 1회):
    python playground/lambda_retry_trial.py --setup --n-anomaly 5 --n-normal 8
  2단계(반복 실험 실행):
    python playground/lambda_retry_trial.py --run --n-anomaly 5 --n-normal 8
  3단계(정리):
    python playground/lambda_retry_trial.py --teardown --n-anomaly 5 --n-normal 8

[생성 파일]
  - 결과: playground/eval_outputs/lambda_retry_trial__invpm{N}_errrate{R}_wait{W}s_n{정상}-{이상}_scriptv{V}_{YYYYMMDD}.json
  - 로그: playground/eval_outputs/logs/lambda_retry_trial_{timestamp}.log

[비용] Lambda 프리티어는 월 100만 요청 + 40만 GB-초로 12개월 제한 없이 상시 무료다.
  이 실험은 함수당 수백~수천 호출이라 비용은 사실상 0. (EC2/EBS와 달리 걱정 없음)
"""

from __future__ import annotations

SCRIPT_VERSION = "2"
# v1 (2026-09-09): 최초 작성.
# v2 (2026-09-09): 아래 6가지 수정.
#   (1) [치명적] --duration-minutes 기본값 5 -> 15. 절대체크(_lambda_error_rate_check)는
#       PERSISTENCE_WINDOW_POINTS=3, 즉 "최근 3개 5분 구간이 모두" invocation>=10 AND
#       error_rate>=50%를 만족해야 트리거된다. 5분만 돌리면 5분 구간이 1개만 채워져서
#       anomaly가 원리상 절대 잡히지 않는다. 15분 미만이면 경고를 띄운다.
#   (2) [치명적] IAM 역할을 새로 만들지 않고 기존 detection-test-lambda-role을 기본
#       재사용한다. 이 프로젝트의 terraform-user는 IAM 쓰기 권한이 제한적이어서
#       (iam:GetInstanceProfile 등도 거부됨) create_role이 AccessDenied로 실패할 수 있다.
#       --role-arn 으로 지정 가능하고, 없을 때만 생성을 시도하되 실패 시 안내한다.
#   (3) 판정 로직 교체 — _zscore_max/_iforest_score(옛 phase_g 헬퍼)는 detection_node와
#       다르다. phase_g는 2026-08-25 작성이고 그 뒤 08-28 persistence가 도입됐지만
#       그 헬퍼엔 반영되지 않았다. production 방식으로 바꾸고 옛 방식은 teammate_compat으로
#       함께 기록(과거 수치 비교용). v1이 _lambda_error_rate_check는 이미 호출하고 있었던
#       점은 유지.
#   (4) cost 지표 반영 — fetch는 invocation_count/error_count/duration_avg만 준다.
#       Lambda의 z-score 대상 지표는 cost와 invocation_count인데 cost가 비어 있으면
#       그만큼 평가가 빠지고, IForest도 학습 때(cost 있음)와 mask가 어긋난다.
#   (5) 베이스라인 워밍업(--baseline-minutes) 추가 — 함수 생성 직후엔 창(30포인트=2.5시간)의
#       앞부분이 0으로 채워져서 어떤 호출이든 "0에서 급증"으로 보여 탐지율이 부풀려진다.
#   (6) 함수별 파라미터 다양화 + 분 내 호출 분산 — v1은 그룹 내 모든 함수가 동일한
#       에러율/호출량이라 "n번의 독립 시행"이 아니라 "1개 설정의 n개 복제본"이 된다
#       (Clopper-Pearson CI의 독립 시행 전제가 깨짐). 또 v1은 분 시작에 호출을 몰아서
#       보내고 60초 쉬는데, 분 안에 고르게 퍼뜨리는 게 실제 트래픽에 가깝다.
#   그리고 teardown 경로 추가.

import argparse
import io
import json
import logging
import os
import random
import sys
import time
import traceback
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
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

import pipeline.detection_agent as da
from pipeline.cloudwatch_client import METRIC_SPEC, _build_dimensions
# 판정 로직·통계는 다른 반복시행 스크립트와 공유한다(한 곳에서만 관리)
from ec2_lambda_repeated_trial import clopper_pearson_ci, compute_metrics, detect_both

# ── 설정값 ────────────────────────────────────────────────────────────────────

LAMBDA_PREFIX = "detection-trial-lambda"
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2")

# 이미 있는 역할을 기본으로 재사용 (IAM 쓰기 권한이 없어도 동작하게)
DEFAULT_ROLE_ARN = os.environ.get(
    "LAMBDA_TRIAL_ROLE_ARN", "arn:aws:iam::268140507066:role/detection-test-lambda-role")
FALLBACK_ROLE_NAME = "detection-trial-lambda-role"

# detection_agent.py 기준
# - LAMBDA_ERROR_RATE_THRESHOLD = 0.5 (50%)
# - LAMBDA_ERROR_RATE_MIN_INVOCATIONS = 10 (5분 구간당)
# - PERSISTENCE_WINDOW_POINTS = 3 (최근 3개 구간 전부)
MIN_DURATION_MINUTES = da.PERSISTENCE_WINDOW_POINTS * 5  # 15분

# 함수별로 서로 다른 값을 써서 그룹 내 복제본이 되지 않게 한다.
# anomaly는 전부 50% 문턱을 넘되 강도를 다르게, normal은 물량/베이스라인 에러율을 다르게.
ANOMALY_PROFILES = [  # (error_rate, invocations_per_minute)
    (0.55, 12), (0.70, 20), (0.85, 30), (0.60, 25), (0.90, 15),
]
NORMAL_PROFILES = [
    (0.00, 12), (0.02, 12), (0.00, 25), (0.03, 25),
    (0.00, 40), (0.01, 40), (0.00, 4), (0.02, 20),
]

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
    if isinstance(event, dict) and event.get("fail"):
        raise Exception("Intentional error for testing")
    return {"statusCode": 200, "body": json.dumps("OK")}
'''


def _create_lambda_zip() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", LAMBDA_CODE)
    return buffer.getvalue()


# ── Lambda 함수 준비 ──────────────────────────────────────────────────────────

def _lambda_function_name(prefix: str, idx: int) -> str:
    return f"{LAMBDA_PREFIX}-{prefix}-{idx}"


def _resolve_role_arn(explicit_arn: str | None) -> str:
    """기존 역할을 재사용한다. 없으면 생성을 시도하되, 이 프로젝트의 자격증명은
    IAM 쓰기 권한이 제한적이라 실패할 수 있으므로 명확히 안내한다."""
    iam = boto3.client("iam", region_name=AWS_REGION)
    candidate = explicit_arn or DEFAULT_ROLE_ARN
    role_name = candidate.rsplit("/", 1)[-1]

    try:
        arn = iam.get_role(RoleName=role_name)["Role"]["Arn"]
        logger.info("기존 IAM 역할 재사용: %s", arn)
        return arn
    except Exception as exc:
        logger.warning("역할 조회 실패(%s): %s", role_name, exc)

    logger.info("역할 생성 시도: %s", FALLBACK_ROLE_NAME)
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }],
    }
    try:
        response = iam.create_role(
            RoleName=FALLBACK_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Role for detection trial Lambda functions",
        )
        iam.attach_role_policy(
            RoleName=FALLBACK_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
        )
        logger.info("역할 생성 완료 (전파 대기 10초)")
        time.sleep(10)
        return response["Role"]["Arn"]
    except Exception as exc:
        raise RuntimeError(
            f"IAM 역할을 재사용도 생성도 할 수 없습니다: {exc}\n"
            f"--role-arn 으로 사용 가능한 Lambda 실행 역할 ARN을 직접 지정하거나, "
            f"IAM 권한이 있는 자격증명으로 실행하세요."
        ) from exc


def _ensure_lambda_function(lam, name: str, role_arn: str, zip_bytes: bytes) -> None:
    try:
        lam.get_function(FunctionName=name)
        logger.info("Lambda 함수 이미 존재: %s (코드 업데이트)", name)
        lam.update_function_code(FunctionName=name, ZipFile=zip_bytes)
    except lam.exceptions.ResourceNotFoundException:
        logger.info("Lambda 함수 생성: %s", name)
        lam.create_function(
            FunctionName=name, Runtime="python3.12", Role=role_arn,
            Handler="lambda_function.handler", Code={"ZipFile": zip_bytes},
            Timeout=30, MemorySize=128,
        )
        lam.get_waiter("function_active").wait(FunctionName=name)


def setup_lambda_functions(n_anomaly: int, n_normal: int,
                            role_arn: str | None = None) -> tuple[list[str], list[str]]:
    lam = boto3.client("lambda", region_name=AWS_REGION)
    resolved_role = _resolve_role_arn(role_arn)
    zip_bytes = _create_lambda_zip()

    anomaly_names = [_lambda_function_name("anomaly", i) for i in range(n_anomaly)]
    normal_names = [_lambda_function_name("normal", i) for i in range(n_normal)]
    for name in anomaly_names + normal_names:
        _ensure_lambda_function(lam, name, resolved_role, zip_bytes)

    logger.info("Lambda 함수 준비 완료: anomaly=%d개, normal=%d개", n_anomaly, n_normal)
    logger.warning("생성 직후에는 CloudWatch 창(30포인트=2.5시간)의 앞부분이 0으로 채워진다. "
                    "--run의 --baseline-minutes 만큼 평상시 호출을 쌓은 뒤 폭증을 걸어야 "
                    "'0에서 급증'이 아닌 실제 재시도폭증 패턴을 측정하게 된다.")
    return anomaly_names, normal_names


def teardown_lambda_functions(n_anomaly: int, n_normal: int) -> None:
    lam = boto3.client("lambda", region_name=AWS_REGION)
    for label, count in (("anomaly", n_anomaly), ("normal", n_normal)):
        for i in range(count):
            name = _lambda_function_name(label, i)
            try:
                lam.delete_function(FunctionName=name)
                logger.info("Lambda 함수 삭제: %s", name)
            except Exception as exc:
                logger.warning("삭제 실패(%s): %s", name, exc)


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
    """production(detection_node 실제 방식: persistence z-score + persistence IForest +
    절대임계값 체크)과 teammate_compat(옛 방식: 창 최댓값 z-score + 마지막 시점 IForest)을
    모두 계산. cost는 detect_both가 estimate_cost_series로 채운다."""
    if end_time is None:
        return detect_both(resource_type, resource_id)
    usage = _fetch_metrics_at(resource_type, resource_id, end_time)
    return detect_both(resource_type, resource_id, usage=usage)


# ── 시행 실행 ─────────────────────────────────────────────────────────────────

def _invoke_lambda(lam, function_name: str, fail: bool) -> bool:
    """RequestResponse(동기)로 호출한다. 비동기(Event)는 실패 시 Lambda가 기본 2회
    자동 재시도해서 Invocations/Errors가 의도한 값보다 부풀려지므로, 에러율을 정확히
    통제하려면 동기 호출이 맞다."""
    try:
        response = lam.invoke(
            FunctionName=function_name,
            InvocationType="RequestResponse",
            Payload=json.dumps({"fail": fail}),
        )
        return response.get("StatusCode") == 200 and not response.get("FunctionError")
    except Exception:
        return False


def _run_invocation_phase(lam, function_name: str, minutes: int, invocations_per_minute: int,
                           error_rate: float, tag: str) -> tuple[int, int]:
    """minutes 동안 분당 invocations_per_minute회 호출. v1처럼 분 시작에 몰아서 보내지 않고
    분 안에 고르게 퍼뜨린다(실제 트래픽에 더 가깝고, 5분 구간이 고르게 채워짐)."""
    interval = 60.0 / max(1, invocations_per_minute)
    total = errors = 0
    for _ in range(minutes):
        minute_start = time.time()
        for _ in range(invocations_per_minute):
            should_fail = random.random() < error_rate
            ok = _invoke_lambda(lam, function_name, fail=should_fail)
            total += 1
            if should_fail or not ok:
                errors += 1
            sleep_for = interval - ((time.time() - minute_start) % interval)
            time.sleep(max(0.0, min(interval, sleep_for)))
        remain = 60 - (time.time() - minute_start)
        if remain > 0:
            time.sleep(remain)
    logger.info("[%s] %s 구간 완료: %d회 호출(에러 %d, 실측 에러율 %.0f%%)",
                function_name, tag, total, errors, (errors / total * 100) if total else 0)
    return total, errors


def run_trial(function_name: str, rep: int, label: str, error_rate: float,
               invocations_per_minute: int, baseline_minutes: int, duration_minutes: int,
               wait_sec: int, normal_baseline_error_rate: float = 0.02) -> dict:
    """anomaly/normal 공통. 베이스라인(평상시 에러율) 구간을 쌓은 뒤,
    anomaly는 error_rate로 폭증시키고 normal은 평상시 수준을 계속 유지한다."""
    t0 = time.time()
    lam = boto3.client("lambda", region_name=AWS_REGION)
    logger.info("[%s rep=%d] function=%s 시작 (베이스라인 %d분 + 본구간 %d분, "
                "inv/min=%d, 본구간 error_rate=%.0f%%)",
                label, rep, function_name, baseline_minutes, duration_minutes,
                invocations_per_minute, error_rate * 100)
    try:
        n_base = e_base = 0
        if baseline_minutes > 0:
            n_base, e_base = _run_invocation_phase(
                lam, function_name, baseline_minutes, invocations_per_minute,
                normal_baseline_error_rate, "베이스라인")

        before = detect("Lambda", function_name)
        logger.info("[%s rep=%d] before production.or_gate=%s", label, rep,
                    before["production"]["or_gate"])

        n_main, e_main = _run_invocation_phase(
            lam, function_name, duration_minutes, invocations_per_minute, error_rate,
            "폭증" if label == "anomaly" else "평상시 유지")

        logger.info("[%s rep=%d] CloudWatch 반영 %d초 대기...", label, rep, wait_sec)
        time.sleep(wait_sec)

        after = detect("Lambda", function_name)
        prod = after["production"]
        logger.info("[%s rep=%d] production.or_gate=%s (z=%s, IF=%s, 절대=%s) / teammate=%s",
                    label, rep, prod["or_gate"], prod["zscore_persistent"],
                    prod["iforest_triggered"], prod["absolute_triggered"],
                    after["teammate_compat"]["anomaly_flag"])

        return {
            "rep": rep, "resource": function_name, "label": label,
            "target_error_rate": error_rate, "invocations_per_minute": invocations_per_minute,
            "baseline_invocations": n_base, "baseline_errors": e_base,
            "main_invocations": n_main, "main_errors": e_main,
            "actual_main_error_rate": round(e_main / n_main, 4) if n_main else 0,
            "before": before, "after": after,
            "detected_production": bool(prod["or_gate"]),
            "detected_teammate_compat": bool(after["teammate_compat"]["anomaly_flag"]),
            "detected_iforest_only": bool(prod["iforest_triggered"]),
            "detected_zscore_only": bool(prod["zscore_persistent"]),
            "detected_absolute_only": bool(prod["absolute_triggered"]),
            "elapsed_sec": round(time.time() - t0, 1),
        }
    except Exception as exc:
        logger.error("[%s rep=%d] 실패: %s\n%s", label, rep, exc, traceback.format_exc())
        return {"rep": rep, "resource": function_name, "label": label, "error": str(exc),
                "detected_production": None, "detected_teammate_compat": None,
                "detected_iforest_only": None, "detected_zscore_only": None,
                "detected_absolute_only": None}


# ── 메인 ────────────────────────────────────────────────────────────────────

def result_filename(invocations_per_min: int, error_rate: float, wait_sec: int,
                    n_normal: int, n_anomaly: int) -> Path:
    date_str = datetime.now().strftime("%Y%m%d")
    error_pct = int(error_rate * 100)
    name = (f"lambda_retry_trial__invpm{invocations_per_min}_errrate{error_pct}_wait{wait_sec}s_"
            f"n{n_normal}-{n_anomaly}_scriptv{SCRIPT_VERSION}_{date_str}.json")
    return RESULT_DIR / name


def main() -> None:
    parser = argparse.ArgumentParser(description="Lambda 재시도폭증 반복 시행 실험")
    parser.add_argument("--setup", action="store_true", help="Lambda 함수 생성")
    parser.add_argument("--run", action="store_true", help="실험 실행")
    parser.add_argument("--teardown", action="store_true", help="Lambda 함수 삭제")
    parser.add_argument("--n-anomaly", type=int, default=5)
    parser.add_argument("--n-normal", type=int, default=8)
    parser.add_argument("--duration-minutes", type=int, default=MIN_DURATION_MINUTES,
                        help=f"본 구간 호출 지속 시간(분). 절대체크는 5분 구간 "
                             f"{da.PERSISTENCE_WINDOW_POINTS}개 연속을 요구하므로 "
                             f"{MIN_DURATION_MINUTES}분 이상이어야 한다")
    parser.add_argument("--baseline-minutes", type=int, default=150,
                        help="평상시 호출 이력을 쌓는 시간. 기본 150분(창 전체). 줄이면 창 앞부분이 "
                             "0으로 채워져 탐지율이 낙관적으로 왜곡됨")
    parser.add_argument("--wait-sec", type=int, default=180, help="CloudWatch 반영 대기 시간")
    parser.add_argument("--role-arn", type=str, default=None,
                        help="Lambda 실행 역할 ARN(미지정 시 기존 detection-test-lambda-role 재사용)")
    args = parser.parse_args()

    log_path = _setup_logging()
    logger.info("로그 파일: %s (SCRIPT_VERSION=%s)", log_path, SCRIPT_VERSION)

    if args.duration_minutes < MIN_DURATION_MINUTES:
        logger.warning(
            "--duration-minutes=%d 는 %d분 미만입니다. 절대체크(_lambda_error_rate_check)는 "
            "최근 %d개 5분 구간이 모두 조건을 만족해야 트리거되므로, 이 설정으로는 "
            "진짜 anomaly도 절대체크로 잡히지 않습니다(z-score/IForest만 기여).",
            args.duration_minutes, MIN_DURATION_MINUTES, da.PERSISTENCE_WINDOW_POINTS)
    if args.baseline_minutes < 150:
        logger.warning("--baseline-minutes=%d 는 창 전체(150분)보다 짧습니다. 창 앞부분이 0으로 "
                        "채워져 탐지율이 실제보다 좋게 나올 수 있으니 보고서에 명시할 것.",
                        args.baseline_minutes)

    if args.teardown:
        teardown_lambda_functions(args.n_anomaly, args.n_normal)
        return

    if args.setup:
        setup_lambda_functions(args.n_anomaly, args.n_normal, args.role_arn)
        return

    if not args.run:
        parser.print_help()
        return

    total_workers = args.n_anomaly + args.n_normal
    logger.info("=== anomaly %d개 + normal %d개, 총 %d개 시행 동시 병렬 시작 ===",
                args.n_anomaly, args.n_normal, total_workers)
    logger.info("함수별로 서로 다른 에러율/호출량을 쓴다(그룹 내 복제본이면 독립 시행이 아니므로)")

    results = []
    with ThreadPoolExecutor(max_workers=total_workers) as executor:
        futures = []
        for i in range(args.n_anomaly):
            err, ipm = ANOMALY_PROFILES[i % len(ANOMALY_PROFILES)]
            futures.append(executor.submit(
                run_trial, _lambda_function_name("anomaly", i), i, "anomaly", err, ipm,
                args.baseline_minutes, args.duration_minutes, args.wait_sec))
        for i in range(args.n_normal):
            err, ipm = NORMAL_PROFILES[i % len(NORMAL_PROFILES)]
            futures.append(executor.submit(
                run_trial, _lambda_function_name("normal", i), i, "normal", err, ipm,
                args.baseline_minutes, args.duration_minutes, args.wait_sec))
        for future in as_completed(futures):
            results.append(future.result())

    results.sort(key=lambda r: (r["label"], r["rep"]))

    metrics = {
        "production(detection_node 실제 방식)": compute_metrics(results, "detected_production"),
        "iforest_only(persistence 적용)": compute_metrics(results, "detected_iforest_only"),
        "zscore_only(persistence 적용)": compute_metrics(results, "detected_zscore_only"),
        "absolute_only(_lambda_error_rate_check)": compute_metrics(results, "detected_absolute_only"),
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

    out_path = result_filename(ANOMALY_PROFILES[0][1], ANOMALY_PROFILES[0][0],
                                args.wait_sec, args.n_normal, args.n_anomaly)
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "script_version": SCRIPT_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "params": {
            "duration_minutes": args.duration_minutes,
            "baseline_minutes": args.baseline_minutes,
            "wait_sec": args.wait_sec,
            "n_normal": args.n_normal, "n_anomaly": args.n_anomaly,
            "anomaly_profiles": ANOMALY_PROFILES[:args.n_anomaly],
            "normal_profiles": NORMAL_PROFILES[:args.n_normal],
        },
        "metrics": metrics,
        "trials": results,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info("결과 저장: %s", out_path)
    logger.info("정리는 --teardown --n-anomaly %d --n-normal %d", args.n_anomaly, args.n_normal)


if __name__ == "__main__":
    main()
