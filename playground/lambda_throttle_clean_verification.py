"""
playground/lambda_throttle_clean_verification.py

기존 13개 Lambda 테스트 함수(anomaly-2~5, normal-1~8)가 오늘 하루 반복시행으로
CloudWatch 지표 이력이 오염됨(Z-score 자기참조 베이스라인이 틀어져서 normal
6/8까지 max|Z|=3.2~3.8로 오탐, IForest도 일부 정상군에서 오탐 발생 — 2026-09-13
게이트별 분해 실측으로 확인). 오염된 함수를 재사용하지 않고, 완전히 새로운
Lambda 함수 13개를 만들어 깨끗한 CloudWatch 이력으로 IForest 단독 탐지 정확도를
재검증한다.

⚠️ 이번 검증은 detection(탐지)만 본다 — classification 폴백
(rule_engine._extract_spike_metrics)의 latest/mean 자기희석 문제는 별도
이슈로 분리됨(사용자 확인: "IF만으로만 탐지할건데 저건 상관없지" — CLF-007
매칭 실패 시 LLM 분류로 넘어가도 cost_spike는 정상 분류되므로 탐지 정확도엔
영향 없음). detection_agent.py는 이번 검증을 위해 건드리지 않는다(오늘 있었던
Z-score 임시 비활성화도 이미 원복 완료).

[구성]
  기존 lambda_throttle_diverse_trial.py와 동일한 설계(anomaly 5개=동시성
  severity 3단계, normal 8개=볼륨 4단계)를 새 함수 13개(이름에 -clean 접미사)
  로 재현한다.

[실행 단계]
  1. create : 13개 함수 생성(이미 있으면 스킵) — 기존 함수와 동일한 핸들러
     코드/런타임/역할(detection-test-lambda-role 재사용)
  2. warmup : 전체 13개에 가벼운 트래픽을 5분 간격으로 WARMUP_DURATION_SEC
     (기본 2.5시간+여유 = 3시간) 동안 흘려서, 생성 이전 구간이 0으로 채워지는
     콜드스타트 왜곡 없이 30포인트 창이 실제 이력으로 채워지게 한다.
  3. test   : anomaly-clean 5개는 동시성 제약 후 버스트, normal-clean 8개는
     무제한 상태로 볼륨만 다르게 부하(기존 diverse trial과 동일 설계) → 측정

[실행 방법]
  python playground/lambda_throttle_clean_verification.py --create
  python playground/lambda_throttle_clean_verification.py --warmup-and-test
  python playground/lambda_throttle_clean_verification.py --cleanup   # 함수 삭제(선택)

[생성 파일]
  playground/eval_outputs/lambda_throttle_clean_verification_{날짜}.json
  playground/team_results/lambda_throttle/clean_verification_20260914.json
  (기존 team_results/lambda_throttle/repeated_trial.json 등은 건드리지 않음)
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

import boto3
from botocore.exceptions import ClientError
from scipy.stats import beta as _beta_dist

from playground.measure_pipeline_timing import measure

AWS_REGION = "ap-northeast-2"
SETUP_PROFILE = "default"
IAM_ROLE_ARN = "arn:aws:iam::268140507066:role/detection-test-lambda-role"

# (function_name, label, profile, concurrency|None, test_burst_size, test_n_bursts)
TARGETS: list[tuple[str, str, str, "int | None", int, int]] = [
    ("detection-test-lambda-anomaly-2-clean", "anomaly", "severe", 1, 40, 3),
    ("detection-test-lambda-anomaly-3-clean", "anomaly", "severe", 1, 40, 3),
    ("detection-test-lambda-anomaly-4-clean", "anomaly", "moderate", 2, 40, 3),
    ("detection-test-lambda-anomaly-5-clean", "anomaly", "moderate", 2, 40, 3),
    ("detection-retry-storm-test-clean", "anomaly", "mild", 3, 40, 3),
    ("detection-test-lambda-normal-1-clean", "normal", "light", None, 5, 3),
    ("detection-test-lambda-normal-2-clean", "normal", "light", None, 5, 3),
    ("detection-test-lambda-normal-3-clean", "normal", "moderate", None, 15, 3),
    ("detection-test-lambda-normal-4-clean", "normal", "moderate", None, 15, 3),
    ("detection-test-lambda-normal-5-clean", "normal", "heavy", None, 30, 3),
    ("detection-test-lambda-normal-6-clean", "normal", "heavy", None, 30, 3),
    ("detection-test-lambda-normal-7-clean", "normal", "bursty", None, 40, 1),
    ("detection-test-lambda-normal-8-clean", "normal", "bursty", None, 40, 1),
]

WARMUP_DURATION_SEC = 10800   # 3시간 (2.5시간 최소 + 여유)
WARMUP_PERIOD_SEC = 300       # 5분마다 한 번씩 가벼운 트래픽
WARMUP_CALLS_PER_TICK = 2     # 매 tick마다 함수당 호출 수 (가벼운 수준)

BURST_INTERVAL_SEC = 300
MAX_EVENT_AGE_SEC = 60
POST_BURST_WAIT_SEC = 180
SHARED_INVOKE_POOL_SIZE = 60

LAMBDA_HANDLER_CODE = (
    'def handler(event, context):\n'
    '    if isinstance(event, dict) and event.get("force_error"):\n'
    '        raise Exception("detection-test-lambda: intentional error for retry-storm test")\n'
    '    return {"statusCode": 200, "body": "detection-test-lambda ok"}\n'
)

LOG_DIR = PROJECT_ROOT / "playground" / "eval_outputs" / "logs"
RESULT_DIR = PROJECT_ROOT / "playground" / "eval_outputs"
TEAM_RESULT_DIR = PROJECT_ROOT / "playground" / "team_results" / "lambda_throttle"

logger = logging.getLogger("lambda_throttle_clean_verification")


def _setup_logging() -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = LOG_DIR / f"lambda_throttle_clean_verification_{ts}.log"
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.addHandler(fh)
    logger.addHandler(sh)
    logger.info("로그 파일: %s", log_path)
    return log_path


def _setup_lambda_client():
    return boto3.Session(profile_name=SETUP_PROFILE).client("lambda", region_name=AWS_REGION)


def _build_zip() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", LAMBDA_HANDLER_CODE)
    return buf.getvalue()


def create_functions() -> None:
    lam = _setup_lambda_client()
    zip_bytes = _build_zip()
    for fn, label, profile, _, _, _ in TARGETS:
        try:
            lam.get_function(FunctionName=fn)
            logger.info("[%s] 이미 존재 — 스킵", fn)
            continue
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ResourceNotFoundException":
                raise
        lam.create_function(
            FunctionName=fn,
            Runtime="python3.12",
            Role=IAM_ROLE_ARN,
            Handler="lambda_function.handler",
            Code={"ZipFile": zip_bytes},
            Timeout=3,
            MemorySize=128,
            Architectures=["x86_64"],
        )
        logger.info("[%s] 생성 완료 (label=%s profile=%s)", fn, label, profile)
    # 생성 직후 활성화 대기
    waiter = lam.get_waiter("function_active_v2")
    for fn, *_ in TARGETS:
        waiter.wait(FunctionName=fn)
    logger.info("전체 %d개 함수 활성화 확인", len(TARGETS))


def _invoke_once(lam, fn: str) -> None:
    try:
        lam.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}")
    except Exception as exc:
        logger.warning("  [%s] warmup invoke 실패: %s", fn, exc)


def warmup_baseline() -> None:
    lam = _setup_lambda_client()
    fns = [t[0] for t in TARGETS]
    n_ticks = WARMUP_DURATION_SEC // WARMUP_PERIOD_SEC
    logger.info("=== 베이스라인 워밍업 시작: %d개 함수, %d회 tick(%d초 간격, 총 %.1f시간) ===",
                len(fns), n_ticks, WARMUP_PERIOD_SEC, WARMUP_DURATION_SEC / 3600)
    with ThreadPoolExecutor(max_workers=SHARED_INVOKE_POOL_SIZE) as pool:
        for i in range(n_ticks):
            futures = []
            for fn in fns:
                for _ in range(WARMUP_CALLS_PER_TICK):
                    futures.append(pool.submit(_invoke_once, lam, fn))
            for f in futures:
                f.result()
            logger.info("워밍업 tick %d/%d 완료", i + 1, n_ticks)
            if i < n_ticks - 1:
                time.sleep(WARMUP_PERIOD_SEC)
    logger.info("=== 베이스라인 워밍업 완료 ===")


def _snapshot_path(function_name: str) -> Path:
    return RESULT_DIR / f".lambda_throttle_clean_verification_snapshot__{function_name}.json"


def _snapshot(lam, function_name: str) -> dict:
    try:
        concurrency = lam.get_function_concurrency(FunctionName=function_name).get(
            "ReservedConcurrentExecutions"
        )
    except Exception:
        concurrency = None
    had_eic = True
    try:
        lam.get_function_event_invoke_config(FunctionName=function_name)
    except lam.exceptions.ResourceNotFoundException:
        had_eic = False
    return {"concurrency": concurrency, "had_event_invoke_config": had_eic}


def _save_snapshot(function_name: str, snap: dict) -> None:
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    with open(_snapshot_path(function_name), "w", encoding="utf-8") as f:
        json.dump(snap, f)


def restore(function_name: str) -> None:
    lam = _setup_lambda_client()
    path = _snapshot_path(function_name)
    if path.exists():
        with open(path, encoding="utf-8") as f:
            snap = json.load(f)
    else:
        snap = {"concurrency": None, "had_event_invoke_config": False}

    if snap["concurrency"] is None:
        try:
            lam.delete_function_concurrency(FunctionName=function_name)
        except Exception:
            pass
    else:
        lam.put_function_concurrency(
            FunctionName=function_name, ReservedConcurrentExecutions=snap["concurrency"]
        )
    if not snap["had_event_invoke_config"]:
        try:
            lam.delete_function_event_invoke_config(FunctionName=function_name)
        except Exception:
            pass
    logger.info("[원복 %s] 완료", function_name)
    if path.exists():
        path.unlink()


def _burst_invoke(lam, function_name: str, burst_size: int, n_bursts: int,
                   invoke_pool: ThreadPoolExecutor) -> list[str]:
    burst_times = []

    def _invoke(i: int) -> bool:
        try:
            lam.invoke(FunctionName=function_name, InvocationType="Event", Payload=b"{}")
            return True
        except Exception as exc:
            logger.warning("  [%s] invoke #%d 실패: %s", function_name, i, exc)
            return False

    for b in range(n_bursts):
        burst_times.append(datetime.now(timezone.utc).isoformat())
        futures = [invoke_pool.submit(_invoke, i) for i in range(burst_size)]
        results = [f.result() for f in futures]
        logger.info("[%s] 버스트 %d/%d 완료: 성공 %d/%d",
                    function_name, b + 1, n_bursts, sum(results), burst_size)
        if b < n_bursts - 1:
            time.sleep(BURST_INTERVAL_SEC)
    return burst_times


def run_trial(function_name: str, label: str, profile: str, concurrency: "int | None",
              burst_size: int, n_bursts: int, invoke_pool: ThreadPoolExecutor) -> dict:
    lam = _setup_lambda_client()

    if concurrency is not None:
        snap = _snapshot(lam, function_name)
        _save_snapshot(function_name, snap)
        lam.put_function_event_invoke_config(
            FunctionName=function_name, MaximumEventAgeInSeconds=MAX_EVENT_AGE_SEC,
            MaximumRetryAttempts=2,
        )
        lam.put_function_concurrency(FunctionName=function_name, ReservedConcurrentExecutions=concurrency)
        logger.info("[%s] %s 셋업 완료 (concurrency=%d, burst=%dx%d)",
                    function_name, profile, concurrency, burst_size, n_bursts)
        time.sleep(3)
    else:
        logger.info("[%s] %s — 동시성 제한 없이 진행 (burst=%dx%d)",
                    function_name, profile, burst_size, n_bursts)

    burst_times = _burst_invoke(lam, function_name, burst_size, n_bursts, invoke_pool)
    time.sleep(POST_BURST_WAIT_SEC)

    logger.info("[%s] measure() 시작", function_name)
    try:
        result = measure(function_name, "Lambda", bypass_approval_for_timing=True)
    except Exception as exc:
        logger.error("[%s] measure() 실패: %s", function_name, exc)
        result = {"resource_id": function_name, "resource_type": "Lambda", "error": str(exc)}

    if concurrency is not None:
        restore(function_name)

    detected = bool(result.get("anomaly_flag")) and result.get("anomaly_type") == "cost_spike"
    logger.info("[%s] 완료 — label=%s profile=%s anomaly_flag=%s anomaly_type=%s detected=%s",
                function_name, label, profile, result.get("anomaly_flag"), result.get("anomaly_type"),
                detected)

    return {**result, "label": label, "profile": profile, "concurrency_setting": concurrency,
            "burst_size": burst_size, "n_bursts": n_bursts, "burst_times_utc": burst_times,
            "detected": detected, "measured_at": datetime.now(timezone.utc).isoformat()}


def _clopper_pearson(k: int, n: int, alpha: float = 0.05) -> tuple[float, float]:
    if n == 0:
        return (0.0, 1.0)
    lo = 0.0 if k == 0 else _beta_dist.ppf(alpha / 2, k, n - k + 1)
    hi = 1.0 if k == n else _beta_dist.ppf(1 - alpha / 2, k + 1, n - k)
    return (float(lo), float(hi))


def compute_metrics(trials: list[dict]) -> dict:
    tp = sum(1 for t in trials if t["label"] == "anomaly" and t["detected"])
    fn = sum(1 for t in trials if t["label"] == "anomaly" and not t["detected"])
    fp = sum(1 for t in trials if t["label"] == "normal" and t["detected"])
    tn = sum(1 for t in trials if t["label"] == "normal" and not t["detected"])
    n = tp + fn + fp + tn
    accuracy = (tp + tn) / n if n else 0.0
    recall = tp / (tp + fn) if (tp + fn) else None
    precision = tp / (tp + fp) if (tp + fp) else None
    fpr = fp / (fp + tn) if (fp + tn) else None
    return {
        "confusion_matrix": {"TP": tp, "FN": fn, "FP": fp, "TN": tn},
        "accuracy": accuracy,
        "accuracy_ci_95_clopper_pearson": _clopper_pearson(tp + tn, n),
        "recall": recall,
        "recall_ci_95_clopper_pearson": _clopper_pearson(tp, tp + fn) if (tp + fn) else None,
        "precision": precision,
        "false_positive_rate": fpr,
        "fpr_ci_95_clopper_pearson": _clopper_pearson(fp, fp + tn) if (fp + tn) else None,
    }


def run_test() -> None:
    logger.info("=== 다양화 시행 시작 (13개 신규 함수, anomaly=5 severity 3단계 / normal=8 volume 4단계) ===")
    trials = []
    try:
        with ThreadPoolExecutor(max_workers=SHARED_INVOKE_POOL_SIZE) as invoke_pool, \
             ThreadPoolExecutor(max_workers=len(TARGETS)) as ex:
            futures = {
                ex.submit(run_trial, fn, label, profile, conc, bsize, nb, invoke_pool): fn
                for fn, label, profile, conc, bsize, nb in TARGETS
            }
            for fut in as_completed(futures):
                trials.append(fut.result())
    finally:
        for fn, _, _, conc, _, _ in TARGETS:
            if conc is not None and _snapshot_path(fn).exists():
                logger.warning("[%s] 정리 안 된 스냅샷 발견 — 재원복", fn)
                restore(fn)

    order = [fn for fn, _, _, _, _, _ in TARGETS]
    trials.sort(key=lambda t: order.index(t["resource_id"]))
    metrics = compute_metrics(trials)

    out = {
        "script_version": "1",
        "scenario": "lambda_throttle_retry_storm_clean_verification",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "note": "오염된 기존 13개 함수(anomaly-2~5, normal-1~8) 대신 신규 생성한 -clean 함수 13개로 "
                "IForest 단독 탐지 정확도를 재검증. detection_agent.py는 수정하지 않음(production 코드 그대로).",
        "params": {
            "n_anomaly": 5, "n_normal": 8,
            "warmup_duration_sec": WARMUP_DURATION_SEC,
            "anomaly_profiles": {"severe": "concurrency=1", "moderate": "concurrency=2", "mild": "concurrency=3"},
            "normal_profiles": {"light": "5x3", "moderate": "15x3", "heavy": "30x3", "bursty": "40x1"},
        },
        "metrics": metrics,
        "trials": trials,
    }

    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = RESULT_DIR / f"lambda_throttle_clean_verification_{datetime.now().strftime('%Y%m%d')}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    TEAM_RESULT_DIR.mkdir(parents=True, exist_ok=True)
    team_out_path = TEAM_RESULT_DIR / f"clean_verification_{datetime.now().strftime('%Y%m%d')}.json"
    with open(team_out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    logger.info("=== 결과 ===")
    logger.info(json.dumps(metrics, ensure_ascii=False, indent=2))
    for t in trials:
        logger.info("  %s (label=%s profile=%s): anomaly_flag=%s detected=%s",
                     t["resource_id"], t["label"], t["profile"], t.get("anomaly_flag"), t["detected"])
    logger.info("결과 저장: %s", out_path)
    logger.info("team_results 별도 파일 저장: %s (기존 repeated_trial.json 등은 건드리지 않음)", team_out_path)


def cleanup() -> None:
    lam = _setup_lambda_client()
    for fn, *_ in TARGETS:
        try:
            lam.delete_function(FunctionName=fn)
            logger.info("[%s] 삭제 완료", fn)
        except Exception as exc:
            logger.warning("[%s] 삭제 실패: %s", fn, exc)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--create", action="store_true")
    parser.add_argument("--warmup-and-test", action="store_true")
    parser.add_argument("--test-only", action="store_true",
                         help="워밍업 생략하고 버스트+측정만 재실행 (베이스라인이 이미 있을 때, "
                              "예: 모델을 재학습한 뒤 같은 -clean 함수로 재검증할 때)")
    parser.add_argument("--cleanup", action="store_true")
    args = parser.parse_args()

    _setup_logging()

    if args.create:
        create_functions()
    elif args.warmup_and_test:
        warmup_baseline()
        run_test()
    elif args.test_only:
        run_test()
    elif args.cleanup:
        cleanup()
    else:
        parser.error("--create, --warmup-and-test, --test-only, --cleanup 중 하나는 지정해야 함")


if __name__ == "__main__":
    main()
