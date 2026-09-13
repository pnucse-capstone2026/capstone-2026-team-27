"""
playground/lambda_throttle_diverse_trial.py

Lambda "스로틀(429)/동시성 소진 재시도 폭증" 시나리오의 실 AWS 반복시행 —
lambda_throttle_repeated_trial.py(anomaly=concurrency 1 전부 동일, normal=무제한
40x3 전부 동일)가 "너무 확실한 케이스만" 테스트한다는 지적에 따라, EC2 좀비
테스트(anomaly=silent/whisper/whisper_more, normal=light/moderate/heavy/bursty)와
같은 철학으로 각 그룹 내부를 다양화한다.

[설계 원칙]
  - anomaly(5개)는 "동시성 제약의 강도"만 다르게 한다 — concurrency < burst_size인
    한 큐잉/스로틀/재시도가 실제로 발생하므로 severe(1)/moderate(2)/mild(3) 전부
    진짜 anomaly로 라벨이 확실하다. (이전 lambda_throttle_boundary_trial.py는 이
    concurrency 2/3/5를 "normal"로 잘못 라벨링해서 ground truth 자체가 틀렸었다.)
  - normal(8개)은 동시성 제약을 전혀 걸지 않고 "호출 볼륨/패턴"만 다양화한다.
    이전 boundary trial에서 "무제한인데도 40x3을 몰아치면 IForest가 1.0을 찍는"
    confound가 발견됐으므로, normal 쪽 볼륨은 anomaly 수준(40x3=120건)보다
    확실히 낮게 잡는다(light 5x3, moderate 15x3, heavy 30x3, bursty 40x1=단발성).

[동시성/볼륨 배분 — 기존 13개 함수 재사용]
  anomaly severe   (concurrency=1) : anomaly-2, anomaly-3        — 40건 x 3회
  anomaly moderate (concurrency=2) : anomaly-4, anomaly-5        — 40건 x 3회
  anomaly mild     (concurrency=3) : detection-retry-storm-test  — 40건 x 3회
  normal light     (무제한)         : normal-1, normal-2          — 5건 x 3회
  normal moderate  (무제한)         : normal-3, normal-4          — 15건 x 3회
  normal heavy     (무제한)         : normal-5, normal-6          — 30건 x 3회
  normal bursty    (무제한)         : normal-7, normal-8          — 40건 x 1회(단발성)

[실행 방법]
  python playground/lambda_throttle_diverse_trial.py --run
  python playground/lambda_throttle_diverse_trial.py --restore   # 중간에 죽었을 때

[생성 파일]
  playground/eval_outputs/lambda_throttle_diverse_trial_{날짜}.json
  playground/eval_outputs/logs/lambda_throttle_diverse_trial_{시각}.log
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

import boto3
from scipy.stats import beta as _beta_dist

from playground.measure_pipeline_timing import measure

AWS_REGION = "ap-northeast-2"
SETUP_PROFILE = "default"

# (function_name, label, profile, concurrency|None, burst_size, n_bursts)
TARGETS: list[tuple[str, str, str, "int | None", int, int]] = [
    ("detection-test-lambda-anomaly-2", "anomaly", "severe", 1, 40, 3),
    ("detection-test-lambda-anomaly-3", "anomaly", "severe", 1, 40, 3),
    ("detection-test-lambda-anomaly-4", "anomaly", "moderate", 2, 40, 3),
    ("detection-test-lambda-anomaly-5", "anomaly", "moderate", 2, 40, 3),
    ("detection-retry-storm-test", "anomaly", "mild", 3, 40, 3),
    ("detection-test-lambda-normal-1", "normal", "light", None, 5, 3),
    ("detection-test-lambda-normal-2", "normal", "light", None, 5, 3),
    ("detection-test-lambda-normal-3", "normal", "moderate", None, 15, 3),
    ("detection-test-lambda-normal-4", "normal", "moderate", None, 15, 3),
    ("detection-test-lambda-normal-5", "normal", "heavy", None, 30, 3),
    ("detection-test-lambda-normal-6", "normal", "heavy", None, 30, 3),
    ("detection-test-lambda-normal-7", "normal", "bursty", None, 40, 1),
    ("detection-test-lambda-normal-8", "normal", "bursty", None, 40, 1),
]

BURST_INTERVAL_SEC = 300
MAX_EVENT_AGE_SEC = 60
POST_BURST_WAIT_SEC = 180
SHARED_INVOKE_POOL_SIZE = 60  # lambda_throttle_repeated_trial.py에서 확인된 스레드 폭증 방지값

LOG_DIR = PROJECT_ROOT / "playground" / "eval_outputs" / "logs"
RESULT_DIR = PROJECT_ROOT / "playground" / "eval_outputs"

logger = logging.getLogger("lambda_throttle_diverse_trial")


def _setup_logging() -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = LOG_DIR / f"lambda_throttle_diverse_trial_{ts}.log"
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


def _snapshot_path(function_name: str) -> Path:
    return RESULT_DIR / f".lambda_throttle_diverse_trial_snapshot__{function_name}.json"


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
        logger.warning("[원복 %s] 저장된 스냅샷 없음 — 미설정으로 정리", function_name)
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
    logger.info("[%s] 완료 — label=%s profile=%s anomaly_flag=%s anomaly_type=%s action=%s detected=%s",
                function_name, label, profile, result.get("anomaly_flag"), result.get("anomaly_type"),
                result.get("selected_action"), detected)

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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--restore", action="store_true")
    args = parser.parse_args()

    if not args.run and not args.restore:
        parser.error("--run 또는 --restore 중 하나는 지정해야 함")

    _setup_logging()

    if args.restore:
        for fn, _, _, conc, _, _ in TARGETS:
            if conc is not None:
                restore(fn)
        return

    logger.info("=== 다양화 시행 시작 (13개 함수, anomaly=5 severity 3단계 / normal=8 volume 4단계) ===")
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
        "scenario": "lambda_throttle_retry_storm_diverse",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "params": {
            "n_anomaly": 5, "n_normal": 8,
            "anomaly_profiles": {"severe": "concurrency=1", "moderate": "concurrency=2", "mild": "concurrency=3"},
            "normal_profiles": {"light": "5x3", "moderate": "15x3", "heavy": "30x3", "bursty": "40x1"},
        },
        "metrics": metrics,
        "trials": trials,
    }

    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = RESULT_DIR / f"lambda_throttle_diverse_trial_{datetime.now().strftime('%Y%m%d')}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    logger.info("=== 결과 ===")
    logger.info(json.dumps(metrics, ensure_ascii=False, indent=2))
    for t in trials:
        logger.info("  %s (label=%s profile=%s): anomaly_flag=%s detected=%s",
                     t["resource_id"], t["label"], t["profile"], t.get("anomaly_flag"), t["detected"])
    logger.info("결과 저장: %s", out_path)


if __name__ == "__main__":
    main()
