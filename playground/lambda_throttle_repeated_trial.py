"""
playground/lambda_throttle_repeated_trial.py

Lambda "스로틀(429)/시스템 에러 재시도 폭증" 시나리오의 실 AWS 반복 시행 실험.

lambda_retry_repeated_trial.py("에러로 인한 재시도" 시나리오)와 동일한 설계
철학 — 독립 시행을 위해 전용 함수를 여러 개 쓴다 — 을 따르되, 시행을 순차가
아니라 **병렬**로 돌린다. 전용 함수 13개(anomaly 5 + normal 8)가 이미
자체 계정에 있어서(다른 시나리오용으로 만들어둔 것 재사용) 가능하다.

[변수 통제]
  - anomaly 5개: Reserved Concurrency=1 + MaximumEventAgeInSeconds=60 설정 후
    40건 동시 버스트 x 3회(5분 간격) — lambda_throttle_retry_trial.py에서
    실측 검증된 방식 그대로.
  - normal 8개: 동시성 제한 **없음**(기본/미설정 그대로) + 완전히 동일한
    40건 동시 버스트 x 3회(5분 간격).
  두 그룹의 유일한 차이는 "동시성 제한 여부"뿐 — 호출 패턴/볼륨은 동일하게
  맞춰서, 스로틀 폭증 오탐(FP)이 "호출량 자체" 때문이 아니라 "동시성 소진"
  때문에만 나는지 확인할 수 있게 했다.

⚠️ 완전한 독립시행은 아님: 13개 함수가 서로 다르므로 함수 간 독립은 보장되고
   (같은 함수에 반복하는 것과 다름), 판정도 각자 CloudWatch 지표 기준이라
   교차 오염은 없다. 다만 lambda_retry_repeated_trial.py처럼 "같은 조건으로
   n_rep회 반복"하는 방식은 아니고 n=1(함수당 1회)이라는 점은 감안할 것 —
   신뢰구간 계산 방식(Clopper-Pearson)은 표본 크기(13)에 그대로 적용 가능하나
   반복 재현성 자체를 보는 건 아니다.

[실행 방법]
  python playground/lambda_throttle_repeated_trial.py --run
  python playground/lambda_throttle_repeated_trial.py --restore   # 중간에 죽었을 때

[생성 파일]
  playground/eval_outputs/lambda_throttle_repeated_trial_{날짜}.json
  playground/eval_outputs/logs/lambda_throttle_repeated_trial_{시각}.log
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
SETUP_PROFILE = "default"  # .env의 detection-runtime 최소권한 우회 (기존 스크립트들과 동일 이유)

ANOMALY_FUNCTIONS = [
    "detection-retry-storm-test",
    "detection-test-lambda-anomaly-2",
    "detection-test-lambda-anomaly-3",
    "detection-test-lambda-anomaly-4",
    "detection-test-lambda-anomaly-5",
]
NORMAL_FUNCTIONS = [f"detection-test-lambda-normal-{i}" for i in range(1, 9)]

BURST_SIZE = 40
N_BURSTS = 3
BURST_INTERVAL_SEC = 300
MAX_EVENT_AGE_SEC = 60
POST_BURST_WAIT_SEC = 180

# 13개 함수 x 40건을 각자 별도 스레드풀로 돌리면 최대 520 스레드까지 치솟아
# GIL 경합으로 사실상 멈추는 걸 실측으로 확인함(2026-09-13) — 전체가 공유하는
# 고정 크기 풀 하나로 invoke 호출을 전부 몰아서 실제 동시성을 이 값으로 제한한다.
SHARED_INVOKE_POOL_SIZE = 60

LOG_DIR = PROJECT_ROOT / "playground" / "eval_outputs" / "logs"
RESULT_DIR = PROJECT_ROOT / "playground" / "eval_outputs"

logger = logging.getLogger("lambda_throttle_repeated_trial")


def _setup_logging() -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = LOG_DIR / f"lambda_throttle_repeated_trial_{ts}.log"
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
    return RESULT_DIR / f".lambda_throttle_repeated_trial_snapshot__{function_name}.json"


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


def _burst_invoke(lam, function_name: str, invoke_pool: ThreadPoolExecutor) -> list[str]:
    """BURST_SIZE건 동시 호출을 N_BURSTS회, BURST_INTERVAL_SEC 간격으로 발사.
    anomaly/normal 공통 — 호출 패턴 자체는 완전히 동일하게 맞춘다.

    ⚠️ 2026-09-13 수정: 함수마다(13개) 매번 새 ThreadPoolExecutor(40)를 만들면
    13개가 동시에 돌 때 스레드가 13x40=520개까지 치솟아서 GIL 경합으로 거의
    멈추는 사고가 실제로 났다(단일 함수 테스트 때 40건에 15~20초였는데, 13개
    동시 실행에서는 몇 분째 함수당 4~5건만 나가고 멈춤을 실측으로 확인). 이제
    모든 invoke 호출이 프로세스 전체가 공유하는 하나의 고정 크기 풀
    (SHARED_INVOKE_POOL_SIZE)로 들어가게 해서, 실제 동시 실행 스레드 수를
    13개 함수와 무관하게 일정하게 유지한다.
    """
    burst_times = []

    def _invoke(i: int) -> bool:
        try:
            lam.invoke(FunctionName=function_name, InvocationType="Event", Payload=b"{}")
            return True
        except Exception as exc:
            logger.warning("  [%s] invoke #%d 실패: %s", function_name, i, exc)
            return False

    for b in range(N_BURSTS):
        burst_times.append(datetime.now(timezone.utc).isoformat())
        futures = [invoke_pool.submit(_invoke, i) for i in range(BURST_SIZE)]
        results = [f.result() for f in futures]
        logger.info("[%s] 버스트 %d/%d 완료: 성공 %d/%d",
                    function_name, b + 1, N_BURSTS, sum(results), BURST_SIZE)
        if b < N_BURSTS - 1:
            time.sleep(BURST_INTERVAL_SEC)
    return burst_times


def run_trial(function_name: str, label: str, invoke_pool: ThreadPoolExecutor) -> dict:
    lam = _setup_lambda_client()

    if label == "anomaly":
        snap = _snapshot(lam, function_name)
        _save_snapshot(function_name, snap)
        lam.put_function_event_invoke_config(
            FunctionName=function_name, MaximumEventAgeInSeconds=MAX_EVENT_AGE_SEC,
            MaximumRetryAttempts=2,
        )
        lam.put_function_concurrency(FunctionName=function_name, ReservedConcurrentExecutions=1)
        logger.info("[%s] anomaly 셋업 완료 (concurrency=1, maxEventAge=%ds)",
                    function_name, MAX_EVENT_AGE_SEC)
        time.sleep(3)
    else:
        logger.info("[%s] normal — 동시성 제한 없이 그대로 진행", function_name)

    burst_times = _burst_invoke(lam, function_name, invoke_pool)
    time.sleep(POST_BURST_WAIT_SEC)

    logger.info("[%s] measure() 시작", function_name)
    try:
        result = measure(function_name, "Lambda", bypass_approval_for_timing=True)
    except Exception as exc:
        logger.error("[%s] measure() 실패: %s", function_name, exc)
        result = {"resource_id": function_name, "resource_type": "Lambda", "error": str(exc)}

    if label == "anomaly":
        restore(function_name)

    detected = bool(result.get("anomaly_flag")) and result.get("anomaly_type") == "cost_spike"
    logger.info("[%s] 완료 — label=%s, anomaly_flag=%s, anomaly_type=%s, action=%s, qa_passed=%s, detected=%s",
                function_name, label, result.get("anomaly_flag"), result.get("anomaly_type"),
                result.get("selected_action"), result.get("qa_passed"), detected)

    return {**result, "label": label, "burst_times_utc": burst_times, "detected": detected,
            "measured_at": datetime.now(timezone.utc).isoformat()}


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
    parser.add_argument("--restore", action="store_true", help="anomaly 5개 수동 원복만 실행")
    args = parser.parse_args()

    if not args.run and not args.restore:
        parser.error("--run 또는 --restore 중 하나는 지정해야 함")

    _setup_logging()

    if args.restore:
        for fn in ANOMALY_FUNCTIONS:
            restore(fn)
        return

    all_targets = [(fn, "anomaly") for fn in ANOMALY_FUNCTIONS] + [(fn, "normal") for fn in NORMAL_FUNCTIONS]
    logger.info("=== 13개 함수 병렬 시행 시작 (anomaly=%d, normal=%d) ===",
                len(ANOMALY_FUNCTIONS), len(NORMAL_FUNCTIONS))

    trials = []
    try:
        with ThreadPoolExecutor(max_workers=SHARED_INVOKE_POOL_SIZE) as invoke_pool, \
             ThreadPoolExecutor(max_workers=len(all_targets)) as ex:
            futures = {ex.submit(run_trial, fn, label, invoke_pool): fn for fn, label in all_targets}
            for fut in as_completed(futures):
                trials.append(fut.result())
    finally:
        # 혹시 실패로 restore()가 안 불린 anomaly 함수가 있으면 여기서 한 번 더 정리
        for fn in ANOMALY_FUNCTIONS:
            if _snapshot_path(fn).exists():
                logger.warning("[%s] 정리 안 된 스냅샷 발견 — 재원복", fn)
                restore(fn)

    order = [fn for fn, _ in all_targets]
    trials.sort(key=lambda t: order.index(t["resource_id"]))
    metrics = compute_metrics(trials)

    out = {
        "script_version": "1",
        "scenario": "lambda_throttle_retry_storm",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "params": {
            "burst_size": BURST_SIZE, "n_bursts": N_BURSTS,
            "burst_interval_sec": BURST_INTERVAL_SEC, "max_event_age_sec": MAX_EVENT_AGE_SEC,
            "n_anomaly": len(ANOMALY_FUNCTIONS), "n_normal": len(NORMAL_FUNCTIONS),
        },
        "metrics": metrics,
        "trials": trials,
    }

    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = RESULT_DIR / f"lambda_throttle_repeated_trial_{datetime.now().strftime('%Y%m%d')}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    logger.info("=== 결과 ===")
    logger.info(json.dumps(metrics, ensure_ascii=False, indent=2))
    logger.info("결과 저장: %s", out_path)


if __name__ == "__main__":
    main()
