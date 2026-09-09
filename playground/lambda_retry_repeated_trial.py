"""
playground/lambda_retry_repeated_trial.py

Lambda "에러 재시도 폭증" 시나리오의 실 AWS 반복 시행 실험 (탐지 담당 A).

s3_repeated_trial.py(v5)와 **동일한 구조/동일한 결과 스키마**를 따른다 — 다섯 명의
결과를 하나로 합칠 때 스키마가 같아야 analyze_iforest_vs_zscore.py /
measure_classification_accuracy.py를 그대로 쓸 수 있기 때문이다. 바뀐 것은
"이상을 어떻게 유발하는가"뿐:
  - S3  : 버킷에 GET 폭증을 발생
  - 여기: Lambda 함수를 실제로 호출하되, 일정 비율을 강제 에러로 만든다
          (payload {"force_error": true} → 핸들러가 예외를 던짐 → Errors 지표 상승)

⚠️ 판정은 반드시 실제 프로덕션 detection_node()를 그대로 호출해서 얻는다.
   (예전 phase_g의 _detect() 헬퍼는 persistence/절대임계값/cost를 빼먹은 낡은 방식이라
    프로덕션과 결과가 달랐다 — s3_repeated_trial.py v4 주석 참고.)

⚠️ 독립 시행을 위해 함수를 13개로 분리한다. CloudWatch 지표는 함수 단위로 집계되므로,
   한 함수에 13번 반복하면 앞 시행의 트래픽이 뒤 시행의 창에 그대로 남아 시행이
   독립이 아니게 되고 Clopper-Pearson CI의 전제가 깨진다.
   또한 같은 그룹 안에서도 에러율/호출간격을 서로 다르게 준다 — 13개가 전부 같은
   설정이면 "1개 패턴을 13번 복사"한 것이라 역시 독립 시행이 아니다.

[실행 방법]
  python playground/lambda_retry_repeated_trial.py --run
  python playground/lambda_retry_repeated_trial.py --run --phase-minutes 15

[생성 파일]
  playground/eval_outputs/lambda_retry_repeated_trial__...json
  playground/eval_outputs/logs/lambda_retry_repeated_trial_{시각}.log
"""

from __future__ import annotations

SCRIPT_VERSION = "1"

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

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

import boto3
from scipy.stats import beta as _beta_dist

import pipeline.detection_agent as da
from pipeline.orchestrator import assemble_resource

# ── 설정값 ────────────────────────────────────────────────────────────────────

AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2")

# ⚠️ 트래픽 생성(lambda:InvokeFunction)과 탐지(CloudWatch 조회)는 서로 다른 자격증명을 쓴다.
#   - 탐지 : .env의 AWS_PROFILE(=detection-runtime) 그대로. 프로덕션이 실제로 쓰는 권한으로
#            재야 의미가 있으므로 절대 바꾸지 않는다.
#   - 유발 : DetectionRuntimeRole에는 lambda:InvokeFunction 권한이 없다(실측 확인:
#            AccessDeniedException). 트래픽 발생은 제품 동작이 아니라 실험 장치이므로,
#            함수를 만든 것과 같은 프로필로 호출한다.
INVOKE_PROFILE_DEFAULT = "default"

# (함수명, 라벨, 목표 에러율, 호출 간격(초))
# 에러율/간격을 전부 다르게 준 이유는 위 주석의 "독립 시행" 참고.
# 정상군도 완전히 조용한 게 아니라 실제 트래픽이 있고 에러율만 낮다(0~3%) — 이래야
# "0과 0 아님"이 아니라 "정상 트래픽과 에러 폭증"을 구분하는지를 검증하게 된다.
LAMBDA_FUNCTIONS = [
    ("detection-test-lambda",           "anomaly", 0.55, 20),
    ("detection-test-lambda-anomaly-2", "anomaly", 0.70, 30),
    ("detection-test-lambda-anomaly-3", "anomaly", 0.85, 12),
    ("detection-test-lambda-anomaly-4", "anomaly", 0.60, 12),
    ("detection-test-lambda-anomaly-5", "anomaly", 0.90, 25),
    ("detection-test-lambda-normal-1",  "normal",  0.00, 30),
    ("detection-test-lambda-normal-2",  "normal",  0.02, 30),
    ("detection-test-lambda-normal-3",  "normal",  0.00, 15),
    ("detection-test-lambda-normal-4",  "normal",  0.03, 15),
    ("detection-test-lambda-normal-5",  "normal",  0.00, 8),
    ("detection-test-lambda-normal-6",  "normal",  0.01, 8),
    ("detection-test-lambda-normal-7",  "normal",  0.00, 60),
    ("detection-test-lambda-normal-8",  "normal",  0.02, 20),
]

LOG_DIR = PROJECT_ROOT / "playground" / "eval_outputs" / "logs"
RESULT_DIR = PROJECT_ROOT / "playground" / "eval_outputs"

logger = logging.getLogger("lambda_retry_repeated_trial")


def _setup_logging() -> Path:
    """콘솔 + 파일 동시 로깅. 파일 경로를 반환한다."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = LOG_DIR / f"lambda_retry_repeated_trial_{ts}.log"
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)

    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.addHandler(fh)
    logger.addHandler(sh)
    logger.propagate = False
    return log_path


# ── 탐지 (실제 프로덕션 detection_node()를 그대로 호출) ─────────────────────────
# s3_repeated_trial.py v5의 detect()와 동일한 구조. assemble_resource()가 cost까지
# 채운 raw_metrics를 만들고, 그걸 detection_node()에 그대로 넣는다.

def detect(resource_type: str, resource_id: str, n_points: int = 30,
           period_seconds: int = 300) -> dict:
    """실제 프로덕션 detection_node() 그대로 호출 + 원본 raw_metrics 전체를 함께 반환."""
    assembled = assemble_resource(resource_id, resource_type,
                                  n_points=n_points, period_seconds=period_seconds)
    raw_metrics = assembled["raw_metrics"]

    state = {
        "trace_id": None,
        "resource_id": resource_id,
        "resource_type": resource_type,
        "raw_metrics": raw_metrics,
        "timestamp": None,
        # Lambda는 나이가드(_low_utilization_check)를 안 쓰지만, assemble_resource가
        # 채워주면 그대로 넘긴다 (EC2 전용 필드라 Lambda에선 보통 None).
        "resource_age_seconds": assembled.get("resource_age_seconds"),
        "anomaly_flag": False,
        "anomaly_score_zscore": None,
        "anomaly_score_iforest": None,
        "triggered_metrics": [],
    }
    result_state = da.detection_node(state)

    return {
        "resource_id": resource_id,
        "raw_metrics": raw_metrics,  # 요약이 아니라 전체 원본 (cost 포함)
        "anomaly_flag": result_state["anomaly_flag"],
        "anomaly_score_zscore": result_state["anomaly_score_zscore"],
        "anomaly_score_iforest": result_state["anomaly_score_iforest"],
        "triggered_metrics": result_state["triggered_metrics"],
    }


# ── 시행 (anomaly / normal 공통 — 차이는 에러율뿐) ────────────────────────────
# S3와 달리 Lambda는 "이상"과 "정상"이 같은 종류의 트래픽이고 에러 비율만 다르다.
# 그래서 유발 로직을 하나로 두고 error_rate로만 구분한다.

def _run_trial(function_name: str, label: str, rep: int, error_rate: float,
               interval_sec: int, phase_minutes: int, wait_sec: int,
               invoke_profile: str = INVOKE_PROFILE_DEFAULT,
               jitter_ratio: float = 0.15) -> dict:
    """error_rate/간격에 ±jitter를 줘서 같은 그룹 안에서도 동일 반복이 되지 않게 한다."""
    t0 = time.time()
    # 트래픽 생성 전용 클라이언트 — 탐지 경로(detect())는 .env 프로필을 그대로 쓴다.
    lam = boto3.Session(profile_name=invoke_profile).client("lambda", region_name=AWS_REGION)

    actual_rate = (min(0.98, max(0.0, error_rate * random.uniform(1 - jitter_ratio, 1 + jitter_ratio)))
                   if error_rate > 0 else 0.0)
    actual_interval = max(5, round(interval_sec * random.uniform(1 - jitter_ratio, 1 + jitter_ratio)))
    n_calls = int(phase_minutes * 60 / actual_interval)

    logger.info("[%s %s] 시작 (에러율=%.2f(기준 %.2f), 간격=%ds(기준 %ds), 호출 %d회)",
                label, function_name, actual_rate, error_rate, actual_interval, interval_sec, n_calls)
    try:
        before = detect("Lambda", function_name)

        n_err = 0
        for _ in range(n_calls):
            is_err = random.random() < actual_rate
            payload = b'{"force_error": true}' if is_err else b'{}'
            try:
                lam.invoke(FunctionName=function_name, InvocationType="Event", Payload=payload)
                n_err += int(is_err)
            except Exception as exc:
                logger.warning("[%s] invoke 실패: %s", function_name, exc)
            time.sleep(actual_interval)

        logger.info("[%s] 호출 완료 (에러 %d/%d), CloudWatch 반영 %d초 대기...",
                    function_name, n_err, n_calls, wait_sec)
        time.sleep(wait_sec)

        after = detect("Lambda", function_name)
        logger.info("[%s %s] anomaly_flag=%s (z=%s, IF=%s, triggered=%s), %.1fs",
                    label, function_name, after["anomaly_flag"],
                    after["anomaly_score_zscore"], after["anomaly_score_iforest"],
                    after["triggered_metrics"], time.time() - t0)

        return {
            "rep": rep,
            "function_name": function_name,
            "label": label,
            "target_error_rate": error_rate,
            "actual_error_rate": round(actual_rate, 4),
            "interval_sec": actual_interval,
            "n_calls": n_calls,
            "n_errors": n_err,
            "before": before,
            "after": after,
            "detected": bool(after.get("anomaly_flag")),
            "elapsed_sec": round(time.time() - t0, 1),
        }
    except Exception as exc:
        logger.error("[%s] 실패: %s\n%s", function_name, exc, traceback.format_exc())
        return {"rep": rep, "function_name": function_name, "label": label,
                "error": str(exc), "detected": None}


# ── 지표 계산 (s3_repeated_trial.py v5와 동일 — 합산 시 정의가 같아야 함) ──────

def clopper_pearson_ci(successes: int, n: int, confidence: float = 0.95) -> tuple[float, float]:
    """양측 Clopper-Pearson 정확 이항 신뢰구간. (실패=0인 경우도 안전하게 처리)"""
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

def result_filename(phase_minutes: int, n_normal: int, n_anomaly: int) -> Path:
    date_str = datetime.now().strftime("%Y%m%d")
    name = (f"lambda_retry_repeated_trial__phase{phase_minutes}m_"
            f"n{n_normal}-{n_anomaly}_scriptv{SCRIPT_VERSION}_{date_str}.json")
    return RESULT_DIR / name


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="store_true", help="반복 실험 실행")
    parser.add_argument("--phase-minutes", type=int, default=15,
                        help="함수당 트래픽 발생 시간(분). 기본 15")
    parser.add_argument("--wait-sec", type=int, default=120,
                        help="트래픽 종료 후 CloudWatch 반영 대기(초). 기본 120")
    parser.add_argument("--n-points", type=int, default=30,
                        help="탐지 윈도우 길이 (기본 30 = 실제 파이프라인과 동일)")
    parser.add_argument("--period-seconds", type=int, default=300, help="구간 길이 (기본 300초 = 5분)")
    parser.add_argument("--invoke-profile", default=INVOKE_PROFILE_DEFAULT,
                        help=f"트래픽 생성(invoke)에 쓸 AWS 프로필. 기본 {INVOKE_PROFILE_DEFAULT} "
                             "(탐지는 .env의 AWS_PROFILE을 그대로 씀)")
    args = parser.parse_args()

    log_path = _setup_logging()
    logger.info("로그 파일: %s (SCRIPT_VERSION=%s)", log_path, SCRIPT_VERSION)

    if not args.run:
        parser.print_help()
        return

    # ── 사전 점검 ────────────────────────────────────────────────────────────
    # 권한이 없으면 invoke가 전부 AccessDenied로 조용히 실패해서 15분을 통째로 날린다
    # (실제로 한 번 겪음). 시작 전에 1회 호출로 확인하고, 안 되면 즉시 중단한다.
    try:
        probe = boto3.Session(profile_name=args.invoke_profile).client("lambda", region_name=AWS_REGION)
        probe.invoke(FunctionName=LAMBDA_FUNCTIONS[0][0], InvocationType="Event", Payload=b"{}")
        logger.info("사전 점검 통과: 프로필 '%s'로 invoke 가능", args.invoke_profile)
    except Exception as exc:
        logger.error("사전 점검 실패 — 프로필 '%s'로 invoke 불가: %s", args.invoke_profile, exc)
        logger.error("--invoke-profile 로 권한 있는 프로필을 지정하거나 IAM 정책에 "
                     "lambda:InvokeFunction 을 추가한 뒤 다시 실행하세요.")
        sys.exit(1)

    anomaly_cfg = [c for c in LAMBDA_FUNCTIONS if c[1] == "anomaly"]
    normal_cfg = [c for c in LAMBDA_FUNCTIONS if c[1] == "normal"]
    n_anomaly, n_normal = len(anomaly_cfg), len(normal_cfg)

    logger.info("=== anomaly %d개 + normal %d개, 총 %d개 시행 동시 병렬 시작 "
                "(시행당 약 %d분 + 대기 %d초) ===",
                n_anomaly, n_normal, n_anomaly + n_normal, args.phase_minutes, args.wait_sec)

    anomaly_results, normal_results = [], []
    with ThreadPoolExecutor(max_workers=n_anomaly + n_normal) as executor:
        futures = {}
        for rep, (fn_name, label, err, interval) in enumerate(anomaly_cfg):
            fut = executor.submit(_run_trial, fn_name, label, rep, err, interval,
                                  args.phase_minutes, args.wait_sec, args.invoke_profile)
            futures[fut] = "anomaly"
        for rep, (fn_name, label, err, interval) in enumerate(normal_cfg):
            fut = executor.submit(_run_trial, fn_name, label, rep, err, interval,
                                  args.phase_minutes, args.wait_sec, args.invoke_profile)
            futures[fut] = "normal"

        for future in as_completed(futures):
            label = futures[future]
            result = future.result()
            (anomaly_results if label == "anomaly" else normal_results).append(result)

    anomaly_results.sort(key=lambda r: r["rep"])
    normal_results.sort(key=lambda r: r["rep"])

    metrics = compute_confusion_metrics(anomaly_results, normal_results)
    logger.info("=== 결과 ===\n%s", json.dumps(metrics, ensure_ascii=False, indent=2))

    out_path = result_filename(args.phase_minutes, n_normal, n_anomaly)
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "script_version": SCRIPT_VERSION,
        "scenario": "lambda_error_retry_surge",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "params": {
            "phase_minutes": args.phase_minutes, "wait_sec": args.wait_sec,
            "n_points": args.n_points, "period_seconds": args.period_seconds,
            "n_normal": n_normal, "n_anomaly": n_anomaly,
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
