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
  python playground/lambda_retry_repeated_trial.py --run --function-set teammate
  (기본: n_points=30 * period_seconds=300 = 2.5시간 창 전체, 마지막 3개 구간만 에러 폭증)

[생성 파일]
  playground/eval_outputs/lambda_retry_repeated_trial__...json
  playground/eval_outputs/logs/lambda_retry_repeated_trial_{시각}.log
"""

from __future__ import annotations

SCRIPT_VERSION = "2"
# v1 (2026-09-10): 최초 작성 — 13개 함수 병렬, detection_node() 직접 호출, 결과 스키마는
#   s3_repeated_trial.py v5와 동일.
# v2 (2026-09-10): 세 가지를 고침.
#   (a) 트래픽이 마지막 3개 구간(--phase-minutes 15)에만 들어가고 앞 27개 구간이 비어
#       있었음 — s3_repeated_trial.py가 v2->v3에서 고친 것과 똑같은 구조적 결함이다
#       (배경이 0이면 무슨 트래픽이든 "0에서 급증"으로 보여 탐지율이 실제보다 좋게 나옴).
#       실제로 팀원 계정 대상 함수들은 30개 구간 중 29개가 0이었다. 이제 n_points(30) *
#       period_seconds(300) = 2.5시간 창 전체에 구간별로 트래픽을 쏘고, anomaly는 앞
#       (n_points - spike_periods)개 구간을 평상시 에러율(--baseline-error-rate)로,
#       마지막 spike_periods(기본 3 = _lambda_error_rate_check의 k)개 구간만 목표
#       에러율로 폭증시킨다. normal은 30개 구간 전부 평상시 수준을 유지한다.
#   (b) 탐지와 트래픽 생성이 서로 다른 AWS 계정을 볼 수 있었음. 탐지는 기본 세션(=.env의
#       AWS_ACCESS_KEY_ID/SECRET이 있으면 그쪽, 없으면 AWS_PROFILE)을 쓰는데 invoke는
#       profile_name="default"로 고정돼 있어서, .env에 다른 계정 키가 들어오면 "A계정
#       Lambda를 호출하고 B계정 CloudWatch를 읽는" 측정이 된다(실측으로 확인). 이제
#       --invoke-profile env(기본)로 탐지와 같은 자격증명을 쓰고, 시작 전에 두 경로의
#       STS Account를 비교해서 다르면 즉시 중단한다.
#   (c) 대상 함수 세트를 --function-set으로 고를 수 있게 함(own / teammate). 이름 규칙이
#       계정마다 다르고, 에러 유발 payload 키도 핸들러마다 다르다(own: force_error,
#       teammate: fail) — 두 키를 같이 보내서 어느 쪽 핸들러든 예외를 던지게 한다.
#       (키가 안 맞으면 에러가 0건이라 2.5시간을 통째로 날린다.)

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

# ⚠️ 트래픽 생성(lambda:InvokeFunction)과 탐지(CloudWatch 조회)의 자격증명 (v2에서 변경).
#   원래는 탐지=.env 프로필 / 유발=profile "default" 로 **고정**돼 있었다. 내 계정 하나만
#   쓸 때는 맞는 얘기였지만(DetectionRuntimeRole에 lambda:InvokeFunction이 없어서 함수를
#   만든 프로필로 호출해야 함), .env에 다른 계정의 액세스 키가 들어오면 기본 세션만 그
#   계정으로 바뀌고 invoke는 여전히 내 계정을 때려서 **호출 대상과 관측 대상이 다른 계정이
#   되는** 사고가 난다(실측 확인). 그래서
#     - 기본값을 "env"(= 기본 세션 = 탐지와 완전히 같은 자격증명)로 바꾸고,
#     - 시작 전에 두 경로의 STS Account를 비교해서 다르면 즉시 중단한다(_assert_same_account).
#   내 계정에서 detection-runtime 롤로 탐지하면서 invoke만 default 프로필로 하고 싶으면
#   --invoke-profile default 를 주면 된다(같은 계정이므로 가드를 통과한다).
INVOKE_PROFILE_DEFAULT = "env"

# 에러 유발 payload. 계정마다 핸들러가 보는 키가 다르다:
#   내 계정   detection-test-lambda*        -> event.get("force_error")
#   팀원 계정 detection-trial-lambda-*      -> event.get("fail")
# 둘 다 넣어서 어느 쪽 핸들러든 예외를 던지게 한다. (키가 안 맞으면 핸들러가 그냥 200을
# 돌려주고 Errors 지표가 0이라, 2.5시간을 다 돌리고 나서야 실패를 알게 된다.)
ERROR_PAYLOAD = b'{"fail": true, "force_error": true}'
OK_PAYLOAD = b"{}"

# 폭증 구간에서 함수당 최소 호출 수(구간당). detection_agent._lambda_error_rate_check는
# LAMBDA_ERROR_RATE_MIN_INVOCATIONS(=10) 미만 구간을 노이즈로 보고 아예 게이트에서
# 거른다. 간격에 ±15% 지터를 주면 "간격 30초 = 정확히 10회"짜리 설정이 8회로 떨어져서
# 폭증을 걸어도 절대임계값이 영영 안 걸리는 일이 생긴다 — 그래서 anomaly 함수는 지터를
# 준 뒤 간격을 이 값 기준으로 한 번 더 조여준다(정상군은 조이지 않는다: 호출량이 적은
# 정상 함수가 게이트에 걸러지는 것 자체가 검증 대상이다).
MIN_CALLS_PER_SPIKE_PERIOD = 12

# (라벨, 목표 에러율, 호출 간격(초)) — 계정과 무관한 실험 설계.
# 에러율/간격을 전부 다르게 준 이유는 위 주석의 "독립 시행" 참고.
# 정상군도 완전히 조용한 게 아니라 실제 트래픽이 있고 에러율만 낮다(0~3%) — 이래야
# "0과 0 아님"이 아니라 "정상 트래픽과 에러 폭증"을 구분하는지를 검증하게 된다.
TRIAL_SPEC = [
    ("anomaly", 0.55, 20),
    ("anomaly", 0.70, 30),
    ("anomaly", 0.85, 12),
    ("anomaly", 0.60, 12),
    ("anomaly", 0.90, 25),
    ("normal",  0.00, 30),
    ("normal",  0.02, 30),
    ("normal",  0.00, 15),
    ("normal",  0.03, 15),
    ("normal",  0.00, 8),
    ("normal",  0.01, 8),
    ("normal",  0.00, 60),
    ("normal",  0.02, 20),
]

# 계정별 함수 이름. 설계(TRIAL_SPEC)는 같고 이름만 갈아끼운다.
#   own      : 내 계정(268140507066)에 있는 기존 함수들. anomaly는 1번만 접미사가 없다.
#   teammate : 팀원 계정(634236767974)에 이미 만들어져 있는 세트. 0-base 연번.
FUNCTION_SETS = {
    "own": (
        ["detection-test-lambda"] + [f"detection-test-lambda-anomaly-{i}" for i in range(2, 6)],
        [f"detection-test-lambda-normal-{i}" for i in range(1, 9)],
    ),
    "teammate": (
        [f"detection-trial-lambda-anomaly-{i}" for i in range(5)],
        [f"detection-trial-lambda-normal-{i}" for i in range(8)],
    ),
}


def build_function_config(function_set: str) -> list[tuple[str, str, float, int]]:
    """(함수명, 라벨, 목표 에러율, 호출 간격) 리스트를 만든다."""
    anomaly_names, normal_names = FUNCTION_SETS[function_set]
    names_by_label = {"anomaly": list(anomaly_names), "normal": list(normal_names)}
    cfg = []
    for label, err, interval in TRIAL_SPEC:
        cfg.append((names_by_label[label].pop(0), label, err, interval))
    leftover = {k: v for k, v in names_by_label.items() if v}
    if leftover:
        raise ValueError(f"TRIAL_SPEC과 함수 개수가 안 맞음: 남은 함수 {leftover}")
    return cfg

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


# ── 자격증명 가드 ────────────────────────────────────────────────────────────

def _account_of(session: boto3.Session | None) -> tuple[str, str]:
    sts = (session or boto3).client("sts", region_name=AWS_REGION)
    ident = sts.get_caller_identity()
    return ident["Account"], ident["Arn"]


def _assert_same_account(invoke_session: boto3.Session | None) -> tuple[str, str]:
    """탐지(기본 세션)와 트래픽 생성(invoke_session)이 같은 계정인지 확인한다.

    다르면 "A계정 Lambda를 호출하고 B계정 CloudWatch를 읽는" 측정이 되는데, 그러면
    호출을 아무리 해도 관측되는 지표는 0이라 전 시행이 조용히 FN으로 나온다.
    """
    detect_acct, detect_arn = _account_of(None)
    invoke_acct, invoke_arn = _account_of(invoke_session)
    logger.info("탐지 자격증명 : %s (account %s)", detect_arn, detect_acct)
    logger.info("유발 자격증명 : %s (account %s)", invoke_arn, invoke_acct)
    if detect_acct != invoke_acct:
        logger.error("탐지 계정(%s)과 트래픽 생성 계정(%s)이 다름 — 호출 대상과 관측 대상이 "
                     "달라서 측정이 성립하지 않는다. --invoke-profile 을 맞추거나 .env의 "
                     "AWS_ACCESS_KEY_ID/AWS_PROFILE 중 하나를 정리할 것.", detect_acct, invoke_acct)
        sys.exit(1)
    return detect_acct, detect_arn


# ── 시행 (anomaly / normal 공통 — 차이는 "마지막 3개 구간의 에러율"뿐) ────────────
# S3와 달리 Lambda는 "이상"과 "정상"이 같은 종류의 트래픽이고 에러 비율만 다르다.
# 그래서 유발 로직을 하나로 두고 error_rate로만 구분한다.
#
# ⚠️ [v1 -> v2에서 수정] 원래는 마지막 15분(3개 구간)에만 트래픽을 넣고 나머지 27개
# 구간은 손대지 않았다. 탐지는 최근 30포인트(2.5시간) 창을 보는데 그 앞부분이 0으로
# 비어 있으면, z-score가 보는 기준선(평균/표준편차)이 "거의 0"이라 어떤 트래픽이든
# 기계적으로 크게 튄다 — 즉 "정상 트래픽과 에러 폭증을 구분하는지"가 아니라 "0과
# 0 아님을 구분하는지"만 재게 된다(s3_repeated_trial.py v2->v3 주석과 같은 문제).
# 이제 창 전체(n_points개 구간)에 구간별로 트래픽을 쏘고, anomaly는 앞
# (n_points - spike_periods)개 구간을 baseline_error_rate로, 마지막 spike_periods개
# 구간만 목표 에러율로 올린다. normal은 30개 구간 전부 평상시 수준을 유지한다.
#
# ⚠️ 비동기(Event) 호출이라 에러가 나면 Lambda가 기본 2회까지 자동 재시도하고, 그
# 재시도도 Invocations/Errors에 그대로 잡힌다 — 이게 바로 이 시나리오가 재현하려는
# "재시도 폭증"이므로 의도된 동작이다(강제로 만든 에러율보다 실제 관측 에러율이 더
# 높게 나오는 이유).

def _run_trial(function_name: str, label: str, rep: int, error_rate: float,
               interval_sec: int, n_points: int, period_seconds: int,
               spike_periods: int, wait_sec: int,
               invoke_session: boto3.Session | None = None,
               baseline_error_rate: float = 0.02,
               jitter_ratio: float = 0.15) -> dict:
    """error_rate/간격에 ±jitter를 줘서 같은 그룹 안에서도 동일 반복이 되지 않게 한다."""
    t0 = time.time()
    # 트래픽 생성 클라이언트. invoke_session=None이면 기본 세션(=탐지와 동일 자격증명).
    lam = (invoke_session or boto3).client("lambda", region_name=AWS_REGION)

    def _jitter(x: float) -> float:
        return x * random.uniform(1 - jitter_ratio, 1 + jitter_ratio)

    spike_rate = (min(0.98, max(0.0, _jitter(error_rate))) if error_rate > 0 else 0.0)
    # 정상 구간 에러율: anomaly 함수도 평상시엔 정상 서비스처럼 낮은 에러율을 갖는다.
    # normal 함수는 자기 설정 에러율(0~3%)을 30개 구간 내내 유지한다.
    base_rate = (min(0.98, max(0.0, _jitter(baseline_error_rate))) if label == "anomaly"
                 else spike_rate)

    actual_interval = max(2, round(_jitter(interval_sec)))
    if label == "anomaly":
        actual_interval = min(actual_interval, max(2, period_seconds // MIN_CALLS_PER_SPIKE_PERIOD))
    calls_per_period = max(1, period_seconds // actual_interval)

    logger.info("[%s %s] 시작 (구간 %d개 x %ds = %.1f시간, 구간당 %d회, 평상시 에러율 %.2f, "
                "마지막 %d개 구간 에러율 %.2f(기준 %.2f), 간격 %ds(기준 %ds))",
                label, function_name, n_points, period_seconds,
                n_points * period_seconds / 3600, calls_per_period, base_rate,
                spike_periods if label == "anomaly" else 0, spike_rate, error_rate,
                actual_interval, interval_sec)
    try:
        before = detect("Lambda", function_name, n_points, period_seconds)
        logger.info("[%s %s] before anomaly_flag=%s (z=%s, IF=%s)", label, function_name,
                    before["anomaly_flag"], before["anomaly_score_zscore"],
                    before["anomaly_score_iforest"])

        n_calls_total, n_err_total, n_invoke_failed = 0, 0, 0
        period_stats = []
        for period_idx in range(n_points):
            period_start = time.time()
            is_spike = (label == "anomaly") and period_idx >= (n_points - spike_periods)
            rate = spike_rate if is_spike else base_rate

            n_err = 0
            for i in range(calls_per_period):
                is_err = random.random() < rate
                try:
                    lam.invoke(FunctionName=function_name, InvocationType="Event",
                               Payload=ERROR_PAYLOAD if is_err else OK_PAYLOAD)
                    n_err += int(is_err)
                    n_calls_total += 1
                except Exception as exc:
                    n_invoke_failed += 1
                    logger.warning("[%s] invoke 실패: %s", function_name, exc)
                if i < calls_per_period - 1:
                    time.sleep(actual_interval)
            n_err_total += n_err
            period_stats.append({"period": period_idx, "spike": is_spike,
                                 "n_calls": calls_per_period, "n_forced_errors": n_err})

            elapsed = time.time() - period_start
            remaining = period_seconds - elapsed
            logger.debug("[%s %s] 구간 %d/%d(%s): %d회(에러 %d) 완료 (%.1fs), %.1fs 대기",
                         label, function_name, period_idx + 1, n_points,
                         "SPIKE" if is_spike else "정상", calls_per_period, n_err,
                         elapsed, max(0.0, remaining))
            if remaining > 0:
                time.sleep(remaining)

        logger.info("[%s] 호출 완료 (강제 에러 %d/%d, invoke 실패 %d), CloudWatch 반영 %d초 대기...",
                    function_name, n_err_total, n_calls_total, n_invoke_failed, wait_sec)
        time.sleep(wait_sec)

        after = detect("Lambda", function_name, n_points, period_seconds)
        logger.info("[%s %s] anomaly_flag=%s (z=%s, IF=%s, triggered=%s), %.1f분",
                    label, function_name, after["anomaly_flag"],
                    after["anomaly_score_zscore"], after["anomaly_score_iforest"],
                    after["triggered_metrics"], (time.time() - t0) / 60)

        return {
            "rep": rep,
            "function_name": function_name,
            "label": label,
            "target_error_rate": error_rate,
            "actual_error_rate": round(spike_rate, 4),
            "baseline_error_rate": round(base_rate, 4),
            "interval_sec": actual_interval,
            "calls_per_period": calls_per_period,
            "spike_periods": spike_periods if label == "anomaly" else 0,
            "period_stats": period_stats,
            "n_calls": n_calls_total,
            "n_errors": n_err_total,
            "n_invoke_failed": n_invoke_failed,
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

def result_filename(n_points: int, period_seconds: int, spike_periods: int,
                    n_normal: int, n_anomaly: int, function_set: str) -> Path:
    date_str = datetime.now().strftime("%Y%m%d")
    hours = n_points * period_seconds / 3600
    name = (f"lambda_retry_repeated_trial__window{hours:.1f}h_spike{spike_periods}p_"
            f"{function_set}_n{n_normal}-{n_anomaly}_scriptv{SCRIPT_VERSION}_{date_str}.json")
    return RESULT_DIR / name


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="store_true", help="반복 실험 실행")
    parser.add_argument("--function-set", default="own", choices=sorted(FUNCTION_SETS),
                        help="대상 함수 세트. own=내 계정, teammate=팀원 계정 (기본 own)")
    parser.add_argument("--n-points", type=int, default=30,
                        help="탐지 윈도우 길이 = 트래픽을 쏘는 구간 수 (기본 30 = 실제 파이프라인과 동일)")
    parser.add_argument("--period-seconds", type=int, default=300, help="구간 길이 (기본 300초 = 5분)")
    parser.add_argument("--spike-periods", type=int, default=3,
                        help="마지막 몇 개 구간에 에러를 폭증시킬지 (기본 3 = "
                             "detection_agent.PERSISTENCE_WINDOW_POINTS)")
    parser.add_argument("--baseline-error-rate", type=float, default=0.02,
                        help="anomaly 함수의 '평상시' 에러율 (기본 0.02). 폭증 전 27개 구간에 적용")
    parser.add_argument("--wait-sec", type=int, default=120,
                        help="트래픽 종료 후 CloudWatch 반영 대기(초). 기본 120")
    parser.add_argument("--invoke-profile", default=INVOKE_PROFILE_DEFAULT,
                        help=f"트래픽 생성(invoke)에 쓸 AWS 프로필. 기본 {INVOKE_PROFILE_DEFAULT} "
                             "(= 탐지와 동일한 기본 세션). 탐지 계정과 다르면 실행을 거부한다")
    args = parser.parse_args()

    log_path = _setup_logging()
    logger.info("로그 파일: %s (SCRIPT_VERSION=%s)", log_path, SCRIPT_VERSION)

    if not args.run:
        parser.print_help()
        return

    invoke_session = None if args.invoke_profile == "env" else boto3.Session(profile_name=args.invoke_profile)

    # ── 사전 점검 1: 탐지 계정 == 트래픽 생성 계정 ────────────────────────────
    account_id, _ = _assert_same_account(invoke_session)

    functions = build_function_config(args.function_set)
    anomaly_cfg = [c for c in functions if c[1] == "anomaly"]
    normal_cfg = [c for c in functions if c[1] == "normal"]
    n_anomaly, n_normal = len(anomaly_cfg), len(normal_cfg)

    # ── 사전 점검 2: invoke 권한 + 에러 payload가 실제로 예외를 던지는가 ───────
    # 권한이 없으면 invoke가 전부 AccessDenied로 조용히 실패해서 2.5시간을 통째로 날린다
    # (실제로 한 번 겪음). 그리고 계정마다 핸들러가 보는 키가 달라서, 권한이 있어도
    # payload 키가 안 맞으면 핸들러가 200을 돌려주고 Errors 지표가 0으로 남는다 —
    # 이건 더 조용한 실패라서 동기 호출로 직접 확인한다.
    lam = (invoke_session or boto3).client("lambda", region_name=AWS_REGION)
    for fn_name, label, _, _ in functions:
        try:
            resp = lam.invoke(FunctionName=fn_name, InvocationType="RequestResponse",
                              Payload=ERROR_PAYLOAD)
        except Exception as exc:
            logger.error("사전 점검 실패 — '%s' invoke 불가: %s", fn_name, exc)
            logger.error("--invoke-profile 로 권한 있는 프로필을 지정하거나 IAM 정책에 "
                         "lambda:InvokeFunction 을 추가한 뒤 다시 실행하세요.")
            sys.exit(1)
        if not resp.get("FunctionError"):
            logger.error("사전 점검 실패 — '%s' 가 에러 payload(%s)에도 정상 응답함. "
                         "핸들러가 보는 키가 다른 것 같다(ERROR_PAYLOAD 주석 참고). "
                         "이대로 돌리면 Errors 지표가 0이라 전 시행이 FN으로 나온다.",
                         fn_name, ERROR_PAYLOAD.decode())
            sys.exit(1)
    logger.info("사전 점검 통과: 계정 %s, 함수 %d개 전부 invoke 가능 + 에러 payload 동작 확인",
                account_id, len(functions))

    total_hours = args.n_points * args.period_seconds / 3600
    logger.info("=== anomaly %d개 + normal %d개, 총 %d개 시행 동시 병렬 시작 "
                "(함수세트=%s, 시행당 약 %.1f시간 + 대기 %d초, 마지막 %d개 구간만 폭증) ===",
                n_anomaly, n_normal, n_anomaly + n_normal, args.function_set,
                total_hours, args.wait_sec, args.spike_periods)

    anomaly_results, normal_results = [], []
    with ThreadPoolExecutor(max_workers=n_anomaly + n_normal) as executor:
        futures = {}
        for rep, (fn_name, label, err, interval) in enumerate(anomaly_cfg):
            fut = executor.submit(_run_trial, fn_name, label, rep, err, interval,
                                  args.n_points, args.period_seconds, args.spike_periods,
                                  args.wait_sec, invoke_session, args.baseline_error_rate)
            futures[fut] = "anomaly"
        for rep, (fn_name, label, err, interval) in enumerate(normal_cfg):
            fut = executor.submit(_run_trial, fn_name, label, rep, err, interval,
                                  args.n_points, args.period_seconds, args.spike_periods,
                                  args.wait_sec, invoke_session, args.baseline_error_rate)
            futures[fut] = "normal"

        for future in as_completed(futures):
            label = futures[future]
            result = future.result()
            (anomaly_results if label == "anomaly" else normal_results).append(result)

    anomaly_results.sort(key=lambda r: r["rep"])
    normal_results.sort(key=lambda r: r["rep"])

    metrics = compute_confusion_metrics(anomaly_results, normal_results)
    logger.info("=== 결과 ===\n%s", json.dumps(metrics, ensure_ascii=False, indent=2))

    out_path = result_filename(args.n_points, args.period_seconds, args.spike_periods,
                               n_normal, n_anomaly, args.function_set)
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "script_version": SCRIPT_VERSION,
        "scenario": "lambda_error_retry_surge",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "params": {
            "function_set": args.function_set, "aws_account_id": account_id,
            "n_points": args.n_points, "period_seconds": args.period_seconds,
            "spike_periods": args.spike_periods,
            "baseline_error_rate": args.baseline_error_rate,
            "wait_sec": args.wait_sec,
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
