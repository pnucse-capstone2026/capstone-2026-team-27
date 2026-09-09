"""
playground/s3_repeated_trial.py

S3 대량다운로드 시나리오를 anomaly n회 + normal n회 반복 실행해서
TP/TN/FP/FN, accuracy, recall(+ Clopper-Pearson 95% CI)을 계산한다.

설계:
- anomaly 시행(GET 폭증)은 버킷을 여러 개(N_ANOMALY_BUCKETS) 병렬로 써서 실행한다.
  같은 버킷에 여러 시행을 동시에 걸면 CloudWatch 지표가 하나로 합쳐져서 "5번의
  독립 시행"이 아니라 "1번의 5배 큰 시행"이 되어버리기 때문 — 그래서 시행마다
  독립된 버킷 + Request Metrics(EntireBucket 필터)가 필요하다.
- normal 시행도 anomaly와 마찬가지로 독립 버킷 여러 개(N_NORMAL_BUCKETS)에 "평상시
  수준"의 실제 GET 트래픽을 만들어서 확인한다. generate_mock_s3.py가 정의한 정상
  기준(5분당 500건 * 시간대 배율 0.5~1.0)을 그대로 따른다 — 완전한 침묵(0건)을
  정상으로 두면 실제 운영 트래픽이 있는 상황에서의 오탐 방어력을 검증 못 하기
  때문. anomaly와 normal 전부(총 n_anomaly+n_normal개 버킷)를 하나의 스레드풀로
  동시 병렬 실행한다.
- 매 rep마다 n_gets/요청수에 ±노이즈를 줘서 완전히 동일한 값의 반복이 되지 않게
  한다 (동일 반복이면 분산이 0이 되어 반복실험의 의미가 없어짐).
- "탐지 성공"은 anomaly_flag(z-score OR iforest, 실제 파이프라인과 동일)를
  대표 지표로 쓰되, z_triggered/iforest_triggered도 각 시행별로 따로 남긴다.
- 신뢰구간은 Clopper-Pearson 정확 이항 신뢰구간(양측 95%)으로 통일 — 정규근사
  방식(mean±SD)은 비율 지표에 쓰면 n이 작을 때 100% 초과/음수 구간이 나올 수
  있어 부적절하다. z_max/iforest_score 같은 연속값에는 대신 mean±SD를 쓴다
  (playground/eval_outputs/scenario_repeatability_result.json과 동일 방식).

[실행 방법]
  1단계(버킷 준비, 최초 1회만):
    python playground/s3_repeated_trial.py --setup --n-anomaly 5 --n-normal 8
  2단계(반복 실험 실행, anomaly+normal 전부 동시 병렬):
    python playground/s3_repeated_trial.py --run --n-anomaly 5 --n-normal 8

[생성 파일]
  - 결과: playground/eval_outputs/s3_repeated_trial__ngets{N}_objsize{S}kb_wait{W}s_n{정상}-{이상}_scriptv{V}_{YYYYMMDD}.json
  - 로그: playground/eval_outputs/logs/s3_repeated_trial_{timestamp}.log (콘솔과 동시 출력)
"""

from __future__ import annotations

SCRIPT_VERSION = "5"
# v1 (2026-09-09): 최초 작성 — anomaly 병렬(버킷 N개) + normal 과거 조용한 윈도우 재사용,
#   Clopper-Pearson 95% CI, 상세 로깅(파일+콘솔), 파라미터 내장 파일명.
# v2 (2026-09-09): normal도 "완전 침묵"이 아니라 mock과 동일한 정상 트래픽 수준(500건
#   *시간대배율)을 실제로 발생시켜 측정하도록 변경 — 침묵만 검증하는 건 실제 운영
#   상황의 오탐 방어력을 못 보여줌. normal도 독립 버킷 병렬화, anomaly에 rep별
#   ±15% 노이즈 추가.
# v3 (2026-09-09): v2를 드라이런으로 실제 실행해보니 normal 8개가 전부 오탐(FP 100%,
#   z_max 전부 5.3852로 동일)이 나옴 — 마지막 5분 구간에만 트래픽을 넣고 나머지
#   29개 구간은 여전히 0이라, "텅 빈 배경 대비 극단적 스파이크"로 보이는 구조적
#   결함이었음. normal/anomaly 둘 다 n_points(기본 30)*period_seconds(기본 300초)
#   =2.5시간 창 "전체"에 5분마다 계속 트래픽을 쏘도록 변경(anomaly는 앞 27구간
#   평상시 + 마지막 3구간만 스파이크, generate_mock_s3.py의 generate_anomaly_s3_window
#   와 동일 구조). 대신 시행 하나에 2.5시간(n_points*period_seconds) 이상 걸림.
# v4 (2026-09-09): detect()가 z-score/IForest를 자체 재구현하고 있었는데, 이게 실제
#   detection_node()와 규칙이 달랐음(지속성 체크 없이 창 30개 중 아무 점이나 초과 시
#   트리거 / 알림용이 아니라 버퍼채택용 Z_SCORE 대상지표 사용 / IForest 마지막 1개
#   시점만 봄 / 절대임계값 체크 미호출 / cost 지표 안 채워짐 — 팀원이 발견). 이제
#   orchestrator.assemble_resource()로 cost까지 채운 뒤 pipeline.detection_agent.
#   detection_node()를 그대로 호출하도록 교체 — 프로덕션과 100% 동일한 판정 로직.
# v5 (2026-09-09): s3.get_object()만 호출하고 반환된 Body(StreamingBody)를 .read()
#   하지 않고 있었음 — boto3는 body를 즉시 다 받아오지 않고 스트림으로만 열어두므로,
#   .read() 없이는 실제로 전체 바이트를 네트워크로 끌어온다는 보장이 없다(실측으로
#   .read() 추가 후 정확히 요청 크기만큼 받아짐을 확인). number_of_requests는
#   올라가도 bytes_downloaded가 의도한 만큼 안 찍힐 위험이 있어 모든 get_object()
#   호출 뒤에 .read()를 붙임. mock의 AVG_BYTES_PER_REQUEST를 50KB로 맞춘 것과 함께,
#   총 전송량은 여전히 ~8.4GB로 무료 티어(100GB) 안.

import argparse
import json
import logging
import os
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

import random

import boto3
from scipy.stats import beta as _beta_dist

import pipeline.detection_agent as da
from pipeline.orchestrator import assemble_resource

# ── 설정값 ────────────────────────────────────────────────────────────────────

ANOMALY_BUCKET_PREFIX = "detection-trial-anomaly"  # 이 뒤에 -0, -1, ... 붙여서 생성
NORMAL_BUCKET_PREFIX = "detection-trial-normal"    # normal 시행도 실제 트래픽을 만들어야 해서
                                                     # 별도 버킷 필요 (anomaly와 동시 병렬 처리)
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2")

# generate_mock_s3.py와 동일한 "정상" 정의 — 5분당 500건 기준에 시간대별 배율(0.5~1.0)을
# 곱한 값. 완전히 조용한(0건) 상태가 아니라 이 정도의 꾸준한 트래픽이 있는 게 "정상"이다.
NORMAL_TRAFFIC_MULTIPLIERS = [0.5, 0.6, 0.8, 0.85, 0.9, 1.0, 1.0, 0.7]  # 8개 rep에 하나씩
NORMAL_BASE_REQUESTS_PER_WINDOW = 500

LOG_DIR = PROJECT_ROOT / "playground" / "eval_outputs" / "logs"
RESULT_DIR = PROJECT_ROOT / "playground" / "eval_outputs"

logger = logging.getLogger("s3_repeated_trial")


def _setup_logging() -> Path:
    """콘솔 + 파일 동시 로깅. 파일 경로를 반환한다."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = LOG_DIR / f"s3_repeated_trial_{ts}.log"

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


# ── S3 버킷 준비 (anomaly/normal 시행용, 최초 1회) ─────────────────────────────

def _anomaly_bucket_name(idx: int) -> str:
    return f"{ANOMALY_BUCKET_PREFIX}-{idx}"


def _normal_bucket_name(idx: int) -> str:
    return f"{NORMAL_BUCKET_PREFIX}-{idx}"


def _ensure_bucket_with_metrics(s3, name: str) -> None:
    try:
        s3.head_bucket(Bucket=name)
        logger.info("버킷 이미 존재: %s (생성 스킵)", name)
    except Exception:
        logger.info("버킷 생성: %s", name)
        if AWS_REGION == "us-east-1":
            s3.create_bucket(Bucket=name)
        else:
            s3.create_bucket(
                Bucket=name,
                CreateBucketConfiguration={"LocationConstraint": AWS_REGION},
            )

    # Request Metrics(EntireBucket 필터) 활성화 — number_of_requests/bytes_downloaded가
    # CloudWatch에 나오려면 반드시 필요 (cloudwatch_client.py 주석 참고).
    s3.put_bucket_metrics_configuration(
        Bucket=name,
        Id="EntireBucket",
        MetricsConfiguration={"Id": "EntireBucket"},
    )
    logger.info("Request Metrics(EntireBucket) 활성화: %s", name)


def setup_buckets(n_anomaly: int, n_normal: int) -> tuple[list[str], list[str]]:
    """anomaly n_anomaly개 + normal n_normal개 버킷을 생성하고 Request Metrics를 켠다."""
    s3 = boto3.client("s3", region_name=AWS_REGION)
    anomaly_names = [_anomaly_bucket_name(i) for i in range(n_anomaly)]
    normal_names = [_normal_bucket_name(i) for i in range(n_normal)]

    for name in anomaly_names + normal_names:
        _ensure_bucket_with_metrics(s3, name)

    logger.warning(
        "Request Metrics는 활성화 직후 바로 반영 안 될 수 있음 — "
        "최소 30분~1시간 정도 워밍업 후 실제 실험(--run)을 시작할 것을 권장."
    )
    return anomaly_names, normal_names


# ── 탐지 (실제 프로덕션 detection_node()를 그대로 호출) ─────────────────────────
# ⚠️ [v3 -> v4에서 수정] 원래 여기서 z_max/iforest_score를 직접 계산했는데, 이게
# 실제 detection_node()와 규칙이 달랐다:
#   - z-score: detection_node는 최근 3개 연속 초과(_zscore_check_persistent) 기준인데
#     여기는 창 30개 중 아무 점이나 초과하면 트리거(_zscore_max)로 계산 — 훨씬 느슨함
#   - z-score 대상 지표: detection_node는 Z_SCORE_TARGET_METRICS(알림용)인데 여기는
#     BUFFER_ZSCORE_TARGET_METRICS(버퍼 채택용, 다른 지표 집합)를 씀
#   - IForest: detection_node는 최근 3개 연속 기준인데 여기는 마지막 1개 시점만 봄
#   - 절대임계값 체크(_low_utilization_check/_lambda_error_rate_check)를 아예 호출 안 함
#   - cost 지표가 안 채워짐 (fetch_metrics는 cost를 안 주는데 estimate_cost_series를
#     안 불렀음 — Z_SCORE_TARGET_METRICS에 "cost"가 있어서 이것도 결과에 영향을 줌)
# 그래서 팀원의 eval_scenario_mock_detection_rates.py와 동일하게, orchestrator.
# assemble_resource()로 cost까지 포함한 raw_metrics를 만든 뒤 detection_node()를
# 직접 호출하는 방식으로 교체한다 — 프로덕션과 100% 동일한 판정 로직을 쓰게 됨.

def detect(resource_type: str, resource_id: str, n_points: int = 30, period_seconds: int = 300) -> dict:
    """실제 프로덕션 detection_node() 그대로 호출 + 원본 raw_metrics 전체를 함께 반환."""
    assembled = assemble_resource(resource_id, resource_type, n_points=n_points, period_seconds=period_seconds)
    raw_metrics = assembled["raw_metrics"]

    state = {
        "trace_id": None,
        "resource_id": resource_id,
        "resource_type": resource_type,
        "raw_metrics": raw_metrics,
        "timestamp": None,
        "resource_age_seconds": None,
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


# ── anomaly 시행 (27개 구간 정상 트래픽 + 마지막 3개 구간 폭증, generate_mock_s3.py의
#    generate_anomaly_s3_window와 동일한 구조) ──────────────────────────────────
# ⚠️ [v2 -> v3에서 수정] "이전(before)"이 텅 빈 배경이면 아무 신호나 다 이상으로
# 보여서 "진짜 폭증을 정상과 구분해내는지"를 검증하는 게 아니라 "0과 0 아님을
# 구분하는지"만 보게 된다 — normal 시행과 같은 이유로 여기도 실제 정상 배경을
# 먼저 깔아야 한다.

def run_anomaly_trial(bucket: str, rep: int, multiplier: float, object_size_bytes: int,
                       n_points: int = 30, period_seconds: int = 300,
                       spike_periods: int = 3, spike_multiplier_range: tuple[float, float] = (4.0, 6.0),
                       jitter_ratio: float = 0.15) -> dict:
    """앞쪽 (n_points-spike_periods)개 구간은 평상시 수준 트래픽, 마지막 spike_periods개
    구간만 base*4~6배로 폭증시킨다 (mock의 anomaly 윈도우 생성 방식과 동일)."""
    t0 = time.time()
    base = NORMAL_BASE_REQUESTS_PER_WINDOW * multiplier
    spike_level = base * random.uniform(*spike_multiplier_range)
    logger.info("[anomaly rep=%d] bucket=%s 시작 (평상시 ~%d건/구간 x %d개, 마지막 %d개는 폭증 ~%d건/구간)",
                rep, bucket, round(base), n_points - spike_periods, spike_periods, round(spike_level))
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        key = f"s3_repeated_trial_load_test_{rep}.bin"
        s3.put_object(Bucket=bucket, Key=key, Body=os.urandom(object_size_bytes))

        before = detect("S3", bucket, n_points, period_seconds)
        logger.debug("[anomaly rep=%d] before=%s", rep, json.dumps(before, ensure_ascii=False))

        period_counts = []
        for period_idx in range(n_points):
            period_start = time.time()
            is_spike = period_idx >= (n_points - spike_periods)
            if is_spike:
                # 마지막 spike_periods개는 거의 동일한 크기로 (지속성 체크 통과 조건,
                # generate_mock_s3.py 주석 참고 — 값이 들쭉날쭉하면 최솟값의 z가
                # 임계값을 못 넘겨 트리거가 안 되는 구조적 한계가 있음)
                n_requests = max(1, round(spike_level * random.uniform(0.98, 1.02)))
            else:
                n_requests = max(1, round(base * random.uniform(1 - jitter_ratio, 1 + jitter_ratio)))
            period_counts.append(n_requests)
            for _ in range(n_requests):
                s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            elapsed_this_period = time.time() - period_start
            remaining = period_seconds - elapsed_this_period
            logger.debug("[anomaly rep=%d] 구간 %d/%d(%s): %d건 완료 (%.1fs), %.1fs 대기",
                         rep, period_idx + 1, n_points, "SPIKE" if is_spike else "정상",
                         n_requests, elapsed_this_period, max(0, remaining))
            if remaining > 0:
                time.sleep(remaining)

        after = detect("S3", bucket, n_points, period_seconds)
        logger.info("[anomaly rep=%d] after anomaly_flag=%s (z=%s, iforest=%s, triggered=%s), elapsed=%.1fs",
                    rep, after.get("anomaly_flag"), after.get("anomaly_score_zscore"),
                    after.get("anomaly_score_iforest"), after.get("triggered_metrics"),
                    time.time() - t0)

        return {"rep": rep, "bucket": bucket, "label": "anomaly", "period_request_counts": period_counts,
                "before": before, "after": after,
                "detected": bool(after.get("anomaly_flag")), "elapsed_sec": time.time() - t0}
    except Exception as exc:
        logger.error("[anomaly rep=%d] 실패: %s\n%s", rep, exc, traceback.format_exc())
        return {"rep": rep, "bucket": bucket, "label": "anomaly", "error": str(exc), "detected": None}


# ── normal 시행 (2.5시간 창 전체에 "평상시 수준" 트래픽을 꾸준히 만들어서 오탐 확인) ─
# generate_mock_s3.py가 정의한 "정상" = 5분당 500건 * 시간대 배율(0.5~1.0). 완전한
# 침묵(0건)이 아니라 이 정도 꾸준한 트래픽에서도 오탐이 없어야 진짜 "정상 오탐률"
# 검증이 된다.
#
# ⚠️ [v2 -> v3에서 수정] 마지막 5분 구간에만 트래픽을 몰아넣으면, 나머지 29개 구간이
# 여전히 0으로 비어있어서 "거의 텅 빈 배경 대비 극단적 스파이크"로 보여 오탐이 100%
# 나오는 구조적 문제가 있었다(실측으로 확인됨: normal 8개 z_max가 전부 5.38로 동일 —
# 배경이 0에 가까워 어떤 크기의 트래픽이든 기계적으로 크게 튀는 현상). 그래서
# n_points(30) * period_seconds(300) = 2.5시간 창 "전체"에 걸쳐 5분마다 계속 트래픽을
# 쏴서, z-score/IForest가 실제로 보는 기준선(평균/표준편차) 자체가 "꾸준한 정상
# 트래픽"이 되게 만든다. 대신 시행 하나에 2.5시간 이상 걸린다.

def run_normal_trial(bucket: str, rep: int, multiplier: float,
                      n_points: int = 30, period_seconds: int = 300,
                      object_size_bytes: int = 50_000, noise_ratio: float = 0.1) -> dict:
    t0 = time.time()
    base = NORMAL_BASE_REQUESTS_PER_WINDOW * multiplier
    logger.info("[normal rep=%d] bucket=%s 시작 (multiplier=%.2f -> 5분당 ~%d건, %d개 구간=%.1f시간 지속)",
                rep, bucket, multiplier, round(base), n_points, n_points * period_seconds / 3600)
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        key = f"s3_repeated_trial_normal_load_{rep}.bin"
        s3.put_object(Bucket=bucket, Key=key, Body=os.urandom(object_size_bytes))

        before = detect("S3", bucket, n_points, period_seconds)
        logger.debug("[normal rep=%d] before=%s", rep, json.dumps(before, ensure_ascii=False))

        period_counts = []
        for period_idx in range(n_points):
            period_start = time.time()
            n_requests = max(1, round(base * random.uniform(1 - noise_ratio, 1 + noise_ratio)))
            period_counts.append(n_requests)
            for _ in range(n_requests):
                s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            elapsed_this_period = time.time() - period_start
            remaining = period_seconds - elapsed_this_period
            logger.debug("[normal rep=%d] 구간 %d/%d: %d건 완료 (%.1fs), %.1fs 대기",
                         rep, period_idx + 1, n_points, n_requests, elapsed_this_period, max(0, remaining))
            if remaining > 0:
                time.sleep(remaining)

        after = detect("S3", bucket, n_points, period_seconds)
        logger.info("[normal rep=%d] after anomaly_flag=%s (z=%s, iforest=%s, triggered=%s), elapsed=%.1fs",
                    rep, after.get("anomaly_flag"), after.get("anomaly_score_zscore"),
                    after.get("anomaly_score_iforest"), after.get("triggered_metrics"),
                    time.time() - t0)

        return {"rep": rep, "bucket": bucket, "label": "normal", "multiplier": multiplier,
                "period_request_counts": period_counts, "before": before, "after": after,
                "detected": bool(after.get("anomaly_flag")), "elapsed_sec": time.time() - t0}
    except Exception as exc:
        logger.error("[normal rep=%d] 실패: %s\n%s", rep, exc, traceback.format_exc())
        return {"rep": rep, "bucket": bucket, "label": "normal", "error": str(exc), "detected": None}


# ── 통계 계산 ────────────────────────────────────────────────────────────────

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

def result_filename(n_points: int, period_seconds: int, object_size_bytes: int,
                     n_normal: int, n_anomaly: int) -> Path:
    date_str = datetime.now().strftime("%Y%m%d")
    obj_kb = object_size_bytes // 1000
    hours = n_points * period_seconds / 3600
    name = (f"s3_repeated_trial__window{hours:.1f}h_objsize{obj_kb}kb_"
            f"n{n_normal}-{n_anomaly}_scriptv{SCRIPT_VERSION}_{date_str}.json")
    return RESULT_DIR / name


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--setup", action="store_true", help="anomaly+normal 시행용 버킷 생성 + Request Metrics 활성화")
    parser.add_argument("--run", action="store_true", help="반복 실험 실행")
    parser.add_argument("--n-anomaly", type=int, default=5)
    parser.add_argument("--n-normal", type=int, default=8)
    parser.add_argument("--n-points", type=int, default=30, help="탐지 윈도우 길이 (기본 30 = 실제 파이프라인과 동일)")
    parser.add_argument("--period-seconds", type=int, default=300, help="구간 길이 (기본 300초 = 5분)")
    parser.add_argument("--object-size-bytes", type=int, default=50_000)
    args = parser.parse_args()

    log_path = _setup_logging()
    logger.info("로그 파일: %s (SCRIPT_VERSION=%s)", log_path, SCRIPT_VERSION)

    if args.setup:
        anomaly_names, normal_names = setup_buckets(args.n_anomaly, args.n_normal)
        logger.info("버킷 준비 완료: anomaly=%s, normal=%s", anomaly_names, normal_names)
        return

    if not args.run:
        parser.print_help()
        return

    anomaly_buckets = [_anomaly_bucket_name(i) for i in range(args.n_anomaly)]
    normal_buckets = [_normal_bucket_name(i) for i in range(args.n_normal)]
    normal_multipliers = [NORMAL_TRAFFIC_MULTIPLIERS[i % len(NORMAL_TRAFFIC_MULTIPLIERS)] for i in range(args.n_normal)]
    anomaly_multipliers = [NORMAL_TRAFFIC_MULTIPLIERS[i % len(NORMAL_TRAFFIC_MULTIPLIERS)] for i in range(args.n_anomaly)]

    total_workers = args.n_anomaly + args.n_normal
    total_hours = args.n_points * args.period_seconds / 3600
    logger.info("=== anomaly %d개 + normal %d개, 총 %d개 시행 동시 병렬 시작 (시행당 약 %.1f시간 소요 예상) ===",
                args.n_anomaly, args.n_normal, total_workers, total_hours)

    anomaly_results, normal_results = [], []
    with ThreadPoolExecutor(max_workers=total_workers) as executor:
        futures = {}
        for i in range(args.n_anomaly):
            fut = executor.submit(run_anomaly_trial, anomaly_buckets[i], i, anomaly_multipliers[i],
                                   args.object_size_bytes, args.n_points, args.period_seconds)
            futures[fut] = ("anomaly", i)
        for i in range(args.n_normal):
            fut = executor.submit(run_normal_trial, normal_buckets[i], i, normal_multipliers[i],
                                   args.n_points, args.period_seconds, args.object_size_bytes)
            futures[fut] = ("normal", i)

        for future in as_completed(futures):
            label, _ = futures[future]
            result = future.result()
            (anomaly_results if label == "anomaly" else normal_results).append(result)

    anomaly_results.sort(key=lambda r: r["rep"])
    normal_results.sort(key=lambda r: r["rep"])

    metrics = compute_confusion_metrics(anomaly_results, normal_results)
    logger.info("=== 결과 ===\n%s", json.dumps(metrics, ensure_ascii=False, indent=2))

    out_path = result_filename(args.n_points, args.period_seconds, args.object_size_bytes,
                                args.n_normal, args.n_anomaly)
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "script_version": SCRIPT_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "params": {
            "n_points": args.n_points, "period_seconds": args.period_seconds,
            "object_size_bytes": args.object_size_bytes,
            "n_normal": args.n_normal, "n_anomaly": args.n_anomaly,
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
