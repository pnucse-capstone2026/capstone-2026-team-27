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
- 신뢰구간은 Clopper-Pearson 정확 이항 신뢰구간(양측 95%)으로 통일 — 정규근사
  방식(mean±SD)은 비율 지표에 쓰면 n이 작을 때 100% 초과/음수 구간이 나올 수
  있어 부적절하다. z_max/iforest_score 같은 연속값에는 대신 mean±SD를 쓴다
  (playground/eval_outputs/scenario_repeatability_result.json과 동일 방식).

⚠️ v3에서 판정 로직이 바뀌었다 (v2까지의 수치와 직접 비교 불가):
  v2까지는 phase_g_real_world_validation.py의 _detect()를 그대로 가져와 썼는데,
  그 헬퍼가 detection_node와 어긋난 상태였다. git 이력으로 확인:
    - 2026-08-25 phase_g 작성 (당시엔 _zscore_max/_iforest_score가 맞는 방식)
    - 2026-08-28 persistence 도입(_zscore_check_persistent) → detection_node만 갱신
    - 2026-09-08 절대임계값 체크 2개 추가 → 역시 phase_g엔 미반영
  S3에 실제로 영향 있는 차이는 두 가지:
    (1) z-score/IForest가 "창 최댓값·마지막 1개 시점"이라 persistence가 없음.
        창 최댓값은 스파이크가 지나간 뒤에도 그 점이 창에서 밀려날 때까지(최대
        2.5시간) 계속 이상으로 잡아서, 정상 구간이 오탐으로 찍힐 수 있다.
    (2) cost가 채워지지 않음. fetch_metrics는 S3에 number_of_requests/
        bytes_downloaded만 주는데 estimate_cost_series를 부르지 않아서, cost가
        z-score 대상(Z_SCORE_TARGET_METRICS)인데도 평가되지 않고, IForest도
        학습 때(cost 있음)와 달리 mask=0으로 들어가 feature가 어긋난다.
  (BUFFER_ZSCORE_TARGET_METRICS vs Z_SCORE_TARGET_METRICS 차이는 network_in
   유무뿐이라 S3에는 영향 없음.)
  그래서 v3는 두 방식을 모두 기록한다:
    - teammate_compat : v2까지의 방식 (과거 수치와 비교용)
    - production      : detection_node가 실제로 쓰는 방식 (보고서에 쓸 값)

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

SCRIPT_VERSION = "3"
# v1 (2026-09-09): 최초 작성 — anomaly 병렬(버킷 N개) + normal 과거 조용한 윈도우 재사용,
#   Clopper-Pearson 95% CI, 상세 로깅(파일+콘솔), 파라미터 내장 파일명.
# v2 (2026-09-09): normal도 "완전 침묵"이 아니라 mock과 동일한 정상 트래픽 수준(500건
#   *시간대배율)을 실제로 발생시켜 측정하도록 변경 — 침묵만 검증하는 건 실제 운영
#   상황의 오탐 방어력을 못 보여줌. normal도 독립 버킷 병렬화, anomaly에 rep별
#   ±15% 노이즈 추가.
# v3 (2026-09-09): 판정 로직 수정. phase_g의 낡은 _detect()(persistence 없음, 절대
#   임계값 체크 없음, cost 미반영)를 쓰던 것을 detection_node 실제 방식으로 교체하고,
#   과거 비교를 위해 옛 방식도 teammate_compat으로 함께 기록. 판정 로직은
#   ec2_lambda_repeated_trial.detect_both()로 일원화(한 곳만 고치면 되게).

import argparse
import json
import logging
import os
import random
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(PROJECT_ROOT / "playground") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "playground"))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

import boto3

import pipeline.detection_agent as da
from pipeline.cloudwatch_client import METRIC_SPEC, _build_dimensions

# 판정 로직·통계는 EC2/Lambda 반복 시행 스크립트와 공유한다 (한 곳에서만 관리)
from ec2_lambda_repeated_trial import clopper_pearson_ci, compute_metrics, detect_both

# ── 설정값 ────────────────────────────────────────────────────────────────────

ANOMALY_BUCKET_PREFIX = "detection-trial-anomaly"
NORMAL_BUCKET_PREFIX = "detection-trial-normal"
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2")

# generate_mock_s3.py와 동일한 "정상" 정의 — 5분당 500건 기준에 시간대별 배율(0.5~1.0)을
# 곱한 값. 완전히 조용한(0건) 상태가 아니라 이 정도의 꾸준한 트래픽이 있는 게 "정상"이다.
NORMAL_TRAFFIC_MULTIPLIERS = [0.5, 0.6, 0.8, 0.85, 0.9, 1.0, 1.0, 0.7]
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


# ── 지표 조회 (end_time을 과거로 지정할 수 있게 자체 구현) ──────────────────────
# cloudwatch_client.fetch_metrics는 항상 "지금"까지만 조회하므로, 과거 구간을 다르게
# 잘라보려면 이 함수가 필요하다. 판정은 detect_both()에 usage를 넘겨서 처리한다.

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
    """teammate_compat / production 두 방식 모두 계산 (detect_both에 위임).
    end_time을 주면 그 시점 기준으로 조회한다."""
    if end_time is None:
        return detect_both(resource_type, resource_id)
    usage = _fetch_metrics_at(resource_type, resource_id, end_time)
    return detect_both(resource_type, resource_id, usage=usage)


# ── anomaly 시행 (버킷 하나에 GET 폭증 + before/after 판정) ────────────────────

def run_anomaly_trial(bucket: str, rep: int, n_gets: int, object_size_bytes: int, wait_sec: int,
                       jitter_ratio: float = 0.15) -> dict:
    t0 = time.time()
    actual_n_gets = max(1, round(n_gets * random.uniform(1 - jitter_ratio, 1 + jitter_ratio)))
    logger.info("[anomaly rep=%d] bucket=%s 시작 (n_gets=%d(기준 %d±%.0f%%), object_size=%dB, wait=%ds)",
                rep, bucket, actual_n_gets, n_gets, jitter_ratio * 100, object_size_bytes, wait_sec)
    try:
        before = detect("S3", bucket)
        logger.debug("[anomaly rep=%d] before=%s", rep,
                     json.dumps({k: v for k, v in before.items() if k != "raw_metrics"}, ensure_ascii=False))

        s3 = boto3.client("s3", region_name=AWS_REGION)
        key = f"s3_repeated_trial_load_test_{rep}.bin"
        s3.put_object(Bucket=bucket, Key=key, Body=os.urandom(object_size_bytes))
        for _ in range(actual_n_gets):
            s3.get_object(Bucket=bucket, Key=key)
        logger.info("[anomaly rep=%d] GET %d회 완료, %d초 대기 중...", rep, actual_n_gets, wait_sec)
        time.sleep(wait_sec)

        after = detect("S3", bucket)
        logger.info("[anomaly rep=%d] production.or_gate=%s / teammate.anomaly_flag=%s, elapsed=%.1fs",
                    rep, after["production"]["or_gate"], after["teammate_compat"]["anomaly_flag"],
                    time.time() - t0)

        return {"rep": rep, "resource": bucket, "label": "anomaly", "n_gets_actual": actual_n_gets,
                "before": before, "after": after,
                "detected_production": bool(after["production"]["or_gate"]),
                "detected_teammate_compat": bool(after["teammate_compat"]["anomaly_flag"]),
                "detected_iforest_only": bool(after["production"]["iforest_triggered"]),
                "elapsed_sec": round(time.time() - t0, 1)}
    except Exception as exc:
        logger.error("[anomaly rep=%d] 실패: %s\n%s", rep, exc, traceback.format_exc())
        return {"rep": rep, "resource": bucket, "label": "anomaly", "error": str(exc),
                "detected_production": None, "detected_teammate_compat": None,
                "detected_iforest_only": None}


# ── normal 시행 (실제 "평상시 수준" GET 트래픽을 만들어서 오탐 여부 확인) ────────

def run_normal_trial(bucket: str, rep: int, multiplier: float, wait_sec: int,
                      object_size_bytes: int = 50_000, noise_ratio: float = 0.1) -> dict:
    t0 = time.time()
    base = NORMAL_BASE_REQUESTS_PER_WINDOW * multiplier
    n_requests = max(1, round(base * random.uniform(1 - noise_ratio, 1 + noise_ratio)))
    logger.info("[normal rep=%d] bucket=%s 시작 (multiplier=%.2f -> 요청 %d건, wait=%ds)",
                rep, bucket, multiplier, n_requests, wait_sec)
    try:
        before = detect("S3", bucket)
        logger.debug("[normal rep=%d] before=%s", rep,
                     json.dumps({k: v for k, v in before.items() if k != "raw_metrics"}, ensure_ascii=False))

        s3 = boto3.client("s3", region_name=AWS_REGION)
        key = f"s3_repeated_trial_normal_load_{rep}.bin"
        s3.put_object(Bucket=bucket, Key=key, Body=os.urandom(object_size_bytes))
        for _ in range(n_requests):
            s3.get_object(Bucket=bucket, Key=key)
        logger.info("[normal rep=%d] GET %d회(평상시 수준) 완료, %d초 대기 중...", rep, n_requests, wait_sec)
        time.sleep(wait_sec)

        after = detect("S3", bucket)
        logger.info("[normal rep=%d] production.or_gate=%s / teammate.anomaly_flag=%s, elapsed=%.1fs",
                    rep, after["production"]["or_gate"], after["teammate_compat"]["anomaly_flag"],
                    time.time() - t0)

        return {"rep": rep, "resource": bucket, "label": "normal", "multiplier": multiplier,
                "n_requests_actual": n_requests, "before": before, "after": after,
                "detected_production": bool(after["production"]["or_gate"]),
                "detected_teammate_compat": bool(after["teammate_compat"]["anomaly_flag"]),
                "detected_iforest_only": bool(after["production"]["iforest_triggered"]),
                "elapsed_sec": round(time.time() - t0, 1)}
    except Exception as exc:
        logger.error("[normal rep=%d] 실패: %s\n%s", rep, exc, traceback.format_exc())
        return {"rep": rep, "resource": bucket, "label": "normal", "error": str(exc),
                "detected_production": None, "detected_teammate_compat": None,
                "detected_iforest_only": None}


# ── 메인 ────────────────────────────────────────────────────────────────────

def result_filename(n_gets: int, object_size_bytes: int, wait_sec: int, n_normal: int, n_anomaly: int) -> Path:
    date_str = datetime.now().strftime("%Y%m%d")
    obj_kb = object_size_bytes // 1000
    name = (f"s3_repeated_trial__ngets{n_gets}_objsize{obj_kb}kb_wait{wait_sec}s_"
            f"n{n_normal}-{n_anomaly}_scriptv{SCRIPT_VERSION}_{date_str}.json")
    return RESULT_DIR / name


def main() -> None:
    parser = argparse.ArgumentParser(description="S3 대량다운로드 반복 시행 실험")
    parser.add_argument("--setup", action="store_true", help="anomaly+normal 시행용 버킷 생성 + Request Metrics 활성화")
    parser.add_argument("--run", action="store_true", help="반복 실험 실행")
    parser.add_argument("--n-anomaly", type=int, default=5)
    parser.add_argument("--n-normal", type=int, default=8)
    parser.add_argument("--n-gets", type=int, default=800)
    parser.add_argument("--object-size-bytes", type=int, default=200_000)
    parser.add_argument("--wait-sec", type=int, default=300)
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
    multipliers = [NORMAL_TRAFFIC_MULTIPLIERS[i % len(NORMAL_TRAFFIC_MULTIPLIERS)] for i in range(args.n_normal)]

    total_workers = args.n_anomaly + args.n_normal
    logger.info("=== anomaly %d개 + normal %d개, 총 %d개 시행 동시 병렬 시작 ===",
                args.n_anomaly, args.n_normal, total_workers)

    results = []
    with ThreadPoolExecutor(max_workers=total_workers) as executor:
        futures = []
        for i in range(args.n_anomaly):
            futures.append(executor.submit(run_anomaly_trial, anomaly_buckets[i], i, args.n_gets,
                                            args.object_size_bytes, args.wait_sec))
        for i in range(args.n_normal):
            futures.append(executor.submit(run_normal_trial, normal_buckets[i], i,
                                            multipliers[i], args.wait_sec))
        for future in as_completed(futures):
            results.append(future.result())

    results.sort(key=lambda r: (r["label"], r["rep"]))

    metrics = {
        "production(detection_node 실제 방식)": compute_metrics(results, "detected_production"),
        "iforest_only(persistence 적용)": compute_metrics(results, "detected_iforest_only"),
        "teammate_compat(v2까지의 방식)": compute_metrics(results, "detected_teammate_compat"),
    }

    for name, m in metrics.items():
        c = m["confusion_matrix"]
        logger.info("[%s] TP=%d TN=%d FP=%d FN=%d / accuracy=%s recall=%s",
                    name, c["TP"], c["TN"], c["FP"], c["FN"],
                    f"{m['accuracy']:.1%}" if m["accuracy"] is not None else "N/A",
                    f"{m['recall']:.1%}" if m["recall"] is not None else "N/A")
        if m["recall_ci_95_clopper_pearson"]:
            lo, hi = m["recall_ci_95_clopper_pearson"]
            logger.info("    recall 95%% CI(Clopper-Pearson) = [%.1f%%, %.1f%%]", lo * 100, hi * 100)

    out_path = result_filename(args.n_gets, args.object_size_bytes, args.wait_sec,
                                args.n_normal, args.n_anomaly)
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "script_version": SCRIPT_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "params": {
            "n_gets": args.n_gets, "object_size_bytes": args.object_size_bytes, "wait_sec": args.wait_sec,
            "n_normal": args.n_normal, "n_anomaly": args.n_anomaly,
        },
        "metrics": metrics,
        "trials": results,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info("결과 저장: %s", out_path)


if __name__ == "__main__":
    main()
