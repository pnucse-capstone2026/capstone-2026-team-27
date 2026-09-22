"""
playground/ec2_lambda_repeated_trial.py

EC2 좀비 / Lambda 에러 재시도 폭증 시나리오를 anomaly n회 + normal n회 반복 실행해서
TP/TN/FP/FN, accuracy, recall(+ Clopper-Pearson 95% CI)을 계산한다.
팀원의 playground/s3_repeated_trial.py와 같은 설계·출력 형식을 따른다.

설계 (s3_repeated_trial.py와 동일한 취지):
- 시행마다 독립된 리소스를 쓴다. 같은 리소스에 여러 시행을 걸면 CloudWatch 지표가
  하나로 합쳐져서 "n번의 독립 시행"이 아니라 "1번의 n배 큰 시행"이 되기 때문
  (EC2는 인스턴스별, Lambda는 함수별로 지표가 분리된다).
- normal 시행도 "완전한 침묵"이 아니라 실제 평상시 수준의 트래픽/부하를 만든다.
  침묵만 검증하면 실제 운영 상황의 오탐 방어력을 못 보여준다.
- 시행마다 부하량에 ±노이즈를 줘서 동일 반복이 되지 않게 한다.
- 신뢰구간은 Clopper-Pearson 정확 이항 신뢰구간(양측 95%).

⚠️ 팀원 스크립트와의 중요한 차이 — 판정 로직을 두 가지로 나눠서 모두 기록한다:
  (a) teammate_compat : _zscore_max(창 최댓값) + _iforest_score(마지막 1개 시점).
      s3_repeated_trial.py / phase_g_real_world_validation.py가 쓰는 방식.
      phase_g가 2026-08-25에 작성된 뒤 2026-08-28에 persistence가, 2026-09-08에
      절대임계값 체크가 detection_node에 도입됐지만 그 헬퍼는 갱신되지 않았다.
  (b) production     : detection_node가 실제로 쓰는 방식.
      _zscore_check_persistent / _iforest_score_and_trigger(둘 다 최근 3개 연속
      조건) + _low_utilization_check / _lambda_error_rate_check(절대임계값).
      EC2 좀비·Lambda 재시도폭증은 절대체크가 주 탐지 수단이라 (a)로 재면
      실제보다 훨씬 나쁘게 나온다.

시나리오별 구조 차이:
- Lambda: 유발(에러 폭증 호출) 전/후가 있으므로 before/after를 둘 다 측정한다.
- EC2 좀비: "유발"이라는 게 없다. 아무 작업도 안 시킨 인스턴스가 그 자체로 좀비이고,
  _low_utilization_check는 창 30포인트(2.5시간) 전체 + 나이가드 2.5시간을 요구한다.
  그래서 before/after 대신 "나이가드 충족 이후 1회 측정"만 한다.

[실행 방법]
  Lambda (기존 13개 함수 재사용, 약 17분):
    python playground/ec2_lambda_repeated_trial.py --scenario lambda --run
  EC2 (기존 13개 인스턴스 재사용, 나이가드 충족 후에만 가능):
    python playground/ec2_lambda_repeated_trial.py --scenario ec2 --run \
      --manifest <ec2_zombie_manifest.json 경로>

[생성 파일]
  - 결과: playground/eval_outputs/{scenario}_repeated_trial__n{정상}-{이상}_scriptv{V}_{YYYYMMDD}.json
  - 로그: playground/eval_outputs/logs/{scenario}_repeated_trial_{timestamp}.log
"""

from __future__ import annotations

SCRIPT_VERSION = "1"
# v1 (2026-09-09): 최초 작성 — 팀원 s3_repeated_trial.py 구조를 따르되, 판정 로직을
#   teammate_compat(낡은 헬퍼 방식) / production(detection_node 실제 방식) 두 가지로
#   나눠서 모두 기록. EC2는 유발 개념이 없어 before/after 대신 단일 측정.

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

os.environ.setdefault("AWS_PROFILE", "default")

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

import boto3
from scipy.stats import beta as _beta_dist

import pipeline.detection_agent as da
from pipeline.cloudwatch_client import fetch_metrics
from pipeline.cost_estimator import estimate_cost_series

AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2")

LOG_DIR = PROJECT_ROOT / "playground" / "eval_outputs" / "logs"
RESULT_DIR = PROJECT_ROOT / "playground" / "eval_outputs"

# Lambda 시행 설정 — 함수별로 서로 다른 패턴(그룹 내 복제본이 되면 독립 시행이 아니게 됨)
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

logger = logging.getLogger("ec2_lambda_repeated_trial")


def _setup_logging(scenario: str) -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = LOG_DIR / f"{scenario}_repeated_trial_{ts}.log"

    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return log_path


# ── 판정 (두 가지 방식을 모두 계산) ────────────────────────────────────────────

def detect_both(resource_type: str, resource_id: str, resource_age_seconds: float | None = None,
                 usage: dict[str, list[float]] | None = None) -> dict:
    """teammate_compat(낡은 헬퍼 방식)와 production(detection_node 실제 방식)을 모두 계산.
    raw_metrics 전체를 결과에 포함한다(요약이 아니라 원본).

    usage를 넘기면 그걸 그대로 쓴다 - S3 스크립트처럼 end_time을 과거로 지정해서
    직접 조회한 지표를 넣을 수 있게 하기 위함(판정 로직을 한 곳에서만 관리).
    """
    if usage is None:
        usage = fetch_metrics(resource_type, resource_id)
    n = len(next(iter(usage.values()))) if usage else 0

    out = {
        "resource_id": resource_id,
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "n_points": n,
        "resource_age_seconds": resource_age_seconds,
        "raw_metrics": usage,
    }
    if n < da.MIN_POINTS_FOR_IFOREST:
        out["note"] = f"포인트 {n}개로 최소 기준({da.MIN_POINTS_FOR_IFOREST}) 미달 - 판단 보류"
        out["teammate_compat"] = {"anomaly_flag": False}
        out["production"] = {"or_gate": False}
        return out

    # cost는 fetch_metrics가 안 채워주므로 별도 계산해서 붙인다
    # (phase_g/_detect는 이걸 빼먹어서 cost 컬럼이 mask=0으로 들어갔다)
    usage_with_cost = dict(usage)
    usage_with_cost["cost"] = estimate_cost_series(resource_type, resource_id, usage)

    # (a) teammate_compat: 창 최댓값 z-score + 마지막 1개 시점 IForest
    z_max = da._zscore_max(usage_with_cost)
    z_trig_max = z_max > da.Z_SCORE_THRESHOLD
    if_score_last = da._iforest_score(resource_type, usage_with_cost)
    if_trig_last = if_score_last > da.IFOREST_THRESHOLD
    out["teammate_compat"] = {
        "z_max": round(z_max, 4),
        "z_triggered": bool(z_trig_max),
        "iforest_score": round(if_score_last, 4),
        "iforest_triggered": bool(if_trig_last),
        "anomaly_flag": bool(z_trig_max or if_trig_last),
    }

    # (b) production: persistence + 절대임계값 체크까지 (detection_node와 동일)
    z_persist = False
    z_persist_max = 0.0
    for metric_name in usage_with_cost:
        if metric_name not in da.Z_SCORE_TARGET_METRICS:
            continue
        z, trig = da._zscore_check_persistent(usage_with_cost[metric_name])
        z_persist_max = max(z_persist_max, z)
        if trig:
            z_persist = True

    if_score_p, if_trig_p = da._iforest_score_and_trigger(resource_type, usage_with_cost)
    _, idle_trig, _ = da._low_utilization_check(resource_type, usage_with_cost, resource_age_seconds)
    _, surge_trig = da._lambda_error_rate_check(resource_type, usage_with_cost)

    out["production"] = {
        "zscore_persistent": bool(z_persist),
        "zscore_last_abs": round(z_persist_max, 4),
        "iforest_score": round(if_score_p, 4),
        "iforest_triggered": bool(if_trig_p),
        "absolute_triggered": bool(idle_trig or surge_trig),
        "absolute_kind": "idle" if idle_trig else ("error_surge" if surge_trig else None),
        "or_gate": bool(z_persist or if_trig_p or idle_trig or surge_trig),
    }
    return out


# ── Lambda 시행 ──────────────────────────────────────────────────────────────

def run_lambda_trial(function_name: str, label: str, error_rate: float, interval_sec: int,
                      phase_minutes: int, wait_sec: int, jitter_ratio: float = 0.15) -> dict:
    """error_rate/간격에 ±jitter를 줘서 동일 반복이 되지 않게 한다."""
    t0 = time.time()
    lam = boto3.client("lambda", region_name=AWS_REGION)

    actual_rate = min(0.98, max(0.0, error_rate * random.uniform(1 - jitter_ratio, 1 + jitter_ratio))) \
        if error_rate > 0 else 0.0
    actual_interval = max(5, round(interval_sec * random.uniform(1 - jitter_ratio, 1 + jitter_ratio)))
    n_calls = int(phase_minutes * 60 / actual_interval)

    logger.info("[%s %s] 시작 (error_rate=%.2f(기준 %.2f), 간격=%ds(기준 %ds), 호출 %d회)",
                label, function_name, actual_rate, error_rate, actual_interval, interval_sec, n_calls)
    try:
        before = detect_both("Lambda", function_name)
        logger.debug("[%s] before=%s", function_name,
                     json.dumps({k: v for k, v in before.items() if k != "raw_metrics"}, ensure_ascii=False))

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

        after = detect_both("Lambda", function_name)
        logger.info("[%s %s] after production.or_gate=%s (IF=%s, 절대=%s) / teammate.anomaly_flag=%s, %.1fs",
                    label, function_name, after["production"]["or_gate"],
                    after["production"]["iforest_triggered"], after["production"]["absolute_triggered"],
                    after["teammate_compat"]["anomaly_flag"], time.time() - t0)

        return {"resource": function_name, "label": label,
                "target_error_rate": error_rate, "actual_error_rate": round(actual_rate, 4),
                "interval_sec": actual_interval, "n_calls": n_calls, "n_errors": n_err,
                "before": before, "after": after,
                "detected_production": bool(after["production"]["or_gate"]),
                "detected_teammate_compat": bool(after["teammate_compat"]["anomaly_flag"]),
                "detected_iforest_only": bool(after["production"]["iforest_triggered"]),
                "elapsed_sec": round(time.time() - t0, 1)}
    except Exception as exc:
        logger.error("[%s] 실패: %s\n%s", function_name, exc, traceback.format_exc())
        return {"resource": function_name, "label": label, "error": str(exc),
                "detected_production": None, "detected_teammate_compat": None,
                "detected_iforest_only": None}


# ── EC2 시행 ─────────────────────────────────────────────────────────────────

def run_ec2_trials(manifest_path: Path) -> list[dict]:
    """EC2 좀비는 '유발'이 없다(방치 자체가 좀비). 나이가드/창 요건이 충족된 뒤
    인스턴스별로 1회 측정만 한다."""
    with open(manifest_path, encoding="utf-8") as f:
        manifest = json.load(f)

    check_earliest = datetime.fromisoformat(manifest["check_earliest_utc"].replace("Z", "+00:00"))
    now = datetime.now(timezone.utc)
    if now < check_earliest:
        raise RuntimeError(
            f"아직 측정 가능 시점이 아닙니다(나이가드 2.5시간 미충족). "
            f"{(check_earliest - now).total_seconds()/60:.1f}분 더 대기 필요 "
            f"(기준 {check_earliest.isoformat()})."
        )

    def one(inst: dict) -> dict:
        t0 = time.time()
        launch = datetime.fromisoformat(inst["launch_time_utc"].replace("Z", "+00:00"))
        age = (datetime.now(timezone.utc) - launch).total_seconds()
        logger.info("[%s %s] 측정 시작 (profile=%s, age=%.0fs)",
                    inst["true_label"], inst["instance_id"], inst.get("profile"), age)
        try:
            res = detect_both("EC2", inst["instance_id"], resource_age_seconds=age)
            logger.info("[%s %s] production.or_gate=%s (IF=%s, 절대=%s) / teammate.anomaly_flag=%s",
                        inst["true_label"], inst["instance_id"], res["production"]["or_gate"],
                        res["production"]["iforest_triggered"], res["production"]["absolute_triggered"],
                        res["teammate_compat"]["anomaly_flag"])
            return {"resource": inst["instance_id"], "label": inst["true_label"],
                    "profile": inst.get("profile"), "age_seconds": round(age, 1),
                    "after": res,
                    "detected_production": bool(res["production"]["or_gate"]),
                    "detected_teammate_compat": bool(res["teammate_compat"]["anomaly_flag"]),
                    "detected_iforest_only": bool(res["production"]["iforest_triggered"]),
                    "elapsed_sec": round(time.time() - t0, 1)}
        except Exception as exc:
            logger.error("[%s] 실패: %s\n%s", inst["instance_id"], exc, traceback.format_exc())
            return {"resource": inst["instance_id"], "label": inst["true_label"], "error": str(exc),
                    "detected_production": None, "detected_teammate_compat": None,
                    "detected_iforest_only": None}

    instances = manifest["instances"]
    results = []
    with ThreadPoolExecutor(max_workers=min(13, len(instances))) as ex:
        futures = [ex.submit(one, inst) for inst in instances]
        for fut in as_completed(futures):
            results.append(fut.result())
    return results


# ── 통계 ────────────────────────────────────────────────────────────────────

def clopper_pearson_ci(successes: int, n: int, confidence: float = 0.95) -> tuple[float, float]:
    if n == 0:
        return (0.0, 1.0)
    alpha = 1 - confidence
    lower = 0.0 if successes == 0 else float(_beta_dist.ppf(alpha / 2, successes, n - successes + 1))
    upper = 1.0 if successes == n else float(_beta_dist.ppf(1 - alpha / 2, successes + 1, n - successes))
    return (lower, upper)


def compute_metrics(results: list[dict], detect_key: str) -> dict:
    anomaly = [r for r in results if r["label"] == "anomaly"]
    normal = [r for r in results if r["label"] == "normal"]

    tp = sum(1 for r in anomaly if r.get(detect_key) is True)
    fn = sum(1 for r in anomaly if r.get(detect_key) is False)
    fp = sum(1 for r in normal if r.get(detect_key) is True)
    tn = sum(1 for r in normal if r.get(detect_key) is False)

    total = tp + fn + fp + tn
    n_anom, n_norm = tp + fn, tn + fp
    return {
        "confusion_matrix": {"TP": tp, "FN": fn, "FP": fp, "TN": tn},
        "n_normal": n_norm,
        "n_anomaly": n_anom,
        "accuracy": (tp + tn) / total if total else None,
        "accuracy_ci_95_clopper_pearson": list(clopper_pearson_ci(tp + tn, total)) if total else None,
        "recall": tp / n_anom if n_anom else None,
        "recall_ci_95_clopper_pearson": list(clopper_pearson_ci(tp, n_anom)) if n_anom else None,
        "precision": tp / (tp + fp) if (tp + fp) else None,
        "false_positive_rate": fp / n_norm if n_norm else None,
        "fpr_ci_95_clopper_pearson": list(clopper_pearson_ci(fp, n_norm)) if n_norm else None,
    }


def result_filename(scenario: str, n_normal: int, n_anomaly: int) -> Path:
    date_str = datetime.now().strftime("%Y%m%d")
    return RESULT_DIR / (f"{scenario}_repeated_trial__n{n_normal}-{n_anomaly}_"
                          f"scriptv{SCRIPT_VERSION}_{date_str}.json")


def main() -> None:
    parser = argparse.ArgumentParser(description="EC2 좀비 / Lambda 재시도폭증 반복 시행 실험")
    parser.add_argument("--scenario", choices=["ec2", "lambda"], required=True)
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--manifest", type=str, help="EC2용 매니페스트 JSON 경로")
    parser.add_argument("--phase-minutes", type=int, default=15, help="Lambda 호출 지속 시간")
    parser.add_argument("--wait-sec", type=int, default=120, help="Lambda CloudWatch 반영 대기")
    args = parser.parse_args()

    log_path = _setup_logging(args.scenario)
    logger.info("로그: %s (SCRIPT_VERSION=%s, scenario=%s)", log_path, SCRIPT_VERSION, args.scenario)

    if not args.run:
        parser.print_help()
        return

    if args.scenario == "lambda":
        logger.info("=== Lambda 13개 함수 동시 병렬 시행 (anomaly 5 + normal 8) ===")
        results = []
        with ThreadPoolExecutor(max_workers=len(LAMBDA_FUNCTIONS)) as ex:
            futures = [
                ex.submit(run_lambda_trial, fn, label, err, iv, args.phase_minutes, args.wait_sec)
                for fn, label, err, iv in LAMBDA_FUNCTIONS
            ]
            for fut in as_completed(futures):
                results.append(fut.result())
        results.sort(key=lambda r: (r["label"], r["resource"]))
    else:
        if not args.manifest:
            parser.error("--scenario ec2 에는 --manifest 가 필요합니다")
        logger.info("=== EC2 좀비: 매니페스트 기반 인스턴스별 1회 측정 ===")
        try:
            results = run_ec2_trials(Path(args.manifest))
        except RuntimeError as exc:
            # 나이가드 미충족은 정상적인 "아직 아님" 상태 - traceback 없이 안내만
            logger.warning("%s", exc)
            return
        results.sort(key=lambda r: (r["label"], r["resource"]))

    metrics = {
        "production(detection_node 실제 방식)": compute_metrics(results, "detected_production"),
        "iforest_only(persistence 적용)": compute_metrics(results, "detected_iforest_only"),
        "teammate_compat(낡은 헬퍼 방식)": compute_metrics(results, "detected_teammate_compat"),
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

    n_norm = metrics["production(detection_node 실제 방식)"]["n_normal"]
    n_anom = metrics["production(detection_node 실제 방식)"]["n_anomaly"]
    out_path = result_filename(args.scenario, n_norm, n_anom)
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({
            "script_version": SCRIPT_VERSION,
            "scenario": args.scenario,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "params": {"phase_minutes": args.phase_minutes, "wait_sec": args.wait_sec,
                        "n_normal": n_norm, "n_anomaly": n_anom},
            "metrics": metrics,
            "trials": results,
        }, f, ensure_ascii=False, indent=2)
    logger.info("결과 저장: %s", out_path)


if __name__ == "__main__":
    main()
