"""
playground/ec2_zombie_replay_trial.py

EC2 "좀비 인스턴스" 시나리오의 실 AWS 측정 결과를, **저장된 실측 raw_metrics를
실제 detection_node()에 재생(replay)**해서 팀 표준 스키마로 다시 뽑는다.

[왜 재측정이 아니라 재생인가]
2026-09-09 14:11 UTC에 실 AWS로 13대(정상 8 / 이상 5)를 측정했고, 그때의 CloudWatch
30포인트 raw_metrics가 결과 JSON에 통째로 저장돼 있다. 그런데 그 뒤 인스턴스의 부하
생성 스크립트가 14:42 UTC에 종료돼서(launch 11:40 UTC + 약 3시간), 지금은 13대가
전부 동일한 유휴 상태다 — 정상군과 이상군이 구분되지 않으므로 지금 다시 재면
정상 8대가 전부 오탐으로 잡힌다. 실행 중인 인스턴스에 부하를 다시 넣으려면 SSM이
필요한데 IAM 역할이 삭제된 상태다.

그래서 "AWS에서 실제로 받아온 지표"는 그대로 두고(= 실연동 데이터), 판정 로직과
IForest 모델만 최신으로 바꿔서 다시 통과시킨다. 재생이 유효한 이유:
  - detection_node()는 raw_metrics(+resource_age_seconds)만 입력으로 받는 순수 함수라
    AWS를 다시 부르든 저장된 값을 넣든 결과가 같다
  - 저장된 raw_metrics는 부하 생성기가 살아있던 시점(14:11 UTC)의 값이라
    정상/이상 구분이 실제로 존재한다 (normal-heavy CPU 25% vs anom-silent 0.3%)
  - IForest 모델은 seed_iforest_from_scenario_mock.py로 재학습된 최신본이 쓰인다

⚠️ 따라서 이 결과의 z-score/IForest 점수는 원본 측정 당시 값이 아니라 **재학습된
   모델 기준의 최신 값**이다. 원본과 다를 수 있고, 그게 의도다.

[실행 방법]
  python playground/ec2_zombie_replay_trial.py
  python playground/ec2_zombie_replay_trial.py --source <원본 결과 JSON 경로>

[생성 파일]
  playground/eval_outputs/ec2_zombie_replay_trial__...json  (팀 표준 스키마)
"""

from __future__ import annotations

SCRIPT_VERSION = "1"

import argparse
import glob
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

from scipy.stats import beta as _beta_dist

import pipeline.detection_agent as da

RESULT_DIR = PROJECT_ROOT / "playground" / "eval_outputs"
LOG_DIR = RESULT_DIR / "logs"

logger = logging.getLogger("ec2_zombie_replay_trial")


def _setup_logging() -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = LOG_DIR / f"ec2_zombie_replay_trial_{ts}.log"
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


def find_source() -> Path:
    candidates = sorted(glob.glob(str(RESULT_DIR / "ec2_repeated_trial__*.json")))
    if not candidates:
        raise FileNotFoundError(f"{RESULT_DIR}에 ec2_repeated_trial__*.json 원본이 없음")
    return Path(candidates[-1])


# ── 재생 (실제 프로덕션 detection_node()를 그대로 호출) ────────────────────────
# s3_repeated_trial.py v5의 detect()와 동일한 구조인데, assemble_resource()로 AWS를
# 다시 부르는 대신 저장된 raw_metrics를 그대로 넣는다는 점만 다르다.

def replay(resource_type: str, resource_id: str, raw_metrics: dict,
           resource_age_seconds: float | None) -> dict:
    state = {
        "trace_id": None,
        "resource_id": resource_id,
        "resource_type": resource_type,
        "raw_metrics": raw_metrics,
        "timestamp": None,
        # EC2 좀비 판정의 나이가드(_low_utilization_check)가 이 값을 쓴다 —
        # 빠뜨리면 2.5시간 미만으로 취급돼 절대임계값 체크가 통째로 보류된다.
        "resource_age_seconds": resource_age_seconds,
        "anomaly_flag": False,
        "anomaly_score_zscore": None,
        "anomaly_score_iforest": None,
        "triggered_metrics": [],
    }
    result_state = da.detection_node(state)

    return {
        "resource_id": resource_id,
        "raw_metrics": raw_metrics,
        "anomaly_flag": result_state["anomaly_flag"],
        "anomaly_score_zscore": result_state["anomaly_score_zscore"],
        "anomaly_score_iforest": result_state["anomaly_score_iforest"],
        "triggered_metrics": result_state["triggered_metrics"],
    }


# ── 지표 계산 (s3_repeated_trial.py v5와 동일 — 합산 시 정의가 같아야 함) ──────

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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default=None,
                        help="원본 실측 결과 JSON 경로 (생략 시 eval_outputs에서 최신 ec2_repeated_trial__*.json)")
    args = parser.parse_args()

    log_path = _setup_logging()
    logger.info("로그 파일: %s (SCRIPT_VERSION=%s)", log_path, SCRIPT_VERSION)

    source = Path(args.source) if args.source else find_source()
    payload_src = json.loads(source.read_text(encoding="utf-8"))
    logger.info("원본: %s (측정 %s, 시행 %d개)",
                source.name, payload_src.get("generated_at"), len(payload_src.get("trials", [])))

    anomaly_results, normal_results = [], []
    rep_counter = {"anomaly": 0, "normal": 0}

    for t in payload_src["trials"]:
        label = t["label"]
        after_src = t.get("after") or {}
        raw_metrics = after_src.get("raw_metrics")
        if not raw_metrics:
            logger.warning("[%s] raw_metrics 없음 — 건너뜀", t.get("resource"))
            continue

        rep = rep_counter[label]
        rep_counter[label] += 1

        after = replay("EC2", t["resource"], raw_metrics, after_src.get("resource_age_seconds"))

        logger.info("[%s %s] profile=%s anomaly_flag=%s (z=%s, IF=%s, triggered=%s)",
                    label, t["resource"], t.get("profile"), after["anomaly_flag"],
                    after["anomaly_score_zscore"], after["anomaly_score_iforest"],
                    after["triggered_metrics"])

        record = {
            "rep": rep,
            "instance_id": t["resource"],
            "label": label,
            "profile": t.get("profile"),
            "resource_age_seconds": after_src.get("resource_age_seconds"),
            "measured_at": after_src.get("measured_at"),
            "after": after,
            "detected": bool(after.get("anomaly_flag")),
        }
        (anomaly_results if label == "anomaly" else normal_results).append(record)

    metrics = compute_confusion_metrics(anomaly_results, normal_results)
    logger.info("=== 결과 ===\n%s", json.dumps(metrics, ensure_ascii=False, indent=2))

    date_str = datetime.now().strftime("%Y%m%d")
    out_path = RESULT_DIR / (f"ec2_zombie_replay_trial__n{len(normal_results)}-{len(anomaly_results)}_"
                             f"scriptv{SCRIPT_VERSION}_{date_str}.json")
    payload = {
        "script_version": SCRIPT_VERSION,
        "scenario": "ec2_zombie_instance",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "replayed_from": source.name,
        "replay_note": ("AWS에서 실측한 raw_metrics를 저장해둔 것을 재학습된 IForest 모델의 "
                        "detection_node()에 재생한 결과. 지표는 실연동 실측값이고 판정 모델만 최신."),
        "params": {
            "n_points": 30, "period_seconds": 300,
            "n_normal": len(normal_results), "n_anomaly": len(anomaly_results),
            "original_measured_at": payload_src.get("generated_at"),
        },
        "metrics": metrics,
        "anomaly_trials": anomaly_results,
        "normal_trials": normal_results,
    }
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info("결과 저장: %s", out_path)


if __name__ == "__main__":
    main()
