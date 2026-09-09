"""
playground/analyze_iforest_vs_zscore.py

s3_repeated_trial.py(또는 동일 스키마의 다른 리소스용 반복실험 스크립트) 결과 JSON을
읽어서, Z-score 단독 / IForest 단독 / 실제 앙상블(OR 결합, 프로덕션 그대로)의
accuracy/recall/FPR을 따로 계산한다.

⚠️ 한계: 여기서 쓰는 "IForest 단독"/"Z-score 단독" 판정은 각 지표의 원점수를
IFOREST_THRESHOLD(0.5)/Z_SCORE_THRESHOLD(2.75)와 단순 비교한 근사치다. 실제
detection_node()의 판정은 지속성 체크(최근 K개 연속 조건)까지 포함하는데, 그
지속성 체크에 쓰인 개별 시점별 점수는 결과 JSON에 저장돼 있지 않아서 정확히
재현할 수 없다. 그래서 이 스크립트의 "단독" 수치는 방향성 판단용 근사치이고,
"앙상블"(anomaly_flag 그대로 사용) 수치만 정확한 실측값이다.

[실행 방법]
  python playground/analyze_iforest_vs_zscore.py [결과 JSON 경로]
  경로 생략 시 playground/eval_outputs/에서 *_repeated_trial__*.json 중 최신 파일 사용
"""

from __future__ import annotations

import glob
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scipy.stats import beta as _beta_dist

from pipeline.detection_agent import Z_SCORE_THRESHOLD, IFOREST_THRESHOLD

EVAL_OUTPUTS = PROJECT_ROOT / "playground" / "eval_outputs"


def clopper_pearson_ci(successes: int, n: int, confidence: float = 0.95) -> tuple[float, float]:
    if n == 0:
        return (0.0, 1.0)
    alpha = 1 - confidence
    lower = 0.0 if successes == 0 else _beta_dist.ppf(alpha / 2, successes, n - successes + 1)
    upper = 1.0 if successes == n else _beta_dist.ppf(1 - alpha / 2, successes + 1, n - successes)
    return (float(lower), float(upper))


_DERIVED_SUFFIXES = ("_analysis.json", "_accuracy.json")


def find_latest_result() -> Path:
    candidates = sorted(
        p for p in glob.glob(str(EVAL_OUTPUTS / "*_repeated_trial__*.json"))
        if not p.endswith(_DERIVED_SUFFIXES)
    )
    if not candidates:
        raise FileNotFoundError(f"{EVAL_OUTPUTS}에 반복실험 결과 파일이 없음")
    return Path(candidates[-1])


def calc_metrics(anomaly_trials, normal_trials, key: str, threshold: float, above_is_anomaly: bool = True):
    def is_anomaly(v):
        if v is None:
            return False
        return (v > threshold) if above_is_anomaly else (v < threshold)

    tp = sum(1 for t in anomaly_trials if is_anomaly(t["after"].get(key)))
    fn = len(anomaly_trials) - tp
    fp = sum(1 for t in normal_trials if is_anomaly(t["after"].get(key)))
    tn = len(normal_trials) - fp
    total = tp + fn + fp + tn

    accuracy = (tp + tn) / total if total else None
    recall = tp / (tp + fn) if (tp + fn) else None
    fpr = fp / (fp + tn) if (fp + tn) else None

    return {
        "confusion_matrix": {"TP": tp, "FN": fn, "FP": fp, "TN": tn},
        "accuracy": accuracy,
        "accuracy_ci_95": list(clopper_pearson_ci(tp + tn, total)) if total else None,
        "recall": recall,
        "recall_ci_95": list(clopper_pearson_ci(tp, tp + fn)) if (tp + fn) else None,
        "false_positive_rate": fpr,
        "fpr_ci_95": list(clopper_pearson_ci(fp, fp + tn)) if (fp + tn) else None,
    }


def main() -> None:
    result_path = Path(sys.argv[1]) if len(sys.argv) > 1 else find_latest_result()
    print(f"결과 파일: {result_path}\n")

    with open(result_path, encoding="utf-8") as f:
        payload = json.load(f)

    anomaly_trials = payload["anomaly_trials"]
    normal_trials = payload["normal_trials"]

    zscore_metrics = calc_metrics(anomaly_trials, normal_trials, "anomaly_score_zscore", Z_SCORE_THRESHOLD)
    iforest_metrics = calc_metrics(anomaly_trials, normal_trials, "anomaly_score_iforest", IFOREST_THRESHOLD)
    ensemble_metrics = payload["metrics"]  # 이미 s3_repeated_trial.py가 정확히 계산해둔 것

    def show(label, m, is_ensemble=False):
        cm = m["confusion_matrix"]
        acc = m["accuracy"]
        rec = m["recall"]
        fpr = m.get("false_positive_rate") if not is_ensemble else m.get("false_positive_rate")
        print(f"[{label}] TP={cm['TP']} FN={cm['FN']} FP={cm['FP']} TN={cm['TN']}")
        print(f"  accuracy={acc*100:.1f}%  recall={rec*100:.1f}%  FPR={fpr*100:.1f}%")
        acc_ci = m.get("accuracy_ci_95") or m.get("accuracy_ci_95_clopper_pearson")
        rec_ci = m.get("recall_ci_95") or m.get("recall_ci_95_clopper_pearson")
        if acc_ci:
            print(f"  accuracy 95% CI=[{acc_ci[0]*100:.1f}%, {acc_ci[1]*100:.1f}%]")
        if rec_ci:
            print(f"  recall 95% CI=[{rec_ci[0]*100:.1f}%, {rec_ci[1]*100:.1f}%]")
        print()

    print("⚠️ 단독 수치는 지속성 체크 미반영 근사치, 앙상블만 정확한 실측값\n")
    show("Z-score 단독 (근사)", zscore_metrics)
    show("IForest 단독 (근사)", iforest_metrics)
    show("앙상블 (실측, 프로덕션 detection_node 그대로)", ensemble_metrics, is_ensemble=True)

    out_path = result_path.parent / f"{result_path.stem}__iforest_vs_zscore_analysis.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({
            "source_result_file": str(result_path),
            "zscore_standalone_approx": zscore_metrics,
            "iforest_standalone_approx": iforest_metrics,
            "ensemble_actual": ensemble_metrics,
        }, f, ensure_ascii=False, indent=2)
    print(f"저장: {out_path}")


if __name__ == "__main__":
    main()
