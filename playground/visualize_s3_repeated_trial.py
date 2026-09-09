"""
playground/visualize_s3_repeated_trial.py

s3_repeated_trial.py의 결과 JSON을 읽어 포스터/보고서용 차트 2종을 만든다.

⚠️ 지표 종류에 따라 다른 그래프를 쓴다 (섞으면 안 됨 — 세션에서 논의된 이유):
  - 비율 지표(accuracy/recall/precision/FPR)는 이항분포이므로 정규근사 에러바가
    아니라 Clopper-Pearson 95% CI를 막대+구간으로 표시한다. n이 작고 0%/100%
    같은 극단값에서 정규근사는 100% 초과/음수 구간을 만들 수 있어 부적절하다.
  - z_max/iforest_score 같은 연속값은 실제로 반복마다 다른 실수값이 나오므로
    mean±SD 에러바가 적절하다(playground/eval_outputs/scenario_repeatability_result.json
    과 동일 관례).

[실행 방법]
  python playground/visualize_s3_repeated_trial.py [결과 JSON 경로]
  경로를 안 주면 playground/eval_outputs/ 에서 s3_repeated_trial__*.json 중
  가장 최근 파일을 자동으로 찾는다.

[생성 파일]
  - playground/eval_outputs/s3_repeated_trial_chart_confusion_ci_{날짜}.png
  - playground/eval_outputs/s3_repeated_trial_chart_scores_errorbar_{날짜}.png
"""

from __future__ import annotations

import glob
import json
import statistics
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
EVAL_OUTPUTS = PROJECT_ROOT / "playground" / "eval_outputs"

import matplotlib
matplotlib.use("Agg")  # 헤드리스 환경에서도 저장 가능하도록
import matplotlib.pyplot as plt

# 한글 라벨/제목이 깨지지 않도록 (Windows 기본 폰트 DejaVu Sans는 한글 미지원)
matplotlib.rcParams["font.family"] = "Malgun Gothic"
matplotlib.rcParams["axes.unicode_minus"] = False


_DERIVED_SUFFIXES = ("_analysis.json", "_accuracy.json")


def find_latest_result() -> Path:
    # 접두사를 s3_로 고정하지 않음 — 팀원이 ec2_repeated_trial__*.json처럼 다른
    # 리소스 타입으로 결과를 저장해도 자동 탐색되게 와일드카드로 둠. 분석 스크립트가
    # 만드는 파생 파일(_analysis.json/_accuracy.json)은 결과 원본이 아니므로 제외.
    candidates = sorted(
        p for p in glob.glob(str(EVAL_OUTPUTS / "*_repeated_trial__*.json"))
        if not p.endswith(_DERIVED_SUFFIXES)
    )
    if not candidates:
        raise FileNotFoundError(
            f"{EVAL_OUTPUTS}에 *_repeated_trial__*.json 결과 파일이 없음 — "
            "s3_repeated_trial.py(또는 동일 스키마의 리소스별 반복실험 스크립트) --run을 먼저 실행할 것."
        )
    return Path(candidates[-1])


def load_result(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _mean_std(values: list[float]) -> tuple[float, float]:
    if not values:
        return 0.0, 0.0
    mean = statistics.mean(values)
    std = statistics.stdev(values) if len(values) > 1 else 0.0
    return mean, std


# ── 차트 1: 비율 지표 (accuracy/recall/precision/FPR) — Clopper-Pearson 95% CI ──

def plot_confusion_ci(payload: dict, out_path: Path) -> None:
    metrics = payload["metrics"]
    cm = metrics["confusion_matrix"]

    labels = ["Accuracy", "Recall", "Precision", "FPR"]
    values = [
        metrics.get("accuracy"),
        metrics.get("recall"),
        metrics.get("precision"),
        metrics.get("false_positive_rate"),
    ]
    cis = [
        metrics.get("accuracy_ci_95_clopper_pearson"),
        metrics.get("recall_ci_95_clopper_pearson"),
        None,  # precision은 CI 계산 안 함 (n_anomaly+n_fp 기준이 recall/FPR과 겹쳐 생략)
        metrics.get("fpr_ci_95_clopper_pearson"),
    ]

    fig, ax = plt.subplots(figsize=(7, 5))
    x = range(len(labels))
    bar_values = [v if v is not None else 0 for v in values]
    colors = ["#4C72B0", "#55A868", "#C44E52", "#8172B2"]
    bars = ax.bar(x, [v * 100 for v in bar_values], color=colors, width=0.55)

    for i, (v, ci) in enumerate(zip(values, cis)):
        if v is None:
            continue
        if ci is not None:
            lo, hi = ci
            err_lo = max(0, (v - lo) * 100)
            err_hi = max(0, (hi - v) * 100)
            ax.errorbar(i, v * 100, yerr=[[err_lo], [err_hi]], fmt="none",
                        ecolor="black", capsize=6, elinewidth=1.5)
            ax.text(i, min(hi * 100 + 4, 108), f"{v*100:.1f}%\n[{lo*100:.1f}, {hi*100:.1f}]",
                    ha="center", va="bottom", fontsize=9)
        else:
            ax.text(i, v * 100 + 2, f"{v*100:.1f}%", ha="center", va="bottom", fontsize=9)

    ax.set_ylim(0, 115)
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels)
    ax.set_ylabel("%")
    ax.set_title(
        f"S3 실측 결과 (TP={cm['TP']} FN={cm['FN']} FP={cm['FP']} TN={cm['TN']})\n"
        f"막대=점추정치, 세로선=95% CI(Clopper-Pearson)"
    )
    ax.axhline(100, color="gray", linewidth=0.5, linestyle="--")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


# ── 차트 2: 연속값 지표 (anomaly_score_zscore / anomaly_score_iforest) — mean±SD ──

def plot_scores_errorbar(payload: dict, out_path: Path) -> None:
    anomaly_trials = [t for t in payload.get("anomaly_trials", []) if "after" in t]
    normal_trials = [t for t in payload.get("normal_trials", []) if "after" in t]

    groups = [
        ("anomaly\n(z-score)", [t["after"]["anomaly_score_zscore"] for t in anomaly_trials
                                  if t["after"].get("anomaly_score_zscore") is not None]),
        ("normal\n(z-score)", [t["after"]["anomaly_score_zscore"] for t in normal_trials
                                 if t["after"].get("anomaly_score_zscore") is not None]),
        ("anomaly\n(IForest)", [t["after"]["anomaly_score_iforest"] for t in anomaly_trials
                                  if t["after"].get("anomaly_score_iforest") is not None]),
        ("normal\n(IForest)", [t["after"]["anomaly_score_iforest"] for t in normal_trials
                                 if t["after"].get("anomaly_score_iforest") is not None]),
    ]

    labels = [g[0] for g in groups]
    means = []
    stds = []
    ns = []
    for _, vals in groups:
        m, s = _mean_std(vals)
        means.append(m)
        stds.append(s)
        ns.append(len(vals))

    fig, ax = plt.subplots(figsize=(7, 5))
    x = range(len(labels))
    colors = ["#C44E52", "#55A868", "#C44E52", "#55A868"]
    ax.bar(x, means, yerr=stds, capsize=6, color=colors, alpha=0.85, width=0.55)

    for i, (m, s, n) in enumerate(zip(means, stds, ns)):
        ax.text(i, m + s + 0.02 * max(means + [1]), f"{m:.3f}±{s:.3f}\n(n={n})",
                ha="center", va="bottom", fontsize=9)

    ax.set_xticks(list(x))
    ax.set_xticklabels(labels)
    ax.set_ylabel("score")
    ax.set_title("S3 실측 결과 — anomaly/normal 시행별 탐지 점수 (mean ± SD)")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def main() -> None:
    if len(sys.argv) > 1:
        result_path = Path(sys.argv[1])
    else:
        result_path = find_latest_result()

    print(f"결과 파일: {result_path}")
    payload = load_result(result_path)

    # 출력 파일명은 입력 결과 파일명에서 따옴 (s3_ 하드코딩하면 팀원이 다른
    # 리소스 결과로 돌렸을 때 차트 이름이 실제 내용과 안 맞게 됨)
    stem = result_path.stem
    date_str = datetime.now().strftime("%Y%m%d")
    ci_path = EVAL_OUTPUTS / f"{stem}_chart_confusion_ci_{date_str}.png"
    score_path = EVAL_OUTPUTS / f"{stem}_chart_scores_errorbar_{date_str}.png"

    plot_confusion_ci(payload, ci_path)
    print(f"저장: {ci_path}")

    plot_scores_errorbar(payload, score_path)
    print(f"저장: {score_path}")


if __name__ == "__main__":
    main()
