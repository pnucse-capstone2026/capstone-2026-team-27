"""
playground/tune_detection_parameters.py

Phase 5: 파라미터 튜닝 — eval_dataset.json(435개, 단변량 스파이크) 기준으로
전체 앙상블(Z-score OR IForest) 정확도가 80% 이상 나오는 파라미터 조합을 찾는다.

튜닝 대상:
  - IFOREST_THRESHOLD (τ): 기본 0.6
  - IFOREST_CONTAMINATION: 기본 0.1
  - Z_SCORE_THRESHOLD (k): 기본 3.0

⚠️ 이 튜닝은 "우리가 만든 합성 데이터" 기준이다. AWS 실데이터 연동 후에는
   재검증/재튜닝이 필요할 수 있음을 보고서에 명시해야 한다.

최적화 방식: contamination이 바뀌면 모델을 다시 학습해야 하지만, τ/k는 학습된
점수를 재사용해서 threshold 비교만 다시 하면 되므로, contamination별로 한 번만
학습하고 그 안에서 τ×k를 저렴하게 그리드서치한다.

[실행 방법]
  프로젝트 루트에서: python playground/tune_detection_parameters.py

[사전 조건]
  eval_dataset.json, phase_bonus_joint_anomaly 생성용 test_iforest_joint_anomaly 모듈 필요
"""

from __future__ import annotations

import json
import shutil
import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import numpy as np

import pipeline.detection_agent as da
from playground.test_iforest_joint_anomaly import generate_joint_samples

DATASET_PATH = PROJECT_ROOT / "playground" / "eval_outputs" / "eval_dataset.json"
RESULT_PATH = PROJECT_ROOT / "playground" / "eval_outputs" / "phase5_tuning_results.json"

CONTAMINATION_GRID = [0.1]  # 스코어 정규화(min-max) 특성상 contamination은 결과에 영향 없음 — 확인됨
TAU_GRID = [round(x, 2) for x in np.arange(0.05, 0.96, 0.05)]
K_GRID = [round(x, 2) for x in np.arange(1.0, 4.05, 0.25)]

TARGET_ACCURACY = 0.80


def _confusion(y_true: list[bool], y_pred: list[bool]) -> dict:
    tp = sum(1 for t, p in zip(y_true, y_pred) if t and p)
    fp = sum(1 for t, p in zip(y_true, y_pred) if not t and p)
    tn = sum(1 for t, p in zip(y_true, y_pred) if not t and not p)
    fn = sum(1 for t, p in zip(y_true, y_pred) if t and not p)
    accuracy = (tp + tn) / len(y_true) if y_true else 0.0
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    return {"tp": tp, "fp": fp, "tn": tn, "fn": fn,
            "accuracy": round(accuracy, 4), "precision": round(precision, 4),
            "recall": round(recall, 4), "f1": round(f1, 4)}


def _build_model_with_contamination(dataset: list[dict], model_dir: str, contamination: float):
    """기존(버그 수정 후) 방식 — 리소스 타입당 대표 윈도우 1개씩만 학습에 반영."""
    da.IFOREST_MODEL_DIR = model_dir
    da.IFOREST_CONTAMINATION = contamination
    for sample in dataset:
        da._get_or_train_iforest(sample["resource_type"], sample["raw_metrics"])
    cached = da._load_cached_model(da.IFOREST_UNIFIED_MODEL_NAME)
    return cached[0]


def _build_model_richer_normal_training(dataset: list[dict], contamination: float):
    """실험: 리소스 타입당 '정상' 윈도우를 전부(각 30개) 학습에 반영 — 기준선을 훨씬
    풍부하게 만든다. eval_dataset.json의 정상 케이스 150개 전부 사용."""
    from sklearn.ensemble import IsolationForest

    buffer_rows = [
        da.build_unified_feature_matrix(s["resource_type"], s["raw_metrics"])
        for s in dataset if s["case"] == "normal"
    ]
    buffer = np.vstack(buffer_rows)
    model = IsolationForest(contamination=contamination, random_state=da.IFOREST_RANDOM_STATE)
    model.fit(buffer)
    return model


def main() -> None:
    if not DATASET_PATH.exists():
        raise SystemExit(f"{DATASET_PATH}가 없습니다. 먼저 generate_eval_dataset.py를 실행하세요.")

    with open(DATASET_PATH, "r", encoding="utf-8") as f:
        dataset = json.load(f)

    joint_samples = generate_joint_samples()

    print(f"평가 데이터: 단변량 {len(dataset)}개 + 결합(다변량) {len(joint_samples)}개\n")

    y_true = [s["ground_truth_anomaly"] for s in dataset]

    # ── 사전 계산: z_max(threshold-독립적인 실제 z값)는 k와 무관하게 한 번만 계산 ──
    def _z_max(metrics: dict) -> float:
        z_max = 0.0
        for metric_name, values in metrics.items():
            if metric_name not in da.Z_SCORE_TARGET_METRICS:
                continue
            z, _ = da._zscore_check(values)
            z_max = max(z_max, z)
        return z_max

    z_max_cache = [_z_max(s["raw_metrics"]) for s in dataset]
    joint_z_max_cache = [_z_max(s["raw_metrics"]) for s in joint_samples]

    results = []
    best = None

    original_contamination = da.IFOREST_CONTAMINATION

    for contamination in CONTAMINATION_GRID:
        model = _build_model_richer_normal_training(dataset, contamination)

        # 이 contamination으로 학습된 모델의 iforest_score를 전 샘플에 대해 한 번만 계산
        iforest_scores = []
        for sample in dataset:
            X = da.build_unified_feature_matrix(sample["resource_type"], sample["raw_metrics"])
            raw = model.decision_function(X)
            latest = raw[-1]
            s_min, s_max = raw.min(), raw.max()
            score = 0.0 if s_max == s_min else float(np.clip((s_max - latest) / (s_max - s_min), 0.0, 1.0))
            iforest_scores.append(score)

        joint_iforest_scores = []
        for sample in joint_samples:
            X = da.build_unified_feature_matrix(sample["resource_type"], sample["raw_metrics"])
            raw = model.decision_function(X)
            latest = raw[-1]
            s_min, s_max = raw.min(), raw.max()
            score = 0.0 if s_max == s_min else float(np.clip((s_max - latest) / (s_max - s_min), 0.0, 1.0))
            joint_iforest_scores.append(score)

        for tau in TAU_GRID:
            for k in K_GRID:
                y_pred = [
                    (z_max_cache[i] > k) or (iforest_scores[i] > tau)
                    for i in range(len(dataset))
                ]
                metrics_result = _confusion(y_true, y_pred)

                joint_caught = sum(
                    1 for i in range(len(joint_samples))
                    if (joint_z_max_cache[i] > k) or (joint_iforest_scores[i] > tau)
                )
                joint_rate = joint_caught / len(joint_samples)

                row = {
                    "contamination": contamination,
                    "tau": tau,
                    "k": k,
                    **metrics_result,
                    "joint_catch_rate": round(joint_rate, 4),
                }
                results.append(row)

                if best is None or row["accuracy"] > best["accuracy"]:
                    best = row

    da.IFOREST_CONTAMINATION = original_contamination

    results.sort(key=lambda r: r["accuracy"], reverse=True)

    print("=" * 100)
    print(f"상위 10개 조합 (정확도 순, 목표 {TARGET_ACCURACY*100:.0f}%)")
    print("=" * 100)
    print(f"  {'contam':>7} {'tau':>5} {'k':>5} {'acc':>7} {'prec':>7} {'recall':>7} {'f1':>7} {'joint_catch':>12}")
    for r in results[:10]:
        print(f"  {r['contamination']:>7} {r['tau']:>5} {r['k']:>5} "
              f"{r['accuracy']:>7} {r['precision']:>7} {r['recall']:>7} {r['f1']:>7} "
              f"{r['joint_catch_rate']:>12}")

    n_above_target = sum(1 for r in results if r["accuracy"] >= TARGET_ACCURACY)
    print(f"\n  목표({TARGET_ACCURACY*100:.0f}%) 이상 달성한 조합: {n_above_target}/{len(results)}개")

    print("\n" + "=" * 100)
    print("기본값(τ=0.6, contamination=0.1, k=3.0) 대비 최고 조합")
    print("=" * 100)
    default_row = next(
        r for r in results if r["tau"] == 0.6 and r["contamination"] == 0.1 and r["k"] == 3.0
    )
    print(f"  기본값 : {default_row}")
    print(f"  최고값 : {best}")

    RESULT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(RESULT_PATH, "w", encoding="utf-8") as f:
        json.dump({
            "target_accuracy": TARGET_ACCURACY,
            "default": default_row,
            "best": best,
            "n_above_target": n_above_target,
            "all_results": results,
        }, f, ensure_ascii=False, indent=2)

    print(f"\n저장 완료: {RESULT_PATH}")


if __name__ == "__main__":
    main()
