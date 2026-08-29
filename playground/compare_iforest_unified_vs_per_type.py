"""
playground/compare_iforest_unified_vs_per_type.py

Phase 2: "통합 IsolationForest 모델"(현재, pipeline/detection_agent.py) vs
"리소스 타입별 개별 모델"(리팩터링 전, git 61f48f8 시점 로직 그대로 이식)의
성능을 Phase 1 데이터셋(playground/eval_outputs/eval_dataset.json)으로 비교한다.

비교 축:
  1) 정답(ground_truth_anomaly) 기준 정확도 — 메인 지표
     - IForest 단독 기여분 (Z-score 영향 배제하고 IForest만 따로)
     - 전체 앙상블 (Z-score OR IForest) — 실제 detection_node와 동일한 최종 판단
  2) 두 모델의 예측 일치율 — 보조 지표 (정답과 무관하게 "둘이 같은 판단을 내렸는가")
  3) 추론 속도(레이턴시) 비교

[실행 방법]
  프로젝트 루트에서: python playground/compare_iforest_unified_vs_per_type.py

[사전 조건]
  playground/generate_eval_dataset.py를 먼저 실행해서 eval_dataset.json이 있어야 함.
"""

from __future__ import annotations

import json
import os
import pickle
import shutil
import sys
import tempfile
import time
from pathlib import Path
from statistics import mean

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import numpy as np
from sklearn.ensemble import IsolationForest

import pipeline.detection_agent as da

DATASET_PATH = PROJECT_ROOT / "playground" / "eval_outputs" / "eval_dataset.json"
RESULT_PATH = PROJECT_ROOT / "playground" / "eval_outputs" / "phase2_comparison.json"

MIN_POINTS_FOR_IFOREST = da.MIN_POINTS_FOR_IFOREST
IFOREST_CONTAMINATION = da.IFOREST_CONTAMINATION
IFOREST_RANDOM_STATE = da.IFOREST_RANDOM_STATE
IFOREST_THRESHOLD = da.IFOREST_THRESHOLD


# ══════════════════════════════════════════════════════════════════════════
# "이전" 버전 — git 61f48f8(리팩터링 전) 로직을 그대로 이식 (리소스 타입별 개별 모델)
# ══════════════════════════════════════════════════════════════════════════

def _model_path_per_type(model_dir: str, resource_type: str) -> str:
    os.makedirs(model_dir, exist_ok=True)
    return os.path.join(model_dir, f"iforest_{resource_type}.pkl")


def _load_cached_model_per_type(model_dir: str, resource_type: str):
    path = _model_path_per_type(model_dir, resource_type)
    if not os.path.exists(path):
        return None
    with open(path, "rb") as f:
        model, feature_keys, _trained_at = pickle.load(f)
    return model, feature_keys


def _save_model_per_type(model_dir: str, resource_type: str, model, feature_keys: list[str]) -> None:
    with open(_model_path_per_type(model_dir, resource_type), "wb") as f:
        pickle.dump((model, feature_keys, time.time()), f)


def _get_or_train_iforest_per_type(model_dir: str, resource_type: str, metrics: dict):
    feature_keys = sorted(metrics.keys())

    cached = _load_cached_model_per_type(model_dir, resource_type)
    if cached is not None:
        model, cached_keys = cached
        if cached_keys == feature_keys:
            return model, feature_keys

    lengths = {len(metrics[k]) for k in feature_keys}
    if len(lengths) != 1 or min(lengths) < MIN_POINTS_FOR_IFOREST:
        return None, feature_keys

    X = np.column_stack([metrics[k] for k in feature_keys])
    model = IsolationForest(contamination=IFOREST_CONTAMINATION, random_state=IFOREST_RANDOM_STATE)
    model.fit(X)
    _save_model_per_type(model_dir, resource_type, model, feature_keys)
    return model, feature_keys


def _iforest_score_per_type(model_dir: str, resource_type: str, metrics: dict) -> tuple[float, float]:
    """(score, elapsed_sec) 반환."""
    model, feature_keys = _get_or_train_iforest_per_type(model_dir, resource_type, metrics)
    if model is None:
        return 0.0, 0.0

    X = np.column_stack([metrics[k] for k in feature_keys])

    start = time.perf_counter()
    raw_scores = model.decision_function(X)
    latest_raw = raw_scores[-1]
    s_min, s_max = raw_scores.min(), raw_scores.max()
    score = 0.0 if s_max == s_min else float(np.clip((s_max - latest_raw) / (s_max - s_min), 0.0, 1.0))
    elapsed = time.perf_counter() - start

    return score, elapsed


# ══════════════════════════════════════════════════════════════════════════
# "현재" 버전 — pipeline/detection_agent.py의 통합 모델 그대로 호출
# ══════════════════════════════════════════════════════════════════════════

def _iforest_score_unified(metrics: dict, resource_type: str) -> tuple[float, float]:
    """(score, elapsed_sec) 반환. da.IFOREST_MODEL_DIR이 호출 전 격리된 dir로 세팅돼 있어야 함."""
    model = da._get_or_train_iforest(resource_type, metrics)
    if model is None:
        return 0.0, 0.0

    X = da.build_unified_feature_matrix(resource_type, metrics)

    start = time.perf_counter()
    raw_scores = model.decision_function(X)
    latest_raw = raw_scores[-1]
    s_min, s_max = raw_scores.min(), raw_scores.max()
    score = 0.0 if s_max == s_min else float(np.clip((s_max - latest_raw) / (s_max - s_min), 0.0, 1.0))
    elapsed = time.perf_counter() - start

    return score, elapsed


# ══════════════════════════════════════════════════════════════════════════
# Z-score (통합/개별 공통 — 리팩터링으로 안 바뀐 부분이라 원본 함수 그대로 재사용)
# ══════════════════════════════════════════════════════════════════════════

def _zscore_triggered(metrics: dict) -> bool:
    for metric_name, values in metrics.items():
        if metric_name not in da.Z_SCORE_TARGET_METRICS:
            continue
        _z, is_triggered = da._zscore_check(values)
        if is_triggered:
            return True
    return False


# ══════════════════════════════════════════════════════════════════════════
# 평가 지표
# ══════════════════════════════════════════════════════════════════════════

def _confusion(y_true: list[bool], y_pred: list[bool]) -> dict:
    tp = sum(1 for t, p in zip(y_true, y_pred) if t and p)
    fp = sum(1 for t, p in zip(y_true, y_pred) if not t and p)
    tn = sum(1 for t, p in zip(y_true, y_pred) if not t and not p)
    fn = sum(1 for t, p in zip(y_true, y_pred) if t and not p)

    accuracy = (tp + tn) / len(y_true) if y_true else 0.0
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0

    return {
        "tp": tp, "fp": fp, "tn": tn, "fn": fn,
        "accuracy": round(accuracy, 4),
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
    }


# ══════════════════════════════════════════════════════════════════════════
# 메인 평가 루프
# ══════════════════════════════════════════════════════════════════════════

def main() -> None:
    if not DATASET_PATH.exists():
        raise SystemExit(f"{DATASET_PATH}가 없습니다. 먼저 generate_eval_dataset.py를 실행하세요.")

    with open(DATASET_PATH, "r", encoding="utf-8") as f:
        dataset = json.load(f)

    print(f"평가 샘플 {len(dataset)}개 로드 완료\n")

    tmp_root = tempfile.mkdtemp(prefix="phase2_")
    unified_dir = os.path.join(tmp_root, "unified")
    per_type_dir = os.path.join(tmp_root, "per_type")
    da.IFOREST_MODEL_DIR = unified_dir

    y_true: list[bool] = []
    zscore_pred: list[bool] = []
    iforest_unified_pred: list[bool] = []
    iforest_per_type_pred: list[bool] = []
    ensemble_unified_pred: list[bool] = []
    ensemble_per_type_pred: list[bool] = []

    unified_times: list[float] = []
    per_type_times: list[float] = []

    disagreements: list[dict] = []

    for sample in dataset:
        rt = sample["resource_type"]
        metrics = sample["raw_metrics"]
        gt = sample["ground_truth_anomaly"]

        z_trig = _zscore_triggered(metrics)

        da.IFOREST_MODEL_DIR = unified_dir
        score_u, t_u = _iforest_score_unified(metrics, rt)
        score_p, t_p = _iforest_score_per_type(per_type_dir, rt, metrics)

        if_u = score_u > IFOREST_THRESHOLD
        if_p = score_p > IFOREST_THRESHOLD
        ens_u = z_trig or if_u
        ens_p = z_trig or if_p

        y_true.append(gt)
        zscore_pred.append(z_trig)
        iforest_unified_pred.append(if_u)
        iforest_per_type_pred.append(if_p)
        ensemble_unified_pred.append(ens_u)
        ensemble_per_type_pred.append(ens_p)

        unified_times.append(t_u)
        per_type_times.append(t_p)

        if ens_u != ens_p:
            disagreements.append({
                "sample_id": sample["sample_id"],
                "ground_truth": gt,
                "unified_pred": ens_u,
                "per_type_pred": ens_p,
                "iforest_score_unified": round(score_u, 4),
                "iforest_score_per_type": round(score_p, 4),
            })

    shutil.rmtree(tmp_root, ignore_errors=True)

    # ── 1) 정답 기준 정확도 (메인) ──────────────────────────────────────────
    metrics_iforest_only_unified = _confusion(y_true, iforest_unified_pred)
    metrics_iforest_only_per_type = _confusion(y_true, iforest_per_type_pred)
    metrics_ensemble_unified = _confusion(y_true, ensemble_unified_pred)
    metrics_ensemble_per_type = _confusion(y_true, ensemble_per_type_pred)

    # ── 2) 두 모델 간 일치율 (보조) ──────────────────────────────────────────
    n = len(dataset)
    agree_iforest_only = sum(
        1 for a, b in zip(iforest_unified_pred, iforest_per_type_pred) if a == b
    ) / n
    agree_ensemble = sum(
        1 for a, b in zip(ensemble_unified_pred, ensemble_per_type_pred) if a == b
    ) / n

    # ── 3) 속도 ──────────────────────────────────────────────────────────
    speed_summary = {
        "unified_avg_ms": round(mean(unified_times) * 1000, 4),
        "per_type_avg_ms": round(mean(per_type_times) * 1000, 4),
    }

    # ── 출력 ─────────────────────────────────────────────────────────────
    print("=" * 78)
    print("[1] 정답 기준 정확도 — IForest 단독 기여분")
    print("=" * 78)
    print(f"  통합 모델    : {metrics_iforest_only_unified}")
    print(f"  리소스별 모델: {metrics_iforest_only_per_type}")

    print("\n" + "=" * 78)
    print("[1] 정답 기준 정확도 — 전체 앙상블 (Z-score OR IForest, 실제 detection_node와 동일)")
    print("=" * 78)
    print(f"  통합 모델    : {metrics_ensemble_unified}")
    print(f"  리소스별 모델: {metrics_ensemble_per_type}")

    print("\n" + "=" * 78)
    print("[2] 두 모델 간 예측 일치율 (보조 지표)")
    print("=" * 78)
    print(f"  IForest 단독 기준 일치율 : {agree_iforest_only * 100:.2f}%")
    print(f"  전체 앙상블 기준 일치율 : {agree_ensemble * 100:.2f}%")
    print(f"  불일치 샘플 수          : {len(disagreements)} / {n}")
    if disagreements:
        print("  불일치 샘플 예시 (최대 10개):")
        for d in disagreements[:10]:
            print(f"    {d}")

    print("\n" + "=" * 78)
    print("[3] 추론 속도 비교 (IForest decision_function만, ms/sample 평균)")
    print("=" * 78)
    print(f"  통합 모델    : {speed_summary['unified_avg_ms']} ms")
    print(f"  리소스별 모델: {speed_summary['per_type_avg_ms']} ms")

    result = {
        "n_samples": n,
        "accuracy_iforest_only": {
            "unified": metrics_iforest_only_unified,
            "per_type": metrics_iforest_only_per_type,
        },
        "accuracy_ensemble": {
            "unified": metrics_ensemble_unified,
            "per_type": metrics_ensemble_per_type,
        },
        "agreement": {
            "iforest_only": round(agree_iforest_only, 4),
            "ensemble": round(agree_ensemble, 4),
            "n_disagreements": len(disagreements),
            "disagreement_examples": disagreements,
        },
        "speed": speed_summary,
    }

    RESULT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(RESULT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n저장 완료: {RESULT_PATH}")


if __name__ == "__main__":
    main()
