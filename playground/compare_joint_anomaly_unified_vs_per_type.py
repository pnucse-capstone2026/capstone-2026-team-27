"""
playground/compare_joint_anomaly_unified_vs_per_type.py

test_iforest_joint_anomaly.py에서 만든 "결합 이상"(Z-score가 못 잡는, 지표 2개가
동시에 완만하게 움직이는) 280개 샘플로, 통합 모델 vs 리소스별 모델의 탐지율을 비교한다.

목적: Phase 2는 "지표 하나만 튀는" 단변량 패턴으로 비교했었는데, 그때는 두 모델이
비슷한 성능(고친 후 76.55% vs 75.17%)이었다. 이번엔 진짜 다변량 패턴에서도
그 결과가 유지되는지, 아니면 통합 모델의 "여러 리소스 타입을 같이 학습"하는 구조가
오히려 다변량 탐지에 유리/불리하게 작용하는지 확인한다.

[실행 방법]
  프로젝트 루트에서: python playground/compare_joint_anomaly_unified_vs_per_type.py

[사전 조건]
  eval_dataset.json 필요 (generate_eval_dataset.py로 생성)
"""

from __future__ import annotations

import json
import shutil
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import numpy as np

import pipeline.detection_agent as da
from playground.analyze_shap_feature_importance import _build_final_unified_model
from playground.compare_iforest_unified_vs_per_type import (
    _get_or_train_iforest_per_type,
    _load_cached_model_per_type,
)
from playground.test_iforest_joint_anomaly import generate_joint_samples, K_POINTS, MULTIPLIER

DATASET_PATH = PROJECT_ROOT / "playground" / "eval_outputs" / "eval_dataset.json"
RESULT_PATH = PROJECT_ROOT / "playground" / "eval_outputs" / "phase_bonus_joint_unified_vs_per_type.json"

IFOREST_THRESHOLD = da.IFOREST_THRESHOLD
RESOURCE_TYPES = da.RESOURCE_TYPES


def _build_final_per_type_models(dataset: list[dict], model_dir: str) -> dict:
    """dataset을 한 번 통과시켜, 리소스 타입마다 각자의 첫 샘플로 학습된 개별 모델
    5개를 만든 뒤 dict로 반환한다 (Phase 2와 동일한 방식 — 타입당 대표 윈도우 1개)."""
    for sample in dataset:
        _get_or_train_iforest_per_type(model_dir, sample["resource_type"], sample["raw_metrics"])

    models = {}
    for rt in RESOURCE_TYPES:
        cached = _load_cached_model_per_type(model_dir, rt)
        if cached is not None:
            models[rt] = cached[0]
    return models


def _score_unified(model, resource_type: str, metrics: dict) -> float:
    X = da.build_unified_feature_matrix(resource_type, metrics)
    raw_scores = model.decision_function(X)
    latest_raw = raw_scores[-1]
    s_min, s_max = raw_scores.min(), raw_scores.max()
    if s_max == s_min:
        return 0.0
    return float(np.clip((s_max - latest_raw) / (s_max - s_min), 0.0, 1.0))


def _score_per_type(model, metrics: dict) -> float:
    feature_keys = sorted(metrics.keys())
    X = np.column_stack([metrics[k] for k in feature_keys])
    raw_scores = model.decision_function(X)
    latest_raw = raw_scores[-1]
    s_min, s_max = raw_scores.min(), raw_scores.max()
    if s_max == s_min:
        return 0.0
    return float(np.clip((s_max - latest_raw) / (s_max - s_min), 0.0, 1.0))


def main() -> None:
    if not DATASET_PATH.exists():
        raise SystemExit(f"{DATASET_PATH}가 없습니다. 먼저 generate_eval_dataset.py를 실행하세요.")

    with open(DATASET_PATH, "r", encoding="utf-8") as f:
        base_dataset = json.load(f)

    joint_samples = generate_joint_samples()
    print(f"결합 이상 샘플 {len(joint_samples)}개 (k={K_POINTS}, 배율={MULTIPLIER})\n")

    tmp_root = tempfile.mkdtemp(prefix="joint_cmp_")
    unified_dir = tmp_root + "/unified"
    per_type_dir = tmp_root + "/per_type"

    final_unified_model = _build_final_unified_model(base_dataset, unified_dir)
    final_per_type_models = _build_final_per_type_models(base_dataset, per_type_dir)
    print("통합/리소스별 모델 둘 다 준비 완료 (5개 리소스 타입 전부 반영)\n")

    n_total = 0
    n_zscore_leaked = 0
    n_unified_caught = 0
    n_per_type_caught = 0
    n_agree = 0

    by_type = defaultdict(lambda: {"total": 0, "unified": 0, "per_type": 0})
    disagreements = []

    for sample in joint_samples:
        rt = sample["resource_type"]
        metrics = sample["raw_metrics"]
        injected = sample["injected_metrics"]

        z_triggered = any(
            da._zscore_check(v)[1] for k, v in metrics.items() if k in da.Z_SCORE_TARGET_METRICS
        )
        if z_triggered:
            n_zscore_leaked += 1
            continue

        n_total += 1
        by_type[rt]["total"] += 1

        score_u = _score_unified(final_unified_model, rt, metrics)
        model_p = final_per_type_models.get(rt)
        score_p = _score_per_type(model_p, metrics) if model_p is not None else 0.0

        caught_u = score_u > IFOREST_THRESHOLD
        caught_p = score_p > IFOREST_THRESHOLD

        if caught_u:
            n_unified_caught += 1
            by_type[rt]["unified"] += 1
        if caught_p:
            n_per_type_caught += 1
            by_type[rt]["per_type"] += 1

        if caught_u == caught_p:
            n_agree += 1
        else:
            disagreements.append({
                "sample_id": sample["sample_id"],
                "resource_type": rt,
                "injected_metrics": injected,
                "unified_score": round(score_u, 4),
                "unified_caught": caught_u,
                "per_type_score": round(score_p, 4),
                "per_type_caught": caught_p,
            })

    shutil.rmtree(tmp_root, ignore_errors=True)

    print("=" * 78)
    print("[결합 이상 탐지율] 통합 모델 vs 리소스별 모델")
    print("=" * 78)
    print(f"  전체 샘플: {len(joint_samples)}개, z-score가 새어 잡은 것 제외: {n_zscore_leaked}개")
    print(f"  '진짜 Z-score-blind' 케이스: {n_total}개\n")
    print(f"  통합 모델    탐지율: {n_unified_caught}/{n_total} = {n_unified_caught/n_total*100:.2f}%")
    print(f"  리소스별 모델 탐지율: {n_per_type_caught}/{n_total} = {n_per_type_caught/n_total*100:.2f}%")
    print(f"  두 모델 일치율: {n_agree}/{n_total} = {n_agree/n_total*100:.2f}%")

    print("\n  리소스 타입별:")
    print(f"  {'타입':<14} {'통합':>10} {'리소스별':>10}")
    for rt in sorted(by_type):
        t = by_type[rt]["total"]
        u = by_type[rt]["unified"]
        p = by_type[rt]["per_type"]
        print(f"  {rt:<14} {u}/{t} ({u/t*100:5.1f}%)   {p}/{t} ({p/t*100:5.1f}%)")

    print(f"\n  불일치 샘플 수: {len(disagreements)}개 (예시 최대 10개)")
    for d in disagreements[:10]:
        print(f"    {d}")

    result = {
        "config": {"k_points": K_POINTS, "multiplier": MULTIPLIER},
        "n_generated": len(joint_samples),
        "n_zscore_leaked": n_zscore_leaked,
        "n_zscore_blind_total": n_total,
        "unified_catch_rate": round(n_unified_caught / n_total, 4) if n_total else None,
        "per_type_catch_rate": round(n_per_type_caught / n_total, 4) if n_total else None,
        "agreement_rate": round(n_agree / n_total, 4) if n_total else None,
        "by_resource_type": {
            rt: {
                "total": v["total"],
                "unified_caught": v["unified"],
                "per_type_caught": v["per_type"],
                "unified_rate": round(v["unified"] / v["total"], 4) if v["total"] else None,
                "per_type_rate": round(v["per_type"] / v["total"], 4) if v["total"] else None,
            }
            for rt, v in by_type.items()
        },
        "disagreement_examples": disagreements[:30],
    }

    RESULT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(RESULT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n저장 완료: {RESULT_PATH}")


if __name__ == "__main__":
    main()
