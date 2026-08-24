"""
playground/test_iforest_joint_anomaly.py

보너스 검증: "IsolationForest가 다변량(multivariate) 이상을 실제로 잘 잡아내는가?"

Phase 1 데이터셋(eval_dataset.json)은 지표 하나만 스파이크시키는 케이스만 다뤄서,
이건 사실 Z-score도 잡을 수 있는 "단변량" 패턴이다. IForest 고유의 강점은
"지표 하나하나는 각자 정상 범위인데, 여러 지표가 같이 움직이는 조합이 이상한 경우"를
잡는 것인데, 지금까지는 이걸 직접 검증한 적이 없다.

이 스크립트는:
  1. 지표 2개를 동시에, Z-score 임계값(k=3.0)을 안 넘는 수준으로만 완만하게
     같이 튀우는 "결합 이상" 샘플을 만든다 (개별 지표만 보면 정상처럼 보임).
  2. Z-score는 예상대로 못 잡는지 확인한다 (안 잡히는 게 정상 — 이게 이 테스트의 전제).
  3. IsolationForest가 이걸 잡아내는 비율(=Z-score가 못 잡는 걸 IForest가
     보완하는 비율)을 측정한다.
  4. SHAP top-2가 실제로 주입한 2개 지표와 얼마나 일치하는지도 같이 확인한다.

[실행 방법]
  프로젝트 루트에서: python playground/test_iforest_joint_anomaly.py

[사전 조건]
  playground/generate_eval_dataset.py, playground/eval_outputs/eval_dataset.json 필요
  (같은 "완성된" 통합 모델을 재사용해서 Phase 4와 일관된 기준으로 비교)
"""

from __future__ import annotations

import itertools
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
from playground.generate_eval_dataset import BASELINES, WINDOW_SIZE
from playground.analyze_shap_feature_importance import _build_final_unified_model

DATASET_PATH = PROJECT_ROOT / "playground" / "eval_outputs" / "eval_dataset.json"
RESULT_PATH = PROJECT_ROOT / "playground" / "eval_outputs" / "phase_bonus_joint_anomaly.json"

K_POINTS = 8          # 마지막 몇 개 포인트를 같이 튀울지
MULTIPLIER = 2.0      # 베이스라인 대비 배율 (완만한 수준 — 10배가 아니라 2배)
N_SAMPLES_PER_PAIR = 10
RANDOM_SEED = 123


def _normal_window(rng, base, noise, n=WINDOW_SIZE):
    return base + rng.uniform(-noise, noise, size=n)


def _joint_shift_window(rng, base, noise, n=WINDOW_SIZE):
    window = base + rng.uniform(-noise, noise, size=n)
    window[-K_POINTS:] = base * MULTIPLIER + rng.uniform(-noise, noise, size=K_POINTS)
    return window


def _make_joint_sample(resource_type: str, metric_a: str, metric_b: str, idx: int) -> dict:
    seed = RANDOM_SEED + hash((resource_type, metric_a, metric_b, idx)) % 1_000_000
    rng = np.random.default_rng(seed)

    metrics = {}
    for metric, (base, noise) in BASELINES[resource_type].items():
        if metric in (metric_a, metric_b):
            metrics[metric] = _joint_shift_window(rng, base, noise).tolist()
        else:
            metrics[metric] = _normal_window(rng, base, noise).tolist()

    return {
        "sample_id": f"{resource_type}_joint_{metric_a}+{metric_b}_{idx:03d}",
        "resource_type": resource_type,
        "injected_metrics": [metric_a, metric_b],
        "raw_metrics": metrics,
    }


def generate_joint_samples() -> list[dict]:
    samples = []
    for resource_type, metric_baselines in BASELINES.items():
        metric_names = list(metric_baselines.keys())
        for metric_a, metric_b in itertools.combinations(metric_names, 2):
            for i in range(N_SAMPLES_PER_PAIR):
                samples.append(_make_joint_sample(resource_type, metric_a, metric_b, i))
    return samples


def main() -> None:
    if not DATASET_PATH.exists():
        raise SystemExit(f"{DATASET_PATH}가 없습니다. 먼저 generate_eval_dataset.py를 실행하세요.")

    with open(DATASET_PATH, "r", encoding="utf-8") as f:
        base_dataset = json.load(f)

    joint_samples = generate_joint_samples()
    print(f"결합 이상 샘플 {len(joint_samples)}개 생성 완료 "
          f"(지표쌍 x {N_SAMPLES_PER_PAIR}개씩, k={K_POINTS}포인트, 배율={MULTIPLIER})\n")

    tmp_dir = tempfile.mkdtemp(prefix="joint_anomaly_")
    final_model = _build_final_unified_model(base_dataset, tmp_dir)
    print("고정된 통합 모델 준비 완료 (Phase 4와 동일한 기준)\n")

    n_total = 0
    n_zscore_leaked = 0     # 의도와 다르게 z-score가 잡아버린 경우 (전제 깨짐 — 제외 대상)
    n_iforest_caught = 0    # z-score는 못 잡았는데 iforest가 잡은 경우 (핵심 지표)
    n_shap_top2_match = 0

    by_type = defaultdict(lambda: {"total": 0, "iforest_caught": 0})
    examples_caught = []
    examples_missed = []

    for sample in joint_samples:
        rt = sample["resource_type"]
        metrics = sample["raw_metrics"]
        injected = set(sample["injected_metrics"])

        z_triggered = any(
            da._zscore_check(v)[1] for k, v in metrics.items() if k in da.Z_SCORE_TARGET_METRICS
        )

        if z_triggered:
            n_zscore_leaked += 1
            continue  # 이 케이스는 "z-score가 못 잡는 이상"이라는 전제가 깨졌으니 집계 제외

        n_total += 1
        by_type[rt]["total"] += 1

        da.IFOREST_MODEL_DIR = tmp_dir  # 안전하게 고정 (모델은 final_model을 직접 넘길 거라 실질 영향 없음)
        X = da.build_unified_feature_matrix(rt, metrics)
        raw_scores = final_model.decision_function(X)
        latest_raw = raw_scores[-1]
        s_min, s_max = raw_scores.min(), raw_scores.max()
        iforest_score = 0.0 if s_max == s_min else float(
            np.clip((s_max - latest_raw) / (s_max - s_min), 0.0, 1.0)
        )
        iforest_caught = iforest_score > da.IFOREST_THRESHOLD

        top_features = da.explain_iforest_top_features(rt, metrics, model=final_model, top_n=2)
        shap_top2 = set(top_features.keys())
        shap_match = shap_top2 == injected

        record = {
            "sample_id": sample["sample_id"],
            "resource_type": rt,
            "injected_metrics": sorted(injected),
            "iforest_score": round(iforest_score, 4),
            "shap_top2": list(top_features.keys()),
        }

        if iforest_caught:
            n_iforest_caught += 1
            by_type[rt]["iforest_caught"] += 1
            if len(examples_caught) < 5:
                examples_caught.append(record)
        else:
            if len(examples_missed) < 5:
                examples_missed.append(record)

        if shap_match:
            n_shap_top2_match += 1

    shutil.rmtree(tmp_dir, ignore_errors=True)

    print("=" * 78)
    print("[결합 이상 탐지 결과] — Z-score는 못 잡는 케이스만 집계")
    print("=" * 78)
    print(f"  전체 결합 이상 샘플: {len(joint_samples)}개")
    print(f"  그중 z-score가 이미 잡아버려서 제외된 것: {n_zscore_leaked}개")
    print(f"  '진짜 Z-score-blind' 케이스: {n_total}개")
    print(f"  → IForest가 이 중 잡아낸 비율: {n_iforest_caught}/{n_total} = "
          f"{n_iforest_caught/n_total*100:.2f}%")
    print(f"  → SHAP top-2가 실제 주입 지표 2개와 정확히 일치한 비율: "
          f"{n_shap_top2_match}/{n_total} = {n_shap_top2_match/n_total*100:.2f}%")

    print("\n  리소스 타입별 IForest 탐지율:")
    for rt in sorted(by_type):
        t = by_type[rt]["total"]
        c = by_type[rt]["iforest_caught"]
        if t:
            print(f"    {rt:<14} {c}/{t} = {c/t*100:.2f}%")

    print("\n  잡아낸 예시 (최대 5개):")
    for e in examples_caught:
        print(f"    {e}")
    print("\n  놓친 예시 (최대 5개):")
    for e in examples_missed:
        print(f"    {e}")

    result = {
        "config": {"k_points": K_POINTS, "multiplier": MULTIPLIER, "n_per_pair": N_SAMPLES_PER_PAIR},
        "n_generated": len(joint_samples),
        "n_zscore_leaked": n_zscore_leaked,
        "n_zscore_blind_total": n_total,
        "iforest_catch_rate": round(n_iforest_caught / n_total, 4) if n_total else None,
        "shap_top2_match_rate": round(n_shap_top2_match / n_total, 4) if n_total else None,
        "by_resource_type": {
            rt: {"total": v["total"], "iforest_caught": v["iforest_caught"],
                 "rate": round(v["iforest_caught"] / v["total"], 4) if v["total"] else None}
            for rt, v in by_type.items()
        },
        "examples_caught": examples_caught,
        "examples_missed": examples_missed,
    }

    RESULT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(RESULT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n저장 완료: {RESULT_PATH}")


if __name__ == "__main__":
    main()
