"""
playground/analyze_shap_feature_importance.py

Phase 4: SHAP 기반 피처 중요도 검증 + "어떤 경우에 어떤 피처가 컸는지" 분석.

방법:
  1. Phase 1 데이터셋(eval_dataset.json)을 한 번 통과시켜 통합 IsolationForest 모델이
     5개 리소스 타입을 전부 학습에 반영하게 만든 뒤, 그 "완성된" 모델 하나를 고정해서
     이후 모든 샘플의 SHAP 설명에 동일하게 사용한다 (샘플마다 모델이 바뀌면 비교가 무의미해짐).
  2. 각 스파이크 샘플(injected_metric이 있는 것)에 대해 SHAP 1위 피처를 구하고,
     실제 주입한 지표와 일치하는지 검사 → 전체/리소스타입별/지표별 일치율 산출.
  3. (리소스타입, case) 조합별로 평균 |SHAP|를 피처별로 집계 → "이 케이스에서 어떤
     피처가 지배적인가"를 개별 샘플 노이즈에 덜 흔들리게 요약.

[실행 방법]
  프로젝트 루트에서: python playground/analyze_shap_feature_importance.py

[사전 조건]
  playground/generate_eval_dataset.py 를 먼저 실행해서 eval_dataset.json이 있어야 함.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pipeline.detection_agent as da

DATASET_PATH = PROJECT_ROOT / "playground" / "eval_outputs" / "eval_dataset.json"
RESULT_PATH = PROJECT_ROOT / "playground" / "eval_outputs" / "phase4_shap_analysis.json"


def _build_final_unified_model(dataset: list[dict], model_dir: str):
    """dataset을 한 번 통과시켜 5개 리소스 타입을 전부 학습에 반영시킨 뒤,
    그 시점의 고정된 모델을 반환 (이후 전체 분석에서 이 모델 하나만 재사용)."""
    da.IFOREST_MODEL_DIR = model_dir
    for sample in dataset:
        da._get_or_train_iforest(sample["resource_type"], sample["raw_metrics"])

    cached = da._load_cached_model(da.IFOREST_UNIFIED_MODEL_NAME)
    if cached is None:
        raise RuntimeError("통합 모델 학습 실패 — eval_dataset이 비었거나 문제 있음")
    model, _feature_keys = cached
    return model


def main() -> None:
    if not DATASET_PATH.exists():
        raise SystemExit(f"{DATASET_PATH}가 없습니다. 먼저 generate_eval_dataset.py를 실행하세요.")

    with open(DATASET_PATH, "r", encoding="utf-8") as f:
        dataset = json.load(f)

    print(f"평가 샘플 {len(dataset)}개 로드 완료")

    tmp_dir = tempfile.mkdtemp(prefix="phase4_")
    final_model = _build_final_unified_model(dataset, tmp_dir)
    print("고정된 통합 모델 준비 완료 (5개 리소스 타입 전부 학습에 반영됨)\n")

    # ── [1] SHAP 1위 지표 vs 실제 주입 지표 일치율 ───────────────────────────────
    total = 0
    matched = 0
    by_type_total = defaultdict(int)
    by_type_matched = defaultdict(int)
    by_metric_total = defaultdict(int)
    by_metric_matched = defaultdict(int)
    mismatches = []

    # ── [2] (리소스타입, case)별 평균 |SHAP| 집계용 ────────────────────────────
    agg_sum: dict[tuple[str, str], dict[str, float]] = defaultdict(lambda: defaultdict(float))
    agg_count: dict[tuple[str, str], int] = defaultdict(int)

    for sample in dataset:
        rt = sample["resource_type"]
        case = sample["case"]
        injected = sample["injected_metric"]

        top_features = da.explain_iforest_top_features(rt, sample["raw_metrics"], model=final_model)

        # 케이스별 평균 |SHAP| 누적
        key = (rt, case)
        for feat, val in top_features.items():
            agg_sum[key][feat] += abs(val)
        agg_count[key] += 1

        if injected is None:
            continue  # 정상 샘플은 "정답 피처"가 없어서 일치율 계산 대상 아님

        total += 1
        by_type_total[rt] += 1
        by_metric_total[injected] += 1

        top1 = next(iter(top_features)) if top_features else None
        is_match = top1 == injected
        if is_match:
            matched += 1
            by_type_matched[rt] += 1
            by_metric_matched[injected] += 1
        else:
            mismatches.append({
                "sample_id": sample["sample_id"],
                "resource_type": rt,
                "injected_metric": injected,
                "shap_top1": top1,
                "shap_top3": dict(list(top_features.items())[:3]),
            })

    shutil.rmtree(tmp_dir, ignore_errors=True)

    # ── 출력: [1] 일치율 ─────────────────────────────────────────────────────
    print("=" * 78)
    print("[1] SHAP 1위 지표 == 실제 주입 지표 일치율")
    print("=" * 78)
    print(f"  전체: {matched}/{total} = {matched/total*100:.2f}%")
    print("\n  리소스 타입별:")
    for rt in sorted(by_type_total):
        t, m = by_type_total[rt], by_type_matched[rt]
        print(f"    {rt:<14} {m}/{t} = {m/t*100:.2f}%")
    print("\n  지표별:")
    for metric in sorted(by_metric_total):
        t, m = by_metric_total[metric], by_metric_matched[metric]
        print(f"    {metric:<28} {m}/{t} = {m/t*100:.2f}%")

    print(f"\n  불일치 샘플 수: {len(mismatches)}개 (예시 최대 10개)")
    for mm in mismatches[:10]:
        print(f"    {mm}")

    # ── 출력: [2] 케이스별 평균 |SHAP| ────────────────────────────────────────
    print("\n" + "=" * 78)
    print("[2] (리소스타입, case)별 평균 |SHAP| 피처 중요도 (상위 3개)")
    print("=" * 78)
    case_summary = {}
    for key in sorted(agg_sum):
        rt, case = key
        n = agg_count[key]
        avg = {feat: round(s / n, 4) for feat, s in agg_sum[key].items()}
        top3 = dict(sorted(avg.items(), key=lambda kv: kv[1], reverse=True)[:3])
        case_summary[f"{rt}/{case}"] = top3
        print(f"  {rt:<14} {case:<32} {top3}")

    # ── 저장 ─────────────────────────────────────────────────────────────────
    result = {
        "n_total_anomaly_samples": total,
        "match_rate_overall": round(matched / total, 4),
        "match_rate_by_resource_type": {
            rt: round(by_type_matched[rt] / by_type_total[rt], 4) for rt in by_type_total
        },
        "match_rate_by_metric": {
            m: round(by_metric_matched[m] / by_metric_total[m], 4) for m in by_metric_total
        },
        "mismatches": mismatches,
        "case_avg_shap_top3": case_summary,
    }

    RESULT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(RESULT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n저장 완료: {RESULT_PATH}")


if __name__ == "__main__":
    main()
