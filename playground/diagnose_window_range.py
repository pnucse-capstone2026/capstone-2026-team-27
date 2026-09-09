"""
후보 H 전제 검증.

가설: min-max 정규화는 "창 안에서 제일 이상한 점"을 무조건 1.0으로 만든다.
창의 raw score 범위(max-min)가 좁으면 = 사실 그 점이 나머지보다 딱히 이상하지도
않은데 1.0으로 밀려 올라간 것 = 왜곡이 크다. 범위가 넓으면 = 그 점이 진짜로 튀는
것이라 1.0이 타당하다.

그래서 오탐(정상인데 트리거)난 창은 raw 범위가 좁고, 정탐(진짜 이상)난 창은
raw 범위가 넓을 것이라 예상된다. 이게 맞으면 "범위가 좁을 때만 버퍼 스케일로
폴백"하는 후보 H가 성립하고, 아니면 전제부터 틀린 것이라 중단해야 한다.

[실행 방법] 프로젝트 루트에서: python playground/diagnose_window_range.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import numpy as np

import pipeline.detection_agent as da

MOCK_DATA_DIR = PROJECT_ROOT / "playground" / "mock_data"

EVAL_TARGETS = [
    ("EC2", "ec2_eval.json"),
    ("Lambda", "lambda_eval_retry.json"),
    ("Lambda", "lambda_eval.json"),
    ("AutoScaling", "autoscaling_eval.json"),
    ("S3", "s3_eval.json"),
]


def main():
    cached = da._load_cached_model(da.IFOREST_UNIFIED_MODEL_NAME)
    model, _ = cached

    rows = []
    for resource_type, filename in EVAL_TARGETS:
        with open(MOCK_DATA_DIR / filename, encoding="utf-8") as f:
            data = json.load(f)
        for w in data:
            X = da.build_unified_feature_matrix(resource_type, w["raw_metrics"])
            raw = model.decision_function(X)
            rng = float(raw.max() - raw.min())

            normalized = da._normalized_scores(model, resource_type, w["raw_metrics"])
            k_eff = min(3, len(normalized))
            triggered = bool(np.all(normalized[-k_eff:] > da.IFOREST_THRESHOLD))

            is_pos = w["label"] == "anomaly"
            if is_pos and triggered:
                outcome = "TP"
            elif is_pos and not triggered:
                outcome = "FN"
            elif not is_pos and triggered:
                outcome = "FP"
            else:
                outcome = "TN"

            rows.append({
                "scenario": f"{resource_type}/{filename}",
                "window_id": w["window_id"],
                "label": w["label"],
                "outcome": outcome,
                "raw_range": rng,
            })

    def summarize(name, subset):
        if not subset:
            print(f"  {name:<6} n=0")
            return
        vals = np.array([r["raw_range"] for r in subset])
        print(f"  {name:<6} n={len(vals):<4} 평균={vals.mean():.4f}  중앙={np.median(vals):.4f}  "
              f"최소={vals.min():.4f}  최대={vals.max():.4f}")

    print("=" * 78)
    print("창 raw score 범위(max-min) - 판정 결과별 분포 (전체 5개 시나리오 합산)")
    print("=" * 78)
    for name in ("FP", "TP", "TN", "FN"):
        summarize(name, [r for r in rows if r["outcome"] == name])

    fp = np.array([r["raw_range"] for r in rows if r["outcome"] == "FP"])
    tp = np.array([r["raw_range"] for r in rows if r["outcome"] == "TP"])
    print()
    if len(fp) and len(tp):
        print(f"핵심 비교: FP 최대={fp.max():.4f}  vs  TP 최소={tp.min():.4f}")
        if fp.max() < tp.min():
            print("  -> 완전 분리됨. 이 사이에 하한선을 그으면 FP만 걸러낼 수 있음(후보 H 성립)")
        else:
            overlap = [r for r in rows if r["outcome"] == "TP" and r["raw_range"] <= fp.max()]
            print(f"  -> 겹침. FP 최대값 이하인 TP가 {len(overlap)}개 "
                  f"(전체 TP {len(tp)}개 중 {len(overlap)/len(tp):.0%}) - 이만큼은 같이 희생됨")

    print()
    print("=" * 78)
    print("시나리오별 FP/TP 범위 (cost_spike가 영향받는지 확인용)")
    print("=" * 78)
    for resource_type, filename in EVAL_TARGETS:
        key = f"{resource_type}/{filename}"
        sub_fp = [r["raw_range"] for r in rows if r["scenario"] == key and r["outcome"] == "FP"]
        sub_tp = [r["raw_range"] for r in rows if r["scenario"] == key and r["outcome"] == "TP"]
        fp_s = f"{min(sub_fp):.4f}~{max(sub_fp):.4f}" if sub_fp else "-"
        tp_s = f"{min(sub_tp):.4f}~{max(sub_tp):.4f}" if sub_tp else "-"
        print(f"  {key:<34} FP({len(sub_fp)}): {fp_s:<20} TP({len(sub_tp)}): {tp_s}")


if __name__ == "__main__":
    main()
