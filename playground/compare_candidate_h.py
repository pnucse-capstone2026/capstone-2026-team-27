"""
후보 H 실측: "창 raw score 범위가 좁을 때만 버퍼 스케일로 폴백".

기본은 지금과 동일(창 min-max + 임계값 0.5 + persistence k=3). 단, 창의 raw score
범위(max-min)가 하한선(floor)보다 좁으면 - 즉 "제일 튀는 점을 1.0으로 밀어올리는"
왜곡이 심한 경우에만 - 창 min/max 대신 타입별 버퍼의 min/max를 스케일로 쓴다.

floor는 감으로 정하지 않고 학습 버퍼 각 윈도우의 raw 범위 분포에서 도출한다
(정상 윈도우들이 보통 어느 정도 범위를 갖는지가 기준).

TP 쪽뿐 아니라 TN -> FP 전환(폴백된 정상 창이 버퍼 스케일에서 걸리는 경우)도
시나리오별로 같이 측정한다.

[실행 방법] 프로젝트 루트에서: python playground/compare_candidate_h.py
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

TRAIN_FILES = {
    "EC2": "ec2_train.json",
    "Lambda": "lambda_train.json",
    "AutoScaling": "autoscaling_train.json",
    "S3": "s3_train.json",
}


def load_buffer_windows(resource_type: str, filename: str) -> list[np.ndarray]:
    with open(MOCK_DATA_DIR / filename, encoding="utf-8") as f:
        data = json.load(f)
    ws = [w for w in data if w["label"] == "normal"][: da.MAX_WINDOWS_PER_TYPE]
    return [da.build_unified_feature_matrix(resource_type, w["raw_metrics"]) for w in ws]


def candidate_current(model, resource_type, metrics):
    normalized = da._normalized_scores(model, resource_type, metrics)
    k_eff = min(3, len(normalized))
    return bool(np.all(normalized[-k_eff:] > da.IFOREST_THRESHOLD))


def candidate_h(model, resource_type, metrics, floor, buf_lo, buf_hi, k=3):
    X = da.build_unified_feature_matrix(resource_type, metrics)
    raw = model.decision_function(X)
    s_min, s_max = float(raw.min()), float(raw.max())
    used_fallback = (s_max - s_min) < floor
    lo, hi = (buf_lo, buf_hi) if used_fallback else (s_min, s_max)
    if hi == lo:
        return False, used_fallback
    normalized = np.clip((hi - raw) / (hi - lo), 0.0, 1.0)
    k_eff = min(k, len(normalized))
    return bool(np.all(normalized[-k_eff:] > da.IFOREST_THRESHOLD)), used_fallback


def main():
    cached = da._load_cached_model(da.IFOREST_UNIFIED_MODEL_NAME)
    model, _ = cached

    # 버퍼: 타입별 윈도우 리스트 + 각 윈도우의 raw 범위 분포에서 floor 도출
    buf_windows = {rt: load_buffer_windows(rt, fn) for rt, fn in TRAIN_FILES.items()}
    buf_scale = {}
    buf_ranges = {}
    for rt, mats in buf_windows.items():
        all_rows = np.vstack(mats)
        d_all = model.decision_function(all_rows)
        buf_scale[rt] = (float(d_all.min()), float(d_all.max()))
        rngs = [float(np.ptp(model.decision_function(m))) for m in mats]
        buf_ranges[rt] = np.array(rngs)

    print("타입별 학습 버퍼 윈도우의 raw 범위 분포 (floor 도출 근거)")
    for rt, r in buf_ranges.items():
        print(f"  {rt:<12} n={len(r)}  p10={np.percentile(r,10):.4f}  중앙={np.median(r):.4f}  "
              f"p90={np.percentile(r,90):.4f}   버퍼스케일={buf_scale[rt][0]:.4f}~{buf_scale[rt][1]:.4f}")

    floors = {
        "p10": {rt: float(np.percentile(r, 10)) for rt, r in buf_ranges.items()},
        "p25": {rt: float(np.percentile(r, 25)) for rt, r in buf_ranges.items()},
        "median": {rt: float(np.median(r)) for rt, r in buf_ranges.items()},
    }

    windows = []
    for resource_type, filename in EVAL_TARGETS:
        with open(MOCK_DATA_DIR / filename, encoding="utf-8") as f:
            for w in json.load(f):
                windows.append((resource_type, filename, w))

    print(f"\n{'후보':<22} {'normal(+edge) 오탐':<20} {'anomaly 탐지':<16} {'폴백된 창':<12}")
    print("-" * 74)

    fp_c = fp_t = tp_c = tp_t = 0
    for rt, fn, w in windows:
        trig = candidate_current(model, rt, w["raw_metrics"])
        if w["label"] in ("normal", "edge_normal"):
            fp_t += 1; fp_c += int(trig)
        elif w["label"] == "anomaly":
            tp_t += 1; tp_c += int(trig)
    print(f"{'현재':<22} {fp_c}/{fp_t} ({fp_c/fp_t:.1%}){'':<6} {tp_c}/{tp_t} ({tp_c/tp_t:.1%}){'':<4} -")
    base = (fp_c / fp_t, tp_c / tp_t)

    results = {}
    for fname, fmap in floors.items():
        fp_c = fp_t = tp_c = tp_t = nfb = 0
        detail = {}
        for rt, fn, w in windows:
            trig, fb = candidate_h(model, rt, w["raw_metrics"], fmap[rt], *buf_scale[rt])
            nfb += int(fb)
            key = f"{rt}/{fn}"
            d = detail.setdefault(key, {"fp": [0, 0], "tp": [0, 0]})
            if w["label"] in ("normal", "edge_normal"):
                fp_t += 1; fp_c += int(trig)
                d["fp"][1] += 1; d["fp"][0] += int(trig)
            elif w["label"] == "anomaly":
                tp_t += 1; tp_c += int(trig)
                d["tp"][1] += 1; d["tp"][0] += int(trig)
        results[fname] = (fp_c, fp_t, tp_c, tp_t, detail)
        print(f"{'H(floor=' + fname + ')':<22} {fp_c}/{fp_t} ({fp_c/fp_t:.1%}){'':<6} "
              f"{tp_c}/{tp_t} ({tp_c/tp_t:.1%}){'':<4} {nfb}/{len(windows)}")

    print(f"\n현재 기준: 오탐 {base[0]:.1%}, 탐지 {base[1]:.1%}")

    for fname, (fp_c, fp_t, tp_c, tp_t, detail) in results.items():
        print(f"\n=== H(floor={fname}) 시나리오별 (오탐 · 탐지) ===")
        for rt, fn in EVAL_TARGETS:
            key = f"{rt}/{fn}"
            # 현재값도 같이
            cf = ct = cfn = ctn = 0
            with open(MOCK_DATA_DIR / fn, encoding="utf-8") as f:
                for w in json.load(f):
                    trig = candidate_current(model, rt, w["raw_metrics"])
                    if w["label"] in ("normal", "edge_normal"):
                        cfn += 1; cf += int(trig)
                    elif w["label"] == "anomaly":
                        ctn += 1; ct += int(trig)
            d = detail[key]
            print(f"  {key:<34} 현재 {cf}/{cfn} · {ct}/{ctn}   ->   H {d['fp'][0]}/{d['fp'][1]} · {d['tp'][0]}/{d['tp'][1]}")


if __name__ == "__main__":
    main()
