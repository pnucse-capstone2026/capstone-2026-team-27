"""
요청 A 후보 D: 알림 경로(_iforest_score_and_trigger)의 min-max(_normalized_scores)를
Stage 2에서 이미 검증한 방식(model.score_samples() + 버퍼에서 직접 계산한 percentile
임계값)으로 교체했을 때, 5개 시나리오 전체에서 오탐/탐지율이 어떻게 나오는지 실측.

기존 구조(최근 k=3개 "전부" 조건 만족)는 그대로 유지하고, "조건"만 min-max>0.5에서
raw score_samples<percentile임계값으로 바꾼다 - 후보 B(기존 min-max 위에 조건을
얹는 것)와 다르게 min-max 메커니즘 자체를 안 쓴다.

임계값은 STAGE2_ADMIT_PERCENTILE(2.0, 이미 그리드 실험으로 채택된 값)을 기본으로,
민감도 확인을 위해 1.0/5.0도 같이 비교.

[실행 방법] 프로젝트 루트에서: python playground/compare_candidate_d.py
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

# seed_iforest_from_scenario_mock.py와 동일 - 실제 모델 학습에 쓰인 타입별 30개
TRAIN_FILES = {
    "EC2": "ec2_train.json",
    "Lambda": "lambda_train.json",
    "AutoScaling": "autoscaling_train.json",
    "S3": "s3_train.json",
}


def load_type_buffer(resource_type: str, filename: str) -> np.ndarray:
    with open(MOCK_DATA_DIR / filename, encoding="utf-8") as f:
        data = json.load(f)
    windows = [w for w in data if w["label"] == "normal"][: da.MAX_WINDOWS_PER_TYPE]
    matrices = [da.build_unified_feature_matrix(resource_type, w["raw_metrics"]) for w in windows]
    return np.vstack(matrices)


def candidate_current(model, resource_type, metrics):
    normalized = da._normalized_scores(model, resource_type, metrics)
    k_eff = min(3, len(normalized))
    return bool(np.all(normalized[-k_eff:] > da.IFOREST_THRESHOLD))


def candidate_d(model, resource_type, metrics, threshold, k=3):
    X = da.build_unified_feature_matrix(resource_type, metrics)
    raw = model.score_samples(X)
    k_eff = min(k, len(raw))
    return bool(np.all(raw[-k_eff:] < threshold))


def main():
    cached = da._load_cached_model(da.IFOREST_UNIFIED_MODEL_NAME)
    if cached is None:
        raise RuntimeError("models/iforest_unified.pkl이 없습니다.")
    model, _ = cached

    # 타입별 버퍼 + percentile별 임계값 사전 계산
    buffers = {rt: load_type_buffer(rt, fn) for rt, fn in TRAIN_FILES.items()}
    percentiles = [1.0, 2.0, 5.0]
    thresholds = {
        rt: {p: float(np.percentile(model.score_samples(buf), p)) for p in percentiles}
        for rt, buf in buffers.items()
    }
    print("타입별 percentile 임계값:")
    for rt, ths in thresholds.items():
        print(f"  {rt}: {ths}")

    windows = []
    for resource_type, filename in EVAL_TARGETS:
        with open(MOCK_DATA_DIR / filename, encoding="utf-8") as f:
            data = json.load(f)
        for w in data:
            windows.append((resource_type, filename, w))

    print(f"\n{'후보':<28} {'normal(+edge) 오탐':<20} {'anomaly 탐지':<16}")
    print("-" * 66)

    def aggregate(fn):
        fp_total = fp_count = tp_total = tp_count = 0
        for resource_type, filename, w in windows:
            triggered = fn(resource_type, w["raw_metrics"])
            if w["label"] in ("normal", "edge_normal"):
                fp_total += 1
                fp_count += int(triggered)
            elif w["label"] == "anomaly":
                tp_total += 1
                tp_count += int(triggered)
        return fp_count, fp_total, tp_count, tp_total

    results = {}
    fp_c, fp_t, tp_c, tp_t = aggregate(lambda rt, m: candidate_current(model, rt, m))
    results["현재(min-max, k=3)"] = (fp_c, fp_t, tp_c, tp_t)
    print(f"{'현재(min-max, k=3)':<28} {fp_c}/{fp_t} ({fp_c/fp_t:.1%}){'':<6} {tp_c}/{tp_t} ({tp_c/tp_t:.1%})")

    for p in percentiles:
        fp_c, fp_t, tp_c, tp_t = aggregate(
            lambda rt, m, p=p: candidate_d(model, rt, m, thresholds[rt][p], k=3)
        )
        name = f"D: score_samples(p={p})"
        results[name] = (fp_c, fp_t, tp_c, tp_t)
        print(f"{name:<28} {fp_c}/{fp_t} ({fp_c/fp_t:.1%}){'':<6} {tp_c}/{tp_t} ({tp_c/tp_t:.1%})")

    # 채택 후보(p=2.0)로 시나리오별 breakdown - 후보 B 때와 같은 형식
    print("\n" + "=" * 90)
    print("현재(k=3) vs D(score_samples, p=2.0) 시나리오별 비교")
    print("=" * 90)
    for resource_type, filename in EVAL_TARGETS:
        with open(MOCK_DATA_DIR / filename, encoding="utf-8") as f:
            data = json.load(f)
        row = {"current": {"fp": [0, 0], "tp": [0, 0]}, "d": {"fp": [0, 0], "tp": [0, 0]}}
        for w in data:
            cur = candidate_current(model, resource_type, w["raw_metrics"])
            d = candidate_d(model, resource_type, w["raw_metrics"], thresholds[resource_type][2.0], k=3)
            bucket = "fp" if w["label"] in ("normal", "edge_normal") else ("tp" if w["label"] == "anomaly" else None)
            if bucket is None:
                continue
            row["current"][bucket][1] += 1
            row["current"][bucket][0] += int(cur)
            row["d"][bucket][1] += 1
            row["d"][bucket][0] += int(d)
        c, d = row["current"], row["d"]
        print(f"{resource_type}/{filename:<22} "
              f"현재 오탐={c['fp'][0]}/{c['fp'][1]} 탐지={c['tp'][0]}/{c['tp'][1]}  |  "
              f"D 오탐={d['fp'][0]}/{d['fp'][1]} 탐지={d['tp'][0]}/{d['tp'][1]}")


if __name__ == "__main__":
    main()
