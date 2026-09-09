"""
요청 A 후보 비교: 실제 5개 시나리오 eval 데이터(전부 anomaly/edge_normal/normal
포함) + 지금 실제 학습된 models/iforest_unified.pkl 기준으로, 알림 경로
(_iforest_score_and_trigger) 후보들을 실측 비교한다.

후보:
  - 현재(k=3, min-max persistence)
  - A: k값 상향(5, 6)
  - B: 절대(decision_function<0) + 상대(min-max>0.5) AND 결합, k=3

각 후보에 대해 normal 오탐률(edge_normal 포함) / anomaly 탐지율을 5개 시나리오
전체에서 측정한다. 롤링 풀 정규화(후보 C)는 리소스 인스턴스별 히스토리 추적이라는
새 아키텍처가 필요해서 이번 비교에서는 제외(추후 별도 검토 필요).

[실행 방법] 프로젝트 루트에서: python playground/compare_alert_path_candidates.py
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


def candidate_current(model, resource_type, metrics):
    normalized = da._normalized_scores(model, resource_type, metrics)
    k_eff = min(3, len(normalized))
    return bool(np.all(normalized[-k_eff:] > da.IFOREST_THRESHOLD))


def candidate_a(model, resource_type, metrics, k):
    normalized = da._normalized_scores(model, resource_type, metrics)
    k_eff = min(k, len(normalized))
    return bool(np.all(normalized[-k_eff:] > da.IFOREST_THRESHOLD))


def candidate_b(model, resource_type, metrics, k=3):
    normalized = da._normalized_scores(model, resource_type, metrics)
    X = da.build_unified_feature_matrix(resource_type, metrics)
    raw = model.decision_function(X)
    k_eff = min(k, len(normalized))
    relative_ok = bool(np.all(normalized[-k_eff:] > da.IFOREST_THRESHOLD))
    absolute_ok = bool(np.all(raw[-k_eff:] < 0))
    return relative_ok and absolute_ok


CANDIDATES = {
    "현재(k=3)": lambda m, rt, metrics: candidate_current(m, rt, metrics),
    "A: k=5": lambda m, rt, metrics: candidate_a(m, rt, metrics, 5),
    "A: k=6": lambda m, rt, metrics: candidate_a(m, rt, metrics, 6),
    "B: 절대+상대 AND(k=3)": lambda m, rt, metrics: candidate_b(m, rt, metrics, 3),
}


def load_windows():
    all_windows = []
    for resource_type, filename in EVAL_TARGETS:
        with open(MOCK_DATA_DIR / filename, encoding="utf-8") as f:
            data = json.load(f)
        for w in data:
            all_windows.append((resource_type, filename, w))
    return all_windows


def main():
    cached = da._load_cached_model(da.IFOREST_UNIFIED_MODEL_NAME)
    if cached is None:
        raise RuntimeError("models/iforest_unified.pkl이 없습니다 - 먼저 재학습 스크립트를 실행하세요.")
    model, _ = cached

    windows = load_windows()

    print(f"{'후보':<26} {'normal(+edge) 오탐':<20} {'anomaly 탐지':<16}")
    print("-" * 64)

    for cand_name, fn in CANDIDATES.items():
        fp_total, fp_count = 0, 0
        tp_total, tp_count = 0, 0
        for resource_type, filename, w in windows:
            triggered = fn(model, resource_type, w["raw_metrics"])
            if w["label"] in ("normal", "edge_normal"):
                fp_total += 1
                fp_count += int(triggered)
            elif w["label"] == "anomaly":
                tp_total += 1
                tp_count += int(triggered)

        print(f"{cand_name:<26} {fp_count}/{fp_total} ({fp_count/fp_total:.1%})"
              f"{'':<6} {tp_count}/{tp_total} ({tp_count/tp_total:.1%})")

    print()
    print("=" * 64)
    print("현재(k=3) vs B(절대+상대 AND, k=3) 시나리오별 비교")
    print("=" * 64)
    for resource_type, filename in EVAL_TARGETS:
        with open(MOCK_DATA_DIR / filename, encoding="utf-8") as f:
            data = json.load(f)
        row = {"current": {"fp": [0, 0], "tp": [0, 0]}, "b": {"fp": [0, 0], "tp": [0, 0]}}
        for w in data:
            cur = candidate_current(model, resource_type, w["raw_metrics"])
            b = candidate_b(model, resource_type, w["raw_metrics"], 3)
            bucket = "fp" if w["label"] in ("normal", "edge_normal") else ("tp" if w["label"] == "anomaly" else None)
            if bucket is None:
                continue
            row["current"][bucket][1] += 1
            row["current"][bucket][0] += int(cur)
            row["b"][bucket][1] += 1
            row["b"][bucket][0] += int(b)
        c, b = row["current"], row["b"]
        print(f"{resource_type}/{filename:<22} "
              f"현재 오탐={c['fp'][0]}/{c['fp'][1]} 탐지={c['tp'][0]}/{c['tp'][1]}  |  "
              f"B 오탐={b['fp'][0]}/{b['fp'][1]} 탐지={b['tp'][0]}/{b['tp'][1]}")


if __name__ == "__main__":
    main()
