"""
요청 A 후보 E/F/G 실측.

지금까지 탈락한 것:
  - A(k 상향): 탐지율 붕괴
  - B(min-max + decision_function<0 AND): 팀원 cost_spike 탐지력 손상
  - D(score_samples + 버퍼 percentile 하드 임계값): 탐지율 84%->38% 붕괴

이번 후보:
  - E: 정규화 기준을 "이 창의 min/max"가 아니라 "타입별 학습 버퍼의 min/max"로
       교체(스케일만 바꿈, 임계값 0.5와 persistence k=3은 그대로).
       min-max의 구조적 결함("창 안 제일 튀는 점은 항상 1.0")을 없애면서
       창-상대 적응성 대신 타입-상대 적응성을 쓴다.
  - F: E와 같지만 버퍼의 p1/p99를 스케일 양끝으로 사용(버퍼 자체 이상치에 견고).
  - G: 후보 D의 percentile을 더 느슨하게(10, 20) - D 계열이 어떤 p에서도
       현재를 못 이기는지 확인(D는 p=1/2/5에서 탐지 36/38/68%, 오탐 4/4/8%였음).

[실행 방법] 프로젝트 루트에서: python playground/compare_candidate_efg.py
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


def load_type_buffer(resource_type: str, filename: str) -> np.ndarray:
    with open(MOCK_DATA_DIR / filename, encoding="utf-8") as f:
        data = json.load(f)
    windows = [w for w in data if w["label"] == "normal"][: da.MAX_WINDOWS_PER_TYPE]
    return np.vstack([da.build_unified_feature_matrix(resource_type, w["raw_metrics"]) for w in windows])


def candidate_current(model, resource_type, metrics):
    normalized = da._normalized_scores(model, resource_type, metrics)
    k_eff = min(3, len(normalized))
    return bool(np.all(normalized[-k_eff:] > da.IFOREST_THRESHOLD))


def candidate_buffer_scaled(model, resource_type, metrics, lo, hi, k=3):
    """버퍼 기준 스케일로 정규화 (1에 가까울수록 이상). lo/hi는 버퍼 raw score의
    하한/상한 - 창 자체의 min/max를 안 쓰므로 "제일 튀는 점=항상 1.0" 구조가 없다."""
    X = da.build_unified_feature_matrix(resource_type, metrics)
    raw = model.decision_function(X)
    if hi == lo:
        return False
    normalized = np.clip((hi - raw) / (hi - lo), 0.0, 1.0)
    k_eff = min(k, len(normalized))
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

    buffers = {rt: load_type_buffer(rt, fn) for rt, fn in TRAIN_FILES.items()}
    buf_dec = {rt: model.decision_function(buf) for rt, buf in buffers.items()}
    buf_scores = {rt: model.score_samples(buf) for rt, buf in buffers.items()}

    # E: 버퍼 min/max,  F: 버퍼 p1/p99
    scale_e = {rt: (float(d.min()), float(d.max())) for rt, d in buf_dec.items()}
    scale_f = {rt: (float(np.percentile(d, 1)), float(np.percentile(d, 99))) for rt, d in buf_dec.items()}
    thr_g = {rt: {p: float(np.percentile(s, p)) for p in (10.0, 20.0)} for rt, s in buf_scores.items()}

    windows = []
    for resource_type, filename in EVAL_TARGETS:
        with open(MOCK_DATA_DIR / filename, encoding="utf-8") as f:
            for w in json.load(f):
                windows.append((resource_type, filename, w))

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

    candidates = {
        "현재(창 min-max, k=3)": lambda rt, m: candidate_current(model, rt, m),
        "E: 버퍼 min/max 스케일": lambda rt, m: candidate_buffer_scaled(model, rt, m, scale_e[rt][0], scale_e[rt][1]),
        "F: 버퍼 p1/p99 스케일": lambda rt, m: candidate_buffer_scaled(model, rt, m, scale_f[rt][0], scale_f[rt][1]),
        "G: D(p=10)": lambda rt, m: candidate_d(model, rt, m, thr_g[rt][10.0]),
        "G: D(p=20)": lambda rt, m: candidate_d(model, rt, m, thr_g[rt][20.0]),
    }

    print(f"{'후보':<26} {'normal(+edge) 오탐':<20} {'anomaly 탐지':<16}")
    print("-" * 64)
    agg = {}
    for name, fn in candidates.items():
        fp_c, fp_t, tp_c, tp_t = aggregate(fn)
        agg[name] = (fp_c, fp_t, tp_c, tp_t)
        print(f"{name:<26} {fp_c}/{fp_t} ({fp_c/fp_t:.1%}){'':<6} {tp_c}/{tp_t} ({tp_c/tp_t:.1%})")

    # 현재를 이기는(오탐<=현재 AND 탐지>=현재) 후보만 시나리오별 상세 출력
    base_fp_rate = agg["현재(창 min-max, k=3)"][0] / agg["현재(창 min-max, k=3)"][1]
    base_tp_rate = agg["현재(창 min-max, k=3)"][2] / agg["현재(창 min-max, k=3)"][3]
    winners = [
        n for n, (fp_c, fp_t, tp_c, tp_t) in agg.items()
        if n != "현재(창 min-max, k=3)" and fp_c / fp_t <= base_fp_rate and tp_c / tp_t >= base_tp_rate
    ]
    print(f"\n현재 기준: 오탐 {base_fp_rate:.1%}, 탐지 {base_tp_rate:.1%}")
    print(f"현재를 (오탐<=, 탐지>=)로 이기는 후보: {winners if winners else '없음'}")

    for name in winners:
        print(f"\n=== {name} 시나리오별 ===")
        for resource_type, filename in EVAL_TARGETS:
            with open(MOCK_DATA_DIR / filename, encoding="utf-8") as f:
                data = json.load(f)
            cf = ct = df = dt = 0
            cfn = ctn = 0
            for w in data:
                cur = candidate_current(model, resource_type, w["raw_metrics"])
                new = candidates[name](resource_type, w["raw_metrics"])
                if w["label"] in ("normal", "edge_normal"):
                    cfn += 1
                    cf += int(cur)
                    df += int(new)
                elif w["label"] == "anomaly":
                    ctn += 1
                    ct += int(cur)
                    dt += int(new)
            print(f"  {resource_type}/{filename:<24} 현재 오탐={cf}/{cfn} 탐지={ct}/{ctn}  |  "
                  f"신규 오탐={df}/{cfn} 탐지={dt}/{ctn}")


if __name__ == "__main__":
    main()
