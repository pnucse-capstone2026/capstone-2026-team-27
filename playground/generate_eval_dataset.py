"""
playground/generate_eval_dataset.py

Phase 1: 라벨링된 합성 평가 데이터셋 생성.

목적: Isolation Forest / Z-score는 비지도 모델이라 스스로 "맞았는지 틀렸는지" 모른다.
      정확도(accuracy/precision/recall)를 계산하려면 정답(ground truth)이 필요한데,
      AWS 미연동이라 실제 이상 사례 데이터가 없으므로, 우리가 직접 정상/이상 패턴을
      설계해서 넣고 정답 라벨을 미리 붙인다.

생성 케이스 (리소스 타입 5개 × ...):
  - normal        : 전 지표 베이스라인 노이즈만 (anomaly=False)
  - spike_<metric>: 지표 하나만 마지막 시점에 스파이크 (anomaly=True, injected_metric=그 지표)
                     → Phase 4에서 "이 케이스에서 SHAP이 진짜 그 지표를 크게 잡는가"를
                        검증하는 데 쓰임 (설계상 정답이 이미 정해져 있음)

[실행 방법]
  프로젝트 루트에서: python playground/generate_eval_dataset.py

[출력]
  playground/eval_outputs/eval_dataset.json
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import numpy as np

OUTPUT_PATH = PROJECT_ROOT / "playground" / "eval_outputs" / "eval_dataset.json"

WINDOW_SIZE = 30
N_NORMAL_PER_TYPE = 30      # 리소스 타입당 정상 샘플 개수
N_SPIKE_PER_METRIC = 15     # 리소스 타입당, 지표 1개마다 스파이크 샘플 개수
SPIKE_MULTIPLIER = 10.0     # 베이스라인 대비 스파이크 배율
RANDOM_SEED = 42

# ── 리소스 타입별 베이스라인 (measure_iforest_latency.py와 동일한 값 사용) ──────
BASELINES: dict[str, dict[str, tuple[float, float]]] = {
    "EC2": {
        "cpu_utilization": (50.0, 2.0),
        "network_in":      (1000.0, 50.0),
        "network_out":     (800.0, 50.0),
        "cost":            (2.0, 0.1),
    },
    "Lambda": {
        "invocation_count": (100.0, 5.0),
        "error_count":      (1.0, 0.5),
        "duration_avg":     (200.0, 10.0),
        "cost":             (1.0, 0.05),
    },
    "S3": {
        "number_of_requests": (500.0, 20.0),
        "bytes_downloaded":   (1000.0, 50.0),
        "cost":               (0.5, 0.05),
    },
    "RDS": {
        "cpu_utilization":      (40.0, 2.0),
        "database_connections": (10.0, 1.0),
        "read_iops":            (100.0, 5.0),
        "write_iops":           (80.0, 5.0),
        "cost":                 (3.0, 0.1),
    },
    "AutoScaling": {
        "group_desired_capacity":     (3.0, 0.5),
        "group_in_service_instances": (3.0, 0.5),
        "cost":                       (1.0, 0.1),
    },
}


def _normal_window(rng: np.random.Generator, base: float, noise: float) -> list[float]:
    return (base + rng.uniform(-noise, noise, size=WINDOW_SIZE)).tolist()


def _spike_window(rng: np.random.Generator, base: float, noise: float) -> list[float]:
    window = base + rng.uniform(-noise, noise, size=WINDOW_SIZE)
    window[-1] = base * SPIKE_MULTIPLIER
    return window.tolist()


def _make_sample(
    resource_type: str,
    case: str,
    injected_metric: str | None,
    sample_idx: int,
) -> dict:
    seed = RANDOM_SEED + hash((resource_type, case, sample_idx)) % 1_000_000
    rng = np.random.default_rng(seed)

    metrics = {}
    for metric, (base, noise) in BASELINES[resource_type].items():
        if metric == injected_metric:
            metrics[metric] = _spike_window(rng, base, noise)
        else:
            metrics[metric] = _normal_window(rng, base, noise)

    return {
        "sample_id": f"{resource_type}_{case}_{sample_idx:03d}",
        "resource_type": resource_type,
        "case": case,
        "ground_truth_anomaly": injected_metric is not None,
        "injected_metric": injected_metric,
        "raw_metrics": metrics,
    }


def generate_dataset() -> list[dict]:
    dataset: list[dict] = []

    for resource_type, metric_baselines in BASELINES.items():
        # ── 정상 샘플 ──
        for i in range(N_NORMAL_PER_TYPE):
            dataset.append(_make_sample(resource_type, "normal", None, i))

        # ── 지표별 단일 스파이크 샘플 ──
        for metric in metric_baselines:
            for i in range(N_SPIKE_PER_METRIC):
                dataset.append(_make_sample(resource_type, f"spike_{metric}", metric, i))

    return dataset


def _print_summary(dataset: list[dict]) -> None:
    from collections import Counter

    by_type = Counter(s["resource_type"] for s in dataset)
    by_label = Counter(s["ground_truth_anomaly"] for s in dataset)
    by_case = Counter((s["resource_type"], s["case"]) for s in dataset)

    print("=" * 70)
    print(f"생성된 샘플 총 개수: {len(dataset)}")
    print("-" * 70)
    print("리소스 타입별:")
    for rt, cnt in by_type.items():
        print(f"  {rt:<14} {cnt}개")
    print("-" * 70)
    print(f"정상(anomaly=False): {by_label[False]}개 / 이상(anomaly=True): {by_label[True]}개")
    print("-" * 70)
    print("리소스×케이스별 상세:")
    for (rt, case), cnt in sorted(by_case.items()):
        print(f"  {rt:<14} {case:<32} {cnt}개")
    print("=" * 70)


def main() -> None:
    dataset = generate_dataset()
    _print_summary(dataset)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(dataset, f, ensure_ascii=False, indent=2)

    print(f"\n저장 완료: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
