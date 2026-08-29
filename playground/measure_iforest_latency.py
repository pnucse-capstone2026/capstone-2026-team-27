"""
playground/measure_iforest_latency.py

Isolation Forest 추론 시간 측정 스크립트.
- 리소스 타입별(EC2/Lambda/S3/RDS/AutoScaling)로 N번 반복 측정 후 평균/최소/최대 출력
- 권장 기준: 1초 미만 / 절대 기준: 10초 미만
- 결과를 보고서에 첨부할 수 있도록 깔끔하게 출력

[실행 방법]
  프로젝트 루트에서:
    python playground/measure_iforest_latency.py

[주의]
  첫 번째 실행은 모델 학습(fit)까지 포함되므로 이후 실행보다 느릴 수 있음.
  그래서 워밍업 N회를 먼저 돌리고 결과에서 제외한 뒤 본 측정을 시작함.
"""

from __future__ import annotations

import os
import sys
import random
import time
import tempfile
from pathlib import Path
from statistics import mean, stdev

# ── sys.path 설정 ──────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

import numpy as np
import pipeline.detection_agent as da

# ── 측정 설정 ──────────────────────────────────────────────────────────────────
N_REPEAT       = 30    # 반복 횟수 (평균 내기용)
WARMUP_COUNT   = 3     # 워밍업 횟수 (결과에서 제외)
WINDOW_SIZE    = 30    # 슬라이딩 윈도우 크기
PASS_THRESHOLD = 1.0   # 권장 기준 (초)
FAIL_THRESHOLD = 10.0  # 절대 기준 (초)
RANDOM_SEED    = 42

random.seed(RANDOM_SEED)


# ── 리소스 타입별 더미 메트릭 ─────────────────────────────────────────────────

def _make_metrics(resource_type: str) -> dict[str, list[float]]:
    def norm(base, noise):
        return [base + random.uniform(-noise, noise) for _ in range(WINDOW_SIZE)]

    if resource_type == "EC2":
        return {
            "cpu_utilization": norm(50, 2),
            "network_in":      norm(1000, 50),
            "network_out":     norm(800, 50),
            "cost":            norm(2.0, 0.1),
        }
    elif resource_type == "Lambda":
        return {
            "invocation_count": norm(100, 5),
            "error_count":      norm(1, 0.5),
            "duration_avg":     norm(200, 10),
            "cost":             norm(1.0, 0.05),
        }
    elif resource_type == "S3":
        return {
            "number_of_requests": norm(500, 20),
            "bytes_downloaded":   norm(1000, 50),
            "cost":               norm(0.5, 0.05),
        }
    elif resource_type == "RDS":
        return {
            "cpu_utilization":      norm(40, 2),
            "database_connections": norm(10, 1),
            "read_iops":            norm(100, 5),
            "write_iops":           norm(80, 5),
            "cost":                 norm(3.0, 0.1),
        }
    elif resource_type == "AutoScaling":
        return {
            "group_desired_capacity":     norm(3, 0.5),
            "group_in_service_instances": norm(3, 0.5),
            "cost":                       norm(1.0, 0.1),
        }
    else:
        raise ValueError(f"알 수 없는 resource_type: {resource_type}")


# ── 추론 시간 측정 ─────────────────────────────────────────────────────────────

def _measure_iforest_inference(resource_type: str, metrics: dict, model_dir: str) -> float:
    """캐시된 모델로 decision_function만 실행하는 순수 추론 시간 측정.

    ⚠️ 리소스 타입별 개별 파일(iforest_{resource_type}.pkl)로 캐싱하던 예전 구조 기준
    코드였음 — 지금은 IFOREST_UNIFIED_MODEL_NAME("unified") 하나로 전 타입을 같이
    캐싱하므로 그에 맞게 로드/피처 구성 방식을 수정."""
    da.IFOREST_MODEL_DIR = model_dir

    # 모델이 없으면 먼저 학습해서 캐싱
    cached = da._load_cached_model(da.IFOREST_UNIFIED_MODEL_NAME)
    if cached is None:
        da._get_or_train_iforest(resource_type, metrics)

    model, feature_keys = da._load_cached_model(da.IFOREST_UNIFIED_MODEL_NAME)
    X = da.build_unified_feature_matrix(resource_type, metrics)

    # 순수 추론 시간만 측정
    start = time.perf_counter()
    raw_scores = model.decision_function(X)
    latest_raw = raw_scores[-1]
    s_min, s_max = raw_scores.min(), raw_scores.max()
    if s_max != s_min:
        _ = (s_max - latest_raw) / (s_max - s_min)
    elapsed = time.perf_counter() - start

    return elapsed


def _measure_detection_node(resource_type: str, metrics: dict, model_dir: str) -> float:
    """detection_node 전체(Z-score + IF + OR 앙상블) 실행 시간 측정."""
    da.IFOREST_MODEL_DIR = model_dir

    state = {
        "resource_id":           f"test-{resource_type.lower()}-001",
        "resource_type":         resource_type,
        "raw_metrics":           metrics,
        "timestamp":             "2026-07-14T10:00:00Z",
        "anomaly_flag":          False,
        "anomaly_score_zscore":  None,
        "anomaly_score_iforest": None,
        "triggered_metrics":     [],
        "anomaly_type":             None,
        "classification_reasoning": None,
        "interim_action_taken":     None,
        "candidate_actions":  [],
        "selected_action":    None,
        "risk_level":         None,
        "requires_approval":  False,
        "decision_reasoning": None,
        "pre_action_snapshot": None,
        "action_executed":     None,
        "action_result":       None,
        "qa_passed":        None,
        "sla_check_result": None,
        "rollback_count":   0,
        "log_entries":      [],
    }

    start = time.perf_counter()
    da.detection_node(state)
    elapsed = time.perf_counter() - start

    return elapsed


def _verdict(avg_sec: float) -> str:
    if avg_sec < PASS_THRESHOLD:
        return "✅ PASS (1초 미만)"
    elif avg_sec < FAIL_THRESHOLD:
        return "⚠️  WARN (1초 이상 10초 미만)"
    else:
        return "❌ FAIL (10초 이상)"


def _print_result(label: str, times: list[float]) -> str:
    avg = mean(times)
    mn  = min(times)
    mx  = max(times)
    sd  = stdev(times) if len(times) > 1 else 0.0
    v   = _verdict(avg)

    print(
        f"  {label:<40} "
        f"평균={avg*1000:7.2f}ms  "
        f"최소={mn*1000:7.2f}ms  "
        f"최대={mx*1000:7.2f}ms  "
        f"표준편차={sd*1000:6.2f}ms  "
        f"{v}"
    )
    return v


# ── 메인 ──────────────────────────────────────────────────────────────────────

def main() -> None:
    resource_types = ["EC2", "Lambda", "S3", "RDS", "AutoScaling"]

    print("=" * 100)
    print("Isolation Forest 추론 시간 측정")
    print(f"  반복 횟수  : {N_REPEAT}회 (워밍업 {WARMUP_COUNT}회 제외)")
    print(f"  윈도우 크기: {WINDOW_SIZE}개 포인트")
    print(f"  권장 기준  : {PASS_THRESHOLD}초 미만")
    print(f"  절대 기준  : {FAIL_THRESHOLD}초 미만")
    print("=" * 100)

    with tempfile.TemporaryDirectory() as model_dir:

        # ── [1] IF 순수 추론 시간 ────────────────────────────────────────────
        print("\n[1] IF 순수 추론 시간 (캐시된 모델로 decision_function만 측정)")
        print("-" * 100)

        inference_results = {}
        for rt in resource_types:
            metrics = _make_metrics(rt)

            for _ in range(WARMUP_COUNT):
                _measure_iforest_inference(rt, metrics, model_dir)

            times = [
                _measure_iforest_inference(rt, metrics, model_dir)
                for _ in range(N_REPEAT)
            ]
            verdict = _print_result(f"IF 추론 ({rt})", times)
            inference_results[rt] = {"times": times, "verdict": verdict}

        # ── [2] detection_node 전체 실행 시간 ────────────────────────────────
        print("\n[2] detection_node 전체 실행 시간 (Z-score + IF + OR 앙상블)")
        print("-" * 100)

        node_results = {}
        for rt in resource_types:
            metrics = _make_metrics(rt)

            for _ in range(WARMUP_COUNT):
                _measure_detection_node(rt, metrics, model_dir)

            times = [
                _measure_detection_node(rt, metrics, model_dir)
                for _ in range(N_REPEAT)
            ]
            verdict = _print_result(f"detection_node ({rt})", times)
            node_results[rt] = {"times": times, "verdict": verdict}

    # ── 전체 요약 ──────────────────────────────────────────────────────────────
    print("\n" + "=" * 100)
    print("전체 요약")
    print("=" * 100)
    print(f"  {'리소스':<14} {'IF 추론 평균':>16} {'IF 판정':<24} {'전체 평균':>14} {'전체 판정'}")
    print("-" * 100)

    all_pass = True
    for rt in resource_types:
        inf_avg  = mean(inference_results[rt]["times"]) * 1000
        node_avg = mean(node_results[rt]["times"]) * 1000
        inf_v    = inference_results[rt]["verdict"]
        node_v   = node_results[rt]["verdict"]
        print(
            f"  {rt:<14} "
            f"{inf_avg:>12.2f}ms  "
            f"{inf_v:<24}  "
            f"{node_avg:>10.2f}ms  "
            f"{node_v}"
        )
        if "FAIL" in inf_v or "FAIL" in node_v:
            all_pass = False

    print()
    if all_pass:
        print("  최종 판정: ✅ 전 리소스 타입 기준 통과")
    else:
        print("  최종 판정: ❌ 일부 리소스 타입 기준 초과 — 최적화 필요")
    print("=" * 100)


if __name__ == "__main__":
    main()