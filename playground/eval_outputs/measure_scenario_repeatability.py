"""
playground/eval_outputs/measure_scenario_repeatability.py

test_scenarios.py의 3개 시나리오(좀비 EC2 / Lambda 호출 폭증 / EDoS 의심)를 각각
N회 반복 실행해서, 탐지 스코어(z-score/iforest)와 처리 시간(elapsed)의
평균/표준편차를 뽑는다. 포스터/보고서용 에러바 차트 데이터 소스.

⚠️ raw_metrics는 test_scenarios.py와 동일하게 여전히 "만든" 값이다 (실제
CloudWatch/Cost Explorer에서 가져온 값이 아니다 — resource_id만 .env의 실제
AWS 리소스 ID를 쓰고, 지표 자체는 시나리오 취지에 맞춰 사람이 설계한 고정
패턴이다). 원본과의 차이는, 그 고정 패턴 위에 매 반복(rep)마다 독립적인
가우시안 노이즈(승수, 평균 1.0)를 곱해서 매번 조금씩 다른 입력을 만든다는
점이다 — 그래야 anomaly_score_zscore/iforest, elapsed에 실제로 반복실험다운
변동(표준편차)이 생긴다. 노이즈 전 버전(고정 상수 그대로 5회 반복)은 스코어
표준편차가 항상 0이라 에러바 자체가 의미가 없었다.

[실행 방법]
  프로젝트 루트에서: python playground/eval_outputs/measure_scenario_repeatability.py
"""

from __future__ import annotations

import contextlib
import io
import json
import random
import statistics
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

import os

from playground.test_scenarios import run_scenario

N_REPEATS = 5
NOISE_STD = 0.05  # 승수 노이즈 표준편차 (평균값의 ±5% 수준)
RANDOM_SEED = 42

OUTPUT_PATH = Path(__file__).parent / "scenario_repeatability_result.json"


def _noisy(values: list[float], rng: random.Random, std: float = NOISE_STD) -> list[float]:
    """각 값에 독립적인 곱셈 가우시안 노이즈(평균 1.0)를 적용, 0 미만은 0으로 클램프."""
    return [max(0.0, v * rng.gauss(1.0, std)) for v in values]


def _make_zombie_metrics(rng: random.Random) -> dict:
    base = {
        "cpu_utilization": [3.0] * 30,
        "network_in":      [100.0] * 30,
        "network_out":     [80.0] * 30,
        "cost":            [0.5] * 27 + [6.0, 6.2, 6.4],
    }
    return {k: _noisy(v, rng) for k, v in base.items()}


def _make_lambda_metrics(rng: random.Random) -> dict:
    base = {
        "invocation_count": [100.0] * 25 + [5000.0] * 5,
        "error_count":      [1.0] * 30,
        "duration_avg":     [200.0] * 30,
        "cost":             [0.1] * 25 + [2.0] * 5,
    }
    return {k: _noisy(v, rng) for k, v in base.items()}


def _make_edos_metrics(rng: random.Random) -> dict:
    base = {
        "group_in_service_instances": [2.0] * 27 + [20.0] * 3,
        "group_desired_capacity":     [2.0] * 27 + [20.0] * 3,
        "cost":                       [0.5] * 27 + [8.0, 8.5, 9.0],
    }
    return {k: _noisy(v, rng) for k, v in base.items()}


SCENARIOS = [
    (
        "1_zombie_ec2",
        "EC2",
        lambda: os.getenv("INSTANCE_ID", "i-DUMMY_INSTANCE_ID"),
        _make_zombie_metrics,
        "cost_inefficiency",
    ),
    (
        "2_lambda_spike",
        "Lambda",
        lambda: os.getenv("LAMBDA_FUNCTION_NAME", "detection-test-lambda"),
        _make_lambda_metrics,
        "cost_spike",
    ),
    (
        "3_edos_suspicion",
        "AutoScaling",
        lambda: os.getenv("ASG_NAME", "detection-test-asg"),
        _make_edos_metrics,
        "risk_security",
    ),
]


def _mean_std(values: list[float]) -> tuple[float, float]:
    if not values:
        return 0.0, 0.0
    mean = statistics.mean(values)
    std = statistics.stdev(values) if len(values) > 1 else 0.0
    return mean, std


def run_repeats(
    name: str, resource_type: str, resource_id_fn, metrics_fn, expected_anomaly_type: str
) -> dict:
    print(f"\n[{name}] {N_REPEATS}회 반복 실행 중 (노이즈 ±{NOISE_STD*100:.0f}%)...")
    rng = random.Random(f"{RANDOM_SEED}-{name}")
    runs = []
    for i in range(N_REPEATS):
        raw_metrics = metrics_fn(rng)
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            result = run_scenario(
                name=name,
                description="(노이즈 반복실험용 실행 — 상세 로그는 표준출력 억제됨)",
                resource_id=resource_id_fn(),
                resource_type=resource_type,
                raw_metrics=raw_metrics,
                expected_anomaly_type=expected_anomaly_type,
            )
        state = result["state"]
        runs.append(
            {
                "rep": i + 1,
                "elapsed_sec": result["elapsed"],
                "anomaly_score_zscore": state.get("anomaly_score_zscore"),
                "anomaly_score_iforest": state.get("anomaly_score_iforest"),
                "anomaly_type": state.get("anomaly_type"),
                "selected_action": state.get("selected_action"),
                "risk_level": state.get("risk_level"),
                "requires_approval": state.get("requires_approval"),
                "action_result": state.get("action_result"),
                "failures": result["failures"],
            }
        )
        print(f"  rep {i+1}/{N_REPEATS}: elapsed={result['elapsed']:.3f}s, "
              f"failures={len(result['failures'])}")

    elapsed_values = [r["elapsed_sec"] for r in runs]
    zscore_values = [r["anomaly_score_zscore"] for r in runs if r["anomaly_score_zscore"] is not None]
    iforest_values = [r["anomaly_score_iforest"] for r in runs if r["anomaly_score_iforest"] is not None]

    elapsed_mean, elapsed_std = _mean_std(elapsed_values)
    zscore_mean, zscore_std = _mean_std(zscore_values)
    iforest_mean, iforest_std = _mean_std(iforest_values)

    total_failures = sum(len(r["failures"]) for r in runs)
    action_counts: dict[str, int] = {}
    for r in runs:
        action_counts[r["selected_action"]] = action_counts.get(r["selected_action"], 0) + 1

    summary = {
        "scenario": name,
        "n_repeats": N_REPEATS,
        "noise_std": NOISE_STD,
        "elapsed_sec": {"mean": elapsed_mean, "std": elapsed_std, "values": elapsed_values},
        "anomaly_score_zscore": {"mean": zscore_mean, "std": zscore_std, "values": zscore_values},
        "anomaly_score_iforest": {"mean": iforest_mean, "std": iforest_std, "values": iforest_values},
        "selected_action_distribution": action_counts,
        "selected_action": runs[0]["selected_action"],
        "risk_level": runs[0]["risk_level"],
        "requires_approval": runs[0]["requires_approval"],
        "total_failures": total_failures,
        "runs": runs,
    }

    print(f"  -> elapsed: {elapsed_mean:.3f}s ± {elapsed_std:.3f}s")
    print(f"  -> zscore : {zscore_mean:.3f} ± {zscore_std:.3f}")
    print(f"  -> iforest: {iforest_mean:.3f} ± {iforest_std:.3f}")
    print(f"  -> selected_action distribution={action_counts}, "
          f"failures={total_failures}/{N_REPEATS}")

    return summary


def main() -> None:
    all_summaries = []
    for name, resource_type, resource_id_fn, metrics_fn, expected_anomaly_type in SCENARIOS:
        all_summaries.append(
            run_repeats(name, resource_type, resource_id_fn, metrics_fn, expected_anomaly_type)
        )

    OUTPUT_PATH.write_text(
        json.dumps(all_summaries, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"\n결과 저장: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
