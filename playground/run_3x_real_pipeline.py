"""
playground/run_3x_real_pipeline.py

"비용절감액"과 "파이프라인 실행시간"을 정확도(n=13, CI)와 같은 수준의 통계적
근거로 재기 위해, 버킷 3개에 각각 독립적으로 짧은(15분) 폭증을 재현한 뒤 곧바로
전체 파이프라인(Detection~Logging, 실제 액션+QA 5분 대기 포함)을 병렬로 3회 실행한다.

⚠️ 여기서 나오는 "절감액"은 Decision이 그 순간 계산한 예측치(estimated_saving_usd)다.
실제로 검증된(재측정된) 절감액이 아니다 — 그건 이 스크립트 실행 후 충분한 시간(60분+)이
지난 뒤 verify_cost_predictions.py를 따로 돌려야 나온다. 두 숫자를 혼동하지 않도록
결과에 "predicted_saving_usd_mean_sd"로 명확히 표시한다.

[실행 방법]
  python playground/run_3x_real_pipeline.py
"""

from __future__ import annotations

import json
import os
import random
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT.parent))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT.parent / ".env")

import boto3

from measure_pipeline_timing import measure

AWS_REGION = os.getenv("AWS_DEFAULT_REGION")
BUCKETS = ["detection-trial-anomaly-0", "detection-trial-anomaly-1", "detection-trial-anomaly-3"]

COST_PREDICTION_LOG_PATH = PROJECT_ROOT.parent / "schema" / "logs" / "cost_prediction_log.jsonl"


def induce_short_spike(bucket: str, spike_periods: int = 3, period_seconds: int = 300) -> None:
    s3 = boto3.client("s3", region_name=AWS_REGION)
    key = "fresh_spike_for_3x_run.bin"
    s3.put_object(Bucket=bucket, Key=key, Body=os.urandom(50_000))

    spike_level = 500 * 5
    for period in range(spike_periods):
        t0 = time.time()
        n = round(spike_level * random.uniform(0.98, 1.02))
        for _ in range(n):
            s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        elapsed = time.time() - t0
        remaining = period_seconds - elapsed
        print(f"  [{bucket}] 구간 {period+1}/{spike_periods}: {n}건 완료 ({elapsed:.1f}s), "
              f"{max(0, remaining):.1f}s 대기")
        if remaining > 0:
            time.sleep(remaining)


def run_one(bucket: str) -> dict:
    t0 = time.time()
    print(f"[{bucket}] 폭증 재현 시작...")
    induce_short_spike(bucket)
    print(f"[{bucket}] 폭증 재현 완료, 파이프라인 실행...")
    result = measure(bucket, "S3", bypass_approval_for_timing=True)
    result["induce_and_run_elapsed_sec"] = time.time() - t0
    print(f"[{bucket}] 완료: {result.get('timings', {}).get('total')}s, "
          f"selected_action={result.get('selected_action')}")
    return result


def _load_recent_cost_predictions(resource_ids: list[str], since: datetime) -> list[dict]:
    if not COST_PREDICTION_LOG_PATH.exists():
        return []
    out = []
    with open(COST_PREDICTION_LOG_PATH, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            e = json.loads(line)
            if e.get("resource_id") not in resource_ids:
                continue
            decided_at = datetime.fromisoformat(e["decided_at"].replace("Z", "+00:00"))
            if decided_at >= since:
                out.append(e)
    return out


def _mean_sd(values: list[float]) -> tuple[float, float]:
    if not values:
        return 0.0, 0.0
    mean = statistics.mean(values)
    sd = statistics.stdev(values) if len(values) > 1 else 0.0
    return mean, sd


def main() -> None:
    start_time = datetime.now(timezone.utc)
    print(f"=== 버킷 {len(BUCKETS)}개 병렬 실행 시작 ===")

    results = []
    with ThreadPoolExecutor(max_workers=len(BUCKETS)) as executor:
        futures = {executor.submit(run_one, b): b for b in BUCKETS}
        for future in as_completed(futures):
            results.append(future.result())

    timings_total = [r["timings"]["total"] for r in results if "total" in r.get("timings", {})]
    timing_mean, timing_sd = _mean_sd(timings_total)

    cost_entries = _load_recent_cost_predictions(BUCKETS, start_time)
    predicted_savings = [e["estimated_saving_usd"] for e in cost_entries]
    saving_mean, saving_sd = _mean_sd(predicted_savings)

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "n": len(results),
        "pipeline_timing_sec": {"values": timings_total, "mean": timing_mean, "sd": timing_sd},
        "predicted_saving_usd": {"values": predicted_savings, "mean": saving_mean, "sd": saving_sd,
                                  "note": "예측치(decision 시점) - 실측 검증은 별도로 verify_cost_predictions.py 필요"},
        "raw_results": results,
        "cost_prediction_entries": cost_entries,
    }

    out_path = PROJECT_ROOT / "eval_outputs" / f"3x_real_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("\n=== 결과 ===")
    print(f"파이프라인 총 실행시간: {timing_mean:.1f}s ± {timing_sd:.1f}s (n={len(timings_total)})")
    print(f"예측 절감액: ${saving_mean:.4f} ± ${saving_sd:.4f} (n={len(predicted_savings)})")
    print(f"저장: {out_path}")


if __name__ == "__main__":
    main()
