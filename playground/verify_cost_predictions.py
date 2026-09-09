"""
playground/verify_cost_predictions.py

decision_agent.py가 결정 시점마다 남기는 schema/logs/cost_prediction_log.jsonl을 읽어,
"예측한 절감액이 실제로 얼마나 맞았는지"를 사후에 실측 검증한다.

이게 왜 QA_agent 안이 아니라 별도 스크립트인지 (세션에서 논의됨):
  QA는 액션 직후 즉시 실행되는데, CloudWatch는 몇 분(이 프로젝트는 5분 단위 구간)
  지나야 그 변화를 반영한다. QA의 cost_ok 체크는 액션 전에 이미 갖고 있던
  raw_metrics를 재활용할 뿐 새로 조회하지 않아서, "액션 후 실제 효과"를 볼 수 있는
  시점이 아니다. 반면 이 스크립트는 충분한 시간이 지난 뒤(--min-age-minutes) 실행되는
  걸 전제로 하므로, 그 시점의 실측 데이터를 봐도 된다.

측정하는 것 2가지:
  1) 예측 vs 실측 절감액 오차 (포스터용 "우리 예측이 얼마나 정확한가" 지표)
  2) QA 정확도 근사치: "이 실측 결과를 봤다면 QA가 통과라고 했어야 하는가"를
     cost_ok 기준(recent <= baseline*1.1)으로 역산해서, 실제 QA가 그 시점에
     qa_passed로 남긴 값(schema/logs/llm_decision_log.jsonl의 qa_result, LLM 판단
     건에 한해서만 존재)과 대조한다. Rule Book으로 처리된 건은 QA_agent가
     qa_result를 안 채워서(팀원 코드의 기존 동작) 대조 불가 — 이 경우는 결과에
     "qa_result_available": false로 표시하고 정확도 집계에서 제외한다.

[실행 방법]
  python playground/verify_cost_predictions.py --min-age-minutes 60

[생성 파일]
  playground/eval_outputs/cost_prediction_verification_{날짜}.json
  (검증 완료된 trace_id는 cost_prediction_log.jsonl에 verified=true로 표시되어
   재검증 시 중복 처리되지 않는다)
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

import math
import os

import boto3

from pipeline.orchestrator import assemble_resource
from pipeline.cost_estimator import estimate_cost_series
from pipeline.cloudwatch_client import METRIC_SPEC, _build_dimensions

AWS_REGION = os.getenv("AWS_DEFAULT_REGION")

COST_PREDICTION_LOG_PATH = PROJECT_ROOT / "schema" / "logs" / "cost_prediction_log.jsonl"
LLM_DECISION_LOG_PATH = PROJECT_ROOT / "schema" / "logs" / "llm_decision_log.jsonl"
RESULT_DIR = PROJECT_ROOT / "playground" / "eval_outputs"

COST_INCREASE_THRESHOLD = 1.1  # QA_agent._check_cost_sla와 동일한 기준(10% 증가 허용)


def _load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    entries = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return entries


def _load_qa_results_by_trace_id() -> dict[str, dict]:
    """llm_decision_log.jsonl에서 trace_id -> qa_result 매핑. LLM 판단 건에만 존재."""
    out = {}
    for entry in _load_jsonl(LLM_DECISION_LOG_PATH):
        trace_id = entry.get("trace_id")
        qa_result = entry.get("qa_result")
        if trace_id and qa_result is not None:
            out[trace_id] = qa_result
    return out


def _real_current_cost(resource_type: str, resource_id: str) -> float | None:
    """assemble_resource()로 실제 지금 시점 cost를 재조회 (최근 3개 포인트 평균 —
    QA_agent._check_cost_sla의 "최근값"보다는 노이즈에 덜 민감하게 소폭 평균)."""
    try:
        assembled = assemble_resource(resource_id, resource_type)
        cost_values = assembled["raw_metrics"].get("cost", [])
        if not cost_values:
            return None
        recent = cost_values[-3:] if len(cost_values) >= 3 else cost_values
        return sum(recent) / len(recent)
    except Exception as exc:
        print(f"  [경고] {resource_type}:{resource_id} 실측 실패: {exc}")
        return None


def _fetch_usage_window(
    resource_type: str, resource_id: str, start_time: datetime, end_time: datetime,
    period_seconds: int = 300,
) -> tuple[dict[str, list[float]], int]:
    """cloudwatch_client.fetch_metrics는 "지금부터 n_points개 이전"만 가능해서
    (end_time 고정 불가), 임의 구간(start_time~end_time)을 조회하려면 별도 구현이
    필요하다. 로직 자체는 fetch_metrics와 동일 — "활동 없음=0.0으로 채움"까지 그대로."""
    n_points = max(1, math.ceil((end_time - start_time).total_seconds() / period_seconds))

    cw = boto3.client("cloudwatch", region_name=AWS_REGION)
    metric_keys = list(METRIC_SPEC[resource_type].keys())
    dimensions = _build_dimensions(resource_type, resource_id)

    queries = []
    for i, metric_key in enumerate(metric_keys):
        namespace, cw_metric_name, stat = METRIC_SPEC[resource_type][metric_key]
        queries.append({
            "Id": f"m{i}",
            "MetricStat": {
                "Metric": {"Namespace": namespace, "MetricName": cw_metric_name, "Dimensions": dimensions},
                "Period": period_seconds,
                "Stat": stat,
            },
            "ReturnData": True,
        })

    response = cw.get_metric_data(
        MetricDataQueries=queries, StartTime=start_time, EndTime=end_time,
        ScanBy="TimestampAscending",
    )
    results_by_id = {r["Id"]: r for r in response["MetricDataResults"]}

    expected_times = [start_time + timedelta(seconds=i * period_seconds) for i in range(n_points)]
    half_period = period_seconds / 2

    usage: dict[str, list[float]] = {}
    for i, metric_key in enumerate(metric_keys):
        row = results_by_id.get(f"m{i}")
        if row is None or not row.get("Timestamps"):
            usage[metric_key] = [0.0] * n_points
            continue
        observed = list(zip(row["Timestamps"], row["Values"]))
        filled = []
        for expected_ts in expected_times:
            match = next((v for ts, v in observed if abs((ts - expected_ts).total_seconds()) < half_period), 0.0)
            filled.append(match)
        usage[metric_key] = filled

    return usage, n_points


def compute_period_totals(
    resource_type: str, resource_id: str, action_start: datetime, window_end: datetime,
    baseline_per_period_cost: float, period_seconds: int = 300,
) -> dict | None:
    """action_start(액션 결정 시점) ~ window_end(다음 액션 시점 또는 지금) 구간 전체의
    실제 발생 비용 총합과, "이 액션을 안 쓰고 baseline_per_period_cost가 그대로 이어
    졌다면"이라는 가정(반사실) 하의 총비용을 비교한다.

    ⚠️ 안 썼을 때 비용은 절대 실측이 아니라 추측이다 — baseline(액션 직전 평균)이
    구간 내내 변하지 않았다고 가정한 값일 뿐이다. 결과 필드명에도 이 점을 명시한다.
    """
    try:
        usage, n_periods = _fetch_usage_window(resource_type, resource_id, action_start, window_end, period_seconds)
        cost_series = estimate_cost_series(
            resource_type, resource_id, usage, period_seconds=period_seconds, end_time=window_end,
        )
    except Exception as exc:
        print(f"  [경고] {resource_type}:{resource_id} 기간 전체 실측 실패: {exc}")
        return None

    actual_total_usd = sum(cost_series)
    counterfactual_total_usd = baseline_per_period_cost * n_periods  # 추측치 (반사실)
    period_saving_usd = counterfactual_total_usd - actual_total_usd
    duration_hours = n_periods * period_seconds / 3600

    return {
        "window_start": action_start.isoformat(),
        "window_end": window_end.isoformat(),
        "duration_hours": round(duration_hours, 2),
        "n_periods": n_periods,
        "actual_total_cost_usd": round(actual_total_usd, 6),
        "counterfactual_total_cost_usd_ASSUMED": round(counterfactual_total_usd, 6),
        "counterfactual_note": (
            "실측 아님 — 액션 직전 평균 비용이 이 구간 내내 변하지 않았다고 "
            "가정한 추정치(반사실)"
        ),
        "period_saving_usd": round(period_saving_usd, 6),
    }


def _find_next_action_time(entries: list[dict], resource_id: str, after: datetime) -> datetime | None:
    """같은 리소스에 대해 이 결정 이후에 기록된 다음 결정의 시각. 없으면 None(=지금까지)."""
    candidates = [
        datetime.fromisoformat(e["decided_at"].replace("Z", "+00:00"))
        for e in entries
        if e.get("resource_id") == resource_id
        and datetime.fromisoformat(e["decided_at"].replace("Z", "+00:00")) > after
    ]
    return min(candidates) if candidates else None


def verify_one(entry: dict, all_entries: list[dict]) -> dict:
    resource_type = entry["resource_type"]
    resource_id = entry["resource_id"]
    predicted_after = entry["predicted_after_cost_usd"]
    current_before = entry["current_cost_usd"]
    estimated_saving = entry["estimated_saving_usd"]

    actual_now_cost = _real_current_cost(resource_type, resource_id)
    if actual_now_cost is None:
        return {**entry, "verified": False, "verify_error": "실측 실패 (리소스 삭제됨/권한 없음 등)"}

    actual_saving_usd = current_before - actual_now_cost
    prediction_error_usd = actual_saving_usd - estimated_saving
    prediction_error_ratio = (
        prediction_error_usd / estimated_saving if estimated_saving > 0 else None
    )

    # QA 정확도 근사치 계산 (cost_ok 기준 역산)
    would_pass_cost_sla = actual_now_cost <= current_before * COST_INCREASE_THRESHOLD
    qa_result = _QA_RESULTS_CACHE.get(entry.get("trace_id"))
    qa_result_available = qa_result is not None
    qa_matched_prediction = None
    if qa_result_available:
        actual_qa_passed = qa_result.get("qa_passed")
        qa_matched_prediction = (actual_qa_passed == would_pass_cost_sla)

    # 기간 전체 적분: 액션 시점 ~ (같은 리소스의 다음 결정 시점 또는 지금)까지
    # 실제 누적 비용 vs "안 썼으면 baseline이 그대로 이어졌을 것"이라는 가정치 비교.
    decided_at = datetime.fromisoformat(entry["decided_at"].replace("Z", "+00:00"))
    window_end = _find_next_action_time(all_entries, resource_id, decided_at) or datetime.now(timezone.utc)
    period_totals = compute_period_totals(
        resource_type, resource_id, decided_at, window_end, current_before,
    )

    return {
        **entry,
        "verified": True,
        "verified_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "actual_now_cost_usd": round(actual_now_cost, 6),
        "actual_saving_usd": round(actual_saving_usd, 6),
        "prediction_error_usd": round(prediction_error_usd, 6),
        "prediction_error_ratio": (
            round(prediction_error_ratio, 4) if prediction_error_ratio is not None else None
        ),
        "would_pass_cost_sla": would_pass_cost_sla,
        "qa_result_available": qa_result_available,
        "qa_actual_passed": qa_result.get("qa_passed") if qa_result_available else None,
        "qa_matched_real_outcome": qa_matched_prediction,
        "period_totals": period_totals,
    }


_QA_RESULTS_CACHE: dict[str, dict] = {}


def main() -> None:
    global _QA_RESULTS_CACHE

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--min-age-minutes", type=int, default=60,
                         help="결정 후 최소 이만큼 지난 항목만 검증 (기본 60분 — "
                              "CloudWatch 반영 지연 + 액션 완료 시간 감안)")
    args = parser.parse_args()

    entries = _load_jsonl(COST_PREDICTION_LOG_PATH)
    if not entries:
        print(f"{COST_PREDICTION_LOG_PATH}에 항목이 없음 — decision_node가 아직 "
              f"NoAction 아닌 결정을 안 남겼거나 로그 파일이 없음.")
        return

    _QA_RESULTS_CACHE = _load_qa_results_by_trace_id()

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=args.min_age_minutes)

    to_verify = []
    for e in entries:
        if e.get("verified"):
            continue  # 이미 검증됨
        decided_at = datetime.fromisoformat(e["decided_at"].replace("Z", "+00:00"))
        if decided_at > cutoff:
            continue  # 아직 min_age_minutes 안 지남
        to_verify.append(e)

    print(f"전체 {len(entries)}건 중 검증 대상 {len(to_verify)}건 "
          f"({args.min_age_minutes}분 이상 경과 + 미검증)")

    if not to_verify:
        return

    verified_results = []
    for e in to_verify:
        print(f"검증 중: {e['resource_type']}:{e['resource_id']} ({e['selected_action']}, "
              f"trace_id={e.get('trace_id')})")
        verified_results.append(verify_one(e, entries))

    # cost_prediction_log.jsonl 갱신 (verified 마커 추가, 나머지는 그대로 보존)
    by_trace_id = {r["trace_id"]: r for r in verified_results if r.get("trace_id")}
    updated_lines = []
    for e in entries:
        tid = e.get("trace_id")
        if tid in by_trace_id:
            updated_lines.append(json.dumps(by_trace_id[tid], ensure_ascii=False))
        else:
            updated_lines.append(json.dumps(e, ensure_ascii=False))
    with open(COST_PREDICTION_LOG_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(updated_lines) + "\n")

    # 요약 통계
    successful = [r for r in verified_results if r.get("verified") and "actual_saving_usd" in r]
    qa_comparable = [r for r in successful if r.get("qa_result_available")]
    qa_correct = [r for r in qa_comparable if r.get("qa_matched_real_outcome")]

    summary = {
        "generated_at": now.isoformat(),
        "min_age_minutes": args.min_age_minutes,
        "n_verified": len(successful),
        "n_failed": len(verified_results) - len(successful),
        "mean_prediction_error_usd": (
            round(sum(r["prediction_error_usd"] for r in successful) / len(successful), 6)
            if successful else None
        ),
        "qa_accuracy_n_comparable": len(qa_comparable),
        "qa_accuracy": (
            round(len(qa_correct) / len(qa_comparable), 4) if qa_comparable else None
        ),
        "qa_accuracy_note": (
            "Rule Book으로 처리된 결정은 QA_agent가 qa_result를 안 남겨서 "
            "비교 불가 — n_comparable은 LLM 판단 건만 포함"
        ),
        "results": verified_results,
    }

    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = RESULT_DIR / f"cost_prediction_verification_{now.strftime('%Y%m%d')}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f"\n=== 결과 ===")
    print(f"검증 성공: {summary['n_verified']}건, 실패: {summary['n_failed']}건")
    print(f"평균 예측 오차: {summary['mean_prediction_error_usd']} USD")
    print(f"QA 정확도: {summary['qa_accuracy']} (비교 가능 {summary['qa_accuracy_n_comparable']}건)")
    print(f"저장: {out_path}")


if __name__ == "__main__":
    main()
