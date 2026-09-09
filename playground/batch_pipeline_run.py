"""
playground/batch_pipeline_run.py

리소스 여러 개에 대해 **전체 파이프라인**(detection -> classification -> decision ->
action -> qa -> logging)을 돌리고, 단계별 소요시간 / 액션 실행 결과 / QA 통과 여부를
한꺼번에 집계한다.

measure_pipeline_timing.py는 리소스 1개 전용이라, 13개 시행을 재려면 13번 따로
실행해야 하고 결과도 합쳐지지 않는다. 이 스크립트는 그 measure()를 그대로 재사용해서
(판정 로직 중복 구현 없이) 병렬로 돌리고 결과를 한 파일에 모은다.

⚠️ 실제 AWS 액션이 실행된다.
   - EC2  cost_inefficiency -> DEC-001 -> Stop     (risk=LOW,  승인 불필요)
   - Lambda cost_spike      -> DEC-002 -> Throttle (risk=MED,  승인 필요)
   Lambda처럼 requires_approval=True인 경로는 measure()가 승인 게이트에서 멈추고
   stopped_at="approval_gate"로 반환한다 — 이게 실제 운영 동작이므로 그대로 기록한다.

⚠️ Lambda는 "이상 상태"가 시간이 지나면 사라진다.
   에러 폭증 트래픽이 끝나고 시간이 지나면 탐지 창의 최근 구간이 무트래픽이 되어
   지속성 체크가 통과하지 않는다(실측: 트래픽 종료 24분 뒤 anomaly_flag=False).
   그래서 --lambda-traffic-minutes 로 파이프라인 직전에 트래픽을 다시 만들 수 있다.

[실행 방법]
  EC2 13대 (이미 좀비 상태라 바로 가능):
    python playground/batch_pipeline_run.py --resource-type EC2
  Lambda 13개 (트래픽 10분 재생성 후 파이프라인):
    python playground/batch_pipeline_run.py --resource-type Lambda --lambda-traffic-minutes 10

[생성 파일]
  playground/eval_outputs/batch_pipeline__{타입}_{YYYYMMDD_HHMMSS}.json
"""

from __future__ import annotations

SCRIPT_VERSION = "1"

import argparse
import json
import os
import random
import statistics
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(PROJECT_ROOT / "playground") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "playground"))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

import boto3

from measure_pipeline_timing import measure
from lambda_retry_repeated_trial import LAMBDA_FUNCTIONS, INVOKE_PROFILE_DEFAULT

AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2")
RESULT_DIR = PROJECT_ROOT / "playground" / "eval_outputs"

# EC2 좀비 실험에 쓴 13대 (ec2_zombie_manifest.json과 동일 — 라벨은 원래 실험 기준)
EC2_INSTANCES = [
    ("i-00f27d6650869a74d", "anomaly", "silent"),
    ("i-094595e331b19be17", "anomaly", "silent"),
    ("i-0238a05593fbcf2f2", "anomaly", "whisper"),
    ("i-046cbf400dd6dc9d7", "anomaly", "whisper"),
    ("i-013a9d143009d1376", "anomaly", "whisper_more"),
    ("i-0e810265b88caeac5", "normal",  "light"),
    ("i-0cd6cf73f56959d2e", "normal",  "light"),
    ("i-07c77db4628d7e7ca", "normal",  "moderate"),
    ("i-04cec80c3045da327", "normal",  "moderate"),
    ("i-0380a0b372a973a5c", "normal",  "heavy"),
    ("i-053230c6e903132f8", "normal",  "heavy"),
    ("i-01aed37041b8e6946", "normal",  "bursty"),
    ("i-0973aa83fa2ba0dd9", "normal",  "bursty"),
]


# ── Lambda 트래픽 재생성 ──────────────────────────────────────────────────────

def _generate_lambda_traffic(minutes: int, invoke_profile: str) -> dict[str, dict]:
    """파이프라인 직전에 에러 폭증/정상 트래픽을 다시 만든다. 함수별 실측 호출/에러 수 반환."""
    lam = boto3.Session(profile_name=invoke_profile).client("lambda", region_name=AWS_REGION)
    stats: dict[str, dict] = {}

    def one(fn_name: str, label: str, error_rate: float, interval_sec: int) -> None:
        n_calls = int(minutes * 60 / interval_sec)
        n_err = 0
        for _ in range(n_calls):
            is_err = random.random() < error_rate
            payload = b'{"force_error": true}' if is_err else b'{}'
            try:
                lam.invoke(FunctionName=fn_name, InvocationType="Event", Payload=payload)
                n_err += int(is_err)
            except Exception as exc:
                print(f"[{fn_name}] invoke 실패: {exc}")
            time.sleep(interval_sec)
        stats[fn_name] = {"label": label, "n_calls": n_calls, "n_errors": n_err,
                          "target_error_rate": error_rate, "interval_sec": interval_sec}
        print(f"[traffic {label} {fn_name}] 완료 에러 {n_err}/{n_calls}")

    print(f"=== Lambda 트래픽 {minutes}분 재생성 시작 ({len(LAMBDA_FUNCTIONS)}개 병렬) ===")
    with ThreadPoolExecutor(max_workers=len(LAMBDA_FUNCTIONS)) as ex:
        futs = [ex.submit(one, n, l, e, i) for (n, l, e, i) in LAMBDA_FUNCTIONS]
        for f in as_completed(futs):
            f.result()

    # CloudWatch 반영 대기 (반복시행 스크립트와 동일하게 120초)
    print("CloudWatch 반영 120초 대기...")
    time.sleep(120)
    return stats


# ── 파이프라인 1건 ───────────────────────────────────────────────────────────

def _run_one(resource_id: str, resource_type: str, label: str, profile: str | None) -> dict:
    t0 = time.time()
    try:
        # bypass_approval_for_timing=False — 승인 게이트를 우회하지 않는다.
        # 승인이 필요한 경로는 거기서 멈춘 사실 자체를 결과로 기록한다.
        result = measure(resource_id, resource_type, bypass_approval_for_timing=False)
        result["label"] = label
        result["profile"] = profile
        result["elapsed_sec"] = round(time.time() - t0, 1)
        return result
    except Exception as exc:
        print(f"[{resource_id}] 실패: {exc}\n{traceback.format_exc()}")
        return {"resource_id": resource_id, "resource_type": resource_type,
                "label": label, "profile": profile, "error": str(exc),
                "elapsed_sec": round(time.time() - t0, 1)}


# ── 집계 ─────────────────────────────────────────────────────────────────────

def _summarize(results: list[dict]) -> dict:
    ok = [r for r in results if "error" not in r]
    detected = [r for r in ok if r.get("anomaly_flag")]
    at_gate = [r for r in detected if r.get("stopped_at") == "approval_gate"]
    executed = [r for r in detected if r.get("stopped_at") != "approval_gate"]
    qa_pass = [r for r in executed if r.get("qa_passed") is True]

    # 라벨별 탐지 (원래 실험 라벨 기준 — EC2는 부하 생성기 종료 후라 정상군도 좀비 상태임에 유의)
    by_label: dict[str, dict[str, int]] = {}
    for r in ok:
        b = by_label.setdefault(r.get("label") or "unknown", {"total": 0, "detected": 0})
        b["total"] += 1
        b["detected"] += int(bool(r.get("anomaly_flag")))

    def stat(key: str) -> dict | None:
        vals = [r["timings"][key] for r in ok if key in r.get("timings", {})]
        if not vals:
            return None
        return {
            "n": len(vals),
            "mean_sec": round(statistics.mean(vals), 3),
            "stdev_sec": round(statistics.stdev(vals), 3) if len(vals) > 1 else 0.0,
            "min_sec": round(min(vals), 3),
            "max_sec": round(max(vals), 3),
        }

    return {
        "n_resources": len(results),
        "n_ok": len(ok),
        "n_error": len(results) - len(ok),
        "n_detected": len(detected),
        "n_stopped_at_approval_gate": len(at_gate),
        "n_action_executed": len(executed),
        "n_qa_passed": len(qa_pass),
        "qa_pass_rate": (len(qa_pass) / len(executed)) if executed else None,
        "detected_by_label": by_label,
        "selected_actions": {a: sum(1 for r in detected if r.get("selected_action") == a)
                             for a in sorted({r.get("selected_action") for r in detected
                                              if r.get("selected_action")})},
        "timings": {k: stat(k) for k in
                    ["detection", "classification", "decision", "action", "qa", "logging", "total"]},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resource-type", required=True, choices=["EC2", "Lambda"])
    parser.add_argument("--lambda-traffic-minutes", type=int, default=0,
                        help="Lambda 전용: 파이프라인 직전에 트래픽을 이 시간(분)만큼 재생성. 0이면 생략")
    parser.add_argument("--invoke-profile", default=INVOKE_PROFILE_DEFAULT,
                        help="Lambda 트래픽 생성에 쓸 AWS 프로필(탐지는 .env 프로필 그대로)")
    parser.add_argument("--max-workers", type=int, default=13)
    args = parser.parse_args()

    traffic_stats = {}
    if args.resource_type == "EC2":
        targets = [(rid, label, prof) for rid, label, prof in EC2_INSTANCES]
    else:
        if args.lambda_traffic_minutes > 0:
            traffic_stats = _generate_lambda_traffic(args.lambda_traffic_minutes, args.invoke_profile)
        targets = [(fn, label, None) for (fn, label, _e, _i) in LAMBDA_FUNCTIONS]

    print(f"=== {args.resource_type} {len(targets)}개 전체 파이프라인 병렬 실행 시작 ===")
    t0 = time.time()
    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=min(args.max_workers, len(targets))) as ex:
        futs = {ex.submit(_run_one, rid, args.resource_type, label, prof): rid
                for rid, label, prof in targets}
        for f in as_completed(futs):
            results.append(f.result())

    order = {rid: i for i, (rid, _l, _p) in enumerate(targets)}
    results.sort(key=lambda r: order.get(r["resource_id"], 999))

    summary = _summarize(results)
    print("\n=== 집계 ===")
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = RESULT_DIR / f"batch_pipeline__{args.resource_type}_{ts}.json"
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "script_version": SCRIPT_VERSION,
        "resource_type": args.resource_type,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "wall_clock_sec": round(time.time() - t0, 1),
        "lambda_traffic_minutes": args.lambda_traffic_minutes,
        "lambda_traffic_stats": traffic_stats,
        "summary": summary,
        "results": results,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"\n결과 저장: {out_path}")


if __name__ == "__main__":
    main()
