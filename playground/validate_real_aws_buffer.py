"""
playground/validate_real_aws_buffer.py

실제 AWS(.env의 INSTANCE_ID / LAMBDA_FUNCTION_NAME / ASG_NAME)로 detection_node()를
반복 실행하면서, 자기참조 학습 버퍼의 채택/제외 판정과 anomaly_flag 이력을 실제
CloudWatch 데이터 기준으로 기록한다.

왜 필요한가:
  playground/phase6_detection_node_e2e.py로 측정한 채택률(24.2% → 마진 완화 후 45.6%)은
  전부 rng.uniform() 균등분포로 만든 합성 데이터 기준이다. 실제 CloudWatch 지표는
  낮/밤·평일/주말 패턴과 시점 간 자기상관이 있어서 합성 데이터와 통계적 성질이
  다르다 — 이 스크립트로 실제 데이터에서는 채택률/오탐률이 어떻게 나오는지 확인한다.

⚠️ 실제 AWS API를 호출한다 (CloudWatch GetMetricData, EC2/Lambda/ASG 설명 조회 등 — 전부
   읽기 전용). detection_node()만 직접 호출하고 classification/decision/action/qa는
   거치지 않으므로 실제 Stop/Throttle 같은 조치는 절대 실행되지 않는다.

⚠️ 모델 캐시는 기본적으로 이 검증 전용 디렉토리(.validation_models_real_aws)를 쓴다 —
   실제 운영 중인 models/ 버퍼에 영향 안 주려는 것. 운영 중인 실제 버퍼 자체를 보고
   싶으면 PIPELINE_MODEL_DIR 환경변수를 운영과 동일하게 맞춰서 실행하면 된다.

[실행 방법]
  프로젝트 루트에서:
    python playground/validate_real_aws_buffer.py --loop              # 5분마다 반복 (Ctrl+C로 중단)
    python playground/validate_real_aws_buffer.py --loop --max-cycles 20   # 20회만 돌고 종료 (~100분)
    python playground/validate_real_aws_buffer.py --once              # 1회만 실행
    python playground/validate_real_aws_buffer.py --summary           # 지금까지 쌓인 로그 집계만 출력
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

import pipeline.detection_agent as da
from pipeline.orchestrator import assemble_resource
from playground.phase6_detection_node_e2e import _BufferDecisionCapture

LOG_PATH = PROJECT_ROOT / "playground" / "eval_outputs" / "real_aws_buffer_validation.jsonl"
DEFAULT_MODEL_DIR = str(PROJECT_ROOT / ".validation_models_real_aws")
POLL_INTERVAL_SECONDS = 300  # run_scheduler.py와 동일한 주기 (5분)
N_POINTS = 30
PERIOD_SECONDS = 300  # 2.5시간 윈도우 (기존 그대로 — 이번 검증은 마진만 대상)


def _real_resources() -> list[dict]:
    """.env에 설정된 실제 테스트 리소스만 사용 (없는 건 건너뜀)."""
    candidates = [
        ("EC2", os.getenv("INSTANCE_ID")),
        ("Lambda", os.getenv("LAMBDA_FUNCTION_NAME")),
        ("AutoScaling", os.getenv("ASG_NAME")),
    ]
    resources = [
        {"resource_type": rt, "resource_id": rid}
        for rt, rid in candidates
        if rid
    ]
    if not resources:
        raise SystemExit(
            ".env에 INSTANCE_ID / LAMBDA_FUNCTION_NAME / ASG_NAME 중 하나도 없습니다. "
            "실제 테스트 리소스를 먼저 설정하세요 (scripts/recreate_test_resources.sh 참고)."
        )
    return resources


def _run_one_cycle(resources: list[dict]) -> list[dict]:
    """리소스마다 실제 CloudWatch 지표를 가져와 detection_node() 1번 호출하고,
    버퍼 채택/제외 판정 로그를 같이 캡쳐해서 레코드로 남긴다."""
    logger = logging.getLogger("pipeline.detection_agent")
    prev_level = logger.level
    logger.setLevel(logging.INFO)

    records = []
    for r in resources:
        capture = _BufferDecisionCapture()
        logger.addHandler(capture)
        row = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "resource_type": r["resource_type"],
            "resource_id": r["resource_id"],
        }
        try:
            assembled = assemble_resource(
                r["resource_id"], r["resource_type"], n_points=N_POINTS, period_seconds=PERIOD_SECONDS
            )
            state = {
                "resource_id": assembled["resource_id"],
                "resource_type": assembled["resource_type"],
                "raw_metrics": assembled["raw_metrics"],
            }
            out = da.detection_node(state)
            row.update({
                "ok": True,
                "anomaly_flag": out["anomaly_flag"],
                "triggered_metrics": out["triggered_metrics"],
                "zscore": out["anomaly_score_zscore"],
                "iforest": out["anomaly_score_iforest"],
                "buffer_decision": capture.records[0] if capture.records else None,  # None = 콜드스타트 경로(로그 자체가 안 남음)
            })
        except Exception as exc:
            row.update({"ok": False, "error": repr(exc)})
            print(f"[validate_real_aws_buffer] {r['resource_type']} {r['resource_id']} 실패: {exc!r}")
        finally:
            logger.removeHandler(capture)
        records.append(row)

    logger.setLevel(prev_level)
    return records


def _append_log(records: list[dict]) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def _print_cycle(records: list[dict]) -> None:
    for r in records:
        if not r.get("ok"):
            continue
        bd = r.get("buffer_decision")
        bd_str = "콜드스타트" if bd is None else ("채택" if bd["accepted"] else "제외")
        print(
            f"  [{r['resource_type']}] {r['resource_id']} anomaly_flag={r['anomaly_flag']} "
            f"zscore={r['zscore']} iforest={r['iforest']} buffer={bd_str}"
        )


def run_loop(max_cycles: int | None) -> None:
    resources = _real_resources()
    da.IFOREST_MODEL_DIR = os.environ.get("PIPELINE_MODEL_DIR", DEFAULT_MODEL_DIR)
    print(f"모델 캐시 디렉토리: {da.IFOREST_MODEL_DIR}")
    print(f"대상 리소스: {[(r['resource_type'], r['resource_id']) for r in resources]}")
    print(f"로그 저장 위치: {LOG_PATH}")

    cycle = 0
    while True:
        cycle += 1
        print(f"\n=== 사이클 {cycle} ({datetime.now(timezone.utc).isoformat()}) ===")
        records = _run_one_cycle(resources)
        _print_cycle(records)
        _append_log(records)

        if max_cycles is not None and cycle >= max_cycles:
            print(f"\nmax_cycles({max_cycles}) 도달 — 종료")
            break

        print(f"{POLL_INTERVAL_SECONDS}초 대기...")
        time.sleep(POLL_INTERVAL_SECONDS)


def run_once() -> None:
    resources = _real_resources()
    da.IFOREST_MODEL_DIR = os.environ.get("PIPELINE_MODEL_DIR", DEFAULT_MODEL_DIR)
    print(f"모델 캐시 디렉토리: {da.IFOREST_MODEL_DIR}")
    records = _run_one_cycle(resources)
    _print_cycle(records)
    _append_log(records)


def print_summary() -> None:
    if not LOG_PATH.exists():
        print(f"{LOG_PATH}가 아직 없습니다 — --once 나 --loop로 먼저 데이터를 쌓으세요.")
        return

    rows = []
    with open(LOG_PATH, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))

    ok_rows = [r for r in rows if r.get("ok")]
    print(f"총 기록 {len(rows)}건 (성공 {len(ok_rows)}건, 실패 {len(rows) - len(ok_rows)}건)")

    by_type: dict[str, dict] = {}
    for r in ok_rows:
        d = by_type.setdefault(r["resource_type"], {
            "n": 0, "n_anomaly": 0,
            "n_buffer_seen": 0, "n_buffer_accepted": 0, "n_cold_start": 0,
        })
        d["n"] += 1
        if r["anomaly_flag"]:
            d["n_anomaly"] += 1
        bd = r.get("buffer_decision")
        if bd is None:
            d["n_cold_start"] += 1
        else:
            d["n_buffer_seen"] += 1
            if bd["accepted"]:
                d["n_buffer_accepted"] += 1

    print("\n리소스 타입별:")
    for rt, d in by_type.items():
        anomaly_rate = d["n_anomaly"] / d["n"] if d["n"] else 0.0
        accept_rate = d["n_buffer_accepted"] / d["n_buffer_seen"] if d["n_buffer_seen"] else None
        print(f"  {rt:<14} 관측={d['n']:<4} anomaly_flag=True 비율={anomaly_rate:.1%} "
              f"(오탐 의심 — 실제로 부하 테스트 중이 아니라면 전부 오탐)  "
              f"버퍼 채택률={'N/A(콜드스타트만)' if accept_rate is None else f'{accept_rate:.1%}'} "
              f"(콜드스타트 제외 {d['n_buffer_seen']}건 중)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--loop", action="store_true", help="5분마다 반복 실행 (Ctrl+C로 중단)")
    parser.add_argument("--once", action="store_true", help="1회만 실행")
    parser.add_argument("--summary", action="store_true", help="쌓인 로그 집계만 출력")
    parser.add_argument("--max-cycles", type=int, default=None, help="--loop와 함께: N회만 돌고 종료")
    args = parser.parse_args()

    if args.summary:
        print_summary()
    elif args.once:
        run_once()
    elif args.loop:
        run_loop(args.max_cycles)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
