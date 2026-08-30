"""
playground/run_full_pipeline.py

⚠️ 실제 조치(action)를 자동 실행할 수 있는 스크립트. 전체 리소스 타입(EC2/Lambda/RDS/
AutoScaling/S3) 대상.

원래 Lambda 하나로 좁혀서 몇 사이클 지켜본 파일럿이었으나(playground/
validate_real_aws_buffer.py로 detection은 이미 밤새 검증됨), 관찰 결과 문제 없어서
전체 리소스로 범위를 넓힘.

detection~logging을 한 번에 잇는 방식:
  discover_all_resources()로 찾은 리소스마다 raw_metrics만 채운 초기 state를 만들고,
  pipeline.graph.app (entry point=detection, build_graph() 그대로) 에 딱 한 번만
  invoke한다. 그래프 안의 detection_router가 이상이면 classification~logging까지
  자동으로 이어주고, 정상이면 바로 logging으로 끝낸다.
  (이전에는 run_detection_cycle_streaming()으로 먼저 탐지한 뒤 그 결과를 다시
  app.invoke()에 넣는 2단계 구조였는데, 그러면 detection_node가 이상 1건당 두 번
  불려서 _get_or_train_iforest의 학습 버퍼가 중복 갱신되는 문제가 있었음 — 그래서
  탐지 전 원본 state로 리소스당 invoke를 정확히 한 번만 하도록 바꿈.)

안전장치:
  - risk_level=MED/HIGH는 기존 설계대로 requires_approval=True → 실제 조치 자동 실행 안 됨
    (LOW로 판정된 것만 실제로 action_agent가 boto3 호출까지 감)
  - EC2/AutoScaling은 action_agent에 실제 조치 로직이 구현되어 있어 LOW 판정 시 진짜 실행됨.
    RDS/S3는 아직 미구현이라 action_node에서 안전하게 no-op됨.
  - pipeline/run_scheduler.py는 전체 리소스 대상 detection-only로 안전 설계 그대로 유지 — 안 건드림
  - 매 사이클 결과를 콘솔 + playground/eval_outputs/full_pipeline.jsonl에 기록

[실행 방법]
  프로젝트 루트에서:
    python playground/run_full_pipeline.py --once                       # 전체 리소스 1회 실행
    python playground/run_full_pipeline.py --once --resource-types Lambda,EC2
    python playground/run_full_pipeline.py --loop                       # 관리자 설정의 폴링 주기로 반복 (Ctrl+C로 중단)
    python playground/run_full_pipeline.py --loop --max-cycles 20
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

from config import decision_policy
from pipeline.resource_discovery import discover_all_resources
from pipeline.orchestrator import assemble_resource
from pipeline.detection_agent import _build_initial_state
from pipeline.graph import app

LOG_PATH = PROJECT_ROOT / "playground" / "eval_outputs" / "full_pipeline.jsonl"


def _append_log(record: dict) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def _run_once(resource_types: list[str] | None = None) -> tuple[int, int]:
    """태그된 리소스 전체 스캔 → 리소스마다 전체 그래프(detection~logging) 1회 invoke.
    (스캔한 리소스 수, 이상 발견 건수) 반환.

    resource_types: --resource-types CLI 인자로 명시적으로 준 경우에만 이 값을 쓰고,
    안 주면(None) 관리자 설정(config/decision_policy.json)의 활성화된 리소스 타입을 쓴다."""
    if resource_types is None:
        resource_types = decision_policy.get_enabled_resource_types()
    discovered = discover_all_resources(resource_types=resource_types)

    n_scanned = 0
    n_anomaly = 0
    for r in discovered:
        n_scanned += 1
        try:
            resource = assemble_resource(r["resource_id"], r["resource_type"])
        except Exception as exc:
            print(f"  [{r['resource_type']} {r['resource_id']}] 지표 수집 실패: {exc}")
            continue

        result = app.invoke(_build_initial_state(resource))

        if not result.get("anomaly_flag"):
            continue
        n_anomaly += 1

        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "resource_id": result.get("resource_id"),
            "resource_type": result.get("resource_type"),
            "anomaly_type": result.get("anomaly_type"),
            "selected_action": result.get("selected_action"),
            "risk_level": result.get("risk_level"),
            "requires_approval": result.get("requires_approval"),
            "action_executed": result.get("action_executed"),
            "action_result": result.get("action_result"),
            "qa_passed": result.get("qa_passed"),
            "rollback_count": result.get("rollback_count"),
        }
        print(f"  [이상 발견] {record['resource_type']} {record['resource_id']}")
        print("  " + json.dumps(record, ensure_ascii=False))
        _append_log(record)

    if n_anomaly == 0:
        print(f"  이상 없음 ({n_scanned}개 리소스 정상)")
    return n_scanned, n_anomaly


def run_loop(max_cycles: int | None, resource_types: list[str] | None) -> None:
    print("전체 파이프라인 시작 — 관리자 설정의 폴링 주기로 반복")
    print(f"로그: {LOG_PATH}")
    cycle = 0
    while True:
        cycle += 1
        print(f"\n=== 사이클 {cycle} ({datetime.now(timezone.utc).isoformat()}) ===")
        _run_once(resource_types)

        if max_cycles is not None and cycle >= max_cycles:
            print(f"\nmax_cycles({max_cycles}) 도달 — 종료")
            break

        poll_interval_seconds = decision_policy.get_polling_interval_minutes() * 60
        print(f"{poll_interval_seconds}초 대기...")
        time.sleep(poll_interval_seconds)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--loop", action="store_true", help="5분마다 반복 실행")
    parser.add_argument("--once", action="store_true", help="1회만 실행")
    parser.add_argument("--max-cycles", type=int, default=None, help="--loop와 함께: N회만 돌고 종료")
    parser.add_argument(
        "--resource-types", type=str, default=None,
        help="쉼표로 구분된 리소스 타입 목록 (예: Lambda,EC2). 안 주면 전체 타입 스캔.",
    )
    args = parser.parse_args()

    resource_types = args.resource_types.split(",") if args.resource_types else None

    if args.once:
        print(f"=== 1회 실행 ({datetime.now(timezone.utc).isoformat()}) ===")
        _run_once(resource_types)
    elif args.loop:
        run_loop(args.max_cycles, resource_types)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
