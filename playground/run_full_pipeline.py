"""
playground/run_full_pipeline.py

실제 조치(action)를 자동 실행할 수 있는 스크립트. 
전체 리소스 타입(EC2/Lambda/RDS/AutoScaling/S3) 대상.


detection~logging을 한 번에 잇는 방식:
  discover_all_resources()로 찾은 리소스마다 raw_metrics만 채운 초기 state를 만들고,
  pipeline.graph.build_approval_graph(checkpointer) (entry point=detection,
  with_approval_gate=True) 에 딱 한 번만 invoke한다. 그래프 안의 detection_router가
  이상이면 classification~logging까지 자동으로 이어주고, 정상이면 바로 logging으로
  끝낸다.
  (이전에는 run_detection_cycle_streaming()으로 먼저 탐지한 뒤 그 결과를 다시
  app.invoke()에 넣는 2단계 구조였는데, 그러면 detection_node가 이상 1건당 두 번
  불려서 _get_or_train_iforest의 학습 버퍼가 중복 갱신되는 문제가 있었음)


안전장치:
  - risk_level=MED/HIGH는 기존 설계대로 requires_approval=True → 실제 조치 자동 실행 안 됨
    (LOW로 판정된 것만 실제로 action_agent가 boto3 호출까지 감), 이제는 그 대신
    Slack 알림 + 관리자 웹 승인 대기 큐에 실제로 걸림
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
import uuid
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

from config import decision_policy, pipeline_live_status
from pipeline.resource_discovery import discover_all_resources
from pipeline.orchestrator import assemble_resource
from pipeline.detection_agent import _build_initial_state
from pipeline.graph import build_approval_graph
from pipeline.checkpointer import get_postgres_checkpointer

LOG_PATH = PROJECT_ROOT / "playground" / "eval_outputs" / "full_pipeline.jsonl"


def _append_log(record: dict) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def _run_once(app, resource_types: list[str] | None = None) -> tuple[int, int]:
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

        # checkpointer가 이 실행을 하나의 "스레드"로 기억해야 나중에 승인/거부로
        # 재개할 수 있다 — 리소스+시각마다 매번 새 thread_id를 발급한다.
        thread_id = f"full-pipeline-{r['resource_type']}-{r['resource_id']}-{uuid.uuid4()}"
        config = {"configurable": {"thread_id": thread_id}}

        # invoke() 대신 stream()으로 돌려서, 노드가 하나 끝날 때마다 즉시
        # config/pipeline_live_status.json을 갱신한다 — 이게 있어야 관리자 웹이
        # "지금 이 순간 어느 노드까지 왔는지"를 실시간으로 보여줄 수 있다
        # (invoke()는 전체가 끝난 뒤 최종 결과만 주기 때문에 중간 진행 상황을 알 수 없음).
        live_nodes = pipeline_live_status.initial_nodes()
        pipeline_live_status.write(live_nodes, r["resource_id"], r["resource_type"])

        for chunk in app.stream(_build_initial_state(resource), config, stream_mode="updates"):
            for node_name in chunk:
                if node_name in live_nodes:
                    live_nodes[node_name] = "success"

            # get_state().next는 LangGraph가 계산해둔 "다음에 실행될 노드"다 — 이걸
            # 미리 running으로 찍어둬야, 다음 청크가 오기 전까지의 실행 시간 동안
            # (예: decision 노드가 LLM 응답을 기다리는 몇 초) 화면에 실제로 반짝인다.
            upcoming = app.get_state(config).next
            for node_name in upcoming:
                if node_name in live_nodes and live_nodes[node_name] == "idle":
                    live_nodes[node_name] = "running"

            pipeline_live_status.write(live_nodes, r["resource_id"], r["resource_type"])

        # app.get_state()가 LangGraph 채널을 통해 이미 병합해둔 최신 state 전체를
        # 그대로 준다 — stream 청크(부분 업데이트)를 직접 합칠 필요가 없다.
        current_state = app.get_state(config)
        result = dict(current_state.values)
        if current_state.next:
            result["__interrupt__"] = True

        if not result.get("anomaly_flag"):
            continue
        n_anomaly += 1

        # 승인 대기로 멈춘 경우, action/qa 관련 필드는 아직 실행 전이라 전부 비어있는
        # 게 정상이다 (approval_gate에서 interrupt()로 멈췄으므로) — thread_id를 남겨야
        # 나중에 관리자 웹이 이 스레드를 찾아서 승인/거부로 재개할 수 있다.
        interrupted = "__interrupt__" in result
        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "thread_id": thread_id,
            "resource_id": result.get("resource_id"),
            "resource_type": result.get("resource_type"),
            "anomaly_type": result.get("anomaly_type"),
            "selected_action": result.get("selected_action"),
            "risk_level": result.get("risk_level"),
            "requires_approval": result.get("requires_approval"),
            "pending_approval": interrupted,
            "action_executed": result.get("action_executed"),
            "action_result": result.get("action_result"),
            "qa_passed": result.get("qa_passed"),
            "rollback_count": result.get("rollback_count"),
        }
        status_label = "승인 대기 중" if interrupted else "처리 완료"
        print(f"  [이상 발견 - {status_label}] {record['resource_type']} {record['resource_id']}")
        print("  " + json.dumps(record, ensure_ascii=False))
        _append_log(record)

    if n_anomaly == 0:
        print(f"  이상 없음 ({n_scanned}개 리소스 정상)")
    return n_scanned, n_anomaly


def run_loop(app, max_cycles: int | None, resource_types: list[str] | None) -> None:
    print("전체 파이프라인 시작 — 관리자 설정의 폴링 주기로 반복")
    print(f"로그: {LOG_PATH}")
    cycle = 0
    while True:
        cycle += 1
        print(f"\n=== 사이클 {cycle} ({datetime.now(timezone.utc).isoformat()}) ===")
        _run_once(app, resource_types)

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

    if not (args.once or args.loop):
        parser.print_help()
        return

    # checkpointer(Postgres)는 스크립트가 살아있는 동안 계속 열어둔다 — 승인 대기로
    # 멈춘 스레드들의 상태가 이 연결이 유지되는 프로세스 안이 아니라 DB에 저장되므로,
    # 이 스크립트가 나중에 재시작돼도(또는 관리자 웹이 별도로) 이어서 조회/재개할 수 있다.
    with get_postgres_checkpointer() as checkpointer:
        checkpointer.setup()
        app = build_approval_graph(checkpointer)

        if args.once:
            print(f"=== 1회 실행 ({datetime.now(timezone.utc).isoformat()}) ===")
            _run_once(app, resource_types)
        else:
            run_loop(app, args.max_cycles, resource_types)


if __name__ == "__main__":
    main()
