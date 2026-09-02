from datetime import datetime, timedelta, timezone

import psycopg2
import psycopg2.extras
from fastapi import APIRouter

from api import graph_runtime, pipeline_stats as run_stats, store
from api.pg import connection_params
from config import decision_policy, pipeline_live_status

router = APIRouter(tags=["status"])

# agent_steps.step_name으로 실제 기록되는 5개 + logging(별도 판단, 아래 참고)
_STEP_NAMES = ["detection", "classification", "decision", "action", "qa"]

# "실행 중" 판단은 두 갈래로 나뉜다 
# 1) 실시간 파일(config/pipeline_live_status.py, FRESHNESS_SECONDS=30)이 최근 것이면 → 그걸 그대로 씀. 
#      run_full_pipeline.py가 지금 이 순간 stream()으로 노드를
#      갱신하고 있다는 뜻이라 pipeline_running=True 확정.
# 2) 실시간 파일이 없거나 오래됐으면(=지금 아무 프로세스도 안 돌고 있음) → 아래 _load_pipeline_status()의 "과거 기록 기반 추정"으로 대체. 
#    이때 쓰는 기준이 _STALE_CYCLES (마지막으로 끝난 실행이 폴링 주기의 3배 이내면
#    그래도 최근에 돌았다고 봄, 한 사이클 정도 응답이 늦어져도 바로 STOPPED로 오판하지 않으려는 여유값)
_STALE_CYCLES = 3


def _load_pipeline_status() -> tuple[dict, bool]:
    """(노드별 상태, pipeline_running) — 최근 1회 실행 기록(agent_runs/agent_steps) 기반.

    실시간으로 "지금 이 순간 어느 노드가 도는 중"을 보여주는 게 아니라(그런 신호를
    남기는 곳이 없음), "가장 최근 실행에서 각 단계가 어떻게 끝났는지"를 보여준다.
    """
    default_nodes = {name: "idle" for name in _STEP_NAMES + ["logging"]}

    try:
        conn = psycopg2.connect(**connection_params())
    except Exception:
        return default_nodes, False

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT to_regclass('public.agent_runs')")
            if cur.fetchone()["to_regclass"] is None:
                return default_nodes, False

            cur.execute(
                "SELECT run_id, anomaly_flag, finished_at FROM agent_runs "
                "ORDER BY finished_at DESC LIMIT 1"
            )
            latest_run = cur.fetchone()
            if latest_run is None:
                return default_nodes, False

            cur.execute(
                "SELECT step_name, status FROM agent_steps WHERE run_id = %s",
                (latest_run["run_id"],),
            )
            step_status_by_name = {row["step_name"]: row["status"] for row in cur.fetchall()}
    finally:
        conn.close()

    # logging_agent.py의 "모든 값이 None이면 skipped" 판정이 decision/qa 단계에서는 부정확하다 
        # — 두 단계 모두 애초에 None이 아닌 기본값(candidate_actions=[],
        # requires_approval=False, rollback_count=0)을 초기 state에 갖고 있어서, 
        # 실제로는 안 돌았어도 항상 status="success"로 기록된다. 
        # logging_agent.py를 고치는 대신,
        # 여기서 그래프 구조상 확정적인 사실(anomaly_flag=False면 detection_router가
        # classification/decision/action/qa를 전부 건너뛰고 바로 logging으로 감)로
        # 그 4단계를 강제로 idle 처리한다.
    nodes = {}
    for name in _STEP_NAMES:
        raw_status = step_status_by_name.get(name)
        nodes[name] = "success" if raw_status == "success" else "idle"
    if not latest_run["anomaly_flag"]:
        for name in ("classification", "decision", "action", "qa"):
            nodes[name] = "idle"
    # logging 자체는 agent_steps에 안 남는다 
    # (logging_node가 이 run을 기록했다는 사실 자체가 
    # logging 단계가 끝까지 실행됐다는 증거이므로 success로 본다.)
    nodes["logging"] = "success"

    finished_at = latest_run["finished_at"]
    if finished_at.tzinfo is None:
        finished_at = finished_at.replace(tzinfo=timezone.utc)
    staleness = datetime.now(timezone.utc) - finished_at
    threshold = timedelta(minutes=decision_policy.get_polling_interval_minutes() * _STALE_CYCLES)
    pipeline_running = staleness < threshold

    return nodes, pipeline_running


@router.get("/status")
def get_status():
    counts = run_stats.get_run_counts()
    store.pipeline_stats["anomaly_detected"] = counts["anomaly_detected"]
    store.pipeline_stats["anomaly_completed"] = counts["anomaly_completed"]
    store.pipeline_stats["anomaly_failed"] = counts["anomaly_failed"]
    store.pipeline_stats["pending_approvals"] = len(graph_runtime.list_pending_approvals())
    store.pipeline_stats["active_rules"] = sum(1 for r in store.rule_book if r["enabled"])

    # 1) 실시간(pipeline_live_status.FRESHNESS_SECONDS=30초 이내) 우선,
    # 2) 없으면 과거 기록 기반 추정(_load_pipeline_status, 기준은 위 _STALE_CYCLES) 
    live = pipeline_live_status.read_if_fresh()
    if live is not None:
        nodes, pipeline_running = live["nodes"], True
    else:
        nodes, pipeline_running = _load_pipeline_status()

    return {
        "pipeline_running": pipeline_running,
        "nodes": nodes,
        "stats": store.pipeline_stats,
    }
