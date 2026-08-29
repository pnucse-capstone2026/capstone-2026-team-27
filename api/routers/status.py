from fastapi import APIRouter

from api import graph_runtime, pipeline_stats as run_stats, store

router = APIRouter(tags=["status"])


@router.get("/status")
def get_status():
    counts = run_stats.get_run_counts()
    store.pipeline_stats["anomaly_detected"] = counts["anomaly_detected"]
    store.pipeline_stats["anomaly_completed"] = counts["anomaly_completed"]
    store.pipeline_stats["anomaly_failed"] = counts["anomaly_failed"]
    store.pipeline_stats["pending_approvals"] = len(graph_runtime.list_pending_approvals())
    store.pipeline_stats["active_rules"] = sum(1 for r in store.rule_book if r["enabled"])
    return {
        "pipeline_running": True,
        "nodes": store.pipeline_nodes,
        "stats": store.pipeline_stats,
    }
