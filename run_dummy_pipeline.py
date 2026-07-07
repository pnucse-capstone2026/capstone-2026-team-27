"""
더미 파이프라인 실행 스크립트.

케이스 1: 정상 실행 — qa_passed=True, 파이프라인 끝까지 통과
케이스 2: 롤백 루프 — qa_passed=False 강제, action → qa 루프가 정확히 2번 돌고 멈추는지 확인
케이스 3: anomaly 없음 — anomaly_flag=False 강제, classification 건너뛰고 logging으로 바로 이동
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pipeline.graph import build_graph
from pipeline.dummy_nodes import qa_node_fail


# ── 공통 초기 State ───────────────────────────────────────────────────────────

def make_initial_state(resource_type="EC2") -> dict:
    metrics_by_type = {
        "EC2": {
            "cpu_utilization": [10.0] * 30,
            "network_in":      [1024.0] * 30,
            "network_out":     [512.0] * 30,
            "cost":            [0.01] * 30,
        },
        "Lambda": {
            "invocation_count": [100.0] * 30,
            "error_count":      [0.0] * 30,
            "duration_avg":     [200.0] * 30,
            "cost":             [0.001] * 30,
        },
    }
    return {
        "resource_id":             "i-0testdummy",
        "resource_type":           resource_type,
        "raw_metrics":             metrics_by_type.get(resource_type, {}),
        "timestamp":               "2026-06-23T00:00:00",
        "anomaly_flag":            False,
        "anomaly_score_zscore":    None,
        "anomaly_score_iforest":   None,
        "triggered_metrics":       [],
        "anomaly_type":            None,
        "classification_reasoning": None,
        "interim_action_taken":    None,
        "candidate_actions":       [],
        "selected_action":         None,
        "risk_level":              None,
        "requires_approval":       False,
        "decision_reasoning":      None,
        "pre_action_snapshot":     None,
        "action_executed":         None,
        "action_result":           None,
        "qa_passed":               None,
        "sla_check_result":        None,
        "rollback_count":          0,
        "log_entries":             [],
    }


def print_result(label: str, result: dict) -> None:
    print(f"\n{'='*60}")
    print(f"  {label}")
    print('='*60)
    skip = {"raw_metrics"}  # 길어서 생략
    for k, v in result.items():
        if k in skip:
            continue
        print(f"  {k:<26}: {v}")
    print()


# ── 케이스 1: 정상 실행 ───────────────────────────────────────────────────────

print("\n[ 케이스 1 ] 정상 실행 (qa_passed=True)")
app_normal = build_graph().compile()
result1 = app_normal.invoke(make_initial_state())
print_result("케이스 1 최종 State", result1)

assert result1["anomaly_flag"]   == True,       "anomaly_flag 오류"
assert result1["anomaly_type"]   == "cost_spike","anomaly_type 오류"
assert result1["selected_action"]== "ScaleDown", "selected_action 오류"
assert result1["qa_passed"]      == True,        "qa_passed 오류"
assert result1["rollback_count"] == 0,           "rollback_count 오류"
assert "파이프라인 완료" in result1["log_entries"][-1], "log_entries 오류"
print("  ✓ 케이스 1 통과")


# ── 케이스 2: 롤백 루프 ───────────────────────────────────────────────────────

print("\n[ 케이스 2 ] 롤백 루프 (qa_passed=False 강제)")
app_rollback = build_graph(qa_node_override=qa_node_fail).compile()
result2 = app_rollback.invoke(make_initial_state())
print_result("케이스 2 최종 State", result2)

assert result2["qa_passed"]      == False, "qa_passed 오류"
assert result2["rollback_count"] == 2,     f"rollback_count가 2여야 하는데 {result2['rollback_count']}"
assert "파이프라인 완료" in result2["log_entries"][-1], "log_entries 오류"
print("  ✓ 케이스 2 통과 — 롤백 루프 정확히 2회 후 종료")


# ── 케이스 3: anomaly 없음 ────────────────────────────────────────────────────

print("\n[ 케이스 3 ] anomaly 없음 (detection에서 조기 종료)")

# detection_node가 anomaly_flag=False를 반환하도록 임시 오버라이드
from schema.state import PipelineState

def detection_no_anomaly(state: PipelineState) -> PipelineState:
    state["anomaly_flag"]          = False
    state["anomaly_score_zscore"]  = None
    state["anomaly_score_iforest"] = None
    state["triggered_metrics"]     = []
    return state

from langgraph.graph import StateGraph, END
from pipeline.dummy_nodes import classification_node, decision_node, action_node, qa_node, logging_node
from pipeline.graph import detection_router, qa_router

g = StateGraph(PipelineState)
g.add_node("detection",      detection_no_anomaly)
g.add_node("classification", classification_node)
g.add_node("decision",       decision_node)
g.add_node("action",         action_node)
g.add_node("qa",             qa_node)
g.add_node("logging",        logging_node)
g.set_entry_point("detection")
g.add_conditional_edges("detection", detection_router,
                         {"classification": "classification", "logging": "logging"})
g.add_edge("classification", "decision")
g.add_edge("decision",       "action")
g.add_edge("action",         "qa")
g.add_conditional_edges("qa", qa_router, {"action": "action", "logging": "logging"})
g.add_edge("logging", END)
app_no_anomaly = g.compile()

result3 = app_no_anomaly.invoke(make_initial_state())
print_result("케이스 3 최종 State", result3)

assert result3["anomaly_flag"]    == False,  "anomaly_flag 오류"
assert result3["anomaly_type"]    is None,   "anomaly_type은 None이어야 함 (classification 미실행)"
assert result3["selected_action"] is None,   "selected_action은 None이어야 함"
assert "파이프라인 완료" in result3["log_entries"][-1], "log_entries 오류"
print("  ✓ 케이스 3 통과 — classification/decision/action/qa 건너뜀")

print("\n\n✓ 전체 3개 케이스 모두 통과\n")
