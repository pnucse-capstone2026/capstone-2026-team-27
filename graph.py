"""
LangGraph 파이프라인 그래프 조립.

조건부 엣지:
  detection → anomaly_flag 기준으로 분기
    True  → classification
    False → logging  (정상 판정, 조기 종료)

  qa → qa_passed + rollback_count 기준으로 분기
    통과              → logging
    실패 + 재시도 가능 → action  (롤백 후 재시도)
    실패 + 2회 초과    → logging (현재 상태 유지, 관리자 알림은 logging_node에서)
"""

from langgraph.graph import StateGraph, END
from schema.state import PipelineState
from pipeline.dummy_nodes import (
    detection_node,
    classification_node,
    decision_node,
    action_node,
    qa_node,
    logging_node,
)


def detection_router(state: PipelineState) -> str:
    return "classification" if state["anomaly_flag"] else "logging"


def qa_router(state: PipelineState) -> str:
    if state["qa_passed"]:
        return "logging"
    elif state["rollback_count"] < 2:
        return "action"   # 롤백 후 재시도
    else:
        return "logging"  # 2회 초과 → 현재 상태 유지


def build_graph(qa_node_override=None) -> StateGraph:
    """
    qa_node_override: 테스트 시 qa_node 대신 다른 함수 주입 가능.
    예) build_graph(qa_node_override=qa_node_fail)
    """
    _qa_node = qa_node_override or qa_node

    graph = StateGraph(PipelineState)

    graph.add_node("detection",      detection_node)
    graph.add_node("classification", classification_node)
    graph.add_node("decision",       decision_node)
    graph.add_node("action",         action_node)
    graph.add_node("qa",             _qa_node)
    graph.add_node("logging",        logging_node)

    graph.set_entry_point("detection")

    graph.add_conditional_edges(
        "detection",
        detection_router,
        {"classification": "classification", "logging": "logging"},
    )
    graph.add_edge("classification", "decision")
    graph.add_edge("decision",       "action")
    graph.add_edge("action",         "qa")
    graph.add_conditional_edges(
        "qa",
        qa_router,
        {"action": "action", "logging": "logging"},
    )
    graph.add_edge("logging", END)

    return graph


# 기본 앱 (정상 qa_node 사용)
app = build_graph().compile()
