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

import time

from langgraph.graph import StateGraph, END
from schema.state import PipelineState

# 실제 agent 노드들 import
from pipeline.detection_agent import detection_node
from pipeline.classification_agent import classification_node
from pipeline.decision_agent import decision_node
from pipeline.action_agent import action_node
from pipeline.approval_gate import approval_gate_node
from pipeline.notify_gate import notify_gate_node
from pipeline.QA_agent import qa_node
from pipeline.logging_agent import logging_node


def _timed(step_name: str, node_fn):
    """노드 함수를 감싸서 실행 시간(ms)을 state["step_timings"][step_name]에 기록한다.

    agent_steps.duration_ms가 지금까지 항상 NULL이었던 것(logging_agent.py 참고)을
    채우기 위함 — 각 agent 파일(detection/classification/decision/action/QA)을
    직접 건드리지 않고 그래프 조립 단계에서만 계측하도록 그래프 레벨에 wrapping했다.
    롤백 루프로 같은 노드를 여러 번 거치면 마지막 실행 시간으로 덮어써진다(로깅은
    실행 종료 시점 상태 한 번만 스냅샷하므로 이걸로 충분하다).
    """
    def wrapped(state: PipelineState) -> PipelineState:
        start = time.perf_counter()
        new_state = node_fn(state)
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        timings = dict(new_state.get("step_timings") or {})
        timings[step_name] = elapsed_ms
        new_state["step_timings"] = timings
        return new_state
    return wrapped


def detection_router(state: PipelineState) -> str:
    return "classification" if state["anomaly_flag"] else "logging"


def qa_router(state: PipelineState) -> str:
    if state["qa_passed"]:
        return "logging"
    elif state["rollback_count"] < 2:
        return "action"   # 롤백 후 재시도
    else:
        return "logging"  # 2회 초과 → 현재 상태 유지


def build_graph(qa_node_override=None, with_approval_gate: bool = False) -> StateGraph:
    """
    qa_node_override: 테스트 시 qa_node 대신 다른 함수 주입 가능.
    예) build_graph(qa_node_override=qa_node_fail)

    with_approval_gate: True면 decision과 action 사이에 approval_gate 노드를 끼워
    넣는다. 이 노드는 requires_approval=True일 때 interrupt()로 그래프 실행을 멈추므로,
    checkpointer 없이 컴파일하면 매번 완료 없이 끊긴 채로 반환된다 — 반드시
    build_approval_graph()를 통해서만 True로 써야 한다. 기본값 False는 기존 playground
    테스트들(checkpointer 없이 app.invoke()/개별 노드 호출)의 동작을 그대로 유지하기
    위함이다.
    """
    _qa_node = qa_node_override or qa_node

    graph = StateGraph(PipelineState)

    graph.add_node("detection",      _timed("detection", detection_node))
    graph.add_node("classification", _timed("classification", classification_node))
    graph.add_node("decision",       _timed("decision", decision_node))
    graph.add_node("action",         _timed("action", action_node))
    graph.add_node("qa",             _timed("qa", _qa_node))
    graph.add_node("logging",        logging_node)

    graph.set_entry_point("detection")

    graph.add_conditional_edges(
        "detection",
        detection_router,
        {"classification": "classification", "logging": "logging"},
    )
    graph.add_edge("classification", "decision")

    if with_approval_gate:
        graph.add_node("notify_gate",   notify_gate_node)
        graph.add_node("approval_gate", approval_gate_node)
        graph.add_edge("decision",       "notify_gate")
        graph.add_edge("notify_gate",    "approval_gate")
        graph.add_edge("approval_gate",  "action")
    else:
        graph.add_edge("decision", "action")

    graph.add_edge("action",         "qa")
    graph.add_conditional_edges(
        "qa",
        qa_router,
        {"action": "action", "logging": "logging"},
    )
    graph.add_edge("logging", END)

    return graph


# 기본 앱 (정상 qa_node 사용, approval_gate 없음 — 기존 playground 테스트 호환용)
app = build_graph().compile()


def build_approval_graph(checkpointer, qa_node_override=None):
    """
    관리자 승인 대기가 실제로 동작하는 그래프. checkpointer(PostgresSaver 등)를
    반드시 넘겨야 한다. (interrupt로 멈춘 thread를 나중에
    Command로 재개하기 위함이다.)
    """
    return build_graph(qa_node_override, with_approval_gate=True).compile(checkpointer=checkpointer)
