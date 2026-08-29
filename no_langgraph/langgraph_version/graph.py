"""
no_langgraph/langgraph_version/graph.py

raw_python 폴더의 6개 stage 함수(detection, classification, qa, decision,
action, rollback_action, logging)를 그대로 재사용하고, "언제/몇 번 부를지"만
LangGraph의 StateGraph + add_conditional_edges로 다시 조립한 버전.

즉 "무엇을 하는가"(각 stage 함수의 로직)는 raw_python과 완전히 동일하다.
"어떤 순서로 하는가"만 이 파일이 새로 정의한다 — 3단계 비교의 핵심 전제
(같은 부품, 다른 조립 방식).
"""

from langgraph.graph import StateGraph, END
from langgraph.types import RetryPolicy

from no_langgraph.raw_python.state import PipelineState
from no_langgraph.raw_python.detection import run_detection
from no_langgraph.raw_python.classification_qa import run_classification, run_qa
from no_langgraph.raw_python.decision_action import run_decision, run_action, rollback_action
from no_langgraph.raw_python.logging_stage import run_logging

# raw_python/orchestrator.py의 MAX_RETRY와 값은 같지만 코드로 연결돼 있지
# 않은, 독립된 두 번째 정의다 (COMPARISON.md 비교 4 참고).
MAX_RETRY = 2


# ── 노드: raw_python 함수를 그대로 호출하는 얇은 래퍼 ─────────────────────────
# (rollback_node만 "롤백 실행 + 카운트 증가"를 하나로 묶은 새 코드다.
#  raw_python에서는 이 두 줄이 orchestrator.py의 while 루프 안에 있었는데,
#  LangGraph는 그래프에 while문을 못 쓰므로 "노드 하나"로 표현해야 한다.)

def detection_node(state: PipelineState) -> PipelineState:
    return run_detection(state)

def classification_node(state: PipelineState) -> PipelineState:
    return run_classification(state)

def decision_node(state: PipelineState) -> PipelineState:
    return run_decision(state)

def action_node(state: PipelineState) -> PipelineState:
    return run_action(state)

def qa_node(state: PipelineState) -> PipelineState:
    return run_qa(state)

def rollback_node(state: PipelineState) -> PipelineState:
    rollback_action(state)
    state.rollback_count += 1
    return state

def logging_node(state: PipelineState) -> PipelineState:
    return run_logging(state)


# ── 라우터: raw_python의 if문 두 군데가 여기로 옮겨왔다 ───────────────────────

def detection_router(state: PipelineState) -> str:
    return "classification" if state.anomaly_flag else "logging"


def qa_router(state: PipelineState) -> str:
    if state.qa_passed:
        return "logging"
    if state.rollback_count < MAX_RETRY:
        return "rollback"
    return "logging"


def build_graph() -> StateGraph:
    graph = StateGraph(PipelineState)

    graph.add_node("detection",      detection_node)
    graph.add_node("classification", classification_node)
    graph.add_node("decision",       decision_node)
    graph.add_node("action",         action_node, retry_policy=RetryPolicy(max_attempts=3))
    graph.add_node("qa",             qa_node)
    graph.add_node("rollback",       rollback_node)
    graph.add_node("logging",        logging_node)

    graph.set_entry_point("detection")

    graph.add_conditional_edges(
        "detection", detection_router,
        {"classification": "classification", "logging": "logging"},
    )
    graph.add_edge("classification", "decision")
    graph.add_edge("decision", "action")
    graph.add_edge("action", "qa")
    graph.add_conditional_edges(
        "qa", qa_router,
        {"rollback": "rollback", "logging": "logging"},
    )
    graph.add_edge("rollback", "action")
    graph.add_edge("logging", END)

    return graph


app = build_graph().compile()
