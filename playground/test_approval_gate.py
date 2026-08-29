"""
playground/test_approval_gate.py

Checkpointer + interrupt 기반 승인 대기 메커니즘이 실제로 동작하는지 확인하는 스크립트.
전체 파이프라인(detection~logging)을 다 태우면 ML 탐지 결과에 따라 흔들리므로,
approval_gate -> action 두 노드만 있는 최소 그래프로 메커니즘 자체만 검증한다.
(approval_gate_node/action_node는 pipeline/graph.py의 build_approval_graph()가 쓰는
것과 동일한 함수라, 여기서 검증되면 실제 파이프라인에서도 같은 방식으로 동작한다.)

MemorySaver를 쓰기 때문에 Postgres 없이 바로 실행 가능하다. 실제 배포에서는
pipeline/checkpointer.py의 PostgresSaver로 교체하면 된다 (사용법은 그 파일 docstring 참고).

resource_type="S3"로 고정한 이유: action_agent.execute_action()이 S3는 아직 구현하지
않아서 승인해도 boto3를 실제로 호출하지 않는다 (not_implemented로 안전하게 끝남).
즉 이 테스트는 실제 AWS 리소스를 절대 건드리지 않는다.

[실행 방법]
  프로젝트 루트에서: python playground/test_approval_gate.py
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from langgraph.types import Command

from schema.state import PipelineState
from pipeline.approval_gate import approval_gate_node
from pipeline.action_agent import action_node


def build_mini_approval_graph():
    """approval_gate -> action 만 있는 최소 그래프. 메커니즘 검증용."""
    graph = StateGraph(PipelineState)
    graph.add_node("approval_gate", approval_gate_node)
    graph.add_node("action", action_node)
    graph.set_entry_point("approval_gate")
    graph.add_edge("approval_gate", "action")
    graph.add_edge("action", END)
    return graph.compile(checkpointer=MemorySaver())


def make_state(action: str) -> dict:
    return {
        "resource_id": "test-bucket",
        "resource_type": "S3",
        "selected_action": action,
        "risk_level": "HIGH",
        "requires_approval": True,
        "decision_reasoning": "테스트용 더미 판단 근거",
        "candidate_actions": [],
        "target_instance_type": None,
    }


def run_case(label: str, action: str, approve: bool) -> None:
    print("\n" + "=" * 70)
    print(f"[{label}] selected_action={action}, 관리자 결정: {'승인' if approve else '거부'}")
    print("=" * 70)

    app = build_mini_approval_graph()
    thread_id = f"test-{label}"
    config = {"configurable": {"thread_id": thread_id}}

    result = app.invoke(make_state(action), config)
    interrupts = result.get("__interrupt__")
    assert interrupts, "interrupt()로 멈췄어야 하는데 안 멈췄음 (checkpointer/interrupt 설정 확인)"
    print(f"  1) interrupt로 정지됨. 관리자에게 보여줄 정보: {interrupts[0].value}")

    resumed = app.invoke(Command(resume={"approved": approve}), config)
    print(f"  2) 재개(resume) 완료.")
    print(f"     selected_action  = {resumed['selected_action']}")
    print(f"     action_executed  = {resumed['action_executed']}")
    print(f"     action_result    = {resumed['action_result']}")

    if approve:
        assert resumed["selected_action"] == action, "승인 시 원래 액션이 유지돼야 함"
    else:
        assert resumed["selected_action"] == "NoAction", "거부 시 NoAction으로 바뀌어야 함"
    print("  [PASS]")


if __name__ == "__main__":
    run_case("승인", action="Stop", approve=True)
    run_case("거부", action="Stop", approve=False)
    print("\n모든 케이스 통과.")
