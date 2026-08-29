"""
Approval Gate
=============
Decision Agent와 Action Agent 사이에 끼워 넣는 승인 대기 노드.

기존에는 action_agent.py가 requires_approval=True인 경우 실제 액션을 실행하지 않고
action_result={"status": "pending_approval"}만 남긴 채 파이프라인이 그냥 끝까지
진행됐다 (state에 "보류" 플래그만 남기는 방식으로, 관리자가 승인해도 재개할 방법이 없었음).

이 노드는 LangGraph의 interrupt()로 그래프 실행 자체를 물리적으로 멈춘다.
checkpointer가 설정된 그래프에서만 의미가 있으므로, 이 노드는 build_graph()의 기본
경로(app)에는 들어가지 않고 with_approval_gate=True로 명시했을 때만 그래프에 추가된다. 
이렇게 분리하여 checkpointer 없이 개별 노드/그래프를 직접
호출하는 기존 playground 테스트들이 영향을 받지 않도록 하기 위함이다.

재개 방법 (admin API가 사용할 형태):
    config = {"configurable": {"thread_id": "<실행 단위 ID>"}}
    graph.invoke(Command(resume={"approved": True}), config)   # 승인
    graph.invoke(Command(resume={"approved": False}), config)  # 거부
"""

from __future__ import annotations

from langgraph.types import interrupt

from schema.state import PipelineState


def approval_gate_node(state: PipelineState) -> PipelineState:
    """
    입력: state["requires_approval"], state["selected_action"], state["risk_level"],
          state["decision_reasoning"], state["resource_id"], state["resource_type"]
    출력:
      - requires_approval=False였다면 그대로 통과 (interrupt 호출 없음)
      - requires_approval=True였다면 interrupt()로 멈췄다가, 재개 시:
          승인 -> requires_approval=False로 바꿔서 action_node가 정상 실행하도록 함
          거부 -> selected_action="NoAction"으로 바꿔서 action_node가 스킵하도록 함
    """
    if not state.get("requires_approval"):
        return state

    decision = interrupt(
        {
            "resource_id": state["resource_id"],
            "resource_type": state["resource_type"],
            "selected_action": state["selected_action"],
            "risk_level": state["risk_level"],
            "decision_reasoning": state.get("decision_reasoning"),
            "candidate_actions": state.get("candidate_actions"),
            "decision_pseudo_code": state.get("decision_pseudo_code"),
        }
    )

    if decision.get("approved", False):
        state["requires_approval"] = False
    else:
        state["selected_action"] = "NoAction"
        state["requires_approval"] = False
        state["decision_reasoning"] = (
            (state.get("decision_reasoning") or "") + " [관리자 거부로 NoAction 처리됨]"
        )

    return state
