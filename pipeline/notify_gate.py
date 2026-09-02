"""
Notify Gate
===========
decision과 approval_gate 사이에 끼워 넣는 알림 전용 노드.

approval_gate_node는 interrupt()로 그래프를 멈췄다가 나중에 Command로 재개하는데,
LangGraph의 interrupt() 특성상 재개 시 그 노드 함수가 처음부터 다시 실행된다.
그래서 만약 Slack 알림을 approval_gate_node 안, interrupt() 호출 전에 넣으면
"멈출 때 한 번 + 재개할 때 한 번" 총 두 번 전송돼버린다.

이 노드는 그 문제를 피하려고 완전히 분리했다 — interrupt 없이 그냥 통과하는
일반 노드라서, 한 번 실행되고 나면 그래프가 재개돼도 다시 실행되지 않는다.
"""

from __future__ import annotations

from schema.state import PipelineState
from utils.slack_notifier import send_slack_alert


def notify_gate_node(state: PipelineState) -> PipelineState:
    if not state.get("requires_approval"):
        return state

    send_slack_alert(
        f"[승인 대기] {state['resource_type']} · {state['resource_id']}\n"
        f"선택된 액션: {state.get('selected_action')} (위험도: {state.get('risk_level')})\n"
        f"판단 근거: {state.get('decision_reasoning')}\n"
        f"관리자 제어판의 '승인 대기' 탭에서 확인해주세요."
    )
    return state
