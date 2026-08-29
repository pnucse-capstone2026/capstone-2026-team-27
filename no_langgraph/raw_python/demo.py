"""
no_langgraph/raw_python/demo.py

4가지 시나리오로 순수 파이썬 버전을 실제로 돌려서 재시도-롤백 흐름과
승인 대기(신규) 흐름이 의도대로 동작하는지 확인한다.

실행: 프로젝트 루트에서 `python -m no_langgraph.raw_python.demo`
"""

import uuid

from no_langgraph.raw_python.state import PipelineState
from no_langgraph.raw_python.orchestrator import run_pipeline, resume_pipeline


def _run(resource_id: str, requires_approval: bool = False) -> PipelineState:
    state = PipelineState(
        resource_id=resource_id,
        resource_type="EC2",
        raw_metrics={"cost": [0.5] * 27 + [3.0, 3.2, 3.5]},
        requires_approval=requires_approval,
    )
    print(f"\n{'='*70}\n실행: {resource_id}\n{'='*70}")
    thread_id = f"demo-{uuid.uuid4()}"
    state, next_step = run_pipeline(state, thread_id=thread_id)
    return state, thread_id, next_step


def main():
    r1, _, next1 = _run("i-normal")
    assert next1 is None
    assert r1.rollback_count == 0 and r1.qa_passed is True

    r2, _, next2 = _run("i-RECOVER1")
    assert next2 is None
    assert r2.rollback_count == 1 and r2.qa_passed is True

    r3, _, next3 = _run("i-FAIL")
    assert next3 is None
    assert r3.rollback_count == 2 and r3.qa_passed is False

    print("\n모든 재시도 시나리오 통과 (rollback_count: 0 / 1 / 2)")

    # ── 신규: 승인 대기 시나리오 ────────────────────────────────────────────
    r4, thread_id, next4 = _run("i-needs-approval", requires_approval=True)
    assert next4 == "action"          # 여기서 멈춰있어야 함
    assert r4.action_executed is None  # 아직 실행 안 됐어야 함
    assert r4.approval_status == "pending"

    print(f"\n[승인 대기] thread_id={thread_id} 에서 멈춤 (approval_status=pending)")

    # 승인 후 재개 (실제로는 별도 프로세스/요청에서 호출됨)
    r4_resumed, next4_resumed = resume_pipeline(thread_id)
    assert next4_resumed is None
    assert r4_resumed.action_executed == "Stop"  # 이제 진짜로 실행됨
    assert r4_resumed.approval_status == "approved"

    print("[승인 대기] 재개 후 정상 완료 (action_executed=Stop)")
    print("\n모든 시나리오 통과")


if __name__ == "__main__":
    main()
