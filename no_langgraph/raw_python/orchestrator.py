"""
no_langgraph/raw_python/orchestrator.py

이 파일은 A/B/C 중 누구의 담당도 아니다. 6개 함수를 "어떤 순서로, 어떤
조건으로 다시 부를지" 결정하려면 A의 anomaly_flag, B의 qa_passed,
C의 action_result/rollback_action 시그니처를 전부 알아야 한다.

즉 이 파일을 쓰는 사람(보통 마지막에 합치는 통합 담당자)은 A/B/C 세 사람의
인터페이스를 전부 파악해야 하는 반면, A/B/C는 이 파일이 자기 함수를 정확히
언제/몇 번 부르는지 몰라도 자기 파트를 개발할 수 있다.

── 승인 대기(신규) 때문에 구조 자체가 바뀐 부분 ─────────────────────────────
원래는 while 루프 하나로 순서를 표현했다. 그런데 "승인 대기 중 프로세스가
꺼졌다 켜져도 이어서 실행돼야 한다"는 요구사항이 생기면, Python 함수 콜스택
자체는 프로세스가 죽으면 사라지므로 더 이상 while 루프로 표현할 수 없다.
"지금 어느 단계까지 왔는지"를 문자열(step)로 외부화하고, 그 문자열을 보고
분기하는 명시적 디스패처로 재작성해야 한다 (아래 STEP_ORDER / run_pipeline).
이게 while 루프 버전보다 코드가 훨씬 길어진 이유다.
"""

from no_langgraph.raw_python.state import PipelineState
from no_langgraph.raw_python.detection import run_detection
from no_langgraph.raw_python.classification_qa import run_classification, run_qa
from no_langgraph.raw_python.decision_action import run_decision, run_action, rollback_action
from no_langgraph.raw_python.logging_stage import run_logging
from no_langgraph.raw_python.persistence import save_checkpoint, load_checkpoint, delete_checkpoint
from no_langgraph.raw_python.retry_policy import retry_with_backoff

MAX_RETRY = 2

# action_node를 직접 수정하지 않고, 조립 계층(orchestrator)에서만 "일시적
# 네트워크 오류 시 자동 재시도"를 씌운다 — decision_action.py(개발자 C 담당)는
# 이 요구사항의 존재 자체를 몰라도 된다.
_run_action_with_retry = retry_with_backoff(max_attempts=3)(run_action)


def run_pipeline(state: PipelineState, thread_id: str, start_step: str = "detection") -> tuple[PipelineState, str | None]:
    """
    start_step부터 실행. approval_gate에 도달하면 checkpoint를 저장하고
    "next_step"과 함께 즉시 리턴한다(=프로세스가 여기서 멈춰도 안전).
    승인 없이 통과되는 경로는 원래 while 루프와 동일하게 끝까지 실행된다.

    반환값: (state, next_step). next_step이 None이면 파이프라인이 끝난 것,
    아니면 그 지점에서 멈춰있다는 뜻 (resume_pipeline으로 재개).
    """
    step = start_step

    while step is not None:
        if step == "detection":
            state = run_detection(state)
            step = "classification" if state.anomaly_flag else "logging"

        elif step == "classification":
            state = run_classification(state)
            step = "decision"

        elif step == "decision":
            state = run_decision(state)
            step = "approval_gate" if state.requires_approval else "action"

        elif step == "approval_gate":
            state.approval_status = "pending"
            save_checkpoint(thread_id, state, next_step="action")
            return state, "action"  # 여기서 실제로 멈춤 — 프로세스가 죽어도 안전

        elif step == "action":
            state = _run_action_with_retry(state)
            step = "qa"

        elif step == "qa":
            state = run_qa(state)
            if state.qa_passed:
                step = "logging"
            elif state.rollback_count < MAX_RETRY:
                rollback_action(state)
                state.rollback_count += 1
                step = "action"
            else:
                step = "logging"

        elif step == "logging":
            state = run_logging(state)
            step = None

        else:
            raise ValueError(f"알 수 없는 step: {step}")

    delete_checkpoint(thread_id)
    return state, None


def resume_pipeline(thread_id: str) -> tuple[PipelineState, str | None]:
    """저장된 checkpoint를 읽어서 이어서 실행 (승인 완료 후 호출)."""
    state, next_step = load_checkpoint(thread_id)
    state.approval_status = "approved"
    return run_pipeline(state, thread_id, start_step=next_step)
