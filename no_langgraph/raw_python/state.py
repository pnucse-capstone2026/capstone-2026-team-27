"""
no_langgraph/raw_python/state.py

3명(A: Detection, B: Classification+QA, C: Decision+Action)이 동시에 개발할 때
가장 먼저 정해야 하는 건 "6개 함수 사이에 데이터를 어떻게 넘길 것인가"다.
검토한 선택지 3가지:

  1) 그냥 dict를 넘긴다
     장점: 아무 파일도 import 안 해도 되고, 각자 자기가 쓸 키만 알면 됨.
     단점: 오타(state["qa_passd"])가 나도 런타임까지 아무도 못 잡는다.
           "이 시점에 어떤 키가 반드시 채워져 있어야 하는지"가 코드 어디에도
           안 적혀 있어서, 다른 사람 파트가 뭘 채워주는지 매번 물어봐야 한다.

  2) 전역 변수(모듈 레벨 dict/객체)를 공유한다
     장점: 함수 시그니처가 짧아진다.
     단점: 테스트 격리가 안 되고, 리소스 여러 개를 동시에 처리해야 하는
           순간 바로 깨진다. 3명이 각자 로컬에서 테스트할 때 전역 상태를
           누가 언제 초기화하는지도 다시 합의해야 해서 오히려 더 번거롭다.

  3) 공유 dataclass (여기서 선택)
     장점: 필드 목록이 코드 한 곳(PipelineState)에 명시적으로 존재해서
           "이 시점에 어떤 필드가 있어야 하는지"를 IDE/타입체커가 확인해준다.
     단점: 3명 다 이 파일을 먼저 보고 시작해야 한다. 다만 "어떤 데이터를
           주고받을지" 합의는 dict를 쓰든 안 쓰든 결국 필요한 비용이고,
           dataclass는 그 합의를 코드로 강제할 뿐이다.

  이 셋 중 3)을 골랐다. 이유는 DESIGN_NOTES.md의 "상태를 넘길 때 발생하는
  문제" 항목에 더 자세히 적었다.
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class PipelineState:
    # ── 입력 ──────────────────────────────────────────────────────────────
    resource_id: str
    resource_type: str
    raw_metrics: dict

    # ── Step 1: Detection (담당: 개발자 A) ──────────────────────────────────
    anomaly_flag: bool = False
    anomaly_score_zscore: Optional[float] = None
    triggered_metrics: list[str] = field(default_factory=list)

    # ── Step 2: Classification (담당: 개발자 B) ─────────────────────────────
    anomaly_type: Optional[str] = None
    classification_reasoning: Optional[str] = None

    # ── Step 3: Decision (담당: 개발자 C) ───────────────────────────────────
    selected_action: Optional[str] = None
    risk_level: Optional[str] = None
    requires_approval: bool = False

    # ── 승인 대기 (신규: 프로세스 재시작에도 살아남는 재개 기능) ────────────────
    approval_status: Optional[str] = None  # None(대기없음) / "pending" / "approved"

    # ── Step 4: Action (담당: 개발자 C) ─────────────────────────────────────
    pre_action_snapshot: Optional[dict] = None
    action_executed: Optional[str] = None
    action_result: Optional[dict] = None

    # ── Step 5: QA (담당: 개발자 B) ──────────────────────────────────────────
    qa_passed: Optional[bool] = None
    rollback_count: int = 0

    # ── Step 6: Logging (담당: 미배정 — 통합 담당자가 떠맡게 됨) ─────────────
    log_entries: list[str] = field(default_factory=list)
