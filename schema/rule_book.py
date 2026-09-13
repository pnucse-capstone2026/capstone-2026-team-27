"""
Rule Book 스키마 정의
--------------------
Classification Agent와 QA Agent가 참조하는 중앙화된 규칙 저장소 스키마
"""

from typing import TypedDict, Optional, Literal


class TimeWindow(TypedDict):
    """시간대 조건"""
    start_hour: int   # 0-23
    end_hour: int     # 0-23
    timezone: str     # "Asia/Seoul"
    days: list[str]   # ["MON", "TUE", ...] 또는 ["*"]


class RuleConditions(TypedDict, total=False):
    """규칙 조건"""
    triggered_metrics: Optional[list[str]]  # 매칭할 지표들
    metric_thresholds: Optional[dict]       # {"cpu_utilization": {"op": "<", "value": 5}}
    sustained_fraction: Optional[dict]      # {"metric": ..., "factor": 2.0, "min_fraction": 0.6, ...}
    time_window: Optional[TimeWindow]       # 시간대 조건
    consecutive_count: Optional[int]        # 연속 탐지 횟수
    action_executed: Optional[list[str]]    # QA용: 실행된 액션 매칭 (예: ["NoAction", null])
    skip_if_whitelisted: Optional[bool]     # True면 화이트리스트 매칭 시 이 규칙 자체가 불일치 처리됨
                                             # (이벤트 기간 등록 등, "예외적으로 정상"인 기간을 배제하는 용도)


class RuleResult(TypedDict, total=False):
    """규칙 결과"""
    # Classification용
    anomaly_type: Optional[str]
    interim_action: Optional[str]

    # QA용
    force_pass: Optional[bool]
    force_fail: Optional[bool]

    # 공통
    reasoning_template: str


class Rule(TypedDict):
    """규칙 정의"""
    rule_id: str                    # "CLF-001", "QA-001" 등
    rule_type: Literal["classification", "qa"]
    resource_types: list[str]       # ["EC2", "Lambda"] 또는 ["*"]

    # 조건부
    conditions: RuleConditions

    # 결과
    result: RuleResult

    # 메타데이터
    priority: int                   # 낮을수록 먼저 평가
    enabled: bool
    created_at: str                 # ISO 8601
    updated_at: str
    author: str
    description: str
    rationale: str                  # 규칙 근거


class WhitelistEntry(TypedDict):
    """화이트리스트 엔트리

    이벤트 기간(예: 세일 기간) 등록 용도로도 쓴다 — effective_from~expires_at 사이만
    유효하게 해서, "이 기간의 트래픽/용량 증가는 예외적으로 정상"이라고 등록할 수 있다
    (2026-09-11, EDoS 오탐 방지 목적으로 확장)."""
    entry_id: str
    resource_id: str              # 특정 리소스 ID 또는 패턴 ("i-*", "arn:aws:lambda:*:my-func")
    resource_type: Optional[str]  # None이면 모든 타입
    reason: str
    category: Optional[str]       # "event_period"면 EDoS 판정 예외로 취급됨. "recurring_hours"면
                                   # 매일 반복되는 시간대(야간 등) 예외로 취급됨. None/기타 값은
                                   # 기존처럼 일반 화이트리스트(비용 모니터링/QA 제외 등)로만 동작 —
                                   # 이벤트/시간대와 무관한 화이트리스트가 EDoS 판정에 영향 주지 않게 구분
    effective_from: Optional[str]  # None이면 created_at부터 즉시 유효 (이벤트 기간 등록 시 시작일 지정)
    expires_at: Optional[str]     # None이면 영구
    # 2026-09-13 추가: category="recurring_hours"일 때만 쓰는 "매일 반복되는 시간대"
    # 필드 - effective_from/expires_at(날짜 범위)과 별개로, 매일 특정 시:분 사이만
    # 예외 처리한다(예: 매일 22:00~06:00 야간은 EDoS 탐지 제외). daily_end_hour가
    # daily_start_hour보다 작으면 자정을 넘기는 것으로 취급(22시~6시 같은 경우).
    daily_start_hour: Optional[int]  # 0~23, UTC 기준
    daily_end_hour: Optional[int]    # 0~23, UTC 기준
    created_at: str
    created_by: str
