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
    time_window: Optional[TimeWindow]       # 시간대 조건
    consecutive_count: Optional[int]        # 연속 탐지 횟수
    action_executed: Optional[list[str]]    # QA용: 실행된 액션 매칭 (예: ["NoAction", null])


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
    """화이트리스트 엔트리"""
    entry_id: str
    resource_id: str              # 특정 리소스 ID 또는 패턴 ("i-*", "arn:aws:lambda:*:my-func")
    resource_type: Optional[str]  # None이면 모든 타입
    reason: str
    expires_at: Optional[str]     # None이면 영구
    created_at: str
    created_by: str
