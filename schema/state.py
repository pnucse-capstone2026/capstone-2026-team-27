"""
cloud-anomaly-agent 전체 파이프라인 공유 State 스키마
"""

from typing import TypedDict, Optional, Literal


# ── raw_metrics 리소스별 구조 ─────────────────────────────────────────────────
# 각 list[float]는 슬라이딩 윈도우 30개 포인트 (CloudWatch 1분 단위)
# cost만 Cost Explorer 1시간 단위

class EC2Metrics(TypedDict):
    cpu_utilization: list[float]   # %
    network_in:      list[float]   # bytes
    network_out:     list[float]   # bytes
    cost:            list[float]   # USD

class LambdaMetrics(TypedDict):
    invocation_count: list[float]  # 횟수
    error_count:      list[float]  # 횟수
    duration_avg:     list[float]  # ms
    cost:             list[float]  # USD

class S3Metrics(TypedDict):
    number_of_requests: list[float]  # 횟수
    bytes_downloaded:   list[float]  # bytes
    cost:               list[float]  # USD

class RDSMetrics(TypedDict):
    cpu_utilization:      list[float]  # %
    database_connections: list[float]  # 연결 수
    read_iops:            list[float]  # IOPS
    write_iops:           list[float]  # IOPS
    cost:                 list[float]  # USD

class AutoScalingMetrics(TypedDict):
    group_desired_capacity:    list[float]  # 목표 인스턴스 수
    group_in_service_instances: list[float] # 실행 중 인스턴스 수
    cost:                      list[float]  # USD


# ── pre_action_snapshot 리소스별 구조 ────────────────────────────────────────
# Action Agent가 액션 실행 전 반드시 저장. QA Agent 롤백 시 이 값으로 복원.
# S3: boto3 get_bucket_policy()["Policy"]는 JSON 문자열로 반환되므로
#     저장 시 json.loads() 후 dict로 변환 필수. 롤백 시 json.dumps()로 복원.

class EC2Snapshot(TypedDict):
    instance_type:     str        # 예: "t3.medium"
    state:             str        # 예: "running"
    security_group_ids: list[str] # 예: ["sg-0abc123"]

class LambdaSnapshot(TypedDict):
    reserved_concurrency: int     # 설정 없으면 -1

class S3BucketPolicy(TypedDict):
    Version:   str
    Statement: list[dict]

class S3PublicAccessBlock(TypedDict):
    BlockPublicAcls:       bool
    IgnorePublicAcls:      bool
    BlockPublicPolicy:     bool
    RestrictPublicBuckets: bool

class S3Snapshot(TypedDict):
    bucket_policy:        S3BucketPolicy       # json.loads() 후 저장
    public_access_block:  S3PublicAccessBlock

class RDSSnapshot(TypedDict):
    instance_class: str   # 예: "db.t3.medium"
    multi_az:       bool

class AutoScalingSnapshot(TypedDict):
    max_size:         int
    desired_capacity: int


# ── 후보 액션 단위 구조 (Decision Agent가 생성) ──────────────────────────────
# anomaly_type별 허용 액션 집합 (이 외의 액션은 NoAction으로 대체):
#   cost_inefficiency → NoAction, Stop, Stop+Schedule, Resize
#   cost_spike        → NoAction, Throttle, Block, ScaleDown
#   risk_security     → NoAction, Block, ScaleDown

class CandidateAction(TypedDict):
    action:          Literal["NoAction", "Stop", "Stop+Schedule", "Resize",
                             "Throttle", "Block", "ScaleDown"]
    saving_rate:     float   # 비용 절감 효과  [0.0, 1.0] (정규화된 비율)
    impact_score:    float   # 서비스 영향도   [0.0, 1.0]  낮을수록 좋음
    stability_score: float   # 시스템 안정성   [0.0, 1.0]
    score:           float   # 0.5×saving - 0.3×impact + 0.2×stability
    estimated_saving_usd: float  # saving_rate 산출에 쓰인 절감 예상액(시간당 USD).
                                  # 결정론적으로 계산 불가능해 LLM 추정치를 그대로
                                  # saving_rate로 쓴 경우 0.0 (근거 없는 금액을
                                  # 만들어내지 않기 위한 안전장치, decision_agent.py 참고)


# ── SLA 검증 결과 단위 구조 (QA Agent가 생성) ────────────────────────────────

class SlaCheckResult(TypedDict):
    cpu_ok:          bool
    cost_ok:         bool
    availability_ok: bool
    detail:          str    # 실패 시 상세 사유


# ── risk_level 판단 룰 (Decision Agent가 적용) ───────────────────────────────
# 1단계: anomaly_type 기반 기본값
#   cost_inefficiency → LOW / cost_spike → MED / risk_security → HIGH
# 2단계: selected_action으로 상향 조정 (하향 없음)
#   Stop+Schedule, Resize → 최소 MED / Block → 최소 HIGH

ANOMALY_TYPE_DEFAULT_RISK: dict[str, str] = {
    "cost_inefficiency": "LOW",
    "cost_spike":        "MED",
    "risk_security":     "HIGH",
}

ACTION_RISK_FLOOR: dict[str, str] = {
    "Stop+Schedule": "MED",
    "Resize":        "MED",
    "Block":         "HIGH",
}

ALLOWED_ACTIONS: dict[str, list[str]] = {
    "cost_inefficiency": ["NoAction", "Stop", "Stop+Schedule", "Resize"],
    "cost_spike":        ["NoAction", "Throttle", "Block", "ScaleDown"],
    "risk_security":     ["NoAction", "Block", "ScaleDown"],
}

RISK_ORDER: dict[str, int] = {"LOW": 0, "MED": 1, "HIGH": 2}


def resolve_risk_level(anomaly_type: str, selected_action: str) -> Literal["LOW", "MED", "HIGH"]:
    """anomaly_type 기본값에서 시작해 selected_action으로 상향 조정."""
    base  = ANOMALY_TYPE_DEFAULT_RISK.get(anomaly_type, "HIGH")
    floor = ACTION_RISK_FLOOR.get(selected_action, "LOW")
    result = base if RISK_ORDER[base] >= RISK_ORDER[floor] else floor
    return result  # type: ignore[return-value]


# ── 메인 파이프라인 State ────────────────────────────────────────────────────

class PipelineState(TypedDict):

    # ── Step 0: 수집된 원본 데이터 ───────────────────────────────────────────
    resource_id:   str
    resource_type: Literal["EC2", "Lambda", "S3", "RDS", "AutoScaling"]
    raw_metrics:   EC2Metrics | LambdaMetrics | S3Metrics | RDSMetrics | AutoScalingMetrics
    timestamp:     str  # ISO 8601

    # ── Step 1: Detection Agent ───────────────────────────────────────────────
    anomaly_flag:          bool
    anomaly_score_zscore:  Optional[float]
    anomaly_score_iforest: Optional[float]
    triggered_metrics:     list[str]

    # ── Step 2: Classification Agent ─────────────────────────────────────────
    anomaly_type: Optional[Literal["cost_inefficiency", "cost_spike", "risk_security"]]
    classification_reasoning: Optional[str]
    interim_action_taken:     Optional[str]

    # ── Step 3: Decision Agent ────────────────────────────────────────────────
    candidate_actions:  list[CandidateAction]
    selected_action:    Optional[Literal["NoAction", "Stop", "Stop+Schedule",
                                         "Resize", "Throttle", "Block", "ScaleDown"]]
    risk_level:         Optional[Literal["LOW", "MED", "HIGH"]]
    requires_approval:  bool
    decision_reasoning: Optional[str]
    target_instance_type: Optional[str]  # Decision Agent가 Resize 선택 시 채움 (기본값 None)

    # ── Step 4: Action Agent ──────────────────────────────────────────────────
    pre_action_snapshot: Optional[
        EC2Snapshot | LambdaSnapshot | S3Snapshot | RDSSnapshot | AutoScalingSnapshot
    ]
    action_executed: Optional[str]
    action_result:   Optional[dict]

    # ── Step 5: QA Agent ──────────────────────────────────────────────────────
    qa_passed:        Optional[bool]
    sla_check_result: Optional[SlaCheckResult]
    rollback_count:   int  # 기본값 0, 최대 2

    # ── Step 6: Logging Agent ─────────────────────────────────────────────────
    log_entries: list[str]
