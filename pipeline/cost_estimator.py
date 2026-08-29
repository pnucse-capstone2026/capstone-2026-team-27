"""
pipeline/cost_estimator.py

Phase B: Cost Explorer 대신, CloudWatch 사용량 지표 + AWS 공개 단가로 비용을 직접 계산.
Cost Allocation Tag 활성화(최대 24시간 대기) 없이 즉시 사용 가능.

⚠️ 이건 "실제 청구액"이 아니라 추정치. Savings Plan 할인, 프리티어, 세부 리전
   요금 차이 등은 반영 안 됨 — 이상 탐지(평소 대비 몇 배 증가했는가) 목적으로는 충분.

단가 출처: AWS Pricing API(ap-northeast-2)로 직접 조회한 실제 값 (조회일 2026-08-25).
가격은 바뀔 수 있으므로 주기적으로 재확인 필요.

⚠️ 중요한 한계: EC2/RDS/AutoScaling은 "인스턴스가 켜져 있는 시간 × 시간당 단가"라서
   사용률(CPU/네트워크)과 무관하게 켜져 있는 동안은 cost가 항상 일정(flat)하다.
   즉 실사용 환경에서는 이 3개 리소스 타입의 cost 시계열이 좀비 리소스처럼 사용률이
   낮아도 스파이크를 일으키지 않는다 — Z-score의 cost 트리거는 주로 Lambda/S3처럼
   "쓴 만큼 과금"되는 리소스에서만 유의미하게 작동한다. EC2/RDS/AutoScaling의
   이상 탐지는 CPU/네트워크/IOPS 등 다른 지표가 더 큰 역할을 하게 된다.

   (EC2/RDS는 CloudTrail Start/Stop 이력을 반영해서 "켜져 있던 시간만" 과금하도록
   보정함 — action_agent의 롤백 등으로 구간 중간에 상태가 바뀌어도 비례 계산됨.
   AutoScaling은 desired_capacity 지표 자체가 이미 실시간 상태를 반영하고 있어서
   별도 보정이 필요 없음.)
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

import boto3

# 리소스 타입별 CloudTrail 상의 시작/정지 API 이벤트 이름
_STATE_CHANGE_EVENTS: dict[str, dict[str, str]] = {
    "EC2": {"start": "StartInstances", "stop": "StopInstances"},
    "RDS": {"start": "StartDBInstance", "stop": "StopDBInstance"},
}

# ── 정적 단가표 (ap-northeast-2, On-Demand, AWS Pricing API로 확인) ────────────

EC2_HOURLY_RATE: dict[str, float] = {
    "t3.micro": 0.013,
}

RDS_HOURLY_RATE: dict[str, float] = {
    "db.t3.micro": 0.028,  # PostgreSQL, Single-AZ 기준
}

LAMBDA_PRICE_PER_GB_SECOND = 0.0000166667
LAMBDA_PRICE_PER_REQUEST = 0.0000002

S3_STORAGE_PRICE_PER_GB_MONTH = 0.025   # 첫 50TB 구간
S3_REQUEST_PRICE_WRITE = 0.0000045      # PUT/COPY/POST/LIST (1,000건당 $0.0045)
S3_REQUEST_PRICE_READ = 0.00000035      # GET 등 (10,000건당 $0.0035)


# ── 순수 계산 함수 (AWS 호출 없음 — 테스트하기 쉽게 분리) ──────────────────────

def estimate_ec2_cost(instance_type: str, hours: float) -> float:
    rate = EC2_HOURLY_RATE.get(instance_type)
    if rate is None:
        raise ValueError(f"단가 미등록 EC2 인스턴스 타입: {instance_type}")
    return rate * hours


def estimate_rds_cost(db_instance_class: str, hours: float) -> float:
    rate = RDS_HOURLY_RATE.get(db_instance_class)
    if rate is None:
        raise ValueError(f"단가 미등록 RDS 인스턴스 클래스: {db_instance_class}")
    return rate * hours


def estimate_lambda_cost(invocations: float, avg_duration_ms: float, memory_mb: int) -> float:
    memory_gb = memory_mb / 1024
    gb_seconds = invocations * (avg_duration_ms / 1000) * memory_gb
    return invocations * LAMBDA_PRICE_PER_REQUEST + gb_seconds * LAMBDA_PRICE_PER_GB_SECOND


def estimate_s3_cost(
    storage_gb: float, get_requests: float, put_requests: float, period_fraction_of_month: float
) -> float:
    storage_cost = storage_gb * S3_STORAGE_PRICE_PER_GB_MONTH * period_fraction_of_month
    request_cost = get_requests * S3_REQUEST_PRICE_READ + put_requests * S3_REQUEST_PRICE_WRITE
    return storage_cost + request_cost


def estimate_autoscaling_cost(instance_type: str, desired_capacity: float, hours: float) -> float:
    return estimate_ec2_cost(instance_type, hours) * desired_capacity


# ── 리소스 설명 정보 조회 (인스턴스 타입 등 — CloudWatch엔 없는 정보) ───────────

def _get_ec2_instance_type(instance_id: str, client=None) -> str:
    ec2 = client or boto3.client("ec2")
    resp = ec2.describe_instances(InstanceIds=[instance_id])
    return resp["Reservations"][0]["Instances"][0]["InstanceType"]


def _get_rds_instance_class(db_instance_id: str, client=None) -> str:
    rds = client or boto3.client("rds")
    resp = rds.describe_db_instances(DBInstanceIdentifier=db_instance_id)
    return resp["DBInstances"][0]["DBInstanceClass"]


def _get_lambda_memory_mb(function_name: str, client=None) -> int:
    lam = client or boto3.client("lambda")
    resp = lam.get_function_configuration(FunctionName=function_name)
    return resp["MemorySize"]


# ── 실제 가동 시간 추정 (CloudTrail Start/Stop 이력 기반) ──────────────────────
# CloudWatch 지표가 있다고 "그 구간 내내 켜져 있었다"고 가정하면, action_agent가
# Stop→Start를 실행한 구간(롤백 등)에서 비용을 과대추정하게 된다. CloudTrail의
# StartInstances/StopInstances(RDS는 StartDBInstance/StopDBInstance) 호출 이력을
# 조회해서, 구간(period)마다 실제로 몇 % 켜져 있었는지 계산한다.

def _get_state_change_events(
    resource_type: str, resource_id: str, start_time: datetime, end_time: datetime, client=None
) -> list[tuple[datetime, bool]]:
    """[(발생 시각, 그 이후 running 여부), ...] 시간순 리스트. CloudTrail 조회 실패 시 빈 리스트."""
    event_names = _STATE_CHANGE_EVENTS.get(resource_type)
    if event_names is None:
        return []

    ct = client or boto3.client("cloudtrail")
    events: list[tuple[datetime, bool]] = []
    try:
        for action, event_name in event_names.items():
            paginator = ct.get_paginator("lookup_events")
            for page in paginator.paginate(
                LookupAttributes=[{"AttributeKey": "ResourceName", "AttributeValue": resource_id}],
                StartTime=start_time,
                EndTime=end_time,
            ):
                for event in page["Events"]:
                    if event["EventName"] == event_name:
                        events.append((event["EventTime"], action == "start"))
    except Exception:
        return []  # CloudTrail 조회 실패 → 아래에서 "계속 실행 중"으로 폴백

    events.sort(key=lambda e: e[0])
    return events


def _running_fraction_per_period(
    resource_type: str,
    resource_id: str,
    start_time: datetime,
    end_time: datetime,
    period_seconds: int,
    n_periods: int,
    currently_running: bool = True,
    client=None,
) -> list[float]:
    """각 period 구간마다 실제로 실행 중이었던 시간 비율(0.0~1.0)을 리스트로 반환.
    CloudTrail에 이벤트가 없으면(대부분의 정상 상황) 전 구간 1.0 — 기존 방식과 동일.
    """
    events = _get_state_change_events(resource_type, resource_id, start_time, end_time, client)
    if not events:
        return [1.0 if currently_running else 0.0] * n_periods

    # 상태 타임라인: (시각, 그 시각부터 다음 이벤트까지의 running 여부)
    timeline: list[tuple[datetime, bool]] = [(start_time, currently_running)]
    timeline += [(ts, running) for ts, running in events if start_time <= ts <= end_time]
    timeline.append((end_time, timeline[-1][1]))

    fractions = []
    for i in range(n_periods):
        period_start = start_time + timedelta(seconds=i * period_seconds)
        period_end = period_start + timedelta(seconds=period_seconds)
        running_seconds = 0.0
        for (t1, running), (t2, _next_running) in zip(timeline, timeline[1:]):
            seg_start, seg_end = max(t1, period_start), min(t2, period_end)
            if seg_start < seg_end and running:
                running_seconds += (seg_end - seg_start).total_seconds()
        fractions.append(min(running_seconds / period_seconds, 1.0))

    return fractions


def _get_asg_instance_type(asg_name: str, asg_client=None, ec2_client=None) -> str:
    asg = asg_client or boto3.client("autoscaling")
    resp = asg.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])
    group = resp["AutoScalingGroups"][0]
    lt = group.get("LaunchTemplate") or group["MixedInstancesPolicy"]["LaunchTemplate"]["LaunchTemplateSpecification"]
    ec2 = ec2_client or boto3.client("ec2")
    lt_data = ec2.describe_launch_template_versions(
        LaunchTemplateId=lt["LaunchTemplateId"], Versions=["$Latest"]
    )
    return lt_data["LaunchTemplateVersions"][0]["LaunchTemplateData"]["InstanceType"]


# ── raw_metrics 윈도우와 같은 길이의 cost 시계열 생성 ──────────────────────────

def estimate_cost_series(
    resource_type: str,
    resource_id: str,
    usage_metrics: dict[str, list[float]],
    period_seconds: int = 300,
    end_time: Optional[datetime] = None,
    currently_running: bool = True,
) -> list[float]:
    """
    Phase A(fetch_metrics)가 가져온 usage_metrics를 받아, 같은 길이의 cost 리스트를 생성.

    EC2/RDS는 CloudTrail의 Start/Stop 이력을 조회해서 구간별 실제 가동 비율을 반영한다
    (action_agent의 롤백 등으로 구간 중간에 Stop→Start가 일어나도 그 구간만 비례해서
    비용이 줄어듦 — "구간 내내 켜져 있었다"는 단순 가정 대신 실제 이력 기반).
    currently_running: 윈도우 마지막 시점의 실행 상태 (이벤트가 하나도 없을 때 폴백값).
    """
    hours_per_period = period_seconds / 3600
    n = len(next(iter(usage_metrics.values()))) if usage_metrics else 0
    end_time = end_time or datetime.now(timezone.utc)
    start_time = end_time - timedelta(seconds=n * period_seconds)

    if resource_type == "EC2":
        instance_type = _get_ec2_instance_type(resource_id)
        cost_per_period = estimate_ec2_cost(instance_type, hours_per_period)
        fractions = _running_fraction_per_period(
            "EC2", resource_id, start_time, end_time, period_seconds, n, currently_running
        )
        return [cost_per_period * f for f in fractions]

    if resource_type == "RDS":
        db_class = _get_rds_instance_class(resource_id)
        cost_per_period = estimate_rds_cost(db_class, hours_per_period)
        fractions = _running_fraction_per_period(
            "RDS", resource_id, start_time, end_time, period_seconds, n, currently_running
        )
        return [cost_per_period * f for f in fractions]

    if resource_type == "AutoScaling":
        instance_type = _get_asg_instance_type(resource_id)
        desired = usage_metrics.get("group_desired_capacity", [])
        return [estimate_autoscaling_cost(instance_type, d, hours_per_period) for d in desired]

    if resource_type == "Lambda":
        memory_mb = _get_lambda_memory_mb(resource_id)
        invocations = usage_metrics.get("invocation_count", [])
        durations = usage_metrics.get("duration_avg", [])
        return [
            estimate_lambda_cost(inv, dur, memory_mb)
            for inv, dur in zip(invocations, durations)
        ]

    if resource_type == "S3":
        # 저장 용량(BucketSizeBytes)은 이 프로젝트의 fetch_metrics 대상에 없어서(일 단위 지표라
        # 5분 윈도우와 안 맞음) 요청 비용만 반영. storage_gb=0으로 근사.
        requests = usage_metrics.get("number_of_requests", [])
        period_fraction = period_seconds / (30 * 24 * 3600)
        return [
            estimate_s3_cost(storage_gb=0.0, get_requests=r, put_requests=0.0,
                              period_fraction_of_month=period_fraction)
            for r in requests
        ]

    raise ValueError(f"지원하지 않는 resource_type: {resource_type}")
