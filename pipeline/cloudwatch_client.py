"""
pipeline/cloudwatch_client.py

Phase A: AWS CloudWatch에서 리소스 타입별 지표를 가져오는 함수.

설계:
- Basic Monitoring(무료, 5분 간격) 그대로 사용. Detailed Monitoring 불필요.
- GetMetricData로 과거 n_points × period_seconds(기본 30×5분=2.5시간)를
  한 번에 배치 조회 → 실시간으로 쌓일 때까지 기다릴 필요 없이 즉시 30개 포인트 확보.
- cost 필드는 여기서 안 다룸 (pipeline/cost_estimator.py에서 사용량 지표 기반으로 별도 계산).

boto3 클라이언트는 함수 내부에서 생성 (action_agent.py와 동일한 패턴 —
테스트 시 mock 주입하기 쉽게).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

import boto3

# ── 리소스 타입별 (Namespace, MetricName, Stat) 매핑 ──────────────────────────
# key는 schema/state.py의 각 리소스 Metrics TypedDict 필드명과 일치시킴.
METRIC_SPEC: dict[str, dict[str, tuple[str, str, str]]] = {
    "EC2": {
        "cpu_utilization": ("AWS/EC2", "CPUUtilization", "Average"),
        "network_in":      ("AWS/EC2", "NetworkIn", "Sum"),
        "network_out":     ("AWS/EC2", "NetworkOut", "Sum"),
    },
    "Lambda": {
        "invocation_count": ("AWS/Lambda", "Invocations", "Sum"),
        "error_count":      ("AWS/Lambda", "Errors", "Sum"),
        "duration_avg":     ("AWS/Lambda", "Duration", "Average"),
    },
    "S3": {
        # ⚠️ S3 Request Metrics를 버킷에서 활성화해야 값이 나옴 (지금은 테스트 보류 상태)
        "number_of_requests": ("AWS/S3", "AllRequests", "Sum"),
        "bytes_downloaded":   ("AWS/S3", "BytesDownloaded", "Sum"),
    },
    "RDS": {
        "cpu_utilization":      ("AWS/RDS", "CPUUtilization", "Average"),
        "database_connections": ("AWS/RDS", "DatabaseConnections", "Average"),
        "read_iops":            ("AWS/RDS", "ReadIOPS", "Average"),
        "write_iops":           ("AWS/RDS", "WriteIOPS", "Average"),
    },
    "AutoScaling": {
        "group_desired_capacity":     ("AWS/AutoScaling", "GroupDesiredCapacity", "Average"),
        "group_in_service_instances": ("AWS/AutoScaling", "GroupInServiceInstances", "Average"),
    },
}

# 리소스 타입별 CloudWatch 차원(Dimension) 키
DIMENSION_NAME: dict[str, str] = {
    "EC2": "InstanceId",
    "Lambda": "FunctionName",
    "S3": "BucketName",
    "RDS": "DBInstanceIdentifier",
    "AutoScaling": "AutoScalingGroupName",
}


def _build_dimensions(resource_type: str, resource_id: str) -> list[dict[str, str]]:
    dimensions = [{"Name": DIMENSION_NAME[resource_type], "Value": resource_id}]
    if resource_type == "S3":
        # S3 Request Metrics는 버킷 이름 + 필터 ID 조합이 필요 (버킷 콘솔에서 "EntireBucket" 필터 생성 전제)
        dimensions.append({"Name": "FilterId", "Value": "EntireBucket"})
    return dimensions


def fetch_metrics(
    resource_type: str,
    resource_id: str,
    n_points: int = 30,
    period_seconds: int = 300,
    client: Optional["boto3.client"] = None,
) -> dict[str, list[float]]:
    """
    resource_type/resource_id에 해당하는 리소스의 최근 n_points개 지표를 반환.
    (과거 n_points*period_seconds 구간을 한 번에 배치 조회 — 실시간 폴링 아님)

    반환값은 schema/state.py의 해당 리소스 Metrics TypedDict와 동일한 키를 가진
    dict[str, list[float]] (cost는 포함 안 됨).

    항상 정확히 n_points 길이로 반환한다. CloudWatch가 활동 없음(예: 호출 0건)으로
    데이터포인트 자체를 비워서 응답한 구간은 0.0으로 채워 넣는다 — "활동 없음"과
    "0"은 의미상 같기 때문. 단, CPUUtilization처럼 리소스가 아예 정지해 있어서
    생기는 공백도 구분 없이 0.0으로 채워지므로, 그 구간이 진짜 "0"인지 "리소스가
    꺼져 있었다"인지는 이 함수만으로는 알 수 없다는 점은 감안해야 한다.
    """
    if resource_type not in METRIC_SPEC:
        raise ValueError(f"지원하지 않는 resource_type: {resource_type}")

    cw = client or boto3.client("cloudwatch")

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(seconds=n_points * period_seconds)

    metric_keys = list(METRIC_SPEC[resource_type].keys())
    dimensions = _build_dimensions(resource_type, resource_id)

    queries = []
    for i, metric_key in enumerate(metric_keys):
        namespace, cw_metric_name, stat = METRIC_SPEC[resource_type][metric_key]
        queries.append({
            "Id": f"m{i}",
            "MetricStat": {
                "Metric": {
                    "Namespace": namespace,
                    "MetricName": cw_metric_name,
                    "Dimensions": dimensions,
                },
                "Period": period_seconds,
                "Stat": stat,
            },
            "ReturnData": True,
        })

    response = cw.get_metric_data(
        MetricDataQueries=queries,
        StartTime=start_time,
        EndTime=end_time,
        ScanBy="TimestampAscending",
    )

    results_by_id = {r["Id"]: r for r in response["MetricDataResults"]}

    # CloudWatch는 활동이 전혀 없던 구간(예: Lambda 호출 0건)은 그 구간 자체를 응답에서
    # 아예 빼버린다(0을 주는 게 아니라 데이터포인트가 없다고 취급) — 그래서 트래픽이
    # 드문 리소스는 실제로 이상(버스트 호출 등)이 생겨도 최소 포인트 수를 못 채워
    # 판단 자체가 보류되는 문제가 있었다. "활동 없음 = 0"이 맞는 의미이므로,
    # 기대되는 시점 그리드를 직접 만들어서 없는 구간은 0.0으로 채워 넣는다.
    expected_times = [start_time + timedelta(seconds=i * period_seconds) for i in range(n_points)]
    half_period = period_seconds / 2

    metrics: dict[str, list[float]] = {}
    for i, metric_key in enumerate(metric_keys):
        row = results_by_id.get(f"m{i}")
        if row is None or not row.get("Timestamps"):
            metrics[metric_key] = [0.0] * n_points
            continue

        observed = list(zip(row["Timestamps"], row["Values"]))
        filled: list[float] = []
        for expected_ts in expected_times:
            match = next(
                (v for ts, v in observed if abs((ts - expected_ts).total_seconds()) < half_period),
                0.0,
            )
            filled.append(match)
        metrics[metric_key] = filled

    return metrics
