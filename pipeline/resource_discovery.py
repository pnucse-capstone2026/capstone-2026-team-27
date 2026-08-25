"""
pipeline/resource_discovery.py

Phase C: 태그(기본 Detection=true) 기준으로 감시 대상 리소스를 찾는다.
계정 전체를 무차별로 스캔하지 않고, 명시적으로 태그가 붙은 것만 대상으로 한다.

EC2는 describe_instances의 Filters로 서버 사이드에서 바로 태그 필터링이 되지만,
Lambda/RDS/AutoScaling/S3는 목록 조회 API 자체에 태그 필터가 없어서 전체를
가져온 뒤 리소스별로 태그를 따로 조회해서 클라이언트 사이드로 걸러야 한다.

boto3 클라이언트는 함수 내부에서 생성 (기존 action_agent.py/cloudwatch_client.py와 동일 패턴).
"""

from __future__ import annotations

from typing import Optional

import boto3

DEFAULT_TAG_KEY = "Detection"
DEFAULT_TAG_VALUE = "true"


def discover_ec2_instances(
    tag_key: str = DEFAULT_TAG_KEY, tag_value: str = DEFAULT_TAG_VALUE, client=None
) -> list[str]:
    """독립 EC2 인스턴스만 반환. AutoScaling 그룹이 관리하는 인스턴스는 제외한다
    (그 인스턴스들은 "AutoScaling" 타입으로 그룹 단위로 이미 감시 대상이라, EC2로
    또 잡히면 이중 감시가 됨 — aws:autoscaling:groupName 태그 유무로 구분)."""
    ec2 = client or boto3.client("ec2")
    instance_ids: list[str] = []
    paginator = ec2.get_paginator("describe_instances")
    for page in paginator.paginate(Filters=[
        {"Name": f"tag:{tag_key}", "Values": [tag_value]},
        {"Name": "instance-state-name", "Values": ["running"]},
    ]):
        for reservation in page["Reservations"]:
            for instance in reservation["Instances"]:
                tags = {t["Key"]: t["Value"] for t in instance.get("Tags", [])}
                if "aws:autoscaling:groupName" in tags:
                    continue  # ASG 관리 인스턴스 — AutoScaling 타입으로 이미 감시됨
                instance_ids.append(instance["InstanceId"])
    return instance_ids


def discover_lambda_functions(
    tag_key: str = DEFAULT_TAG_KEY, tag_value: str = DEFAULT_TAG_VALUE, client=None
) -> list[str]:
    lam = client or boto3.client("lambda")
    names: list[str] = []
    paginator = lam.get_paginator("list_functions")
    for page in paginator.paginate():
        for fn in page["Functions"]:
            tags = lam.list_tags(Resource=fn["FunctionArn"]).get("Tags", {})
            if tags.get(tag_key) == tag_value:
                names.append(fn["FunctionName"])
    return names


def discover_rds_instances(
    tag_key: str = DEFAULT_TAG_KEY, tag_value: str = DEFAULT_TAG_VALUE, client=None
) -> list[str]:
    rds = client or boto3.client("rds")
    names: list[str] = []
    paginator = rds.get_paginator("describe_db_instances")
    for page in paginator.paginate():
        for db in page["DBInstances"]:
            tags_resp = rds.list_tags_for_resource(ResourceName=db["DBInstanceArn"])
            tags = {t["Key"]: t["Value"] for t in tags_resp.get("TagList", [])}
            if tags.get(tag_key) == tag_value:
                names.append(db["DBInstanceIdentifier"])
    return names


def discover_autoscaling_groups(
    tag_key: str = DEFAULT_TAG_KEY, tag_value: str = DEFAULT_TAG_VALUE, client=None
) -> list[str]:
    asg = client or boto3.client("autoscaling")
    names: list[str] = []
    paginator = asg.get_paginator("describe_auto_scaling_groups")
    for page in paginator.paginate():
        for group in page["AutoScalingGroups"]:
            tags = {t["Key"]: t["Value"] for t in group.get("Tags", [])}
            if tags.get(tag_key) == tag_value:
                names.append(group["AutoScalingGroupName"])
    return names


def discover_s3_buckets(
    tag_key: str = DEFAULT_TAG_KEY, tag_value: str = DEFAULT_TAG_VALUE, client=None
) -> list[str]:
    s3 = client or boto3.client("s3")
    names: list[str] = []
    for bucket in s3.list_buckets()["Buckets"]:
        try:
            tags_resp = s3.get_bucket_tagging(Bucket=bucket["Name"])
            tags = {t["Key"]: t["Value"] for t in tags_resp.get("TagSet", [])}
        except s3.exceptions.ClientError:
            tags = {}  # 태그가 아예 없는 버킷은 get_bucket_tagging이 에러를 던짐
        if tags.get(tag_key) == tag_value:
            names.append(bucket["Name"])
    return names


_DISCOVERERS = {
    "EC2": discover_ec2_instances,
    "Lambda": discover_lambda_functions,
    "RDS": discover_rds_instances,
    "AutoScaling": discover_autoscaling_groups,
    "S3": discover_s3_buckets,
}


def discover_all_resources(
    tag_key: str = DEFAULT_TAG_KEY,
    tag_value: str = DEFAULT_TAG_VALUE,
    resource_types: Optional[list[str]] = None,
) -> list[dict]:
    """
    태그 기준으로 감시 대상 리소스를 전부 찾아서 [{resource_id, resource_type}, ...] 반환.
    raw_metrics는 아직 안 채워짐 — pipeline/cloudwatch_client.py, cost_estimator.py로
    별도로 채워서 pipeline/detection_agent.py의 scan_resources_sequential에 넘겨야 함.

    resource_types를 안 주면 5개 타입 전부 조회 (S3는 실제로는 테스트 보류 상태라
    태그된 버킷이 없으면 자동으로 빈 리스트).
    """
    types_to_check = resource_types or list(_DISCOVERERS.keys())

    resources: list[dict] = []
    for resource_type in types_to_check:
        discoverer = _DISCOVERERS.get(resource_type)
        if discoverer is None:
            raise ValueError(f"지원하지 않는 resource_type: {resource_type}")
        for resource_id in discoverer(tag_key, tag_value):
            resources.append({"resource_id": resource_id, "resource_type": resource_type})

    return resources
