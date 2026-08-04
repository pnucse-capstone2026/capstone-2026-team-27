"""
Rule Promoter 테스트
--------------------
더미 로그로 승격 후보가 제대로 뽑히는지 검증
"""

import json
import os
import sys
import tempfile
import shutil

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pipeline.rule_promoter import (
    find_promotion_candidates,
    is_already_covered,
    create_rule_from_candidate,
    get_next_rule_id,
    normalize_metrics,
)


def create_dummy_log_entry(
    trace_id: str,
    resource_type: str,
    triggered_metrics: list[str],
    anomaly_type: str,
    qa_passed: bool,
) -> dict:
    """테스트용 더미 로그 엔트리 생성."""
    return {
        "trace_id": trace_id,
        "logged_at": "2025-01-01T00:00:00Z",
        "input": {
            "resource_id": f"test-{resource_type.lower()}-001",
            "resource_type": resource_type,
            "triggered_metrics": triggered_metrics,
            "metrics_summary": {"cpu": {"latest": 80, "mean": 50}},
        },
        "output": {
            "anomaly_type": anomaly_type,
            "interim_action": "테스트 임시 조치",
            "reasoning": "[LLM] 테스트 reasoning",
        },
        "matched_rule_id": None,
        "qa_result": {
            "qa_passed": qa_passed,
            "rollback_count": 0 if qa_passed else 1,
        } if qa_passed is not None else None,
    }


def test_normalize_metrics():
    """triggered_metrics 정규화 테스트."""
    print("\n[ normalize_metrics 테스트 ]")

    result1 = normalize_metrics(["cpu_utilization", "network_in"])
    result2 = normalize_metrics(["network_in", "cpu_utilization"])

    assert result1 == result2, "순서가 달라도 같은 튜플이어야 함"
    print("  PASS: 메트릭 정규화 정상")


def test_find_promotion_candidates():
    """승격 후보 찾기 테스트."""
    print("\n[ find_promotion_candidates 테스트 ]")

    # 더미 로그 생성: EC2 + [cpu, network] → cost_spike 패턴 5번 (qa_passed=True)
    logs = []
    for i in range(5):
        logs.append(create_dummy_log_entry(
            trace_id=f"trace-{i}",
            resource_type="EC2",
            triggered_metrics=["cpu_utilization", "network_in"],
            anomaly_type="cost_spike",
            qa_passed=True,
        ))

    # 같은 패턴이지만 qa_passed=False인 것 2개 추가 (카운트 안 됨)
    for i in range(2):
        logs.append(create_dummy_log_entry(
            trace_id=f"trace-fail-{i}",
            resource_type="EC2",
            triggered_metrics=["cpu_utilization", "network_in"],
            anomaly_type="cost_spike",
            qa_passed=False,
        ))

    # 다른 패턴: Lambda + [invocation_count] → cost_spike 2번 (min_count 미달)
    for i in range(2):
        logs.append(create_dummy_log_entry(
            trace_id=f"trace-lambda-{i}",
            resource_type="Lambda",
            triggered_metrics=["invocation_count"],
            anomaly_type="cost_spike",
            qa_passed=True,
        ))

    # min_count=3으로 후보 찾기
    candidates = find_promotion_candidates(logs, min_count=3)

    assert len(candidates) == 1, f"후보 1개여야 함, 실제: {len(candidates)}"
    assert candidates[0]["resource_type"] == "EC2"
    assert candidates[0]["count"] == 5, f"카운트 5여야 함, 실제: {candidates[0]['count']}"
    print(f"  PASS: 후보 1개 발견 (EC2, count=5)")

    # min_count=2로 후보 찾기
    candidates2 = find_promotion_candidates(logs, min_count=2)
    assert len(candidates2) == 2, f"후보 2개여야 함, 실제: {len(candidates2)}"
    print(f"  PASS: min_count=2일 때 후보 2개 발견")


def test_is_already_covered():
    """기존 규칙 커버 여부 테스트."""
    print("\n[ is_already_covered 테스트 ]")

    existing_rules = [
        {
            "rule_id": "CLF-001",
            "resource_types": ["EC2"],
            "conditions": {"triggered_metrics": ["cost"]},
        },
        {
            "rule_id": "CLF-002",
            "resource_types": ["*"],
            "conditions": {"triggered_metrics": ["network_out"]},
        },
    ]

    # EC2 + [cost] → 커버됨
    candidate1 = {
        "resource_type": "EC2",
        "triggered_metrics": ["cost"],
    }
    assert is_already_covered(candidate1, existing_rules) == True
    print("  PASS: EC2+[cost] → 커버됨")

    # EC2 + [cpu, network] → 커버 안 됨
    candidate2 = {
        "resource_type": "EC2",
        "triggered_metrics": ["cpu_utilization", "network_in"],
    }
    assert is_already_covered(candidate2, existing_rules) == False
    print("  PASS: EC2+[cpu,network] → 커버 안 됨")

    # Lambda + [network_out] → * 규칙으로 커버됨
    candidate3 = {
        "resource_type": "Lambda",
        "triggered_metrics": ["network_out"],
    }
    assert is_already_covered(candidate3, existing_rules) == True
    print("  PASS: Lambda+[network_out] → * 규칙으로 커버됨")


def test_get_next_rule_id():
    """규칙 ID 채번 테스트."""
    print("\n[ get_next_rule_id 테스트 ]")

    existing_rules = [
        {"rule_id": "CLF-001"},
        {"rule_id": "CLF-003"},
        {"rule_id": "CLF-005"},
    ]

    next_id = get_next_rule_id(existing_rules)
    assert next_id == "CLF-006", f"CLF-006이어야 함, 실제: {next_id}"
    print(f"  PASS: 다음 ID = {next_id}")

    # 빈 리스트
    next_id_empty = get_next_rule_id([])
    assert next_id_empty == "CLF-001", f"CLF-001이어야 함, 실제: {next_id_empty}"
    print(f"  PASS: 빈 리스트 → {next_id_empty}")


def test_create_rule_from_candidate():
    """후보에서 규칙 생성 테스트."""
    print("\n[ create_rule_from_candidate 테스트 ]")

    candidate = {
        "resource_type": "EC2",
        "triggered_metrics": ["cpu_utilization", "network_in"],
        "anomaly_type": "cost_spike",
        "count": 5,
        "sample_reasoning": "[LLM] CPU와 네트워크 동시 급증",
        "sample_interim_action": "트래픽 모니터링",
    }

    rule = create_rule_from_candidate(candidate, "CLF-006")

    assert rule["rule_id"] == "CLF-006"
    assert rule["resource_types"] == ["EC2"]
    assert rule["conditions"]["triggered_metrics"] == ["cpu_utilization", "network_in"]
    assert rule["result"]["anomaly_type"] == "cost_spike"
    assert rule["author"] == "auto-promoted"
    assert "5건" in rule["rationale"]
    print(f"  PASS: 규칙 생성됨 (rule_id={rule['rule_id']})")


def test_full_flow_with_temp_files():
    """임시 파일로 전체 플로우 테스트."""
    print("\n[ 전체 플로우 테스트 (임시 파일) ]")

    # 임시 디렉토리 생성
    temp_dir = tempfile.mkdtemp()
    temp_log_path = os.path.join(temp_dir, "test_log.jsonl")
    temp_rules_path = os.path.join(temp_dir, "test_rules.json")

    try:
        # 더미 로그 생성
        logs = []
        for i in range(5):
            logs.append(create_dummy_log_entry(
                trace_id=f"trace-{i}",
                resource_type="EC2",
                triggered_metrics=["cpu_utilization", "network_in"],
                anomaly_type="cost_spike",
                qa_passed=True,
            ))

        # 로그 파일 저장
        with open(temp_log_path, "w", encoding="utf-8") as f:
            for log in logs:
                f.write(json.dumps(log, ensure_ascii=False) + "\n")

        # 빈 규칙 파일 저장
        with open(temp_rules_path, "w", encoding="utf-8") as f:
            json.dump([], f)

        # 후보 찾기
        candidates = find_promotion_candidates(logs, min_count=3)
        assert len(candidates) == 1

        # 규칙 생성
        rule = create_rule_from_candidate(candidates[0], "CLF-006")
        assert rule["rule_id"] == "CLF-006"

        print("  PASS: 전체 플로우 정상")

    finally:
        # 임시 파일 정리
        shutil.rmtree(temp_dir)


if __name__ == "__main__":
    print("=" * 60)
    print("Rule Promoter 테스트")
    print("=" * 60)

    test_normalize_metrics()
    test_find_promotion_candidates()
    test_is_already_covered()
    test_get_next_rule_id()
    test_create_rule_from_candidate()
    test_full_flow_with_temp_files()

    print("\n" + "=" * 60)
    print("모든 테스트 통과!")
    print("=" * 60)
