"""
QA Rule Promoter 테스트
-----------------------
더미 로그로 QA 승격 후보가 제대로 뽑히는지 검증 (playground/test_rule_promoter.py와 동일한 구조)
"""

import json
import os
import sys
import tempfile
import shutil

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pipeline.qa_rule_promoter import (
    find_qa_promotion_candidates,
    is_qa_already_covered,
    create_qa_rule_from_candidate,
    get_next_qa_rule_id,
)


def create_dummy_log_entry(
    trace_id: str,
    resource_type: str,
    action_executed: str,
    qa_passed: bool,
    qa_matched_rule_id: str | None = None,
) -> dict:
    """테스트용 더미 로그 엔트리 생성 (QA_agent._update_llm_log_with_qa_result 출력 형태)."""
    return {
        "trace_id": trace_id,
        "logged_at": "2025-01-01T00:00:00Z",
        "input": {
            "resource_id": f"test-{resource_type.lower()}-001",
            "resource_type": resource_type,
            "triggered_metrics": [],
            "metrics_summary": {"cpu": {"latest": 80, "mean": 50}},
        },
        "output": {
            "anomaly_type": "cost_spike",
            "interim_action": None,
            "reasoning": "[LLM] 테스트 reasoning",
        },
        "matched_rule_id": None,
        "qa_result": {
            "qa_passed": qa_passed,
            "action_executed": action_executed,
            "rollback_count": 0 if qa_passed else 1,
            "sla_check_result": {"detail": "테스트 SLA 근거"},
            "qa_matched_rule_id": qa_matched_rule_id,
            "whitelisted": False,
        },
    }


def test_find_qa_promotion_candidates():
    """QA 승격 후보 찾기 테스트."""
    print("\n[ find_qa_promotion_candidates 테스트 ]")

    logs = []
    # EC2 + Resize -> 항상 통과 5번 (LLM 판단, qa_matched_rule_id 없음)
    for i in range(5):
        logs.append(create_dummy_log_entry(f"t-{i}", "EC2", "Resize", True))

    # 같은 조건인데 실패인 것 2개 (다른 그룹이라 별도로 카운트됨 -> min_count 미달)
    for i in range(2):
        logs.append(create_dummy_log_entry(f"t-fail-{i}", "EC2", "Resize", False))

    # 다른 액션: Lambda + Throttle -> 통과 2번 (min_count=3 미달)
    for i in range(2):
        logs.append(create_dummy_log_entry(f"t-lambda-{i}", "Lambda", "Throttle", True))

    # 이미 QA 규칙으로 처리된 것 (qa_matched_rule_id 있음) -> "LLM 판단" 아니므로 제외돼야 함
    for i in range(5):
        logs.append(create_dummy_log_entry(f"t-ruled-{i}", "EC2", "Resize", True, qa_matched_rule_id="QA-001"))

    candidates = find_qa_promotion_candidates(logs, min_count=3)

    assert len(candidates) == 1, f"후보 1개여야 함, 실제: {len(candidates)}"
    assert candidates[0]["resource_type"] == "EC2"
    assert candidates[0]["action_executed"] == "Resize"
    assert candidates[0]["qa_passed"] is True
    assert candidates[0]["count"] == 5, f"카운트 5여야 함, 실제: {candidates[0]['count']}"
    print("  PASS: 후보 1개 발견 (EC2+Resize, 통과, count=5)")
    print("  PASS: qa_matched_rule_id 있는 엔트리는 후보 카운트에서 제외됨")

    # min_count=2면 (EC2,Resize,True)=5, (EC2,Resize,False)=2, (Lambda,Throttle,True)=2
    # 세 그룹 모두 조건을 만족한다 (실패 2회짜리도 별도 그룹으로 카운트됨)
    candidates2 = find_qa_promotion_candidates(logs, min_count=2)
    assert len(candidates2) == 3, f"min_count=2일 때 후보 3개여야 함, 실제: {len(candidates2)}"
    print("  PASS: min_count=2일 때 후보 3개 발견 (통과/실패 그룹이 분리돼 각각 카운트됨)")


def test_is_qa_already_covered():
    """기존 QA 규칙 커버 여부 테스트."""
    print("\n[ is_qa_already_covered 테스트 ]")

    existing_rules = [
        {
            "rule_id": "QA-001",
            "resource_types": ["*"],
            "conditions": {"action_executed": ["NoAction", None]},
        },
        {
            "rule_id": "QA-004",
            "resource_types": ["Lambda"],
            "conditions": {"action_executed": ["Throttle"]},
        },
    ]

    assert is_qa_already_covered(
        {"resource_type": "EC2", "action_executed": "NoAction"}, existing_rules
    ) is True
    print("  PASS: EC2+NoAction -> '*' 규칙으로 커버됨")

    assert is_qa_already_covered(
        {"resource_type": "Lambda", "action_executed": "Throttle"}, existing_rules
    ) is True
    print("  PASS: Lambda+Throttle -> 커버됨")

    assert is_qa_already_covered(
        {"resource_type": "EC2", "action_executed": "Resize"}, existing_rules
    ) is False
    print("  PASS: EC2+Resize -> 커버 안 됨")


def test_get_next_qa_rule_id():
    """QA 규칙 ID 채번 테스트."""
    print("\n[ get_next_qa_rule_id 테스트 ]")

    existing_rules = [{"rule_id": "QA-001"}, {"rule_id": "QA-003"}, {"rule_id": "QA-005"}]
    assert get_next_qa_rule_id(existing_rules) == "QA-006"
    print("  PASS: 다음 ID = QA-006")

    assert get_next_qa_rule_id([]) == "QA-001"
    print("  PASS: 빈 리스트 -> QA-001")


def test_create_qa_rule_from_candidate():
    """후보에서 QA 규칙 생성 테스트."""
    print("\n[ create_qa_rule_from_candidate 테스트 ]")

    candidate = {
        "resource_type": "EC2",
        "action_executed": "Resize",
        "qa_passed": True,
        "count": 5,
        "sample_reasoning": "테스트 SLA 근거",
    }

    rule = create_qa_rule_from_candidate(candidate, "QA-006")

    assert rule["rule_id"] == "QA-006"
    assert rule["rule_type"] == "qa"
    assert rule["resource_types"] == ["EC2"]
    assert rule["conditions"]["action_executed"] == ["Resize"]
    assert rule["result"]["force_pass"] is True
    assert rule["result"]["force_fail"] is False
    assert rule["author"] == "auto-promoted"
    assert "5건" in rule["rationale"]
    print(f"  PASS: 규칙 생성됨 (rule_id={rule['rule_id']}, force_pass=True)")

    # 실패 케이스 -> force_fail
    candidate_fail = {**candidate, "qa_passed": False}
    rule_fail = create_qa_rule_from_candidate(candidate_fail, "QA-007")
    assert rule_fail["result"]["force_pass"] is False
    assert rule_fail["result"]["force_fail"] is True
    print("  PASS: 실패 반복 케이스 -> force_fail=True")


def test_full_flow_with_temp_files():
    """임시 파일로 전체 플로우 테스트 (실제 schema/rules/qa_rules.json은 건드리지 않음)."""
    print("\n[ 전체 플로우 테스트 (임시 파일) ]")

    temp_dir = tempfile.mkdtemp()
    try:
        logs = [create_dummy_log_entry(f"t-{i}", "RDS", "Stop", True) for i in range(4)]

        candidates = find_qa_promotion_candidates(logs, min_count=3)
        assert len(candidates) == 1

        rule = create_qa_rule_from_candidate(candidates[0], "QA-006")
        assert rule["rule_id"] == "QA-006"

        temp_rules_path = os.path.join(temp_dir, "test_qa_rules.json")
        with open(temp_rules_path, "w", encoding="utf-8") as f:
            json.dump([rule], f, ensure_ascii=False)

        with open(temp_rules_path, "r", encoding="utf-8") as f:
            saved = json.load(f)
        assert saved[0]["rule_id"] == "QA-006"

        print("  PASS: 전체 플로우 정상")
    finally:
        shutil.rmtree(temp_dir)


if __name__ == "__main__":
    print("=" * 60)
    print("QA Rule Promoter 테스트")
    print("=" * 60)

    test_find_qa_promotion_candidates()
    test_is_qa_already_covered()
    test_get_next_qa_rule_id()
    test_create_qa_rule_from_candidate()
    test_full_flow_with_temp_files()

    print("\n" + "=" * 60)
    print("모든 테스트 통과!")
    print("=" * 60)
