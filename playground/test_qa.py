# playground/test_qa.py
"""
QA Agent 테스트
---------------
다양한 시나리오에서 SLA 검증 및 롤백 로직 테스트

테스트 케이스:
1. NoAction - 항상 통과
2. 액션 성공 + 모든 SLA 충족 - 통과
3. 액션 성공 + CPU SLA 위반 - 실패
4. 액션 성공 + 비용 SLA 위반 - 실패
5. 액션 실패 - 실패
6. 롤백 카운트 누적 테스트
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.QA_agent import (
    qa_node,
    qa_node_force_fail,
    qa_node_force_pass,
    _check_cpu_sla,
    _check_cost_sla,
    _check_availability_sla,
)
from schema.state import PipelineState

# ── 테스트용 State 만들기 ──────────────────────────────────────────────────


def make_base_state() -> PipelineState:
    """기본 State 생성."""
    return {
        "resource_id": "i-0test",
        "resource_type": "EC2",
        "raw_metrics": {
            "cpu_utilization": [30.0] * 30,  # 정상 CPU
            "network_in": [1000.0] * 30,
            "network_out": [500.0] * 30,
            "cost": [0.05] * 30,  # 안정적인 비용
        },
        "timestamp": "2026-06-23T00:00:00",
        "anomaly_flag": True,
        "anomaly_score_zscore": 3.5,
        "anomaly_score_iforest": 0.7,
        "triggered_metrics": ["cpu_utilization"],
        "anomaly_type": "cost_spike",
        "classification_reasoning": "테스트용",
        "interim_action_taken": None,
        "candidate_actions": [],
        "selected_action": "ScaleDown",
        "risk_level": "MED",
        "requires_approval": True,
        "decision_reasoning": "테스트용",
        "pre_action_snapshot": {
            "instance_type": "t3.medium",
            "state": "running",
            "security_group_ids": ["sg-0abc1234"],
        },
        "action_executed": "ScaleDown",
        "action_result": {"status": "success", "ResponseMetadata": {"HTTPStatusCode": 200}},
        "qa_passed": None,
        "sla_check_result": None,
        "rollback_count": 0,
        "log_entries": [],
    }


# ── 개별 SLA 체크 함수 테스트 ─────────────────────────────────────────────

print("=" * 60)
print("개별 SLA 체크 함수 테스트")
print("=" * 60)

# CPU SLA 테스트
print("\n[ CPU SLA 테스트 ]")
state_cpu_ok = make_base_state()
state_cpu_ok["raw_metrics"]["cpu_utilization"] = [50.0] * 30  # 정상
cpu_ok, cpu_detail = _check_cpu_sla(state_cpu_ok)
print(f"  CPU 50% → ok={cpu_ok}, detail={cpu_detail}")

state_cpu_fail = make_base_state()
state_cpu_fail["raw_metrics"]["cpu_utilization"] = [85.0] * 30  # SLA 위반
cpu_ok, cpu_detail = _check_cpu_sla(state_cpu_fail)
print(f"  CPU 85% → ok={cpu_ok}, detail={cpu_detail}")

# Lambda (CPU 지표 없음)
state_lambda = make_base_state()
state_lambda["resource_type"] = "Lambda"
state_lambda["raw_metrics"] = {
    "invocation_count": [100.0] * 30,
    "error_count": [5.0] * 30,
    "duration_avg": [200.0] * 30,
    "cost": [0.01] * 30,
}
cpu_ok, cpu_detail = _check_cpu_sla(state_lambda)
print(f"  Lambda (CPU 없음) → ok={cpu_ok}, detail={cpu_detail}")

# 비용 SLA 테스트
print("\n[ 비용 SLA 테스트 ]")
state_cost_ok = make_base_state()
state_cost_ok["raw_metrics"]["cost"] = [0.05] * 29 + [0.04]  # 비용 감소
cost_ok, cost_detail = _check_cost_sla(state_cost_ok)
print(f"  비용 감소 → ok={cost_ok}, detail={cost_detail}")

state_cost_fail = make_base_state()
state_cost_fail["raw_metrics"]["cost"] = [0.05] * 29 + [0.10]  # 비용 2배 증가
cost_ok, cost_detail = _check_cost_sla(state_cost_fail)
print(f"  비용 2배 증가 → ok={cost_ok}, detail={cost_detail}")

# 가용성 SLA 테스트
print("\n[ 가용성 SLA 테스트 ]")
state_avail_ok = make_base_state()
state_avail_ok["action_result"] = {"status": "success", "ResponseMetadata": {"HTTPStatusCode": 200}}
avail_ok, avail_detail = _check_availability_sla(state_avail_ok)
print(f"  액션 성공 → ok={avail_ok}, detail={avail_detail}")

state_avail_fail = make_base_state()
state_avail_fail["action_result"] = {"status": "failed", "error": "인스턴스 접근 불가"}
avail_ok, avail_detail = _check_availability_sla(state_avail_fail)
print(f"  액션 실패 → ok={avail_ok}, detail={avail_detail}")


# ── QA 노드 통합 테스트 ────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("QA 노드 통합 테스트")
print("=" * 60)

# 테스트 케이스 정의
test_cases = [
    {
        "name": "NoAction - 항상 통과",
        "setup": lambda s: (
            s.update({"action_executed": "NoAction", "action_result": None}),
            s
        )[1],
        "expected_pass": True,
    },
    {
        "name": "액션 성공 + 모든 SLA 충족",
        "setup": lambda s: s,  # 기본 상태 그대로 사용
        "expected_pass": True,
    },
    {
        "name": "액션 성공 + CPU SLA 위반",
        "setup": lambda s: (
            s["raw_metrics"].update({"cpu_utilization": [85.0] * 30}),
            s
        )[1],
        "expected_pass": False,
    },
    {
        "name": "액션 성공 + 비용 SLA 위반 (비용 급증)",
        "setup": lambda s: (
            s["raw_metrics"].update({"cost": [0.05] * 29 + [0.15]}),
            s
        )[1],
        "expected_pass": False,
    },
    {
        "name": "액션 실패",
        "setup": lambda s: (
            s.update({"action_result": {"status": "failed", "error": "EC2 API 오류"}}),
            s
        )[1],
        "expected_pass": False,
    },
]

for tc in test_cases:
    state = make_base_state()
    state = tc["setup"](state)
    result = qa_node(state)

    status = "PASS" if result["qa_passed"] == tc["expected_pass"] else "FAIL"
    print(f"\n[ {tc['name']} ] - {status}")
    print(f"  qa_passed      : {result['qa_passed']} (expected: {tc['expected_pass']})")
    print(f"  rollback_count : {result['rollback_count']}")
    print(f"  sla_check_result: {result['sla_check_result']}")


# ── 롤백 카운트 누적 테스트 ────────────────────────────────────────────────

print("\n" + "=" * 60)
print("롤백 카운트 누적 테스트")
print("=" * 60)

state = make_base_state()
state["raw_metrics"]["cpu_utilization"] = [85.0] * 30  # CPU SLA 위반

print("\n[ 1차 QA 실패 ]")
result = qa_node(state)
print(f"  qa_passed={result['qa_passed']}, rollback_count={result['rollback_count']}")

print("\n[ 2차 QA 실패 (롤백 후 재시도) ]")
result = qa_node(result)
print(f"  qa_passed={result['qa_passed']}, rollback_count={result['rollback_count']}")

print("\n[ 3차 QA 실패 - 2회 초과, 관리자 알림 필요 ]")
result = qa_node(result)
print(f"  qa_passed={result['qa_passed']}, rollback_count={result['rollback_count']}")
print(f"  마지막 로그: {result['log_entries'][-3:]}")


# ── 리소스 타입별 테스트 ───────────────────────────────────────────────────

print("\n" + "=" * 60)
print("리소스 타입별 QA 테스트")
print("=" * 60)

resource_test_cases = [
    {
        "name": "EC2 - 정상",
        "resource_type": "EC2",
        "raw_metrics": {
            "cpu_utilization": [30.0] * 30,
            "network_in": [1000.0] * 30,
            "network_out": [500.0] * 30,
            "cost": [0.05] * 30,
        },
        "action_executed": "Stop",
        "expected_pass": True,
    },
    {
        "name": "Lambda - Throttle 성공",
        "resource_type": "Lambda",
        "raw_metrics": {
            "invocation_count": [100.0] * 30,
            "error_count": [5.0] * 30,
            "duration_avg": [200.0] * 30,
            "cost": [0.01] * 30,
        },
        "action_executed": "Throttle",
        "expected_pass": True,
    },
    {
        "name": "S3 - Block 성공",
        "resource_type": "S3",
        "raw_metrics": {
            "number_of_requests": [100.0] * 30,
            "bytes_downloaded": [5000.0] * 30,
            "cost": [0.02] * 30,
        },
        "action_executed": "Block",
        "expected_pass": True,
    },
    {
        "name": "AutoScaling - ScaleDown 성공",
        "resource_type": "AutoScaling",
        "raw_metrics": {
            "group_desired_capacity": [4.0] * 29 + [2.0],  # 스케일 다운
            "group_in_service_instances": [4.0] * 29 + [2.0],
            "cost": [0.1] * 29 + [0.05],  # 비용 감소
        },
        "action_executed": "ScaleDown",
        "expected_pass": True,
    },
]

for tc in resource_test_cases:
    state = make_base_state()
    state["resource_type"] = tc["resource_type"]
    state["raw_metrics"] = tc["raw_metrics"]
    state["action_executed"] = tc["action_executed"]
    state["action_result"] = {"status": "success", "ResponseMetadata": {"HTTPStatusCode": 200}}

    result = qa_node(state)

    status = "PASS" if result["qa_passed"] == tc["expected_pass"] else "FAIL"
    print(f"\n[ {tc['name']} ] - {status}")
    print(f"  qa_passed: {result['qa_passed']} (expected: {tc['expected_pass']})")


# ── 테스트 헬퍼 함수 테스트 ────────────────────────────────────────────────

print("\n" + "=" * 60)
print("테스트 헬퍼 함수 테스트")
print("=" * 60)

print("\n[ qa_node_force_pass ]")
state = make_base_state()
result = qa_node_force_pass(state)
print(f"  qa_passed: {result['qa_passed']} (expected: True)")

print("\n[ qa_node_force_fail ]")
state = make_base_state()
result = qa_node_force_fail(state)
print(f"  qa_passed: {result['qa_passed']} (expected: False)")
print(f"  rollback_count: {result['rollback_count']} (expected: 1)")


# ── 전체 파이프라인 통합 테스트 (선택적) ────────────────────────────────────

print("\n" + "=" * 60)
print("전체 파이프라인 통합 테스트 (QA 노드 포함)")
print("=" * 60)

try:
    from pipeline.graph import build_graph
    from pipeline.QA_agent import qa_node as real_qa_node

    # QA 노드를 실제 구현으로 교체한 그래프 빌드
    # 참고: graph.py에서 dummy_nodes의 qa_node를 사용하므로
    # 실제 통합 테스트 시 graph.py 수정 필요

    print("\n[INFO] 전체 파이프라인 통합 테스트는 graph.py 수정 후 가능")
    print("[INFO] graph.py에서 qa_node를 QA_agent.qa_node로 교체 필요")

except ImportError as e:
    print(f"\n[SKIP] 파이프라인 import 실패: {e}")


print("\n" + "=" * 60)
print("테스트 완료")
print("=" * 60)
