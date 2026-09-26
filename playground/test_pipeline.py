"""
파이프라인 전체 흐름 테스트 스크립트
실제 agent 노드들을 사용해서 전체 연결과 루프를 확인합니다.
"""

from pipeline.graph import build_graph
from schema.state import PipelineState

# 테스트용 초기 상태 (이상 탐지가 트리거되도록 설정)
test_state: PipelineState = {
    # Step 0: 원본 데이터
    "resource_id": "i-test123456",
    "resource_type": "EC2",
    "raw_metrics": {
        "cpu_utilization": [20.0, 25.0, 30.0, 85.0, 90.0, 95.0],  # 급증 패턴
        "network_in": [1000.0, 1200.0, 1500.0, 50000.0, 60000.0, 70000.0],  # 급증
        "network_out": [500.0, 600.0, 700.0, 800.0, 900.0, 1000.0],
        "cost": [0.5, 0.5, 0.5, 0.6, 0.7, 2.0],  # 비용 급증
    },
    "timestamp": "2024-01-15T10:00:00Z",

    # 나머지 필드들 초기화
    "anomaly_flag": False,
    "anomaly_score_zscore": None,
    "anomaly_score_iforest": None,
    "triggered_metrics": [],
    "anomaly_type": None,
    "classification_reasoning": None,
    "interim_action_taken": None,
    "candidate_actions": [],
    "selected_action": None,
    "risk_level": None,
    "requires_approval": False,
    "decision_reasoning": None,
    "pre_action_snapshot": None,
    "action_executed": None,
    "action_result": None,
    "qa_passed": None,
    "sla_check_result": None,
    "rollback_count": 0,
    "log_entries": [],
}


def run_test():
    print("=" * 60)
    print("파이프라인 테스트 시작")
    print("=" * 60)

    # 그래프 빌드 및 컴파일
    graph = build_graph()
    app = graph.compile()

    print("\n[그래프 구조]")
    print(app.get_graph().draw_ascii())

    print("\n[실행 시작]")
    print("-" * 60)

    # 스트리밍 모드로 각 노드 실행 추적
    step = 0
    for event in app.stream(test_state):
        step += 1
        node_name = list(event.keys())[0]
        state = event[node_name]

        print(f"\nStep {step}: {node_name}")
        print("-" * 40)

        if node_name == "detection":
            print(f"  anomaly_flag: {state.get('anomaly_flag')}")
            print(f"  z-score: {state.get('anomaly_score_zscore')}")
            print(f"  iforest: {state.get('anomaly_score_iforest')}")
            print(f"  triggered: {state.get('triggered_metrics')}")

        elif node_name == "classification":
            print(f"  anomaly_type: {state.get('anomaly_type')}")
            print(f"  reasoning: {state.get('classification_reasoning')}")

        elif node_name == "decision":
            print(f"  selected_action: {state.get('selected_action')}")
            print(f"  risk_level: {state.get('risk_level')}")
            print(f"  requires_approval: {state.get('requires_approval')}")
            print(f"  candidates: {[c['action'] for c in state.get('candidate_actions', [])]}")

        elif node_name == "action":
            print(f"  action_executed: {state.get('action_executed')}")
            print(f"  action_result: {state.get('action_result')}")

        elif node_name == "qa":
            print(f"  qa_passed: {state.get('qa_passed')}")
            print(f"  rollback_count: {state.get('rollback_count')}")
            print(f"  sla_check: {state.get('sla_check_result')}")

        elif node_name == "logging":
            print(f"  log_entries count: {len(state.get('log_entries', []))}")

    print("\n" + "=" * 60)
    print("파이프라인 테스트 완료")
    print("=" * 60)

    # 최종 로그 출력
    final_logs = state.get("log_entries", [])
    if final_logs:
        print("\n[최종 로그]")
        for log in final_logs:
            print(f"  {log}")


def run_rollback_loop_test():
    """QA 실패 시 롤백 루프 테스트 (qa → action → qa 반복)"""
    from pipeline.QA_agent import qa_node_force_fail

    print("\n" + "=" * 60)
    print("롤백 루프 테스트 시작 (QA 항상 실패)")
    print("=" * 60)

    # 롤백 테스트용 상태 (requires_approval=False로 액션 실행되도록)
    rollback_state: PipelineState = {
        "resource_id": "i-test789",
        "resource_type": "EC2",
        "raw_metrics": {
            "cpu_utilization": [20.0, 25.0, 30.0, 35.0, 40.0, 45.0],
            "network_in": [1000.0] * 6,
            "network_out": [500.0] * 6,
            "cost": [0.5, 0.5, 0.5, 2.0, 2.5, 3.0],  # 비용 급증
        },
        "timestamp": "2024-01-15T10:00:00Z",
        "anomaly_flag": False,
        "anomaly_score_zscore": None,
        "anomaly_score_iforest": None,
        "triggered_metrics": [],
        "anomaly_type": None,
        "classification_reasoning": None,
        "interim_action_taken": None,
        "candidate_actions": [],
        "selected_action": None,
        "risk_level": None,
        "requires_approval": False,
        "decision_reasoning": None,
        "pre_action_snapshot": None,
        "action_executed": None,
        "action_result": None,
        "qa_passed": None,
        "sla_check_result": None,
        "rollback_count": 0,
        "log_entries": [],
    }

    # QA 실패 노드로 그래프 빌드
    graph = build_graph(qa_node_override=qa_node_force_fail)
    app = graph.compile()

    print("\n[실행 시작 - QA 실패 시 action으로 롤백 (최대 2회)]")
    print("-" * 60)

    step = 0
    for event in app.stream(rollback_state):
        step += 1
        node_name = list(event.keys())[0]
        state = event[node_name]

        print(f"\nStep {step}: {node_name}")

        if node_name == "qa":
            print(f"  qa_passed: {state.get('qa_passed')}")
            print(f"  rollback_count: {state.get('rollback_count')}")
            if state.get('rollback_count', 0) >= 2:
                print("  [!] 롤백 2회 초과 → logging으로 이동 (관리자 알림)")

        elif node_name == "action":
            print(f"  action_executed: {state.get('action_executed')}")

    print("\n" + "=" * 60)
    print("롤백 루프 테스트 완료")
    print(f"총 {step}개 노드 실행")
    print("=" * 60)


if __name__ == "__main__":
    run_test()

    # 롤백 루프 테스트 실행 여부
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--rollback":
        run_rollback_loop_test()
