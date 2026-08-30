"""
Rule Evolution 시뮬레이션 테스트
--------------------------------
Mock State를 사용하여 자가진화 로직을 검증

테스트 시나리오:
1. 규칙 성능 기록 (win/lose)
2. 저성능 규칙 탐지 및 비활성화
3. LLM 규칙 개선
4. 유사 규칙 통합

사용법:
    python -m playground.test_rule_evolution_simulation

주의:
    - 실제 PostgreSQL DB 사용 (.env 설정 필요)
    - 실제 Gemini API 사용 (API 키 필요)
    - 테스트 후 DB 및 규칙 파일 자동 정리
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

# 프로젝트 루트 추가
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

import psycopg2

from schema.state import PipelineState


# ══════════════════════════════════════════════════════════════════════════════
# 설정
# ══════════════════════════════════════════════════════════════════════════════

# 테스트용 규칙 ID 접두사 (정리 시 사용)
TEST_RULE_PREFIX = "TEST-EVO-"

# 테스트 시나리오별 반복 횟수
SIMULATION_RUNS = 5  # 5회 실행하여 3회 이상 누적되도록


# ══════════════════════════════════════════════════════════════════════════════
# DB 연결
# ══════════════════════════════════════════════════════════════════════════════

def get_db_connection():
    """PostgreSQL 연결."""
    return psycopg2.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=os.environ.get("PGPORT", "5432"),
        dbname=os.environ.get("PGDATABASE", "cloud_anomaly_agent"),
        user=os.environ.get("PGUSER", "postgres"),
        password=os.environ.get("PGPASSWORD", ""),
    )


def cleanup_test_data(conn):
    """테스트 데이터 정리."""
    with conn.cursor() as cur:
        # rule_stats에서 테스트 데이터 삭제
        cur.execute("DELETE FROM rule_stats WHERE rule_id LIKE %s", (f"{TEST_RULE_PREFIX}%",))
        deleted = cur.rowcount
        print(f"  [정리] rule_stats에서 {deleted}건 삭제")
    conn.commit()


def cleanup_test_rules():
    """테스트로 생성된 규칙 파일 정리."""
    rules_dir = os.path.join(os.path.dirname(__file__), "..", "schema", "rules")

    for filename in ["classification_rules.json", "qa_rules.json"]:
        filepath = os.path.join(rules_dir, filename)
        if not os.path.exists(filepath):
            continue

        with open(filepath, "r", encoding="utf-8") as f:
            rules = json.load(f)

        original_count = len(rules)
        rules = [r for r in rules if not r.get("rule_id", "").startswith(TEST_RULE_PREFIX)]

        if len(rules) < original_count:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(rules, f, ensure_ascii=False, indent=2)
            print(f"  [정리] {filename}에서 {original_count - len(rules)}건 삭제")


# ══════════════════════════════════════════════════════════════════════════════
# Mock State 생성
# ══════════════════════════════════════════════════════════════════════════════

def create_mock_state(
    rule_id: str,
    rule_type: str = "classification",
    qa_passed: bool = True,
    cost_ok: bool = True,
    availability_ok: bool = True,
    rollback_count: int = 0,
    anomaly_type: str = "risk_cost",
    resource_type: str = "EC2",
) -> PipelineState:
    """테스트용 Mock State 생성."""
    state = {
        "resource_id": f"test-resource-{rule_id}",
        "resource_type": resource_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "anomaly_flag": True,
        "anomaly_type": anomaly_type,
        "qa_passed": qa_passed,
        "sla_check_result": {
            "cpu_ok": True,
            "cost_ok": cost_ok,
            "availability_ok": availability_ok,
            "detail": "테스트",
        },
        "rollback_count": rollback_count,
        "selected_action": "Resize",
        "action_executed": "Resize",
        "action_result": {"status": "success"},
    }

    # 규칙 타입에 따라 적절한 필드 설정
    if rule_type == "classification":
        state["matched_rule_id"] = rule_id
        state["qa_matched_rule_id"] = None
    elif rule_type == "qa":
        state["matched_rule_id"] = None
        state["qa_matched_rule_id"] = rule_id
    else:
        state["matched_rule_id"] = None
        state["qa_matched_rule_id"] = None

    return state


# ══════════════════════════════════════════════════════════════════════════════
# 테스트 시나리오
# ══════════════════════════════════════════════════════════════════════════════

def test_scenario_1_rule_stats_recording(conn):
    """
    시나리오 1: 규칙 성능 기록 테스트

    - 같은 규칙을 5번 실행 (3 win, 2 lose)
    - rule_stats에 올바르게 기록되는지 확인
    - win_rate = 60%가 되는지 확인
    """
    print("\n" + "=" * 60)
    print("시나리오 1: 규칙 성능 기록 테스트")
    print("=" * 60)

    from pipeline.rule_stats_logger import record_rule_stats, is_win

    rule_id = f"{TEST_RULE_PREFIX}STATS-001"

    # 5번 실행: win 3회, lose 2회
    results = []
    for i in range(5):
        is_success = i < 3  # 처음 3번은 성공, 나머지 2번은 실패

        state = create_mock_state(
            rule_id=rule_id,
            qa_passed=is_success,
            cost_ok=is_success,
        )

        result = record_rule_stats(state, conn)
        conn.commit()
        results.append(result)

        print(f"  실행 {i+1}: is_win={result['is_win']}, win_rate={result['current_win_rate']:.1%}")

    # 검증
    final_result = results[-1]
    assert final_result["total_runs"] == 5, f"total_runs가 5여야 함 (실제: {final_result['total_runs']})"
    assert abs(final_result["current_win_rate"] - 0.6) < 0.01, f"win_rate가 60%여야 함 (실제: {final_result['current_win_rate']:.1%})"

    print("\n  [PASS] 규칙 성능 기록 정상")
    return True


def test_scenario_2_evolution_trigger(conn):
    """
    시나리오 2: 자가진화 트리거 테스트

    - 규칙을 3번 실행하여 진화 트리거 조건 충족
    - trigger_evolution이 True가 되는지 확인
    """
    print("\n" + "=" * 60)
    print("시나리오 2: 자가진화 트리거 테스트")
    print("=" * 60)

    from pipeline.rule_stats_logger import record_rule_stats

    rule_id = f"{TEST_RULE_PREFIX}TRIGGER-001"

    # 3번 실행
    for i in range(3):
        state = create_mock_state(rule_id=rule_id, qa_passed=True)
        result = record_rule_stats(state, conn)
        conn.commit()

        print(f"  실행 {i+1}: trigger_evolution={result['trigger_evolution']}")

        # 3번째 실행에서 트리거 발동해야 함
        if i == 2:
            assert result["trigger_evolution"] == True, "3회 실행 후 트리거가 발동해야 함"

    print("\n  [PASS] 진화 트리거 정상 발동")
    return True


def test_scenario_3_underperforming_detection(conn):
    """
    시나리오 3: 저성능 규칙 탐지 테스트

    - win_rate 40%인 규칙 생성 (5회 중 2회 성공)
    - 저성능 규칙으로 탐지되는지 확인
    """
    print("\n" + "=" * 60)
    print("시나리오 3: 저성능 규칙 탐지 테스트")
    print("=" * 60)

    from pipeline.rule_stats_logger import record_rule_stats, get_all_underperforming_rules

    rule_id = f"{TEST_RULE_PREFIX}UNDERPERF-001"

    # 5번 실행: win 2회, lose 3회 (win_rate = 40%)
    for i in range(5):
        is_success = i < 2  # 처음 2번만 성공
        state = create_mock_state(rule_id=rule_id, qa_passed=is_success, cost_ok=is_success)
        record_rule_stats(state, conn)
        conn.commit()

    # 저성능 규칙 탐지
    underperforming = get_all_underperforming_rules(conn, threshold=0.6, min_runs=3)

    print(f"  저성능 규칙 목록: {[r['rule_id'] for r in underperforming]}")

    # 검증
    rule_ids = [r["rule_id"] for r in underperforming]
    assert rule_id in rule_ids, f"{rule_id}가 저성능 규칙으로 탐지되어야 함"

    target_rule = next(r for r in underperforming if r["rule_id"] == rule_id)
    assert abs(target_rule["win_rate"] - 0.4) < 0.01, f"win_rate가 40%여야 함"

    print(f"\n  [PASS] 저성능 규칙 탐지 정상 (win_rate={target_rule['win_rate']:.1%})")
    return True


def test_scenario_4_llm_rule_improvement(conn):
    """
    시나리오 4: LLM 규칙 개선 테스트

    - 실제 Gemini API를 호출하여 규칙 개선안 받기
    - improve/disable/merge 중 하나의 추천을 받는지 확인
    """
    print("\n" + "=" * 60)
    print("시나리오 4: LLM 규칙 개선 테스트")
    print("=" * 60)

    from pipeline.rule_evolution_engine import improve_rule_with_llm

    # 테스트용 저성능 규칙
    rule = {
        "rule_id": f"{TEST_RULE_PREFIX}LLM-001",
        "rule_type": "classification",
        "description": "CPU 사용률 급증 시 비용 이상으로 분류",
        "resource_types": ["EC2"],
        "conditions": {
            "triggered_metrics": ["cpu_utilization"],
        },
        "result": {
            "anomaly_type": "risk_cost",
        },
        "enabled": True,
    }

    rule_stats = {
        "rule_id": rule["rule_id"],
        "total_runs": 10,
        "total_wins": 3,
        "win_rate": 0.3,
    }

    similar_rules = []  # 유사 규칙 없음

    print("  LLM 호출 중...")
    result = improve_rule_with_llm(rule, rule_stats, similar_rules)

    print(f"  LLM 응답:")
    print(f"    - recommendation: {result.get('recommendation')}")
    print(f"    - analysis: {result.get('analysis', '')[:100]}...")
    print(f"    - reasoning: {result.get('reasoning', '')[:100]}...")

    # 검증
    valid_recommendations = ["improve", "disable", "merge"]
    assert result.get("recommendation") in valid_recommendations, \
        f"recommendation이 {valid_recommendations} 중 하나여야 함"

    print(f"\n  [PASS] LLM 규칙 개선 정상 (추천: {result.get('recommendation')})")
    return True


def test_scenario_5_rule_disable(conn):
    """
    시나리오 5: 규칙 비활성화 테스트

    - 테스트 규칙을 JSON에 추가
    - disable_rule() 호출하여 비활성화
    - enabled=false로 변경되었는지 확인
    """
    print("\n" + "=" * 60)
    print("시나리오 5: 규칙 비활성화 테스트")
    print("=" * 60)

    from pipeline.rule_evolution_engine import disable_rule, _load_rule_by_id, _load_all_rules, _save_rules

    rule_id = f"{TEST_RULE_PREFIX}DISABLE-001"

    # 테스트 규칙 추가
    test_rule = {
        "rule_id": rule_id,
        "rule_type": "classification",
        "description": "테스트용 비활성화 규칙",
        "resource_types": ["EC2"],
        "conditions": {"triggered_metrics": ["cpu_utilization"]},
        "result": {"anomaly_type": "risk_cost"},
        "priority": 999,
        "enabled": True,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "author": "test",
        "rationale": "테스트용",
    }

    rules = _load_all_rules("classification")
    rules.append(test_rule)
    _save_rules(rules, "classification")
    print(f"  테스트 규칙 추가: {rule_id}")

    # 비활성화
    success = disable_rule(rule_id, "classification", "테스트 비활성화")
    assert success, "disable_rule이 성공해야 함"
    print(f"  규칙 비활성화 실행")

    # 검증
    rules = _load_all_rules("classification")
    target = next((r for r in rules if r["rule_id"] == rule_id), None)

    assert target is not None, "규칙이 존재해야 함"
    assert target.get("enabled") == False, "enabled가 False여야 함"
    assert "disabled_at" in target, "disabled_at 필드가 있어야 함"

    print(f"\n  [PASS] 규칙 비활성화 정상")
    return True


def test_scenario_6_similar_rules_detection():
    """
    시나리오 6: 유사 규칙 탐지 테스트

    - 조건이 비슷한 규칙들 생성
    - find_similar_rules()로 유사 규칙 탐지
    """
    print("\n" + "=" * 60)
    print("시나리오 6: 유사 규칙 탐지 테스트")
    print("=" * 60)

    from pipeline.rule_evolution_engine import find_similar_rules

    rule = {
        "rule_id": f"{TEST_RULE_PREFIX}SIM-001",
        "rule_type": "classification",
        "resource_types": ["EC2"],
        "conditions": {"triggered_metrics": ["cpu_utilization", "memory_utilization"]},
        "result": {"anomaly_type": "risk_cost"},
        "enabled": True,
    }

    all_rules = [
        rule,
        {
            "rule_id": f"{TEST_RULE_PREFIX}SIM-002",
            "rule_type": "classification",
            "resource_types": ["EC2"],
            "conditions": {"triggered_metrics": ["cpu_utilization"]},  # 50% 겹침
            "result": {"anomaly_type": "risk_cost"},
            "enabled": True,
        },
        {
            "rule_id": f"{TEST_RULE_PREFIX}SIM-003",
            "rule_type": "classification",
            "resource_types": ["Lambda"],  # 다른 리소스
            "conditions": {"triggered_metrics": ["invocations"]},
            "result": {"anomaly_type": "risk_cost"},
            "enabled": True,
        },
    ]

    similar = find_similar_rules(rule, all_rules)
    similar_ids = [s["rule"]["rule_id"] for s in similar]

    print(f"  유사 규칙: {similar_ids}")

    # 검증
    assert f"{TEST_RULE_PREFIX}SIM-002" in similar_ids, "SIM-002는 유사 규칙으로 탐지되어야 함"
    assert f"{TEST_RULE_PREFIX}SIM-003" not in similar_ids, "SIM-003은 다른 리소스이므로 제외되어야 함"

    print(f"\n  [PASS] 유사 규칙 탐지 정상")
    return True


def test_scenario_7_full_evolution_cycle(conn):
    """
    시나리오 7: 전체 진화 사이클 테스트

    1. 저성능 규칙 생성 (5회 실행, 2회 성공 = 40%)
    2. 진화 트리거 발동
    3. LLM 개선안 적용 (또는 비활성화)
    4. 결과 확인
    """
    print("\n" + "=" * 60)
    print("시나리오 7: 전체 진화 사이클 테스트")
    print("=" * 60)

    from pipeline.rule_stats_logger import record_rule_stats, mark_evolution_completed
    from pipeline.rule_evolution_engine import (
        trigger_evolution,
        _load_all_rules,
        _save_rules,
    )

    rule_id = f"{TEST_RULE_PREFIX}FULL-001"

    # 1. 테스트 규칙 추가
    test_rule = {
        "rule_id": rule_id,
        "rule_type": "classification",
        "description": "전체 사이클 테스트용 규칙 - CPU 급증",
        "resource_types": ["EC2"],
        "conditions": {"triggered_metrics": ["cpu_utilization"]},
        "result": {"anomaly_type": "risk_cost", "reasoning_template": "CPU 급증"},
        "priority": 999,
        "enabled": True,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "author": "test",
        "rationale": "테스트용",
    }

    rules = _load_all_rules("classification")
    rules.append(test_rule)
    _save_rules(rules, "classification")
    print(f"  1. 테스트 규칙 추가: {rule_id}")

    # 2. 저성능 시뮬레이션: 5회 실행, 2회만 성공 (win_rate = 40%)
    print("  2. 저성능 시뮬레이션 (5회 실행, 2회 성공)")
    for i in range(5):
        is_success = i < 2
        state = create_mock_state(rule_id=rule_id, qa_passed=is_success, cost_ok=is_success)
        result = record_rule_stats(state, conn)
        conn.commit()
        print(f"     - 실행 {i+1}: is_win={result['is_win']}, trigger={result['trigger_evolution']}")

    # 3. 진화 트리거 실행
    print("  3. 진화 트리거 실행")
    evolution_result = trigger_evolution(rule_id, conn)
    conn.commit()

    print(f"     - success: {evolution_result['success']}")
    print(f"     - actions: {evolution_result['actions_taken']}")

    # 4. 결과 확인
    print("  4. 결과 확인")
    rules = _load_all_rules("classification")
    target = next((r for r in rules if r["rule_id"] == rule_id), None)

    if target:
        print(f"     - enabled: {target.get('enabled')}")
        if not target.get("enabled"):
            print(f"     - disabled_reason: {target.get('disabled_reason', 'N/A')}")

    print(f"\n  [PASS] 전체 진화 사이클 완료")
    return True


# ══════════════════════════════════════════════════════════════════════════════
# 메인
# ══════════════════════════════════════════════════════════════════════════════

def main():
    """모든 시뮬레이션 테스트 실행."""
    print("=" * 60)
    print("Rule Evolution 시뮬레이션 테스트")
    print("=" * 60)
    print(f"시작 시각: {datetime.now().isoformat()}")

    conn = None
    try:
        # DB 연결
        print("\nDB 연결 중...")
        conn = get_db_connection()
        print("DB 연결 성공")

        # 기존 테스트 데이터 정리
        print("\n기존 테스트 데이터 정리 중...")
        cleanup_test_data(conn)
        cleanup_test_rules()

        # 테스트 실행
        results = []

        results.append(("시나리오 1: 규칙 성능 기록", test_scenario_1_rule_stats_recording(conn)))
        results.append(("시나리오 2: 자가진화 트리거", test_scenario_2_evolution_trigger(conn)))
        results.append(("시나리오 3: 저성능 규칙 탐지", test_scenario_3_underperforming_detection(conn)))
        results.append(("시나리오 4: LLM 규칙 개선", test_scenario_4_llm_rule_improvement(conn)))
        results.append(("시나리오 5: 규칙 비활성화", test_scenario_5_rule_disable(conn)))
        results.append(("시나리오 6: 유사 규칙 탐지", test_scenario_6_similar_rules_detection()))
        results.append(("시나리오 7: 전체 진화 사이클", test_scenario_7_full_evolution_cycle(conn)))

        # 결과 요약
        print("\n" + "=" * 60)
        print("테스트 결과 요약")
        print("=" * 60)

        all_passed = True
        for name, passed in results:
            status = "PASS" if passed else "FAIL"
            print(f"  [{status}] {name}")
            if not passed:
                all_passed = False

        print("\n" + "=" * 60)
        if all_passed:
            print("모든 시뮬레이션 테스트 통과!")
        else:
            print("일부 테스트 실패!")
        print("=" * 60)

    except Exception as e:
        print(f"\n[ERROR] 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        # 정리
        print("\n테스트 데이터 정리 중...")
        if conn:
            cleanup_test_data(conn)
            conn.close()
        cleanup_test_rules()
        print("정리 완료")

    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
