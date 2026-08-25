"""
Rule Evolution 테스트
---------------------
자가진화 로직 단위 테스트 및 통합 시나리오 테스트

사용법:
    python -m playground.test_rule_evolution
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from datetime import datetime
from typing import Any
from unittest.mock import MagicMock, patch

# 프로젝트 루트 추가
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from schema.state import PipelineState


# ══════════════════════════════════════════════════════════════════════════════
# 테스트 유틸리티
# ══════════════════════════════════════════════════════════════════════════════

def create_test_state(
    qa_passed: bool = True,
    cost_ok: bool = True,
    availability_ok: bool = True,
    rollback_count: int = 0,
    matched_rule_id: str = None,
    qa_matched_rule_id: str = None,
    anomaly_type: str = "risk_cost",
) -> PipelineState:
    """테스트용 State 생성."""
    return {
        "resource_id": "test-resource-001",
        "resource_type": "EC2",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "qa_passed": qa_passed,
        "sla_check_result": {
            "cpu_ok": True,
            "cost_ok": cost_ok,
            "availability_ok": availability_ok,
            "detail": "테스트",
        },
        "rollback_count": rollback_count,
        "matched_rule_id": matched_rule_id,
        "qa_matched_rule_id": qa_matched_rule_id,
        "anomaly_type": anomaly_type,
    }


def create_mock_conn():
    """Mock PostgreSQL 연결 객체 생성."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn, cursor


# ══════════════════════════════════════════════════════════════════════════════
# rule_stats_logger 테스트
# ══════════════════════════════════════════════════════════════════════════════

def test_is_win():
    """is_win() 함수 테스트."""
    from pipeline.rule_stats_logger import is_win

    print("\n=== test_is_win ===")

    # Case 1: 모든 조건 충족 → win
    state = create_test_state(qa_passed=True, cost_ok=True, availability_ok=True, rollback_count=0)
    assert is_win(state) == True, "모든 조건 충족 시 win이어야 함"
    print("  [PASS] 모든 조건 충족 → win")

    # Case 2: qa_passed=False → lose
    state = create_test_state(qa_passed=False)
    assert is_win(state) == False, "qa_passed=False면 lose"
    print("  [PASS] qa_passed=False → lose")

    # Case 3: cost_ok=False → lose
    state = create_test_state(qa_passed=True, cost_ok=False)
    assert is_win(state) == False, "cost_ok=False면 lose"
    print("  [PASS] cost_ok=False → lose")

    # Case 4: availability_ok=False → lose
    state = create_test_state(qa_passed=True, availability_ok=False)
    assert is_win(state) == False, "availability_ok=False면 lose"
    print("  [PASS] availability_ok=False → lose")

    # Case 5: rollback_count > 0 → lose
    state = create_test_state(qa_passed=True, rollback_count=1)
    assert is_win(state) == False, "rollback_count > 0이면 lose"
    print("  [PASS] rollback_count > 0 → lose")


def test_get_rule_id_from_state():
    """get_rule_id_from_state() 함수 테스트."""
    from pipeline.rule_stats_logger import get_rule_id_from_state

    print("\n=== test_get_rule_id_from_state ===")

    # Case 1: Classification 규칙 매칭
    state = create_test_state(matched_rule_id="CLF-001")
    rule_id, rule_type = get_rule_id_from_state(state)
    assert rule_id == "CLF-001", "Classification 규칙 ID 추출 실패"
    assert rule_type == "classification", "Classification 타입 추출 실패"
    print("  [PASS] Classification 규칙 매칭")

    # Case 2: QA 규칙 매칭
    state = create_test_state(qa_matched_rule_id="QA-002")
    rule_id, rule_type = get_rule_id_from_state(state)
    assert rule_id == "QA-002", "QA 규칙 ID 추출 실패"
    assert rule_type == "qa", "QA 타입 추출 실패"
    print("  [PASS] QA 규칙 매칭")

    # Case 3: LLM 판단 (규칙 미매칭)
    state = create_test_state(anomaly_type="risk_security")
    rule_id, rule_type = get_rule_id_from_state(state)
    assert rule_id == "LLM-risk_security", "LLM 판단 ID 생성 실패"
    assert rule_type == "llm", "LLM 타입 추출 실패"
    print("  [PASS] LLM 판단")


def test_evolution_trigger_check():
    """_check_evolution_trigger() 함수 테스트."""
    from pipeline.rule_stats_logger import _check_evolution_trigger

    print("\n=== test_evolution_trigger_check ===")

    conn, cursor = create_mock_conn()

    # Case 1: 3회 이상 실행 → 트리거
    cursor.fetchone.return_value = (5, 2)  # total_runs=5, last_evo=2 → 3회 신규
    result = _check_evolution_trigger(conn, "CLF-001", n=3)
    assert result == True, "3회 이상 신규 실행 시 트리거되어야 함"
    print("  [PASS] 3회 이상 신규 실행 → 트리거")

    # Case 2: 2회만 실행 → 트리거 안 됨
    cursor.fetchone.return_value = (4, 2)  # total_runs=4, last_evo=2 → 2회 신규
    result = _check_evolution_trigger(conn, "CLF-001", n=3)
    assert result == False, "3회 미만이면 트리거되면 안 됨"
    print("  [PASS] 2회만 실행 → 트리거 안 됨")

    # Case 3: 규칙 없음 → 트리거 안 됨
    cursor.fetchone.return_value = None
    result = _check_evolution_trigger(conn, "CLF-999", n=3)
    assert result == False, "규칙이 없으면 트리거되면 안 됨"
    print("  [PASS] 규칙 없음 → 트리거 안 됨")


# ══════════════════════════════════════════════════════════════════════════════
# rule_evolution_engine 테스트
# ══════════════════════════════════════════════════════════════════════════════

def test_find_similar_rules():
    """find_similar_rules() 함수 테스트."""
    from pipeline.rule_evolution_engine import find_similar_rules

    print("\n=== test_find_similar_rules ===")

    rule = {
        "rule_id": "CLF-001",
        "rule_type": "classification",
        "resource_types": ["EC2"],
        "conditions": {
            "triggered_metrics": ["cpu_utilization", "memory_utilization"],
        },
        "result": {
            "anomaly_type": "risk_cost",
        },
        "enabled": True,
    }

    all_rules = [
        rule,
        {
            "rule_id": "CLF-002",
            "rule_type": "classification",
            "resource_types": ["EC2"],
            "conditions": {
                "triggered_metrics": ["cpu_utilization"],  # 50% 겹침
            },
            "result": {
                "anomaly_type": "risk_cost",
            },
            "enabled": True,
        },
        {
            "rule_id": "CLF-003",
            "rule_type": "classification",
            "resource_types": ["Lambda"],  # 다른 리소스
            "conditions": {
                "triggered_metrics": ["invocations"],
            },
            "result": {
                "anomaly_type": "risk_cost",
            },
            "enabled": True,
        },
        {
            "rule_id": "CLF-004",
            "rule_type": "classification",
            "resource_types": ["EC2"],
            "conditions": {
                "triggered_metrics": ["cpu_utilization", "memory_utilization"],  # 100% 겹침
            },
            "result": {
                "anomaly_type": "risk_security",  # 다른 결과
            },
            "enabled": True,
        },
    ]

    similar = find_similar_rules(rule, all_rules)

    # CLF-002와 CLF-004는 유사해야 함
    similar_ids = [s["rule"]["rule_id"] for s in similar]
    assert "CLF-002" in similar_ids, "CLF-002는 유사 규칙으로 감지되어야 함"
    assert "CLF-004" in similar_ids, "CLF-004는 유사 규칙으로 감지되어야 함"
    assert "CLF-003" not in similar_ids, "CLF-003은 다른 리소스이므로 제외되어야 함"
    print(f"  [PASS] 유사 규칙 탐지: {similar_ids}")


def test_detect_rule_conflicts():
    """detect_rule_conflicts() 함수 테스트."""
    from pipeline.rule_evolution_engine import detect_rule_conflicts

    print("\n=== test_detect_rule_conflicts ===")

    rules = [
        {
            "rule_id": "CLF-001",
            "rule_type": "classification",
            "conditions": {
                "triggered_metrics": ["cpu_utilization"],
            },
            "result": {
                "anomaly_type": "risk_cost",
            },
            "enabled": True,
        },
        {
            "rule_id": "CLF-002",
            "rule_type": "classification",
            "conditions": {
                "triggered_metrics": ["cpu_utilization"],  # 같은 조건
            },
            "result": {
                "anomaly_type": "risk_security",  # 다른 결과 → 충돌!
            },
            "enabled": True,
        },
    ]

    conflicts = detect_rule_conflicts(rules)
    assert len(conflicts) > 0, "동일 조건 + 다른 결과는 충돌로 감지되어야 함"
    print(f"  [PASS] 충돌 감지: {conflicts}")


def test_backup_and_rollback():
    """백업 및 롤백 테스트."""
    from pipeline.rule_evolution_engine import (
        _backup_rules,
        rollback_rules,
        list_backups,
        _HISTORY_DIR,
    )

    print("\n=== test_backup_and_rollback ===")

    # 임시 디렉토리 사용
    with tempfile.TemporaryDirectory() as tmpdir:
        # _HISTORY_DIR을 임시 디렉토리로 패치
        with patch("pipeline.rule_evolution_engine._HISTORY_DIR", tmpdir):
            rules = [
                {"rule_id": "TEST-001", "enabled": True, "description": "테스트 규칙"},
            ]

            # 백업 생성
            backup_path = _backup_rules(rules, "classification")
            assert os.path.exists(backup_path), "백업 파일이 생성되어야 함"
            print(f"  [PASS] 백업 생성: {backup_path}")

            # 백업 목록 조회
            backups = list_backups("classification")
            assert len(backups) > 0, "백업 목록에 항목이 있어야 함"
            print(f"  [PASS] 백업 목록 조회: {backups}")


# ══════════════════════════════════════════════════════════════════════════════
# 통합 시나리오 테스트
# ══════════════════════════════════════════════════════════════════════════════

def test_integration_scenario():
    """통합 시나리오: 저성능 규칙 탐지 → LLM 개선 → 적용."""
    from pipeline.rule_stats_logger import is_win, get_rule_id_from_state

    print("\n=== test_integration_scenario ===")

    # 시나리오: 규칙 CLF-001이 5번 실행되어 2번만 성공 (win_rate=40%)
    wins = 0
    losses = 0

    for i in range(5):
        state = create_test_state(
            qa_passed=(i < 2),  # 처음 2번만 성공
            matched_rule_id="CLF-001",
        )
        if is_win(state):
            wins += 1
        else:
            losses += 1

    win_rate = wins / (wins + losses)
    print(f"  시뮬레이션 결과: wins={wins}, losses={losses}, win_rate={win_rate:.1%}")

    assert win_rate == 0.4, "win_rate가 40%여야 함"
    assert win_rate < 0.6, "win_rate가 60% 미만이므로 저성능 규칙"
    print("  [PASS] 저성능 규칙 시나리오 확인")


# ══════════════════════════════════════════════════════════════════════════════
# 메인
# ══════════════════════════════════════════════════════════════════════════════

def main():
    """모든 테스트 실행."""
    print("=" * 60)
    print("Rule Evolution 테스트 시작")
    print("=" * 60)

    try:
        # rule_stats_logger 테스트
        test_is_win()
        test_get_rule_id_from_state()
        test_evolution_trigger_check()

        # rule_evolution_engine 테스트
        test_find_similar_rules()
        test_detect_rule_conflicts()
        test_backup_and_rollback()

        # 통합 테스트
        test_integration_scenario()

        print("\n" + "=" * 60)
        print("모든 테스트 통과!")
        print("=" * 60)

    except AssertionError as e:
        print(f"\n[FAIL] 테스트 실패: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] 예외 발생: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
