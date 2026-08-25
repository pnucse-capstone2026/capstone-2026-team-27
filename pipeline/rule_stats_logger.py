"""
Rule Stats Logger
-----------------
규칙 실행 결과를 rule_stats 테이블에 기록하고, 자가진화 트리거 조건을 감지.

주요 기능:
1. win/lose 판단: qa_passed + cost_ok + availability_ok + rollback_count=0
2. rule_stats 테이블 UPSERT (PostgreSQL ON CONFLICT 활용)
3. n건 누적 시 자가진화 트리거 감지
"""

from __future__ import annotations

from typing import Any, Optional

from schema.state import PipelineState


# ── 설정값 ────────────────────────────────────────────────────────────────────
EVOLUTION_TRIGGER_COUNT = 3  # 자가진화 트리거를 위한 최소 실행 횟수
WIN_RATE_THRESHOLD = 0.6     # 저성능 규칙 판단 임계값 (60%)


def is_win(state: PipelineState) -> bool:
    """
    win 조건 판단.

    win 조건 (모두 충족 시):
    1. qa_passed = True
    2. sla_check_result.cost_ok = True (비용 감소 또는 유지)
    3. sla_check_result.availability_ok = True (가용성 유지)
    4. rollback_count = 0 (첫 시도에 성공)

    Returns:
        True if win, False otherwise
    """
    # qa_passed 체크
    if not state.get("qa_passed"):
        return False

    # SLA 체크 결과 확인
    sla = state.get("sla_check_result", {})
    if not sla:
        return False

    # 비용 + 가용성 모두 OK여야 함
    if not sla.get("cost_ok") or not sla.get("availability_ok"):
        return False

    # 롤백 없이 첫 시도에 성공해야 함
    if state.get("rollback_count", 0) > 0:
        return False

    return True


def get_rule_id_from_state(state: PipelineState) -> tuple[str, str]:
    """
    State에서 적용된 규칙 ID와 타입을 추출.

    Returns:
        (rule_id, rule_type)
        - Classification 규칙: matched_rule_id → "classification"
        - QA 규칙: qa_matched_rule_id → "qa"
        - LLM 판단: "LLM-{anomaly_type}" → "llm"
    """
    # 1. Classification 규칙이 적용된 경우
    matched_rule_id = state.get("matched_rule_id")
    if matched_rule_id:
        return matched_rule_id, "classification"

    # 2. QA 규칙이 적용된 경우
    qa_matched_rule_id = state.get("qa_matched_rule_id")
    if qa_matched_rule_id:
        return qa_matched_rule_id, "qa"

    # 3. LLM 판단인 경우 (규칙 미매칭)
    anomaly_type = state.get("anomaly_type", "unknown")
    return f"LLM-{anomaly_type}", "llm"


def record_rule_stats(state: PipelineState, conn) -> dict[str, Any]:
    """
    규칙 실행 결과를 rule_stats 테이블에 기록.

    Args:
        state: 파이프라인 State
        conn: PostgreSQL 연결 객체

    Returns:
        {
            "rule_id": str,
            "rule_type": str,
            "is_win": bool,
            "trigger_evolution": bool,
            "current_win_rate": float,
            "total_runs": int,
        }
    """
    rule_id, rule_type = get_rule_id_from_state(state)
    win = is_win(state)

    # UPSERT 실행
    _upsert_rule_stats(conn, rule_id, rule_type, win)

    # 현재 상태 조회
    stats = _get_rule_stats(conn, rule_id)

    # 진화 트리거 조건 확인
    trigger = _check_evolution_trigger(conn, rule_id)

    return {
        "rule_id": rule_id,
        "rule_type": rule_type,
        "is_win": win,
        "trigger_evolution": trigger,
        "current_win_rate": stats.get("win_rate", 0.0) if stats else 0.0,
        "total_runs": stats.get("total_runs", 0) if stats else 0,
    }


def _upsert_rule_stats(conn, rule_id: str, rule_type: str, is_win: bool) -> None:
    """
    rule_stats 테이블에 UPSERT (INSERT ON CONFLICT DO UPDATE).

    PostgreSQL의 ON CONFLICT를 활용하여:
    - 새 규칙이면 INSERT
    - 기존 규칙이면 total_runs++, total_wins += is_win, win_rate 재계산
    """
    win_value = 1 if is_win else 0

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO rule_stats (rule_id, rule_type, total_runs, total_wins, win_rate, updated_at)
            VALUES (%(rule_id)s, %(rule_type)s, 1, %(win_value)s, %(win_value)s, NOW())
            ON CONFLICT (rule_id) DO UPDATE SET
                total_runs = rule_stats.total_runs + 1,
                total_wins = rule_stats.total_wins + EXCLUDED.total_wins,
                win_rate = (rule_stats.total_wins + EXCLUDED.total_wins)::FLOAT
                           / (rule_stats.total_runs + 1),
                updated_at = NOW()
        """, {
            "rule_id": rule_id,
            "rule_type": rule_type,
            "win_value": win_value,
        })


def _get_rule_stats(conn, rule_id: str) -> Optional[dict[str, Any]]:
    """규칙의 현재 통계 조회."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT rule_id, rule_type, total_runs, total_wins, win_rate,
                   last_evolution_run_count, last_evolution_at
            FROM rule_stats
            WHERE rule_id = %s
        """, (rule_id,))
        row = cur.fetchone()

        if row is None:
            return None

        return {
            "rule_id": row[0],
            "rule_type": row[1],
            "total_runs": row[2],
            "total_wins": row[3],
            "win_rate": row[4],
            "last_evolution_run_count": row[5],
            "last_evolution_at": row[6],
        }


def _check_evolution_trigger(
    conn,
    rule_id: str,
    n: int = EVOLUTION_TRIGGER_COUNT
) -> bool:
    """
    자가진화 트리거 조건 확인.

    트리거 조건: 마지막 진화 이후로 n건 이상 새로 실행됨

    Args:
        conn: PostgreSQL 연결 객체
        rule_id: 규칙 ID
        n: 트리거 임계값 (기본 3)

    Returns:
        True if 진화 트리거 조건 충족
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT total_runs, COALESCE(last_evolution_run_count, 0) as last_evo
            FROM rule_stats
            WHERE rule_id = %s
        """, (rule_id,))
        row = cur.fetchone()

        if row is None:
            return False

        total_runs, last_evo = row
        runs_since_last_evolution = total_runs - last_evo

        return runs_since_last_evolution >= n


def mark_evolution_completed(conn, rule_id: str) -> None:
    """
    진화 완료 후 last_evolution_run_count 업데이트.

    다음 진화 트리거까지 다시 n건을 기다리도록 현재 total_runs를 기록.
    """
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE rule_stats
            SET last_evolution_run_count = total_runs,
                last_evolution_at = NOW()
            WHERE rule_id = %s
        """, (rule_id,))


def get_all_underperforming_rules(
    conn,
    threshold: float = WIN_RATE_THRESHOLD,
    min_runs: int = EVOLUTION_TRIGGER_COUNT
) -> list[dict[str, Any]]:
    """
    저성능 규칙 목록 조회.

    조건:
    - win_rate < threshold (기본 60%)
    - total_runs >= min_runs (기본 3회 이상)

    Returns:
        저성능 규칙 목록 (win_rate 오름차순 정렬)
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT rule_id, rule_type, total_runs, total_wins, win_rate
            FROM rule_stats
            WHERE win_rate < %s AND total_runs >= %s
            ORDER BY win_rate ASC
        """, (threshold, min_runs))

        return [
            {
                "rule_id": r[0],
                "rule_type": r[1],
                "total_runs": r[2],
                "total_wins": r[3],
                "win_rate": r[4],
            }
            for r in cur.fetchall()
        ]


def get_rule_failure_summary(conn, rule_id: str) -> dict[str, Any]:
    """
    규칙의 실패 통계 요약 (LLM 규칙 개선에 활용).

    Returns:
        {
            "rule_id": str,
            "total_runs": int,
            "total_wins": int,
            "total_losses": int,
            "win_rate": float,
            "loss_rate": float,
        }
    """
    stats = _get_rule_stats(conn, rule_id)
    if not stats:
        return {"rule_id": rule_id, "total_runs": 0, "total_wins": 0, "total_losses": 0, "win_rate": 0.0, "loss_rate": 0.0}

    total_runs = stats["total_runs"]
    total_wins = stats["total_wins"]
    total_losses = total_runs - total_wins

    return {
        "rule_id": rule_id,
        "total_runs": total_runs,
        "total_wins": total_wins,
        "total_losses": total_losses,
        "win_rate": stats["win_rate"],
        "loss_rate": total_losses / total_runs if total_runs > 0 else 0.0,
    }
