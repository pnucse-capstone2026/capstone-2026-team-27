"""
Rule Evolution E2E 테스트
-------------------------
실제 파이프라인을 반복 실행하여 자가진화 시스템 통합 검증

AWS 연동 후 이 테스트를 실행하여:
1. 실제 메트릭 기반 파이프라인 실행
2. rule_stats 누적 확인
3. 자가진화 트리거 발동 확인
4. 규칙 개선/비활성화 확인

사용법:
    # 기본 실행 (EC2 시나리오 3회)
    python -m playground.test_rule_evolution_e2e

    # 특정 시나리오 지정
    python -m playground.test_rule_evolution_e2e --scenario ec2_cpu_spike --runs 5

    # 모든 시나리오 실행
    python -m playground.test_rule_evolution_e2e --all

주의:
    - AWS 연동이 완료되어야 함
    - 실제 PostgreSQL DB 사용
    - 실제 Gemini API 사용
    - 테스트 후 정리 옵션 제공
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Optional

# 프로젝트 루트 추가
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

import psycopg2


# ══════════════════════════════════════════════════════════════════════════════
# 설정
# ══════════════════════════════════════════════════════════════════════════════

# 기본 반복 횟수
DEFAULT_RUNS = 5

# 실행 간 대기 시간 (초) - API rate limit 고려
RUN_INTERVAL_SEC = 2


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


def get_rule_stats(conn, rule_id: str = None) -> list[dict]:
    """rule_stats 조회."""
    with conn.cursor() as cur:
        if rule_id:
            cur.execute("""
                SELECT rule_id, rule_type, total_runs, total_wins, win_rate,
                       last_evolution_run_count, last_evolution_at, updated_at
                FROM rule_stats
                WHERE rule_id = %s
            """, (rule_id,))
        else:
            cur.execute("""
                SELECT rule_id, rule_type, total_runs, total_wins, win_rate,
                       last_evolution_run_count, last_evolution_at, updated_at
                FROM rule_stats
                ORDER BY updated_at DESC
            """)

        rows = cur.fetchall()
        return [
            {
                "rule_id": r[0],
                "rule_type": r[1],
                "total_runs": r[2],
                "total_wins": r[3],
                "win_rate": r[4],
                "last_evolution_run_count": r[5],
                "last_evolution_at": r[6],
                "updated_at": r[7],
            }
            for r in rows
        ]


def get_recent_runs(conn, limit: int = 10) -> list[dict]:
    """최근 agent_runs 조회."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT run_id, resource_id, resource_type, anomaly_type,
                   selected_action, qa_passed, rollback_count, status, finished_at
            FROM agent_runs
            ORDER BY finished_at DESC
            LIMIT %s
        """, (limit,))

        rows = cur.fetchall()
        return [
            {
                "run_id": str(r[0]),
                "resource_id": r[1],
                "resource_type": r[2],
                "anomaly_type": r[3],
                "selected_action": r[4],
                "qa_passed": r[5],
                "rollback_count": r[6],
                "status": r[7],
                "finished_at": r[8],
            }
            for r in rows
        ]


# ══════════════════════════════════════════════════════════════════════════════
# 테스트 시나리오 정의
# ══════════════════════════════════════════════════════════════════════════════

SCENARIOS = {
    "ec2_cpu_spike": {
        "description": "EC2 CPU 급증 시나리오",
        "resource_type": "EC2",
        "resource_id": "i-test-cpu-spike",
        "metrics": {
            "cpu_utilization": [20, 25, 30, 85, 90, 95, 98, 97, 95, 92,
                                90, 88, 85, 82, 80, 78, 75, 72, 70, 68,
                                65, 62, 60, 58, 55, 52, 50, 48, 45, 42],
            "memory_utilization": [50] * 30,
            "network_in": [1000] * 30,
            "network_out": [500] * 30,
            "cost": [0.1] * 30,
        },
    },
    "ec2_cost_anomaly": {
        "description": "EC2 비용 이상 시나리오",
        "resource_type": "EC2",
        "resource_id": "i-test-cost-anomaly",
        "metrics": {
            "cpu_utilization": [30] * 30,
            "memory_utilization": [40] * 30,
            "network_in": [1000] * 30,
            "network_out": [500] * 30,
            "cost": [0.1, 0.1, 0.1, 0.1, 0.1, 0.5, 0.8, 1.2, 1.5, 2.0,
                     2.5, 3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0, 6.5, 7.0,
                     7.5, 8.0, 8.5, 9.0, 9.5, 10.0, 10.0, 10.0, 10.0, 10.0],
        },
    },
    "lambda_throttle": {
        "description": "Lambda 스로틀링 시나리오",
        "resource_type": "Lambda",
        "resource_id": "arn:aws:lambda:test-throttle",
        "metrics": {
            "invocations": [100, 120, 150, 200, 500, 1000, 2000, 3000, 4000, 5000,
                            4500, 4000, 3500, 3000, 2500, 2000, 1500, 1000, 500, 200,
                            150, 120, 100, 90, 80, 70, 60, 50, 40, 30],
            "errors": [0, 0, 0, 0, 5, 10, 50, 100, 200, 300,
                       250, 200, 150, 100, 50, 20, 10, 5, 2, 1,
                       0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            "throttles": [0, 0, 0, 0, 10, 50, 100, 200, 300, 400,
                          350, 300, 250, 200, 150, 100, 50, 20, 10, 5,
                          2, 1, 0, 0, 0, 0, 0, 0, 0, 0],
            "duration": [100] * 30,
            "cost": [0.01] * 30,
        },
    },
    "autoscaling_edos": {
        "description": "AutoScaling EDoS 의심 시나리오",
        "resource_type": "AutoScaling",
        "resource_id": "asg-test-edos",
        "metrics": {
            "group_desired_capacity": [2, 2, 2, 2, 4, 8, 16, 32, 64, 100,
                                       100, 100, 100, 100, 100, 80, 60, 40, 20, 10,
                                       8, 6, 4, 4, 4, 2, 2, 2, 2, 2],
            "group_in_service_instances": [2, 2, 2, 2, 4, 8, 16, 32, 64, 100,
                                           100, 100, 100, 100, 100, 80, 60, 40, 20, 10,
                                           8, 6, 4, 4, 4, 2, 2, 2, 2, 2],
            "cost": [0.2, 0.2, 0.2, 0.2, 0.4, 0.8, 1.6, 3.2, 6.4, 10.0,
                     10.0, 10.0, 10.0, 10.0, 10.0, 8.0, 6.0, 4.0, 2.0, 1.0,
                     0.8, 0.6, 0.4, 0.4, 0.4, 0.2, 0.2, 0.2, 0.2, 0.2],
        },
    },
}


def build_test_state(scenario_name: str) -> dict:
    """테스트 시나리오로부터 State 생성."""
    scenario = SCENARIOS[scenario_name]

    return {
        "resource_id": scenario["resource_id"],
        "resource_type": scenario["resource_type"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "raw_metrics": scenario["metrics"],
        # 나머지 필드는 파이프라인이 채움
    }


# ══════════════════════════════════════════════════════════════════════════════
# 파이프라인 실행
# ══════════════════════════════════════════════════════════════════════════════

def run_pipeline(state: dict) -> dict:
    """파이프라인 실행."""
    from pipeline.graph import build_graph

    graph = build_graph()
    result = graph.invoke(state)
    return result


def run_e2e_test(
    scenario_name: str,
    num_runs: int,
    conn,
) -> dict:
    """E2E 테스트 실행."""
    print(f"\n{'=' * 60}")
    print(f"E2E 테스트: {scenario_name}")
    print(f"설명: {SCENARIOS[scenario_name]['description']}")
    print(f"반복 횟수: {num_runs}")
    print(f"{'=' * 60}")

    results = []
    initial_stats = get_rule_stats(conn)
    initial_rule_count = len(initial_stats)

    for i in range(num_runs):
        print(f"\n--- 실행 {i + 1}/{num_runs} ---")

        try:
            state = build_test_state(scenario_name)
            result = run_pipeline(state)

            run_result = {
                "run": i + 1,
                "anomaly_flag": result.get("anomaly_flag"),
                "anomaly_type": result.get("anomaly_type"),
                "selected_action": result.get("selected_action"),
                "qa_passed": result.get("qa_passed"),
                "rollback_count": result.get("rollback_count", 0),
                "matched_rule_id": result.get("matched_rule_id"),
                "qa_matched_rule_id": result.get("qa_matched_rule_id"),
            }

            results.append(run_result)

            print(f"  anomaly: {run_result['anomaly_flag']} ({run_result['anomaly_type']})")
            print(f"  action: {run_result['selected_action']}")
            print(f"  qa_passed: {run_result['qa_passed']}")
            print(f"  matched_rule: {run_result['matched_rule_id'] or run_result['qa_matched_rule_id'] or 'LLM'}")

        except Exception as e:
            print(f"  [ERROR] 실행 실패: {e}")
            results.append({"run": i + 1, "error": str(e)})

        # API rate limit 대기
        if i < num_runs - 1:
            time.sleep(RUN_INTERVAL_SEC)

    # 결과 분석
    print(f"\n{'=' * 60}")
    print("테스트 결과 분석")
    print(f"{'=' * 60}")

    # rule_stats 변화 확인
    final_stats = get_rule_stats(conn)
    print(f"\n[rule_stats 변화]")
    print(f"  테스트 전: {initial_rule_count}개 규칙")
    print(f"  테스트 후: {len(final_stats)}개 규칙")

    # 실행 통계
    success_runs = [r for r in results if not r.get("error") and r.get("qa_passed")]
    fail_runs = [r for r in results if not r.get("error") and not r.get("qa_passed")]
    error_runs = [r for r in results if r.get("error")]

    print(f"\n[실행 통계]")
    print(f"  성공 (qa_passed=True): {len(success_runs)}회")
    print(f"  실패 (qa_passed=False): {len(fail_runs)}회")
    print(f"  오류: {len(error_runs)}회")

    # 사용된 규칙
    rules_used = set()
    for r in results:
        if r.get("matched_rule_id"):
            rules_used.add(r["matched_rule_id"])
        if r.get("qa_matched_rule_id"):
            rules_used.add(r["qa_matched_rule_id"])

    print(f"\n[사용된 규칙]")
    if rules_used:
        for rule_id in rules_used:
            stats = next((s for s in final_stats if s["rule_id"] == rule_id), None)
            if stats:
                print(f"  {rule_id}: runs={stats['total_runs']}, win_rate={stats['win_rate']:.1%}")
    else:
        print("  (모두 LLM 판단)")

    # 진화 발생 여부 확인
    evolution_happened = False
    for stats in final_stats:
        if stats.get("last_evolution_at"):
            evolution_happened = True
            print(f"\n[진화 발생]")
            print(f"  규칙: {stats['rule_id']}")
            print(f"  시각: {stats['last_evolution_at']}")
            break

    if not evolution_happened:
        print(f"\n[진화 미발생] - 아직 트리거 조건(3회 누적) 미충족 또는 저성능 규칙 없음")

    return {
        "scenario": scenario_name,
        "num_runs": num_runs,
        "success": len(success_runs),
        "fail": len(fail_runs),
        "error": len(error_runs),
        "rules_used": list(rules_used),
        "evolution_happened": evolution_happened,
    }


# ══════════════════════════════════════════════════════════════════════════════
# 규칙 상태 확인 유틸리티
# ══════════════════════════════════════════════════════════════════════════════

def print_rule_stats_summary(conn):
    """현재 rule_stats 요약 출력."""
    stats = get_rule_stats(conn)

    print(f"\n{'=' * 60}")
    print("현재 Rule Stats 요약")
    print(f"{'=' * 60}")

    if not stats:
        print("  (데이터 없음)")
        return

    print(f"  {'규칙 ID':<20} {'타입':<15} {'실행':<8} {'성공':<8} {'승률':<10}")
    print(f"  {'-' * 60}")

    for s in stats:
        print(f"  {s['rule_id']:<20} {s['rule_type']:<15} {s['total_runs']:<8} {s['total_wins']:<8} {s['win_rate']:.1%}")


def print_rules_status():
    """현재 규칙 파일 상태 출력."""
    rules_dir = os.path.join(os.path.dirname(__file__), "..", "schema", "rules")

    print(f"\n{'=' * 60}")
    print("현재 규칙 파일 상태")
    print(f"{'=' * 60}")

    for filename in ["classification_rules.json", "qa_rules.json"]:
        filepath = os.path.join(rules_dir, filename)
        if not os.path.exists(filepath):
            print(f"\n[{filename}] 파일 없음")
            continue

        with open(filepath, "r", encoding="utf-8") as f:
            rules = json.load(f)

        enabled = [r for r in rules if r.get("enabled", True)]
        disabled = [r for r in rules if not r.get("enabled", True)]

        print(f"\n[{filename}]")
        print(f"  총 규칙: {len(rules)}개 (활성: {len(enabled)}, 비활성: {len(disabled)})")

        if disabled:
            print(f"  비활성화된 규칙:")
            for r in disabled:
                print(f"    - {r.get('rule_id')}: {r.get('disabled_reason', 'N/A')}")


# ══════════════════════════════════════════════════════════════════════════════
# 메인
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Rule Evolution E2E 테스트")
    parser.add_argument(
        "--scenario",
        choices=list(SCENARIOS.keys()),
        default="ec2_cpu_spike",
        help="테스트 시나리오 선택",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=DEFAULT_RUNS,
        help=f"반복 횟수 (기본: {DEFAULT_RUNS})",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="모든 시나리오 실행",
    )
    parser.add_argument(
        "--status-only",
        action="store_true",
        help="현재 상태만 출력 (테스트 실행 안 함)",
    )

    args = parser.parse_args()

    print("=" * 60)
    print("Rule Evolution E2E 테스트")
    print("=" * 60)
    print(f"시작 시각: {datetime.now().isoformat()}")

    conn = None
    try:
        # DB 연결
        print("\nDB 연결 중...")
        conn = get_db_connection()
        print("DB 연결 성공")

        # 상태만 출력
        if args.status_only:
            print_rule_stats_summary(conn)
            print_rules_status()
            return

        # 시나리오 실행
        if args.all:
            scenarios = list(SCENARIOS.keys())
        else:
            scenarios = [args.scenario]

        all_results = []
        for scenario in scenarios:
            result = run_e2e_test(scenario, args.runs, conn)
            all_results.append(result)

        # 최종 상태 출력
        print_rule_stats_summary(conn)
        print_rules_status()

        # 전체 요약
        print(f"\n{'=' * 60}")
        print("전체 테스트 요약")
        print(f"{'=' * 60}")

        for r in all_results:
            status = "PASS" if r["error"] == 0 else "PARTIAL"
            print(f"  [{status}] {r['scenario']}: {r['success']}/{r['num_runs']} 성공")

    except Exception as e:
        print(f"\n[ERROR] 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
        return

    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
