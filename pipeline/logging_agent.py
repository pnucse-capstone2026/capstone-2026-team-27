"""
pipeline/logging_agent.py (박소영)

3.3.6 Logging Agent (Audit Log)
- 전체 에이전트 실행 과정/결과를 PostgreSQL 기반 Audit Log로 기록.
- 테이블 3개:
    agent_runs  : 파이프라인 실행 1회 = 1행 (리소스/이상유형/액션/리스크/QA 결과 요약)
    agent_steps : 실행 중 거친 각 단계(detection/classification/decision/action/qa) 1행씩
    action_log  : 실제로 액션이 실행된 경우의 상세 기록 (전/후 스냅샷, 성공 여부)

⚠️ Grafana 시각화는 지금 단계에서 만들지 않음.
   - 아직 AWS 미연동이라 비용 추이/탐지 빈도 등이 실데이터를 반영 못 함
   - Grafana는 별도 서버/인프라가 필요한 운영 단계 작업
   - 대신 나중에 바로 쓸 수 있는 패널용 SQL은 grafana_dashboard_queries.sql에 미리 정리해둠

⚠️ agent_steps.duration_ms(단계별 지연 시간)는 현재 NULL.
   각 agent 노드가 자기 시작/종료 시각을 state에 남기지 않고 있어서 아직 측정 불가.
   팀에서 instrumentation(타이밍 기록) 추가하면 그때 채울 수 있음 — 대화로 따로 제안.
"""

from __future__ import annotations

import json
import os
from typing import Any, Optional

import psycopg2
from dotenv import load_dotenv

from schema.state import PipelineState

# .env 파일 로드 (PGHOST 등 접속 정보를 읽기 전에 반드시 실행되어야 함)
load_dotenv()

# ── DB 연결 설정 (psycopg2 표준 환경변수 사용) ────────────────────────────────
# PGHOST / PGPORT / PGDATABASE / PGUSER / PGPASSWORD 로 접속 정보를 주입한다.
# 코드에 자격증명을 하드코딩하지 않는다.

# .env에 반드시 있어야 하는 키 (없어도 기본값으로 동작은 하지만,
# 기본값(postgres/빈 비밀번호)으로 실제 로컬 DB에 붙는 경우는 거의 없으므로
# 누락 시 원인을 바로 알 수 있도록 미리 경고한다)
_REQUIRED_PG_VARS = ["PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"]


def _pg_connection_params() -> dict[str, str]:
    """.env에서 읽은 PG 접속 정보를 dict로 반환 (비밀번호도 포함, 로그 출력 금지)."""
    return {
        "host":     os.environ.get("PGHOST", "localhost"),
        "port":     os.environ.get("PGPORT", "5432"),
        "dbname":   os.environ.get("PGDATABASE", "cloud_anomaly_agent"),
        "user":     os.environ.get("PGUSER", "postgres"),
        "password": os.environ.get("PGPASSWORD", ""),
    }


def _warn_missing_pg_env() -> None:
    """PG 관련 환경변수가 .env에 없으면 어떤 키가 비어있는지 미리 알려준다."""
    missing = [k for k in _REQUIRED_PG_VARS if not os.environ.get(k)]
    if missing:
        print(
            f"[logging_node] 경고: .env에 다음 PostgreSQL 접속 변수가 없습니다: {missing} "
            "— 기본값(localhost:5432/cloud_anomaly_agent/postgres, 빈 비밀번호)으로 "
            "접속을 시도하며, 대부분 이 값으로는 로컬 DB 인증에 실패합니다."
        )


def _get_connection():
    params = _pg_connection_params()
    _warn_missing_pg_env()
    try:
        return psycopg2.connect(**params)
    except Exception as exc:
        # 비밀번호는 절대 출력하지 않고, 나머지 접속 정보 + 원인만 노출한다.
        safe_params = {k: v for k, v in params.items() if k != "password"}
        raise RuntimeError(
            f"[logging_node] PostgreSQL 연결 실패 (접속정보: {safe_params}) — 원인: {exc}"
        ) from exc


_DDL = """
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS agent_runs (
    run_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    resource_id       TEXT NOT NULL,
    resource_type     TEXT NOT NULL,
    metric_timestamp  TIMESTAMPTZ NOT NULL,
    finished_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    anomaly_flag      BOOLEAN,
    anomaly_type      TEXT,
    selected_action   TEXT,
    risk_level        TEXT,
    requires_approval BOOLEAN,
    qa_passed         BOOLEAN,
    rollback_count    INTEGER,
    status            TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS agent_steps (
    step_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id      UUID NOT NULL REFERENCES agent_runs(run_id) ON DELETE CASCADE,
    step_name   TEXT NOT NULL,
    status      TEXT NOT NULL,
    output      JSONB NOT NULL,
    duration_ms INTEGER,
    logged_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS action_log (
    action_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id               UUID NOT NULL REFERENCES agent_runs(run_id) ON DELETE CASCADE,
    resource_id          TEXT NOT NULL,
    action_name          TEXT NOT NULL,
    risk_level           TEXT,
    requires_approval    BOOLEAN,
    pre_action_snapshot  JSONB,
    action_result        JSONB,
    success              BOOLEAN,
    executed_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

def _ensure_tables(conn) -> None:
    """테이블이 없으면 생성. CREATE TABLE IF NOT EXISTS라서 이미 있으면 아무것도 안 함."""
    with conn.cursor() as cur:
        cur.execute(_DDL)
    conn.commit()


# ── state → 레코드 변환 (DB 없이도 단독 테스트 가능하도록 순수 함수로 분리) ─────

def _build_run_record(state: PipelineState) -> dict[str, Any]:
    status = "completed"
    if state.get("qa_passed") is False:
        status = "failed_qa"
    if state.get("rollback_count", 0) >= 2:
        status = "rollback_exhausted"

    return {
        "resource_id":       state["resource_id"],
        "resource_type":     state["resource_type"],
        "metric_timestamp":  state["timestamp"],
        "anomaly_flag":      state.get("anomaly_flag"),
        "anomaly_type":      state.get("anomaly_type"),
        "selected_action":   state.get("selected_action"),
        "risk_level":        state.get("risk_level"),
        "requires_approval": state.get("requires_approval"),
        "qa_passed":         state.get("qa_passed"),
        "rollback_count":    state.get("rollback_count", 0),
        "status":            status,
    }


def _build_step_records(state: PipelineState) -> list[dict[str, Any]]:
    steps = [
        ("detection", {
            "anomaly_flag":          state.get("anomaly_flag"),
            "anomaly_score_zscore":  state.get("anomaly_score_zscore"),
            "anomaly_score_iforest": state.get("anomaly_score_iforest"),
            "triggered_metrics":     state.get("triggered_metrics"),
        }),
        ("classification", {
            "anomaly_type":   state.get("anomaly_type"),
            "reasoning":      state.get("classification_reasoning"),
            "interim_action": state.get("interim_action_taken"),
        }),
        ("decision", {
            "candidate_actions":  state.get("candidate_actions"),
            "selected_action":    state.get("selected_action"),
            "risk_level":         state.get("risk_level"),
            "requires_approval":  state.get("requires_approval"),
            "reasoning":          state.get("decision_reasoning"),
        }),
        ("action", {
            "action_executed": state.get("action_executed"),
            "action_result":   state.get("action_result"),
        }),
        ("qa", {
            "qa_passed":        state.get("qa_passed"),
            "sla_check_result": state.get("sla_check_result"),
            "rollback_count":   state.get("rollback_count"),
        }),
    ]

    records = []
    for step_name, output in steps:
        status = "skipped" if all(v is None for v in output.values()) else "success"
        records.append({
            "step_name":   step_name,
            "status":      status,
            "output":      output,
            "duration_ms": None,  # TODO: timing instrumentation 추가 후 채움
        })
    return records


def _build_action_record(state: PipelineState) -> Optional[dict[str, Any]]:
    if not state.get("action_executed"):
        return None  # NoAction 선택 또는 아직 액션 미실행

    result = state.get("action_result") or {}

    # QA_agent.py의 _trigger_rollback()이 action_result에 rolled_back=True를 남긴다.
    # 실행 자체는 성공(status=="success")했더라도 이후 QA 실패로 롤백됐다면
    # 감사 로그(action_log.success)에는 최종적으로 "성공한 액션"으로 남기면 안 된다.
    success = result.get("status") == "success" and not result.get("rolled_back", False)

    return {
        "resource_id":         state["resource_id"],
        "action_name":         state["action_executed"],
        "risk_level":          state.get("risk_level"),
        "requires_approval":   state.get("requires_approval"),
        "pre_action_snapshot": state.get("pre_action_snapshot"),
        "action_result":       result,
        "success":             success,
    }


def _format_human_readable(state: PipelineState) -> list[str]:
    """state["log_entries"]에 쌓는 사람이 읽기 좋은 요약 (DB와 별개, 기존 계약 유지)."""
    return [
        f"[detection]       anomaly_flag={state.get('anomaly_flag')}, triggered={state.get('triggered_metrics')}",
        f"[classification]  anomaly_type={state.get('anomaly_type')}, interim={state.get('interim_action_taken')}",
        f"[decision]        selected={state.get('selected_action')}, risk={state.get('risk_level')}, approval={state.get('requires_approval')}",
        f"[action]          executed={state.get('action_executed')}, result={state.get('action_result')}",
        f"[qa]              passed={state.get('qa_passed')}, rollback_count={state.get('rollback_count')}",
        "파이프라인 완료",
    ]


# ── 실제 DB INSERT ───────────────────────────────────────────────────────────

def _insert_run(conn, record: dict) -> str:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO agent_runs
                (resource_id, resource_type, metric_timestamp, anomaly_flag,
                 anomaly_type, selected_action, risk_level, requires_approval,
                 qa_passed, rollback_count, status)
            VALUES (%(resource_id)s, %(resource_type)s, %(metric_timestamp)s, %(anomaly_flag)s,
                    %(anomaly_type)s, %(selected_action)s, %(risk_level)s, %(requires_approval)s,
                    %(qa_passed)s, %(rollback_count)s, %(status)s)
            RETURNING run_id
            """,
            record,
        )
        return cur.fetchone()[0]


def _insert_steps(conn, run_id: str, records: list[dict]) -> None:
    with conn.cursor() as cur:
        for r in records:
            cur.execute(
                """
                INSERT INTO agent_steps (run_id, step_name, status, output, duration_ms)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (run_id, r["step_name"], r["status"],
                 json.dumps(r["output"], ensure_ascii=False, default=str), r["duration_ms"]),
            )


def _insert_action(conn, run_id: str, record: Optional[dict]) -> None:
    if record is None:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO action_log
                (run_id, resource_id, action_name, risk_level, requires_approval,
                 pre_action_snapshot, action_result, success)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                run_id, record["resource_id"], record["action_name"], record["risk_level"],
                record["requires_approval"],
                json.dumps(record["pre_action_snapshot"], ensure_ascii=False, default=str),
                json.dumps(record["action_result"], ensure_ascii=False, default=str),
                record["success"],
            ),
        )


def logging_node(state: PipelineState) -> PipelineState:
    run_record = _build_run_record(state)
    step_records = _build_step_records(state)
    action_record = _build_action_record(state)

    # DB 연결 시도.
    # 파이프라인 자체는 DB 장애와 무관하게 계속 진행해야 하므로 예외를 밖으로
    # 던지지는 않지만, 원인 파악이 안 되면 안 되므로 "무시됨"으로 뭉개지 않고
    # 연결 실패/저장 실패 원인을 반드시 콘솔에 출력한다.
    try:
        conn = _get_connection()   # 실패 시 RuntimeError(원인 포함) 발생
        try:
            _ensure_tables(conn)
            run_id = _insert_run(conn, run_record)
            _insert_steps(conn, run_id, step_records)
            _insert_action(conn, run_id, action_record)
            conn.commit()
            print(f"[logging_node] DB 저장 성공 (run_id={run_id})")
        except Exception as e:
            conn.rollback()
            print(f"[logging_node] DB 저장 실패 (INSERT/DDL 단계) — 원인: {e!r}")
        finally:
            conn.close()
    except Exception as e:
        # _get_connection()에서 발생한 RuntimeError (원인이 이미 메시지에 포함됨)
        print(f"[logging_node] {e}")

    # DB 적재와 별개로, state["log_entries"]엔 기존처럼 사람이 읽기 좋은 요약을 유지
    entries = state.get("log_entries", [])
    entries.extend(_format_human_readable(state))
    state["log_entries"] = entries

    return state