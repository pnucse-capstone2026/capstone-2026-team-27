"""

detection_agent.py + logging_agent.py 모듈 테스트.

[실행 방법]
  프로젝트 루트에서:
    pytest test_agents.py -v

[사전 준비]
  pip install pytest scikit-learn numpy psycopg2-binary python-dotenv

[주의]
  - logging_agent DB 연동 테스트는 .env에 PostgreSQL 접속 정보가 있어야 함.
  - detection_agent 테스트는 DB 없이도 전부 실행 가능.
"""

import os
import random
import sys

import pytest
from dotenv import load_dotenv

load_dotenv()

# ── 헬퍼: 기본 state 뼈대 ─────────────────────────────────────────────────────

def make_state(resource_type: str, raw_metrics: dict) -> dict:
    return {
        "resource_id":   f"test-{resource_type.lower()}-001",
        "resource_type": resource_type,
        "raw_metrics":   raw_metrics,
        "timestamp":     "2026-06-29T10:00:00Z",
        "anomaly_flag":          False,
        "anomaly_score_zscore":  None,
        "anomaly_score_iforest": None,
        "triggered_metrics":     [],
        "anomaly_type":              None,
        "classification_reasoning":  None,
        "interim_action_taken":      None,
        "candidate_actions":  [],
        "selected_action":    None,
        "risk_level":         None,
        "requires_approval":  False,
        "decision_reasoning": None,
        "pre_action_snapshot": None,
        "action_executed":     None,
        "action_result":       None,
        "qa_passed":        None,
        "sla_check_result": None,
        "rollback_count":   0,
        "log_entries":      [],
    }


def normal_values(n: int = 30, base: float = 50.0, noise: float = 2.0) -> list[float]:
    """정상 범위 내 값 n개 생성."""
    random.seed(42)
    return [base + random.uniform(-noise, noise) for _ in range(n)]


def spike_at_end(n: int = 30, base: float = 50.0, noise: float = 2.0,
                 spike: float = 200.0) -> list[float]:
    """마지막 포인트에만 스파이크."""
    random.seed(42)
    return [base + random.uniform(-noise, noise) for _ in range(n - 1)] + [spike]


# ══════════════════════════════════════════════════════════════════════════════
# detection_agent 테스트
# ══════════════════════════════════════════════════════════════════════════════

class TestDetectionAgent:
    """detection_node 단위 테스트."""

    @pytest.fixture(autouse=True)
    def cleanup_model_cache(self, tmp_path, monkeypatch):
        """테스트마다 모델 캐시 폴더를 임시 경로로 격리."""
        monkeypatch.setenv("PIPELINE_MODEL_DIR", str(tmp_path / "models"))
        # detection_agent 모듈의 상수도 갱신
        import pipeline.detection_agent as da
        monkeypatch.setattr(da, "IFOREST_MODEL_DIR", str(tmp_path / "models"))

    # ── 1. 정상 데이터 → anomaly_flag=False ───────────────────────────────────
    def test_normal_ec2_no_anomaly(self):
        """EC2 정상 데이터 → 이상 탐지 안 됨."""
        from pipeline.detection_agent import detection_node

        metrics = {
            "cpu_utilization": normal_values(30, 50, 2),
            "network_in":      normal_values(30, 1000, 50),
            "network_out":     normal_values(30, 800, 50),
            "cost":            normal_values(30, 2.0, 0.1),
        }
        state = make_state("EC2", metrics)
        result = detection_node(state)

        assert result["anomaly_flag"] is False
        assert result["triggered_metrics"] == []
        assert result["anomaly_score_zscore"] is not None
        assert result["anomaly_score_iforest"] is not None

    # ── 2. cost 스파이크 → cost 트리거 ────────────────────────────────────────
    def test_cost_spike_triggers(self):
        """cost에 스파이크 → triggered_metrics에 cost 포함, anomaly_flag=True."""
        from pipeline.detection_agent import detection_node

        metrics = {
            "cpu_utilization": normal_values(30, 50, 2),
            "network_in":      normal_values(30, 1000, 50),
            "network_out":     normal_values(30, 800, 50),
            "cost":            spike_at_end(30, 2.0, 0.1, spike=20.0),
        }
        state = make_state("EC2", metrics)
        result = detection_node(state)

        assert result["anomaly_flag"] is True
        assert "cost" in result["triggered_metrics"]

    # ── 3. CPU만 스파이크 → Z-score 대상 아니라 z-score로는 안 잡힘 ─────────────
    def test_cpu_spike_not_zscore_target(self):
        """CPU는 Z-score 대상 아니라, CPU만 튀어도 triggered_metrics에 안 잡힘."""
        from pipeline.detection_agent import detection_node

        metrics = {
            "cpu_utilization": spike_at_end(30, 50, 2, spike=500.0),
            "network_in":      normal_values(30, 1000, 50),
            "network_out":     normal_values(30, 800, 50),
            "cost":            normal_values(30, 2.0, 0.1),
        }
        state = make_state("EC2", metrics)
        result = detection_node(state)

        assert "cpu_utilization" not in result["triggered_metrics"]

    # ── 4. network_in 스파이크 → 트리거 ──────────────────────────────────────
    def test_network_in_spike_triggers(self):
        """network_in 스파이크 → triggered_metrics에 network_in 포함."""
        from pipeline.detection_agent import detection_node

        metrics = {
            "cpu_utilization": normal_values(30, 50, 2),
            "network_in":      spike_at_end(30, 1000, 50, spike=50000.0),
            "network_out":     normal_values(30, 800, 50),
            "cost":            normal_values(30, 2.0, 0.1),
        }
        state = make_state("EC2", metrics)
        result = detection_node(state)

        assert "network_in" in result["triggered_metrics"]
        assert result["anomaly_flag"] is True

    # ── 5. Lambda: invocation_count 스파이크 ──────────────────────────────────
    def test_lambda_invocation_count_spike(self):
        """Lambda의 invocation_count 스파이크 → 트리거."""
        from pipeline.detection_agent import detection_node

        metrics = {
            "invocation_count": spike_at_end(30, 100, 5, spike=5000.0),
            "error_count":      normal_values(30, 1, 0.5),
            "duration_avg":     normal_values(30, 200, 10),
            "cost":             normal_values(30, 1.0, 0.05),
        }
        state = make_state("Lambda", metrics)
        result = detection_node(state)

        assert "invocation_count" in result["triggered_metrics"]
        assert result["anomaly_flag"] is True

    # ── 6. S3: number_of_requests 스파이크 ────────────────────────────────────
    def test_s3_number_of_requests_spike(self):
        """S3의 number_of_requests 스파이크 → 트리거."""
        from pipeline.detection_agent import detection_node

        metrics = {
            "number_of_requests": spike_at_end(30, 500, 20, spike=20000.0),
            "bytes_downloaded":   normal_values(30, 1000, 50),
            "cost":               normal_values(30, 0.5, 0.05),
        }
        state = make_state("S3", metrics)
        result = detection_node(state)

        assert "number_of_requests" in result["triggered_metrics"]
        assert result["anomaly_flag"] is True

    # ── 7. RDS 정상 → anomaly_flag=False ─────────────────────────────────────
    def test_rds_normal_no_anomaly(self):
        """RDS 정상 데이터 → 이상 없음."""
        from pipeline.detection_agent import detection_node

        metrics = {
            "cpu_utilization":      normal_values(30, 40, 2),
            "database_connections": normal_values(30, 10, 1),
            "read_iops":            normal_values(30, 100, 5),
            "write_iops":           normal_values(30, 80, 5),
            "cost":                 normal_values(30, 3.0, 0.1),
        }
        state = make_state("RDS", metrics)
        result = detection_node(state)

        assert result["anomaly_flag"] is False

    # ── 8. 데이터 포인트 부족 → Isolation Forest 학습 보류 ─────────────────────
    def test_insufficient_data_skips_iforest(self):
        """윈도우가 5개 미만이면 Isolation Forest 학습 보류 → iforest_score = 0.0."""
        from pipeline.detection_agent import detection_node

        metrics = {
            "cpu_utilization": [50.0, 51.0, 49.0],  # 3개만
            "network_in":      [1000.0, 1010.0, 990.0],
            "network_out":     [800.0, 810.0, 790.0],
            "cost":            [2.0, 2.1, 1.9],
        }
        state = make_state("EC2", metrics)
        result = detection_node(state)

        assert result["anomaly_score_iforest"] == 0.0

    # ── 9. state 출력 필드 타입 검증 ─────────────────────────────────────────
    def test_output_field_types(self):
        """detection_node 출력 필드가 정해진 타입인지 확인."""
        from pipeline.detection_agent import detection_node

        metrics = {
            "cpu_utilization": normal_values(30, 50, 2),
            "network_in":      normal_values(30, 1000, 50),
            "network_out":     normal_values(30, 800, 50),
            "cost":            normal_values(30, 2.0, 0.1),
        }
        state = make_state("EC2", metrics)
        result = detection_node(state)

        assert isinstance(result["anomaly_flag"], bool)
        assert isinstance(result["anomaly_score_zscore"], float)
        assert isinstance(result["anomaly_score_iforest"], float)
        assert isinstance(result["triggered_metrics"], list)

    # ── 10. 캐시 재사용: 같은 resource_type 두 번 호출 시 모델 파일 mtime 불변 ──
    def test_iforest_model_cache_reused(self, tmp_path, monkeypatch):
        """두 번째 호출에서 모델 파일을 재학습하지 않고 캐시 재사용."""
        import time
        import pipeline.detection_agent as da
        monkeypatch.setattr(da, "IFOREST_MODEL_DIR", str(tmp_path / "models"))

        metrics = {
            "cpu_utilization": normal_values(30, 50, 2),
            "network_in":      normal_values(30, 1000, 50),
            "network_out":     normal_values(30, 800, 50),
            "cost":            normal_values(30, 2.0, 0.1),
        }
        da.detection_node(make_state("EC2", metrics))
        path = da._model_path("EC2")
        mtime1 = os.path.getmtime(path)

        time.sleep(0.5)
        da.detection_node(make_state("EC2", metrics))
        mtime2 = os.path.getmtime(path)

        assert mtime1 == mtime2, "두 번째 호출에서 모델이 재학습됨 (캐시 재사용 실패)"


# ══════════════════════════════════════════════════════════════════════════════
# logging_agent 테스트 — _build_* 순수 함수 (DB 없이)
# ══════════════════════════════════════════════════════════════════════════════

class TestLoggingAgentPureFunctions:
    """DB 연결 없이 레코드 변환 로직만 검증."""

    def _full_state(self, action_executed=None, qa_passed=True, rollback_count=0):
        state = make_state("EC2", {})
        state.update({
            "anomaly_flag":          True,
            "anomaly_score_zscore":  5.38,
            "anomaly_score_iforest": 1.0,
            "triggered_metrics":     ["cost"],
            "anomaly_type":          "cost_spike",
            "classification_reasoning": "비용 급등",
            "selected_action":  action_executed,
            "risk_level":       "MED",
            "requires_approval": True,
            "action_executed":  action_executed,
            "action_result":    {"status": "success"} if action_executed else None,
            "qa_passed":        qa_passed,
            "sla_check_result": {"cpu_ok": True, "cost_ok": True,
                                 "availability_ok": True, "detail": ""},
            "rollback_count":   rollback_count,
        })
        return state

    # ── 11. run record status: completed ─────────────────────────────────────
    def test_run_record_status_completed(self):
        from pipeline.logging_agent import _build_run_record
        rec = _build_run_record(self._full_state(qa_passed=True))
        assert rec["status"] == "completed"

    # ── 12. run record status: failed_qa ─────────────────────────────────────
    def test_run_record_status_failed_qa(self):
        from pipeline.logging_agent import _build_run_record
        rec = _build_run_record(self._full_state(qa_passed=False))
        assert rec["status"] == "failed_qa"

    # ── 13. run record status: rollback_exhausted ─────────────────────────────
    def test_run_record_status_rollback_exhausted(self):
        from pipeline.logging_agent import _build_run_record
        rec = _build_run_record(self._full_state(qa_passed=False, rollback_count=2))
        assert rec["status"] == "rollback_exhausted"

    # ── 14. step records 5개 생성 ─────────────────────────────────────────────
    def test_step_records_count(self):
        from pipeline.logging_agent import _build_step_records
        records = _build_step_records(self._full_state())
        step_names = [r["step_name"] for r in records]
        assert step_names == ["detection", "classification", "decision", "action", "qa"]

    # ── 15. action record: 액션 있는 경우 ────────────────────────────────────
    def test_action_record_when_executed(self):
        from pipeline.logging_agent import _build_action_record
        rec = _build_action_record(self._full_state(action_executed="ScaleDown"))
        assert rec is not None
        assert rec["action_name"] == "ScaleDown"
        assert rec["success"] is True

    # ── 16. action record: 액션 없는 경우 → None ─────────────────────────────
    def test_action_record_when_not_executed(self):
        from pipeline.logging_agent import _build_action_record
        rec = _build_action_record(self._full_state(action_executed=None))
        assert rec is None

    # ── 17. step status: 값이 있는 단계 → success ────────────────────────────
    def test_step_status_success_when_filled(self):
        from pipeline.logging_agent import _build_step_records
        records = _build_step_records(self._full_state())
        detection_step = next(r for r in records if r["step_name"] == "detection")
        assert detection_step["status"] == "success"

    # ── 18. step status: 값 없는 단계 → skipped ──────────────────────────────
    def test_step_status_skipped_when_empty(self):
        from pipeline.logging_agent import _build_step_records
        # action_executed=None이면 action 단계가 skipped
        state = make_state("EC2", {})
        records = _build_step_records(state)
        action_step = next(r for r in records if r["step_name"] == "action")
        assert action_step["status"] == "skipped"

    # ── 19. log_entries 형식 확인 ─────────────────────────────────────────────
    def test_format_human_readable(self):
        from pipeline.logging_agent import _format_human_readable
        entries = _format_human_readable(self._full_state())
        assert any("[detection]" in e for e in entries)
        assert any("[classification]" in e for e in entries)
        assert any("파이프라인 완료" in e for e in entries)
        assert len(entries) == 6


# ══════════════════════════════════════════════════════════════════════════════
# logging_agent DB 연동 테스트 (PostgreSQL 필요)
# ══════════════════════════════════════════════════════════════════════════════

def is_db_available() -> bool:
    """PostgreSQL 연결 가능한지 확인 (환경변수 없으면 skip)."""
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=os.environ.get("PGHOST", "localhost"),
            port=os.environ.get("PGPORT", "5432"),
            dbname=os.environ.get("PGDATABASE", "cloud_anomaly_agent"),
            user=os.environ.get("PGUSER", "postgres"),
            password=os.environ.get("PGPASSWORD", ""),
        )
        conn.close()
        return True
    except Exception:
        return False


@pytest.mark.skipif(not is_db_available(), reason="PostgreSQL 연결 불가 — DB 테스트 건너뜀")
class TestLoggingAgentDB:
    """실제 PostgreSQL INSERT 테스트."""

    def _run_logging(self, action_executed=None, qa_passed=True, rollback_count=0):
        from pipeline.logging_agent import logging_node

        state = make_state("EC2", {})
        state.update({
            "anomaly_flag":          True,
            "anomaly_score_zscore":  5.38,
            "anomaly_score_iforest": 1.0,
            "triggered_metrics":     ["cost"],
            "anomaly_type":          "cost_spike",
            "classification_reasoning": "테스트용",
            "selected_action":  action_executed,
            "risk_level":       "MED",
            "requires_approval": True,
            "action_executed":  action_executed,
            "action_result":    {"status": "success"} if action_executed else None,
            "pre_action_snapshot": {"instance_type": "t3.medium",
                                    "state": "running",
                                    "security_group_ids": ["sg-test"]} if action_executed else None,
            "qa_passed":        qa_passed,
            "sla_check_result": {"cpu_ok": True, "cost_ok": True,
                                 "availability_ok": True, "detail": ""},
            "rollback_count":   rollback_count,
        })
        return logging_node(state)

    # ── 20. 기본 INSERT 성공 ──────────────────────────────────────────────────
    def test_db_insert_basic(self):
        """logging_node가 에러 없이 실행되고 log_entries가 채워지는지."""
        result = self._run_logging()
        assert len(result["log_entries"]) == 6
        assert any("파이프라인 완료" in e for e in result["log_entries"])

    # ── 21. action_log: 액션 있는 경우 INSERT 확인 ───────────────────────────
    def test_db_action_log_inserted_when_executed(self):
        """action_executed가 있으면 action_log에도 INSERT되는지."""
        import psycopg2
        self._run_logging(action_executed="ScaleDown")

        conn = psycopg2.connect(
            host=os.environ.get("PGHOST", "localhost"),
            port=os.environ.get("PGPORT", "5432"),
            dbname=os.environ.get("PGDATABASE", "cloud_anomaly_agent"),
            user=os.environ.get("PGUSER", "postgres"),
            password=os.environ.get("PGPASSWORD", ""),
        )
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM action_log WHERE action_name = 'ScaleDown'")
                count = cur.fetchone()[0]
            assert count >= 1
        finally:
            conn.close()

    # ── 22. action_log: 액션 없는 경우 INSERT 안 됨 ──────────────────────────
    def test_db_action_log_not_inserted_when_no_action(self):
        """action_executed가 None이면 action_log에 INSERT 안 되는지."""
        import psycopg2
        conn = psycopg2.connect(
            host=os.environ.get("PGHOST", "localhost"),
            port=os.environ.get("PGPORT", "5432"),
            dbname=os.environ.get("PGDATABASE", "cloud_anomaly_agent"),
            user=os.environ.get("PGUSER", "postgres"),
            password=os.environ.get("PGPASSWORD", ""),
        )
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM action_log")
                before = cur.fetchone()[0]
        finally:
            conn.close()

        self._run_logging(action_executed=None)

        conn = psycopg2.connect(
            host=os.environ.get("PGHOST", "localhost"),
            port=os.environ.get("PGPORT", "5432"),
            dbname=os.environ.get("PGDATABASE", "cloud_anomaly_agent"),
            user=os.environ.get("PGUSER", "postgres"),
            password=os.environ.get("PGPASSWORD", ""),
        )
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM action_log")
                after = cur.fetchone()[0]
        finally:
            conn.close()

        assert before == after

    # ── 23. agent_steps 5개 INSERT 확인 ──────────────────────────────────────
    def test_db_agent_steps_count(self):
        """logging_node 1회 실행 시 agent_steps에 5개 행이 INSERT되는지."""
        import psycopg2
        conn = psycopg2.connect(
            host=os.environ.get("PGHOST", "localhost"),
            port=os.environ.get("PGPORT", "5432"),
            dbname=os.environ.get("PGDATABASE", "cloud_anomaly_agent"),
            user=os.environ.get("PGUSER", "postgres"),
            password=os.environ.get("PGPASSWORD", ""),
        )
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT count(*) FROM agent_steps
                    WHERE run_id = (
                        SELECT run_id FROM agent_runs ORDER BY finished_at DESC LIMIT 1
                    )
                """)
                # 테스트 전에 먼저 logging_node 실행
        finally:
            conn.close()

        self._run_logging()

        conn = psycopg2.connect(
            host=os.environ.get("PGHOST", "localhost"),
            port=os.environ.get("PGPORT", "5432"),
            dbname=os.environ.get("PGDATABASE", "cloud_anomaly_agent"),
            user=os.environ.get("PGUSER", "postgres"),
            password=os.environ.get("PGPASSWORD", ""),
        )
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT count(*) FROM agent_steps
                    WHERE run_id = (
                        SELECT run_id FROM agent_runs ORDER BY finished_at DESC LIMIT 1
                    )
                """)
                count = cur.fetchone()[0]
            assert count == 5
        finally:
            conn.close()