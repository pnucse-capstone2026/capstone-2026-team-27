"""
api/pipeline_process.py
========================
웹 제어판의 "파이프라인 실행/종료" 버튼이 실제로 제어하는 대상 —
playground/run_full_pipeline.py --loop 를 별도 프로세스로 띄우고 죽인다.

config/pipeline_live_status.py(노드별 실시간 상태)나 api/routers/status.py의
"pipeline_running" 추정치와는 다른 개념이다: 저건 "최근에 뭔가 돌았는지"를
로그/파일 최신성으로 추측하는 것이고, 여기는 "우리가 실제로 띄운 프로세스가
지금 살아있는지"를 PID로 직접 확인한다.

FastAPI(uvicorn)를 여러 워커로 띄우면 이 모듈의 전역 변수가 워커마다 따로 생겨서
어긋날 수 있는데, 이 프로젝트는 단일 워커 전제라 PID를 파일에도 같이 남겨서
(API 서버가 재시작돼도 상태를 다시 알 수 있게) 방어한다.
"""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import psutil

PROJECT_ROOT = Path(__file__).resolve().parent.parent
_STATE_PATH = PROJECT_ROOT / "config" / "pipeline_process_state.json"
_STDOUT_LOG_PATH = PROJECT_ROOT / "playground" / "eval_outputs" / "logs" / "web_pipeline_stdout.log"

_process: subprocess.Popen | None = None


def _write_state(pid: int | None, started_at: str | None) -> None:
    try:
        with open(_STATE_PATH, "w", encoding="utf-8") as f:
            json.dump({"pid": pid, "started_at": started_at}, f)
    except OSError:
        pass  # 상태 파일 쓰기 실패로 파이프라인 제어 자체가 죽으면 안 됨


def _read_state() -> dict:
    try:
        with open(_STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"pid": None, "started_at": None}


def _is_our_process(pid: int) -> bool:
    """PID 재사용(다른 프로그램이 같은 PID를 새로 받은 경우) 오판 방지 — cmdline에
    run_full_pipeline.py가 있는 프로세스인지 확인."""
    try:
        proc = psutil.Process(pid)
        return any("run_full_pipeline.py" in part for part in proc.cmdline())
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return False


def is_running() -> bool:
    global _process
    if _process is not None:
        if _process.poll() is None:
            return True
        _process = None  # 이미 종료된 프로세스 핸들 정리

    state = _read_state()
    pid = state.get("pid")
    return bool(pid and _is_our_process(pid))


def get_status() -> dict:
    state = _read_state()
    running = is_running()
    return {
        "running": running,
        "pid": state.get("pid") if running else None,
        "started_at": state.get("started_at") if running else None,
    }


def start(resource_types: list[str] | None = None) -> dict:
    global _process
    if is_running():
        raise RuntimeError("파이프라인이 이미 실행 중입니다.")

    _STDOUT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, str(PROJECT_ROOT / "playground" / "run_full_pipeline.py"), "--loop"]
    if resource_types:
        cmd += ["--resource-types", ",".join(resource_types)]

    log_file = open(_STDOUT_LOG_PATH, "a", encoding="utf-8")
    _process = subprocess.Popen(
        cmd, cwd=str(PROJECT_ROOT), stdout=log_file, stderr=subprocess.STDOUT,
    )
    started_at = datetime.now(timezone.utc).isoformat()
    _write_state(_process.pid, started_at)
    return get_status()


def stop() -> dict:
    global _process
    state = _read_state()
    pid = state.get("pid")

    stopped = False
    if _process is not None and _process.poll() is None:
        _process.terminate()
        try:
            _process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            _process.kill()
        stopped = True
    elif pid and _is_our_process(pid):
        try:
            proc = psutil.Process(pid)
            proc.terminate()
            psutil.wait_procs([proc], timeout=10)
        except psutil.NoSuchProcess:
            pass
        stopped = True

    _process = None
    _write_state(None, None)
    return {"running": False, "pid": None, "started_at": None, "stopped": stopped}
