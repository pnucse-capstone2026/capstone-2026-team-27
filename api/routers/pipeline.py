from fastapi import APIRouter, HTTPException

from api import pipeline_process
from config import decision_policy

router = APIRouter(prefix="/pipeline", tags=["pipeline"])


@router.get("/status")
def get_pipeline_process_status():
    """웹 제어판이 실제로 띄운 파이프라인 프로세스의 실행 상태(PID 기반 실측).
    /status의 pipeline_running(로그 최신성 기반 추정)과는 다른 값이다."""
    return pipeline_process.get_status()


@router.post("/start")
def start_pipeline():
    try:
        return pipeline_process.start(decision_policy.get_enabled_resource_types())
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.post("/stop")
def stop_pipeline():
    return pipeline_process.stop()
