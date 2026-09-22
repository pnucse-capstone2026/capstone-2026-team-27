"""
playground/lambda_throttle_boundary_trial.py

Lambda 스로틀(429) 재시도 폭증 시나리오의 "경계 케이스" 실 AWS 시행.

lambda_throttle_repeated_trial.py(동시성 1 vs 무제한, 100% 정확도)는 극단
두 개만 비교한 테스트였다 — 실제로 어려운 건 그 사이 경계다. 이 스크립트는
동시성을 1/2/3/5/무제한 5단계로 나눠서 같은 호출 패턴(40건 x 3회, 5분 간격)을
걸고, **throttle_rate 실측값과 detection_node의 실제 판정(anomaly_flag)이
동시성에 따라 어떻게 변하는지** 확인한다.

⚠️ 2026-09-13 아키텍처 변경 반영: Lambda의 throttle_rate/error_rate는 이제
detection_node의 직접 트리거가 아니라 IForest 입력 feature로만 작동한다
(pipeline/detection_agent.py 리팩터링 참고). 즉 이번 시행은 "throttle_rate가
THROTTLE_RATE_THRESHOLD(0.4)를 넘었는가"가 아니라 "IForest가 실제로 어느
동시성 지점부터 이상으로 판단하는가"를 보는 것이다 — 두 기준이 다를 수 있다는
전제로 결과를 해석해야 한다.

[동시성별 배분 — 기존 13개 함수 재사용]
  concurrency=1          : anomaly-2,3,4          (3개, 기존에 검증된 극단 재확인)
  concurrency=2          : normal-1,2,3            (3개)
  concurrency=3          : normal-4,5,6            (3개)
  concurrency=5          : normal-7,8              (2개)
  무제한(대조군)          : detection-retry-storm-test, anomaly-5  (2개)

[실행 방법]
  python playground/lambda_throttle_boundary_trial.py --run
  python playground/lambda_throttle_boundary_trial.py --restore

[생성 파일]
  playground/eval_outputs/lambda_throttle_boundary_trial_{날짜}.json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

import boto3

from playground.measure_pipeline_timing import measure

AWS_REGION = "ap-northeast-2"
SETUP_PROFILE = "default"

# (function_name, concurrency 또는 None=무제한)
TARGETS: list[tuple[str, "int | None"]] = [
    ("detection-test-lambda-anomaly-2", 1),
    ("detection-test-lambda-anomaly-3", 1),
    ("detection-test-lambda-anomaly-4", 1),
    ("detection-test-lambda-normal-1", 2),
    ("detection-test-lambda-normal-2", 2),
    ("detection-test-lambda-normal-3", 2),
    ("detection-test-lambda-normal-4", 3),
    ("detection-test-lambda-normal-5", 3),
    ("detection-test-lambda-normal-6", 3),
    ("detection-test-lambda-normal-7", 5),
    ("detection-test-lambda-normal-8", 5),
    ("detection-retry-storm-test", None),
    ("detection-test-lambda-anomaly-5", None),
]

BURST_SIZE = 40
N_BURSTS = 3
BURST_INTERVAL_SEC = 300
MAX_EVENT_AGE_SEC = 60
POST_BURST_WAIT_SEC = 180
SHARED_INVOKE_POOL_SIZE = 60  # lambda_throttle_repeated_trial.py에서 확인된 스레드 폭증 방지값

LOG_DIR = PROJECT_ROOT / "playground" / "eval_outputs" / "logs"
RESULT_DIR = PROJECT_ROOT / "playground" / "eval_outputs"

logger = logging.getLogger("lambda_throttle_boundary_trial")


def _setup_logging() -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = LOG_DIR / f"lambda_throttle_boundary_trial_{ts}.log"
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.addHandler(fh)
    logger.addHandler(sh)
    logger.info("로그 파일: %s", log_path)
    return log_path


def _setup_lambda_client():
    return boto3.Session(profile_name=SETUP_PROFILE).client("lambda", region_name=AWS_REGION)


def _snapshot_path(function_name: str) -> Path:
    return RESULT_DIR / f".lambda_throttle_boundary_trial_snapshot__{function_name}.json"


def _snapshot(lam, function_name: str) -> dict:
    try:
        concurrency = lam.get_function_concurrency(FunctionName=function_name).get(
            "ReservedConcurrentExecutions"
        )
    except Exception:
        concurrency = None
    had_eic = True
    try:
        lam.get_function_event_invoke_config(FunctionName=function_name)
    except lam.exceptions.ResourceNotFoundException:
        had_eic = False
    return {"concurrency": concurrency, "had_event_invoke_config": had_eic}


def _save_snapshot(function_name: str, snap: dict) -> None:
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    with open(_snapshot_path(function_name), "w", encoding="utf-8") as f:
        json.dump(snap, f)


def restore(function_name: str) -> None:
    lam = _setup_lambda_client()
    path = _snapshot_path(function_name)
    if path.exists():
        with open(path, encoding="utf-8") as f:
            snap = json.load(f)
    else:
        logger.warning("[원복 %s] 저장된 스냅샷 없음 — 미설정으로 정리", function_name)
        snap = {"concurrency": None, "had_event_invoke_config": False}

    if snap["concurrency"] is None:
        try:
            lam.delete_function_concurrency(FunctionName=function_name)
        except Exception:
            pass
    else:
        lam.put_function_concurrency(
            FunctionName=function_name, ReservedConcurrentExecutions=snap["concurrency"]
        )
    if not snap["had_event_invoke_config"]:
        try:
            lam.delete_function_event_invoke_config(FunctionName=function_name)
        except Exception:
            pass
    logger.info("[원복 %s] 완료", function_name)
    if path.exists():
        path.unlink()


def _burst_invoke(lam, function_name: str, invoke_pool: ThreadPoolExecutor) -> None:
    def _invoke(i: int) -> bool:
        try:
            lam.invoke(FunctionName=function_name, InvocationType="Event", Payload=b"{}")
            return True
        except Exception as exc:
            logger.warning("  [%s] invoke #%d 실패: %s", function_name, i, exc)
            return False

    for b in range(N_BURSTS):
        futures = [invoke_pool.submit(_invoke, i) for i in range(BURST_SIZE)]
        results = [f.result() for f in futures]
        logger.info("[%s] 버스트 %d/%d 완료: 성공 %d/%d",
                    function_name, b + 1, N_BURSTS, sum(results), BURST_SIZE)
        if b < N_BURSTS - 1:
            time.sleep(BURST_INTERVAL_SEC)


def run_trial(function_name: str, concurrency: "int | None", invoke_pool: ThreadPoolExecutor) -> dict:
    lam = _setup_lambda_client()

    if concurrency is not None:
        snap = _snapshot(lam, function_name)
        _save_snapshot(function_name, snap)
        lam.put_function_event_invoke_config(
            FunctionName=function_name, MaximumEventAgeInSeconds=MAX_EVENT_AGE_SEC,
            MaximumRetryAttempts=2,
        )
        lam.put_function_concurrency(FunctionName=function_name, ReservedConcurrentExecutions=concurrency)
        logger.info("[%s] concurrency=%d 설정 완료", function_name, concurrency)
        time.sleep(3)
    else:
        logger.info("[%s] 무제한(대조군) — 설정 변경 없음", function_name)

    _burst_invoke(lam, function_name, invoke_pool)
    time.sleep(POST_BURST_WAIT_SEC)

    logger.info("[%s] measure() 시작", function_name)
    try:
        result = measure(function_name, "Lambda", bypass_approval_for_timing=True)
    except Exception as exc:
        logger.error("[%s] measure() 실패: %s", function_name, exc)
        result = {"resource_id": function_name, "resource_type": "Lambda", "error": str(exc)}

    if concurrency is not None:
        restore(function_name)

    logger.info("[%s] 완료 — concurrency=%s, anomaly_flag=%s, anomaly_type=%s, action=%s",
                function_name, concurrency, result.get("anomaly_flag"), result.get("anomaly_type"),
                result.get("selected_action"))

    return {**result, "concurrency_setting": concurrency, "measured_at": datetime.now(timezone.utc).isoformat()}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--restore", action="store_true")
    args = parser.parse_args()

    if not args.run and not args.restore:
        parser.error("--run 또는 --restore 중 하나는 지정해야 함")

    _setup_logging()

    if args.restore:
        for fn, conc in TARGETS:
            if conc is not None:
                restore(fn)
        return

    logger.info("=== 동시성 구배 시행 시작 (13개 함수, 레벨: 1/2/3/5/무제한) ===")
    trials = []
    try:
        with ThreadPoolExecutor(max_workers=SHARED_INVOKE_POOL_SIZE) as invoke_pool, \
             ThreadPoolExecutor(max_workers=len(TARGETS)) as ex:
            futures = {ex.submit(run_trial, fn, conc, invoke_pool): fn for fn, conc in TARGETS}
            for fut in as_completed(futures):
                trials.append(fut.result())
    finally:
        for fn, conc in TARGETS:
            if conc is not None and _snapshot_path(fn).exists():
                logger.warning("[%s] 정리 안 된 스냅샷 발견 — 재원복", fn)
                restore(fn)

    order = [fn for fn, _ in TARGETS]
    trials.sort(key=lambda t: order.index(t["resource_id"]))

    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = RESULT_DIR / f"lambda_throttle_boundary_trial_{datetime.now().strftime('%Y%m%d')}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(trials, f, ensure_ascii=False, indent=2)

    logger.info("=== 결과 요약 (동시성 -> anomaly_flag) ===")
    for t in trials:
        logger.info("  %s (concurrency=%s): anomaly_flag=%s anomaly_type=%s action=%s",
                     t["resource_id"], t["concurrency_setting"], t.get("anomaly_flag"),
                     t.get("anomaly_type"), t.get("selected_action"))
    logger.info("결과 저장: %s (실제 throttle_rate/iforest 점수는 트리거 후 별도"
                " CloudWatch 재조회로 분석 예정 — measure()는 이 값들을 반환 안 함)", out_path)


if __name__ == "__main__":
    main()
