"""
playground/lambda_throttle_retry_trial.py

Lambda "스로틀(429)/시스템 에러 재시도 폭증" 시나리오의 실 AWS 종단 검증
(계획서 C/F 항목 — 2026-09-12 계획서 참고).

⚠️ lambda_retry_repeated_trial.py("에러로 인한 재시도" 시나리오)와는 완전히 다른
메커니즘이다. 그쪽은 함수 코드가 예외를 던져서 생기는 재시도(최대 2회, ~3분,
invocation_count/error_count에 그대로 잡힘)를 다루고, 이 스크립트는 "동시성 소진으로
인한 스로틀" 재시도(정해진 횟수 없음, 최대 6시간)를 다룬다 — AWS 공식 문서 확인:
이 재시도는 invocation_count/error_count(Invocations/Errors)에 전혀 안 잡히고
Throttles/AsyncEventAge에만 나타난다. 그래서 트리거 방법도, 판정에 쓰는 지표도 다르다.

[재현 방법론 — 반드시 읽을 것]
이 스크립트는 organic한 대량 트래픽을 재현하는 게 아니라, EC2 zombie 시나리오
(EC2_IDLE_* 체크, playground/ec2_zombie_replay_trial.py)와 동일한 논리로
"인위적으로 조건을 만들어 재현하는 테스트 기법"이다:
  - 대상 함수의 Reserved Concurrency를 인위적으로 1로 낮춘다 (원래는 미설정 상태)
  - MaximumEventAgeInSeconds를 60초(최솟값)로 낮춰서 재시도가 최대 6시간이 아니라
    수분 안에 끝나게 한다
  - 그 상태에서 동시(스레드) 호출 버스트를 쏴서 동시성 캡을 초과시켜 스로틀→재시도를
    유도한다
관찰되는 현상(Throttles 급증, AsyncEventAge 상승)은 이 함수에 대한 실제 이상 신호가
맞지만, 재현 방법 자체는 자연 발생 트래픽이 아니라 설정을 조작한 것이라는 점을
명시한다.

[실측 근거 — F-1, 2026-09-12, detection-retry-storm-test 대상]
- Reserved Concurrency=0(완전 차단)은 "시도" 자체가 없어서 Throttles/AsyncEventAge가
  전혀 안 찍힘 (AsyncEventsReceived/AsyncEventsDropped만 찍힘) — 이 스크립트는 쓰지 않음.
- Reserved Concurrency=1 + 40건 동시 버스트: Throttles=109건(요청의 2.7배),
  AsyncEventAge 평균 2.5s(최대 7.6s)까지 상승 — 신호 확인됨.
- Reserved Concurrency=1 + 1건/초 트리클(99건, 150초): Throttles=0 — 완만한 도착은
  동시성=1이어도 그때그때 처리돼서 백로그 자체가 안 생김. **버스트 강도가 핵심이지
  지속시간이 핵심이 아니다.**
- Reserved Concurrency=1 + 40건 버스트 x 3회(5분 간격, =PERSISTENCE_WINDOW_POINTS 1구간):
  구간별 throttle_rate = 0.50 / 0.75 / 0.67 — 3구간 연속 유지, 0으로 안 떨어짐.
  → 이 스크립트의 기본값(BURST_SIZE=40, N_BURSTS=3, BURST_INTERVAL_SEC=300)은 이 실측
    그대로 채택한 것이다.
- AsyncEventsDropped는 위 어떤 조건에서도 유의미하게 안 나와서(전부 0) 이번 스코프의
  판정 지표에서 제외했다(계획서 A/B 참고).

[실행 방법]
  python playground/lambda_throttle_retry_trial.py --run --function-name detection-retry-storm-test
  python playground/lambda_throttle_retry_trial.py --restore --function-name detection-retry-storm-test

[생성 파일]
  playground/eval_outputs/lambda_throttle_retry_trial_{날짜}.json
  playground/eval_outputs/logs/lambda_throttle_retry_trial_{시각}.log
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor
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

# ⚠️ 2026-09-13 실측으로 발견: .env의 AWS_PROFILE(detection-runtime, DetectionRuntimeRole)은
# 프로덕션 파이프라인이 실제로 필요한 권한(PutFunctionConcurrency 등, Throttle 액션)만
# 최소권한으로 갖고 있어서 lambda:*FunctionEventInvokeConfig 권한이 없다 — 이건 테스트
# 하네스가 재시도 시간을 단축하려고 쓰는 설정일 뿐 프로덕션 액션이 아니므로 애초에
# 권한을 줄 필요가 없었던 것. 그래서 이 스크립트의 "테스트 셋업"(스냅샷/버스트 발사/원복)
# 전용 클라이언트는 SETUP_PROFILE(기본 default, 전체 권한)을 명시적으로 써서
# .env가 프로세스 전역에 심어둔 AWS_PROFILE을 우회한다. measure()가 내부적으로 호출하는
# 프로덕션 파이프라인 노드(action_node의 실제 Throttle 등)는 이 영향을 안 받고
# 그대로 실제 운영 자격증명(detection-runtime)으로 실행돼야 검증 의미가 있다.
SETUP_PROFILE = "default"


def _setup_lambda_client():
    return boto3.Session(profile_name=SETUP_PROFILE).client("lambda", region_name=AWS_REGION)

# ── F-1 실측으로 검증된 기본값 (위 docstring 참고, 임의로 늘리지 말 것) ──────
BURST_SIZE = 40
N_BURSTS = 3                # = PERSISTENCE_WINDOW_POINTS
BURST_INTERVAL_SEC = 300    # = period_seconds 기본값 (1구간)
MAX_EVENT_AGE_SEC = 60      # AWS 최솟값
POST_BURST_WAIT_SEC = 180   # 마지막 버스트 이후 지표 반영 대기

LOG_DIR = PROJECT_ROOT / "playground" / "eval_outputs" / "logs"
RESULT_DIR = PROJECT_ROOT / "playground" / "eval_outputs"

logger = logging.getLogger("lambda_throttle_retry_trial")


def _setup_logging() -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = LOG_DIR / f"lambda_throttle_retry_trial_{ts}.log"
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


def _snapshot_path(function_name: str) -> Path:
    return RESULT_DIR / f".lambda_throttle_retry_trial_snapshot__{function_name}.json"


def _snapshot(lam, function_name: str) -> dict:
    """현재 상태를 조회한다 — "원래 상태"가 아니라 호출 시점의 현재 상태다.
    ⚠️ 2026-09-13 버그로 발견: 원복 시점에 이 함수를 다시 불러서 "현재 값"을
    "원래 값"으로 오인하면 안 된다 — 그 사이 프로덕션 액션(Throttle)이 값을
    바꿔놨을 수 있다(실제로 1->5로 바뀐 뒤 원복이 "5가 원래 값"이라고 착각해
    그대로 둔 사고가 있었음). 그래서 원복은 반드시 induce_throttle_storm()이
    테스트 시작 "전"에 찍어서 파일로 저장해둔 스냅샷을 읽어서 쓴다
    (_load_saved_snapshot/_save_snapshot 참고) — 이 함수 자체는 스냅샷을
    "찍는" 용도로만 쓰고, 복원 대상 값을 "알아내는" 용도로 재사용하지 않는다.
    """
    try:
        concurrency = lam.get_function_concurrency(FunctionName=function_name).get(
            "ReservedConcurrentExecutions"
        )
    except Exception as exc:
        logger.warning("동시성 조회 실패: %s", exc)
        concurrency = None

    had_event_invoke_config = True
    try:
        lam.get_function_event_invoke_config(FunctionName=function_name)
    except lam.exceptions.ResourceNotFoundException:
        had_event_invoke_config = False

    return {"concurrency": concurrency, "had_event_invoke_config": had_event_invoke_config}


def _save_snapshot(function_name: str, snap: dict) -> None:
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    with open(_snapshot_path(function_name), "w", encoding="utf-8") as f:
        json.dump(snap, f)


def _load_saved_snapshot(function_name: str) -> dict | None:
    path = _snapshot_path(function_name)
    if not path.exists():
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def restore(function_name: str) -> None:
    """저장된 사전 스냅샷(induce_throttle_storm이 테스트 "전"에 찍어둔 것)으로
    원복한다. --run의 finally에서 자동 호출되지만, 중간에 프로세스가 죽어서
    거기까지 못 갔을 때 --restore로 수동 실행하는 용도로도 쓴다.

    저장된 스냅샷 파일이 없으면(예: --run을 아예 실행한 적이 없거나 이미
    한 번 복구해서 파일이 지워진 경우) 현재 상태를 그대로 조회해서 "미설정"
    쪽으로 안전하게 정리한다 — 이 경우엔 진짜 원래 값을 알 방법이 없다는
    걸 경고로 남긴다.
    """
    lam = _setup_lambda_client()
    snap = _load_saved_snapshot(function_name)
    if snap is None:
        logger.warning(
            "[원복] 저장된 사전 스냅샷이 없음 — 진짜 원래 값을 알 수 없어 "
            "동시성/event-invoke-config를 미설정 상태로 정리한다(안전 기본값)."
        )
        snap = {"concurrency": None, "had_event_invoke_config": False}

    if snap["concurrency"] is None:
        try:
            lam.delete_function_concurrency(FunctionName=function_name)
            logger.info("[원복] 동시성 설정 제거 완료 (원래 미설정)")
        except Exception as exc:
            logger.warning("[원복] 동시성 제거 실패(이미 없을 수 있음): %s", exc)
    else:
        lam.put_function_concurrency(
            FunctionName=function_name, ReservedConcurrentExecutions=snap["concurrency"]
        )
        logger.info("[원복] 동시성 %s로 복구 완료", snap["concurrency"])

    if not snap["had_event_invoke_config"]:
        try:
            lam.delete_function_event_invoke_config(FunctionName=function_name)
            logger.info("[원복] event-invoke-config 제거 완료 (원래 미설정)")
        except Exception as exc:
            logger.warning("[원복] event-invoke-config 제거 실패(이미 없을 수 있음): %s", exc)
    else:
        logger.warning(
            "[원복] 원래 event-invoke-config가 있었음(이 스크립트가 만든 게 아님) — "
            "값 자체는 이 스크립트가 알지 못하므로 수동 확인 필요."
        )

    path = _snapshot_path(function_name)
    if path.exists():
        path.unlink()


def induce_throttle_storm(function_name: str) -> dict:
    """BURST_SIZE건 동시 호출을 N_BURSTS회, BURST_INTERVAL_SEC 간격으로 발사한다.
    F-1 실측(위 docstring)으로 검증된 유일한 조합 — 순차/트리클 방식은 신호가
    전혀 안 나온다는 걸 확인했으므로 바꾸지 말 것.
    """
    lam = _setup_lambda_client()

    logger.info("[유발 %s] 사전 상태 스냅샷...", function_name)
    snap = _snapshot(lam, function_name)
    _save_snapshot(function_name, snap)  # 원복 시점엔 이미 값이 바뀌어 있을 수 있어 반드시 지금 저장
    logger.info("[유발 %s] 원래 동시성=%s, event-invoke-config 있었음=%s",
                function_name, snap["concurrency"], snap["had_event_invoke_config"])

    lam.put_function_event_invoke_config(
        FunctionName=function_name,
        MaximumEventAgeInSeconds=MAX_EVENT_AGE_SEC,
        MaximumRetryAttempts=2,
    )
    lam.put_function_concurrency(FunctionName=function_name, ReservedConcurrentExecutions=1)
    logger.info("[유발 %s] ReservedConcurrentExecutions=1, MaximumEventAgeInSeconds=%ds 설정 완료",
                function_name, MAX_EVENT_AGE_SEC)
    time.sleep(3)  # 설정 전파 대기

    def _invoke(i: int) -> bool:
        try:
            lam.invoke(FunctionName=function_name, InvocationType="Event", Payload=b"{}")
            return True
        except Exception as exc:
            logger.warning("  invoke #%d 실패: %s", i, exc)
            return False

    burst_times = []
    for b in range(N_BURSTS):
        burst_start = datetime.now(timezone.utc)
        burst_times.append(burst_start.isoformat())
        with ThreadPoolExecutor(max_workers=BURST_SIZE) as ex:
            results = list(ex.map(_invoke, range(BURST_SIZE)))
        logger.info("[유발 %s] 버스트 %d/%d 발사 완료: 성공 %d/%d",
                    function_name, b + 1, N_BURSTS, sum(results), BURST_SIZE)
        if b < N_BURSTS - 1:
            logger.info("[유발 %s] 다음 버스트까지 %d초 대기...", function_name, BURST_INTERVAL_SEC)
            time.sleep(BURST_INTERVAL_SEC)

    logger.info("[유발 %s] 전체 버스트 종료. %d초 추가 대기(지표 반영)...",
                function_name, POST_BURST_WAIT_SEC)
    time.sleep(POST_BURST_WAIT_SEC)

    return {"function_name": function_name, "burst_times_utc": burst_times,
            "burst_size": BURST_SIZE, "n_bursts": N_BURSTS,
            "orig_snapshot": {"concurrency": snap["concurrency"]}}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="store_true", help="유발 + 파이프라인 종단 실측 + 원복")
    parser.add_argument("--restore", action="store_true", help="수동 원복만 실행 (--run 중간에 실패했을 때)")
    parser.add_argument("--function-name", default="detection-retry-storm-test",
                        help="대상 Lambda 함수명 (기본: 자체 계정 detection-retry-storm-test)")
    args = parser.parse_args()

    if not args.run and not args.restore:
        parser.error("--run 또는 --restore 중 하나는 지정해야 함")

    _setup_logging()

    if args.restore:
        restore(args.function_name)
        return

    induce_result = None
    try:
        induce_result = induce_throttle_storm(args.function_name)

        logger.info("파이프라인 종단 실측 시작 (detection -> classification -> decision -> action -> QA)...")
        measure_result = measure(args.function_name, "Lambda", bypass_approval_for_timing=True)
        logger.info(
            "측정 완료 — anomaly_flag=%s, anomaly_type=%s, action=%s, qa_passed=%s",
            measure_result.get("anomaly_flag"),
            measure_result.get("anomaly_type"),
            measure_result.get("selected_action"),
            measure_result.get("qa_passed"),
        )

        RESULT_DIR.mkdir(parents=True, exist_ok=True)
        out_path = RESULT_DIR / f"lambda_throttle_retry_trial_{datetime.now().strftime('%Y%m%d')}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump({"induce": induce_result, "measure": measure_result}, f, ensure_ascii=False, indent=2)
        logger.info("결과 저장: %s", out_path)

    finally:
        logger.info("원복 시작...")
        restore(args.function_name)


if __name__ == "__main__":
    main()
