"""
playground/run_3x_real_pipeline.py

Lambda "에러 재시도 폭증" 시나리오의 파이프라인 타이밍/절감액 실측 (팀원 가이드 B).

타겟 Lambda 3개에 짧게(기본 15분 = 탐지가 보는 지속성 구간 3개) 재시도 폭증을 병렬로
유발한 뒤, 곧바로 measure_pipeline_timing.measure(..., bypass_approval_for_timing=True)를
3개 모두에 대해 병렬로 돌린다. measure()는 리소스 타입과 무관하게 이미 작성돼 있으므로
그대로 재사용하고, 이 스크립트가 새로 하는 일은 "Lambda에 이상을 유발하는 것"뿐이다.

⚠️ 유발 시간(--induce-minutes)의 기본값 15분에는 이유가 있다. detection_agent의
   _lambda_error_rate_check는 최근 PERSISTENCE_WINDOW_POINTS(=3)개 포인트가 "전부"
   invocation_count >= 10 AND error_count/invocation_count >= 0.5 여야 트리거된다.
   포인트 하나가 5분이므로 3개 = 15분. 이보다 짧으면 마지막 구간만 채워져서 지속성
   조건에 걸리고, 파이프라인이 detection에서 끝나 Action/QA 시간을 못 잰다.

⚠️ 이 스크립트는 **실제 액션을 실행한다**. Lambda cost_spike -> Throttle(DEC-002)이므로
   대상 함수의 예약 동시성이 action_agent.DEFAULT_LAMBDA_THROTTLE_LIMIT(=5)로 설정된다.
   원복은 --restore 로 따로 실행한다 — 측정 직후에 원복하면 안 되기 때문이다:
   verify_cost_predictions.py(가이드 D)가 60분 뒤에 "예측한 절감액이 실제로 실현됐는지"를
   CloudWatch로 재조회하는데, 그 전에 스로틀을 풀어버리면 되돌린 액션의 효과를 재는 꼴이
   된다. 순서는 B -> C -> (60분) -> D -> --restore.

[실행 방법]
  python playground/run_3x_real_pipeline.py --run --function-set teammate
  python playground/run_3x_real_pipeline.py --restore --function-set teammate   # D 끝난 뒤

[생성 파일]
  playground/eval_outputs/pipeline_timing_lambda_{날짜}.json   (measure() 리턴값들의 리스트)
  playground/eval_outputs/logs/run_3x_real_pipeline_{시각}.log
"""

from __future__ import annotations

SCRIPT_VERSION = "1"

import argparse
import json
import logging
import random
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

import boto3

from playground.lambda_retry_repeated_trial import (
    AWS_REGION, ERROR_PAYLOAD, OK_PAYLOAD, FUNCTION_SETS, _account_of,
)
from playground.measure_pipeline_timing import measure

LOG_DIR = PROJECT_ROOT / "playground" / "eval_outputs" / "logs"
RESULT_DIR = PROJECT_ROOT / "playground" / "eval_outputs"

logger = logging.getLogger("run_3x_real_pipeline")


def _setup_logging() -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = LOG_DIR / f"run_3x_real_pipeline_{ts}.log"
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.addHandler(fh)
    logger.addHandler(sh)
    logger.propagate = False
    return log_path


def induce_retry_surge(function_name: str, minutes: int, error_rate: float,
                       interval_sec: int, jitter_ratio: float = 0.1) -> dict:
    """지정한 함수에 minutes 동안 재시도 폭증을 유발한다.

    비동기(Event) 호출이라 에러 하나가 Lambda 자동 재시도(기본 2회)를 더 부르고 그
    재시도도 Invocations/Errors에 잡힌다 — 그게 이 시나리오가 재현하려는 "재시도 폭증"이다.
    """
    t0 = time.time()
    lam = boto3.client("lambda", region_name=AWS_REGION)
    interval = max(2, round(interval_sec * random.uniform(1 - jitter_ratio, 1 + jitter_ratio)))
    deadline = t0 + minutes * 60
    n_calls, n_err, n_failed = 0, 0, 0
    logger.info("[유발 %s] %d분간 에러율 %.2f, 간격 %ds 시작",
                function_name, minutes, error_rate, interval)
    while time.time() < deadline:
        is_err = random.random() < error_rate
        try:
            lam.invoke(FunctionName=function_name, InvocationType="Event",
                       Payload=ERROR_PAYLOAD if is_err else OK_PAYLOAD)
            n_calls += 1
            n_err += int(is_err)
        except Exception as exc:
            n_failed += 1
            logger.warning("[유발 %s] invoke 실패: %s", function_name, exc)
        time.sleep(interval)
    logger.info("[유발 %s] 완료 — %d회 호출(강제 에러 %d, 실패 %d), %.1f분",
                function_name, n_calls, n_err, n_failed, (time.time() - t0) / 60)
    return {"function_name": function_name, "n_calls": n_calls, "n_forced_errors": n_err,
            "n_invoke_failed": n_failed, "interval_sec": interval,
            "induce_minutes": minutes, "elapsed_sec": round(time.time() - t0, 1)}


def _measure_one(function_name: str) -> dict:
    logger.info("[측정 %s] measure() 시작 (bypass_approval_for_timing=True)", function_name)
    try:
        result = measure(function_name, "Lambda", bypass_approval_for_timing=True)
        logger.info("[측정 %s] 완료 — anomaly_flag=%s, action=%s, qa_passed=%s, total=%.1fs",
                    function_name, result.get("anomaly_flag"), result.get("selected_action"),
                    result.get("qa_passed"), result.get("timings", {}).get("total", -1))
    except Exception as exc:
        logger.error("[측정 %s] 실패: %s", function_name, exc)
        logger.error(traceback.format_exc())
        return {"resource_id": function_name, "resource_type": "Lambda", "error": str(exc)}
    return {**result, "measured_at": datetime.now(timezone.utc).isoformat()}


def _concurrency_snapshot_path(function_name: str) -> Path:
    return RESULT_DIR / f".run_3x_real_pipeline_concurrency_snapshot__{function_name}.json"


def snapshot_concurrency_before_run(targets: list[str]) -> None:
    """⚠️ 2026-09-13 수정: 예전엔 --restore 시점에 "현재 값"을 조회해서 그걸 기준으로
    원복 여부를 판단했다 — 이러면 원래(=--run 시작 전) 동시성이 unset이 아니라 어떤
    값으로 설정돼 있던 경우, --restore가 그 진짜 원래 값을 모른 채 "설정돼 있으니
    삭제"해버려서 원래 설정을 지워버리는 사고가 날 수 있다(lambda_throttle_retry_trial.py
    에서 실제로 같은 패턴의 버그 — "원복 시점 재조회 = 원래 값"으로 착각 — 발견됨,
    거기서는 반대로 액션이 바꿔놓은 값을 "원래 값"으로 착각해 원복을 안 한 방향으로
    나타났음). 이제 --run 시작 시점(=아직 아무 액션도 실행되기 전)에 스냅샷을 찍어
    파일로 저장해두고, restore_concurrency는 그 파일을 기준으로 복원한다.
    """
    lam = boto3.client("lambda", region_name=AWS_REGION)
    for fn in targets:
        try:
            current = lam.get_function_concurrency(FunctionName=fn).get("ReservedConcurrentExecutions")
        except Exception as exc:
            logger.warning("[스냅샷 %s] 조회 실패: %s", fn, exc)
            continue
        RESULT_DIR.mkdir(parents=True, exist_ok=True)
        with open(_concurrency_snapshot_path(fn), "w", encoding="utf-8") as f:
            json.dump({"concurrency": current}, f)
        logger.info("[스냅샷 %s] --run 시작 전 동시성=%s 저장", fn, current)


def restore_concurrency(targets: list[str]) -> None:
    """snapshot_concurrency_before_run()이 --run 시작 시점에 저장해둔 값 기준으로
    원상복구한다 (D 실행이 끝난 뒤에 쓸 것). 저장된 스냅샷이 없으면(예: --run 없이
    --restore만 실행했거나 이미 한 번 복구해서 파일이 지워진 경우) 원래 값을 알 수
    없으므로 안전 기본값(미설정)으로 정리하고 경고를 남긴다.
    """
    lam = boto3.client("lambda", region_name=AWS_REGION)
    for fn in targets:
        path = _concurrency_snapshot_path(fn)
        if path.exists():
            with open(path, encoding="utf-8") as f:
                orig_concurrency = json.load(f)["concurrency"]
        else:
            logger.warning(
                "[원복 %s] 저장된 사전 스냅샷이 없음 — 진짜 원래 값을 알 수 없어 "
                "미설정 상태로 정리한다(안전 기본값).", fn,
            )
            orig_concurrency = None

        if orig_concurrency is None:
            try:
                lam.delete_function_concurrency(FunctionName=fn)
                logger.info("[원복 %s] 예약 동시성 설정 제거 완료 (원래 미설정)", fn)
            except Exception as exc:
                logger.warning("[원복 %s] 제거 실패(이미 없을 수 있음): %s", fn, exc)
        else:
            lam.put_function_concurrency(FunctionName=fn, ReservedConcurrentExecutions=orig_concurrency)
            logger.info("[원복 %s] 예약 동시성 %s로 복구 완료", fn, orig_concurrency)

        if path.exists():
            path.unlink()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="store_true", help="유발 + measure() 실행")
    parser.add_argument("--restore", action="store_true",
                        help="Throttle로 걸린 예약 동시성 제거 (D 실행 후에 쓸 것)")
    parser.add_argument("--function-set", default="teammate", choices=sorted(FUNCTION_SETS))
    parser.add_argument("--n-targets", type=int, default=3, help="타겟 함수 수 (기본 3)")
    parser.add_argument("--induce-minutes", type=int, default=15,
                        help="이상 유발 시간(분). 기본 15 = 지속성 구간 3개 (위 주석 참고)")
    parser.add_argument("--error-rate", type=float, default=0.8,
                        help="유발 구간 에러율 (기본 0.8 — 임계값 0.5를 지터로도 안 내려가게)")
    parser.add_argument("--interval-sec", type=int, default=20,
                        help="호출 간격(초). 기본 20 = 구간당 15회 >= 최소 호출수 게이트 10")
    args = parser.parse_args()

    log_path = _setup_logging()
    logger.info("로그 파일: %s (SCRIPT_VERSION=%s)", log_path, SCRIPT_VERSION)

    anomaly_names, _ = FUNCTION_SETS[args.function_set]
    targets = list(anomaly_names)[: args.n_targets]

    account_id, arn = _account_of(None)
    logger.info("AWS 자격증명: %s (account %s)", arn, account_id)
    logger.info("타겟 함수 %d개: %s", len(targets), targets)

    if args.restore:
        restore_concurrency(targets)
        return

    if not args.run:
        parser.print_help()
        return

    # ── 0) 액션(Throttle)이 실행되기 전, 원복 기준이 될 사전 동시성 스냅샷 저장 ──
    snapshot_concurrency_before_run(targets)

    # ── 1) 3개 함수에 병렬로 재시도 폭증 유발 ────────────────────────────────
    t_start = time.time()
    induce_results = []
    with ThreadPoolExecutor(max_workers=len(targets)) as ex:
        futures = {ex.submit(induce_retry_surge, fn, args.induce_minutes,
                             args.error_rate, args.interval_sec): fn for fn in targets}
        for fut in as_completed(futures):
            induce_results.append(fut.result())

    # ── 2) 유발 직후 3개 모두 파이프라인 전체 실측 (병렬) ─────────────────────
    logger.info("=== 유발 완료 (%.1f분). measure() %d개 병렬 시작 ===",
                (time.time() - t_start) / 60, len(targets))
    measure_results = []
    with ThreadPoolExecutor(max_workers=len(targets)) as ex:
        futures = {ex.submit(_measure_one, fn): fn for fn in targets}
        for fut in as_completed(futures):
            measure_results.append(fut.result())
    measure_results.sort(key=lambda r: targets.index(r["resource_id"]))

    # ── 3) 저장 ─────────────────────────────────────────────────────────────
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")
    out_path = RESULT_DIR / f"pipeline_timing_lambda_{date_str}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(measure_results, f, ensure_ascii=False, indent=2)

    detail_path = RESULT_DIR / f"pipeline_timing_lambda_{date_str}__induce_detail.json"
    with open(detail_path, "w", encoding="utf-8") as f:
        json.dump({"script_version": SCRIPT_VERSION, "scenario": "lambda_error_retry_surge",
                   "generated_at": datetime.now(timezone.utc).isoformat(),
                   "aws_account_id": account_id, "function_set": args.function_set,
                   "params": {"induce_minutes": args.induce_minutes, "error_rate": args.error_rate,
                              "interval_sec": args.interval_sec, "n_targets": len(targets)},
                   "induce_results": induce_results}, f, ensure_ascii=False, indent=2)

    summary = [{"resource_id": r.get("resource_id"), "anomaly_flag": r.get("anomaly_flag"),
                "selected_action": r.get("selected_action"), "qa_passed": r.get("qa_passed"),
                "total_sec": r.get("timings", {}).get("total")} for r in measure_results]
    logger.info("=== 결과 ===")
    logger.info(json.dumps(summary, ensure_ascii=False, indent=2))
    logger.info("결과 저장: %s (유발 상세: %s)", out_path, detail_path)
    logger.warning("예약 동시성은 아직 걸려 있음 — D(verify_cost_predictions.py)까지 끝낸 뒤 "
                   "'--restore' 로 원복할 것.")


if __name__ == "__main__":
    main()
