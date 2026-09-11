"""
playground/lambda_batch_pipeline_replay.py

batch_pipeline_replay.py(EC2용)와 같은 목적을, Lambda 재시도폭증 시나리오용으로
재구성한 것. **저장된 실측 raw_metrics**를 detection에 넣어 전체 파이프라인
(detection -> classification -> decision -> action -> qa -> logging)을 돌려서
탐지 정확도와 별개로 Action 성공률 / QA 통과율 / 파이프라인 실행시간을 잰다.

[EC2용과 다른 점 — 왜 병렬이 아니라 순차인가]
lambda_retry_repeated_trial.py는 EC2처럼 13개의 서로 다른 리소스를 쓰지 않고,
**함수 하나(detection-test-lambda)를 13번 반복 측정**했다(이상 5회 + 정상 8회,
각기 다른 시간대의 재시도폭증 재현). 즉 이 13개 시행은 "같은 리소스에 대한
독립 시행"이라 병렬로 돌리면 여러 시행이 동시에 같은 함수의 concurrency 설정을
두고 경쟁하게 된다(Throttle 액션이 서로 덮어씀, QA가 다른 시행의 액션 직후
상태를 잘못 읽음). 그래서 EC2 replay와 달리 스레드풀 없이 **순차 실행**한다.

detection 입력만 저장된 실측 raw_metrics(재시도폭증이 살아있던 시점)이고, action은
실제 함수에 실행되며(Throttle) QA는 라이브 지표를 재조회하는 하이브리드인 것은
EC2 replay와 동일하다.

⚠️ 실제 AWS 액션이 실행된다 (Lambda cost_spike -> DEC-002 -> Throttle,
   concurrency를 DEFAULT_LAMBDA_THROTTLE_LIMIT로 제한). 같은 함수에 5번
   반복 적용되지만 멱등적이라(같은 값으로 재설정) 문제없다.

[실행 방법]
  python playground/lambda_batch_pipeline_replay.py
  python playground/lambda_batch_pipeline_replay.py --source <lambda_retry_repeated_trial__*.json>

[생성 파일]
  playground/eval_outputs/batch_pipeline_replay__Lambda_{YYYYMMDD_HHMMSS}.json
  (스키마는 batch_pipeline_replay__EC2_*.json과 동일하게 맞춤 — team_results/lambda/
   pipeline_timing.json / action_execution_log.jsonl / cost_prediction_log.jsonl은
   여기서 나온 results를 그대로 쓸 수 있다)
"""

from __future__ import annotations

SCRIPT_VERSION = "1"

import argparse
import glob
import json
import statistics
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

from pipeline.detection_agent import detection_node
from pipeline.classification_agent import classification_node
from pipeline.decision_agent import decision_node
from pipeline.action_agent import action_node
from pipeline.QA_agent import qa_node
from pipeline.logging_agent import logging_node
from pipeline.cost_estimator import estimate_cost_series

RESULT_DIR = PROJECT_ROOT / "playground" / "eval_outputs"


def find_source() -> Path:
    candidates = sorted(glob.glob(str(RESULT_DIR / "lambda_retry_repeated_trial__*.json")))
    if not candidates:
        raise FileNotFoundError(f"{RESULT_DIR}에 lambda_retry_repeated_trial__*.json 원본이 없음")
    return Path(candidates[-1])


def _build_state(resource_id: str, raw_metrics: dict, measured_at: str | None = None) -> dict:
    """batch_pipeline_replay.py._build_state와 동일한 패턴(EC2 docstring 참고).

    Lambda 저장 raw_metrics엔 EC2와 달리 이미 cost가 들어있는 경우가 대부분이지만
    (실측 확인), 혹시 빠진 시행이 있을 경우를 대비해 EC2와 동일하게 없으면
    estimate_cost_series()로 채운다. EC2용 age-guard(resource_age_seconds)는
    Lambda엔 없는 개념이라 None으로 둔다(measure_pipeline_timing.py와 동일)."""
    if "cost" not in raw_metrics:
        usage_metrics = {k: v for k, v in raw_metrics.items() if k != "cost"}
        end_time = datetime.fromisoformat(measured_at) if measured_at else None
        raw_metrics = {
            **raw_metrics,
            "cost": estimate_cost_series(
                "Lambda", resource_id, usage_metrics,
                end_time=end_time, currently_running=True,
            ),
        }

    return {
        "trace_id": None,
        "resource_id": resource_id,
        "resource_type": "Lambda",
        "raw_metrics": raw_metrics,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "resource_age_seconds": None,
        "anomaly_flag": False,
        "anomaly_score_zscore": None,
        "anomaly_score_iforest": None,
        "triggered_metrics": [],
        "anomaly_type": None,
        "classification_reasoning": None,
        "interim_action_taken": None,
        "matched_rule_id": None,
        "candidate_actions": [],
        "selected_action": None,
        "risk_level": None,
        "requires_approval": False,
        "decision_reasoning": None,
        "target_instance_type": None,
        "pre_action_snapshot": None,
        "action_executed": None,
        "action_result": None,
        "qa_passed": None,
        "sla_check_result": None,
        "rollback_count": 0,
        "qa_matched_rule_id": None,
        "whitelisted": False,
        "log_entries": [],
    }


def run_one(rep: int, label: str, raw_metrics: dict, measured_at: str | None,
            bypass_approval_for_timing: bool = False) -> dict:
    """저장된 지표로 detection부터 시작해 파이프라인을 끝까지 흘린다 (Lambda, 1시행)."""
    resource_id = "detection-test-lambda"
    timings: dict[str, float] = {}
    t_total = time.time()
    out: dict = {"resource_id": f"{resource_id}#{rep}", "label": label, "rep": rep}

    try:
        state = _build_state(resource_id, raw_metrics, measured_at)

        t0 = time.time()
        state = detection_node(state)
        timings["detection"] = time.time() - t0
        out["anomaly_flag"] = bool(state["anomaly_flag"])
        out["anomaly_score_zscore"] = state["anomaly_score_zscore"]
        out["anomaly_score_iforest"] = state["anomaly_score_iforest"]
        out["triggered_metrics"] = state["triggered_metrics"]
        print(f"[{label} rep={rep}] detection {timings['detection']:.3f}s "
              f"-> flag={state['anomaly_flag']} triggered={state['triggered_metrics']}")

        if not state["anomaly_flag"]:
            timings["total"] = time.time() - t_total
            out["stopped_at"] = "detection"
            out["timings"] = {k: round(v, 3) for k, v in timings.items()}
            out["elapsed_sec"] = round(time.time() - t_total, 1)
            return out

        t0 = time.time()
        state = classification_node(state)
        timings["classification"] = time.time() - t0
        out["anomaly_type"] = state["anomaly_type"]
        out["matched_rule_id"] = state.get("matched_rule_id")

        t0 = time.time()
        state = decision_node(state)
        timings["decision"] = time.time() - t0
        out["selected_action"] = state["selected_action"]
        out["risk_level"] = state["risk_level"]
        out["requires_approval"] = bool(state["requires_approval"])
        out["matched_decision_rule_id"] = state.get("matched_decision_rule_id")
        out["candidate_actions"] = state.get("candidate_actions")
        print(f"[{label} rep={rep}] decision -> {state['selected_action']} "
              f"risk={state['risk_level']} approval={state['requires_approval']}")

        out["approval_bypassed_for_timing"] = False
        if state["requires_approval"] and bypass_approval_for_timing:
            # [측정 전용] cost_spike는 ANOMALY_TYPE_DEFAULT_RISK상 액션과 무관하게 항상
            # MED라 실제 운영에서는 항상 사람 승인이 필요하다(schema/state.py 참고).
            # 승인 대기시간은 무한정이라 자동 측정이 불가능하므로, Action/QA/timing을
            # 재기 위해서만 여기서 우회한다 — 실제 승인 게이트 정책을 바꾸는 게 아니다.
            print(f"[{label} rep={rep}] requires_approval=True - 타이밍 측정 목적으로만 승인 게이트 우회함")
            state["requires_approval"] = False
            out["approval_bypassed_for_timing"] = True
        elif state["requires_approval"]:
            timings["total"] = time.time() - t_total
            out["stopped_at"] = "approval_gate"
            out["timings"] = {k: round(v, 3) for k, v in timings.items()}
            out["elapsed_sec"] = round(time.time() - t_total, 1)
            return out

        t0 = time.time()
        state = action_node(state)
        timings["action"] = time.time() - t0
        out["action_executed"] = state.get("action_executed")
        out["action_result"] = state.get("action_result")
        out["action_success"] = (state.get("action_result") or {}).get("status") == "success"
        print(f"[{label} rep={rep}] action {timings['action']:.3f}s "
              f"-> {state.get('action_executed')} {(state.get('action_result') or {}).get('status')}")

        t0 = time.time()
        state = qa_node(state)
        timings["qa"] = time.time() - t0
        out["qa_passed"] = state.get("qa_passed")
        out["sla_check_result"] = state.get("sla_check_result")
        out["rollback_count"] = state.get("rollback_count", 0)
        out["qa_matched_rule_id"] = state.get("qa_matched_rule_id")
        print(f"[{label} rep={rep}] qa {timings['qa']:.3f}s -> passed={state.get('qa_passed')}")

        t0 = time.time()
        try:
            state = logging_node(state)
        except Exception as exc:
            print(f"[rep={rep}] logging 실패(타이밍엔 영향 없음): {exc}")
        timings["logging"] = time.time() - t0

        timings["total"] = time.time() - t_total
        out["stopped_at"] = None
        out["timings"] = {k: round(v, 3) for k, v in timings.items()}
        out["elapsed_sec"] = round(time.time() - t_total, 1)
        return out

    except Exception as exc:
        print(f"[rep={rep}] 실패: {exc}\n{traceback.format_exc()}")
        out["error"] = str(exc)
        out["timings"] = {k: round(v, 3) for k, v in timings.items()}
        out["elapsed_sec"] = round(time.time() - t_total, 1)
        return out


def _summarize(results: list[dict]) -> dict:
    ok = [r for r in results if "error" not in r]
    anomaly = [r for r in ok if r["label"] == "anomaly"]
    normal = [r for r in ok if r["label"] == "normal"]

    tp = sum(1 for r in anomaly if r.get("anomaly_flag"))
    fn = len(anomaly) - tp
    fp = sum(1 for r in normal if r.get("anomaly_flag"))
    tn = len(normal) - fp

    executed = [r for r in ok if r.get("action_executed")]
    action_ok = [r for r in executed if r.get("action_success")]
    qa_judged = [r for r in executed if r.get("qa_passed") is not None]
    qa_pass = [r for r in qa_judged if r.get("qa_passed") is True]

    def stat(key: str) -> dict | None:
        vals = [r["timings"][key] for r in ok if key in r.get("timings", {})]
        if not vals:
            return None
        return {"n": len(vals), "mean_sec": round(statistics.mean(vals), 3),
                "stdev_sec": round(statistics.stdev(vals), 3) if len(vals) > 1 else 0.0,
                "min_sec": round(min(vals), 3), "max_sec": round(max(vals), 3)}

    total = tp + fn + fp + tn
    return {
        "n_resources": len(results),
        "n_error": len(results) - len(ok),
        "detection": {
            "confusion_matrix": {"TP": tp, "FN": fn, "FP": fp, "TN": tn},
            "accuracy": (tp + tn) / total if total else None,
            "recall": tp / (tp + fn) if (tp + fn) else None,
            "false_positive_rate": fp / (fp + tn) if (fp + tn) else None,
        },
        "stopped_at": {k: sum(1 for r in ok if r.get("stopped_at") == k)
                       for k in ["detection", "approval_gate", None]},
        "action": {
            "n_executed": len(executed),
            "n_success": len(action_ok),
            "success_rate": (len(action_ok) / len(executed)) if executed else None,
            "by_action": {a: sum(1 for r in executed if r.get("action_executed") == a)
                          for a in sorted({r.get("action_executed") for r in executed if r.get("action_executed")})},
        },
        "qa": {
            "n_judged": len(qa_judged),
            "n_passed": len(qa_pass),
            "pass_rate": (len(qa_pass) / len(qa_judged)) if qa_judged else None,
        },
        "timings": {k: stat(k) for k in
                    ["detection", "classification", "decision", "action", "qa", "logging", "total"]},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default=None,
                        help="원본 실측 결과 JSON (생략 시 최신 lambda_retry_repeated_trial__*.json)")
    parser.add_argument("--bypass-approval", action="store_true",
                        help="[측정 전용] cost_spike는 항상 MED라 실제 승인 대기가 무한정이므로, "
                             "Action/QA/timing 측정을 위해서만 승인 게이트를 우회한다")
    args = parser.parse_args()

    source = Path(args.source) if args.source else find_source()
    payload_src = json.loads(source.read_text(encoding="utf-8"))
    trials = payload_src["anomaly_trials"] + payload_src["normal_trials"]
    print(f"원본: {source.name} (측정 {payload_src.get('generated_at')}, 시행 {len(trials)}개 "
          f"- 같은 함수를 순차 재생하므로 병렬 실행 안 함)")

    print(f"=== Lambda {len(trials)}개 순차 재생 파이프라인 시작 ===")
    t0 = time.time()
    results: list[dict] = []
    for t in trials:
        after = t.get("after") or {}
        if not after.get("raw_metrics"):
            print(f"[rep={t.get('rep')}] raw_metrics 없음 - 건너뜀")
            continue
        results.append(run_one(t["rep"], t["label"], after["raw_metrics"],
                                payload_src.get("generated_at"), args.bypass_approval))

    summary = _summarize(results)
    print("\n=== 집계 ===")
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = RESULT_DIR / f"batch_pipeline_replay__Lambda_{ts}.json"
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    out = {
        "script_version": SCRIPT_VERSION,
        "resource_type": "Lambda",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "replayed_from": source.name,
        "replay_note": ("detection 입력만 저장된 실측 raw_metrics(재시도폭증이 살아있던 시점)이고, "
                        "action은 실제 함수에 실행되며(Throttle) QA는 라이브 지표를 재조회하는 "
                        "하이브리드. EC2와 달리 13개 시행이 같은 함수 하나를 공유하므로 순차 실행함."),
        "bypass_approval_for_timing": args.bypass_approval,
        "bypass_approval_note": ("cost_spike는 ANOMALY_TYPE_DEFAULT_RISK상 액션과 무관하게 항상 MED라 "
                                 "실제 운영에서는 항상 사람 승인이 필요하다. 승인 대기시간은 무한정이라 "
                                 "자동 측정이 불가능하므로, --bypass-approval을 켰을 때만 Action/QA/timing "
                                 "측정을 위해 게이트를 우회했다 — 실제 승인 정책이 바뀐 게 아니다."),
        "wall_clock_sec": round(time.time() - t0, 1),
        "summary": summary,
        "results": results,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"\n결과 저장: {out_path}")


if __name__ == "__main__":
    main()
