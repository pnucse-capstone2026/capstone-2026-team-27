"""
playground/batch_pipeline_replay.py

**저장된 실측 raw_metrics**를 detection에 넣어 전체 파이프라인
(detection -> classification -> decision -> action -> qa -> logging)을 돌린다.

[batch_pipeline_run.py와의 차이 — 왜 이게 필요한가]
batch_pipeline_run.py는 detection도 라이브 CloudWatch를 조회한다. 그런데 EC2 좀비
실험에 쓴 13대는 부하 생성 스크립트가 2026-09-09 14:42 UTC에 종료돼서, 그 이후로는
정상 8대와 이상 5대가 모두 동일한 유휴 상태다(실측: normal-heavy CPU 25% -> 0.3%).
즉 라이브로 돌리면 "정상 8대"라는 라벨이 현재 데이터를 설명하지 못하고, 정상
인스턴스까지 좀비로 탐지돼 Stop된다.

저장된 raw_metrics는 부하가 살아있던 시점(14:11 UTC)의 값이라 라벨이 유효하다.
그걸 detection에 넣으면:
  - 정상 8대 -> anomaly_flag=False -> detection에서 파이프라인 종료 (Stop 안 됨)
  - 좀비 5대 -> classification -> decision -> action -> qa 까지 진행
결과적으로 라벨이 유효한 판정 위에서 Action 성공률 / QA / 실행시간을 재게 된다.

⚠️ 하이브리드임을 분명히 해둔다.
   detection 입력만 저장된 과거 실측이고, action은 **실제 현재 리소스**에 실행되며
   QA는 라이브 지표를 재조회한다. detection_node/classification_node/decision_node는
   raw_metrics만 보는 순수 함수라 재생이 성립하지만, action/qa는 그렇지 않다.

⚠️ 실제 AWS 액션이 실행된다 (EC2 cost_inefficiency -> DEC-001).
   대상 인스턴스가 running 상태여야 의미가 있으므로, 필요하면 --ensure-running으로
   먼저 start하고 running이 될 때까지 기다린다.

[실행 방법]
  python playground/batch_pipeline_replay.py --ensure-running
  python playground/batch_pipeline_replay.py --source <ec2_repeated_trial__*.json>

[생성 파일]
  playground/eval_outputs/batch_pipeline_replay__EC2_{YYYYMMDD_HHMMSS}.json
"""

from __future__ import annotations

SCRIPT_VERSION = "1"

import argparse
import glob
import json
import os
import statistics
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

from pipeline.detection_agent import detection_node
from pipeline.classification_agent import classification_node
from pipeline.decision_agent import decision_node
from pipeline.action_agent import action_node
from pipeline.QA_agent import qa_node
from pipeline.logging_agent import logging_node

RESULT_DIR = PROJECT_ROOT / "playground" / "eval_outputs"


def _freeze_credentials_for_threads() -> str:
    """스레드를 띄우기 전에 자격증명을 **메인 스레드에서 1회** 해석해 환경변수로 고정한다.

    왜 필요한가 (실측 확인):
      ~/.aws/config의 detection-runtime 프로필은 role_arn + source_profile=default 체인이다.
      스레드 13개가 각자 boto3.client()를 만들면 같은 assume-role 체인을 동시에 걷는데,
      botocore의 프로필 방문 추적이 충돌해서 3건이 이렇게 죽었다:
        "Infinite loop in credential configuration detected. Attempting to load from
         profile default which has already been visited.
         Visited profiles: ['detection-runtime', 'default']"
      여기서 임시 자격증명을 미리 받아 AWS_ACCESS_KEY_ID/SECRET/SESSION_TOKEN으로 넣고
      AWS_PROFILE을 제거하면, 각 스레드는 프로필 체인을 아예 걷지 않아 경쟁이 사라진다.
      (assume-role 임시 자격증명은 기본 1시간 유효 — 이 스크립트 실행 시간보다 길다.)
    """
    profile = os.environ.get("AWS_PROFILE")
    session = boto3.Session()
    frozen = session.get_credentials().get_frozen_credentials()
    os.environ["AWS_ACCESS_KEY_ID"] = frozen.access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = frozen.secret_key
    if frozen.token:
        os.environ["AWS_SESSION_TOKEN"] = frozen.token
    os.environ.pop("AWS_PROFILE", None)
    identity = boto3.client("sts").get_caller_identity()
    print(f"자격증명 고정 완료 (원래 프로필={profile}, Arn={identity['Arn']})")
    return identity["Arn"]


def find_source() -> Path:
    candidates = sorted(glob.glob(str(RESULT_DIR / "ec2_repeated_trial__*.json")))
    if not candidates:
        raise FileNotFoundError(f"{RESULT_DIR}에 ec2_repeated_trial__*.json 원본이 없음")
    return Path(candidates[-1])


def _build_state(resource_id: str, resource_type: str, raw_metrics: dict,
                 resource_age_seconds: float | None) -> dict:
    """measure_pipeline_timing._build_initial_state와 같은 형태지만, assemble_resource로
    AWS를 다시 부르지 않고 저장된 raw_metrics를 그대로 쓴다.

    resource_age_seconds도 저장값을 넘긴다 — measure_pipeline_timing은 이걸 None으로
    두는데, 그러면 EC2 유휴 판정의 나이가드가 아예 작동하지 않는다(detection_agent.py
    _low_utilization_check는 None이면 가드를 건너뛴다). 재생은 원본 측정과 같은
    조건을 재현해야 하므로 저장된 나이를 그대로 쓴다."""
    return {
        "trace_id": None,
        "resource_id": resource_id,
        "resource_type": resource_type,
        "raw_metrics": raw_metrics,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "resource_age_seconds": resource_age_seconds,
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


def run_one(resource_id: str, label: str, profile: str | None, raw_metrics: dict,
            resource_age_seconds: float | None) -> dict:
    """저장된 지표로 detection부터 시작해 파이프라인을 끝까지 흘린다."""
    timings: dict[str, float] = {}
    t_total = time.time()
    out: dict = {"resource_id": resource_id, "label": label, "profile": profile}

    try:
        state = _build_state(resource_id, "EC2", raw_metrics, resource_age_seconds)

        t0 = time.time()
        state = detection_node(state)
        timings["detection"] = time.time() - t0
        out["anomaly_flag"] = bool(state["anomaly_flag"])
        out["anomaly_score_zscore"] = state["anomaly_score_zscore"]
        out["anomaly_score_iforest"] = state["anomaly_score_iforest"]
        out["triggered_metrics"] = state["triggered_metrics"]
        print(f"[{label} {resource_id}] detection {timings['detection']:.3f}s "
              f"-> flag={state['anomaly_flag']} triggered={state['triggered_metrics']}")

        if not state["anomaly_flag"]:
            # 정상 판정 -> 파이프라인 종료. 액션이 실행되지 않으므로 인스턴스는 그대로 유지된다.
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
        print(f"[{label} {resource_id}] decision -> {state['selected_action']} "
              f"risk={state['risk_level']} approval={state['requires_approval']}")

        if state["requires_approval"]:
            # 승인 게이트를 우회하지 않는다 — 실제 운영 동작 그대로 기록.
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
        print(f"[{label} {resource_id}] action {timings['action']:.3f}s "
              f"-> {state.get('action_executed')} {(state.get('action_result') or {}).get('status')}")

        t0 = time.time()
        state = qa_node(state)
        timings["qa"] = time.time() - t0
        out["qa_passed"] = state.get("qa_passed")
        out["sla_check_result"] = state.get("sla_check_result")
        out["rollback_count"] = state.get("rollback_count", 0)
        out["qa_matched_rule_id"] = state.get("qa_matched_rule_id")
        print(f"[{label} {resource_id}] qa {timings['qa']:.3f}s -> passed={state.get('qa_passed')}")

        t0 = time.time()
        try:
            state = logging_node(state)
        except Exception as exc:
            print(f"[{resource_id}] logging 실패(타이밍엔 영향 없음): {exc}")
        timings["logging"] = time.time() - t0

        timings["total"] = time.time() - t_total
        out["stopped_at"] = None
        out["timings"] = {k: round(v, 3) for k, v in timings.items()}
        out["elapsed_sec"] = round(time.time() - t_total, 1)
        return out

    except Exception as exc:
        print(f"[{resource_id}] 실패: {exc}\n{traceback.format_exc()}")
        out["error"] = str(exc)
        out["timings"] = {k: round(v, 3) for k, v in timings.items()}
        out["elapsed_sec"] = round(time.time() - t_total, 1)
        return out


def _ensure_running(instance_ids: list[str]) -> dict[str, str]:
    """액션 대상이 running이어야 Stop이 의미가 있으므로, stopped인 것만 start하고 기다린다."""
    ec2 = boto3.client("ec2")
    desc = ec2.describe_instances(InstanceIds=instance_ids)
    states = {i["InstanceId"]: i["State"]["Name"]
              for r in desc["Reservations"] for i in r["Instances"]}
    to_start = [i for i, s in states.items() if s in ("stopped", "stopping")]
    if not to_start:
        print("모두 이미 running 상태")
        return states

    # stopping 중인 것은 stopped가 되기 전엔 start가 거부되므로 먼저 기다린다
    stopping = [i for i in to_start if states[i] == "stopping"]
    if stopping:
        print(f"stopping -> stopped 대기: {stopping}")
        ec2.get_waiter("instance_stopped").wait(InstanceIds=stopping)

    print(f"start 요청 {len(to_start)}대: {to_start}")
    ec2.start_instances(InstanceIds=to_start)
    print("running 대기...")
    ec2.get_waiter("instance_running").wait(InstanceIds=to_start)

    desc = ec2.describe_instances(InstanceIds=instance_ids)
    return {i["InstanceId"]: i["State"]["Name"]
            for r in desc["Reservations"] for i in r["Instances"]}


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
                        help="원본 실측 결과 JSON (생략 시 최신 ec2_repeated_trial__*.json)")
    parser.add_argument("--ensure-running", action="store_true",
                        help="대상 인스턴스가 stopped면 먼저 start하고 running까지 대기")
    parser.add_argument("--max-workers", type=int, default=13)
    args = parser.parse_args()

    caller_arn = _freeze_credentials_for_threads()

    source = Path(args.source) if args.source else find_source()
    payload_src = json.loads(source.read_text(encoding="utf-8"))
    trials = payload_src["trials"]
    print(f"원본: {source.name} (측정 {payload_src.get('generated_at')}, 시행 {len(trials)}개)")

    instance_states = {}
    if args.ensure_running:
        instance_states = _ensure_running([t["resource"] for t in trials])
        print(f"인스턴스 상태: {instance_states}")

    print(f"=== EC2 {len(trials)}개 재생 파이프라인 병렬 실행 시작 ===")
    t0 = time.time()
    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=min(args.max_workers, len(trials))) as ex:
        futs = {}
        for t in trials:
            after = t.get("after") or {}
            if not after.get("raw_metrics"):
                print(f"[{t.get('resource')}] raw_metrics 없음 — 건너뜀")
                continue
            fut = ex.submit(run_one, t["resource"], t["label"], t.get("profile"),
                            after["raw_metrics"], after.get("resource_age_seconds"))
            futs[fut] = t["resource"]
        for f in as_completed(futs):
            results.append(f.result())

    order = {t["resource"]: i for i, t in enumerate(trials)}
    results.sort(key=lambda r: order.get(r["resource_id"], 999))

    summary = _summarize(results)
    print("\n=== 집계 ===")
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = RESULT_DIR / f"batch_pipeline_replay__EC2_{ts}.json"
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    out = {
        "script_version": SCRIPT_VERSION,
        "resource_type": "EC2",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "replayed_from": source.name,
        "caller_arn": caller_arn,
        "replay_note": ("detection 입력만 저장된 실측 raw_metrics(부하 생성기가 살아있던 "
                        "2026-09-09 14:11 UTC 시점)이고, action은 실제 현재 인스턴스에 "
                        "실행되며 QA는 라이브 지표를 재조회하는 하이브리드."),
        "instance_states_before": instance_states,
        "wall_clock_sec": round(time.time() - t0, 1),
        "summary": summary,
        "results": results,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"\n결과 저장: {out_path}")


if __name__ == "__main__":
    main()
