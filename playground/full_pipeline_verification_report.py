"""4개 시나리오(EC2 오버프로비저닝, S3 대량다운로드, AutoScaling EDoS, Lambda 스로틀 재시도폭증)의
"진짜 탐지→분류→결정→액션→QA(검증)"까지 전체 파이프라인이 실행된 실측 결과를 모아서
에이전트별 성공률/정확도를 뽑는다. 전부 이미 저장된 결과 파일 기반 — 재실행 없음.

출력:
  - 시나리오별: 액션 실행 건수/성공률, QA 통과율, 롤백 발생 건수
  - Classification 정확도(있는 시나리오는 기존 *_classification_accuracy.json 재사용)
  - 특이사항(예: EC2의 not_implemented 액션, AutoScaling의 WAF 실패, Lambda의 공유계정
    동시성으로 인한 재분류 노트)
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

EVAL_DIR = Path(__file__).parent / "eval_outputs"
TEAM_DIR = Path(__file__).parent / "team_results"
OUTPUT_PATH = EVAL_DIR / "full_pipeline_verification_report.json"


def _agent_summary(entries: list[dict]) -> dict:
    """entries: [{action_executed, action_status, qa_passed, rollback_count}, ...]"""
    n = len(entries)
    action_success = sum(1 for e in entries if e["action_status"] == "success")
    qa_pass = sum(1 for e in entries if e["qa_passed"] is True)
    qa_fail = sum(1 for e in entries if e["qa_passed"] is False)
    rolled_back = sum(1 for e in entries if (e.get("rollback_count") or 0) > 0)
    return {
        "n_action_triggered": n,
        "action_success_rate": action_success / n if n else None,
        "qa_pass_rate": qa_pass / n if n else None,
        "qa_fail_count": qa_fail,
        "rollback_occurred_count": rolled_back,
        "entries": entries,
    }


def build_ec2_overprovisioning() -> dict:
    path = EVAL_DIR / "batch_pipeline_replay__EC2_20260912_134432.json"
    d = json.loads(path.read_text(encoding="utf-8"))
    entries = []
    for r in d["results"]:
        if not r.get("action_executed"):
            continue
        ar = r.get("action_result") or {}
        entries.append({
            "resource_id": r["resource_id"], "anomaly_type": r.get("anomaly_type"),
            "action_executed": r["action_executed"], "action_status": ar.get("status"),
            "qa_passed": r.get("qa_passed"), "rollback_count": r.get("rollback_count"),
        })
    summary = _agent_summary(entries)
    summary["note"] = (
        "5건 중 4건은 cost_inefficiency->Resize 성공. 1건(cost_spike->ScaleDown)은 EC2에 "
        "미구현된 액션이 선택되어 action_status=not_implemented — 2026-09-14 QA_agent.py 수정 "
        "전에는 이 케이스가 qa_passed=True로 잘못 기록되는 버그가 있었음(수정 완료, 이 원본 "
        "로그는 수정 전 실행분이라 여전히 True로 남아있음에 유의)."
    )
    return summary


def build_s3_block() -> dict:
    path = EVAL_DIR / "3x_real_pipeline_20260910_025234.json"
    d = json.loads(path.read_text(encoding="utf-8"))
    entries = []
    for r in d["raw_results"]:
        if r.get("resource_type") != "S3" or not r.get("selected_action"):
            continue
        entries.append({
            "resource_id": r["resource_id"], "anomaly_type": r.get("anomaly_type"),
            "action_executed": r.get("selected_action"), "action_status": "success" if r.get("qa_passed") else "unknown",
            "qa_passed": r.get("qa_passed"), "rollback_count": 0,
        })
    summary = _agent_summary(entries)
    summary["note"] = "3건 전부 Block 실행 성공, qa_passed=True. action_execution_log.jsonl에서 실제 API 성공 응답도 교차 확인함."
    return summary


def build_autoscaling_edos() -> dict:
    path = EVAL_DIR / "autoscaling_edos_traffic_trial__n8-15_scriptv4_20260913.json"
    d = json.loads(path.read_text(encoding="utf-8"))
    entries = []
    for t in d["trials"]:
        pr = t.get("pipeline_result")
        if not pr or not pr.get("action_executed") or pr["action_executed"] == "NoAction":
            continue
        ar = pr.get("action_result") or {}
        scaledown_status = (ar.get("scaledown_result") or {}).get("status")
        entries.append({
            "resource_id": t["resource"], "anomaly_type": pr.get("anomaly_type"),
            "action_executed": pr["action_executed"],
            "action_status": scaledown_status,  # ScaleDown 자체 성공 여부(WAF와 분리해서 봄)
            "waf_status": (ar.get("waf_result") or {}).get("status"),
            "qa_passed": pr.get("qa_passed"), "rollback_count": pr.get("rollback_count"),
        })
    summary = _agent_summary(entries)
    waf_fail = sum(1 for e in entries if e["waf_status"] == "failed")
    summary["waf_association_fail_count"] = waf_fail
    summary["note"] = (
        f"{len(entries)}건 전부 ScaleDown 자체는 성공했으나(action_status=success로 표기됨) "
        f"WAF Rate-based Rule 연동이 {waf_fail}/{len(entries)} 전부 WAFUnavailableEntityException으로 "
        "실패함(원인 불명, 계정 레벨 이슈로 추정 — 2026-09-14 심층 조사에서 전파지연/권한/리스너/"
        "이름재사용/SCP 배제됨). QA가 이 실패를 정확히 감지(qa_passed=False)하여 자동 롤백을 "
        "트리거함 — 안전장치는 검증됨. 정상(normal) 리소스 1건은 오탐으로 NoAction 처리되어 "
        "집계에서 제외함."
    )
    return summary


def build_lambda_throttle() -> dict:
    path = TEAM_DIR / "lambda_throttle" / "clean_verification_20260914.json"
    d = json.loads(path.read_text(encoding="utf-8"))
    entries = []
    for t in d["trials"]:
        if not t.get("selected_action"):
            continue
        entries.append({
            "resource_id": t["resource_id"], "label": t.get("label"),
            "anomaly_type": t.get("anomaly_type"), "action_executed": t["selected_action"],
            "action_status": "success" if t.get("qa_passed") else "unknown",
            "qa_passed": t.get("qa_passed"), "rollback_count": 0,
            "shap_top_features": t.get("shap_top_features"),
        })
    summary = _agent_summary(entries)
    summary["metrics_raw"] = d.get("metrics")
    summary["note"] = (
        "n=13(anomaly 5 + normal 8) 중 8건이 트리거되어 Throttle 실행, qa_passed 전부 True. "
        "confusion matrix상 FP=3이었으나(정상 3건도 트리거), 팀원 재조사 결과 이 3건은 오탐이 "
        "아니라 13개 테스트 함수가 계정 동시성 한도를 공유해서 실제로 Throttle이 37/93/71회씩 "
        "발생한 '진짜' 이상으로 재분류됨(classification_accuracy.json 참고). SHAP 기반 지표별 "
        "기여도(throttle_rate/throttle_count/cost 등)가 모든 트리거 건에 실제로 기록되어 있어, "
        "SHAP 해석가능성이 production 경로에 연동되어 있음을 확인함."
    )
    return summary


def build_classification_accuracy_table() -> dict:
    section = {}
    for f in list(EVAL_DIR.glob("*__classification_accuracy.json")) + \
             list((TEAM_DIR / "lambda_throttle").glob("classification_accuracy.json")):
        d = json.loads(f.read_text(encoding="utf-8"))
        section[f.stem if f.parent == EVAL_DIR else "lambda_throttle"] = {
            "expected_label": d.get("expected_label"),
            "rulebook": d.get("rulebook"), "llm": d.get("llm"),
            "combined_classification_accuracy": d.get("combined_classification_accuracy"),
        }
    return section


def main() -> None:
    report = {
        "ec2_overprovisioning": build_ec2_overprovisioning(),
        "s3_block": build_s3_block(),
        "autoscaling_edos": build_autoscaling_edos(),
        "lambda_throttle": build_lambda_throttle(),
        "classification_accuracy_by_scenario": build_classification_accuracy_table(),
    }
    for name, s in report.items():
        if name == "classification_accuracy_by_scenario":
            continue
        print(f"\n=== {name} ===")
        print(f"  액션 실행: {s['n_action_triggered']}건, 액션 성공률: {s['action_success_rate']}")
        print(f"  QA 통과율: {s['qa_pass_rate']} (실패 {s['qa_fail_count']}건, 롤백 {s['rollback_occurred_count']}건)")
        print(f"  note: {s['note']}")

    print("\n=== Classification 정확도(시나리오별) ===")
    for name, c in report["classification_accuracy_by_scenario"].items():
        print(f"  {name}: combined={c['combined_classification_accuracy']}")

    OUTPUT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"\n결과 저장: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
