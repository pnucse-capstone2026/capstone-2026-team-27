"""
playground/test_scenarios.py

3가지 더미 시나리오로 전체 파이프라인(Detection → Classification → Decision →
Action → QA → Logging)을 순서대로 실행하고, 각 단계 결과를 보기 좋게 출력한다.

시나리오:
  1. 좀비 리소스 (EC2, cost_inefficiency)  → risk_level=LOW  → 실제 boto3 Stop 실행됨
  2. Lambda 호출 폭증 (cost_spike)         → risk_level=MED  → pending_approval
  3. EDoS 의심 (AutoScaling, risk_security) → risk_level=HIGH → pending_approval

[실행 방법]
  프로젝트 루트에서: python playground/test_scenarios.py
  (playground/ 안에서 바로 실행해도 sys.path 보정 코드가 처리해준다)

[주의 - 시나리오 1]
  cpu_utilization / network_in / cost를 전부 상수(flat)로만 주면 Z-score의 표준편차 σ가
  0이 되어 Z 값이 항상 0으로 계산되고, Isolation Forest도 분산이 없는 데이터에서는
  이상치를 못 잡아서 anomaly_flag=False로 끝나버린다 (Detection Agent는 "변화"를 탐지하는
  알고리즘이라 "계속 낮은 값 그 자체"는 못 잡음).
  그래서 "좀비 리소스"라는 시나리오 취지(CPU/네트워크가 계속 낮음)는 그대로 유지하면서,
  cost 지표 마지막 구간에 아주 작은 스파이크를 하나 넣어서 Detection이 트리거되게 했다.
  (실제 운영에서는 이런 "계속 낮음" 패턴은 Z-score/IForest가 아니라 별도의 저사용 규칙
  탐지기로 잡는 게 맞다 — 지금 파이프라인 알고리즘 특성상 필요한 최소한의 보정이다.)
"""

from __future__ import annotations

import os
import sys
import time
import copy
from pathlib import Path

# ── sys.path 설정: playground/ 안에서 실행해도 프로젝트 루트를 찾을 수 있도록 ──
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

from schema.state import PipelineState, ALLOWED_ACTIONS
from pipeline.detection_agent import detection_node
from pipeline.classification_agent import classification_node
from pipeline.decision_agent import decision_node
from pipeline.action_agent import action_node
from pipeline.QA_agent import qa_node


# ── 공통 헬퍼 ─────────────────────────────────────────────────────────────────

def _base_state(resource_id: str, resource_type: str, raw_metrics: dict) -> PipelineState:
    """node_contracts.md 명시된 필수 초기값을 채운 빈 PipelineState 생성."""
    return {
        "resource_id":   resource_id,
        "resource_type": resource_type,
        "raw_metrics":   raw_metrics,
        "timestamp":     "2026-07-14T10:00:00Z",

        "anomaly_flag":          False,
        "anomaly_score_zscore":  None,
        "anomaly_score_iforest": None,
        "triggered_metrics":     [],

        "anomaly_type":             None,
        "classification_reasoning": None,
        "interim_action_taken":     None,

        "candidate_actions":  [],
        "selected_action":    None,
        "risk_level":         None,
        "requires_approval":  False,
        "decision_reasoning": None,
        "target_instance_type": None,

        "pre_action_snapshot": None,
        "action_executed":     None,
        "action_result":       None,

        "qa_passed":        None,
        "sla_check_result": None,
        "rollback_count":   0,

        "log_entries": [],
    }


def _metrics_summary_text(raw_metrics: dict) -> str:
    """입력 메트릭을 (첫값 → 마지막값, 평균) 형태로 한 줄씩 요약."""
    lines = []
    for key, values in raw_metrics.items():
        if not isinstance(values, list) or not values:
            continue
        avg = sum(values) / len(values)
        lines.append(
            f"    - {key}: 시작={values[0]:.2f}, 끝={values[-1]:.2f}, "
            f"평균={avg:.2f}, n={len(values)}"
        )
    return "\n".join(lines)


def _print_section(title: str) -> None:
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def _safe_assert(condition: bool, message: str, failures: list[str]) -> None:
    """assert 실패해도 전체 실행이 멈추지 않도록, 실패 내용을 모아서 나중에 출력."""
    if condition:
        print(f"  [PASS] {message}")
    else:
        print(f"  [FAIL] {message}")
        failures.append(message)


# ── 시나리오 실행 공통 로직 ───────────────────────────────────────────────────

def run_scenario(
    name: str,
    description: str,
    resource_id: str,
    resource_type: str,
    raw_metrics: dict,
    expected_anomaly_type: str,
) -> dict:
    """
    한 시나리오를 Detection → Classification → Decision → Action → QA까지 실행하고
    결과를 출력한다. (Logging Agent는 test_scenarios.py 자체 목적이 파이프라인
    동작 확인이므로, DB 적재 확인은 별도로 pipeline/graph.py의 app.invoke()를
    통해서도 확인 가능 — 여기서는 각 노드를 직접 호출해 단계별 결과를 세밀히 본다)
    """
    _print_section(f"시나리오: {name}")
    print(description)
    print("\n[입력 메트릭 요약]")
    print(f"  resource_type={resource_type}, resource_id={resource_id}")
    print(_metrics_summary_text(raw_metrics))

    state = _base_state(resource_id, resource_type, raw_metrics)
    failures: list[str] = []

    start_time = time.time()

    # 1) Detection
    state = detection_node(state)
    print("\n[Detection 결과]")
    print(f"  anomaly_flag        = {state['anomaly_flag']}")
    print(f"  anomaly_score_zscore  = {state['anomaly_score_zscore']}")
    print(f"  anomaly_score_iforest = {state['anomaly_score_iforest']}")
    print(f"  triggered_metrics   = {state['triggered_metrics']}")

    if not state["anomaly_flag"]:
        # anomaly_flag=False면 node_contracts.md 규칙상 classification 이후 단계로
        # 가지 않고 바로 Logging으로 빠진다. 시나리오 실험 목적상 여기서 중단하고
        # 실패 사유를 남긴 뒤 다음 시나리오로 넘어간다.
        _safe_assert(False, "anomaly_flag == True", failures)
        elapsed = time.time() - start_time
        print(f"\n[총 실행 시간] {elapsed:.3f}초 (Detection에서 조기 종료)")
        _print_failures(failures)
        return {"state": state, "elapsed": elapsed, "failures": failures}

    _safe_assert(state["anomaly_flag"] is True, "anomaly_flag == True", failures)

    # 2) Classification
    state = classification_node(state)
    print("\n[Classification 결과]")
    print(f"  anomaly_type = {state['anomaly_type']}")
    print(f"  reasoning    = {state['classification_reasoning']}")
    print(f"  interim_action_taken = {state['interim_action_taken']}")

    _safe_assert(
        state["anomaly_type"] == expected_anomaly_type,
        f"anomaly_type == {expected_anomaly_type!r} (실제: {state['anomaly_type']!r})",
        failures,
    )

    # 3) Decision
    state = decision_node(state)
    print("\n[Decision 결과]")
    print(f"  selected_action   = {state['selected_action']}")
    print(f"  risk_level        = {state['risk_level']}")
    print(f"  requires_approval = {state['requires_approval']}")
    print(f"  decision_reasoning = {state['decision_reasoning']}")
    print("  후보 액션 목록:")
    for c in state["candidate_actions"]:
        print(
            f"    - {c['action']:<14} score={c['score']:.3f} "
            f"(saving={c['saving_rate']:.2f}, impact={c['impact_score']:.2f}, "
            f"stability={c['stability_score']:.2f})"
        )

    allowed = ALLOWED_ACTIONS.get(state["anomaly_type"], ["NoAction"])
    _safe_assert(
        state["selected_action"] in allowed,
        f"selected_action({state['selected_action']!r})이 허용 액션 집합 {allowed} 안에 있는지",
        failures,
    )

    # 4) Action
    state = action_node(state)
    print("\n[Action 결과]")
    print(f"  action_executed = {state['action_executed']}")
    print(f"  action_result   = {state['action_result']}")

    # 5) QA
    state = qa_node(state)
    print("\n[QA 결과]")
    print(f"  qa_passed        = {state['qa_passed']}")
    print(f"  rollback_count   = {state['rollback_count']}")
    print(f"  sla_check_result = {state['sla_check_result']}")

    _safe_assert(state["qa_passed"] is True, "qa_passed == True", failures)

    elapsed = time.time() - start_time
    print(f"\n[총 실행 시간] {elapsed:.3f}초")

    _print_failures(failures)

    return {"state": state, "elapsed": elapsed, "failures": failures}


def _print_failures(failures: list[str]) -> None:
    if failures:
        print(f"\n  >>> 이 시나리오에서 {len(failures)}건의 검증 실패가 있었습니다:")
        for f in failures:
            print(f"      - {f}")
    else:
        print("\n  >>> 모든 검증 통과")


# ── 시나리오 1: 좀비 리소스 (EC2, cost_inefficiency) ─────────────────────────

def scenario_1_zombie_ec2() -> dict:
    resource_id = os.getenv("INSTANCE_ID")
    if not resource_id:
        print("\n[경고] .env에 INSTANCE_ID가 없습니다. 더미 ID로 대체합니다 "
              "(실제 boto3 Stop 호출은 실패할 수 있음).")
        resource_id = "i-DUMMY_INSTANCE_ID"

    raw_metrics = {
        # CPU가 계속 낮음 (AWS Trusted Advisor 기준 10% 이하 → 낭비)
        "cpu_utilization": [3.0] * 30,
        # 네트워크 입력도 계속 낮음 (5MB 이하 → 낭비)
        "network_in":      [100.0] * 30,
        "network_out":     [80.0] * 30,
        # cost는 마지막 3개 포인트에만 작은 스파이크를 줘서 Z-score가 트리거되게 함
        # (CPU/network가 flat이라는 "좀비 리소스" 특징은 그대로 유지)
        "cost":            [0.5] * 27 + [3.0, 3.2, 3.5],
    }

    return run_scenario(
        name="1. 좀비 리소스 (cost_inefficiency)",
        description=(
            "EC2 CPU/네트워크 사용률이 계속 낮게 유지되는 좀비 리소스 패턴.\n"
            "기대 결과: anomaly_type=cost_inefficiency, selected_action=Stop, "
            "risk_level=LOW, requires_approval=False\n"
            "risk_level=LOW이므로 이 시나리오만 실제 boto3 EC2 Stop이 실행된다."
        ),
        resource_id=resource_id,
        resource_type="EC2",
        raw_metrics=raw_metrics,
        expected_anomaly_type="cost_inefficiency",
    )


# ── 시나리오 2: Lambda 호출 폭증 (cost_spike) ────────────────────────────────

def scenario_2_lambda_spike() -> dict:
    resource_id = os.getenv("LAMBDA_FUNCTION_NAME", "detection-test-lambda")

    raw_metrics = {
        # 호출 횟수가 마지막 5개 포인트에서 급증
        "invocation_count": [100.0] * 25 + [5000.0] * 5,
        "error_count":      [1.0] * 30,
        "duration_avg":     [200.0] * 30,
        "cost":             [0.1] * 25 + [2.0] * 5,
    }

    return run_scenario(
        name="2. Lambda 호출 폭증 (cost_spike)",
        description=(
            "Lambda invocation_count가 갑자기 급증하는 패턴 (더미 함수명, 실제 호출 안 함).\n"
            "기대 결과: anomaly_type=cost_spike, selected_action=Throttle, risk_level=MED\n"
            "requires_approval=True → action_result는 pending_approval로 반환된다 "
            "(코드가 구현되어 있음을 보여주는 목적)."
        ),
        resource_id=resource_id,
        resource_type="Lambda",
        raw_metrics=raw_metrics,
        expected_anomaly_type="cost_spike",
    )


# ── 시나리오 3: EDoS 의심 (AutoScaling, risk_security) ───────────────────────

def scenario_3_edos_suspicion() -> dict:
    resource_id = os.getenv("ASG_NAME", "detection-test-asg")

    raw_metrics = {
        # 실행 중 인스턴스 수가 마지막 5개 포인트에서 급증
        "group_in_service_instances": [2.0] * 25 + [20.0] * 5,
        "group_desired_capacity":     [2.0] * 25 + [20.0] * 5,
        "cost":                       [0.5] * 25 + [5.0] * 5,
    }

    return run_scenario(
        name="3. EDoS 의심 (risk_security)",
        description=(
            "AutoScaling group_in_service_instances가 갑자기 급증하는 패턴 "
            "(더미 그룹명, 실제 호출 안 함).\n"
            "기대 결과: anomaly_type=risk_security, selected_action=ScaleDown, risk_level=HIGH\n"
            "requires_approval=True → action_result는 pending_approval로 반환된다."
        ),
        resource_id=resource_id,
        resource_type="AutoScaling",
        raw_metrics=raw_metrics,
        expected_anomaly_type="risk_security",
    )


# ── 메인 ──────────────────────────────────────────────────────────────────────

def main() -> None:
    _print_section("시나리오 실험 시작 (3개 시나리오 순서대로 실행)")

    results = []
    results.append(scenario_1_zombie_ec2())
    results.append(scenario_2_lambda_spike())
    results.append(scenario_3_edos_suspicion())

    _print_section("전체 요약")
    total_elapsed = sum(r["elapsed"] for r in results)
    total_failures = sum(len(r["failures"]) for r in results)
    for i, r in enumerate(results, start=1):
        state = r["state"]
        status = "실패 있음" if r["failures"] else "전부 통과"
        print(
            f"  시나리오 {i}: {status} | 실행시간={r['elapsed']:.3f}초 | "
            f"anomaly_type={state.get('anomaly_type')} | "
            f"selected_action={state.get('selected_action')} | "
            f"risk_level={state.get('risk_level')}"
        )
    print(f"\n  총 실행 시간: {total_elapsed:.3f}초")
    print(f"  총 검증 실패 건수: {total_failures}건")


if __name__ == "__main__":
    main()
