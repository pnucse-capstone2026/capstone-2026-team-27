"""
Decision Pseudocode Promoter
-----------------------------
decision_agent.py가 남긴 LLM 액션 선택 판단 로그(schema/logs/llm_decision_log.jsonl)를
모아, LLM이 반복적으로 같은 액션을 선택하는 패턴을 찾아 Rule Book에 자동 승격한다.

사용법:
    CLI: python -m pipeline.decision_pseudocode_promoter [--min-count N] [--dry-run]
    코드: from pipeline.decision_pseudocode_promoter import auto_promote_decision_rules

승격 조건:
    1. 같은 조건(resource_type + anomaly_type)에서
    2. LLM이 N번 이상 동일한 selected_action을 선택
    3. 그 판단들이 qa_passed=True로 검증됨
    4. 액션 일관성이 80% 이상

자동 승격 (파이프라인 연동):
    - logging_agent.py에서 파이프라인 완료 시 auto_promote_decision_rules() 호출
    - 사용자 승인 없이 조건 충족 시 자동 승격
"""

import argparse
import json
import logging
import os
import re
from collections import defaultdict
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

SCRIPT_DIR = os.path.dirname(__file__)
LLM_DECISION_LOG_PATH = os.path.join(SCRIPT_DIR, "..", "schema", "logs", "llm_decision_log.jsonl")
DECISION_RULES_PATH = os.path.join(SCRIPT_DIR, "..", "schema", "rules", "decision_rules.json")

DEFAULT_MIN_COUNT = 3
# 그룹 내에서 가장 흔한 pseudo_code가 이 비율 이상을 차지하면 "안정적인 패턴"으로 본다.
CONSISTENCY_THRESHOLD = 0.8


def load_decision_logs() -> list[dict]:
    """decision_agent LLM 판단 로그 로드."""
    if not os.path.exists(LLM_DECISION_LOG_PATH):
        print(f"[decision_pseudocode_promoter] 로그 파일 없음: {LLM_DECISION_LOG_PATH}")
        return []

    logs = []
    with open(LLM_DECISION_LOG_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                logs.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return logs


def _is_verified(entry: dict) -> bool:
    """qa_result가 있고 qa_passed=True인 로그만 검증된 판단으로 취급 (rule_promoter와 동일 기준)."""
    qa_result = entry.get("qa_result")
    return bool(qa_result) and qa_result.get("qa_passed") is True


def _normalize_pseudo_code(pseudo_code: str) -> str:
    """공백/대소문자 차이를 무시하고 비교하기 위한 정규화."""
    return re.sub(r"\s+", " ", pseudo_code.strip().lower())


def find_patterns(logs: list[dict], min_count: int) -> list[dict]:
    """
    그룹핑 키: (resource_type, anomaly_type) — selected_action은 그룹 안에서
    얼마나 일치하는지를 "액션 일치율"로 따로 집계한다 (액션 자체를 그룹핑 키에
    넣으면 애초에 액션이 갈린 케이스가 보이지 않기 때문).

    조건: used_llm=True(실제 LLM 판단, fallback 아님)이고 qa_passed=True인 항목만 카운트.
    """
    groups: dict[tuple, list[dict]] = defaultdict(list)

    for entry in logs:
        output = entry.get("output", {})
        if not output.get("used_llm"):
            continue  # 룰 기반 fallback (pseudo_code 없음) - 패턴 분석 대상 아님
        if not _is_verified(entry):
            continue

        input_data = entry.get("input", {})
        resource_type = input_data.get("resource_type")
        anomaly_type = input_data.get("anomaly_type")
        selected_action = output.get("selected_action")
        pseudo_code = output.get("pseudo_code", "")

        if not (resource_type and anomaly_type and selected_action and pseudo_code):
            continue

        key = (resource_type, anomaly_type)
        groups[key].append(entry)

    patterns = []
    for (resource_type, anomaly_type), entries in groups.items():
        if len(entries) < min_count:
            continue

        # 1) 액션 일치율: 이 조건에서 LLM이 결과적으로 같은 액션을 골랐는지
        action_counts: dict[str, int] = defaultdict(int)
        for entry in entries:
            action_counts[entry["output"]["selected_action"]] += 1
        dominant_action = max(action_counts, key=action_counts.get)
        dominant_action_count = action_counts[dominant_action]
        action_consistency = dominant_action_count / len(entries)

        # 2) pseudo_code 일관성: 같은 액션을 고른 것들 중에서, 그 판단 로직(문구)까지
        # 얼마나 똑같이 표현했는지 (변수명/조건식이 매번 즉흥적으로 달라질 수 있음)
        dominant_entries = [e for e in entries if e["output"]["selected_action"] == dominant_action]
        pseudo_code_counts: dict[str, int] = defaultdict(int)
        pseudo_code_samples: dict[str, str] = {}
        for entry in dominant_entries:
            pc = entry["output"]["pseudo_code"]
            key_pc = _normalize_pseudo_code(pc)
            pseudo_code_counts[key_pc] += 1
            pseudo_code_samples.setdefault(key_pc, pc)

        dominant_pc_key = max(pseudo_code_counts, key=pseudo_code_counts.get)
        dominant_pc_count = pseudo_code_counts[dominant_pc_key]
        pseudo_code_consistency = dominant_pc_count / len(dominant_entries)

        patterns.append({
            "resource_type": resource_type,
            "anomaly_type": anomaly_type,
            "count": len(entries),
            "action_counts": dict(action_counts),
            "dominant_action": dominant_action,
            "dominant_action_count": dominant_action_count,
            "action_consistency": action_consistency,
            "distinct_pseudo_code": len(pseudo_code_counts),
            "dominant_pseudo_code": pseudo_code_samples[dominant_pc_key],
            "dominant_pseudo_code_count": dominant_pc_count,
            "pseudo_code_consistency": pseudo_code_consistency,
            "action_is_stable": action_consistency >= CONSISTENCY_THRESHOLD,
            "pseudo_code_is_stable": pseudo_code_consistency >= CONSISTENCY_THRESHOLD,
        })

    patterns.sort(key=lambda p: p["count"], reverse=True)
    return patterns


def display_pattern(pattern: dict, index: int) -> None:
    print(f"\n{'='*60}")
    print(f"패턴 #{index + 1}")
    print(f"{'='*60}")
    print(f"  리소스 타입      : {pattern['resource_type']}")
    print(f"  이상 유형        : {pattern['anomaly_type']}")
    print(f"  검증된 판단 횟수 : {pattern['count']}회")

    action_breakdown = ", ".join(f"{a}={c}" for a, c in pattern["action_counts"].items())
    print(f"  액션 분포        : {action_breakdown}")
    print(f"  액션 일치율      : {pattern['action_consistency']*100:.1f}% "
          f"({pattern['dominant_action_count']}/{pattern['count']}건이 '{pattern['dominant_action']}')")

    print(f"  pseudo_code 일관성 : {pattern['pseudo_code_consistency']*100:.1f}% "
          f"({pattern['dominant_pseudo_code_count']}/{pattern['dominant_action_count']}건이 동일 로직, "
          f"'{pattern['dominant_action']}' 선택 건 중 서로 다른 pseudo_code {pattern['distinct_pseudo_code']}종)")
    print(f"  대표 pseudo_code : {pattern['dominant_pseudo_code']}")

    if pattern["action_is_stable"] and pattern["pseudo_code_is_stable"]:
        print(f"  → 완전 안정적. 액션도 판단 로직도 일관됨 - 규칙 엔진으로 그대로 "
              f"승격 가능한 후보입니다 (잠재적 절감: {pattern['dominant_action_count']}회).")
    elif pattern["action_is_stable"]:
        print(f"  → 액션은 일관되지만({pattern['action_consistency']*100:.0f}%) pseudo_code 표현이 "
              f"매번 달라집니다. '어떤 액션을 고를지'는 규칙화 가능하나, "
              f"'왜/어떤 조건으로'는 아직 사람이 임계값을 정리해줘야 합니다.")
    else:
        print(f"  → 액션 자체가 조건마다 갈립니다({action_breakdown}). "
              f"추가 데이터로 원인(메트릭 차이)을 더 봐야 합니다.")


# ── Decision 규칙 로드/저장 ─────────────────────────────────────────────────────


def load_existing_decision_rules() -> list[dict]:
    """기존 Decision 규칙 로드."""
    if not os.path.exists(DECISION_RULES_PATH):
        return []
    with open(DECISION_RULES_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_decision_rules(rules: list[dict]) -> None:
    """Decision 규칙 저장."""
    with open(DECISION_RULES_PATH, "w", encoding="utf-8") as f:
        json.dump(rules, f, ensure_ascii=False, indent=2)


def get_next_decision_rule_id(existing_rules: list[dict]) -> str:
    """다음 Decision 규칙 ID 생성 (DEC-005, DEC-006, ...)."""
    max_num = 0
    for rule in existing_rules:
        rule_id = rule.get("rule_id", "")
        if rule_id.startswith("DEC-"):
            try:
                num = int(rule_id.replace("DEC-", ""))
                max_num = max(max_num, num)
            except ValueError:
                continue
    return f"DEC-{max_num + 1:03d}"


def is_decision_rule_covered(pattern: dict, existing_rules: list[dict]) -> bool:
    """이미 기존 규칙으로 커버되는지 확인."""
    resource_type = pattern["resource_type"]
    anomaly_type = pattern["anomaly_type"]

    for rule in existing_rules:
        rule_types = rule.get("resource_types", [])
        conditions = rule.get("conditions", {})

        # 리소스 타입 매칭
        if "*" not in rule_types and resource_type not in rule_types:
            continue

        # anomaly_type 매칭
        rule_anomaly_type = conditions.get("anomaly_type")
        if rule_anomaly_type and rule_anomaly_type == anomaly_type:
            return True

    return False


def create_decision_rule_from_pattern(pattern: dict, rule_id: str) -> dict:
    """패턴에서 Decision Rule 생성."""
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    now_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    return {
        "rule_id": rule_id,
        "rule_type": "decision",
        "description": f"{pattern['resource_type']} {pattern['anomaly_type']} -> {pattern['dominant_action']}",
        "resource_types": [pattern["resource_type"]],
        "conditions": {
            "anomaly_type": pattern["anomaly_type"]
        },
        "result": {
            "selected_action": pattern["dominant_action"],
            "reasoning_template": f"{pattern['resource_type']} {pattern['anomaly_type']} 탐지 -> {pattern['dominant_action']} 액션 선택 (자동 승격 규칙)"
        },
        "priority": 100,  # 자동 승격 규칙은 낮은 우선순위
        "enabled": True,
        "created_at": now_iso,
        "updated_at": now_iso,
        "author": "auto-promoted",
        "rationale": f"{pattern['dominant_action_count']}건의 LLM 판단 로그 기반 자동 승격 (액션 일관성 {pattern['action_consistency']*100:.0f}%), 승격일 {today}"
    }


# ── 자동 승격 함수 (파이프라인 연동용) ─────────────────────────────────────────


def auto_promote_decision_rules(min_count: int = DEFAULT_MIN_COUNT) -> list[dict]:
    """
    조건을 충족하는 패턴을 사용자 승인 없이 자동으로 Decision Rule Book에 승격.

    파이프라인(logging_agent.py)에서 호출되어 실시간으로 규칙 자동 생성.

    승격 조건:
        - min_count 이상 반복
        - 액션 일관성 >= CONSISTENCY_THRESHOLD (80%)

    Args:
        min_count: 최소 반복 횟수 (기본값: DEFAULT_MIN_COUNT)

    Returns:
        승격된 규칙 목록 [{"rule_id": "DEC-XXX", "description": "...", ...}, ...]
    """
    promoted_rules = []

    try:
        # 로그 로드
        logs = load_decision_logs()
        if not logs:
            return []

        # 기존 규칙 로드
        existing_rules = load_existing_decision_rules()

        # 승격 후보 찾기 (액션이 안정적인 패턴만)
        patterns = find_patterns(logs, min_count)
        stable_patterns = [p for p in patterns if p["action_is_stable"]]

        if not stable_patterns:
            return []

        # 새로운 후보만 필터링
        new_patterns = [
            p for p in stable_patterns
            if not is_decision_rule_covered(p, existing_rules)
        ]

        if not new_patterns:
            return []

        # 자동 승격 (사용자 승인 없이)
        for pattern in new_patterns:
            rule_id = get_next_decision_rule_id(existing_rules)
            new_rule = create_decision_rule_from_pattern(pattern, rule_id)
            existing_rules.append(new_rule)
            promoted_rules.append(new_rule)

            logger.info(
                "[auto_promote_decision] 규칙 자동 승격: %s (%s + %s -> %s, %d건 검증됨, 일관성 %.0f%%)",
                rule_id,
                pattern["resource_type"],
                pattern["anomaly_type"],
                pattern["dominant_action"],
                pattern["dominant_action_count"],
                pattern["action_consistency"] * 100,
            )

        # 규칙 저장
        if promoted_rules:
            save_decision_rules(existing_rules)
            logger.info("[auto_promote_decision] %d개 규칙 저장 완료", len(promoted_rules))

            # RuleEngine 리로드
            try:
                from pipeline.rule_engine import reload_rules
                reload_rules()
            except Exception:
                pass

        return promoted_rules

    except Exception as e:
        logger.error("[auto_promote_decision] 자동 승격 실패: %s", e)
        return []


def check_decision_promotion_candidates(min_count: int = DEFAULT_MIN_COUNT) -> list[dict]:
    """
    승격 가능한 후보가 있는지 확인만 (실제 승격은 안 함).

    Returns:
        승격 가능한 후보 목록
    """
    try:
        logs = load_decision_logs()
        if not logs:
            return []

        existing_rules = load_existing_decision_rules()
        patterns = find_patterns(logs, min_count)
        stable_patterns = [p for p in patterns if p["action_is_stable"]]

        return [
            {
                "resource_type": p["resource_type"],
                "anomaly_type": p["anomaly_type"],
                "dominant_action": p["dominant_action"],
                "count": p["dominant_action_count"],
                "action_consistency": p["action_consistency"],
            }
            for p in stable_patterns
            if not is_decision_rule_covered(p, existing_rules)
        ]
    except Exception:
        return []


def main():
    parser = argparse.ArgumentParser(description="Decision Agent LLM pseudo_code 판단 패턴 분석")
    parser.add_argument("--min-count", type=int, default=DEFAULT_MIN_COUNT,
                        help=f"패턴으로 볼 최소 반복 횟수 (기본값: {DEFAULT_MIN_COUNT})")
    args = parser.parse_args()

    print(f"\n[decision_pseudocode_promoter] 로그 파일: {LLM_DECISION_LOG_PATH}")

    logs = load_decision_logs()
    if not logs:
        print("\n[decision_pseudocode_promoter] 분석할 로그가 없습니다.")
        return

    total_llm_calls = sum(1 for e in logs if e.get("output", {}).get("used_llm"))
    verified_llm_calls = sum(1 for e in logs if e.get("output", {}).get("used_llm") and _is_verified(e))
    print(f"[decision_pseudocode_promoter] 총 {len(logs)}건 로드 "
          f"(LLM 실제 판단 {total_llm_calls}건, 그중 QA 검증됨 {verified_llm_calls}건)")

    patterns = find_patterns(logs, args.min_count)
    if not patterns:
        print(f"\n[decision_pseudocode_promoter] {args.min_count}회 이상 반복된 패턴이 없습니다.")
        return

    print(f"\n[decision_pseudocode_promoter] {len(patterns)}개 패턴 발견")
    for i, pattern in enumerate(patterns):
        display_pattern(pattern, i)

    action_stable = [p for p in patterns if p["action_is_stable"]]
    fully_stable = [p for p in patterns if p["action_is_stable"] and p["pseudo_code_is_stable"]]
    action_skippable_calls = sum(p["dominant_action_count"] for p in action_stable)
    fully_skippable_calls = sum(p["dominant_action_count"] for p in fully_stable)

    print(f"\n{'='*60}")
    print(f"[decision_pseudocode_promoter] 요약")
    print(f"  액션까지만 안정적인 패턴  : {len(action_stable)}개 "
          f"(액션 기준 규칙화 시 LLM 호출 없이 처리 가능했을 판단 {action_skippable_calls}건)")
    print(f"  액션+로직까지 안정적인 패턴: {len(fully_stable)}개 "
          f"(그대로 규칙 승격 가능했을 판단 {fully_skippable_calls}건)")
    print(f"  검증된 LLM 판단 총 {verified_llm_calls}건 중 액션 기준으로는 "
          f"{action_skippable_calls}건({action_skippable_calls/verified_llm_calls*100:.1f}%)이 절감 후보")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
