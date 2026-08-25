"""
QA Rule Promoter
----------------
QA_agent.py가 Rule Book 매칭 실패로 LLM(Gemini)에 위임한 SLA 판단들을 분석해서,
반복적으로 동일하게 나온 (resource_type, action_executed) → qa_passed 패턴을
QA Rule Book(schema/rules/qa_rules.json)에 자동 승격하는 스크립트.

pipeline/rule_promoter.py(classification용)와 철학/구조는 동일하다 — 다만 아래
한 가지 차이는 승격 후보를 승인할 때 반드시 감안해야 한다:

⚠️ classification과의 결정적 차이 (검증 단계 부재)
    classification의 승격 조건은 두 겹이다: "LLM이 N번 반복 판단" + "그 판단이
    이후 QA 통과로 검증됨". 여기서 QA 통과 여부가 일종의 사후 검증 역할을 한다.
    그런데 QA 자신의 판단에는 그런 후속 검증 단계가 파이프라인에 없다 (QA가
    Logging 이전 마지막 자동 검증 지점이다). 그래서 이 스크립트의 승격 조건은
    "같은 조건에서 N번 반복해서 같은 verdict"라는 한 겹뿐이다 — classification보다
    안전장치가 하나 적으므로, 승격 후보를 y/n으로 승인할 때 사람이 조금 더
    신중하게 봐야 한다 (반복 횟수만으로 규칙화하기엔 근거가 약할 수 있음).

승격 조건:
    1. 같은 조건(resource_type + action_executed 조합)에서
    2. LLM이 N번 이상 동일한 qa_passed(True/False)로 판단
       (qa_result.qa_matched_rule_id가 이미 있는 엔트리는 "QA Rule Book으로 처리된
        것"이라 LLM 판단이 아니므로 제외한다)

승격 시:
    - rule_id: QA-0XX 형식으로 자동 채번
    - author: "auto-promoted"
    - result: qa_passed 다수결에 따라 force_pass 또는 force_fail
    - rationale: "N건의 LLM SLA 판단 로그 기반 자동 승격, 승격일 YYYY-MM-DD"

사용법:
    python -m pipeline.qa_rule_promoter [--min-count N] [--dry-run]
"""

import argparse
import json
import os
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional

from pipeline.rule_promoter import load_llm_logs

# 설정
MIN_PROMOTION_COUNT = 3  # classification과 동일한 기본값 (필요시 조정 가능)

# 파일 경로
SCRIPT_DIR = os.path.dirname(__file__)
QA_RULES_PATH = os.path.join(SCRIPT_DIR, "..", "schema", "rules", "qa_rules.json")


def load_existing_qa_rules() -> list[dict]:
    """기존 QA 규칙 로드."""
    if not os.path.exists(QA_RULES_PATH):
        return []
    with open(QA_RULES_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_qa_rules(rules: list[dict]) -> None:
    """QA 규칙 저장."""
    with open(QA_RULES_PATH, "w", encoding="utf-8") as f:
        json.dump(rules, f, ensure_ascii=False, indent=2)


def get_next_qa_rule_id(existing_rules: list[dict]) -> str:
    """다음 규칙 ID 생성 (QA-006, QA-007, ...)."""
    max_num = 0
    for rule in existing_rules:
        rule_id = rule.get("rule_id", "")
        if rule_id.startswith("QA-"):
            try:
                num = int(rule_id.replace("QA-", ""))
                max_num = max(max_num, num)
            except ValueError:
                continue
    return f"QA-{max_num + 1:03d}"


def find_qa_promotion_candidates(logs: list[dict], min_count: int) -> list[dict]:
    """
    승격 후보 찾기.

    그룹핑 키: (resource_type, action_executed, qa_passed)
    → 같은 조건에서 verdict가 갈리면(예: 2번 통과, 1번 실패) 서로 다른 그룹으로
      나뉘어 각각 min_count 미달로 걸러지므로, 별도의 "불일치 감지" 로직 없이도
      일관되게 반복된 패턴만 후보로 올라온다.
    """
    groups: dict[tuple, list[dict]] = defaultdict(list)

    for entry in logs:
        qa_result = entry.get("qa_result")
        if qa_result is None:
            continue  # 아직 QA를 거치지 않은 classification 로그

        # 이미 QA Rule Book으로 처리된 것은 "LLM 판단"이 아니므로 제외
        if qa_result.get("qa_matched_rule_id") is not None:
            continue

        qa_passed = qa_result.get("qa_passed")
        if qa_passed is None:
            continue

        input_data = entry.get("input", {})
        resource_type = input_data.get("resource_type")
        action_executed = qa_result.get("action_executed")

        if not resource_type:
            continue

        key = (resource_type, action_executed, bool(qa_passed))
        groups[key].append(entry)

    candidates = []
    for (resource_type, action_executed, qa_passed), entries in groups.items():
        if len(entries) >= min_count:
            sample_reasoning = None
            for e in entries:
                sla = e.get("qa_result", {}).get("sla_check_result")
                if sla and sla.get("detail"):
                    sample_reasoning = sla["detail"]
                    break

            candidates.append({
                "resource_type": resource_type,
                "action_executed": action_executed,
                "qa_passed": qa_passed,
                "count": len(entries),
                "sample_reasoning": sample_reasoning or "",
                "entries": entries,  # 디버깅/분석용
            })

    candidates.sort(key=lambda x: x["count"], reverse=True)
    return candidates


def is_qa_already_covered(candidate: dict, existing_rules: list[dict]) -> bool:
    """이미 기존 QA 규칙으로 커버되는지 확인."""
    resource_type = candidate["resource_type"]
    action_executed = candidate["action_executed"]

    for rule in existing_rules:
        rule_types = rule.get("resource_types", [])
        conditions = rule.get("conditions", {})

        if "*" not in rule_types and resource_type not in rule_types:
            continue

        rule_actions = conditions.get("action_executed")
        if rule_actions:
            normalized = [None if x is None or x == "null" else x for x in rule_actions]
            if action_executed in normalized:
                return True

    return False


def create_qa_rule_from_candidate(candidate: dict, rule_id: str) -> dict:
    """후보에서 QA Rule Book 규칙 생성."""
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    now_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    action_label = candidate["action_executed"] or "None"
    verdict_label = "통과" if candidate["qa_passed"] else "실패"

    return {
        "rule_id": rule_id,
        "rule_type": "qa",
        "description": f"{candidate['resource_type']} {action_label} -> 항상 {verdict_label}",
        "resource_types": [candidate["resource_type"]],
        "conditions": {
            "action_executed": [candidate["action_executed"]]
        },
        "result": {
            "force_pass": True if candidate["qa_passed"] else False,
            "force_fail": False if candidate["qa_passed"] else True,
            "reasoning_template": (
                f"LLM SLA 판단 로그 {candidate['count']}건 반복 기반 자동 승격 "
                f"({action_label} -> 항상 {verdict_label})"
            ),
        },
        "priority": 100,  # 자동 승격 규칙은 낮은 우선순위 (classification과 동일한 관례)
        "enabled": True,
        "created_at": now_iso,
        "updated_at": now_iso,
        "author": "auto-promoted",
        "rationale": (
            f"{candidate['count']}건의 LLM SLA 판단 로그 기반 자동 승격, 승격일 {today} "
            f"(⚠️ classification과 달리 이 승격에는 후속 검증 단계가 없음 — 반복 횟수만 근거)"
        ),
    }


def display_candidate(candidate: dict, index: int) -> None:
    """후보 정보 출력."""
    print(f"\n{'='*60}")
    print(f"후보 #{index + 1}")
    print(f"{'='*60}")
    print(f"  리소스 타입      : {candidate['resource_type']}")
    print(f"  실행된 액션      : {candidate['action_executed']}")
    print(f"  QA 판정          : {'통과' if candidate['qa_passed'] else '실패'}")
    print(f"  반복 횟수        : {candidate['count']}회")
    print(f"  샘플 근거        : {candidate['sample_reasoning'][:80]}")


def main() -> None:
    parser = argparse.ArgumentParser(description="LLM SLA 판단 로그 기반 QA 규칙 자동 승격")
    parser.add_argument("--min-count", type=int, default=MIN_PROMOTION_COUNT,
                        help=f"최소 반복 횟수 (기본값: {MIN_PROMOTION_COUNT})")
    parser.add_argument("--dry-run", action="store_true",
                        help="실제 저장 없이 후보만 출력")
    args = parser.parse_args()

    print(f"\n[qa_rule_promoter] LLM SLA 판단 로그 분석 시작 (min_count={args.min_count})")
    print(f"[qa_rule_promoter] 규칙 파일: {QA_RULES_PATH}")
    print(
        "[qa_rule_promoter] ⚠️ classification과 달리 이 승격에는 후속 검증 단계가 없습니다 "
        "— 반복 횟수만으로 판단하므로 승인 시 신중하게 검토하세요."
    )

    logs = load_llm_logs()
    if not logs:
        print("\n[qa_rule_promoter] 분석할 로그가 없습니다.")
        return

    print(f"\n[qa_rule_promoter] 총 {len(logs)}개 로그 로드됨")

    existing_rules = load_existing_qa_rules()
    print(f"[qa_rule_promoter] 기존 QA 규칙 {len(existing_rules)}개 로드됨")

    candidates = find_qa_promotion_candidates(logs, args.min_count)

    if not candidates:
        print(f"\n[qa_rule_promoter] 승격 조건을 만족하는 후보가 없습니다.")
        print(f"  (조건: 'LLM이 직접 판단한' 케이스에서 동일 패턴 {args.min_count}회 이상)")
        return

    new_candidates = []
    for candidate in candidates:
        if is_qa_already_covered(candidate, existing_rules):
            print(
                f"\n[skip] 이미 규칙 존재: {candidate['resource_type']} + "
                f"{candidate['action_executed']}"
            )
        else:
            new_candidates.append(candidate)

    if not new_candidates:
        print(f"\n[qa_rule_promoter] 새로 승격할 후보가 없습니다. (모두 기존 규칙으로 커버됨)")
        return

    print(f"\n[qa_rule_promoter] 승격 후보 {len(new_candidates)}개 발견")

    promoted_count = 0
    for i, candidate in enumerate(new_candidates):
        display_candidate(candidate, i)

        if args.dry_run:
            print("\n  [dry-run] 실제 저장하지 않음")
            continue

        while True:
            response = input("\n  이 패턴을 규칙으로 승격하시겠습니까? (y/n): ").strip().lower()
            if response in ("y", "n"):
                break
            print("  'y' 또는 'n'으로 입력해주세요.")

        if response == "y":
            rule_id = get_next_qa_rule_id(existing_rules)
            new_rule = create_qa_rule_from_candidate(candidate, rule_id)
            existing_rules.append(new_rule)
            save_qa_rules(existing_rules)
            promoted_count += 1
            print(f"  [승격 완료] {rule_id} 추가됨")
        else:
            print("  [스킵]")

    print(f"\n{'='*60}")
    print(f"[qa_rule_promoter] 완료: {promoted_count}개 규칙 승격됨")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
