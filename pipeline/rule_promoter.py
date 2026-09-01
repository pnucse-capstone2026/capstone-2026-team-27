"""
Rule Promoter
-------------
LLM 판단 로그를 분석하여 반복적으로 검증된 패턴을 Rule Book에 자동 승격하는 스크립트.

사용법:
    CLI: python -m pipeline.rule_promoter [--min-count N] [--dry-run]
    코드: from pipeline.rule_promoter import auto_promote_rules; auto_promote_rules()

승격 조건:
    1. 같은 조건(resource_type + triggered_metrics 조합)에서
    2. LLM이 N번 이상 동일한 anomaly_type으로 판단
    3. 그 판단들이 qa_passed=True로 검증됨

승격 시:
    - rule_id: CLF-0XX 형식으로 자동 채번
    - author: "auto-promoted"
    - rationale: "N건의 LLM 판단 로그 기반 자동 승격, 승격일 YYYY-MM-DD"

자동 승격 (파이프라인 연동):
    - logging_agent.py에서 파이프라인 완료 시 auto_promote_rules() 호출
    - 사용자 승인 없이 조건 충족 시 자동 승격
"""

import argparse
import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# 설정
MIN_PROMOTION_COUNT = 3  # 최소 반복 횟수 (나중에 조정 가능)

# 파일 경로
SCRIPT_DIR = os.path.dirname(__file__)
LLM_LOG_PATH = os.path.join(SCRIPT_DIR, "..", "schema", "logs", "llm_classification_log.jsonl")
CLASSIFICATION_RULES_PATH = os.path.join(SCRIPT_DIR, "..", "schema", "rules", "classification_rules.json")
PENDING_PROMOTIONS_PATH = os.path.join(SCRIPT_DIR, "..", "schema", "logs", "pending_rule_promotions.json")


def load_llm_logs() -> list[dict]:
    """LLM 판단 로그 로드."""
    if not os.path.exists(LLM_LOG_PATH):
        print(f"[rule_promoter] 로그 파일 없음: {LLM_LOG_PATH}")
        return []

    logs = []
    with open(LLM_LOG_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                logs.append(entry)
            except json.JSONDecodeError:
                continue
    return logs


def load_existing_rules() -> list[dict]:
    """기존 Classification 규칙 로드."""
    if not os.path.exists(CLASSIFICATION_RULES_PATH):
        return []

    with open(CLASSIFICATION_RULES_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_rules(rules: list[dict]) -> None:
    """Classification 규칙 저장."""
    with open(CLASSIFICATION_RULES_PATH, "w", encoding="utf-8") as f:
        json.dump(rules, f, ensure_ascii=False, indent=2)


def get_next_rule_id(existing_rules: list[dict]) -> str:
    """다음 규칙 ID 생성 (CLF-006, CLF-007, ...)."""
    max_num = 0
    for rule in existing_rules:
        rule_id = rule.get("rule_id", "")
        if rule_id.startswith("CLF-"):
            try:
                num = int(rule_id.replace("CLF-", ""))
                max_num = max(max_num, num)
            except ValueError:
                continue
    return f"CLF-{max_num + 1:03d}"


def normalize_metrics(metrics: list[str]) -> tuple[str, ...]:
    """triggered_metrics를 정렬된 튜플로 정규화 (해시 가능하게)."""
    return tuple(sorted(metrics))


def _extract_spike_metrics(metrics_summary: dict, threshold: float = 2.0) -> list[str]:
    """
    metrics_summary에서 latest가 mean 대비 threshold배 이상인 지표들을 추출.
    triggered_metrics가 비어있을 때 (IForest만으로 탐지된 경우) 대체용.

    예: {"invocation_count": {"latest": 5000, "mean": 916}} → ["invocation_count"]
    """
    spike_metrics = []
    for metric_name, values in metrics_summary.items():
        if not isinstance(values, dict):
            continue
        latest = values.get("latest", 0)
        mean = values.get("mean", 0)
        if mean > 0 and latest / mean >= threshold:
            spike_metrics.append(metric_name)
    return spike_metrics


def find_promotion_candidates(logs: list[dict], min_count: int) -> list[dict]:
    """
    승격 후보 찾기.

    그룹핑 키: (resource_type, sorted(triggered_metrics), anomaly_type)
    조건: qa_passed=True인 항목만 카운트, min_count 이상이면 후보

    ⚠️ triggered_metrics가 비어있는 경우 (Isolation Forest만으로 탐지된 케이스):
       metrics_summary에서 평균 대비 2배 이상 급증한 지표들을 추출하여 대체.
    """
    # 그룹별 카운트 및 샘플 저장
    groups: dict[tuple, list[dict]] = defaultdict(list)

    for entry in logs:
        qa_result = entry.get("qa_result")
        if qa_result is None:
            continue  # QA 결과 없음

        if not qa_result.get("qa_passed"):
            continue  # QA 실패

        input_data = entry.get("input", {})
        output_data = entry.get("output", {})

        resource_type = input_data.get("resource_type")
        triggered_metrics = input_data.get("triggered_metrics", [])
        anomaly_type = output_data.get("anomaly_type")

        if not resource_type or not anomaly_type:
            continue

        # triggered_metrics가 비어있으면 metrics_summary에서 급증 지표 추출
        if not triggered_metrics:
            triggered_metrics = _extract_spike_metrics(input_data.get("metrics_summary", {}))

        # 그래도 비어있으면 스킵 (판단 근거 없음)
        if not triggered_metrics:
            continue

        key = (resource_type, normalize_metrics(triggered_metrics), anomaly_type)
        groups[key].append(entry)

    # 후보 추출
    candidates = []
    for (resource_type, metrics_tuple, anomaly_type), entries in groups.items():
        if len(entries) >= min_count:
            # 대표 샘플에서 reasoning 추출
            sample_reasoning = entries[0].get("output", {}).get("reasoning", "")
            sample_interim_action = entries[0].get("output", {}).get("interim_action")

            candidates.append({
                "resource_type": resource_type,
                "triggered_metrics": list(metrics_tuple),
                "anomaly_type": anomaly_type,
                "count": len(entries),
                "sample_reasoning": sample_reasoning,
                "sample_interim_action": sample_interim_action,
                "entries": entries,  # 디버깅/분석용
            })

    # 카운트 내림차순 정렬
    candidates.sort(key=lambda x: x["count"], reverse=True)
    return candidates


def is_already_covered(candidate: dict, existing_rules: list[dict]) -> bool:
    """이미 기존 규칙으로 커버되는지 확인."""
    resource_type = candidate["resource_type"]
    triggered_metrics = set(candidate["triggered_metrics"])

    for rule in existing_rules:
        rule_types = rule.get("resource_types", [])
        conditions = rule.get("conditions", {})

        # 리소스 타입 매칭
        if "*" not in rule_types and resource_type not in rule_types:
            continue

        # triggered_metrics 조건 매칭
        rule_metrics = conditions.get("triggered_metrics")
        if rule_metrics:
            if set(rule_metrics) == triggered_metrics:
                return True

    return False


def create_rule_from_candidate(candidate: dict, rule_id: str) -> dict:
    """후보에서 Rule Book 규칙 생성."""
    from datetime import timezone
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    now_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    return {
        "rule_id": rule_id,
        "rule_type": "classification",
        "description": f"{candidate['resource_type']} {'+'.join(candidate['triggered_metrics'])} -> {candidate['anomaly_type']}",
        "resource_types": [candidate["resource_type"]],
        "conditions": {
            "triggered_metrics": candidate["triggered_metrics"]
        },
        "result": {
            "anomaly_type": candidate["anomaly_type"],
            "interim_action": candidate["sample_interim_action"],
            "reasoning_template": candidate["sample_reasoning"].replace("[LLM] ", "")
        },
        "priority": 100,  # 자동 승격 규칙은 낮은 우선순위
        "enabled": True,
        "created_at": now_iso,
        "updated_at": now_iso,
        "author": "auto-promoted",
        "rationale": f"{candidate['count']}건의 LLM 판단 로그 기반 자동 승격, 승격일 {today}"
    }


def display_candidate(candidate: dict, index: int) -> None:
    """후보 정보 출력."""
    print(f"\n{'='*60}")
    print(f"후보 #{index + 1}")
    print(f"{'='*60}")
    print(f"  리소스 타입      : {candidate['resource_type']}")
    print(f"  트리거 메트릭    : {candidate['triggered_metrics']}")
    print(f"  이상 유형        : {candidate['anomaly_type']}")
    print(f"  검증된 판단 횟수 : {candidate['count']}회")
    print(f"  샘플 reasoning   : {candidate['sample_reasoning'][:80]}...")
    print(f"  샘플 interim_action: {candidate['sample_interim_action']}")


# ── 승인 대기 큐 관리 ─────────────────────────────────────────────────────────


def load_pending_promotions() -> dict:
    """승인 대기 중인 규칙 로드."""
    if not os.path.exists(PENDING_PROMOTIONS_PATH):
        return {"classification": [], "decision": []}
    try:
        with open(PENDING_PROMOTIONS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {"classification": [], "decision": []}


def save_pending_promotions(pending: dict) -> None:
    """승인 대기 중인 규칙 저장."""
    os.makedirs(os.path.dirname(PENDING_PROMOTIONS_PATH), exist_ok=True)
    with open(PENDING_PROMOTIONS_PATH, "w", encoding="utf-8") as f:
        json.dump(pending, f, ensure_ascii=False, indent=2)


def is_already_pending(candidate: dict, pending_list: list[dict]) -> bool:
    """이미 승인 대기 중인지 확인."""
    resource_type = candidate["resource_type"]
    triggered_metrics = set(candidate["triggered_metrics"])

    for pending in pending_list:
        if pending.get("resource_type") != resource_type:
            continue
        pending_metrics = set(pending.get("triggered_metrics", []))
        if pending_metrics == triggered_metrics:
            return True
    return False


# ── 승인 대기 큐 추가 함수 (파이프라인 연동용) ─────────────────────────────────


def queue_promotion_candidates(min_count: int = MIN_PROMOTION_COUNT) -> list[dict]:
    """
    조건을 충족하는 패턴을 승인 대기 큐에 추가 (관리자 승인 필요).

    파이프라인(logging_agent.py)에서 호출되어 승격 후보를 대기 큐에 등록.
    실제 승격은 관리자가 웹에서 승인 시 approve_pending_rule()로 진행.

    Args:
        min_count: 최소 반복 횟수 (기본값: MIN_PROMOTION_COUNT)

    Returns:
        대기 큐에 추가된 후보 목록
    """
    queued_candidates = []

    try:
        # 로그 로드
        logs = load_llm_logs()
        if not logs:
            return []

        # 기존 규칙 로드
        existing_rules = load_existing_rules()

        # 승인 대기 큐 로드
        pending = load_pending_promotions()
        pending_clf = pending.get("classification", [])

        # 승격 후보 찾기
        candidates = find_promotion_candidates(logs, min_count)
        if not candidates:
            return []

        # 새로운 후보만 필터링 (기존 규칙에도 없고, 대기 큐에도 없는 것)
        for candidate in candidates:
            if is_already_covered(candidate, existing_rules):
                continue
            if is_already_pending(candidate, pending_clf):
                continue

            # 대기 큐에 추가할 항목 생성
            pending_entry = {
                "id": f"pending-clf-{len(pending_clf) + 1}",
                "resource_type": candidate["resource_type"],
                "triggered_metrics": candidate["triggered_metrics"],
                "anomaly_type": candidate["anomaly_type"],
                "count": candidate["count"],
                "sample_reasoning": candidate["sample_reasoning"],
                "sample_interim_action": candidate["sample_interim_action"],
                "queued_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            pending_clf.append(pending_entry)
            queued_candidates.append(pending_entry)

            logger.info(
                "[queue_promotion] 승인 대기 큐 추가: %s + %s -> %s (%d건 검증됨)",
                candidate["resource_type"],
                candidate["triggered_metrics"],
                candidate["anomaly_type"],
                candidate["count"],
            )

        # 대기 큐 저장
        if queued_candidates:
            pending["classification"] = pending_clf
            save_pending_promotions(pending)
            logger.info("[queue_promotion] %d개 후보 대기 큐 추가 완료", len(queued_candidates))

        return queued_candidates

    except Exception as e:
        logger.error("[queue_promotion] 대기 큐 추가 실패: %s", e)
        return []


def approve_pending_rule(pending_id: str) -> Optional[dict]:
    """
    승인 대기 중인 규칙을 승인하여 Rule Book에 추가.

    Args:
        pending_id: 대기 중인 규칙 ID (예: "pending-clf-1")

    Returns:
        승격된 규칙 dict, 실패 시 None
    """
    try:
        pending = load_pending_promotions()
        pending_clf = pending.get("classification", [])

        # 해당 ID 찾기
        target = None
        target_idx = -1
        for i, entry in enumerate(pending_clf):
            if entry.get("id") == pending_id:
                target = entry
                target_idx = i
                break

        if target is None:
            logger.warning("[approve_rule] 대기 중인 규칙 없음: %s", pending_id)
            return None

        # 규칙 생성 및 저장
        existing_rules = load_existing_rules()
        rule_id = get_next_rule_id(existing_rules)

        candidate = {
            "resource_type": target["resource_type"],
            "triggered_metrics": target["triggered_metrics"],
            "anomaly_type": target["anomaly_type"],
            "count": target["count"],
            "sample_reasoning": target["sample_reasoning"],
            "sample_interim_action": target["sample_interim_action"],
        }
        new_rule = create_rule_from_candidate(candidate, rule_id)
        existing_rules.append(new_rule)
        save_rules(existing_rules)

        # 대기 큐에서 제거
        pending_clf.pop(target_idx)
        pending["classification"] = pending_clf
        save_pending_promotions(pending)

        logger.info("[approve_rule] 규칙 승격 완료: %s -> %s", pending_id, rule_id)
        return new_rule

    except Exception as e:
        logger.error("[approve_rule] 규칙 승인 실패: %s", e)
        return None


def reject_pending_rule(pending_id: str) -> bool:
    """
    승인 대기 중인 규칙을 거부 (대기 큐에서 제거).

    Args:
        pending_id: 대기 중인 규칙 ID

    Returns:
        성공 여부
    """
    try:
        pending = load_pending_promotions()
        pending_clf = pending.get("classification", [])

        # 해당 ID 찾아서 제거
        for i, entry in enumerate(pending_clf):
            if entry.get("id") == pending_id:
                pending_clf.pop(i)
                pending["classification"] = pending_clf
                save_pending_promotions(pending)
                logger.info("[reject_rule] 규칙 거부됨: %s", pending_id)
                return True

        logger.warning("[reject_rule] 대기 중인 규칙 없음: %s", pending_id)
        return False

    except Exception as e:
        logger.error("[reject_rule] 규칙 거부 실패: %s", e)
        return False


# ── 하위 호환용 (기존 auto_promote_rules 유지) ─────────────────────────────────


def auto_promote_rules(min_count: int = MIN_PROMOTION_COUNT) -> list[dict]:
    """
    [Deprecated] 이전 버전 호환용. 이제 queue_promotion_candidates() 사용 권장.

    이 함수는 이제 자동 승격 대신 승인 대기 큐에 추가합니다.
    """
    return queue_promotion_candidates(min_count)


def check_promotion_candidates(min_count: int = MIN_PROMOTION_COUNT) -> list[dict]:
    """
    승격 가능한 후보가 있는지 확인만 (실제 승격은 안 함).

    Returns:
        승격 가능한 후보 목록
    """
    try:
        logs = load_llm_logs()
        if not logs:
            return []

        existing_rules = load_existing_rules()
        candidates = find_promotion_candidates(logs, min_count)

        return [
            {
                "resource_type": c["resource_type"],
                "triggered_metrics": c["triggered_metrics"],
                "anomaly_type": c["anomaly_type"],
                "count": c["count"],
            }
            for c in candidates
            if not is_already_covered(c, existing_rules)
        ]
    except Exception:
        return []


def main():
    parser = argparse.ArgumentParser(description="LLM 판단 로그 기반 규칙 자동 승격")
    parser.add_argument("--min-count", type=int, default=MIN_PROMOTION_COUNT,
                        help=f"최소 반복 횟수 (기본값: {MIN_PROMOTION_COUNT})")
    parser.add_argument("--dry-run", action="store_true",
                        help="실제 저장 없이 후보만 출력")
    args = parser.parse_args()

    print(f"\n[rule_promoter] LLM 판단 로그 분석 시작 (min_count={args.min_count})")
    print(f"[rule_promoter] 로그 파일: {LLM_LOG_PATH}")
    print(f"[rule_promoter] 규칙 파일: {CLASSIFICATION_RULES_PATH}")

    # 로그 로드
    logs = load_llm_logs()
    if not logs:
        print("\n[rule_promoter] 분석할 로그가 없습니다.")
        return

    print(f"\n[rule_promoter] 총 {len(logs)}개 로그 로드됨")

    # 기존 규칙 로드
    existing_rules = load_existing_rules()
    print(f"[rule_promoter] 기존 규칙 {len(existing_rules)}개 로드됨")

    # 승격 후보 찾기
    candidates = find_promotion_candidates(logs, args.min_count)

    if not candidates:
        print(f"\n[rule_promoter] 승격 조건을 만족하는 후보가 없습니다.")
        print(f"  (조건: qa_passed=True이고 동일 패턴 {args.min_count}회 이상)")
        return

    # 이미 커버되는 후보 필터링
    new_candidates = []
    for candidate in candidates:
        if is_already_covered(candidate, existing_rules):
            print(f"\n[skip] 이미 규칙 존재: {candidate['resource_type']} + {candidate['triggered_metrics']}")
        else:
            new_candidates.append(candidate)

    if not new_candidates:
        print(f"\n[rule_promoter] 새로 승격할 후보가 없습니다. (모두 기존 규칙으로 커버됨)")
        return

    print(f"\n[rule_promoter] 승격 후보 {len(new_candidates)}개 발견")

    # 각 후보에 대해 승인 요청
    promoted_count = 0
    for i, candidate in enumerate(new_candidates):
        display_candidate(candidate, i)

        if args.dry_run:
            print("\n  [dry-run] 실제 저장하지 않음")
            continue

        # 사용자 승인
        while True:
            response = input("\n  이 패턴을 규칙으로 승격하시겠습니까? (y/n): ").strip().lower()
            if response in ("y", "n"):
                break
            print("  'y' 또는 'n'으로 입력해주세요.")

        if response == "y":
            rule_id = get_next_rule_id(existing_rules)
            new_rule = create_rule_from_candidate(candidate, rule_id)
            existing_rules.append(new_rule)
            save_rules(existing_rules)
            promoted_count += 1
            print(f"  [승격 완료] {rule_id} 추가됨")
        else:
            print("  [스킵]")

    print(f"\n{'='*60}")
    print(f"[rule_promoter] 완료: {promoted_count}개 규칙 승격됨")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
