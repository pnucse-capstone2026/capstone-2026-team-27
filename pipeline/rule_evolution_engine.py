"""
Rule Evolution Engine
---------------------
Rule Book 자가진화 핵심 로직

주요 기능:
1. 저성능 규칙 탐지 (win_rate < 60%)
2. LLM 기반 규칙 개선/비활성화 결정
3. 유사 규칙 통합
4. JSON 파일 자동 업데이트 및 즉시 적용
5. 변경 이력 관리 (백업/롤백)
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any, Optional
from zoneinfo import ZoneInfo

from filelock import FileLock, Timeout

from pipeline.rule_stats_logger import (
    get_all_underperforming_rules,
    get_rule_failure_summary,
    mark_evolution_completed,
    WIN_RATE_THRESHOLD,
    EVOLUTION_TRIGGER_COUNT,
)
from pipeline.rule_engine import reload_rules

# LLM 호출 유틸리티
try:
    from utils.llm_utils import call_gemini
except ImportError:
    call_gemini = None

# ── 경로 설정 ─────────────────────────────────────────────────────────────────
_RULES_DIR = os.path.join(os.path.dirname(__file__), "..", "schema", "rules")
_HISTORY_DIR = os.path.join(_RULES_DIR, "rule_history")
_CLF_RULES_PATH = os.path.join(_RULES_DIR, "classification_rules.json")
_QA_RULES_PATH = os.path.join(_RULES_DIR, "qa_rules.json")

# ── 설정값 ────────────────────────────────────────────────────────────────────
FILE_LOCK_TIMEOUT = 10  # 파일 락 타임아웃 (초)
BACKUP_RETENTION_DAYS = 30  # 백업 보관 기간 (일)


# ══════════════════════════════════════════════════════════════════════════════
# LLM 프롬프트
# ══════════════════════════════════════════════════════════════════════════════

RULE_IMPROVEMENT_PROMPT = """
당신은 AWS 클라우드 이상 탐지 Rule Book 전문가입니다.
아래 저성능 규칙을 분석하고 개선안을 JSON으로만 제안하세요.
마크다운 코드블록, 설명 텍스트 없이 JSON만 출력하세요.

## 저성능 규칙 정보
- rule_id: {rule_id}
- rule_type: {rule_type}
- 설명: {description}
- 조건: {conditions}
- 결과: {result}
- 성능: total_runs={total_runs}, wins={total_wins}, win_rate={win_rate:.1%}

## 유사한 다른 규칙들
{similar_rules}

## 분석 요청
1. 왜 이 규칙의 win_rate가 낮은지 추론하세요
2. 개선 방안을 제안하세요:
   - "improve": 조건/결과 수정으로 정확도 향상 가능
   - "disable": 규칙 자체가 부적절하거나 더 이상 유효하지 않음
   - "merge": 유사 규칙과 통합 필요

## 응답 형식 (JSON만, 다른 텍스트 금지)
{{
  "analysis": "저성능 원인 분석 (2-3문장)",
  "recommendation": "improve" | "disable" | "merge",
  "improved_rule": {{...}},
  "merge_with": "rule_id (merge인 경우만)",
  "reasoning": "개선/비활성화/통합 근거 (1-2문장)"
}}

improved_rule 필드는 recommendation이 "improve" 또는 "merge"일 때만 포함하세요.
improved_rule의 구조는 원본 규칙과 동일해야 합니다.
"""


# ══════════════════════════════════════════════════════════════════════════════
# 메인 트리거
# ══════════════════════════════════════════════════════════════════════════════

def trigger_evolution(rule_id: str, conn) -> dict[str, Any]:
    """
    자가진화 메인 트리거.

    Args:
        rule_id: 트리거된 규칙 ID
        conn: PostgreSQL 연결 객체

    Returns:
        {
            "triggered_rule_id": str,
            "underperforming_rules": list,
            "actions_taken": list[dict],
            "success": bool,
        }
    """
    print(f"[evolution] 자가진화 시작 (trigger: {rule_id})")

    result = {
        "triggered_rule_id": rule_id,
        "underperforming_rules": [],
        "actions_taken": [],
        "success": False,
    }

    try:
        # 1. 저성능 규칙 전체 탐지
        underperforming = get_all_underperforming_rules(conn)
        result["underperforming_rules"] = underperforming

        if not underperforming:
            print("[evolution] 저성능 규칙 없음 - 진화 스킵")
            mark_evolution_completed(conn, rule_id)
            result["success"] = True
            return result

        print(f"[evolution] 저성능 규칙 {len(underperforming)}개 발견")

        # 2. 각 저성능 규칙에 대해 개선 시도
        for rule_stats in underperforming:
            try:
                action = _evolve_single_rule(rule_stats, conn)
                result["actions_taken"].append(action)
            except Exception as e:
                result["actions_taken"].append({
                    "rule_id": rule_stats["rule_id"],
                    "action": "error",
                    "error": str(e),
                })
                print(f"[evolution] 규칙 {rule_stats['rule_id']} 진화 실패: {e}")

        # 3. 진화 완료 마킹
        mark_evolution_completed(conn, rule_id)
        result["success"] = True

    except Exception as e:
        print(f"[evolution] 자가진화 전체 실패: {e}")
        result["success"] = False

    return result


def _evolve_single_rule(rule_stats: dict, conn) -> dict[str, Any]:
    """
    단일 규칙에 대한 진화 수행.

    Returns:
        {
            "rule_id": str,
            "action": "improved" | "disabled" | "merged" | "skipped",
            "details": dict,
        }
    """
    rule_id = rule_stats["rule_id"]
    rule_type = rule_stats["rule_type"]

    print(f"[evolution] 규칙 진화 시도: {rule_id} (win_rate={rule_stats['win_rate']:.1%})")

    # LLM 판단은 규칙이 아니므로 스킵
    if rule_type == "llm":
        print(f"[evolution] LLM 판단은 규칙이 아님 - 스킵")
        return {"rule_id": rule_id, "action": "skipped", "details": {"reason": "LLM 판단"}}

    # 규칙 원본 로드
    rule = _load_rule_by_id(rule_id, rule_type)
    if rule is None:
        return {"rule_id": rule_id, "action": "skipped", "details": {"reason": "규칙 찾기 실패"}}

    # 유사 규칙 탐지
    all_rules = _load_all_rules(rule_type)
    similar = find_similar_rules(rule, all_rules)

    # LLM에게 개선 요청
    improvement = improve_rule_with_llm(rule, rule_stats, similar)

    if improvement is None:
        return {"rule_id": rule_id, "action": "skipped", "details": {"reason": "LLM 호출 실패"}}

    recommendation = improvement.get("recommendation", "disable")

    # 추천에 따라 액션 수행
    if recommendation == "improve":
        improved_rule = improvement.get("improved_rule")
        if improved_rule:
            apply_rule_changes([{"action": "update", "rule": improved_rule}], rule_type)
            return {
                "rule_id": rule_id,
                "action": "improved",
                "details": {
                    "reasoning": improvement.get("reasoning"),
                    "analysis": improvement.get("analysis"),
                },
            }

    elif recommendation == "merge":
        merge_with = improvement.get("merge_with")
        improved_rule = improvement.get("improved_rule")
        if merge_with and improved_rule:
            # 원본 비활성화 + 통합 규칙 업데이트
            apply_rule_changes([
                {"action": "disable", "rule_id": rule_id},
                {"action": "update", "rule": improved_rule},
            ], rule_type)
            return {
                "rule_id": rule_id,
                "action": "merged",
                "details": {
                    "merged_with": merge_with,
                    "reasoning": improvement.get("reasoning"),
                },
            }

    # 기본: 비활성화
    disable_rule(rule_id, rule_type, improvement.get("reasoning", "저성능 규칙 자동 비활성화"))
    return {
        "rule_id": rule_id,
        "action": "disabled",
        "details": {
            "reasoning": improvement.get("reasoning"),
            "analysis": improvement.get("analysis"),
        },
    }


# ══════════════════════════════════════════════════════════════════════════════
# 규칙 로드/저장
# ══════════════════════════════════════════════════════════════════════════════

def _get_rules_path(rule_type: str) -> str:
    """규칙 타입에 따른 JSON 파일 경로 반환."""
    if rule_type == "classification":
        return _CLF_RULES_PATH
    elif rule_type == "qa":
        return _QA_RULES_PATH
    else:
        raise ValueError(f"알 수 없는 규칙 타입: {rule_type}")


def _load_all_rules(rule_type: str) -> list[dict]:
    """지정된 타입의 모든 규칙 로드."""
    rules_path = _get_rules_path(rule_type)
    if not os.path.exists(rules_path):
        return []

    with open(rules_path, "r", encoding="utf-8") as f:
        return json.load(f)


def _load_rule_by_id(rule_id: str, rule_type: str) -> Optional[dict]:
    """ID로 특정 규칙 로드."""
    rules = _load_all_rules(rule_type)
    for rule in rules:
        if rule.get("rule_id") == rule_id:
            return rule
    return None


def _save_rules(rules: list[dict], rule_type: str) -> None:
    """규칙 목록을 JSON 파일에 저장."""
    rules_path = _get_rules_path(rule_type)
    with open(rules_path, "w", encoding="utf-8") as f:
        json.dump(rules, f, ensure_ascii=False, indent=2)


# ══════════════════════════════════════════════════════════════════════════════
# 규칙 변경 적용
# ══════════════════════════════════════════════════════════════════════════════

def apply_rule_changes(changes: list[dict], rule_type: str) -> bool:
    """
    규칙 변경사항을 JSON에 적용하고 즉시 reload.

    Args:
        changes: 변경 목록
            - {"action": "update", "rule": {...}}
            - {"action": "add", "rule": {...}}
            - {"action": "disable", "rule_id": "..."}
            - {"action": "delete", "rule_id": "..."}
        rule_type: "classification" 또는 "qa"

    Returns:
        True if 성공
    """
    rules_path = _get_rules_path(rule_type)
    lock_path = f"{rules_path}.lock"

    try:
        with FileLock(lock_path, timeout=FILE_LOCK_TIMEOUT):
            # 1. 현재 규칙 로드
            rules = _load_all_rules(rule_type)

            # 2. 변경 전 백업
            _backup_rules(rules, rule_type)

            # 3. 변경 적용
            for change in changes:
                action = change.get("action")

                if action == "add":
                    new_rule = change.get("rule")
                    if new_rule:
                        # updated_at 추가
                        new_rule["updated_at"] = datetime.utcnow().isoformat() + "Z"
                        rules.append(new_rule)
                        print(f"[evolution] 규칙 추가: {new_rule.get('rule_id')}")

                elif action == "update":
                    updated_rule = change.get("rule")
                    if updated_rule:
                        rule_id = updated_rule.get("rule_id")
                        updated_rule["updated_at"] = datetime.utcnow().isoformat() + "Z"
                        for i, r in enumerate(rules):
                            if r.get("rule_id") == rule_id:
                                rules[i] = updated_rule
                                print(f"[evolution] 규칙 업데이트: {rule_id}")
                                break

                elif action == "disable":
                    rule_id = change.get("rule_id")
                    for r in rules:
                        if r.get("rule_id") == rule_id:
                            r["enabled"] = False
                            r["disabled_at"] = datetime.utcnow().isoformat() + "Z"
                            r["disabled_reason"] = change.get("reason", "자가진화 자동 비활성화")
                            print(f"[evolution] 규칙 비활성화: {rule_id}")
                            break

                elif action == "delete":
                    rule_id = change.get("rule_id")
                    rules = [r for r in rules if r.get("rule_id") != rule_id]
                    print(f"[evolution] 규칙 삭제: {rule_id}")

            # 4. 충돌 감지
            conflicts = detect_rule_conflicts(rules)
            if conflicts:
                print(f"[evolution] 경고: 규칙 충돌 감지됨 - {conflicts}")
                # 충돌이 있어도 일단 저장 (로그로 경고만)

            # 5. 저장
            _save_rules(rules, rule_type)

        # 6. 즉시 적용 (락 해제 후)
        reload_rules()
        print(f"[evolution] 규칙 reload 완료 ({rule_type})")

        return True

    except Timeout:
        print(f"[evolution] 파일 락 타임아웃 - 변경 스킵 ({rules_path})")
        return False
    except Exception as e:
        print(f"[evolution] 규칙 변경 적용 실패: {e}")
        return False


def disable_rule(rule_id: str, rule_type: str, reason: str) -> bool:
    """
    규칙을 비활성화.

    Args:
        rule_id: 규칙 ID
        rule_type: "classification" 또는 "qa"
        reason: 비활성화 사유

    Returns:
        True if 성공
    """
    return apply_rule_changes([{
        "action": "disable",
        "rule_id": rule_id,
        "reason": reason,
    }], rule_type)


# ══════════════════════════════════════════════════════════════════════════════
# LLM 규칙 개선
# ══════════════════════════════════════════════════════════════════════════════

def improve_rule_with_llm(
    rule: dict,
    rule_stats: dict,
    similar_rules: list[dict]
) -> Optional[dict]:
    """
    LLM에게 규칙 개선을 요청.

    Args:
        rule: 원본 규칙
        rule_stats: 규칙 통계 (total_runs, win_rate 등)
        similar_rules: 유사 규칙 목록

    Returns:
        {
            "analysis": str,
            "recommendation": "improve" | "disable" | "merge",
            "improved_rule": dict (optional),
            "merge_with": str (optional),
            "reasoning": str,
        }
        또는 None (LLM 호출 실패 시)
    """
    if call_gemini is None:
        print("[evolution] LLM 유틸리티 없음 - 기본 비활성화 추천")
        return {
            "analysis": "LLM 호출 불가",
            "recommendation": "disable",
            "reasoning": f"win_rate {rule_stats.get('win_rate', 0):.1%} < 60% (자동 비활성화)",
        }

    # 프롬프트 구성
    similar_str = "없음"
    if similar_rules:
        similar_str = json.dumps(
            [{"rule_id": r["rule"].get("rule_id"), "similarity": r["similarity"]}
             for r in similar_rules[:3]],
            ensure_ascii=False
        )

    prompt = RULE_IMPROVEMENT_PROMPT.format(
        rule_id=rule.get("rule_id", ""),
        rule_type=rule.get("rule_type", ""),
        description=rule.get("description", ""),
        conditions=json.dumps(rule.get("conditions", {}), ensure_ascii=False),
        result=json.dumps(rule.get("result", {}), ensure_ascii=False),
        total_runs=rule_stats.get("total_runs", 0),
        total_wins=rule_stats.get("total_wins", 0),
        win_rate=rule_stats.get("win_rate", 0),
        similar_rules=similar_str,
    )

    try:
        raw_text = call_gemini(prompt, temperature=0.2)
        parsed = _parse_llm_json(raw_text)
        return parsed
    except Exception as e:
        print(f"[evolution] LLM 호출 실패: {e}")
        return {
            "analysis": f"LLM 호출 실패: {e}",
            "recommendation": "disable",
            "reasoning": f"win_rate {rule_stats.get('win_rate', 0):.1%} < 60% (LLM 실패로 자동 비활성화)",
        }


def _parse_llm_json(text: str) -> dict:
    """LLM 응답에서 JSON 추출."""
    import re
    cleaned = re.sub(r"```(?:json)?", "", text).replace("```", "").strip()
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        return {
            "analysis": f"JSON 파싱 실패: {text[:200]}",
            "recommendation": "disable",
            "reasoning": "LLM 응답 파싱 실패로 안전하게 비활성화",
        }


# ══════════════════════════════════════════════════════════════════════════════
# 유사 규칙 탐지
# ══════════════════════════════════════════════════════════════════════════════

def find_similar_rules(rule: dict, all_rules: list[dict]) -> list[dict]:
    """
    유사 규칙 탐지 (통합 후보).

    유사 판단 기준:
    1. resource_types가 겹침 (하나라도 교집합) - 필수 조건
    2. triggered_metrics가 겹침 (50% 이상)
    3. anomaly_type 또는 action_executed가 동일

    Returns:
        [{"rule": dict, "similarity": float}, ...]
        (similarity 내림차순 정렬)
    """
    similar = []
    rule_id = rule.get("rule_id")
    rule_type = rule.get("rule_type")
    r1_types = set(rule.get("resource_types", []))

    for other in all_rules:
        if other.get("rule_id") == rule_id:
            continue
        if other.get("rule_type") != rule_type:
            continue
        if not other.get("enabled", True):
            continue  # 이미 비활성화된 규칙은 제외

        # resource_types가 겹치지 않으면 스킵 (필수 조건)
        r2_types = set(other.get("resource_types", []))
        if "*" not in r1_types and "*" not in r2_types:
            if not (r1_types & r2_types):
                continue  # 겹치는 리소스 타입이 없으면 유사 규칙 아님

        similarity = _calculate_similarity(rule, other)
        if similarity >= 0.3:  # 30% 이상 유사하면 후보에 포함
            similar.append({
                "rule": other,
                "similarity": similarity,
            })

    return sorted(similar, key=lambda x: x["similarity"], reverse=True)


def _calculate_similarity(rule1: dict, rule2: dict) -> float:
    """
    두 규칙의 유사도 계산 (0.0 ~ 1.0).
    """
    scores = []

    # 1. resource_types 비교
    r1_types = set(rule1.get("resource_types", []))
    r2_types = set(rule2.get("resource_types", []))
    if r1_types and r2_types:
        if "*" in r1_types or "*" in r2_types:
            scores.append(1.0)
        else:
            intersection = len(r1_types & r2_types)
            union = len(r1_types | r2_types)
            scores.append(intersection / union if union > 0 else 0)

    # 2. conditions 비교
    c1 = rule1.get("conditions", {})
    c2 = rule2.get("conditions", {})

    # triggered_metrics 비교
    m1 = set(c1.get("triggered_metrics", []))
    m2 = set(c2.get("triggered_metrics", []))
    if m1 and m2:
        intersection = len(m1 & m2)
        union = len(m1 | m2)
        scores.append(intersection / union if union > 0 else 0)

    # action_executed 비교 (QA 규칙)
    a1 = set(c1.get("action_executed", []))
    a2 = set(c2.get("action_executed", []))
    if a1 and a2:
        intersection = len(a1 & a2)
        union = len(a1 | a2)
        scores.append(intersection / union if union > 0 else 0)

    # 3. result 비교
    res1 = rule1.get("result", {})
    res2 = rule2.get("result", {})

    # anomaly_type 비교 (classification 규칙)
    if res1.get("anomaly_type") and res2.get("anomaly_type"):
        scores.append(1.0 if res1["anomaly_type"] == res2["anomaly_type"] else 0.0)

    # force_pass/force_fail 비교 (QA 규칙)
    if "force_pass" in res1 and "force_pass" in res2:
        scores.append(1.0 if res1["force_pass"] == res2["force_pass"] else 0.0)

    return sum(scores) / len(scores) if scores else 0.0


# ══════════════════════════════════════════════════════════════════════════════
# 규칙 충돌 감지
# ══════════════════════════════════════════════════════════════════════════════

def detect_rule_conflicts(rules: list[dict]) -> list[dict]:
    """
    규칙 충돌 감지.

    충돌 유형:
    1. 완전 동일 조건 + 다른 결과 (anomaly_type 또는 force_pass)
    2. 더 넓은 조건이 더 좁은 조건을 가리는 경우 (우선순위 역전)

    Returns:
        [{"rule_1": str, "rule_2": str, "type": str, "detail": str}, ...]
    """
    conflicts = []
    enabled_rules = [r for r in rules if r.get("enabled", True)]

    for i, r1 in enumerate(enabled_rules):
        for r2 in enabled_rules[i+1:]:
            if r1.get("rule_type") != r2.get("rule_type"):
                continue

            c1 = r1.get("conditions", {})
            c2 = r2.get("conditions", {})

            # 조건이 겹치는지 확인
            if not _conditions_overlap(c1, c2):
                continue

            # 결과가 충돌하는지 확인
            res1 = r1.get("result", {})
            res2 = r2.get("result", {})

            if _results_conflict(res1, res2, r1.get("rule_type")):
                conflicts.append({
                    "rule_1": r1.get("rule_id"),
                    "rule_2": r2.get("rule_id"),
                    "type": "conflicting_results",
                    "detail": f"동일 조건에 다른 결과",
                })

    return conflicts


def _conditions_overlap(c1: dict, c2: dict) -> bool:
    """두 조건이 겹치는지 (동시에 매칭될 수 있는지) 확인."""
    # triggered_metrics 비교
    m1 = set(c1.get("triggered_metrics", []))
    m2 = set(c2.get("triggered_metrics", []))
    if m1 and m2 and not (m1 & m2):
        return False  # 겹치는 메트릭이 없으면 동시 매칭 불가

    # action_executed 비교
    a1 = set(c1.get("action_executed", []))
    a2 = set(c2.get("action_executed", []))
    if a1 and a2 and not (a1 & a2):
        return False

    return True


def _results_conflict(res1: dict, res2: dict, rule_type: str) -> bool:
    """결과가 충돌하는지 확인."""
    if rule_type == "classification":
        at1 = res1.get("anomaly_type")
        at2 = res2.get("anomaly_type")
        return at1 and at2 and at1 != at2

    elif rule_type == "qa":
        fp1 = res1.get("force_pass")
        fp2 = res2.get("force_pass")
        ff1 = res1.get("force_fail")
        ff2 = res2.get("force_fail")

        # force_pass vs force_fail 충돌
        if (fp1 and ff2) or (ff1 and fp2):
            return True
        # 둘 다 force_pass지만 값이 다름
        if fp1 is not None and fp2 is not None and fp1 != fp2:
            return True

    return False


# ══════════════════════════════════════════════════════════════════════════════
# 백업 및 롤백
# ══════════════════════════════════════════════════════════════════════════════

def _backup_rules(rules: list[dict], rule_type: str) -> str:
    """
    변경 전 규칙 상태를 히스토리 디렉토리에 백업.

    Returns:
        백업 파일 경로
    """
    os.makedirs(_HISTORY_DIR, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")
    backup_path = os.path.join(_HISTORY_DIR, f"{rule_type}_{timestamp}.json")

    with open(backup_path, "w", encoding="utf-8") as f:
        json.dump(rules, f, ensure_ascii=False, indent=2)

    print(f"[evolution] 규칙 백업 완료: {backup_path}")

    # 오래된 백업 정리
    _cleanup_old_backups(rule_type)

    return backup_path


def _cleanup_old_backups(rule_type: str, max_age_days: int = BACKUP_RETENTION_DAYS) -> None:
    """오래된 백업 파일 삭제."""
    if not os.path.exists(_HISTORY_DIR):
        return

    cutoff = datetime.utcnow() - timedelta(days=max_age_days)
    prefix = f"{rule_type}_"

    for filename in os.listdir(_HISTORY_DIR):
        if not filename.startswith(prefix):
            continue

        filepath = os.path.join(_HISTORY_DIR, filename)
        try:
            # 파일 수정 시간 기준
            mtime = datetime.fromtimestamp(os.path.getmtime(filepath))
            if mtime < cutoff:
                os.remove(filepath)
                print(f"[evolution] 오래된 백업 삭제: {filename}")
        except Exception:
            pass


def rollback_rules(rule_type: str, target_timestamp: str) -> bool:
    """
    특정 시점의 규칙으로 롤백.

    Args:
        rule_type: "classification" 또는 "qa"
        target_timestamp: 백업 파일의 타임스탬프 (예: "2025-01-15T12-30-45Z")

    Returns:
        True if 성공
    """
    backup_path = os.path.join(_HISTORY_DIR, f"{rule_type}_{target_timestamp}.json")

    if not os.path.exists(backup_path):
        raise FileNotFoundError(f"백업 파일 없음: {backup_path}")

    # 백업 파일 로드
    with open(backup_path, "r", encoding="utf-8") as f:
        rules = json.load(f)

    # 현재 상태도 백업 (롤백 전)
    current_rules = _load_all_rules(rule_type)
    _backup_rules(current_rules, f"{rule_type}_pre_rollback")

    # 롤백 적용
    _save_rules(rules, rule_type)
    reload_rules()

    print(f"[evolution] 롤백 완료: {rule_type} -> {target_timestamp}")
    return True


def list_backups(rule_type: str) -> list[str]:
    """
    사용 가능한 백업 목록 반환.

    Returns:
        타임스탬프 목록 (최신순)
    """
    if not os.path.exists(_HISTORY_DIR):
        return []

    prefix = f"{rule_type}_"
    backups = []

    for filename in os.listdir(_HISTORY_DIR):
        if filename.startswith(prefix) and filename.endswith(".json"):
            timestamp = filename[len(prefix):-5]  # 접두사와 .json 제거
            backups.append(timestamp)

    return sorted(backups, reverse=True)
