"""
Rule Engine
-----------
Rule Book 기반 규칙 로딩, 매칭, 평가 엔진
"""

import json
import os
import fnmatch
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

from schema.rule_book import Rule, WhitelistEntry, TimeWindow
from schema.state import PipelineState


class RuleEngine:
    """Rule Book 기반 규칙 엔진"""

    def __init__(self):
        self.classification_rules: list[Rule] = []
        self.qa_rules: list[Rule] = []
        self.whitelist: list[WhitelistEntry] = []
        self._rules_dir = os.path.join(os.path.dirname(__file__), "..", "schema", "rules")
        self.load_rules()

    def load_rules(self) -> None:
        """JSON 파일에서 규칙 로드"""
        # Classification 규칙 로드
        clf_path = os.path.join(self._rules_dir, "classification_rules.json")
        if os.path.exists(clf_path):
            with open(clf_path, "r", encoding="utf-8") as f:
                rules = json.load(f)
                self.classification_rules = [r for r in rules if r.get("enabled", True)]
                # 우선순위 순 정렬 (낮을수록 먼저)
                self.classification_rules.sort(key=lambda r: r.get("priority", 999))

        # QA 규칙 로드
        qa_path = os.path.join(self._rules_dir, "qa_rules.json")
        if os.path.exists(qa_path):
            with open(qa_path, "r", encoding="utf-8") as f:
                rules = json.load(f)
                self.qa_rules = [r for r in rules if r.get("enabled", True)]
                self.qa_rules.sort(key=lambda r: r.get("priority", 999))

        # 화이트리스트 로드
        wl_path = os.path.join(self._rules_dir, "whitelist.json")
        if os.path.exists(wl_path):
            with open(wl_path, "r", encoding="utf-8") as f:
                self.whitelist = json.load(f)

    def is_whitelisted(self, resource_id: str, resource_type: str) -> tuple[bool, Optional[WhitelistEntry]]:
        """
        화이트리스트 체크 (Glob 패턴 매칭 지원)

        Returns:
            (is_whitelisted, matched_entry)
        """
        now = datetime.now(ZoneInfo("UTC"))

        for entry in self.whitelist:
            # 만료 체크
            expires_at = entry.get("expires_at")
            if expires_at:
                try:
                    expire_dt = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
                    if now > expire_dt:
                        continue  # 만료됨
                except ValueError:
                    pass  # 파싱 실패 시 무시

            # 리소스 타입 체크
            entry_type = entry.get("resource_type")
            if entry_type and entry_type != resource_type:
                continue

            # 리소스 ID 패턴 매칭 (fnmatch 사용)
            pattern = entry.get("resource_id", "")
            if fnmatch.fnmatch(resource_id, pattern):
                return True, entry

        return False, None

    def match_classification_rules(self, state: PipelineState) -> Optional[Rule]:
        """우선순위 순으로 매칭되는 첫 번째 Classification 규칙 반환"""
        resource_type = state.get("resource_type", "")

        for rule in self.classification_rules:
            # 리소스 타입 체크
            rule_types = rule.get("resource_types", [])
            if "*" not in rule_types and resource_type not in rule_types:
                continue

            # 조건 평가
            if self.evaluate_conditions(rule, state):
                return rule

        return None

    def match_qa_rules(self, state: PipelineState) -> Optional[Rule]:
        """우선순위 순으로 매칭되는 첫 번째 QA 규칙 반환"""
        resource_type = state.get("resource_type", "")

        for rule in self.qa_rules:
            # 리소스 타입 체크
            rule_types = rule.get("resource_types", ["*"])
            if "*" not in rule_types and resource_type not in rule_types:
                continue

            # 조건 평가
            if self.evaluate_conditions(rule, state):
                return rule

        return None

    def evaluate_conditions(self, rule: Rule, state: PipelineState) -> bool:
        """규칙 조건 평가"""
        conditions = rule.get("conditions", {})

        # 조건이 없으면 항상 매칭
        if not conditions:
            return True

        # triggered_metrics 조건
        triggered_metrics_cond = conditions.get("triggered_metrics")
        if triggered_metrics_cond:
            state_metrics = set(state.get("triggered_metrics", []))

            # triggered_metrics가 비어있으면 raw_metrics에서 급증 지표 추출
            # (IForest만으로 탐지된 경우 Z-score 기반 triggered_metrics가 비어있음)
            if not state_metrics:
                state_metrics = self._extract_spike_metrics(state.get("raw_metrics", {}))

            cond_metrics = set(triggered_metrics_cond)
            if not cond_metrics.issubset(state_metrics):
                return False

        # metric_thresholds 조건
        metric_thresholds = conditions.get("metric_thresholds")
        if metric_thresholds:
            if not self._evaluate_metric_thresholds(metric_thresholds, state):
                return False

        # time_window 조건
        time_window = conditions.get("time_window")
        if time_window:
            if not self.check_time_window(time_window):
                return False

        # action_executed 조건 (QA용)
        action_executed_cond = conditions.get("action_executed")
        if action_executed_cond is not None:
            action_executed = state.get("action_executed")
            # null을 None으로 변환하여 비교
            normalized_cond = [None if x is None or x == "null" else x for x in action_executed_cond]
            if action_executed not in normalized_cond:
                return False

        return True

    def _extract_spike_metrics(self, raw_metrics: dict, threshold: float = 2.0) -> set[str]:
        """
        raw_metrics에서 latest가 mean 대비 threshold배 이상인 지표들을 추출.
        triggered_metrics가 비어있을 때 (IForest만으로 탐지된 경우) 대체용.

        Args:
            raw_metrics: {"metric_name": [v1, v2, ...], ...}
            threshold: latest/mean >= threshold 이면 급증으로 판단

        Returns:
            급증 지표 이름들의 set
        """
        spike_metrics = set()
        for metric_name, values in raw_metrics.items():
            if not isinstance(values, list) or not values:
                continue
            latest = values[-1]
            mean = sum(values) / len(values)
            if mean > 0 and latest / mean >= threshold:
                spike_metrics.add(metric_name)
        return spike_metrics

    def _evaluate_metric_thresholds(self, thresholds: dict, state: PipelineState) -> bool:
        """지표 임계값 조건 평가"""
        raw_metrics = state.get("raw_metrics", {})

        for metric_name, threshold_spec in thresholds.items():
            values = raw_metrics.get(metric_name, [])
            if not values:
                return False

            latest = values[-1] if values else 0
            mean = sum(values) / len(values) if values else 0

            op = threshold_spec.get("op", ">")
            ref = threshold_spec.get("ref")  # "mean", "latest", 또는 None
            value = threshold_spec.get("value")
            factor = threshold_spec.get("factor", 1.0)

            # 비교 대상 결정
            if ref == "mean":
                compare_value = mean * factor
            elif ref == "latest":
                compare_value = latest * factor
            elif value is not None:
                compare_value = value
            else:
                return False

            # 연산자 평가
            if op == ">":
                if not (latest > compare_value):
                    return False
            elif op == ">=":
                if not (latest >= compare_value):
                    return False
            elif op == "<":
                if not (latest < compare_value):
                    return False
            elif op == "<=":
                if not (latest <= compare_value):
                    return False
            elif op == "==":
                if not (latest == compare_value):
                    return False

        return True

    def check_time_window(self, time_window: TimeWindow) -> bool:
        """시간대 조건 체크"""
        tz_str = time_window.get("timezone", "UTC")
        try:
            tz = ZoneInfo(tz_str)
        except Exception:
            tz = ZoneInfo("UTC")

        now = datetime.now(tz)
        current_hour = now.hour
        current_day = now.strftime("%a").upper()[:3]  # MON, TUE, ...

        # 요일 체크
        days = time_window.get("days", ["*"])
        if "*" not in days and current_day not in days:
            return False

        # 시간 체크
        start_hour = time_window.get("start_hour", 0)
        end_hour = time_window.get("end_hour", 23)

        # 자정을 넘는 경우 처리 (예: 22시~06시)
        if start_hour <= end_hour:
            if not (start_hour <= current_hour <= end_hour):
                return False
        else:
            # 자정 넘김: 22~6 → 22~23 또는 0~6
            if not (current_hour >= start_hour or current_hour <= end_hour):
                return False

        return True

    def format_reasoning(self, rule: Rule, state: PipelineState) -> str:
        """규칙의 reasoning_template을 실제 값으로 포맷팅"""
        template = rule.get("result", {}).get("reasoning_template", "")
        raw_metrics = state.get("raw_metrics", {})

        # 메트릭 요약 계산
        format_vars = {
            "resource_type": state.get("resource_type", ""),
            "resource_id": state.get("resource_id", ""),
            "action_executed": state.get("action_executed", ""),
        }

        # 메트릭별 최신값/평균값 추가
        for metric_name, values in raw_metrics.items():
            if isinstance(values, list) and values:
                format_vars[f"{metric_name}_latest"] = round(values[-1], 3)
                format_vars[f"{metric_name}_mean"] = round(sum(values) / len(values), 3)

        # factor 등 조건에서 사용한 값들
        conditions = rule.get("conditions", {})
        thresholds = conditions.get("metric_thresholds", {})
        for metric_name, spec in thresholds.items():
            if "factor" in spec:
                format_vars["factor"] = spec["factor"]

        try:
            return template.format(**format_vars)
        except KeyError:
            return template


# 싱글톤 인스턴스
_rule_engine: Optional[RuleEngine] = None


def get_rule_engine() -> RuleEngine:
    """RuleEngine 싱글톤 인스턴스 반환"""
    global _rule_engine
    if _rule_engine is None:
        _rule_engine = RuleEngine()
    return _rule_engine


def reload_rules() -> None:
    """규칙 다시 로드 (테스트/런타임 갱신용)"""
    global _rule_engine
    if _rule_engine is not None:
        _rule_engine.load_rules()
