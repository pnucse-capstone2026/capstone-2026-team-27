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
        self.decision_rules: list[Rule] = []
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

        # Decision 규칙 로드
        dec_path = os.path.join(self._rules_dir, "decision_rules.json")
        if os.path.exists(dec_path):
            with open(dec_path, "r", encoding="utf-8") as f:
                rules = json.load(f)
                self.decision_rules = [r for r in rules if r.get("enabled", True)]
                self.decision_rules.sort(key=lambda r: r.get("priority", 999))

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
            # 시작 체크 (이벤트 기간처럼 미래에 시작하는 항목을 미리 등록해둘 수 있게)
            effective_from = entry.get("effective_from")
            if effective_from:
                try:
                    start_dt = datetime.fromisoformat(effective_from.replace("Z", "+00:00"))
                    if now < start_dt:
                        continue  # 아직 시작 전
                except ValueError:
                    pass

            # 만료 체크
            expires_at = entry.get("expires_at")
            if expires_at:
                try:
                    expire_dt = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
                    if now > expire_dt:
                        continue  # 만료됨
                except ValueError:
                    pass  # 파싱 실패 시 무시

            # 2026-09-13 추가: category="recurring_hours"면 effective_from/expires_at
            # (날짜 범위)과 별개로, "매일 이 시:분 사이"인지도 확인한다(예: 매일 22~06시
            # 야간). daily_end_hour < daily_start_hour면 자정을 넘기는 구간으로 취급.
            if entry.get("category") == "recurring_hours":
                start_h = entry.get("daily_start_hour")
                end_h = entry.get("daily_end_hour")
                if start_h is not None and end_h is not None:
                    current_h = now.hour
                    if start_h <= end_h:
                        in_window = start_h <= current_h < end_h
                    else:
                        in_window = current_h >= start_h or current_h < end_h  # 자정 넘김
                    if not in_window:
                        continue  # 지금은 그 시간대가 아님

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

    def match_decision_rules(self, state: PipelineState) -> Optional[Rule]:
        """우선순위 순으로 매칭되는 첫 번째 Decision 규칙 반환"""
        resource_type = state.get("resource_type", "")
        anomaly_type = state.get("anomaly_type", "")

        for rule in self.decision_rules:
            # 리소스 타입 체크
            rule_types = rule.get("resource_types", ["*"])
            if "*" not in rule_types and resource_type not in rule_types:
                continue

            # anomaly_type 조건 체크
            conditions = rule.get("conditions", {})
            rule_anomaly_type = conditions.get("anomaly_type")
            if rule_anomaly_type and rule_anomaly_type != anomaly_type:
                continue

            # 기타 조건 평가
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

        # sustained_fraction 조건 (EDoS vs 단순 인기 폭증 구분용, 2026-09-11)
        # "진짜 인기 폭증은 몇 시간 안에 꺾이고, 공격은 계속 유지된다"는 가정 하에,
        # latest 시점 하나만 튄 게 아니라 최근 구간 대부분이 계속 높게 유지되는지 확인한다.
        sustained_fraction = conditions.get("sustained_fraction")
        if sustained_fraction:
            if not self._evaluate_sustained_fraction(sustained_fraction, state):
                return False

        # skip_if_whitelisted 조건 (이벤트 기간 등록용, 2026-09-11)
        # "요청자 집중"/"에러 동반"은 인프라가 없어 스코프 밖이지만, "알려진 이벤트
        # 기간인지"는 whitelist.json에 category="event_period"로 미리 등록해두는
        # 것으로 대체한다 — 세일 등으로 예정된 트래픽 증가를 EDoS로 오탐하지 않게 함.
        # ⚠️ 화이트리스트엔 "개발서버 제외" 같은 이벤트와 무관한 항목도 있으므로,
        # category가 정확히 "event_period"인 항목에 매칭될 때만 예외 처리한다 —
        # 화이트리스트 매칭 여부 자체만으로 판단하지 않는다.
        if conditions.get("skip_if_whitelisted"):
            resource_id = state.get("resource_id", "")
            resource_type = state.get("resource_type", "")
            is_wl, wl_entry = self.is_whitelisted(resource_id, resource_type)
            if is_wl and wl_entry and wl_entry.get("category") == "event_period":
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

        # ec2_utilization_band 조건 (EC2 좀비 vs 오버프로비저닝 구분용, Decision 전용)
        band_cond = conditions.get("ec2_utilization_band")
        if band_cond is not None:
            if state.get("ec2_utilization_band") != band_cond:
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

    def _evaluate_sustained_fraction(self, spec: dict, state: PipelineState) -> bool:
        """최근 구간 대부분이 기준선 대비 계속 높게 유지되는지 확인 (EDoS vs 단순 인기 폭증 구분).

        latest 한 시점만 튀었다가 이미 꺾인 경우(단순 인기 폭증)와, 최근 구간 내내
        높게 유지되는 경우(EDoS 의심)를 구분한다. 윈도우를 baseline 구간(앞부분)과
        recent 구간(뒷부분)으로 나눠서, baseline 평균 대비 factor배를 넘는 지점이
        recent 구간에서 min_fraction 이상 차지해야 통과한다.

        spec 예시: {"metric": "group_desired_capacity", "factor": 2.0,
                   "min_fraction": 0.6, "recent_window_ratio": 0.2, "min_recent_points": 3}
        """
        raw_metrics = state.get("raw_metrics", {})
        metric_name = spec.get("metric")
        values = raw_metrics.get(metric_name, [])

        factor = spec.get("factor", 2.0)
        min_fraction = spec.get("min_fraction", 0.5)
        recent_ratio = spec.get("recent_window_ratio", 0.2)
        min_recent_points = spec.get("min_recent_points", 3)

        n_recent = max(min_recent_points, round(len(values) * recent_ratio))
        if len(values) <= n_recent:
            return False  # baseline 구간이 없으면 판단 불가

        baseline_values = values[:-n_recent]
        recent_values = values[-n_recent:]
        baseline_mean = sum(baseline_values) / len(baseline_values)
        if baseline_mean <= 0:
            return False  # 기준선이 0이면 배율 비교 자체가 의미 없음

        threshold = baseline_mean * factor
        above_count = sum(1 for v in recent_values if v > threshold)
        return (above_count / len(recent_values)) >= min_fraction

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
