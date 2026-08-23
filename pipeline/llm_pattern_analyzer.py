"""
LLM Pattern Analyzer
---------------------
classification_agent.py / QA_agent.py에서 실제로 LLM(Gemini)이 내린 판단들을 모아,
"LLM이 대체로 어떤 입력 패턴에서 어떻게 판단해왔는지"를 decision tree pseudocode로
보여주는 분석 도구.

왜 필요한가
-----------
LLM 자체는 "이 조건이면 이렇게 판단한다"는 명시적 경계가 없는 블랙박스라, 쌓인 판단
기록만 봐서는 그 안에 어떤 패턴이 있는지 사람이 한눈에 파악하기 어렵다. 이 스크립트는
schema/logs/llm_classification_log.jsonl에 이미 쌓여 있는 (입력 지표 → LLM 판단) 기록을
학습 데이터 삼아 얕은 DecisionTreeClassifier를 학습시키고, 그 트리를 pseudocode로
출력한다 — "LLM이 지금까지 이런 식으로 판단해왔다"를 사람이 읽을 수 있는 형태로 보여주는
것이 유일한 목적이다.

이 스크립트는 규칙을 rule-book에 자동으로 쓰지 않는다 (순수 확인/분석용).
반복되는 classification 패턴을 실제로 rule-book에 반영하려면 이미 있는
pipeline/rule_promoter.py를 쓰면 된다.

대상
----
- classification: 입력 지표(metrics_summary) → anomaly_type
  (이 로그 파일의 모든 엔트리는 classification_agent.py가 Rule Book 매칭에 실패해
   LLM에 위임한 경우에만 기록되므로, 별도 필터링 없이 그 자체로 "LLM 판단" 데이터다)
- qa: 입력(action_executed, anomaly_type, 지표) → qa_passed
  (QA_agent.py도 Rule Book 매칭 실패 시에만 LLM에 위임하므로, 그중에서도
   qa_result.qa_matched_rule_id가 없는 엔트리만 "진짜 LLM 판단"으로 취급한다 —
   QA_agent가 이 로그 파일에 qa_result를 덧붙일 때는 QA 규칙이 적용된 경우도
   포함되기 때문에, 그 경우를 골라내야 순수한 LLM 판단만 남는다)

사용법
------
    python -m pipeline.llm_pattern_analyzer                      # classification + qa 둘 다
    python -m pipeline.llm_pattern_analyzer --target classification
    python -m pipeline.llm_pattern_analyzer --target qa
    python -m pipeline.llm_pattern_analyzer --max-depth 3 --min-samples 8
"""

from __future__ import annotations

import argparse
from collections import Counter
from typing import Optional

import numpy as np
from sklearn.tree import DecisionTreeClassifier, export_text

from pipeline.rule_promoter import load_llm_logs

DEFAULT_MAX_DEPTH = 4
DEFAULT_MIN_SAMPLES = 8  # 이보다 적으면 트리를 학습해도 신뢰할 수 없어 분석을 건너뜀
RANDOM_STATE = 42


# ── 로그 → feature 행렬 변환 ──────────────────────────────────────────────────

def _flatten_metrics_summary(metrics_summary: dict, all_metrics: list[str]) -> list[float]:
    """
    로그의 metrics_summary({"cost": {"latest":.., "mean":..}, ...})를 고정된 컬럼
    순서의 숫자 벡터로 변환. 로그마다 resource_type이 달라 등장하는 지표가 다르므로,
    "이 로그에 그 지표가 있었는지" presence flag도 함께 넣어 리소스 타입 간
    비교가 자연스럽게 되게 한다 (지표가 없으면 0으로 채우고 present=0).
    """
    row: list[float] = []
    for m in all_metrics:
        entry = metrics_summary.get(m)
        if entry is None:
            row.extend([0.0, 0.0, 0.0])  # latest, mean, present
        else:
            row.extend([float(entry.get("latest", 0.0)), float(entry.get("mean", 0.0)), 1.0])
    return row


def _feature_names(all_metrics: list[str], extra: list[str]) -> list[str]:
    names: list[str] = []
    for m in all_metrics:
        names.extend([f"{m}_latest", f"{m}_mean", f"{m}_present"])
    names.extend(extra)
    return names


class AnalysisResult:
    def __init__(self, target_name: str, n_samples: int, class_counts: Counter,
                 tree: Optional[DecisionTreeClassifier], pseudocode: Optional[str],
                 self_accuracy: Optional[float], skip_reason: Optional[str]):
        self.target_name = target_name
        self.n_samples = n_samples
        self.class_counts = class_counts
        self.tree = tree
        self.pseudocode = pseudocode
        self.self_accuracy = self_accuracy
        self.skip_reason = skip_reason  # None이면 분석 정상 수행됨


# ── classification LLM 판단 분석 ─────────────────────────────────────────────

def analyze_classification(
    logs: list[dict], max_depth: int = DEFAULT_MAX_DEPTH, min_samples: int = DEFAULT_MIN_SAMPLES,
) -> AnalysisResult:
    """
    classification_agent.py가 Rule Book 매칭 실패로 LLM에 위임한 판단들
    (이 로그 파일의 모든 엔트리)을 대상으로, "지표 패턴 → anomaly_type" 결정 트리를 학습.
    """
    samples = [
        e for e in logs
        if e.get("output", {}).get("anomaly_type") is not None
    ]

    all_metrics = sorted({
        m for e in samples for m in e.get("input", {}).get("metrics_summary", {}).keys()
    })
    resource_types = sorted({e["input"]["resource_type"] for e in samples if e.get("input", {}).get("resource_type")})

    y = [e["output"]["anomaly_type"] for e in samples]
    class_counts = Counter(y)

    if len(samples) < min_samples:
        return AnalysisResult(
            "classification", len(samples), class_counts, None, None, None,
            f"로그가 {len(samples)}건뿐 (최소 {min_samples}건 필요) — 파이프라인을 더 돌려 "
            f"LLM 판단이 쌓인 뒤 다시 실행하세요.",
        )
    if len(class_counts) < 2:
        return AnalysisResult(
            "classification", len(samples), class_counts, None, None, None,
            f"지금까지 LLM이 항상 같은 anomaly_type({next(iter(class_counts))})으로만 판단해서 "
            f"트리를 학습할 경계가 없습니다 (전부 한 클래스). 다른 유형의 이상 케이스가 "
            f"들어오면 다시 실행해보세요.",
        )

    extra_names = [f"is_{rt}" for rt in resource_types]
    X = []
    for e in samples:
        row = _flatten_metrics_summary(e["input"].get("metrics_summary", {}), all_metrics)
        rt = e["input"].get("resource_type")
        row.extend([1.0 if rt == r else 0.0 for r in resource_types])
        X.append(row)
    X = np.asarray(X, dtype=float)

    tree = DecisionTreeClassifier(
        max_depth=max_depth, min_samples_leaf=max(2, len(samples) // 20),
        class_weight="balanced", random_state=RANDOM_STATE,
    )
    tree.fit(X, y)
    self_accuracy = float((tree.predict(X) == np.asarray(y)).mean())

    feature_names = _feature_names(all_metrics, extra_names)
    pseudocode = export_text(tree, feature_names=feature_names, decimals=3)

    return AnalysisResult("classification", len(samples), class_counts, tree, pseudocode, self_accuracy, None)


# ── QA LLM 판단 분석 ──────────────────────────────────────────────────────────

def analyze_qa(
    logs: list[dict], max_depth: int = DEFAULT_MAX_DEPTH, min_samples: int = DEFAULT_MIN_SAMPLES,
) -> AnalysisResult:
    """
    QA_agent.py가 Rule Book 매칭 실패로 LLM에 위임해 SLA를 판단한 케이스만 골라
    (qa_result.qa_matched_rule_id가 없는 것만 — 있으면 QA 규칙이 적용된 것이라 LLM
    판단이 아님), "액션/이상유형/지표 → qa_passed" 결정 트리를 학습.
    """
    samples = [
        e for e in logs
        if e.get("qa_result") is not None and e["qa_result"].get("qa_matched_rule_id") is None
        and e["qa_result"].get("qa_passed") is not None
    ]

    all_metrics = sorted({
        m for e in samples for m in e.get("input", {}).get("metrics_summary", {}).keys()
    })
    resource_types = sorted({e["input"]["resource_type"] for e in samples if e.get("input", {}).get("resource_type")})
    anomaly_types = sorted({e["output"]["anomaly_type"] for e in samples if e.get("output", {}).get("anomaly_type")})

    y = [bool(e["qa_result"]["qa_passed"]) for e in samples]
    class_counts = Counter(y)

    if len(samples) < min_samples:
        return AnalysisResult(
            "qa", len(samples), class_counts, None, None, None,
            f"'LLM이 직접 판단한' QA 로그가 {len(samples)}건뿐 (최소 {min_samples}건 필요). "
            f"지금 쌓인 QA 로그는 대부분 Rule Book(QA-XXX)으로 처리돼서 LLM 판단 표본이 "
            f"부족합니다 — Rule Book이 커버 못 하는 케이스가 쌓이면 다시 실행해보세요.",
        )
    if len(class_counts) < 2:
        verdict = "통과" if next(iter(class_counts)) else "실패"
        return AnalysisResult(
            "qa", len(samples), class_counts, None, None, None,
            f"지금까지 LLM이 항상 '{verdict}'로만 판단해서 트리를 학습할 경계가 없습니다.",
        )

    extra_names = [f"is_{rt}" for rt in resource_types] + [f"anomaly_{a}" for a in anomaly_types]
    X = []
    for e in samples:
        row = _flatten_metrics_summary(e["input"].get("metrics_summary", {}), all_metrics)
        rt = e["input"].get("resource_type")
        row.extend([1.0 if rt == r else 0.0 for r in resource_types])
        a = e.get("output", {}).get("anomaly_type")
        row.extend([1.0 if a == at else 0.0 for at in anomaly_types])
        X.append(row)
    X = np.asarray(X, dtype=float)

    tree = DecisionTreeClassifier(
        max_depth=max_depth, min_samples_leaf=max(2, len(samples) // 20),
        class_weight="balanced", random_state=RANDOM_STATE,
    )
    tree.fit(X, y)
    self_accuracy = float((tree.predict(X) == np.asarray(y)).mean())

    feature_names = _feature_names(all_metrics, extra_names)
    pseudocode = export_text(tree, feature_names=feature_names, decimals=3)

    return AnalysisResult("qa", len(samples), class_counts, tree, pseudocode, self_accuracy, None)


# ── 리포트 출력 ────────────────────────────────────────────────────────────────

def _print_report(result: AnalysisResult) -> None:
    label = "Classification Agent" if result.target_name == "classification" else "QA Agent"
    print("\n" + "=" * 78)
    print(f"[{label}] LLM 판단 패턴 분석")
    print("=" * 78)
    print(f"  분석에 쓰인 판단 건수 : {result.n_samples}")
    print(f"  판단 분포            : {dict(result.class_counts)}")

    if result.skip_reason is not None:
        print(f"\n[분석 보류] {result.skip_reason}")
        return

    print(f"  트리가 로그를 재현하는 정도(자기적합도) : {result.self_accuracy:.4f}")
    print(
        "\n[Pseudocode] (LLM 자체가 아니라, 지금까지의 LLM 판단 로그를 흉내내도록 "
        "학습된 Decision Tree — 'LLM이 대체로 이렇게 판단해왔다'는 패턴)"
    )
    print(result.pseudocode)
    print(
        "[참고] 표본 수가 적으면 이 패턴은 우연일 수 있습니다. 판단 로그가 더 쌓인 뒤 "
        "다시 실행해서 패턴이 유지되는지 확인하는 것을 권장합니다."
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="classification_agent/QA_agent의 LLM 판단 로그를 decision tree "
        "pseudocode로 분석한다 (rule-book에 자동 반영하지 않음, 확인/분석 전용)."
    )
    parser.add_argument("--target", choices=["classification", "qa", "both"], default="both")
    parser.add_argument("--max-depth", type=int, default=DEFAULT_MAX_DEPTH)
    parser.add_argument("--min-samples", type=int, default=DEFAULT_MIN_SAMPLES)
    args = parser.parse_args()

    logs = load_llm_logs()
    if not logs:
        print("[llm_pattern_analyzer] 분석할 로그가 없습니다 (schema/logs/llm_classification_log.jsonl 비어있음).")
        return

    if args.target in ("classification", "both"):
        _print_report(analyze_classification(logs, args.max_depth, args.min_samples))
    if args.target in ("qa", "both"):
        _print_report(analyze_qa(logs, args.max_depth, args.min_samples))


if __name__ == "__main__":
    main()
