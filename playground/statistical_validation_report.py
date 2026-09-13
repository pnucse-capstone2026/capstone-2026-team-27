"""이미 저장된 시나리오별 실험 결과 JSON에서 재실행 없이 통계 검증 지표를 계산한다.

계산하는 것:
- Clopper-Pearson 95% 신뢰구간 (accuracy/recall/precision/FPR) — 저장돼 있으면 그대로,
  없으면 confusion matrix로부터 재계산.
- 이항검정(binomial test): recall이 "우연(50%)"보다 유의하게 높은지, FPR이 "우연"보다
  유의하게 낮은지.
- McNemar 정확검정: 같은 시나리오 안에 탐지 방식이 여럿 저장돼 있으면(예: production vs
  iforest_only vs zscore_only), 방식 쌍마다 "한쪽만 맞춘 샘플 수(불일치 쌍)"를 가지고
  양측 이항검정으로 두 방식의 성능 차이가 우연인지 확인한다. (statsmodels 없이 scipy만으로
  계산하는 정확 McNemar 검정 — 불일치 쌍 b, c에 대해 binomtest(min(b,c), b+c, 0.5)와 동치.)

시나리오/파일 목록은 SCENARIOS에서 관리한다. 새 실험 결과가 나오면 여기에 한 줄만 추가하면 됨
(예: v6 전체 파이프라인 n=13 결과).

사용법:
    python playground/statistical_validation_report.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

from scipy.stats import beta as _beta_dist
from scipy.stats import binomtest

sys.stdout.reconfigure(encoding="utf-8")

EVAL_DIR = Path(__file__).parent / "eval_outputs"
OUTPUT_PATH = EVAL_DIR / "statistical_validation_report.json"

# format="unified": 최상위 "trials" 리스트, 각 원소가 label + detected_<method> 키들을 가짐
#   (ec2_repeated_trial류, lambda_repeated_trial류, autoscaling_edos_traffic_trial류)
# format="split": 최상위 "anomaly_trials" + "normal_trials" 두 리스트, 각 원소가
#   label + "detected"(단일 방식) 키를 가짐 (s3_repeated_trial류)
SCENARIOS = [
    {
        "name": "S3 (bytes_downloaded 유출 탐지)",
        "file": "s3_repeated_trial__window2.5h_objsize50kb_n8-5_scriptv5_20260910.json",
        "format": "split",
        "methods": {"detected": "production"},
    },
    {
        "name": "EC2 좀비(사용 안 되는 유휴 인스턴스)",
        "file": "ec2_repeated_trial__n8-5_scriptv1_20260912.json",
        "format": "unified",
        "methods": {
            "detected_production": "production",
            "detected_iforest_only": "iforest_only",
            "detected_teammate_compat": "teammate_compat",
        },
    },
    {
        "name": "EC2 오버프로비저닝",
        "file": "ec2_overprovision_repeated_trial__converted.json",
        "format": "unified",
        "methods": {
            "detected_production": "production",
            "detected_iforest_only": "iforest_only",
            "detected_teammate_compat": "teammate_compat",
        },
    },
    {
        "name": "Lambda (호출/에러 급증)",
        "file": "lambda_repeated_trial__n8-5_scriptv1_20260909.json",
        "format": "unified",
        "methods": {
            "detected_production": "production",
            "detected_iforest_only": "iforest_only",
            "detected_teammate_compat": "teammate_compat",
        },
    },
    {
        "name": "AutoScaling EDoS (request_count 기반, v5)",
        "file": "autoscaling_edos_traffic_trial__n5-5_scriptv4_20260913.json",
        "format": "unified",
        "methods": {
            "detected_production": "production",
            "detected_iforest_only": "iforest_only",
            "detected_zscore_only": "zscore_only",
        },
    },
]


def clopper_pearson_ci(successes: int, n: int, confidence: float = 0.95) -> list[float] | None:
    """양측 Clopper-Pearson 정확 이항 신뢰구간. (실패=0/전체=0인 경우도 안전하게 처리)"""
    if n == 0:
        return None
    alpha = 1 - confidence
    lo = 0.0 if successes == 0 else _beta_dist.ppf(alpha / 2, successes, n - successes + 1)
    hi = 1.0 if successes == n else _beta_dist.ppf(1 - alpha / 2, successes + 1, n - successes)
    return [float(lo), float(hi)]


def load_trials(scenario: dict) -> list[dict]:
    """시나리오 파일을 읽어 [{"label": "anomaly"/"normal", "results": {method_name: bool}}] 형태로 통일."""
    path = EVAL_DIR / scenario["file"]
    data = json.loads(path.read_text(encoding="utf-8"))

    raw_trials: list[dict]
    if scenario["format"] == "unified":
        raw_trials = data["trials"]
    elif scenario["format"] == "split":
        raw_trials = data.get("anomaly_trials", []) + data.get("normal_trials", [])
    else:
        raise ValueError(f"unknown format: {scenario['format']}")

    unified: list[dict] = []
    for t in raw_trials:
        results = {}
        for raw_key, method_name in scenario["methods"].items():
            if raw_key in t:
                results[method_name] = bool(t[raw_key])
        if results:
            unified.append({"label": t["label"], "results": results})
    return unified


def confusion_and_ci(trials: list[dict], method: str) -> dict:
    tp = fn = fp = tn = 0
    for t in trials:
        if method not in t["results"]:
            continue
        is_anomaly = t["label"] == "anomaly"
        detected = t["results"][method]
        if is_anomaly and detected:
            tp += 1
        elif is_anomaly and not detected:
            fn += 1
        elif not is_anomaly and detected:
            fp += 1
        else:
            tn += 1

    n_anomaly = tp + fn
    n_normal = fp + tn
    total = n_anomaly + n_normal
    accuracy = (tp + tn) / total if total else None
    recall = tp / n_anomaly if n_anomaly else None
    precision = tp / (tp + fp) if (tp + fp) else None
    fpr = fp / n_normal if n_normal else None

    result = {
        "confusion_matrix": {"TP": tp, "FN": fn, "FP": fp, "TN": tn},
        "n_anomaly": n_anomaly,
        "n_normal": n_normal,
        "accuracy": accuracy,
        "accuracy_ci_95": clopper_pearson_ci(tp + tn, total),
        "recall": recall,
        "recall_ci_95": clopper_pearson_ci(tp, n_anomaly),
        "precision": precision,
        "false_positive_rate": fpr,
        "fpr_ci_95": clopper_pearson_ci(fp, n_normal),
    }

    # 이항검정: recall이 "동전 던지기(50%)"보다 유의하게 높은가?
    if n_anomaly:
        bt = binomtest(tp, n_anomaly, 0.5, alternative="greater")
        result["recall_vs_chance_binomial_test"] = {
            "p_value": float(bt.pvalue),
            "significant_at_0.05": bool(bt.pvalue < 0.05),
            "interpretation": "recall이 우연(50%)보다 유의하게 높음" if bt.pvalue < 0.05
            else "표본이 작아 우연과 통계적으로 구분 안 됨(유의하지 않음)",
        }
    # 이항검정: FPR이 "동전 던지기(50%)"보다 유의하게 낮은가? (오탐이 우연보다 적은지)
    if n_normal:
        bt = binomtest(fp, n_normal, 0.5, alternative="less")
        result["fpr_vs_chance_binomial_test"] = {
            "p_value": float(bt.pvalue),
            "significant_at_0.05": bool(bt.pvalue < 0.05),
            "interpretation": "오탐률이 우연(50%)보다 유의하게 낮음" if bt.pvalue < 0.05
            else "표본이 작아 우연과 통계적으로 구분 안 됨(유의하지 않음)",
        }
    return result


def mcnemar_exact(trials: list[dict], method_a: str, method_b: str) -> dict | None:
    """정확(exact) McNemar 검정. 불일치 쌍(b, c)에 대해 binomtest(min(b,c), b+c, 0.5)와 동치.
    b = A만 맞춘 샘플 수, c = B만 맞춘 샘플 수.
    """
    b = c = 0
    n_compared = 0
    for t in trials:
        if method_a not in t["results"] or method_b not in t["results"]:
            continue
        n_compared += 1
        is_anomaly = t["label"] == "anomaly"
        correct_a = t["results"][method_a] == is_anomaly
        correct_b = t["results"][method_b] == is_anomaly
        if correct_a and not correct_b:
            b += 1
        elif not correct_a and correct_b:
            c += 1

    if n_compared == 0:
        return None

    discordant = b + c
    if discordant == 0:
        return {
            "n_compared": n_compared,
            "b_only_a_correct": b,
            "c_only_b_correct": c,
            "p_value": None,
            "note": "두 방식이 모든 샘플에서 동일하게 판정함(불일치 쌍 없음) — 검정 불가",
        }

    p_value = float(binomtest(min(b, c), discordant, 0.5, alternative="two-sided").pvalue)
    return {
        "n_compared": n_compared,
        "b_only_a_correct": b,
        "c_only_b_correct": c,
        "p_value": p_value,
        "significant_at_0.05": p_value < 0.05,
        "interpretation": (
            f"{method_a} vs {method_b}: 두 방식의 정답률 차이가 통계적으로 유의함"
            if p_value < 0.05
            else f"{method_a} vs {method_b}: 표본이 작아 두 방식 차이가 우연과 통계적으로 구분 안 됨"
        ),
    }


def main() -> None:
    report: dict = {"scenarios": {}}

    for scenario in SCENARIOS:
        path = EVAL_DIR / scenario["file"]
        if not path.exists():
            print(f"[건너뜀] 파일 없음: {scenario['file']}")
            continue

        trials = load_trials(scenario)
        methods = sorted({m for t in trials for m in t["results"]})
        print(f"\n=== {scenario['name']} (n={len(trials)}, 방식={methods}) ===")

        scenario_report: dict = {"file": scenario["file"], "n_trials": len(trials), "methods": {}}

        for method in methods:
            m = confusion_and_ci(trials, method)
            scenario_report["methods"][method] = m
            cm = m["confusion_matrix"]
            print(f"  [{method}] TP={cm['TP']} FN={cm['FN']} FP={cm['FP']} TN={cm['TN']}")
            if m["accuracy"] is not None:
                print(f"    accuracy={m['accuracy']*100:.1f}% CI={m['accuracy_ci_95']}")
            if m["recall"] is not None:
                print(f"    recall={m['recall']*100:.1f}% CI={m['recall_ci_95']}")
                bt = m.get("recall_vs_chance_binomial_test")
                if bt:
                    print(f"    recall vs 우연(50%) 이항검정 p={bt['p_value']:.4f} -> {bt['interpretation']}")
            if m.get("fpr_vs_chance_binomial_test"):
                bt = m["fpr_vs_chance_binomial_test"]
                print(f"    FPR vs 우연(50%) 이항검정 p={bt['p_value']:.4f} -> {bt['interpretation']}")

        if len(methods) >= 2:
            scenario_report["mcnemar"] = {}
            for i in range(len(methods)):
                for j in range(i + 1, len(methods)):
                    a, b = methods[i], methods[j]
                    result = mcnemar_exact(trials, a, b)
                    if result is None:
                        continue
                    scenario_report["mcnemar"][f"{a}_vs_{b}"] = result
                    print(f"  [McNemar] {a} vs {b}: b={result['b_only_a_correct']} c={result['c_only_b_correct']} "
                          f"p={result['p_value']}")

        report["scenarios"][scenario["name"]] = scenario_report

    report["notes"] = {
        "hyperparameter_tuning": (
            "playground/eval_outputs/phase5_tuning_results.json에 IsolationForest "
            "contamination/tau/k 그리드서치 결과 있음(mock/시뮬레이션 데이터 기반, 247개 조합). "
            "실제 AWS 트래픽 기반 실험(v1~v6)에서는 이 튜닝된 값을 참고해 임계값을 실측 기반으로 "
            "수동 조정함 — 정식 그리드서치는 아님."
        ),
        "train_eval_independence_caveat": (
            "detection_agent.py의 _get_or_train_iforest()는 온라인 학습 구조라, '정상으로 판단된' "
            "윈도우가 즉시 훈련 버퍼에 편입되고 모델이 재학습된다. EDoS 실험 패턴상 같은 리소스의 "
            "베이스라인(before) 구간이 먼저 버퍼에 들어간 뒤, 그 리소스의 공격 후(after) 구간을 "
            "채점하므로 완전한 훈련/평가 데이터 독립은 아니다. 온라인 학습 설계상 의도된 부분이지만 "
            "보고서에 한계점으로 명시 필요."
        ),
        "mcnemar_method": (
            "statsmodels 미설치 환경이라 scipy binomtest만으로 계산되는 정확(exact) McNemar 검정을 "
            "사용함: 불일치 쌍 b, c에 대해 binomtest(min(b,c), b+c, p=0.5, alternative='two-sided')."
        ),
    }

    OUTPUT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n결과 저장: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
