"""보고서용 정량 지표를 한 번에 뽑는 통합 스크립트.

전부 이미 저장된 결과 파일에서 계산한다 — AWS 재호출·재실행 없음.

출력하는 것:
  1. 시나리오별 표본 수(n_anomaly/n_normal), Confusion Matrix, accuracy/recall/precision/FPR
     + 95% CI(Clopper-Pearson), F1-score, 이항검정(recall/FPR vs 우연 50%)
  2. 여러 탐지 방식이 있는 시나리오는 방식 간 McNemar 검정
  3. 파이프라인 전체 성공률(5개 시나리오 confusion matrix 풀링)
  4. 에이전트별(Classification) 정확도 — Rule Book vs LLM 경로 구분
  5. 실행 소요시간(단계별/시나리오별) + MTTD(=Detection 평균)/MTTR(=Detection~Action 평균)
  6. 훈련/평가 데이터 독립성 & 오버피팅 가능성 — 정성적 근거(코드 기반 사실관계)

사용법:
    python playground/full_metrics_report.py
"""
from __future__ import annotations

import json
import sys
import glob
from pathlib import Path

import numpy as np
from scipy.stats import beta as _beta_dist
from scipy.stats import binomtest

sys.stdout.reconfigure(encoding="utf-8")

EVAL_DIR = Path(__file__).parent / "eval_outputs"
OUTPUT_PATH = EVAL_DIR / "full_metrics_report.json"

# ══════════════════════════════════════════════════════════════════════════
# 1~3. 탐지 정확도 (statistical_validation_report.py와 동일 로직, 표준화해서 재사용)
# ══════════════════════════════════════════════════════════════════════════

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
        "name": "AutoScaling EDoS (request_count 기반, v7)",
        "file": "autoscaling_edos_traffic_trial__n8-15_scriptv4_20260913.json",
        "format": "unified",
        "methods": {
            "detected_production": "production",
            "detected_iforest_only": "iforest_only",
            "detected_zscore_only": "zscore_only",
        },
    },
]


def clopper_pearson_ci(successes: int, n: int, confidence: float = 0.95) -> list[float] | None:
    if n == 0:
        return None
    alpha = 1 - confidence
    lo = 0.0 if successes == 0 else _beta_dist.ppf(alpha / 2, successes, n - successes + 1)
    hi = 1.0 if successes == n else _beta_dist.ppf(1 - alpha / 2, successes + 1, n - successes)
    return [float(lo), float(hi)]


def load_trials(scenario: dict) -> list[dict]:
    path = EVAL_DIR / scenario["file"]
    data = json.loads(path.read_text(encoding="utf-8"))
    if scenario["format"] == "unified":
        raw_trials = data["trials"]
    else:
        raw_trials = data.get("anomaly_trials", []) + data.get("normal_trials", [])

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
    f1 = (2 * precision * recall / (precision + recall)) if (precision and recall and (precision + recall) > 0) else None

    result = {
        "confusion_matrix": {"TP": tp, "FN": fn, "FP": fp, "TN": tn},
        "n_anomaly": n_anomaly, "n_normal": n_normal,
        "accuracy": accuracy, "accuracy_ci_95": clopper_pearson_ci(tp + tn, total),
        "recall": recall, "recall_ci_95": clopper_pearson_ci(tp, n_anomaly),
        "precision": precision, "f1_score": f1,
        "false_positive_rate": fpr, "fpr_ci_95": clopper_pearson_ci(fp, n_normal),
    }
    if n_anomaly:
        bt = binomtest(tp, n_anomaly, 0.5, alternative="greater")
        result["recall_vs_chance_binomial_test"] = {
            "p_value": float(bt.pvalue), "significant_at_0.05": bool(bt.pvalue < 0.05),
        }
    if n_normal:
        bt = binomtest(fp, n_normal, 0.5, alternative="less")
        result["fpr_vs_chance_binomial_test"] = {
            "p_value": float(bt.pvalue), "significant_at_0.05": bool(bt.pvalue < 0.05),
        }
    return result


def mcnemar_exact(trials: list[dict], method_a: str, method_b: str) -> dict | None:
    b = c = n_compared = 0
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
        return {"n_compared": n_compared, "b": b, "c": c, "p_value": None}
    p_value = float(binomtest(min(b, c), discordant, 0.5, alternative="two-sided").pvalue)
    return {"n_compared": n_compared, "b": b, "c": c, "p_value": p_value,
            "significant_at_0.05": p_value < 0.05}


def build_detection_section() -> dict:
    section: dict = {"scenarios": {}}
    for scenario in SCENARIOS:
        path = EVAL_DIR / scenario["file"]
        if not path.exists():
            print(f"[건너뜀] 파일 없음: {scenario['file']}")
            continue
        trials = load_trials(scenario)
        methods = sorted({m for t in trials for m in t["results"]})
        print(f"\n=== {scenario['name']} (n={len(trials)}, 방식={methods}) ===")

        sr: dict = {"file": scenario["file"], "n_trials": len(trials), "methods": {}}
        for method in methods:
            m = confusion_and_ci(trials, method)
            sr["methods"][method] = m
            cm = m["confusion_matrix"]
            print(f"  [{method}] n_anomaly={m['n_anomaly']} n_normal={m['n_normal']} "
                  f"(TP={cm['TP']} FN={cm['FN']} FP={cm['FP']} TN={cm['TN']})")
            if m["accuracy"] is not None:
                print(f"    accuracy={m['accuracy']*100:.1f}% recall={m['recall']*100:.1f}% "
                      f"F1={m['f1_score']:.3f}" if m['f1_score'] is not None else "")

        if len(methods) >= 2:
            sr["mcnemar"] = {}
            for i in range(len(methods)):
                for j in range(i + 1, len(methods)):
                    a, bm = methods[i], methods[j]
                    result = mcnemar_exact(trials, a, bm)
                    if result:
                        sr["mcnemar"][f"{a}_vs_{bm}"] = result

        section["scenarios"][scenario["name"]] = sr

    # 풀링(전체 파이프라인 성공률)
    pooled_tp = pooled_fn = pooled_fp = pooled_tn = 0
    included = []
    for name, sr in section["scenarios"].items():
        m = sr["methods"].get("production")
        if m is None:
            continue
        cm = m["confusion_matrix"]
        pooled_tp += cm["TP"]; pooled_fn += cm["FN"]; pooled_fp += cm["FP"]; pooled_tn += cm["TN"]
        included.append(name)
    n_anom, n_norm = pooled_tp + pooled_fn, pooled_fp + pooled_tn
    total = n_anom + n_norm
    acc = (pooled_tp + pooled_tn) / total if total else None
    rec = pooled_tp / n_anom if n_anom else None
    prec = pooled_tp / (pooled_tp + pooled_fp) if (pooled_tp + pooled_fp) else None
    f1 = (2 * prec * rec / (prec + rec)) if (prec and rec and (prec + rec) > 0) else None
    section["pipeline_overall_pooled"] = {
        "scenarios_included": included,
        "confusion_matrix": {"TP": pooled_tp, "FN": pooled_fn, "FP": pooled_fp, "TN": pooled_tn},
        "n_anomaly": n_anom, "n_normal": n_norm,
        "accuracy": acc, "accuracy_ci_95": clopper_pearson_ci(pooled_tp + pooled_tn, total),
        "recall": rec, "recall_ci_95": clopper_pearson_ci(pooled_tp, n_anom),
        "precision": prec, "f1_score": f1,
    }
    print(f"\n=== 파이프라인 전체 성공률(풀링, n={total}) ===")
    print(f"  accuracy={acc*100:.1f}% recall={rec*100:.1f}% F1={f1:.3f}")
    return section


# ══════════════════════════════════════════════════════════════════════════
# 4. 에이전트별(Classification) 정확도 — 기존 *__classification_accuracy.json 그대로 취합
# ══════════════════════════════════════════════════════════════════════════

def build_classification_accuracy_section() -> dict:
    section = {}
    for f in glob.glob(str(EVAL_DIR / "*__classification_accuracy.json")):
        d = json.loads(Path(f).read_text(encoding="utf-8"))
        name = Path(f).stem
        section[name] = d
        print(f"\n=== Classification 정확도: {name} ===")
        print(f"  Rule Book: {d['rulebook']['correct']}/{d['rulebook']['total']} "
              f"(accuracy={d['rulebook']['accuracy']})")
        print(f"  LLM      : {d['llm']['correct']}/{d['llm']['total']} "
              f"(accuracy={d['llm']['accuracy']})")
        print(f"  종합: {d['combined_classification_accuracy']}")
    return section


# ══════════════════════════════════════════════════════════════════════════
# 5. 실행 소요시간 (단계별/전체) + MTTD/MTTR
# ══════════════════════════════════════════════════════════════════════════

TIMING_FILES = (
    list((EVAL_DIR).glob("batch_pipeline_replay__*.json"))
    + [EVAL_DIR / "pipeline_timing_20260910.json", EVAL_DIR / "pipeline_timing_s3_exploratory_20260910.json",
       EVAL_DIR / "3x_real_pipeline_20260910_025234.json"]
)
STAGES = ["detection", "classification", "decision", "action", "qa", "logging", "total"]


def _load_timing_samples() -> list[dict]:
    samples = []
    for f in TIMING_FILES:
        if not f.exists():
            continue
        d = json.loads(f.read_text(encoding="utf-8"))
        if isinstance(d, list):
            items = d
        else:
            items = d.get("raw_results", d.get("results", [d]))
        for it in items:
            t = it.get("timings")
            if t:
                samples.append(t)
    return samples


def build_timing_section() -> dict:
    samples = _load_timing_samples()
    section: dict = {"n_samples": len(samples), "stages": {}}
    print(f"\n=== 실행 소요시간 (n={len(samples)}) ===")
    for s in STAGES:
        vals = np.array([t[s] for t in samples if s in t])
        if len(vals) == 0:
            continue
        section["stages"][s] = {
            "n": len(vals), "mean": float(vals.mean()), "std": float(vals.std()),
            "min": float(vals.min()), "max": float(vals.max()),
        }
        print(f"  {s:15s} n={len(vals):3d} mean={vals.mean():8.2f}s std={vals.std():8.2f}s "
              f"min={vals.min():7.3f}s max={vals.max():8.2f}s")

    # MTTD = Detection 평균, MTTR = detection+classification+decision+action 평균(action까지 도달한 샘플만)
    mttd = section["stages"].get("detection", {}).get("mean")
    mttr_vals = [
        t.get("detection", 0) + t.get("classification", 0) + t.get("decision", 0) + t.get("action", 0)
        for t in samples if "action" in t
    ]
    mttr_mean = float(np.mean(mttr_vals)) if mttr_vals else None
    mttr_median = float(np.median(mttr_vals)) if mttr_vals else None
    section["mttd_seconds"] = mttd
    section["mttr_seconds"] = {"n": len(mttr_vals), "mean": mttr_mean, "median": mttr_median}
    print(f"\n  MTTD(평균 탐지 시간) = {mttd:.3f}초" if mttd is not None else "")
    print(f"  MTTR(탐지~조치 적용) = 평균 {mttr_mean:.2f}초, 중앙값 {mttr_median:.2f}초 (n={len(mttr_vals)})"
          if mttr_mean is not None else "")
    return section


# ══════════════════════════════════════════════════════════════════════════
# 6. 훈련/평가 데이터 독립성 & 오버피팅 — 정성적 근거(코드 사실관계 기반, 재계산 아님)
# ══════════════════════════════════════════════════════════════════════════

QUALITATIVE_NOTES = {
    "train_eval_independence": (
        "detection_agent.py의 _get_or_train_iforest()는 온라인 학습 구조라, '정상으로 판단된' "
        "윈도우가 즉시 훈련 버퍼에 편입되고 모델이 재학습된다. EDoS 실험 패턴상 같은 리소스의 "
        "베이스라인(before) 구간이 먼저 버퍼에 들어간 뒤 그 리소스의 공격 후(after) 구간을 채점하므로, "
        "완전한 훈련/평가 데이터 독립은 아니다. 실제로 v6(2026-09-13)에서 이 구조로 인해 모델이 "
        "'1개 윈도우로만 학습된 좁은 모델' 상태에 갇혀 recall이 80%→0%로 급락하는 사례가 실측으로 "
        "확인되었다(warmup 스크립트로 완화)."
    ),
    "overfitting_risk": (
        "IsolationForest는 온라인 버퍼 크기가 작을 때(특히 1개 윈도우) 그 좁은 분산에 과적합되어, "
        "이후 들어오는 정상 데이터조차 이상으로 오판하는 자기강화적 패턴이 실측으로 확인되었다 "
        "(detection_agent.py:392-397 주석의 'Phase 5, 창 1개 학습 = 정상 32.7% 오탐' 문제와 동일 "
        "메커니즘). MAX_WINDOWS_PER_TYPE(FIFO)로 완화하나 근본적으로 온라인 학습 특유의 리스크다."
    ),
    "sample_size_limitation": (
        "EC2/Lambda/S3 시나리오는 recall 100%가 나왔으나 n_anomaly=5로 작아 개별 시나리오의 "
        "95% CI가 넓다(예: [47.8%, 100%]). 이항검정에서 recall이 우연(50%)보다 유의하려면 최소 "
        "n=5(recall 100% 가정) ~ n=11(recall 80% 가정)이 필요함을 확인하였고, 5개 시나리오를 "
        "풀링(n=62)하면 CI가 크게 좁아짐(86.5~99.0%)을 실측으로 확인하였다."
    ),
    "hyperparameter_tuning": (
        "phase5_tuning_results.json에 IsolationForest contamination/tau/k 그리드서치 결과가 "
        "있음(mock 데이터 기반, 247개 조합). 실제 AWS 실험(v1~v7)에서는 이 값을 참고해 임계값을 "
        "실측 기반으로 수동 조정하였고, 정식 그리드서치를 실제 데이터로 재수행하지는 않았다."
    ),
    "detection_only_vs_full_pipeline_caveat": (
        "S3/EC2좀비/EC2오버프로비저닝/Lambda의 confusion matrix는 탐지(Detection) 단계만의 "
        "정확도이며, 실제 액션 실행 및 QA 검증까지 포함한 전체 파이프라인 성공률이 아니다. "
        "전체 파이프라인 종단 검증은 AutoScaling EDoS(v6/v7, run_full_pipeline_from_metrics)에서만 "
        "실측하였다."
    ),
}


def main() -> None:
    report = {
        "detection_accuracy": build_detection_section(),
        "classification_accuracy_by_agent": build_classification_accuracy_section(),
        "execution_timing": build_timing_section(),
        "qualitative_notes": QUALITATIVE_NOTES,
    }
    OUTPUT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"\n결과 저장: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
