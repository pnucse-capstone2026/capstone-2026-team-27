"""시나리오별 전체 지표를 한 곳에서 계산하는 통합 스크립트.

statistical_validation_report.py(탐지 성능 통계) + full_metrics_report.py(분류 정확도,
실행시간/MTTD·MTTR) + full_pipeline_verification_report.py(액션 성공률/QA 통과율)를
하나로 합친 것 — 계산 로직을 새로 만든 게 아니라 세 스크립트에 흩어져 있던 SCENARIOS
목록과 로직을 한 군데로 모았다.

이렇게 합친 이유: 세 스크립트가 각자 파일 경로를 따로 관리하고 있어서, 한쪽에서
"EC2 좀비" 파일 참조를 잘못 넣어도(2026-09-14 실제로 발생 — ec2_overprovision 재측정
데이터를 EC2 좀비로 착각) 다른 스크립트는 그걸 모르고 넘어갔다. SCENARIOS를 한 곳에서만
관리하면 이런 사고가 구조적으로 줄어든다. 또한 Lambda 스로틀(팀원 PR#40, 파일 포맷이
다름)이 기존 statistical_validation_report.py의 SCENARIOS에는 아예 없어서 지금까지
수동으로 계산했는데, 이번에 정식으로 편입했다.

전부 이미 저장된 결과 파일에서 계산한다 — AWS 재호출·재실행 없음.

출력:
  1. 시나리오별 n_anomaly/n_normal, Confusion Matrix, accuracy/recall/precision/FPR
     + 95% CI(Clopper-Pearson), F1-score
  2. Recall/FPR vs 우연(50%) 이항검정
  3. 여러 탐지 방식이 있으면 방식 간 McNemar 정확검정
  4. 에이전트별 정확도: 탐지/분류(Rule Book·LLM)/액션/QA
  5. 실행 소요시간(에이전트별) + MTTD/MTTR — 시나리오별로 구분해서 집계
  6. 훈련/평가 데이터 독립성 & 오버피팅 — 정성적 근거(시스템 공통, 시나리오별 아님)
  7. 전체 풀링(confusion matrix 합산)

사용법:
    python playground/scenario_full_report.py
"""
from __future__ import annotations

import glob
import json
import sys
from pathlib import Path

import numpy as np
from scipy.stats import beta as _beta_dist
from scipy.stats import binomtest

sys.stdout.reconfigure(encoding="utf-8")

EVAL_DIR = Path(__file__).parent / "eval_outputs"
TEAM_DIR = Path(__file__).parent / "team_results"
OUTPUT_PATH = EVAL_DIR / "scenario_full_report.json"

# ══════════════════════════════════════════════════════════════════════════
# 시나리오 목록 (탐지 원본 파일 + 분류/액션/QA/타이밍 소스를 한 곳에서 관리)
# ══════════════════════════════════════════════════════════════════════════
# detection_format:
#   "unified" — 최상위 "trials" 리스트, 각 원소가 label + detected_<method> 키들
#   "split"   — 최상위 "anomaly_trials"+"normal_trials", 각 원소가 label + "detected"
#   "team"    — 팀원 파일 포맷(Lambda 스로틀 전용): 최상위 "trials", label + 개별
#               게이트 불리언 필드(detected/gate_iforest_triggered/gate_zscore_triggered)

SCENARIOS = [
    {
        "name": "S3 대량다운로드",
        "detection_file": "s3_repeated_trial__window2.5h_objsize50kb_n8-5_scriptv5_20260910.json",
        "detection_format": "split",
        "methods": {"detected": "production"},
        "classification_key": "s3_repeated_trial__window2.5h_objsize50kb_n8-5_scriptv5_20260910__classification_accuracy",
        "pipeline_builder": "s3_block",
        "timing_files": ["3x_real_pipeline_20260910_025234.json"],
    },
    {
        # [버그 수정, 2026-09-14] 예전엔 ec2_repeated_trial__n8-5_scriptv1_20260912.json을
        # 가리켰는데 그건 사실 오버프로비저닝 재측정 데이터였다(같은 인스턴스ID·raw_metrics를
        # ec2_overprovision_repeated_trial__converted.json과 공유하는 것으로 실측 확인).
        "name": "EC2 좀비(유휴 리소스)",
        "detection_file": "ec2_zombie_replay_trial__n8-5_scriptv1_20260910.json",
        "detection_format": "split",
        "methods": {"detected": "production"},
        "classification_key": "ec2_zombie_replay_trial__n8-5_scriptv1_20260910__classification_accuracy",
        "pipeline_builder": None,  # 탐지 전용 replay라 액션/QA 데이터 자체가 없음
        "timing_files": [],
    },
    {
        "name": "EC2 오버프로비저닝",
        "detection_file": "ec2_overprovision_repeated_trial__converted.json",
        "detection_format": "unified",
        "methods": {
            "detected_production": "production",
            "detected_iforest_only": "iforest_only",
            "detected_teammate_compat": "teammate_compat",
        },
        "classification_key": "ec2_overprovision_repeated_trial__converted__classification_accuracy",
        "pipeline_builder": "ec2_overprovisioning",
        # [주의, 2026-09-14] eval_outputs에는 batch_pipeline_replay__EC2_*.json이 6개
        # 있으나(9/10 탐색용 4개 + 9/12 확정본 2개), confusion matrix/액션/QA에 쓰는 실제
        # 검증된 5건짜리 실행분은 이 파일 하나뿐이다. glob으로 전부 잡으면 관계없는
        # 예전 탐색 실행분까지 섞여 표본이 부풀려진다(실측으로 확인, n=78로 과다 집계됨).
        "timing_files": ["batch_pipeline_replay__EC2_20260912_134432.json"],
    },
    {
        "name": "Lambda(구버전, 호출/에러 급증)",
        "detection_file": "lambda_repeated_trial__n8-5_scriptv1_20260909.json",
        "detection_format": "unified",
        "methods": {
            "detected_production": "production",
            "detected_iforest_only": "iforest_only",
            "detected_teammate_compat": "teammate_compat",
        },
        "classification_key": None,  # classification_accuracy 파일 자체가 생성된 적 없음
        "pipeline_builder": None,  # 탐지 전용 replay
        "timing_files": [],  # elapsed_sec(스크립트 총 소요시간)만 있고 에이전트별 분해 없음
    },
    {
        "name": "Lambda 스로틀(신버전, 팀원 PR#40)",
        "detection_file": str(TEAM_DIR / "lambda_throttle" / "clean_verification_20260914.json"),
        "detection_format": "team",
        "methods": {
            "detected": "production",
            "gate_iforest_triggered": "iforest_only",
            "gate_zscore_triggered": "zscore_only",
        },
        "classification_key": "lambda_throttle",  # team_results 쪽 별도 파일
        "pipeline_builder": "lambda_throttle",
        "timing_files": [str(TEAM_DIR / "lambda_throttle" / "clean_verification_20260914.json")],
    },
    {
        "name": "AutoScaling EDoS (v7, n=15+8)",
        "detection_file": "autoscaling_edos_traffic_trial__n8-15_scriptv4_20260913.json",
        "detection_format": "unified",
        "methods": {
            "detected_production": "production",
            "detected_iforest_only": "iforest_only",
            "detected_zscore_only": "zscore_only",
        },
        "classification_key": None,  # classification_accuracy 파일 없음
        "pipeline_builder": "autoscaling_edos",
        "timing_files": [],  # pipeline_result.timings 필드 자체가 없음(계측 추가 이전 실험)
    },
]


# ══════════════════════════════════════════════════════════════════════════
# 탐지 통계 (statistical_validation_report.py 로직 재사용)
# ══════════════════════════════════════════════════════════════════════════

def clopper_pearson_ci(successes: int, n: int, confidence: float = 0.95) -> list[float] | None:
    if n == 0:
        return None
    alpha = 1 - confidence
    lo = 0.0 if successes == 0 else _beta_dist.ppf(alpha / 2, successes, n - successes + 1)
    hi = 1.0 if successes == n else _beta_dist.ppf(1 - alpha / 2, successes + 1, n - successes)
    return [float(lo), float(hi)]


def load_trials(scenario: dict) -> list[dict]:
    fmt = scenario["detection_format"]
    path = scenario["detection_file"]
    path = path if Path(path).is_absolute() else EVAL_DIR / path
    data = json.loads(Path(path).read_text(encoding="utf-8"))

    if fmt == "unified" or fmt == "team":
        raw_trials = data["trials"]
    elif fmt == "split":
        raw_trials = data.get("anomaly_trials", []) + data.get("normal_trials", [])
    else:
        raise ValueError(f"unknown detection_format: {fmt}")

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
        "n_anomaly": n_anomaly,
        "n_normal": n_normal,
        "accuracy": accuracy,
        "accuracy_ci_95": clopper_pearson_ci(tp + tn, total),
        "recall": recall,
        "recall_ci_95": clopper_pearson_ci(tp, n_anomaly),
        "precision": precision,
        "f1_score": f1,
        "false_positive_rate": fpr,
        "fpr_ci_95": clopper_pearson_ci(fp, n_normal),
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
        return {"n_compared": n_compared, "b": b, "c": c, "p_value": None,
                "note": "두 방식이 모든 샘플에서 동일 판정(불일치 쌍 없음) — 검정 불가"}
    p_value = float(binomtest(min(b, c), discordant, 0.5, alternative="two-sided").pvalue)
    return {"n_compared": n_compared, "b": b, "c": c, "p_value": p_value,
            "significant_at_0.05": p_value < 0.05}


# ══════════════════════════════════════════════════════════════════════════
# 분류 정확도 (full_pipeline_verification_report.py 로직 재사용)
# ══════════════════════════════════════════════════════════════════════════

def load_classification_accuracy(key: str | None) -> dict | None:
    if key is None:
        return None
    if key == "lambda_throttle":
        path = TEAM_DIR / "lambda_throttle" / "classification_accuracy.json"
    else:
        path = EVAL_DIR / f"{key}.json"
    if not path.exists():
        return None
    d = json.loads(path.read_text(encoding="utf-8"))
    return {
        "rulebook": d.get("rulebook"), "llm": d.get("llm"),
        "combined_classification_accuracy": d.get("combined_classification_accuracy"),
    }


# ══════════════════════════════════════════════════════════════════════════
# 액션/QA (full_pipeline_verification_report.py 함수 재사용)
# ══════════════════════════════════════════════════════════════════════════

def _agent_summary(entries: list[dict]) -> dict:
    n = len(entries)
    action_success = sum(1 for e in entries if e["action_status"] == "success")
    qa_pass = sum(1 for e in entries if e["qa_passed"] is True)
    qa_fail = sum(1 for e in entries if e["qa_passed"] is False)
    rolled_back = sum(1 for e in entries if (e.get("rollback_count") or 0) > 0)
    return {
        "n_action_triggered": n,
        "action_success_rate": action_success / n if n else None,
        "qa_pass_rate": qa_pass / n if n else None,
        "qa_fail_count": qa_fail,
        "rollback_occurred_count": rolled_back,
    }


def build_pipeline_ec2_overprovisioning() -> dict:
    path = EVAL_DIR / "batch_pipeline_replay__EC2_20260912_134432.json"
    d = json.loads(path.read_text(encoding="utf-8"))
    entries = []
    for r in d["results"]:
        if not r.get("action_executed"):
            continue
        ar = r.get("action_result") or {}
        entries.append({"action_status": ar.get("status"), "qa_passed": r.get("qa_passed"),
                         "rollback_count": r.get("rollback_count")})
    return _agent_summary(entries)


def build_pipeline_s3_block() -> dict:
    path = EVAL_DIR / "3x_real_pipeline_20260910_025234.json"
    d = json.loads(path.read_text(encoding="utf-8"))
    entries = []
    for r in d["raw_results"]:
        if r.get("resource_type") != "S3" or not r.get("selected_action"):
            continue
        entries.append({"action_status": "success" if r.get("qa_passed") else "unknown",
                         "qa_passed": r.get("qa_passed"), "rollback_count": 0})
    return _agent_summary(entries)


def build_pipeline_autoscaling_edos() -> dict:
    path = EVAL_DIR / "autoscaling_edos_traffic_trial__n8-15_scriptv4_20260913.json"
    d = json.loads(path.read_text(encoding="utf-8"))
    entries = []
    for t in d["trials"]:
        pr = t.get("pipeline_result")
        if not pr or not pr.get("action_executed") or pr["action_executed"] == "NoAction":
            continue
        ar = pr.get("action_result") or {}
        scaledown_status = (ar.get("scaledown_result") or {}).get("status")
        waf_status = (ar.get("waf_result") or {}).get("status")
        entries.append({"action_status": scaledown_status, "waf_status": waf_status,
                         "qa_passed": pr.get("qa_passed"), "rollback_count": pr.get("rollback_count")})
    summary = _agent_summary(entries)
    summary["waf_association_fail_count"] = sum(1 for e in entries if e.get("waf_status") == "failed")
    return summary


def build_pipeline_lambda_throttle() -> dict:
    path = TEAM_DIR / "lambda_throttle" / "clean_verification_20260914.json"
    d = json.loads(path.read_text(encoding="utf-8"))
    entries = []
    for t in d["trials"]:
        if not t.get("selected_action"):
            continue
        entries.append({"action_status": "success" if t.get("qa_passed") else "unknown",
                         "qa_passed": t.get("qa_passed"), "rollback_count": 0})
    return _agent_summary(entries)


PIPELINE_BUILDERS = {
    "s3_block": build_pipeline_s3_block,
    "ec2_overprovisioning": build_pipeline_ec2_overprovisioning,
    "autoscaling_edos": build_pipeline_autoscaling_edos,
    "lambda_throttle": build_pipeline_lambda_throttle,
}


# ══════════════════════════════════════════════════════════════════════════
# 실행시간 (시나리오별로 구분 — full_metrics_report.py는 전부 뭉쳐서 집계했으나
# 이번엔 scenario["timing_files"]를 시나리오별로 태깅해서 분리 집계)
# ══════════════════════════════════════════════════════════════════════════

STAGES = ["detection", "classification", "decision", "action", "qa", "logging", "total"]


def _load_timing_samples(files: list) -> list[dict]:
    samples = []
    for f in files:
        f = Path(f)
        if not f.is_absolute():
            f = EVAL_DIR / f
        if not f.exists():
            continue
        d = json.loads(f.read_text(encoding="utf-8"))
        items = d if isinstance(d, list) else d.get("raw_results", d.get("trials", d.get("results", [d])))
        for it in items:
            t = it.get("timings")
            if t:
                samples.append(t)
    return samples


def build_timing_section(samples: list[dict]) -> dict | None:
    if not samples:
        return None
    section: dict = {"n_samples": len(samples), "stages": {}}
    for s in STAGES:
        vals = np.array([t[s] for t in samples if s in t])
        if len(vals) == 0:
            continue
        section["stages"][s] = {"n": len(vals), "mean": float(vals.mean()), "std": float(vals.std()),
                                 "min": float(vals.min()), "max": float(vals.max())}
    mttd = section["stages"].get("detection", {}).get("mean")
    mttr_vals = [t.get("detection", 0) + t.get("classification", 0) + t.get("decision", 0) + t.get("action", 0)
                 for t in samples if "action" in t]
    section["mttd_seconds"] = mttd
    section["mttr_seconds"] = {
        "n": len(mttr_vals),
        "mean": float(np.mean(mttr_vals)) if mttr_vals else None,
        "median": float(np.median(mttr_vals)) if mttr_vals else None,
    }
    return section


# ══════════════════════════════════════════════════════════════════════════
# 훈련/평가 데이터 독립성 & 오버피팅 — 시스템 공통 서술(시나리오별 아님)
# ══════════════════════════════════════════════════════════════════════════

QUALITATIVE_NOTES = {
    "train_eval_independence": (
        "detection_agent.py의 IsolationForest는 온라인 학습 구조라, '정상으로 판단된' 윈도우가 "
        "즉시 훈련 버퍼에 편입되고 모델이 재학습된다. AutoScaling EDoS 실험 패턴상 같은 리소스의 "
        "베이스라인(전) 구간이 먼저 버퍼에 들어간 뒤 그 리소스의 공격 후 구간을 채점하므로, 완전한 "
        "훈련/평가 데이터 독립은 아니다. v6(2026-09-13)에서 이 구조로 인한 자기강화적 오탐이 "
        "실측으로 확인됨. 시나리오별 수치가 아니라 통합 모델 구조 자체의 특성이다."
    ),
    "overfitting_risk": (
        "온라인 버퍼가 작을 때(특히 창 1개) 좁은 분산에 과적합되어 이후 정상 데이터도 이상으로 "
        "오판하는 자기강화적 패턴이 실측으로 확인됨(Phase 5, 창 1개 학습=정상 32.7% 오탐). "
        "MAX_WINDOWS_PER_TYPE(FIFO)로 완화하나 근본적으로 온라인 학습 특유의 리스크."
    ),
    "note_on_held_out_eval": (
        "PDF 4.3.4절의 'held-out 재검증 오탐률 0%'는 이 6개 실측 시나리오가 아니라 "
        "generate_eval_dataset.py가 만든 435개 합성 평가 데이터셋 기준이며, 실제 AWS n=13/23 "
        "실험과는 다른 데이터이므로 섞어서 인용하면 안 된다."
    ),
}


# ══════════════════════════════════════════════════════════════════════════
# 메인
# ══════════════════════════════════════════════════════════════════════════

def main() -> None:
    report: dict = {"scenarios": {}, "pooled": None, "qualitative_notes": QUALITATIVE_NOTES}
    pooled_tp = pooled_fn = pooled_fp = pooled_tn = 0
    pooled_scenarios = []

    for scenario in SCENARIOS:
        name = scenario["name"]
        det_path = scenario["detection_file"]
        det_path = det_path if Path(det_path).is_absolute() else EVAL_DIR / det_path
        if not Path(det_path).exists():
            print(f"[건너뜀] 탐지 파일 없음: {det_path}")
            continue

        print(f"\n{'='*70}\n{name}\n{'='*70}")
        trials = load_trials(scenario)
        methods = sorted({m for t in trials for m in t["results"]})
        entry: dict = {"detection": {}, "classification": None, "pipeline": None, "timing": None}

        for method in methods:
            m = confusion_and_ci(trials, method)
            entry["detection"][method] = m
            c = m["confusion_matrix"]
            print(f"  [탐지:{method}] n_a={m['n_anomaly']} n_n={m['n_normal']} "
                  f"TP={c['TP']} FN={c['FN']} FP={c['FP']} TN={c['TN']} "
                  f"acc={m['accuracy']*100:.1f}% recall={m['recall']*100:.1f}% "
                  + (f"F1={m['f1_score']:.3f}" if m['f1_score'] else "F1=N/A"))

        if "production" in methods:
            pm = confusion_and_ci(trials, "production")
            cm = pm["confusion_matrix"]
            pooled_tp += cm["TP"]; pooled_fn += cm["FN"]; pooled_fp += cm["FP"]; pooled_tn += cm["TN"]
            pooled_scenarios.append(name)

        mcnemar_results = {}
        for i, ma in enumerate(methods):
            for mb in methods[i + 1:]:
                mc = mcnemar_exact(trials, ma, mb)
                if mc:
                    mcnemar_results[f"{ma}_vs_{mb}"] = mc
                    print(f"  [McNemar] {ma} vs {mb}: b={mc['b']} c={mc['c']} p={mc['p_value']}")
        entry["mcnemar"] = mcnemar_results if mcnemar_results else None
        if len(methods) < 2:
            print("  [McNemar] 불가 — 저장된 탐지 방식 1개뿐")

        cls = load_classification_accuracy(scenario["classification_key"])
        entry["classification"] = cls
        if cls:
            print(f"  [분류] combined={cls['combined_classification_accuracy']}")
        else:
            print("  [분류] 데이터 없음")

        if scenario["pipeline_builder"]:
            entry["pipeline"] = PIPELINE_BUILDERS[scenario["pipeline_builder"]]()
            p = entry["pipeline"]
            print(f"  [액션/QA] n={p['n_action_triggered']} action_success={p['action_success_rate']} "
                  f"qa_pass={p['qa_pass_rate']}")
        else:
            print("  [액션/QA] 데이터 없음(탐지 전용)")

        timing_samples = _load_timing_samples(scenario["timing_files"])
        entry["timing"] = build_timing_section(timing_samples)
        if entry["timing"]:
            print(f"  [실행시간] n={entry['timing']['n_samples']} "
                  f"MTTD={entry['timing']['mttd_seconds']:.3f}s "
                  f"MTTR={entry['timing']['mttr_seconds']['mean']}")
        else:
            print("  [실행시간] 데이터 없음")

        report["scenarios"][name] = entry

    # 풀링
    pooled_total = pooled_tp + pooled_fn + pooled_fp + pooled_tn
    pooled_acc = (pooled_tp + pooled_tn) / pooled_total if pooled_total else None
    pooled_n_a = pooled_tp + pooled_fn
    pooled_recall = pooled_tp / pooled_n_a if pooled_n_a else None
    pooled_prec = pooled_tp / (pooled_tp + pooled_fp) if (pooled_tp + pooled_fp) else None
    pooled_f1 = (2 * pooled_prec * pooled_recall / (pooled_prec + pooled_recall)) if pooled_prec and pooled_recall else None
    report["pooled"] = {
        "scenarios_included": pooled_scenarios,
        "confusion_matrix": {"TP": pooled_tp, "FN": pooled_fn, "FP": pooled_fp, "TN": pooled_tn},
        "accuracy": pooled_acc, "accuracy_ci_95": clopper_pearson_ci(pooled_tp + pooled_tn, pooled_total),
        "recall": pooled_recall, "recall_ci_95": clopper_pearson_ci(pooled_tp, pooled_n_a),
        "f1_score": pooled_f1,
    }
    print(f"\n{'='*70}\n풀링(전체 시나리오 production 기준 합산, {pooled_scenarios})\n{'='*70}")
    print(f"  n={pooled_total} TP={pooled_tp} FN={pooled_fn} FP={pooled_fp} TN={pooled_tn}")
    print(f"  accuracy={pooled_acc*100:.1f}% recall={pooled_recall*100:.1f}% F1={pooled_f1:.3f}")

    OUTPUT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"\n결과 저장: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
