"""
playground/combine_team_results.py

각자 시나리오별로 만든 실측 결과 파일들을 한 폴더 구조에 모아두면, 그걸 전부 읽어서
"3개(또는 N개) 시나리오 합산" 기준의 최종 4개 지표(전체 파이프라인 성공률 구성요소,
비용절감액, 파이프라인 실행시간)를 계산한다.

⚠️ 절대 퍼센트를 평균 내지 않는다 — 원본 TP/TN/FP/FN, 성공/시도, 정답/전체, 일치/
비교가능 건수를 전부 더한 뒤 그 합계로 재계산한다(세션에서 여러 번 확인한 원칙).
비용절감액은 단순 합산, 파이프라인 실행시간은 진짜 평균(mean±SD).

[폴더 구조] (팀원마다 이렇게 모아서 team_results/ 밑에 두면 됨 — 파일 없는 항목은 스킵)
  team_results/
    ec2/
      repeated_trial.json                 (s3_repeated_trial.py류 결과 - confusion_matrix)
      classification_accuracy.json        (measure_classification_accuracy.py 결과)
      action_execution_log.jsonl          (팀원의 action_execution_log.jsonl 사본)
      cost_prediction_verification.json   (verify_cost_predictions.py 결과)
      pipeline_timing.json                (measure_pipeline_timing.py 결과, 리스트)
    lambda/
      ... 동일 구조
    s3/
      ... 동일 구조

[실행 방법]
  python playground/combine_team_results.py [team_results 폴더 경로]
  (생략 시 playground/team_results 사용)
"""

from __future__ import annotations

import json
import statistics
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scipy.stats import beta as _beta_dist

DEFAULT_TEAM_RESULTS_DIR = PROJECT_ROOT / "playground" / "team_results"


def clopper_pearson_ci(successes: int, n: int, confidence: float = 0.95) -> tuple[float, float]:
    if n == 0:
        return (0.0, 1.0)
    alpha = 1 - confidence
    lower = 0.0 if successes == 0 else _beta_dist.ppf(alpha / 2, successes, n - successes + 1)
    upper = 1.0 if successes == n else _beta_dist.ppf(1 - alpha / 2, successes + 1, n - successes)
    return (float(lower), float(upper))


def _load_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    out = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out


def collect_scenario(scenario_dir: Path) -> dict:
    """시나리오 폴더 하나에서 있는 파일만 읽어서 원본 수치를 뽑아낸다."""
    name = scenario_dir.name
    result = {"scenario": name, "sources_found": []}

    # 1. 탐지 정확도 (confusion_matrix)
    rt = _load_json(scenario_dir / "repeated_trial.json")
    if rt and "metrics" in rt:
        cm = rt["metrics"]["confusion_matrix"]
        result["detection"] = cm
        result["sources_found"].append("repeated_trial.json")

    # 2. 분류 정확도
    ca = _load_json(scenario_dir / "classification_accuracy.json")
    if ca:
        result["classification"] = {
            "rulebook_total": ca["rulebook"]["total"], "rulebook_correct": ca["rulebook"]["correct"],
            "llm_total": ca["llm"]["total"], "llm_correct": ca["llm"]["correct"],
        }
        result["sources_found"].append("classification_accuracy.json")

    # 3. Action 성공률
    actions = _load_jsonl(scenario_dir / "action_execution_log.jsonl")
    if actions:
        result["action"] = {
            "total": len(actions),
            "success": sum(1 for a in actions if a.get("status") == "success"),
        }
        result["sources_found"].append("action_execution_log.jsonl")

    # 4. QA 정확도 + 비용절감액 (스냅샷 + 기간 적분)
    verify = _load_json(scenario_dir / "cost_prediction_verification.json")
    if verify:
        results_list = verify.get("results", [])
        qa_comparable = [r for r in results_list if r.get("qa_result_available")]
        qa_correct = [r for r in qa_comparable if r.get("qa_matched_real_outcome")]
        result["qa"] = {"comparable": len(qa_comparable), "correct": len(qa_correct)}

        period_savings = [
            r["period_totals"]["period_saving_usd"] for r in results_list
            if r.get("period_totals") and r["period_totals"].get("period_saving_usd") is not None
        ]
        result["actual_saving_usd_total"] = sum(period_savings) if period_savings else None
        result["actual_saving_usd_per_case"] = period_savings
        result["sources_found"].append("cost_prediction_verification.json")

    # 5. 파이프라인 실행시간
    timing = _load_json(scenario_dir / "pipeline_timing.json")
    if timing:
        entries = timing if isinstance(timing, list) else [timing]
        totals = [e["timings"]["total"] for e in entries if "total" in e.get("timings", {})]
        result["pipeline_timing_sec"] = totals
        result["sources_found"].append("pipeline_timing.json")

    return result


def combine(scenarios: list[dict]) -> dict:
    # ── 탐지(IForest+Zscore 앙상블) ──
    tp = sum(s.get("detection", {}).get("TP", 0) for s in scenarios)
    fn = sum(s.get("detection", {}).get("FN", 0) for s in scenarios)
    fp = sum(s.get("detection", {}).get("FP", 0) for s in scenarios)
    tn = sum(s.get("detection", {}).get("TN", 0) for s in scenarios)
    n_detection = tp + fn + fp + tn
    detection_accuracy = (tp + tn) / n_detection if n_detection else None
    detection_recall = tp / (tp + fn) if (tp + fn) else None

    # ── 분류(Rule Book/LLM) ──
    rb_total = sum(s.get("classification", {}).get("rulebook_total", 0) for s in scenarios)
    rb_correct = sum(s.get("classification", {}).get("rulebook_correct", 0) for s in scenarios)
    llm_total = sum(s.get("classification", {}).get("llm_total", 0) for s in scenarios)
    llm_correct = sum(s.get("classification", {}).get("llm_correct", 0) for s in scenarios)
    n_classification = rb_total + llm_total
    p_rulebook = rb_total / n_classification if n_classification else None
    p_llm = llm_total / n_classification if n_classification else None
    rb_accuracy = rb_correct / rb_total if rb_total else None
    llm_accuracy = llm_correct / llm_total if llm_total else None
    classification_accuracy = (
        (p_rulebook or 0) * (rb_accuracy or 0) + (p_llm or 0) * (llm_accuracy or 0)
    ) if n_classification else None

    # ── Action 성공률 ──
    action_total = sum(s.get("action", {}).get("total", 0) for s in scenarios)
    action_success = sum(s.get("action", {}).get("success", 0) for s in scenarios)
    action_success_rate = action_success / action_total if action_total else None

    # ── QA 정확도 ──
    qa_comparable = sum(s.get("qa", {}).get("comparable", 0) for s in scenarios)
    qa_correct = sum(s.get("qa", {}).get("correct", 0) for s in scenarios)
    qa_accuracy = qa_correct / qa_comparable if qa_comparable else None

    # ── 전체 파이프라인 성공률 (1회 시행 기준, 독립 가정) ──
    overall_success_rate = None
    if all(v is not None for v in [detection_recall, classification_accuracy, action_success_rate, qa_accuracy]):
        overall_success_rate = detection_recall * classification_accuracy * action_success_rate * qa_accuracy

    # ── 비용절감액 (합산) ──
    all_savings = [s["actual_saving_usd_total"] for s in scenarios if s.get("actual_saving_usd_total") is not None]
    total_saving_usd = sum(all_savings) if all_savings else None

    # ── 파이프라인 실행시간 (평균±SD) ──
    all_timings = [t for s in scenarios for t in s.get("pipeline_timing_sec", [])]
    timing_mean = statistics.mean(all_timings) if all_timings else None
    timing_sd = statistics.stdev(all_timings) if len(all_timings) > 1 else 0.0

    return {
        "n_scenarios": len(scenarios),
        "scenario_names": [s["scenario"] for s in scenarios],
        "detection": {
            "TP": tp, "FN": fn, "FP": fp, "TN": tn, "n": n_detection,
            "accuracy": detection_accuracy, "accuracy_ci_95": list(clopper_pearson_ci(tp + tn, n_detection)) if n_detection else None,
            "recall": detection_recall, "recall_ci_95": list(clopper_pearson_ci(tp, tp + fn)) if (tp + fn) else None,
        },
        "classification": {
            "p_rulebook": p_rulebook, "p_llm": p_llm,
            "rulebook_accuracy": rb_accuracy, "llm_accuracy": llm_accuracy,
            "combined_accuracy": classification_accuracy, "n": n_classification,
        },
        "action": {"success": action_success, "total": action_total, "success_rate": action_success_rate},
        "qa": {"correct": qa_correct, "comparable": qa_comparable, "accuracy": qa_accuracy},
        "overall_pipeline_success_rate": overall_success_rate,
        "total_saving_usd": total_saving_usd,
        "saving_per_case_usd": all_savings,
        "pipeline_timing_sec": {"mean": timing_mean, "sd": timing_sd, "n": len(all_timings), "values": all_timings},
        "per_scenario_raw": scenarios,
    }


def main() -> None:
    team_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_TEAM_RESULTS_DIR
    if not team_dir.exists():
        print(f"{team_dir} 없음 — 팀원 결과 파일을 이 구조로 모아두고 다시 실행할 것:\n"
              f"  {team_dir}/<시나리오이름>/repeated_trial.json 등 (docstring 참고)")
        return

    scenario_dirs = [d for d in team_dir.iterdir() if d.is_dir()]
    if not scenario_dirs:
        print(f"{team_dir} 안에 시나리오 폴더가 없음")
        return

    scenarios = [collect_scenario(d) for d in scenario_dirs]
    for s in scenarios:
        print(f"[{s['scenario']}] 발견된 파일: {s['sources_found'] or '없음'}")

    combined = combine(scenarios)

    print("\n=== 합산 결과 ===")
    print(f"시나리오 {combined['n_scenarios']}개: {combined['scenario_names']}")
    d = combined["detection"]
    print(f"\n[탐지] TP={d['TP']} FN={d['FN']} FP={d['FP']} TN={d['TN']} (n={d['n']})")
    if d["accuracy"] is not None:
        print(f"  accuracy={d['accuracy']*100:.1f}% recall={d['recall']*100:.1f}%")

    c = combined["classification"]
    print(f"\n[분류] P(RuleBook)={c['p_rulebook']}, P(LLM)={c['p_llm']}, "
          f"결합정확도={c['combined_accuracy']}, n={c['n']}")

    a = combined["action"]
    print(f"\n[Action] {a['success']}/{a['total']} = {a['success_rate']}")

    q = combined["qa"]
    print(f"\n[QA] {q['correct']}/{q['comparable']} = {q['accuracy']}")

    print(f"\n[전체 파이프라인 성공률] {combined['overall_pipeline_success_rate']}")
    print(f"[총 절감액] ${combined['total_saving_usd']}")
    t = combined["pipeline_timing_sec"]
    print(f"[파이프라인 실행시간] {t['mean']} ± {t['sd']} 초 (n={t['n']})")

    out_path = PROJECT_ROOT / "playground" / "eval_outputs" / "combined_team_results.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(combined, f, ensure_ascii=False, indent=2)
    print(f"\n저장: {out_path}")


if __name__ == "__main__":
    main()
