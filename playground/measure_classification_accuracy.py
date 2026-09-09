"""
playground/measure_classification_accuracy.py

s3_repeated_trial.py 결과 JSON에 저장된 실제 raw_metrics(각 시행의 실측 데이터)를
그대로 실제 classification_node()에 통과시켜서, Rule Book/LLM 분류 정확도를 잰다.

"정답 라벨"은 이상(anomaly)으로 실제 유발한 시행에 한해서만 정의된다 — 정상(normal)
시행이 오탐(FP)으로 잘못 감지된 경우는 애초에 "정상적으로 분류되면 안 되는" 상황이라
정답 자체가 없으므로 분류 정확도 계산에서 제외하고 참고 정보로만 남긴다.

[실행 방법]
  python playground/measure_classification_accuracy.py [결과 JSON 경로]
"""

from __future__ import annotations

import glob
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

from scipy.stats import beta as _beta_dist

from pipeline.classification_agent import classification_node
from pipeline.rule_engine import get_rule_engine

EVAL_OUTPUTS = PROJECT_ROOT / "playground" / "eval_outputs"


def clopper_pearson_ci(successes: int, n: int, confidence: float = 0.95) -> tuple[float, float]:
    if n == 0:
        return (0.0, 1.0)
    alpha = 1 - confidence
    lower = 0.0 if successes == 0 else _beta_dist.ppf(alpha / 2, successes, n - successes + 1)
    upper = 1.0 if successes == n else _beta_dist.ppf(1 - alpha / 2, successes + 1, n - successes)
    return (float(lower), float(upper))


_DERIVED_SUFFIXES = ("_analysis.json", "_accuracy.json")


def find_latest_result() -> Path:
    candidates = sorted(
        p for p in glob.glob(str(EVAL_OUTPUTS / "*_repeated_trial__*.json"))
        if not p.endswith(_DERIVED_SUFFIXES)
    )
    if not candidates:
        raise FileNotFoundError(f"{EVAL_OUTPUTS}에 반복실험 결과 파일이 없음")
    return Path(candidates[-1])


def _expected_label_for_scenario(resource_type: str) -> str | None:
    """이 반복실험이 유발한 anomaly가 Rule Book상 어떤 anomaly_type으로 분류돼야
    "정답"인지, classification_rules.json에서 직접 읽어온다(하드코딩 대신 —
    나중에 risk_security->cost_spike 재분류가 실제 적용돼도 이 스크립트를 안
    고쳐도 되게). resource_type만 일치하는 규칙 중 우선순위가 가장 높은 것을 씀.
    """
    engine = get_rule_engine()
    candidates = [r for r in engine.classification_rules if resource_type in r.get("resource_types", [])]
    if not candidates:
        return None
    candidates.sort(key=lambda r: r.get("priority", 999))
    return candidates[0].get("result", {}).get("anomaly_type")


def main() -> None:
    result_path = Path(sys.argv[1]) if len(sys.argv) > 1 else find_latest_result()
    print(f"결과 파일: {result_path}\n")

    with open(result_path, encoding="utf-8") as f:
        payload = json.load(f)

    resource_type = "S3"  # s3_repeated_trial.py 결과 전용. 다른 스크립트로 확장 시 여기만 바꾸면 됨
    expected_label = _expected_label_for_scenario(resource_type)
    print(f"이 리소스 타입의 '정답' anomaly_type (Rule Book 기준): {expected_label}\n")

    anomaly_trials = payload["anomaly_trials"]
    normal_trials = payload["normal_trials"]

    rulebook_total, rulebook_correct = 0, 0
    llm_total, llm_correct = 0, 0
    fp_classification_notes = []

    def classify(after: dict, trace_label: str):
        state = {
            "trace_id": None,
            "resource_id": "analysis",
            "resource_type": resource_type,
            "raw_metrics": after["raw_metrics"],
            "triggered_metrics": after.get("triggered_metrics", []),
            "anomaly_flag": True,
            "anomaly_score_zscore": after.get("anomaly_score_zscore"),
            "anomaly_score_iforest": after.get("anomaly_score_iforest"),
        }
        state = classification_node(state)
        return state["matched_rule_id"], state["anomaly_type"], state["classification_reasoning"]

    # 1. 진짜 이상(anomaly) 시행 중 실제로 탐지된 것(TP)만 - "정답"이 정의됨
    for t in anomaly_trials:
        if not t.get("detected"):
            continue  # 놓친 것(FN)은 애초에 classification까지 못 감
        after = t["after"]
        rule_id, anomaly_type, reasoning = classify(after, f"anomaly-rep{t['rep']}")
        is_rulebook = rule_id is not None
        is_correct = (anomaly_type == expected_label)
        if is_rulebook:
            rulebook_total += 1
            rulebook_correct += int(is_correct)
        else:
            llm_total += 1
            llm_correct += int(is_correct)
        print(f"[anomaly rep={t['rep']}] matched_rule={rule_id}, anomaly_type={anomaly_type}, "
              f"정답={'O' if is_correct else 'X'}")

    # 2. 오탐(FP)으로 걸린 정상 시행 - "정답"이 없으니 참고용으로만 어떻게 분류됐는지 기록
    for t in normal_trials:
        if not t.get("detected"):
            continue
        after = t["after"]
        rule_id, anomaly_type, reasoning = classify(after, f"normal-rep{t['rep']}(FP)")
        fp_classification_notes.append({"rep": t["rep"], "matched_rule_id": rule_id, "anomaly_type": anomaly_type})
        print(f"[normal rep={t['rep']}, 오탐] matched_rule={rule_id}, anomaly_type={anomaly_type} "
              f"(참고용 - 애초에 정답 없음)")

    print("\n=== 결과 ===")
    total = rulebook_total + llm_total
    p_rulebook = rulebook_total / total if total else None
    p_llm = llm_total / total if total else None
    rulebook_acc = rulebook_correct / rulebook_total if rulebook_total else None
    llm_acc = llm_correct / llm_total if llm_total else None

    print(f"P(Rule Book 처리) = {rulebook_total}/{total} = {p_rulebook*100:.1f}%" if total else "케이스 없음")
    print(f"P(LLM 처리) = {llm_total}/{total} = {p_llm*100:.1f}%" if total else "")
    if rulebook_total:
        ci = clopper_pearson_ci(rulebook_correct, rulebook_total)
        print(f"Rule Book 정확도 = {rulebook_correct}/{rulebook_total} = {rulebook_acc*100:.1f}% "
              f"(95% CI [{ci[0]*100:.1f}%, {ci[1]*100:.1f}%])")
    if llm_total:
        ci = clopper_pearson_ci(llm_correct, llm_total)
        print(f"LLM 정확도 = {llm_correct}/{llm_total} = {llm_acc*100:.1f}% "
              f"(95% CI [{ci[0]*100:.1f}%, {ci[1]*100:.1f}%])")

    combined_classification_accuracy = (
        (p_rulebook or 0) * (rulebook_acc or 0) + (p_llm or 0) * (llm_acc or 0)
    ) if total else None
    print(f"\n분류 정확도(가중합, 공식용) = {combined_classification_accuracy*100:.1f}%" if total else "")

    out = {
        "source_result_file": str(result_path),
        "expected_label": expected_label,
        "rulebook": {"total": rulebook_total, "correct": rulebook_correct, "accuracy": rulebook_acc},
        "llm": {"total": llm_total, "correct": llm_correct, "accuracy": llm_acc},
        "p_rulebook": p_rulebook,
        "p_llm": p_llm,
        "combined_classification_accuracy": combined_classification_accuracy,
        "false_positive_classification_notes": fp_classification_notes,
    }
    out_path = result_path.parent / f"{result_path.stem}__classification_accuracy.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"\n저장: {out_path}")


if __name__ == "__main__":
    main()
