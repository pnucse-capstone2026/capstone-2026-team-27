"""
playground/lambda_retry_real_aws_per_mechanism.py

Lambda 재시도폭증 실 AWS 테스트 - 13개 함수(parallel_test.py로 이미 호출 완료된
상태)의 CloudWatch 지표를 다시 읽어서, z-score/IForest/절대(_lambda_error_rate_check)/
OR게이트(detection_node 전체) 각각의 confusion matrix를 계산한다.

호출 단계는 다시 안 함(이미 끝난 실제 호출의 CloudWatch 데이터를 그대로 재사용) -
ec2_zombie_real_aws_iforest_report.py의 compute_all_signals/_confusion_and_metrics를
그대로 재사용.

[실행 방법] 프로젝트 루트에서: python playground/lambda_retry_real_aws_per_mechanism.py
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

os.environ.setdefault("AWS_PROFILE", "default")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(PROJECT_ROOT / "playground") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "playground"))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

from pipeline.cloudwatch_client import fetch_metrics
from ec2_zombie_real_aws_iforest_report import compute_all_signals, _confusion_and_metrics

# parallel_test.py의 FUNCTIONS와 동일 - true_label만 필요(호출 패턴은 이미 끝났으므로 무관)
FUNCTIONS = (
    [("detection-test-lambda", "anomaly")]
    + [(f"detection-test-lambda-anomaly-{i}", "anomaly") for i in (2, 3, 4, 5)]
    + [(f"detection-test-lambda-normal-{i}", "normal") for i in range(1, 9)]
)


def main():
    windows = []
    for fn, true_label in FUNCTIONS:
        usage = fetch_metrics("Lambda", fn)
        signals = compute_all_signals("Lambda", usage, resource_age_seconds=None)
        windows.append({
            "id": fn,
            "true_label": true_label,
            "z_score": signals["z_score"],
            "iforest": signals["iforest"],
            "iforest_score": signals["iforest_score"],
            "absolute_error_surge": signals["absolute"],
            "or_gate": signals["or_gate"],
            "last_invocation": usage["invocation_count"][-3:],
            "last_error": usage["error_count"][-3:],
        })

    per_mechanism = {
        "z_score": _confusion_and_metrics(windows, "z_score"),
        "iforest": _confusion_and_metrics(windows, "iforest"),
        "absolute_error_surge": _confusion_and_metrics(windows, "absolute_error_surge"),
        "or_gate(detection_node 전체)": _confusion_and_metrics(windows, "or_gate"),
    }

    report = {"scenario": "Lambda 재시도폭증 (실 AWS, 13개 함수 병렬)", "windows": windows, "per_mechanism": per_mechanism}

    print(f"{'function':<35} {'true_label':<10} {'IF단독':<8} {'OR게이트(전체)':<14} {'iforest_score':<14}")
    print("-" * 90)
    for w in windows:
        print(f"{w['id']:<35} {w['true_label']:<10} {str(w['iforest']):<8} {str(w['or_gate']):<14} {w['iforest_score']:<14}")

    def _print_row(name, r):
        c, m = r["confusion"], r["metrics"]
        acc = f"{m['accuracy']:.1%}" if m['accuracy'] is not None else "N/A"
        rec = f"{m['recall']:.1%}" if m['recall'] is not None else "N/A"
        print(f"{name:<25} {r['n_normal']:<10} {r['n_anomaly']:<10} {c['TP']:<4} {c['TN']:<4} {c['FP']:<4} {c['FN']:<4} {acc:<10} {rec:<10}")

    header = f"{'메커니즘':<25} {'n_normal':<10} {'n_anomaly':<10} {'TP':<4} {'TN':<4} {'FP':<4} {'FN':<4} {'accuracy':<10} {'recall':<10}"

    print("\n[핵심] IForest 단독 vs OR게이트(detection_node 전체)")
    print(header)
    print("-" * 95)
    _print_row("iforest", per_mechanism["iforest"])
    _print_row("or_gate(detection_node 전체)", per_mechanism["or_gate(detection_node 전체)"])

    print("\n[참고] z-score 단독 / 절대체크 단독")
    print(header)
    print("-" * 95)
    _print_row("z_score", per_mechanism["z_score"])
    _print_row("absolute_error_surge", per_mechanism["absolute_error_surge"])

    out_path = PROJECT_ROOT / "playground" / "eval_outputs" / "lambda_retry_real_aws_per_mechanism.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(f"\n저장: {out_path}")


if __name__ == "__main__":
    main()
