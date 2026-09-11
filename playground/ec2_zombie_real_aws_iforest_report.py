"""
playground/ec2_zombie_real_aws_iforest_report.py

EC2 좀비 시나리오 실 AWS 테스트 - IForest 단독(_iforest_score_and_trigger,
detection_node 전체가 아님) 결과를 표준 포맷으로 뽑는다.

매니페스트(스크래치패드의 ec2_zombie_manifest.json)에 기록된 인스턴스ID·정답
라벨(true_label)·launch 시각을 읽어서, 각 인스턴스의 실제 CloudWatch 지표를
가져와 IForest 단독 판정과 비교한다.

[실행 방법] 프로젝트 루트에서:
  python playground/ec2_zombie_real_aws_iforest_report.py <manifest.json 경로>
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

os.environ.setdefault("AWS_PROFILE", "default")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

from pipeline.cloudwatch_client import fetch_metrics
from pipeline.cost_estimator import estimate_cost_series
import pipeline.detection_agent as da


def compute_all_signals(resource_type: str, metrics: dict, resource_age_seconds=None) -> dict:
    """4개 메커니즘 + OR게이트를 전부 독립적으로 계산.
    ⚠️ z-score는 detection_node의 triggered_metrics를 그대로 읽으면 idle/error_surge
    체크가 같은 리스트에 결과를 이어붙이는 것 때문에 오염된다 - z-score 루프를
    독립적으로 재현해서 별도 boolean으로 받는다."""
    z_triggered = False
    max_abs_z = 0.0
    for metric_name in metrics:
        if metric_name not in da.Z_SCORE_TARGET_METRICS:
            continue
        z, is_trig = da._zscore_check_persistent(metrics[metric_name])
        max_abs_z = max(max_abs_z, z)
        if is_trig:
            z_triggered = True

    iforest_score, iforest_triggered = da._iforest_score_and_trigger(resource_type, metrics)
    _, idle_triggered, _ = da._low_utilization_check(resource_type, metrics, resource_age_seconds)
    _, error_surge_triggered = da._lambda_error_rate_check(resource_type, metrics)

    absolute_triggered = idle_triggered or error_surge_triggered  # 타입별로 하나만 의미 있음
    or_gate = z_triggered or iforest_triggered or idle_triggered or error_surge_triggered

    return {
        "z_score": z_triggered,
        "z_max": round(max_abs_z, 4),
        "iforest": iforest_triggered,
        "iforest_score": round(iforest_score, 4),
        "absolute": absolute_triggered,
        "or_gate": or_gate,
    }


def _confusion_and_metrics(windows: list[dict], predicted_key: str) -> dict:
    TP = sum(1 for w in windows if w["true_label"] == "anomaly" and w[predicted_key])
    FN = sum(1 for w in windows if w["true_label"] == "anomaly" and not w[predicted_key])
    FP = sum(1 for w in windows if w["true_label"] == "normal" and w[predicted_key])
    TN = sum(1 for w in windows if w["true_label"] == "normal" and not w[predicted_key])
    n_anomaly, n_normal = TP + FN, TN + FP
    accuracy = (TP + TN) / len(windows) if windows else None
    recall = TP / n_anomaly if n_anomaly else None
    precision = TP / (TP + FP) if (TP + FP) else None
    return {
        "n_normal": n_normal, "n_anomaly": n_anomaly,
        "confusion": {"TP": TP, "TN": TN, "FP": FP, "FN": FN},
        "metrics": {
            "accuracy": round(accuracy, 4) if accuracy is not None else None,
            "recall": round(recall, 4) if recall is not None else None,
            "precision": round(precision, 4) if precision is not None else None,
        },
    }


def build_report(manifest_path: Path) -> dict:
    with open(manifest_path, encoding="utf-8") as f:
        manifest = json.load(f)

    check_earliest = datetime.fromisoformat(manifest["check_earliest_utc"].replace("Z", "+00:00"))
    now = datetime.now(timezone.utc)
    if now < check_earliest:
        remaining = (check_earliest - now).total_seconds()
        raise RuntimeError(
            f"아직 체크 가능 시점이 안 됐습니다 (나이가드 2.5시간 미충족). "
            f"{remaining/60:.1f}분 더 기다려야 합니다 (기준: {check_earliest.isoformat()})."
        )

    windows = []
    for inst in manifest["instances"]:
        instance_id = inst["instance_id"]
        true_label = inst["true_label"]
        launch_time = datetime.fromisoformat(inst["launch_time_utc"].replace("Z", "+00:00"))
        age_seconds = (now - launch_time).total_seconds()

        usage = fetch_metrics("EC2", instance_id)
        usage["cost"] = estimate_cost_series("EC2", instance_id, usage)

        signals = compute_all_signals("EC2", usage, resource_age_seconds=age_seconds)

        windows.append({
            "id": instance_id,
            "true_label": true_label,
            "profile": inst.get("profile"),
            "age_seconds": round(age_seconds, 1),
            "z_score": signals["z_score"],
            "iforest": signals["iforest"],
            "iforest_score": signals["iforest_score"],
            "absolute_idle": signals["absolute"],
            "or_gate": signals["or_gate"],
            "last_cpu": usage["cpu_utilization"][-3:],
            "last_network_in": usage["network_in"][-3:],
        })

    per_mechanism = {
        "z_score": _confusion_and_metrics(windows, "z_score"),
        "iforest": _confusion_and_metrics(windows, "iforest"),
        "absolute_idle": _confusion_and_metrics(windows, "absolute_idle"),
        "or_gate(detection_node 전체)": _confusion_and_metrics(windows, "or_gate"),
    }

    return {
        "scenario": "EC2 좀비 (실 AWS)",
        "windows": windows,
        "per_mechanism": per_mechanism,
    }


def main():
    manifest_path = Path(sys.argv[1]) if len(sys.argv) > 1 else None
    if manifest_path is None:
        print("사용법: python playground/ec2_zombie_real_aws_iforest_report.py <manifest.json 경로>")
        sys.exit(1)

    report = build_report(manifest_path)

    print(f"{'instance_id':<22} {'true_label':<10} {'profile':<14} {'IF단독':<8} {'OR게이트(전체)':<14} {'iforest_score':<14}")
    print("-" * 90)
    for w in report["windows"]:
        print(f"{w['id']:<22} {w['true_label']:<10} {str(w.get('profile')):<14} "
              f"{str(w['iforest']):<8} {str(w['or_gate']):<14} {w['iforest_score']:<14}")

    def _print_row(name, r):
        c, m = r["confusion"], r["metrics"]
        acc = f"{m['accuracy']:.1%}" if m['accuracy'] is not None else "N/A"
        rec = f"{m['recall']:.1%}" if m['recall'] is not None else "N/A"
        print(f"{name:<25} {r['n_normal']:<10} {r['n_anomaly']:<10} {c['TP']:<4} {c['TN']:<4} {c['FP']:<4} {c['FN']:<4} {acc:<10} {rec:<10}")

    header = f"{'메커니즘':<25} {'n_normal':<10} {'n_anomaly':<10} {'TP':<4} {'TN':<4} {'FP':<4} {'FN':<4} {'accuracy':<10} {'recall':<10}"

    print("\n[핵심] IForest 단독 vs OR게이트(detection_node 전체)")
    print(header)
    print("-" * 95)
    _print_row("iforest", report["per_mechanism"]["iforest"])
    _print_row("or_gate(detection_node 전체)", report["per_mechanism"]["or_gate(detection_node 전체)"])

    print("\n[참고] z-score 단독 / 절대체크 단독")
    print(header)
    print("-" * 95)
    _print_row("z_score", report["per_mechanism"]["z_score"])
    _print_row("absolute_idle", report["per_mechanism"]["absolute_idle"])

    out_path = PROJECT_ROOT / "playground" / "eval_outputs" / "ec2_zombie_real_aws_iforest_report.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(f"\n저장: {out_path}")


if __name__ == "__main__":
    main()
