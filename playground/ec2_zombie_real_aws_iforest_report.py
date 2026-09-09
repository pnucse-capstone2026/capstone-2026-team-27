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

        usage = fetch_metrics("EC2", instance_id)
        usage["cost"] = estimate_cost_series("EC2", instance_id, usage)

        iforest_score, iforest_triggered = da._iforest_score_and_trigger("EC2", usage)

        windows.append({
            "id": instance_id,
            "true_label": true_label,
            "predicted": iforest_triggered,
            "iforest_score": round(iforest_score, 4),
            "last_cpu": usage["cpu_utilization"][-3:],
            "last_network_in": usage["network_in"][-3:],
        })

    TP = sum(1 for w in windows if w["true_label"] == "anomaly" and w["predicted"])
    FN = sum(1 for w in windows if w["true_label"] == "anomaly" and not w["predicted"])
    FP = sum(1 for w in windows if w["true_label"] == "normal" and w["predicted"])
    TN = sum(1 for w in windows if w["true_label"] == "normal" and not w["predicted"])

    n_anomaly = TP + FN
    n_normal = TN + FP
    accuracy = (TP + TN) / len(windows) if windows else None
    recall = TP / n_anomaly if n_anomaly else None
    precision = TP / (TP + FP) if (TP + FP) else None

    return {
        "scenario": "EC2 좀비 (실 AWS)",
        "mechanism": "IForest 단독 (_iforest_score_and_trigger)",
        "n_normal": n_normal,
        "n_anomaly": n_anomaly,
        "windows": windows,
        "confusion": {"TP": TP, "TN": TN, "FP": FP, "FN": FN},
        "metrics": {
            "accuracy": round(accuracy, 4) if accuracy is not None else None,
            "recall": round(recall, 4) if recall is not None else None,
            "precision": round(precision, 4) if precision is not None else None,
        },
    }


def main():
    manifest_path = Path(sys.argv[1]) if len(sys.argv) > 1 else None
    if manifest_path is None:
        print("사용법: python playground/ec2_zombie_real_aws_iforest_report.py <manifest.json 경로>")
        sys.exit(1)

    report = build_report(manifest_path)

    print(f"{'instance_id':<22} {'true_label':<10} {'predicted':<10} {'iforest_score':<14}")
    print("-" * 60)
    for w in report["windows"]:
        print(f"{w['id']:<22} {w['true_label']:<10} {str(w['predicted']):<10} {w['iforest_score']:<14}")

    print()
    c = report["confusion"]
    m = report["metrics"]
    print(f"n_normal={report['n_normal']} n_anomaly={report['n_anomaly']}")
    print(f"TP={c['TP']} TN={c['TN']} FP={c['FP']} FN={c['FN']}")
    print(f"accuracy={m['accuracy']:.1%}  recall={m['recall']:.1%}"
          + (f"  precision={m['precision']:.1%}" if m['precision'] is not None else "  precision=N/A(FP+TP=0)"))

    out_path = PROJECT_ROOT / "playground" / "eval_outputs" / "ec2_zombie_real_aws_iforest_report.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(f"\n저장: {out_path}")


if __name__ == "__main__":
    main()
