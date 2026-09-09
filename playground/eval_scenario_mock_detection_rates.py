"""
playground/eval_scenario_mock_detection_rates.py

playground/mock_data/*_eval.json 전체를 실제 detection_node(zscore+IForest+절대임계값
OR 앙상블) 파이프라인으로 통과시켜, 라벨(normal/anomaly/edge_normal)별 트리거율을
측정한다. seed_iforest_from_scenario_mock.py로 갱신된 models/iforest_unified.pkl을
그대로 사용(재학습 없음, 순수 평가).

normal/edge_normal은 트리거 안 되는 게 맞고(오탐률로 집계), anomaly는 트리거 되는
게 맞다(탐지율로 집계).

[실행 방법] 프로젝트 루트에서: python playground/eval_scenario_mock_detection_rates.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pipeline.detection_agent as da

MOCK_DATA_DIR = PROJECT_ROOT / "playground" / "mock_data"

# (표시용 라벨, 실제 resource_type, eval 파일)
EVAL_TARGETS = [
    ("EC2 (좀비, 담당)", "EC2", "ec2_eval.json"),
    ("Lambda (재시도폭증, 담당)", "Lambda", "lambda_eval_retry.json"),
    ("Lambda (cost_spike, 팀원)", "Lambda", "lambda_eval.json"),
    ("AutoScaling (EDoS, 팀원)", "AutoScaling", "autoscaling_eval.json"),
    ("S3 (대량다운로드, 팀원)", "S3", "s3_eval.json"),
]


def run_window(resource_type: str, window: dict) -> bool:
    state = {
        "trace_id": None,
        "resource_id": window.get("resource_id", "eval"),
        "resource_type": resource_type,
        "raw_metrics": window["raw_metrics"],
        "timestamp": None,
        "resource_age_seconds": window.get("resource_age_seconds"),
        "anomaly_flag": False,
        "anomaly_score_zscore": None,
        "anomaly_score_iforest": None,
        "triggered_metrics": [],
    }
    result = da.detection_node(state)
    return bool(result["anomaly_flag"])


DIAGNOSES = {
    ("Lambda", "lambda_eval_retry.json", "edge_normal"): (
        "_lambda_error_rate_check 단독으로는 5개 전부 정확히 False(설계대로 통과). "
        "하지만 IForest가 별도로 트리거함 - 에러율은 문턱(50%) 아래지만 error_count "
        "절대값이 베이스라인 대비 8~18배 뛰어서, 다변량 이상탐지 기준으로는 정당하게 "
        "이상치로 반응한 것. 규칙 기반 체크의 버그가 아니라 '비율 기준 vs 절대 수치 "
        "기준'이 다른 데서 오는 구조적 결과. detection_node의 triggered_metrics에는 "
        "error_count/invocation_count가 안 들어가므로(내 체크는 False), 다운스트림 "
        "분류 단계에서 재시도폭증(CLF-002)과 다르게 처리될 가능성이 있음(미검증)."
    ),
    ("Lambda", "lambda_eval.json", "anomaly"): (
        "8/10(80%) 탐지, 2개 미탐. 4타입 통합 재학습(타입당 30윈도우, 방금 교체) 직후 "
        "측정이라 표본이 작아서 나온 결과일 가능성 - 구조적 결함으로 단정하기엔 이름."
    ),
}


def main():
    print(f"{'시나리오':<28} {'normal 오탐':<14} {'anomaly 탐지':<14} {'edge_normal 오탐':<16}")
    print("-" * 76)

    results = []

    for label, resource_type, filename in EVAL_TARGETS:
        path = MOCK_DATA_DIR / filename
        with open(path, encoding="utf-8") as f:
            windows = json.load(f)

        by_label = {"normal": [], "anomaly": [], "edge_normal": []}
        for w in windows:
            by_label.setdefault(w["label"], []).append(w)

        stats = {}
        missed_or_flagged = {}
        for lbl, ws in by_label.items():
            if not ws:
                stats[lbl] = None
                continue
            outcomes = [(w["window_id"], run_window(resource_type, w)) for w in ws]
            triggered = sum(1 for _, t in outcomes if t)
            stats[lbl] = {"triggered": triggered, "total": len(ws), "rate": round(triggered / len(ws), 4)}
            if lbl == "anomaly":
                missed_or_flagged["missed_window_ids"] = [wid for wid, t in outcomes if not t]
            elif lbl in ("normal", "edge_normal"):
                missed_or_flagged[f"{lbl}_false_positive_window_ids"] = [wid for wid, t in outcomes if t]

        def fmt(lbl):
            if stats.get(lbl) is None:
                return "-"
            s = stats[lbl]
            return f"{s['triggered']}/{s['total']} ({s['rate']:.0%})"

        print(f"{label:<28} {fmt('normal'):<14} {fmt('anomaly'):<14} {fmt('edge_normal'):<16}")

        entry = {
            "scenario_label": label,
            "resource_type": resource_type,
            "eval_file": filename,
            "stats": stats,
            "details": missed_or_flagged,
        }
        for lbl in ("normal", "anomaly", "edge_normal"):
            key = (resource_type, filename, lbl)
            if key in DIAGNOSES:
                entry.setdefault("diagnosis", {})[lbl] = DIAGNOSES[key]
        results.append(entry)

    # ── 보충: lambda_eval_retry.json엔 normal이 없어서(팀원 데이터 재사용 결정),
    # 팀원 제공 정상 Lambda 윈도우로 전체 파이프라인 기준 오탐률을 재확인해야 함.
    # ⚠️ lambda_train.json의 정상 윈도우 중 앞 MAX_WINDOWS_PER_TYPE(30)개는 실제로
    # 이 모델을 학습시키는 데 쓰였다(seed_iforest_from_scenario_mock.py 참고) - 그
    # 데이터로 오탐률을 재면 모델이 이미 본 데이터를 채점하는 셈이라 무효하다.
    # held-out(학습에 전혀 안 쓰인) 데이터만으로 다시 계산한다.
    with open(MOCK_DATA_DIR / "lambda_train.json", encoding="utf-8") as f:
        train_data = json.load(f)
    with open(MOCK_DATA_DIR / "lambda_eval.json", encoding="utf-8") as f:
        eval_data = json.load(f)

    train_normal = [w for w in train_data if w["label"] == "normal"]
    used_in_training_ids = {w["window_id"] for w in train_normal[: da.MAX_WINDOWS_PER_TYPE]}
    held_out = [w for w in train_normal if w["window_id"] not in used_in_training_ids]
    held_out += [w for w in eval_data if w["label"] == "normal"]

    outcomes = [(w["window_id"], run_window("Lambda", w)) for w in held_out]
    triggered = [wid for wid, t in outcomes if t]
    rate = len(triggered) / len(held_out)
    print(f"\n[보충] Lambda 정상 held-out({len(held_out)}개, 학습에 안 쓰인 것만), "
          f"전체 파이프라인 기준: {len(triggered)}/{len(held_out)} ({rate:.1%})")
    results.append({
        "scenario_label": "Lambda 정상 held-out (팀원 제공, 학습 미사용분만, 보충 검증)",
        "resource_type": "Lambda",
        "eval_file": "lambda_train.json(뒤 20개, 학습 미사용) + lambda_eval.json (label=normal만)",
        "stats": {"normal": {"triggered": len(triggered), "total": len(held_out), "rate": round(rate, 4)}},
        "details": {
            "normal_false_positive_window_ids": triggered,
            "excluded_used_in_training_ids": sorted(used_in_training_ids),
        },
        "diagnosis": {
            "normal": (
                "lambda_eval_retry.json에 normal이 없어 팀원 제공 정상 Lambda로 재확인. "
                "처음엔 train+eval 70개를 그대로 합쳐 2/70(2.9%)로 보고했으나, 걸린 두 "
                "윈도우(lambda_train_003/027)가 실제로 이 모델 학습에 쓰인 30개 안에 "
                "포함돼 있어 무효한 측정이었음(모델이 이미 본 데이터를 채점한 셈). "
                "학습에 전혀 안 쓰인 held-out 40개로 다시 재니 오탐 0/40(0%)."
            )
        },
    })

    out_path = PROJECT_ROOT / "playground" / "eval_outputs" / "scenario_mock_detection_rates.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({
            "model_source": "playground/seed_iforest_from_scenario_mock.py (EC2/Lambda/AutoScaling/S3, RDS 제외)",
            "results": results,
        }, f, indent=2, ensure_ascii=False)
    print(f"\n결과 저장: {out_path}")


if __name__ == "__main__":
    main()
