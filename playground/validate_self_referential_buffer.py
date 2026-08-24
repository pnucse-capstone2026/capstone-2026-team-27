"""
playground/validate_self_referential_buffer.py

배포 전 검증: pipeline/detection_agent.py에 실제로 반영한 "자기참조 학습 버퍼"
(정답 라벨 없이, 모델 스스로 정상이라 판단한 윈도우만 학습에 반영)가, Phase 5에서
정답 라벨을 미리 알고 진행했던 "치팅" 버전만큼 정확도가 나오는지 확인한다.

방식: eval_dataset.json(435개)을 실제 프로덕션 함수(_iforest_score, _zscore_check)로
"온라인"으로(한 샘플씩 순서대로, 그 시점까지 쌓인 버퍼/모델만 사용해서) 처리하며
바로바로 예측한다 — Phase 5처럼 "결과가 다 나온 뒤 완성된 모델"로 전체를 채점하는
것과 달리, 이건 실제 운영 상황(데이터가 시간순으로 하나씩 들어옴)을 그대로 재현한다.

두 가지로 검증:
  1) eval_dataset.json 원래 순서 (타입별로 정상이 먼저 오는, 버퍼 워밍업에 유리한 순서)
  2) 무작위로 섞은 순서 (버퍼 워밍업이 불리한, 더 현실적인/가혹한 조건)

마지막으로 결합(다변량) 이상 280개도 온라인으로 워밍업된 상태에서 재확인한다.

[실행 방법]
  프로젝트 루트에서: python playground/validate_self_referential_buffer.py
"""

from __future__ import annotations

import json
import logging
import random
import shutil
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pipeline.detection_agent as da
from playground.test_iforest_joint_anomaly import generate_joint_samples

logging.getLogger("pipeline.detection_agent").setLevel(logging.WARNING)  # 버퍼 로그는 끔 (너무 많음)

DATASET_PATH = PROJECT_ROOT / "playground" / "eval_outputs" / "eval_dataset.json"
RESULT_PATH = PROJECT_ROOT / "playground" / "eval_outputs" / "phase5b_online_validation.json"
MODEL_DIR = str(PROJECT_ROOT / ".tmp_online_validation_models")


def _confusion(y_true: list[bool], y_pred: list[bool]) -> dict:
    tp = sum(1 for t, p in zip(y_true, y_pred) if t and p)
    fp = sum(1 for t, p in zip(y_true, y_pred) if not t and p)
    tn = sum(1 for t, p in zip(y_true, y_pred) if not t and not p)
    fn = sum(1 for t, p in zip(y_true, y_pred) if t and not p)
    accuracy = (tp + tn) / len(y_true) if y_true else 0.0
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    return {"tp": tp, "fp": fp, "tn": tn, "fn": fn,
            "accuracy": round(accuracy, 4), "precision": round(precision, 4),
            "recall": round(recall, 4), "f1": round(f1, 4)}


def _run_online(samples: list[dict]) -> tuple[list[bool], list[bool]]:
    """samples를 순서대로 실제 프로덕션 함수로 하나씩 처리 (온라인 시뮬레이션)."""
    if Path(MODEL_DIR).exists():
        shutil.rmtree(MODEL_DIR)
    da.IFOREST_MODEL_DIR = MODEL_DIR

    y_true, y_pred = [], []
    for sample in samples:
        rt = sample["resource_type"]
        metrics = sample["raw_metrics"]

        z_max = da._zscore_max(metrics)
        z_triggered = z_max > da.Z_SCORE_THRESHOLD
        iforest_score = da._iforest_score(rt, metrics)  # 내부에서 버퍼링/재학습까지 수행
        anomaly_flag = z_triggered or (iforest_score > da.IFOREST_THRESHOLD)

        y_true.append(sample["ground_truth_anomaly"])
        y_pred.append(anomaly_flag)

    shutil.rmtree(MODEL_DIR, ignore_errors=True)
    return y_true, y_pred


def _run_online_joint(joint_samples: list[dict]) -> tuple[int, int]:
    """결합 이상 샘플들을 온라인으로 처리 (별도의 새 워밍업부터). (caught, total) 반환."""
    if Path(MODEL_DIR).exists():
        shutil.rmtree(MODEL_DIR)
    da.IFOREST_MODEL_DIR = MODEL_DIR

    caught = 0
    total = 0
    for sample in joint_samples:
        rt = sample["resource_type"]
        metrics = sample["raw_metrics"]

        z_max = da._zscore_max(metrics)
        if z_max > da.Z_SCORE_THRESHOLD:
            continue  # z-score가 이미 잡은 건 "z-score-blind" 집계에서 제외

        total += 1
        iforest_score = da._iforest_score(rt, metrics)
        if iforest_score > da.IFOREST_THRESHOLD:
            caught += 1

    shutil.rmtree(MODEL_DIR, ignore_errors=True)
    return caught, total


def main() -> None:
    if not DATASET_PATH.exists():
        raise SystemExit(f"{DATASET_PATH}가 없습니다. 먼저 generate_eval_dataset.py를 실행하세요.")

    with open(DATASET_PATH, "r", encoding="utf-8") as f:
        dataset = json.load(f)

    print(f"온라인 검증 — 실제 프로덕션 코드로 {len(dataset)}개 샘플을 순서대로 처리\n")

    # ── 1) 원래 순서 ──────────────────────────────────────────────────────────
    y_true, y_pred = _run_online(dataset)
    result_in_order = _confusion(y_true, y_pred)
    print("=" * 78)
    print("[1] 원래 순서 (타입별로 정상이 먼저 오는 순서)")
    print("=" * 78)
    print(f"  {result_in_order}")

    # ── warm-up 효과 확인: 앞 절반 vs 뒷 절반 ────────────────────────────────
    half = len(dataset) // 2
    first_half = _confusion(y_true[:half], y_pred[:half])
    second_half = _confusion(y_true[half:], y_pred[half:])
    print(f"\n  앞 절반(워밍업 전 비중 높음)  정확도: {first_half['accuracy']}")
    print(f"  뒷 절반(워밍업 된 후)        정확도: {second_half['accuracy']}")

    # ── 2) 무작위로 섞은 순서 (더 가혹한 조건) ───────────────────────────────
    shuffled = dataset[:]
    random.Random(42).shuffle(shuffled)
    y_true_s, y_pred_s = _run_online(shuffled)
    result_shuffled = _confusion(y_true_s, y_pred_s)
    print("\n" + "=" * 78)
    print("[2] 무작위로 섞은 순서 (정상/이상이 뒤섞여 들어오는 현실적인 조건)")
    print("=" * 78)
    print(f"  {result_shuffled}")

    # ── 3) 결합(다변량) 이상 — 온라인 워밍업 후 ──────────────────────────────
    joint_samples = generate_joint_samples()
    warmup_then_joint = dataset + joint_samples  # 먼저 435개로 워밍업 후 결합 이상 흘려보냄

    if Path(MODEL_DIR).exists():
        shutil.rmtree(MODEL_DIR)
    da.IFOREST_MODEL_DIR = MODEL_DIR
    for sample in dataset:  # 워밍업
        da._iforest_score(sample["resource_type"], sample["raw_metrics"])

    caught, total = 0, 0
    for sample in joint_samples:
        rt = sample["resource_type"]
        metrics = sample["raw_metrics"]
        z_max = da._zscore_max(metrics)
        if z_max > da.Z_SCORE_THRESHOLD:
            continue
        total += 1
        score = da._iforest_score(rt, metrics)
        if score > da.IFOREST_THRESHOLD:
            caught += 1
    shutil.rmtree(MODEL_DIR, ignore_errors=True)

    print("\n" + "=" * 78)
    print("[3] 결합(다변량) 이상 — 435개로 워밍업 후 온라인 처리")
    print("=" * 78)
    print(f"  z-score-blind 케이스: {total}개, IForest가 잡은 것: {caught}개 "
          f"({caught/total*100:.2f}%)" if total else "  (해당 없음)")

    result = {
        "in_order": result_in_order,
        "in_order_first_half": first_half,
        "in_order_second_half": second_half,
        "shuffled": result_shuffled,
        "joint_after_warmup": {
            "total": total, "caught": caught,
            "rate": round(caught / total, 4) if total else None,
        },
    }
    RESULT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(RESULT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"\n저장 완료: {RESULT_PATH}")


if __name__ == "__main__":
    main()
