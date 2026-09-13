"""
playground/analyze_iforest_edos_real_data.py

"IForest가 (z-score 대신/추가로) EDoS 스파이크를 잡을 수 있는가?"를, AWS를 다시
띄우지 않고 이미 확보한 실측 데이터로 오프라인 검증한다.

배경:
  - autoscaling_edos_trial.py의 실측 결과(2026-09-12)에서 production(z-score) /
    iforest_only 전부 recall=0%로 나왔다.
  - z-score 실패 원인: masking effect(스파이크 자체가 창의 평균/표준편차를 끌어올려
    스파이크가 상대적으로 덜 튀어 보이게 됨) — 알려진 방법론적 한계.
  - iforest_only 실패 원인은 다른 문제: pipeline/detection_agent.py의
    MOCK_SEED_BUFFER_FROZEN=True 때문에 실측 데이터를 전혀 학습하지 않은 mock
    시드 모델을 그대로 썼다. 즉 "IForest가 시도했다가 졌다"가 아니라 "IForest가
    아예 우리 데이터를 본 적이 없다".
  - 친구가 올린 PR #24(mock_data/autoscaling_*.json)는 baseline=4대/스파이크
    10~16대라는 완전히 다른 스케일의 합성 데이터라 우리 실험 설계(baseline=1대,
    스파이크=3대, 진짜 t3.micro ASG)와 안 맞는다.

이 스크립트가 하는 일:
  1. 실측 결과 JSON(autoscaling_edos_trial__*.json)에서 각 trial의 before/after
     raw_metrics(30포인트짜리 실제 CloudWatch 값)를 그대로 가져온다.
  2. MOCK_SEED_BUFFER_FROZEN을 우회해서(_get_or_train_iforest를 안 거치고)
     detection_agent.build_unified_feature_matrix로 피처를 만들고, "정상으로 알려진
     윈도우"(모든 trial의 before + normal trial의 after)만으로 IsolationForest를
     새로 학습시킨다 — 이번엔 진짜 데이터로.
  3. anomaly trial의 after(실제 스파이크 상태) 윈도우를 이 모델에 넣어 점수/트리거
     여부를 계산하고, production(z-score)과 나란히 비교한다.

⚠️ 주의(정직하게 밝힘): normal 쪽 평가는 "학습에 쓴 데이터를 그대로 다시 채점"하는
   in-sample 평가라 FPR은 참고용이다(과소평가될 수 있음). anomaly 쪽은 학습에 전혀
   쓰이지 않은 진짜 held-out 데이터라 TP/FN 판단은 유효하다.

[실행 방법]
  python playground/analyze_iforest_edos_real_data.py \
    --input playground/eval_outputs/autoscaling_edos_trial__capacity3_wait300s_n8-5_scriptv2_20260912.json

  # 보완 시뮬레이션(선택): 정상 학습 데이터에 자연스러운 변동이 있었다면 어땠을지
  # ⚠️ 이건 실측이 아니라 합성 노이즈를 주입한 가설 검증용이다 — 결과에 "시뮬레이션"임을
  #   명시하고 실측 결과(위 기본 실행)와 절대 섞어서 보고하지 않는다.
  python playground/analyze_iforest_edos_real_data.py \
    --input playground/eval_outputs/autoscaling_edos_trial__capacity3_wait300s_n8-5_scriptv2_20260912.json \
    --inject-noise-std 0.3
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")  # Windows 콘솔 기본 cp949가 em-dash 등에서 죽는 문제 방지

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sklearn.ensemble import IsolationForest

from pipeline.detection_agent import (
    ALL_METRICS,
    IFOREST_CONTAMINATION,
    IFOREST_RANDOM_STATE,
    IFOREST_THRESHOLD,
    PERSISTENCE_WINDOW_POINTS,
    build_unified_feature_matrix,
    _normalized_scores_absolute,
)
from ec2_lambda_repeated_trial import clopper_pearson_ci

# build_unified_feature_matrix가 만드는 컬럼 순서: ALL_METRICS 각각 (value, mask) 2개씩,
# 그 뒤에 리소스타입 원핫. group_desired_capacity/group_in_service_instances의 value
# 컬럼 인덱스만 계산해둔다(노이즈를 실제 값 컬럼에만 주입하고 mask/원핫은 안 건드리기 위해).
_VALUE_COL_INDEX = {m: 2 * i for i, m in enumerate(ALL_METRICS)}
_NOISE_TARGET_METRICS = ("group_desired_capacity", "group_in_service_instances")


def _score_window(
    model: IsolationForest, resource_type: str, metrics: dict[str, list[float]],
    buffer_windows: list[np.ndarray], k: int = PERSISTENCE_WINDOW_POINTS,
) -> tuple[float, bool]:
    """2026-09-12 production 수정과 동일하게 버퍼 기준 절대 정규화를 쓴다(윈도우
    자기참조 min-max 아님) - 그래야 이 오프라인 분석이 실제 파이프라인과 같은 답을 준다."""
    normalized = _normalized_scores_absolute(model, resource_type, metrics, buffer_windows)
    latest_score = float(normalized[-1])
    k_eff = min(k, len(normalized))
    is_triggered = bool(np.all(normalized[-k_eff:] > IFOREST_THRESHOLD))
    return latest_score, is_triggered


def _inject_noise(window: np.ndarray, std: float, rng: np.random.Generator) -> np.ndarray:
    """group_desired_capacity/group_in_service_instances의 값 컬럼에만 가우시안 노이즈를
    더한다(음수 방지를 위해 0 밑으로는 clip). mask/원핫 컬럼은 그대로 둔다 — 그건 "지표가
    있다/이 리소스 타입이다"라는 구조적 신호지 실측값이 아니라서 노이즈를 줄 대상이 아님."""
    noisy = window.copy()
    for metric in _NOISE_TARGET_METRICS:
        col = _VALUE_COL_INDEX[metric]
        noisy[:, col] = np.clip(noisy[:, col] + rng.normal(0, std, size=noisy.shape[0]), 0, None)
    return noisy


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=str, required=True, help="autoscaling_edos_trial.py --run 결과 JSON 경로")
    parser.add_argument(
        "--inject-noise-std", type=float, default=0.0,
        help="0보다 크면 학습 데이터(정상 윈도우)의 capacity 값에 가우시안 노이즈(표준편차)를 "
             "주입하는 보완 시뮬레이션 모드. ⚠️ 실측이 아님 — 결과를 실측치와 섞어서 보고하지 말 것.",
    )
    parser.add_argument("--noise-seed", type=int, default=42, help="노이즈 재현성을 위한 시드")
    parser.add_argument(
        "--persistence-k", type=int, default=PERSISTENCE_WINDOW_POINTS,
        help=f"연속 몇 포인트가 임계값을 넘어야 트리거로 볼지 (기본값 {PERSISTENCE_WINDOW_POINTS}, "
             "production의 PERSISTENCE_WINDOW_POINTS와 동일값). 점수 자체가 임계값을 "
             "한 번도 못 넘으면 이 값을 낮춰도 트리거되지 않는다.",
    )
    args = parser.parse_args()

    if args.inject_noise_std > 0:
        print("=" * 70)
        print(f"⚠️  시뮬레이션 모드: 학습 데이터에 표준편차 {args.inject_noise_std}의 합성 가우시안 노이즈를 주입합니다.")
        print("   이 실행 결과는 실측이 아니며, '정상 상태에 자연스러운 변동이 있었다면'이라는")
        print("   가설을 검증하는 보완 자료입니다. 실측 결과(노이즈 없는 실행)와 절대 혼동하지 마세요.")
        print("=" * 70)

    with open(args.input, "r", encoding="utf-8") as f:
        data = json.load(f)

    trials = data["trials"]
    anomaly_trials = [t for t in trials if t["label"] == "anomaly" and "error" not in t]
    normal_trials = [t for t in trials if t["label"] == "normal" and "error" not in t]

    print("=" * 70)
    print(f"입력: {args.input}")
    print(f"anomaly trial {len(anomaly_trials)}개, normal trial {len(normal_trials)}개")
    print("=" * 70)

    # ── 1) "정상으로 알려진" 윈도우만으로 학습 데이터 구성 ──
    # anomaly trial의 before(스파이크 전, capacity=1 유지 상태)와
    # normal trial의 before+after(계속 capacity=1) 전부 정상으로 간주.
    rng = np.random.default_rng(args.noise_seed)
    train_windows = []
    for t in anomaly_trials:
        w = build_unified_feature_matrix("AutoScaling", t["before"]["raw_metrics"])
        if args.inject_noise_std > 0:
            w = _inject_noise(w, args.inject_noise_std, rng)
        train_windows.append(w)
    for t in normal_trials:
        for snapshot in ("before", "after"):
            w = build_unified_feature_matrix("AutoScaling", t[snapshot]["raw_metrics"])
            if args.inject_noise_std > 0:
                w = _inject_noise(w, args.inject_noise_std, rng)
            train_windows.append(w)

    train_matrix = np.vstack(train_windows)
    noise_label = f"노이즈 주입 시뮬레이션(std={args.inject_noise_std})" if args.inject_noise_std > 0 else "실측 데이터 그대로"
    print(f"\n학습 데이터: 윈도우 {len(train_windows)}개 x 포인트 30개 = 행 {train_matrix.shape[0]}개 "
          f"({noise_label}, MOCK_SEED_BUFFER_FROZEN 우회 - mock 시드 아님)")

    model = IsolationForest(contamination=IFOREST_CONTAMINATION, random_state=IFOREST_RANDOM_STATE)
    model.fit(train_matrix)

    # ── 2) anomaly trial의 after(진짜 스파이크 상태, held-out) 채점 ──
    print("\n--- anomaly trial의 after(held-out, 스파이크 실제 반영됨) ---")
    anomaly_results = []
    for t in anomaly_trials:
        score, triggered = _score_window(model, "AutoScaling", t["after"]["raw_metrics"], train_windows, args.persistence_k)
        z_gate = t["after"]["production"]["or_gate"]
        anomaly_results.append(triggered)
        print(f"  {t['resource']:35s} iforest_score(재학습)={score:.3f} triggered={triggered}"
              f"  | 기존 production(z-score) or_gate={z_gate}")

    # ── 3) normal trial의 after 채점 (in-sample — 참고용, 본문에 그렇게 명시할 것) ──
    print("\n--- normal trial의 after (주의: 학습에 쓰인 데이터라 in-sample, FPR 참고용) ---")
    normal_results = []
    for t in normal_trials:
        score, triggered = _score_window(model, "AutoScaling", t["after"]["raw_metrics"], train_windows, args.persistence_k)
        normal_results.append(triggered)
        print(f"  {t['resource']:35s} iforest_score(재학습)={score:.3f} triggered={triggered}")

    tp = sum(1 for r in anomaly_results if r)
    fn = sum(1 for r in anomaly_results if not r)
    fp = sum(1 for r in normal_results if r)
    tn = sum(1 for r in normal_results if not r)

    accuracy = (tp + tn) / (tp + tn + fp + fn) if (tp + tn + fp + fn) else None
    recall = tp / (tp + fn) if (tp + fn) else None
    ci = clopper_pearson_ci(tp, tp + fn) if (tp + fn) else None

    print("\n" + "=" * 70)
    print(f"[재학습된 IForest, {noise_label}] TP={tp} TN={tn} FP={fp} FN={fn}"
          f" / accuracy={accuracy:.1%} recall={recall:.1%}")
    if ci:
        lo, hi = ci
        print(f"    recall 95% CI(Clopper-Pearson) = [{lo*100:.1f}%, {hi*100:.1f}%]")
    print("주의: normal(FP/TN)은 학습에 쓰인 데이터를 다시 채점한 in-sample 평가입니다.")
    print("   실제 오탐율(FPR)은 이 값보다 높을 수 있습니다. anomaly(TP/FN)는 학습에")
    print("   전혀 안 쓰인 held-out 데이터라 이 판정은 유효합니다.")
    if args.inject_noise_std > 0:
        print(f"⚠️ 이 결과는 표준편차 {args.inject_noise_std}의 합성 노이즈를 학습 데이터에 주입한")
        print("   시뮬레이션입니다 - 실측 결과가 아니므로 보고서에 '실측'으로 표기하지 마세요.")
    print("=" * 70)


if __name__ == "__main__":
    main()
