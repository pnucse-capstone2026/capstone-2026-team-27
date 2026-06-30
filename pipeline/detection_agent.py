"""
pipeline/detection_agent.py (박소영)

3.3.1 Detection Agent (이상 탐지)
- Z-score 기반 탐지 (단기 스파이크 대응) + Isolation Forest 탐지 (다변량 복합 드리프트 대응)
  → 두 알고리즘을 병렬 적용하고 OR 앙상블로 결합.

⚠️ 현재 AWS 미연동 상태
- 실제로는 CloudWatch에서 EC2/Lambda/S3/RDS 지표를 30분 슬라이딩 윈도우로 가져와야 하지만,
  지금은 state["raw_metrics"]로 전달되는 윈도우 데이터를 그대로 사용한다.
- Isolation Forest의 "24시간 주기 재학습"은 실제로는 24시간치 누적 CloudWatch 데이터로
  배치 학습하는 구조가 맞다. 지금은 그 데이터가 없으므로, 모델을 리소스 타입별로
  파일(pickle)에 캐싱해두고 "캐시가 없거나 24시간 지났으면 재학습" 만 흉내내며,
  재학습 시 학습 데이터는 그 순간 들어온 윈도우를 임시로 사용한다.
  AWS 연동 후에는 `_get_or_train_iforest`의 학습 데이터 소스만
  (현재 윈도우 → 24시간 누적 CloudWatch 데이터)로 교체하면 된다.
"""

from __future__ import annotations

import os
import pickle
import time
from typing import Optional

import numpy as np
from sklearn.ensemble import IsolationForest

from schema.state import PipelineState

# ── 보고서 3.3.1 기준 파라미터 ────────────────────────────────────────────────
Z_SCORE_THRESHOLD = 3.0                      # k = 3.0
Z_SCORE_EPSILON = 1e-9                       # ε (분모 0 방지)
IFOREST_THRESHOLD = 0.6                      # τ = 0.6
IFOREST_CONTAMINATION = 0.1
IFOREST_RANDOM_STATE = 42
IFOREST_RETRAIN_INTERVAL_SEC = 24 * 60 * 60  # 24시간 재학습 주기
IFOREST_MODEL_DIR = os.environ.get("PIPELINE_MODEL_DIR", "models")
MIN_POINTS_FOR_IFOREST = 5

# Z-score는 "비용, 네트워크 입력, 호출 횟수" 지표에만 적용 (보고서 3.3.1).
# 리소스마다 필드명이 달라 의미 단위로 매핑한다.
#   비용        → cost                  (전 리소스 공통)
#   네트워크 입력 → network_in            (EC2)
#   호출 횟수    → invocation_count       (Lambda)
#               → number_of_requests     (S3)
Z_SCORE_TARGET_METRICS = {
    "cost",
    "network_in",
    "invocation_count",
    "number_of_requests",
}


def _zscore_check(values: list[float]) -> tuple[float, bool]:
    """슬라이딩 윈도우 전체로 μ, σ를 구하고, 윈도우 내 각 시점 x에 대해
    Z = (x - μ) / (σ + ε) 를 산출. 윈도우 내 |Z|의 최댓값이 k(=3.0)을 넘으면 트리거.
    """
    arr = np.asarray(values, dtype=float)
    if arr.size < 2:
        return 0.0, False

    mu = arr.mean()
    sigma = arr.std()

    z_scores = (arr - mu) / (sigma + Z_SCORE_EPSILON)
    max_abs_z = float(np.max(np.abs(z_scores)))

    is_triggered = max_abs_z > Z_SCORE_THRESHOLD
    return max_abs_z, is_triggered


def _model_path(resource_type: str) -> str:
    os.makedirs(IFOREST_MODEL_DIR, exist_ok=True)
    return os.path.join(IFOREST_MODEL_DIR, f"iforest_{resource_type}.pkl")


def _load_cached_model(resource_type: str) -> Optional[tuple[IsolationForest, list[str]]]:
    """캐시된 (model, feature_keys) 로드. 캐시가 없거나 24시간 지났으면 None."""
    path = _model_path(resource_type)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "rb") as f:
            model, feature_keys, trained_at = pickle.load(f)
    except Exception:
        return None
    if time.time() - trained_at > IFOREST_RETRAIN_INTERVAL_SEC:
        return None  # 재학습 주기 도달 → 캐시 무효화
    return model, feature_keys


def _save_model(resource_type: str, model: IsolationForest, feature_keys: list[str]) -> None:
    with open(_model_path(resource_type), "wb") as f:
        pickle.dump((model, feature_keys, time.time()), f)


def _get_or_train_iforest(
    resource_type: str, metrics: dict[str, list[float]]
) -> tuple[Optional[IsolationForest], list[str]]:
    """24시간 캐시 모델이 있으면 재사용, 없거나 만료됐으면 재학습 후 캐시 저장.

    ⚠️ AWS 미연동 상태이므로 지금은 "재학습용 데이터" = 현재 들어온 윈도우.
       AWS 연동 후엔 여기서 24시간치 누적 베이스라인 데이터를 가져오도록
       데이터 소스만 바꾸면 된다 (인터페이스는 그대로 유지).
    """
    feature_keys = sorted(metrics.keys())  # CPU, 네트워크 입/출력, 비용, 호출 횟수 등 전부 포함

    cached = _load_cached_model(resource_type)
    if cached is not None:
        model, cached_keys = cached
        if cached_keys == feature_keys:
            return model, feature_keys

    lengths = {len(metrics[k]) for k in feature_keys}
    if len(lengths) != 1 or min(lengths) < MIN_POINTS_FOR_IFOREST:
        return None, feature_keys  # 데이터 부족 → 학습 보류

    X = np.column_stack([metrics[k] for k in feature_keys])
    model = IsolationForest(
        contamination=IFOREST_CONTAMINATION,
        random_state=IFOREST_RANDOM_STATE,
    )
    model.fit(X)
    _save_model(resource_type, model, feature_keys)
    return model, feature_keys


def _iforest_score(resource_type: str, metrics: dict[str, list[float]]) -> float:
    """CPU, 네트워크 입출력, 비용, 호출 횟수 등 해당 리소스의 모든 지표를
    하나의 다변량 feature 벡터로 구성해 Isolation Forest에 입력하고,
    최신 시점의 이상 점수를 0~1로 정규화해서 반환 (1에 가까울수록 이상).
    """
    model, feature_keys = _get_or_train_iforest(resource_type, metrics)
    if model is None:
        return 0.0

    X = np.column_stack([metrics[k] for k in feature_keys])
    raw_scores = model.decision_function(X)  # 낮을수록 이상치
    latest_raw = raw_scores[-1]

    s_min, s_max = raw_scores.min(), raw_scores.max()
    if s_max == s_min:
        return 0.0

    normalized = (s_max - latest_raw) / (s_max - s_min)
    return float(np.clip(normalized, 0.0, 1.0))


def detection_node(state: PipelineState) -> PipelineState:
    metrics = state["raw_metrics"]
    resource_type = state["resource_type"]

    # ── 1) Z-score 탐지 (비용 / 네트워크 입력 / 호출 횟수 지표만 대상) ──────────
    triggered_metrics: list[str] = []
    max_abs_z = 0.0

    for metric_name in metrics:
        if metric_name not in Z_SCORE_TARGET_METRICS:
            continue
        z, is_triggered = _zscore_check(metrics[metric_name])
        if is_triggered:
            triggered_metrics.append(metric_name)
        max_abs_z = max(max_abs_z, z)

    # ── 2) Isolation Forest 탐지 (해당 리소스의 모든 지표, 다변량) ────────────
    iforest_score = _iforest_score(resource_type, metrics)
    iforest_triggered = iforest_score > IFOREST_THRESHOLD

    # ── 3) OR 앙상블 결합 ─────────────────────────────────────────────────
    anomaly_flag = bool(triggered_metrics) or iforest_triggered

    state["anomaly_flag"] = anomaly_flag
    state["anomaly_score_zscore"] = round(max_abs_z, 4)
    state["anomaly_score_iforest"] = round(iforest_score, 4)
    state["triggered_metrics"] = triggered_metrics

    return state