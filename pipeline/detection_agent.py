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
from datetime import datetime, timezone
from typing import Iterator, Optional

import numpy as np
from sklearn.ensemble import IsolationForest

import typing
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

# ── Isolation Forest 통합 모델용 스키마 (state.py에서 자동 추출) ──────────
_raw_metrics_type = typing.get_type_hints(PipelineState)["raw_metrics"]
_metric_typeddicts = typing.get_args(_raw_metrics_type)

_RESOURCE_TYPEDDICTS: dict[str, type] = {
    td.__name__.removesuffix("Metrics"): td
    for td in _metric_typeddicts
}

RESOURCE_TYPES: list[str] = list(_RESOURCE_TYPEDDICTS.keys())

_expected_resource_types = set(
    typing.get_args(typing.get_type_hints(PipelineState)["resource_type"])
)
assert set(RESOURCE_TYPES) == _expected_resource_types, (
    f"RESOURCE_TYPES 불일치: {RESOURCE_TYPES} vs {_expected_resource_types}"
)

RESOURCE_METRIC_KEYS: dict[str, list[str]] = {
    rt: list(typing.get_type_hints(td).keys())
    for rt, td in _RESOURCE_TYPEDDICTS.items()
}

_all_metrics_set = set()

for keys in RESOURCE_METRIC_KEYS.values():   # 바깥 루프: 리소스별 지표 리스트를 하나씩 꺼냄
    for metric in keys:                       # 안쪽 루프: 그 리스트 안의 지표 이름을 하나씩 꺼냄
        _all_metrics_set.add(metric)          # set에 추가 (중복이면 자동 무시됨)

ALL_METRICS: list[str] = sorted(_all_metrics_set)

IFOREST_UNIFIED_MODEL_NAME = "unified"


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

def build_unified_feature_matrix(
    resource_type: str, metrics: dict[str, list[float]]
) -> np.ndarray:
    n = len(next(iter(metrics.values())))
    cols: list[np.ndarray] = []

    for m in ALL_METRICS:
        if m in metrics:
            cols.append(np.asarray(metrics[m], dtype=float))
            cols.append(np.ones(n))
        else:
            cols.append(np.zeros(n))
            cols.append(np.zeros(n))

    for rt in RESOURCE_TYPES:
        cols.append(np.full(n, 1.0 if rt == resource_type else 0.0))

    return np.column_stack(cols)

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


# ── 통합 모델 학습 버퍼 ────────────────────────────────────────────────────────
# 통합 모델은 리소스 타입에 무관하게 같은 feature 스키마(ALL_METRICS)를 쓰기 때문에,
# "캐시된 feature_keys가 지금 feature_keys와 같은가"만으로는 재학습 여부를 절대
# 판단할 수 없었다 (항상 같아서 처음 학습된 이후로 영원히 재학습이 안 됨 — 버그).
# 대신 "지금까지 실제로 학습 데이터에 반영된 리소스 타입 집합"을 별도로 추적해서,
# 처음 보는 리소스 타입이 들어올 때마다 그 데이터를 누적 버퍼에 추가하고 재학습한다.
# (24시간 안에 최대 RESOURCE_TYPES 개수만큼만 재학습되므로 비용 부담 적음)

def _buffer_path() -> str:
    os.makedirs(IFOREST_MODEL_DIR, exist_ok=True)
    return os.path.join(IFOREST_MODEL_DIR, "iforest_unified_train_buffer.pkl")


def _load_training_buffer() -> tuple[set[str], Optional[np.ndarray]]:
    path = _buffer_path()
    if not os.path.exists(path):
        return set(), None
    try:
        with open(path, "rb") as f:
            seen_types, buffer = pickle.load(f)
        return seen_types, buffer
    except Exception:
        return set(), None


def _save_training_buffer(seen_types: set[str], buffer: np.ndarray) -> None:
    with open(_buffer_path(), "wb") as f:
        pickle.dump((seen_types, buffer), f)


def _fit_and_cache_unified(buffer: np.ndarray) -> IsolationForest:
    model = IsolationForest(
        contamination=IFOREST_CONTAMINATION,
        random_state=IFOREST_RANDOM_STATE,
    )
    model.fit(buffer)
    _save_model(IFOREST_UNIFIED_MODEL_NAME, model, ALL_METRICS)
    return model


def _get_or_train_iforest(
    resource_type: str, metrics: dict[str, list[float]]
) -> Optional[IsolationForest]:
    """24시간 캐시 모델이 있으면 재사용, 없거나 만료됐으면 재학습 후 캐시 저장.

    ⚠️ AWS 미연동 상태이므로 지금은 "재학습용 데이터" = 지금까지 들어온 리소스
       타입들의 윈도우를 모은 누적 버퍼. AWS 연동 후엔 여기서 24시간치 누적
       CloudWatch 데이터를 가져오도록 데이터 소스만 바꾸면 된다 (인터페이스는 그대로 유지).
    """
    cached = _load_cached_model(IFOREST_UNIFIED_MODEL_NAME)
    n = len(next(iter(metrics.values())))

    if cached is not None:
        model, cached_keys = cached
        if cached_keys == ALL_METRICS:
            seen_types, buffer = _load_training_buffer()
            if resource_type in seen_types:
                return model  # 이 리소스 타입 데이터는 이미 학습에 반영됨

            if n < MIN_POINTS_FOR_IFOREST:
                return model  # 새 타입이지만 데이터 부족 → 기존 모델 유지

            X_new = build_unified_feature_matrix(resource_type, metrics)
            buffer = X_new if buffer is None else np.vstack([buffer, X_new])
            seen_types = seen_types | {resource_type}

            model = _fit_and_cache_unified(buffer)
            _save_training_buffer(seen_types, buffer)
            return model

    if n < MIN_POINTS_FOR_IFOREST:
        return None

    X = build_unified_feature_matrix(resource_type, metrics)
    model = _fit_and_cache_unified(X)
    _save_training_buffer({resource_type}, X)
    return model


def _iforest_score(resource_type: str, metrics: dict[str, list[float]]) -> float:
    """CPU, 네트워크 입출력, 비용, 호출 횟수 등 해당 리소스의 모든 지표를
    하나의 다변량 feature 벡터로 구성해 Isolation Forest에 입력하고,
    최신 시점의 이상 점수를 0~1로 정규화해서 반환 (1에 가까울수록 이상).
    """
    model = _get_or_train_iforest(resource_type, metrics)
    if model is None:
        return 0.0

    X = build_unified_feature_matrix(resource_type, metrics)
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


# ── Phase 0: 여러 리소스 순차 스캔 디스패처 ───────────────────────────────────
# 여러 리소스에서 동시에 이상이 감지될 수 있는 상황에서, 한꺼번에 모아 배치로
# 넘기지 않고 하나씩 순차적으로 detection_node를 돌려서 발견 즉시 넘긴다
# (병렬 fan-out이 아니라 의도적인 순차 처리).

def _build_initial_state(resource: dict) -> PipelineState:
    """resource: {resource_id, resource_type, raw_metrics, timestamp(optional)}
    나머지 PipelineState 필드는 파이프라인 시작 전 기본값으로 채운다.
    """
    return {
        "trace_id":      None,
        "resource_id":   resource["resource_id"],
        "resource_type": resource["resource_type"],
        "raw_metrics":   resource["raw_metrics"],
        "timestamp":     resource.get("timestamp") or datetime.now(timezone.utc).isoformat(),

        "anomaly_flag":          False,
        "anomaly_score_zscore":  None,
        "anomaly_score_iforest": None,
        "triggered_metrics":     [],

        "anomaly_type":             None,
        "classification_reasoning": None,
        "interim_action_taken":     None,
        "matched_rule_id":          None,

        "candidate_actions":   [],
        "selected_action":     None,
        "risk_level":          None,
        "requires_approval":   False,
        "decision_reasoning":  None,
        "target_instance_type": None,

        "pre_action_snapshot": None,
        "action_executed":     None,
        "action_result":       None,

        "qa_passed":         None,
        "sla_check_result":  None,
        "rollback_count":    0,
        "qa_matched_rule_id": None,
        "whitelisted":       False,

        "log_entries": [],
    }


def scan_resources_sequential(resource_list: list[dict]) -> Iterator[PipelineState]:
    """
    resource_list: [{resource_id, resource_type, raw_metrics, timestamp}, ...]
    리소스를 하나씩 순서대로 detection_node에 넣고, anomaly_flag=True인 것만
    발견 즉시 yield한다. 전체를 모았다가 한 번에 넘기지 않는다.
    """
    for resource in resource_list:
        state = _build_initial_state(resource)
        result = detection_node(state)
        if result["anomaly_flag"]:
            yield result


def benchmark_iforest_inference(
    model: IsolationForest, X: np.ndarray, n_runs: int = 100, warmup: int = 10
) -> dict[str, float]:
    for _ in range(warmup):
        model.decision_function(X)

    times = []
    for _ in range(n_runs):
        t0 = time.perf_counter()
        model.decision_function(X)
        t1 = time.perf_counter()
        times.append(t1 - t0)

    times = np.asarray(times)
    return {
        "n_runs": n_runs,
        "mean_sec": float(times.mean()),
        "max_sec": float(times.max()),
        "min_sec": float(times.min()),
        "std_sec": float(times.std()),
    }