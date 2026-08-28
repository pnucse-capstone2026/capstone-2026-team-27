"""
pipeline/detection_agent.py (박소영)

3.3.1 Detection Agent (이상 탐지)
- Z-score 기반 탐지 (단기 스파이크 대응) + Isolation Forest 탐지 (다변량 복합 드리프트 대응)
  → 두 알고리즘을 병렬 적용하고 OR 앙상블로 결합.

⚠️ 현재 AWS 미연동 상태
- 실제로는 CloudWatch에서 EC2/Lambda/S3/RDS 지표를 30분 슬라이딩 윈도우로 가져와야 하지만,
  지금은 state["raw_metrics"]로 전달되는 윈도우 데이터를 그대로 사용한다.
- Isolation Forest 모델은 리소스 타입별로 파일(pickle)에 캐싱해두고, "모델이 확신하는
  정상 윈도우"만 골라 학습 버퍼에 누적하며 5개 쌓일 때마다 재학습한다 (자기참조 학습,
  아래 학습 버퍼 섹션 참고). 타입당 최대 MAX_WINDOWS_PER_TYPE개만 유지하고 오래된
  것부터 자동으로 교체(FIFO)하는 방식으로 concept drift에 대응한다 — 예전엔 24시간마다
  버퍼 전체를 통째로 리셋하는 방식이었는데, 리셋될 때마다 콜드 스타트(창 1개 학습)로
  되돌아가 정확도가 급락하는 문제가 있어 제거했다.
"""

from __future__ import annotations

import logging
import os
import pickle
import time
from datetime import datetime, timezone
from typing import Iterator, Optional

import numpy as np
from sklearn.ensemble import IsolationForest

import typing
from schema.state import PipelineState

logger = logging.getLogger(__name__)

# ── 보고서 3.3.1 기준 파라미터 ────────────────────────────────────────────────
# τ=0.6, k=3.0 → Phase 5 파라미터 튜닝(playground/tune_detection_parameters.py)에서
# 합성 평가 데이터셋(435개) 기준 정확도가 76.78%에 머물러 80% 목표에 미달했던 것을,
# "학습 버퍼를 리소스 타입당 다수 윈도우로 확장"(아래 학습 버퍼 섹션 참고)하면서
# 재튜닝해 0.5 / 2.75로 변경 — 정확도 80.46%, 결합(다변량) 이상 탐지율 99.29% 확인.
Z_SCORE_THRESHOLD = 2.75                     # k = 2.75 (기존 3.0)
Z_SCORE_EPSILON = 1e-9                       # ε (분모 0 방지)
IFOREST_THRESHOLD = 0.5                      # τ = 0.5 (기존 0.6)
IFOREST_CONTAMINATION = 0.1                  # 스코어의 창 내부 min-max 정규화 특성상 결과에 영향 없음 (Phase 5에서 확인)
IFOREST_RANDOM_STATE = 42
IFOREST_MODEL_DIR = os.environ.get("PIPELINE_MODEL_DIR", "models")
MIN_POINTS_FOR_IFOREST = 5

# 알림 판단(지속성 체크)용: 최근 이만큼의 연속 시점이 전부 임계값을 넘어야 트리거.
# period_seconds=300초(5분) 기준 3개 = 15분 — 순간적인 노이즈 튐 한 번에는 반응하지 않되,
# 너무 오래 기다리지도 않는 절충값으로 임의 설정. period_seconds를 바꾸면 실제 지속 시간도
# 같이 바뀐다는 점 감안. (참고: Nagios류 모니터링의 기본 재확인 횟수 3회, Prometheus 흔한
# `for: 15m` 관례와 유사한 수준)
PERSISTENCE_WINDOW_POINTS = 3

# ── 학습 버퍼 정책 (리소스 타입당 다수 정상 윈도우 누적) ────────────────────────
MAX_WINDOWS_PER_TYPE = 30          # 타입당 최대 보관 윈도우 수 (Phase 5 실험값)
RETRAIN_EVERY_N_NEW_WINDOWS = 5    # 새 윈도우가 이만큼 쌓일 때마다 재학습
BUFFER_SCORE_MARGIN = 0.7          # 버퍼링 기준 = 탐지 임계값의 70% (탐지보다 보수적)
BUFFER_ZSCORE_MARGIN = 0.7

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

    ⚠️ detection_node의 알림 판단에는 안 쓰임(_zscore_check_persistent 사용) — 이 함수는
    학습 버퍼 채택 여부(_zscore_max) 판단 전용. 버퍼에는 윈도우 전체(30개 행)가 그대로
    들어가므로, 마지막 값은 정상이어도 윈도우 중간에 스파이크가 섞여 있으면 그 윈도우를
    "정상"으로 학습에 반영하면 안 되기 때문에 window-max를 유지한다.
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


def _zscore_check_persistent(
    values: list[float], k: int = PERSISTENCE_WINDOW_POINTS
) -> tuple[float, bool]:
    """윈도우 전체로 μ, σ를 구하되, 트리거 판단은 최근 k개 시점이 "전부" 임계값을
    넘어야 한다(지속성 체크): Z_i = (x_i - μ) / (σ + ε), i는 최근 k개 시점.
    보고용 점수는 그중 가장 최근(마지막) 시점의 |Z|를 반환한다. k=1이면 마지막
    시점 하나만 보는 것과 동일.

    detection_node의 알림 판단 전용. 두 가지 극단을 피하려고 만들었다:
    - window-max(_zscore_check) 그대로 쓰면, 스파이크가 지나가고 값이 정상으로
      돌아와도 그 스파이크가 윈도우에서 밀려날 때까지(최대 2.5시간, n_points=30 ×
      period_seconds=300초) 계속 이상으로 잡힘.
    - 마지막 1개 시점만 보면(k=1), 반대로 순간적인 노이즈 튐 한 번에도 바로
      반응해서 알림이 튀는(flapping) 문제가 있음.
    최근 PERSISTENCE_WINDOW_POINTS(기본 3)개, 즉 15분(period_seconds=300초 기준)
    동안 연속으로 임계값을 넘었을 때만 트리거하도록 절충했다.
    """
    arr = np.asarray(values, dtype=float)
    if arr.size < 2:
        return 0.0, False

    mu = arr.mean()
    sigma = arr.std()

    z_scores_abs = np.abs((arr - mu) / (sigma + Z_SCORE_EPSILON))
    k_eff = min(k, arr.size)
    recent = z_scores_abs[-k_eff:]

    is_triggered = bool(np.all(recent > Z_SCORE_THRESHOLD))
    return float(recent[-1]), is_triggered

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
    """캐시된 (model, feature_keys) 로드. 캐시가 없으면 None.

    ⚠️ 예전엔 "24시간 지나면 캐시 전체 무효화"가 있었는데 제거함 — 그 방식은 리셋될
    때마다 학습 버퍼가 통째로 비워져서 콜드 스타트(창 1개로만 학습) 상태로 되돌아가고,
    그때마다 정확도가 급락하는 문제가 있었다 (Phase 5에서 확인한 "창 1개 학습 = 정상
    32.7% 오탐" 문제가 재발). 대신 MAX_WINDOWS_PER_TYPE 기반 FIFO(오래된 윈도우부터
    자동 교체)가 이미 concept drift를 점진적으로, 급락 없이 처리해주고 있어서 이걸로 충분.
    """
    path = _model_path(resource_type)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "rb") as f:
            model, feature_keys, _trained_at = pickle.load(f)
    except Exception:
        return None
    return model, feature_keys


def _save_model(resource_type: str, model: IsolationForest, feature_keys: list[str]) -> None:
    with open(_model_path(resource_type), "wb") as f:
        pickle.dump((model, feature_keys, time.time()), f)


# ── 통합 모델 학습 버퍼 ────────────────────────────────────────────────────────
# 통합 모델은 리소스 타입에 무관하게 같은 feature 스키마(ALL_METRICS)를 쓰기 때문에,
# "캐시된 feature_keys가 지금 feature_keys와 같은가"만으로는 재학습 여부를 절대
# 판단할 수 없었다 (항상 같아서 처음 학습된 이후로 영원히 재학습이 안 됨 — 버그, 이미 수정).
#
# 그 수정만으로는(리소스 타입당 대표 윈도우 딱 1개) 학습 데이터가 너무 빈약해서
# 정상 샘플의 32.7%가 오탐되는 문제가 있었다 (playground/tune_detection_parameters.py).
# 리소스 타입당 정상 윈도우를 다수(최대 MAX_WINDOWS_PER_TYPE개) 누적해서 학습하면
# 정확도가 크게 개선됨을 확인했는데(76.78% → 80.46%), 실서비스에는 "이게 정상인지"
# 알려주는 정답 라벨이 없다. 그래서 "지금 모델이 이상이라고 판단하지 않은(그것도
# 탐지 임계값보다 더 보수적인 기준으로) 윈도우"를 잠정적 정상으로 간주해 버퍼에
# 쌓는 자기참조(self-referential) 방식을 쓴다 — 실제 정확도는
# playground/validate_self_referential_buffer.py로 라벨 없이도 검증함.

def _buffer_path() -> str:
    os.makedirs(IFOREST_MODEL_DIR, exist_ok=True)
    return os.path.join(IFOREST_MODEL_DIR, "iforest_unified_train_buffer.pkl")


def _load_training_buffer() -> tuple[dict[str, list[np.ndarray]], int]:
    """반환: (리소스 타입별 학습 윈도우 리스트, 마지막 재학습 이후 새로 쌓인 개수)."""
    path = _buffer_path()
    if not os.path.exists(path):
        return {}, 0
    try:
        with open(path, "rb") as f:
            buffer_by_type, pending_count = pickle.load(f)
        return buffer_by_type, pending_count
    except Exception:
        return {}, 0


def _save_training_buffer(buffer_by_type: dict[str, list[np.ndarray]], pending_count: int) -> None:
    with open(_buffer_path(), "wb") as f:
        pickle.dump((buffer_by_type, pending_count), f)


def _fit_and_cache_unified(buffer: np.ndarray) -> IsolationForest:
    model = IsolationForest(
        contamination=IFOREST_CONTAMINATION,
        random_state=IFOREST_RANDOM_STATE,
    )
    model.fit(buffer)
    _save_model(IFOREST_UNIFIED_MODEL_NAME, model, ALL_METRICS)
    return model


def _zscore_max(metrics: dict[str, list[float]]) -> float:
    """Z-score 대상 지표들 중 |Z|의 최댓값. detection_node의 트리거 판단과 별개로,
    학습 버퍼링 여부를 결정할 때도 재사용한다."""
    z_max = 0.0
    for metric_name, values in metrics.items():
        if metric_name not in Z_SCORE_TARGET_METRICS:
            continue
        z, _is_triggered = _zscore_check(values)
        z_max = max(z_max, z)
    return z_max


def _normalized_scores(
    model: IsolationForest, resource_type: str, metrics: dict[str, list[float]]
) -> np.ndarray:
    """윈도우 전체에 대해 IsolationForest decision_function을 창 내부 min-max로
    0~1 정규화한 배열을 반환 (1에 가까울수록 이상). _score_with_model과
    _iforest_score_and_trigger가 공유하는 정규화 로직."""
    X = build_unified_feature_matrix(resource_type, metrics)
    raw_scores = model.decision_function(X)  # 낮을수록 이상치

    s_min, s_max = raw_scores.min(), raw_scores.max()
    if s_max == s_min:
        return np.zeros_like(raw_scores)

    return np.clip((s_max - raw_scores) / (s_max - s_min), 0.0, 1.0)


def _score_with_model(model: IsolationForest, resource_type: str, metrics: dict[str, list[float]]) -> float:
    return float(_normalized_scores(model, resource_type, metrics)[-1])


def _get_or_train_iforest(
    resource_type: str, metrics: dict[str, list[float]]
) -> Optional[IsolationForest]:
    """캐시된 모델이 있으면 재사용, 없으면(콜드 스타트) 학습 후 캐시 저장.
    새로운 리소스 타입이 처음 보이거나 버퍼에 새 윈도우가 쌓이면 그때그때 재학습.

    ⚠️ AWS 미연동 상태이므로 지금은 "재학습용 데이터" = 지금까지 들어온 윈도우 중
       모델이 잠정적으로 정상이라고 판단한 것들을 리소스 타입별로 모은 누적 버퍼.
       AWS 연동 후엔 이 버퍼링 정책을 유지하면서 데이터 소스만 확장하면 된다.
    """
    cached = _load_cached_model(IFOREST_UNIFIED_MODEL_NAME)
    n = len(next(iter(metrics.values())))

    if cached is not None:
        model, cached_keys = cached
        if cached_keys == ALL_METRICS:
            buffer_by_type, pending_count = _load_training_buffer()

            if n >= MIN_POINTS_FOR_IFOREST:
                provisional_score = _score_with_model(model, resource_type, metrics)
                z_max = _zscore_max(metrics)
                believed_normal = (
                    provisional_score < IFOREST_THRESHOLD * BUFFER_SCORE_MARGIN
                    and z_max < Z_SCORE_THRESHOLD * BUFFER_ZSCORE_MARGIN
                )

                if believed_normal:
                    bucket = buffer_by_type.setdefault(resource_type, [])
                    bucket.append(build_unified_feature_matrix(resource_type, metrics))
                    if len(bucket) > MAX_WINDOWS_PER_TYPE:
                        del bucket[: len(bucket) - MAX_WINDOWS_PER_TYPE]  # FIFO — 오래된 것부터 제거
                    pending_count += 1
                    logger.info(
                        "[iforest_buffer] 채택 resource_type=%s score=%.4f z_max=%.4f "
                        "버퍼크기=%d pending=%d",
                        resource_type, provisional_score, z_max, len(bucket), pending_count,
                    )
                else:
                    logger.info(
                        "[iforest_buffer] 제외(경계/이상 의심) resource_type=%s score=%.4f z_max=%.4f",
                        resource_type, provisional_score, z_max,
                    )

                _save_training_buffer(buffer_by_type, pending_count)

                if pending_count >= RETRAIN_EVERY_N_NEW_WINDOWS and buffer_by_type:
                    combined = np.vstack([np.vstack(v) for v in buffer_by_type.values() if v])
                    model = _fit_and_cache_unified(combined)
                    _save_training_buffer(buffer_by_type, 0)
                    logger.info(
                        "[iforest_buffer] 재학습 완료 총 윈도우=%d (타입별=%s)",
                        combined.shape[0], {k: len(v) for k, v in buffer_by_type.items()},
                    )

            return model

    if n < MIN_POINTS_FOR_IFOREST:
        return None

    X = build_unified_feature_matrix(resource_type, metrics)
    model = _fit_and_cache_unified(X)
    _save_training_buffer({resource_type: [X]}, 0)
    return model


def _iforest_score(resource_type: str, metrics: dict[str, list[float]]) -> float:
    """CPU, 네트워크 입출력, 비용, 호출 횟수 등 해당 리소스의 모든 지표를
    하나의 다변량 feature 벡터로 구성해 Isolation Forest에 입력하고,
    최신 시점의 이상 점수를 0~1로 정규화해서 반환 (1에 가까울수록 이상).

    ⚠️ detection_node에서는 안 쓰임(_iforest_score_and_trigger 사용) — 이 함수는
    playground 평가/검증 스크립트 전용으로 남겨둠(각 스크립트가 "호출 1번 = 모델
    로드+버퍼 갱신 1번"을 전제로 하고 있어서 시그니처를 그대로 유지).
    """
    model = _get_or_train_iforest(resource_type, metrics)
    if model is None:
        return 0.0
    return _score_with_model(model, resource_type, metrics)


def _iforest_score_and_trigger(
    resource_type: str, metrics: dict[str, list[float]], k: int = PERSISTENCE_WINDOW_POINTS
) -> tuple[float, bool]:
    """detection_node 전용: 최신 시점의 이상 점수(리포팅용)와, 최근 k개 시점이
    "전부" 임계값을 넘었는지(트리거 판단, 지속성 체크)를 함께 반환한다.

    ⚠️ _iforest_score를 두 번(점수용 1번 + 트리거용 1번) 부르지 않는 이유:
    _get_or_train_iforest는 호출할 때마다 학습 버퍼를 갱신하는 부수효과가 있어서,
    같은 요청 안에서 두 번 부르면 같은 윈도우가 버퍼에 중복 반영되거나 재학습
    카운트가 두 배로 올라가는 버그가 생긴다. 모델을 한 번만 불러와 재사용한다.
    """
    model = _get_or_train_iforest(resource_type, metrics)
    if model is None:
        return 0.0, False

    normalized = _normalized_scores(model, resource_type, metrics)
    latest_score = float(normalized[-1])

    k_eff = min(k, len(normalized))
    is_triggered = bool(np.all(normalized[-k_eff:] > IFOREST_THRESHOLD))
    return latest_score, is_triggered


# ── Phase 3: SHAP 해석가능성 (평가/설명 전용 — detection_node 프로덕션 경로엔 안 쓰임) ──
# IsolationForest는 비지도 모델이라 "왜 이상이라고 판단했는지"를 스스로 설명 못 한다.
# SHAP(TreeExplainer)로 각 피처가 최종 이상 점수에 얼마나/어느 방향으로 기여했는지를
# 사후적으로 계산해서, "이 케이스에서 어떤 지표가 결정적이었는지" 보고서용으로 뽑는다.

def _unified_feature_names() -> list[str]:
    """build_unified_feature_matrix가 만드는 컬럼 순서와 1:1로 대응하는 이름 목록."""
    names: list[str] = []
    for m in ALL_METRICS:
        names.append(f"{m}_value")
        names.append(f"{m}_mask")
    for rt in RESOURCE_TYPES:
        names.append(f"onehot_{rt}")
    return names


def explain_iforest(
    resource_type: str, metrics: dict[str, list[float]], model: Optional[IsolationForest] = None
) -> dict[str, float]:
    """윈도우의 마지막 시점(=_iforest_score가 실제로 이상 여부를 판단하는 시점)에 대한
    피처별 SHAP 기여도를 전부(값/마스크/원-핫 컬럼 포함) 반환한다.
    model을 안 넘기면 캐시된(또는 새로 학습된) 통합 모델을 그대로 사용한다.
    """
    import shap  # 평가 전용 함수라 지연 import — 프로덕션 detection_node 경로엔 의존성 안 걸리게 함

    if model is None:
        model = _get_or_train_iforest(resource_type, metrics)
    if model is None:
        return {}

    X = build_unified_feature_matrix(resource_type, metrics)
    feature_names = _unified_feature_names()

    explainer = shap.TreeExplainer(model)
    shap_values = np.asarray(explainer.shap_values(X))

    last_point_shap = shap_values[-1]
    return dict(zip(feature_names, (float(v) for v in last_point_shap)))


def explain_iforest_top_features(
    resource_type: str,
    metrics: dict[str, list[float]],
    model: Optional[IsolationForest] = None,
    top_n: Optional[int] = None,
) -> dict[str, float]:
    """explain_iforest() 결과에서 실제 지표값 컬럼(_value)만 추려,
    기여도 절댓값이 큰 순서로 정렬해서 반환. mask/onehot 컬럼은 구조적 신호일 뿐
    "어떤 지표가 이상 판단에 컸는가"라는 질문과는 무관해서 제외한다.
    """
    raw = explain_iforest(resource_type, metrics, model=model)
    value_only = {
        name.removesuffix("_value"): value
        for name, value in raw.items()
        if name.endswith("_value")
    }
    ordered = dict(sorted(value_only.items(), key=lambda kv: abs(kv[1]), reverse=True))
    if top_n is not None:
        ordered = dict(list(ordered.items())[:top_n])
    return ordered


def detection_node(state: PipelineState) -> PipelineState:
    metrics = state["raw_metrics"]
    resource_type = state["resource_type"]

    # ── 1) Z-score 탐지 (비용 / 네트워크 입력 / 호출 횟수 지표만 대상, 최근 몇 시점 지속 기준) ──
    # window-max도 마지막 1개 시점도 아니고 "최근 PERSISTENCE_WINDOW_POINTS개 연속"인
    # 이유: _zscore_check_persistent 문서 참고.
    triggered_metrics: list[str] = []
    max_abs_z = 0.0

    for metric_name in metrics:
        if metric_name not in Z_SCORE_TARGET_METRICS:
            continue
        z, is_triggered = _zscore_check_persistent(metrics[metric_name])
        if is_triggered:
            triggered_metrics.append(metric_name)
        max_abs_z = max(max_abs_z, z)

    # ── 2) Isolation Forest 탐지 (해당 리소스의 모든 지표, 다변량, 마찬가지로 지속성 체크) ──
    iforest_score, iforest_triggered = _iforest_score_and_trigger(resource_type, metrics)

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