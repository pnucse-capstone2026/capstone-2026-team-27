"""
pipeline/detection_agent.py (박소영)

3.3.1 Detection Agent (이상 탐지)
- Z-score 기반 탐지 (단기 스파이크 대응) + Isolation Forest 탐지 (다변량 복합 드리프트 대응)
  + EC2 저사용률(유휴) 절대임계값 체크 (좀비 인스턴스 대응, 신규)
  + Lambda 에러 재시도 폭증 절대임계값 체크 (신규)
  → 네 경로를 병렬 적용하고 OR 앙상블로 결합.

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

# ── EC2 저사용률(유휴) 체크 (절대임계값, 신규) ────────────────────────────────
# 출처: AWS Compute Optimizer idle recommendations 기준
#   https://docs.aws.amazon.com/compute-optimizer/latest/ug/view-idle-recommendations.html
#   "peak CPU utilization < 5% AND network I/O < 5MB/day (14일 lookback)"
#
# ⚠️ 팀 논의로 확정한 단순화(2026-09-05, 시나리오 1 작업): 이 파이프라인은 14일치
# 일별 데이터가 아니라 2.5시간(n_points=30 × period_seconds=300초, cloudwatch_client.py
# 기본값) 슬라이딩 윈도우만 갖고 있어서, AWS의 "14일 중 4일 이상 지속" 조건을 그대로
# 재현하지 않는다. 대신 "윈도우 전체(30포인트)가 처음부터 끝까지 임계값 이하"를
# 지속성 조건으로 대체한 근사치다 — AWS 원문 기준을 그대로 적용한 게 아니라 우리
# 시스템의 윈도우 스케일(2.5시간)로 축소 적용한 것임을 로그/문서에 명시할 것.
# (참고로 "5%"는 Compute Optimizer 기준이고, "4일 이상"은 별개 체크인 Trusted
# Advisor의 Low Utilization EC2 Instances 체크(CPU 10%대)에서 온 것이라 두 체크가
# 섞여 있었음 — 이번에 5%(Compute Optimizer) 쪽으로 통일해서 채택.)
EC2_IDLE_CPU_THRESHOLD_PCT = 5.0   # peak(윈도우 내 최댓값) 기준

_EC2_IDLE_NETWORK_IO_MB_PER_DAY = 5.0
_EC2_IDLE_WINDOW_HOURS = (30 * 300) / 3600  # n_points × period_seconds 기본값 = 2.5시간
# 5MB/day를 윈도우 길이에 비례 환산 (network_in/network_out은 Sum 스탯이라
# 누적량이므로 비례식이 그대로 성립) → 약 546,133 bytes(~0.52MB)
EC2_IDLE_NETWORK_IO_BYTES_THRESHOLD = (
    _EC2_IDLE_NETWORK_IO_MB_PER_DAY * 1024 * 1024 * (_EC2_IDLE_WINDOW_HOURS / 24)
)

# network_in + network_out 합산을 "network I/O"로 매핑 (AWS 정의상 in+out 합산이 일반적)
EC2_IDLE_TARGET_METRICS = ("cpu_utilization", "network_in", "network_out")

# ── Lambda 에러 재시도 폭증 체크 (절대임계값, 신규, 시나리오 4) ────────────────
# 스코프: "에러로 인한 재시도 폭증"(Lambda 비동기 호출은 에러 시 기본 2회, 최대 6시간에
# 걸쳐 재시도하며 이 추가 호출도 전부 과금됨)만 다룬다. AWS Lambda의 재귀 루프 감지
# (X-Ray Lineage 헤더 기반 16회 초과 차단, RecursiveInvocationsDropped 지표)는 에러 없이도
# 발생 가능한 별개 현상이라 이번 범위에서 제외 — 별도 시나리오로 분리하기로 팀 논의 확정
# (2026-09-05). 출처: https://aws.amazon.com/blogs/compute/implementing-error-handling-for-aws-lambda-asynchronous-invocations/
#
# EC2 유휴 체크와 반대로 "윈도우 전체"가 아니라 "최근 k개 포인트"(PERSISTENCE_WINDOW_POINTS
# 재사용, 기본 3개=15분) 지속 여부를 본다 — 유휴 탐지는 신생 리소스 오탐을 피하려고
# 일부러 보수적으로(느리게) 설계했지만, 비용이 실시간으로 새는 재시도 폭증은 반대로
# 빨리 반응하는 게 유리하기 때문 (설계 의도가 정반대).
LAMBDA_ERROR_RATE_THRESHOLD = 0.5   # 50% — Lambda 기본 재시도 최대 2회 감안, 진짜 지속적
                                     # 장애면 호출의 절반 이상이 실패로 나타날 가능성이 높음.
                                     # 이보다 낮추면(예: 20%) 정상 서비스의 베이스라인 에러율
                                     # (외부 API 오류 등)까지 폭증으로 오탐할 위험이 커짐.

# 노이즈 방지용 최소 호출수 게이트(포인트당). 베이스라인 에러율 5%인 정상 서비스가
# 우연히 한 포인트에서 50% 이상 에러로 보일 확률: N=3이면 약 0.7%, N=5면 약 0.11%,
# N=10이면 약 0.006% — 게다가 이 조건을 연속 3개 포인트(PERSISTENCE_WINDOW_POINTS)
# 전부에서 요구하므로 최종 오탐 확률은 사실상 0에 수렴한다. N=10을 채택.
LAMBDA_ERROR_RATE_MIN_INVOCATIONS = 10

# ── 학습 버퍼 정책 (리소스 타입당 다수 정상 윈도우 누적) ────────────────────────
MAX_WINDOWS_PER_TYPE = 30          # 타입당 최대 보관 윈도우 수 (Phase 5 실험값)
RETRAIN_EVERY_N_NEW_WINDOWS = 5    # 새 윈도우가 이만큼 쌓일 때마다 재학습

# ⚠️ 임시 동결 플래그 (사전학습 mock 시딩 도입, 2026-09-09) ───────────────────
# playground/seed_mock_iforest_buffer.py로 5개 타입 × 30개씩 mock 윈도우를
# 미리 채워서 iforest_unified.pkl/버퍼를 만들어둔 직후 상태. 이 시점엔 아직
# "실제 데이터가 들어오면 FIFO로 mock을 밀어내며 자연 교체"하는 정상 경로를
# 켜지 않고, 일단 mock 버퍼를 그대로 고정해서 쓴다.
#
# True인 동안 _get_or_train_iforest는 버퍼 채택/재학습을 전혀 안 하고 캐시된
# 모델을 그대로만 반환한다 — 즉 실제 데이터가 버퍼에 못 들어가고 mock 상태가
# 계속 유지된다.
#
# TODO(정상 경로 전환): mock→실데이터 자연 교체를 켜려면 이 값을 False로
# 바꾸기만 하면 된다 — 그 아래 버퍼 채택/FIFO/재학습 로직은 이미 구현·검증돼
# 있어서 추가 코드 변경이 필요 없다(단, Stage 2 후보A 확정 후 전환 권장 —
# 그 전엔 정상운영 판정 자체가 아직 미확정이라 어떤 실데이터를 받아들일지
# 기준이 없음).
MOCK_SEED_BUFFER_FROZEN = True
BUFFER_SCORE_MARGIN = 0.9          # 버퍼링 기준 = 탐지 임계값의 90% (기존 0.7 — 콜드스타트
BUFFER_ZSCORE_MARGIN = 0.9         # 구간에서 채택률이 24%에 그쳐 완화. 게이팅 대신 기준
                                    # 완화 쪽으로 팀 결정 — phase6 진단 스크립트로 검증함)

# Z-score는 "비용, 네트워크 입력, 호출 횟수" 지표에만 적용 (보고서 3.3.1).
# 리소스마다 필드명이 달라 의미 단위로 매핑한다.
#   비용        → cost                  (전 리소스 공통)
#   네트워크 입력 → network_in            (EC2)
#   호출 횟수    → invocation_count       (Lambda)
#               → number_of_requests     (S3)
#   전송량      → bytes_downloaded       (S3)
# [ADDED] bytes_downloaded 누락 수정: classification_rules.json의 CLF-003(S3 대량
# 다운로드 -> risk_security)이 triggered_metrics에 "bytes_downloaded"가 있어야
# 매칭되는데, 이 지표가 원래 대상에서 빠져있어서 Z-score로는 절대 안 잡히고
# IForest 콜드스타트(모델 없을 때)에만 우연히 걸리는 불안정한 상태였음.
Z_SCORE_TARGET_METRICS = {
    "cost",
    "network_in",
    "invocation_count",
    "number_of_requests",
    "bytes_downloaded",
}

# 학습 버퍼 채택 판정(_zscore_max) 전용 — 알림 판단(Z_SCORE_TARGET_METRICS)과 다르게
# network_in을 뺐다. 실제 AWS 실환경 검증(playground/validate_real_aws_buffer.py)에서
# EC2 network_in이 20~40분 주기로 반복적으로 튀는(9K/16K/23K대 다단계) 패턴을 가진 걸
# 확인했는데, window-max 방식이라 2.5시간 윈도우 안에 이 튐이 항상 하나쯤 들어있어서
# 마진(BUFFER_SCORE_MARGIN/BUFFER_ZSCORE_MARGIN)을 아무리 풀어도 EC2 버퍼 채택률이
# 7% 밑으로 막혀 있었다. 이 반복 패턴은 실제 이상이 아니라 이 리소스의 정상 트래픽
# 특성이라, 버퍼 채택 판정에서만 network_in을 빼서 학습이 이 패턴을 정상으로
# 받아들이게 한다. 알림 판단(detection_node)과 IForest 피처에는 network_in이
# 그대로 남아있어서, 진짜 지속되는 이상은 여전히 감지된다.
BUFFER_ZSCORE_TARGET_METRICS = Z_SCORE_TARGET_METRICS - {"network_in"}

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


def _low_utilization_check(
    resource_type: str,
    metrics: dict[str, list[float]],
    resource_age_seconds: Optional[float] = None,
) -> tuple[list[str], bool]:
    """EC2 저사용률(유휴/좀비) 절대임계값 체크. EC2_IDLE_* 상수 정의 위 주석 참고.

    z-score/IForest와 달리 window 내부 평균·표준편차를 쓰지 않는 절대 기준이라,
    "윈도우 내내 낮기만 하고 변동이 없는" 진짜 유휴 패턴(z-score가 놓치는 케이스)도
    잡을 수 있다. peak(=max) CPU와 window 전체 network I/O 합산을 보므로, 윈도우
    30포인트 전부가 임계값을 만족해야 트리거된다(자체로 이미 지속성 조건).

    EC2 전용 — 다른 리소스 타입(RDS 등)은 이번 범위에서 제외, 항상 (), False 반환.

    ⚠️ 신생 인스턴스 오탐 방지 가드 (2026-09-05 실 AWS 테스트에서 발견): CloudWatch는
    리소스가 존재하기 전 구간을 0으로 채워서 반환한다(cloudwatch_client.py:85-89).
    막 생성된 인스턴스는 윈도우 대부분이 "진짜 유휴"가 아니라 "아직 이력이 없어서
    0"인 값이라, 하필 부팅 트래픽마저 작았다면 즉시 좀비로 오판될 수 있다. 나이가
    윈도우 길이(EC2_IDLE 상수 정의 위 _EC2_IDLE_WINDOW_HOURS)보다 어리면 판단을
    보류한다. resource_age_seconds=None(나이를 모름 — EC2 외 타입이거나 조회 실패)이면
    가드를 적용하지 않고 기존처럼 그냥 평가한다(하위호환 기본값).
    """
    if resource_type != "EC2":
        return [], False
    if not all(m in metrics and metrics[m] for m in EC2_IDLE_TARGET_METRICS):
        return [], False
    if resource_age_seconds is not None and resource_age_seconds < _EC2_IDLE_WINDOW_HOURS * 3600:
        return [], False

    peak_cpu = max(metrics["cpu_utilization"])
    network_io_bytes = sum(metrics["network_in"]) + sum(metrics["network_out"])

    is_idle = (
        peak_cpu <= EC2_IDLE_CPU_THRESHOLD_PCT
        and network_io_bytes <= EC2_IDLE_NETWORK_IO_BYTES_THRESHOLD
    )
    triggered_metrics = list(EC2_IDLE_TARGET_METRICS) if is_idle else []
    return triggered_metrics, is_idle


def _lambda_error_rate_check(
    resource_type: str,
    metrics: dict[str, list[float]],
    k: int = PERSISTENCE_WINDOW_POINTS,
) -> tuple[list[str], bool]:
    """Lambda 에러 재시도 폭증 절대임계값 체크. LAMBDA_ERROR_RATE_* 상수 정의 위 주석 참고.

    최근 k개 포인트가 "전부" invocation_count >= LAMBDA_ERROR_RATE_MIN_INVOCATIONS
    AND error_count/invocation_count >= LAMBDA_ERROR_RATE_THRESHOLD를 만족해야 트리거된다
    (_zscore_check_persistent와 동일한 "최근 k개 전부" 지속성 패턴).

    게이트(최소 호출수)를 나눗셈보다 먼저 확인하므로 invocation_count=0인 포인트는
    항상 게이트에서 먼저 걸러져 0으로 나누는 경우가 발생하지 않는다.

    Lambda 전용 — 다른 리소스 타입은 항상 (), False 반환.
    """
    if resource_type != "Lambda":
        return [], False
    if not all(m in metrics and metrics[m] for m in ("invocation_count", "error_count")):
        return [], False

    invocation = metrics["invocation_count"]
    error = metrics["error_count"]
    k_eff = min(k, len(invocation))
    recent_invocation = invocation[-k_eff:]
    recent_error = error[-k_eff:]

    is_surge = all(
        inv >= LAMBDA_ERROR_RATE_MIN_INVOCATIONS and (err / inv) >= LAMBDA_ERROR_RATE_THRESHOLD
        for inv, err in zip(recent_invocation, recent_error)
    )
    triggered_metrics = ["error_count", "invocation_count"] if is_surge else []
    return triggered_metrics, is_surge


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
    """BUFFER_ZSCORE_TARGET_METRICS(학습 버퍼 채택 판정 전용 — Z_SCORE_TARGET_METRICS와
    다름, 위 상수 정의 참고) 중 |Z|의 최댓값. detection_node의 알림 판단과는 별개로
    학습 버퍼링 여부를 결정할 때만 쓰인다."""
    z_max = 0.0
    for metric_name, values in metrics.items():
        if metric_name not in BUFFER_ZSCORE_TARGET_METRICS:
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
        # 창 내부 min-max로는 정규화가 불가능(0/0)한 퇴화 케이스 — 윈도우 30개
        # 전부가 raw_score가 동일하게 나온 경우다. 예전엔 이걸 무조건 "전부 정상(0점)"
        # 으로 처리했는데, 실제로는 "진짜로 다 정상이라 점수가 같음"과 "이 리소스
        # 타입 학습이 부족해서 모델이 값 차이를 구분 못 함"을 구분하지 못하는 문제였다
        # (실 AWS 파일럿에서 AutoScaling 10배 스파이크가 이걸로 조용히 통과된 사고 확인).
        # 창 내부 상대 비교 대신, 모델이 학습 시 contamination으로 이미 고정해둔 절대
        # 기준(decision_function의 부호 — model.predict()가 쓰는 것과 동일한 기준)으로
        # 대체한다. 창이 오염되지 않은 모델이라면 최소한 이 폴백에서도 신호가 산다.
        is_anomaly = bool(raw_scores[0] < 0)
        return np.full_like(raw_scores, 1.0 if is_anomaly else 0.0)

    return np.clip((s_max - raw_scores) / (s_max - s_min), 0.0, 1.0)


# Stage 2 후보 A 그리드 실험(2026-09-09) 결과 채택값 — 실측 근거:
# contamination 환산 0.02(=percentile 2.0)/K=3 조합이 정상 오탐 6.3%로 가장 낮으면서
# 장기·지속형 이상(spike_len 15~24)은 87~100% 거부. 짧은 스파이크(len 3~6)는 이 게이트가
# 아니라 z-score(_zscore_check_persistent)가 먼저 잡는 역할 분담을 전제로 함.
STAGE2_ADMIT_PERCENTILE = 2.0
STAGE2_ADMIT_K_MIN = 3


def _absolute_score_and_admit(
    model: IsolationForest,
    resource_type: str,
    metrics: dict[str, list[float]],
    buffer_windows: list[np.ndarray],
    percentile: float = STAGE2_ADMIT_PERCENTILE,
    k_min: int = STAGE2_ADMIT_K_MIN,
) -> tuple[float, bool]:
    """Stage 2 후보 A: provisional_score(버퍼 admission 판정)를 창 내부 min-max
    (_normalized_scores) 대신, 모델의 raw score_samples()와 버퍼 자체에서 직접 계산한
    percentile 임계값으로 판단한다.

    ⚠️ min-max와 다른 점: score_samples()는 트리 구조(fit 시 확정, contamination과
    무관)에서만 나오는 순수 이상치 점수라 "이 창 안에서 제일 이상한 점은 항상 1.0"
    같은 구조적 왜곡이 없다. 대신 "얼마나 낮으면 이상치로 볼지" 기준(threshold)을
    모델의 built-in offset_(contamination=0.1 기준, 최종 알림 판정용) 대신 버퍼
    자체에서 우리가 원하는 percentile로 별도 계산한다 — sklearn이 모델 생성 시
    내부적으로 하는 계산(percentile(score_samples(학습데이터), 100*contamination))과
    같은 공식을, 버퍼 admission이라는 다른 목적에 맞는 값(2%)으로 재사용하는 것.
    모델을 두 번 학습시킬 필요가 없다(트리는 contamination과 무관하므로).

    ⚠️ 아직 어디서도 호출 안 됨(독립·테스트 전용) - _get_or_train_iforest의
    provisional_score 판정에 실제로 연결하려면 이 함수를 호출하도록 바꿔야 하는데,
    시연 전 실측 검증(요청 A 포함) 전까지는 보류하기로 함(2026-09-09).

    반환: (최근 시점 raw score_samples 값 — 리포팅용, 버퍼에 받아들여도 되는가)
    """
    if not buffer_windows:
        return 0.0, False

    buffer_matrix = np.vstack(buffer_windows)
    threshold = float(np.percentile(model.score_samples(buffer_matrix), percentile))

    X = build_unified_feature_matrix(resource_type, metrics)
    raw = model.score_samples(X)
    outliers = int((raw < threshold).sum())
    believed_normal = outliers < k_min

    return float(raw[-1]), believed_normal


def _score_with_model(model: IsolationForest, resource_type: str, metrics: dict[str, list[float]]) -> float:
    return float(_normalized_scores(model, resource_type, metrics)[-1])


# 콜드스타트 자체 검증(_self_referential_iforest_check) 전용 기준.
# contamination=0.1 → 30개 창에서 항상 ~3개는 "상대적으로 가장 이상치"로 분류되는데
# (min-max 정규화 방식은 이 3개 중 최댓값이 무조건 1.0이 돼버려서 못 씀 — 첫 시도에서
# 발견한 버그), 그 3개가 "최근 구간에 몰려있는가"를 대신 본다. 순수 정상 데이터로
# 무작위 30회 시뮬레이션한 결과 이 기준(최근 6개 중 2개 이상)의 오탐률은 10%
# (콜드스타트가 가끔 한 사이클 늦어지는 정도라 감내 가능한 수준으로 판단).
SELF_CHECK_RECENT_WINDOW = 6
SELF_CHECK_MIN_OUTLIERS = 2


def _self_referential_iforest_check(resource_type: str, metrics: dict[str, list[float]]) -> bool:
    """콜드스타트 시드 후보 검증 전용. 아직 저장된 모델이 없어 정식 IForest 점수를
    못 매기므로(_score_with_model은 학습된 모델이 필요), 이 윈도우 자체(30개 행)로
    임시 IsolationForest를 하나 학습시켜서 "최근 시점들 중 다수가 나머지 대비
    이상치로 분류되는가"를 자체 판단한다 — 저장은 안 하고 이 판단에만 쓰고 버린다.
    Z-score의 window-max(_zscore_max)와 같은 철학: 윈도우 자기 자신을 기준으로 삼는다.
    반환값 True면 시드 거부 대상."""
    X = build_unified_feature_matrix(resource_type, metrics)
    temp_model = IsolationForest(contamination=IFOREST_CONTAMINATION, random_state=IFOREST_RANDOM_STATE)
    predictions = temp_model.fit_predict(X)  # -1=이상치, 1=정상

    k_eff = min(SELF_CHECK_RECENT_WINDOW, len(predictions))
    n_outliers_recent = int((predictions[-k_eff:] == -1).sum())
    return n_outliers_recent >= SELF_CHECK_MIN_OUTLIERS


def _model_independent_seed_check(
    resource_type: str, metrics: dict[str, list[float]]
) -> tuple[float, bool]:
    """저장된 모델 없이도(또는 모델은 있지만 이 리소스 타입은 한 번도 못 봤을 때도)
    계산 가능한 검증만으로 "이 윈도우가 그 자체로 이상해 보이는가"를 판단한다.
    Z-score(모델 불필요)와 "이 윈도우 자체로 임시 학습해서 자체 검증"하는
    _self_referential_iforest_check 둘 다 모델 없이 가능하므로 이 둘만 건다.

    두 곳에서 공유: ① 전역 콜드스타트(모델 파일 자체가 없음) ② 모델은 있지만 이
    resource_type의 윈도우가 버퍼에 하나도 없는 경우 — 후자를 별도 취급 안 하면,
    다른 타입 하나로 시드된 모델이 "낯선 타입=이상"으로 계속 오판해서 새 타입이
    영영 버퍼에 못 들어가는 문제가 있었다(실 AWS 파일럿에서 EC2 1개 윈도우로 시드된
    모델이 Lambda/AutoScaling을 전부 이상으로 오판한 사고로 발견).

    반환: (z_max, believed_normal)
    """
    z_max = _zscore_max(metrics)
    self_iforest_flagged = _self_referential_iforest_check(resource_type, metrics)
    believed_normal = (
        z_max < Z_SCORE_THRESHOLD * BUFFER_ZSCORE_MARGIN and not self_iforest_flagged
    )
    return z_max, believed_normal


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

        # MOCK_SEED_BUFFER_FROZEN=True인 동안은 버퍼 채택/재학습을 전부 건너뛰고
        # mock으로 시딩해둔 모델을 그대로 반환한다 (정상 경로 전환 전 임시 동결).
        if MOCK_SEED_BUFFER_FROZEN:
            return model

        if cached_keys == ALL_METRICS:
            buffer_by_type, pending_count = _load_training_buffer()

            if n >= MIN_POINTS_FOR_IFOREST:
                # 이 타입은 모델은 있어도 버퍼에 한 번도 안 쌓여본 "모델 입장에서 낯선
                # 타입" — 기존 모델의 provisional_score로 평가하면 학습한 적 없는
                # 타입이라 항상 이상치처럼 보여서 계속 튕겨나간다. 콜드스타트와 동일한
                # 모델-독립적 검증으로 대신 판단한다.
                type_unseen = not buffer_by_type.get(resource_type)

                if type_unseen:
                    z_max, believed_normal = _model_independent_seed_check(resource_type, metrics)
                    # 0.0: "모델 기반 score 조건은 이 경로에서 평가 안 함 — z_max/자기참조
                    # IForest만으로 판단"이라는 뜻. score=None을 쓰지 않는 이유: 이 로그를
                    # 파싱하는 playground/phase6_detection_node_e2e.py의
                    # _BufferDecisionCapture가 args[1]을 항상 float(score)로 변환하므로
                    # (resource_type, score, z_max) 위치와 타입을 그대로 유지해야 한다.
                    provisional_score = 0.0
                else:
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
                    # ⚠️ 로그 인자 순서 (resource_type, score, z_max, ...)는 playground/
                    # phase6_detection_node_e2e.py의 _BufferDecisionCapture가
                    # record.args[0:3]을 그대로 파싱하므로 앞 3자리는 유지하고
                    # type_unseen은 뒤에 덧붙인다.
                    logger.info(
                        "[iforest_buffer] 채택 resource_type=%s score=%.4f z_max=%.4f "
                        "버퍼크기=%d pending=%d (신규타입=%s)",
                        resource_type, provisional_score, z_max, len(bucket), pending_count, type_unseen,
                    )
                else:
                    logger.info(
                        "[iforest_buffer] 제외(경계/이상 의심) resource_type=%s score=%.4f z_max=%.4f "
                        "(신규타입=%s)",
                        resource_type, provisional_score, z_max, type_unseen,
                    )

                _save_training_buffer(buffer_by_type, pending_count)

                # 신규 타입이 방금 채택됐으면 재학습 카운트(5개)를 기다리지 않고 즉시
                # 반영한다 — 안 그러면 이 타입은 다음 정기 재학습 전까지 계속 "모델이
                # 모르는 타입" 상태로 남아 매번 다시 튕겨날 수 있다.
                type_just_learned = believed_normal and type_unseen
                if (pending_count >= RETRAIN_EVERY_N_NEW_WINDOWS or type_just_learned) and buffer_by_type:
                    combined = np.vstack([np.vstack(v) for v in buffer_by_type.values() if v])
                    model = _fit_and_cache_unified(combined)
                    _save_training_buffer(buffer_by_type, 0)
                    logger.info(
                        "[iforest_buffer] 재학습 완료 총 윈도우=%d (타입별=%s)%s",
                        combined.shape[0], {k: len(v) for k, v in buffer_by_type.items()},
                        " (신규 타입 즉시 반영)" if type_just_learned else "",
                    )

            return model

    if n < MIN_POINTS_FOR_IFOREST:
        return None

    # ⚠️ 콜드스타트 시드 검증 (실 AWS 파일럿 테스트에서 발견): 예전엔 첫 윈도우를
    # 무조건(어떤 검사도 없이) 정상으로 확정해서 시드 모델을 학습시켰다. 실제로 Lambda
    # 파일럿에서 부하 테스트 시작 직후에 콜드스타트가 겹치면서 부하 자체가 시드로
    # 굳어버리는 사고가 있었음 (z_max=4.88로 명백히 이상했는데도 무조건 통과됐음).
    z_max, believed_normal = _model_independent_seed_check(resource_type, metrics)
    if not believed_normal:
        logger.info(
            "[iforest_buffer] 콜드스타트 시드 거부(첫 윈도우가 이미 이상해 보임) "
            "resource_type=%s z_max=%.4f — 다음 사이클에 재시도",
            resource_type, z_max,
        )
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

    # ── 3) EC2 저사용률(유휴) 절대임계값 체크 (신규, EC2 전용) ──────────────
    idle_metrics, idle_triggered = _low_utilization_check(
        resource_type, metrics, state.get("resource_age_seconds")
    )
    for m in idle_metrics:
        if m not in triggered_metrics:
            triggered_metrics.append(m)

    # ── 4) Lambda 에러 재시도 폭증 절대임계값 체크 (신규, Lambda 전용) ───────
    error_surge_metrics, error_surge_triggered = _lambda_error_rate_check(resource_type, metrics)
    for m in error_surge_metrics:
        if m not in triggered_metrics:
            triggered_metrics.append(m)

    # ── 5) OR 앙상블 결합 ─────────────────────────────────────────────────
    anomaly_flag = (
        bool(triggered_metrics) or iforest_triggered or idle_triggered or error_surge_triggered
    )

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
        "resource_age_seconds": resource.get("resource_age_seconds"),

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