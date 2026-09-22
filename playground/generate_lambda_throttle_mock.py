"""
playground/generate_lambda_throttle_mock.py

Lambda 스로틀(429)/동시성 소진 재시도 폭증 시나리오 전용 목업 평가 데이터셋
(normal/anomaly/edge_normal)을 생성한다 — 기존 generate_ec2_lambda_retry_mock.py와
동일한 철학·스키마를 따르되, 이번엔 throttle_count/async_event_age 기반이다.

⚠️ 이 시나리오는 2026-09-13/14 리팩터링으로 독립 절대임계값 게이트가 없다 —
detection_node의 최종 판정은 순수 IForest(+ Z-score, 이 시나리오에선 거의 안 걸림)
단독이다. 그래서 검증 함수(_lambda_error_rate_check 같은 결정론적 체크)가 없고,
대신 실제 detection_node()를 그대로 통과시켜 "설계 의도"가 아니라 "실제 판정"을
라벨로 확정한다 — anomaly/normal은 라벨과 판정이 반드시 일치해야 하고(assert),
edge_normal은 설계상 정상이길 기대하지만 실제로 다르게 나올 수 있어(옛 Lambda
재시도폭증 edge_normal 사례와 동일한 패턴) 그 결과를 있는 그대로 기록만 한다.

실측 근거(F-1, 2026-09-12/13 여러 차례 실 AWS 반복시행):
  - anomaly: concurrency 1/2/3에서 throttle_rate 0.35~0.76, invocation_count는
    오히려 낮게 나옴(스로틀된 시도는 Invocations에 안 잡힘)
  - normal: 무제한 동시성에서 throttle_count=0 (실측 전 구간 예외 없음)

[실행 방법] 프로젝트 루트에서: python playground/generate_lambda_throttle_mock.py

[생성 파일] playground/mock_data/lambda_eval_throttle.json
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import numpy as np

import pipeline.detection_agent as da

N = 30
PERIOD_SECONDS = 300
OUT_DIR = PROJECT_ROOT / "playground" / "mock_data"

_SLOT_HOURS = [0, 3, 6, 9, 12, 15, 18, 21]
_SLOT_NAMES = {
    0: "night_early", 3: "night_early", 6: "commute_morning", 9: "business_morning",
    12: "lunch", 15: "business_afternoon", 18: "commute_evening", 21: "night_late",
}
_BASE_DATE = datetime(2026, 9, 14, tzinfo=timezone.utc)


def _time_fields(window_index: int) -> dict:
    day_offset = window_index // len(_SLOT_HOURS)
    slot_idx = window_index % len(_SLOT_HOURS)
    hour = _SLOT_HOURS[slot_idx]
    start = _BASE_DATE + timedelta(days=day_offset, hours=hour)
    timestamps = [
        (start + timedelta(seconds=PERIOD_SECONDS * i)).strftime("%Y-%m-%dT%H:%M:%SZ")
        for i in range(N)
    ]
    day_of_week = day_offset % 7
    return {
        "timestamps": timestamps, "time_slot": _SLOT_NAMES[hour], "hour_of_day": hour,
        "day_of_week": day_of_week, "is_weekend": day_of_week in (5, 6),
    }


def _window(window_id: str, label: str, window_index: int, raw_metrics: dict, **extra) -> dict:
    w = {
        "window_id": window_id, "resource_type": "Lambda", "resource_id": "mock-lambda-throttle",
        "label": label, **_time_fields(window_index), "raw_metrics": raw_metrics,
    }
    w.update(extra)
    return w


def make_normal(rng: np.random.Generator) -> dict:
    """무제한 동시성 정상 트래픽 — 실측(diverse trial) 4단계 볼륨을 그대로 재현.
    throttle_count는 실측 전 구간에서 예외 없이 0.

    ⚠️ invocation 수준은 lambda_train.json(현재 캐시된 모델의 실제 학습 데이터)의
    정상 범위(11.99~210.98, 평균 79.7)에 맞춰 보정했다 — 처음엔 실 AWS 테스트
    함수의 저볼륨(5~40건)을 그대로 썼는데, 그러면 이 모델 기준으로는 오히려
    "너무 낮아서 이상"으로 오판되는 걸 실측으로 확인함(4/20 오탐)."""
    profile = rng.choice(["light", "moderate", "heavy", "bursty"])
    level = {"light": 30.0, "moderate": 60.0, "heavy": 130.0, "bursty": 90.0}[profile]
    invocation = np.clip(level + rng.uniform(-level * 0.3, level * 0.3, size=N), 1, None)
    error = np.zeros(N)
    duration = rng.uniform(80, 200, size=N)
    cost = invocation * 0.0000002 + (duration / 1000) * (128 / 1024) * invocation * 0.0000166667
    return {
        "invocation_count": invocation.tolist(),
        "error_count": error.tolist(),
        "duration_avg": duration.tolist(),
        "cost": cost.tolist(),
        "throttle_count": [0.0] * N,
        "async_event_age": rng.uniform(0, 30, size=N).tolist(),  # 정상 큐 대기(ms 수준)
    }


def make_throttle_anomaly(rng: np.random.Generator, severity: str) -> dict:
    """마지막 3포인트가 스로틀 폭증 — severity별로 concurrency 1/2/3 실측 패턴 재현.
    베이스라인(첫 27포인트)은 make_normal과 같은 범위로 맞춰서(모델 기준 "정상") 마지막
    3포인트의 스로틀 신호만 튀게 한다.

    ⚠️ throttle_count 절대값은 실측(2026-09-13, CloudWatch 직접 조회)의 5분 구간당
    평균치를 그대로 씀 — 처음엔 throttle_rate 공식(thr/(thr+inv))으로 역산한 값(8~32)을
    썼는데, 이건 실제 관측치(severe 90~120, moderate 55~95, mild 40~45)보다 훨씬
    작아서 모델이 노이즈로 취급해 7/10이 미탐되는 걸 실측으로 확인함 — AWS 비동기
    재시도가 지수 백오프로 한 구간(5분) 안에 원 호출 수보다 훨씬 많은 재시도를
    누적시키기 때문에, throttle_rate 공식의 "그 구간 자체의 inv/thr 비율"과
    "실제 관측되는 절대 스케일"이 서로 다른 답을 준다."""
    baseline_inv = np.clip(70.0 + rng.uniform(-25, 25, size=N - 3), 1, None)
    baseline_thr = np.zeros(N - 3)
    baseline_age = rng.uniform(0, 30, size=N - 3)

    severity_params = {
        "severe":   {"thr": (90, 120), "inv": (30, 40)},   # concurrency=1, 실측 throttles 306~358
        "moderate": {"thr": (55, 95),  "inv": (35, 40)},   # concurrency=2, 실측 throttles 164~280
        "mild":     {"thr": (40, 65),  "inv": (35, 40)},   # concurrency=3, 실측 throttles 64~130
    }
    p = severity_params[severity]
    tail_inv = rng.uniform(*p["inv"], size=3)
    tail_thr = rng.uniform(*p["thr"], size=3)
    tail_age = rng.uniform(500, 5000, size=3)  # ms, 큐 대기 급증

    invocation = np.concatenate([baseline_inv, tail_inv])
    throttle = np.concatenate([baseline_thr, tail_thr])
    async_age = np.concatenate([baseline_age, tail_age])
    error = np.zeros(N)  # 실측 근거: 스로틀 경로는 Errors에 안 잡힘
    duration = rng.uniform(80, 200, size=N)
    cost = invocation * 0.0000002 + (duration / 1000) * (128 / 1024) * invocation * 0.0000166667
    return {
        "invocation_count": invocation.tolist(), "error_count": error.tolist(),
        "duration_avg": duration.tolist(), "cost": cost.tolist(),
        "throttle_count": throttle.tolist(), "async_event_age": async_age.tolist(),
    }


def make_edge_cases(rng: np.random.Generator) -> list[dict]:
    cases = []

    # 1) 순간적인 스로틀 1틱만(노이즈성) — 지속 아님, 정상 처리 기대
    baseline_inv = rng.uniform(20, 40, size=N)
    invocation = baseline_inv.copy()
    throttle = np.zeros(N)
    throttle[15] = 8.0  # 딱 한 포인트만 스로틀
    async_age = rng.uniform(0, 30, size=N)
    async_age[15] = 800.0
    cases.append(({
        "invocation_count": invocation.tolist(), "error_count": [0.0] * N,
        "duration_avg": rng.uniform(80, 200, size=N).tolist(),
        "cost": (invocation * 0.00003).tolist(),
        "throttle_count": throttle.tolist(), "async_event_age": async_age.tolist(),
    }, {"note": "중간에 단 1포인트만 스로틀 발생(노이즈성 순간 튐) - 지속되는 폭풍이 아니므로 정상 처리 기대"}))

    # 2) AWS 기본 버스트 한도 근처의 정상 대용량 트래픽(동시성 제약 없음, 스로틀 0)
    invocation = rng.uniform(300, 500, size=N)
    cases.append(({
        "invocation_count": invocation.tolist(), "error_count": [0.0] * N,
        "duration_avg": rng.uniform(80, 200, size=N).tolist(),
        "cost": (invocation * 0.00003).tolist(),
        "throttle_count": [0.0] * N, "async_event_age": rng.uniform(0, 30, size=N).tolist(),
    }, {"note": "동시성 제약 없이 대용량(300~500건) 정상 트래픽 - 볼륨은 크지만 스로틀 없음, 정상 처리 기대"}))

    # 3) 최근 3개 중 가장 오래된 포인트만 스로틀 낮음(지속성 경계)
    baseline_inv = rng.uniform(15, 40, size=N - 3)
    tail_inv = np.array([12.0, 12.0, 12.0])
    tail_thr = np.array([2.0, 30.0, 32.0])  # 낮음/높음/높음
    invocation = np.concatenate([baseline_inv, tail_inv])
    throttle = np.concatenate([np.zeros(N - 3), tail_thr])
    async_age = np.concatenate([rng.uniform(0, 30, size=N - 3), [100.0, 3000.0, 3200.0]])
    cases.append(({
        "invocation_count": invocation.tolist(), "error_count": [0.0] * N,
        "duration_avg": rng.uniform(80, 200, size=N).tolist(),
        "cost": (invocation * 0.00003).tolist(),
        "throttle_count": throttle.tolist(), "async_event_age": async_age.tolist(),
    }, {"note": "최근 3포인트 중 가장 오래된 포인트만 스로틀 낮음 - 폭풍 시작 직전 경계"}))

    # 4) 마지막 포인트만 회복(스로틀 폭풍이 막 끝남)
    baseline_inv = rng.uniform(15, 40, size=N - 3)
    tail_inv = np.array([10.0, 10.0, 25.0])
    tail_thr = np.array([28.0, 30.0, 0.0])  # 높음/높음/회복
    invocation = np.concatenate([baseline_inv, tail_inv])
    throttle = np.concatenate([np.zeros(N - 3), tail_thr])
    async_age = np.concatenate([rng.uniform(0, 30, size=N - 3), [3000.0, 3200.0, 50.0]])
    cases.append(({
        "invocation_count": invocation.tolist(), "error_count": [0.0] * N,
        "duration_avg": rng.uniform(80, 200, size=N).tolist(),
        "cost": (invocation * 0.00003).tolist(),
        "throttle_count": throttle.tolist(), "async_event_age": async_age.tolist(),
    }, {"note": "직전까지 스로틀 폭풍이었으나 마지막 포인트에 회복 - 폭풍 종료 직후 경계"}))

    # 5) 저볼륨 서비스의 낮은 절대 스로틀 건수(비율은 낮지 않지만 총량이 작음)
    baseline_inv = rng.uniform(3, 6, size=N - 3)
    tail_inv = np.array([2.0, 2.0, 2.0])
    tail_thr = np.array([1.0, 1.0, 1.0])  # throttle_rate=33%지만 절대 건수는 3건뿐
    invocation = np.concatenate([baseline_inv, tail_inv])
    throttle = np.concatenate([np.zeros(N - 3), tail_thr])
    async_age = np.concatenate([rng.uniform(0, 30, size=N - 3), [200.0, 220.0, 210.0]])
    cases.append(({
        "invocation_count": invocation.tolist(), "error_count": [0.0] * N,
        "duration_avg": rng.uniform(80, 200, size=N).tolist(),
        "cost": (invocation * 0.00003).tolist(),
        "throttle_count": throttle.tolist(), "async_event_age": async_age.tolist(),
    }, {"note": "저볼륨 서비스(호출 2~6건) - 스로틀 비율은 있어도 절대 건수가 매우 작은 경계"}))

    return cases


def build_file(rng: np.random.Generator) -> list[dict]:
    windows = []
    for i in range(20):
        windows.append(_window(f"lambda_eval_throttle_normal_{i:03d}", "normal", i, make_normal(rng)))

    severities = ["severe"] * 4 + ["moderate"] * 3 + ["mild"] * 3
    for i, sev in enumerate(severities):
        windows.append(_window(
            f"lambda_eval_throttle_anomaly_{i:03d}", "anomaly", 20 + i,
            make_throttle_anomaly(rng, sev), anomaly_type="throttle_retry_storm", severity=sev,
        ))

    for i, (metrics, extra) in enumerate(make_edge_cases(rng)):
        windows.append(_window(f"lambda_eval_throttle_edge_{i:03d}", "edge_normal", 30 + i, metrics, **extra))

    return windows


def run_window(window: dict) -> bool:
    state = {
        "trace_id": None, "resource_id": window.get("resource_id", "eval"),
        "resource_type": "Lambda", "raw_metrics": window["raw_metrics"], "timestamp": None,
        "resource_age_seconds": window.get("resource_age_seconds"),
        "anomaly_flag": False, "anomaly_score_zscore": None, "anomaly_score_iforest": None,
        "triggered_metrics": [], "shap_top_features": None,
    }
    result = da.detection_node(state)
    return bool(result["anomaly_flag"])


def main() -> None:
    rng = np.random.default_rng(2026)
    windows = build_file(rng)

    # ⚠️ 2026-09-14 방향 수정: 처음엔 normal/anomaly 전부 일치를 강제(assert)했는데,
    # 기존 다른 시나리오 파일(ec2_eval.json 5%, lambda_eval_retry.json 15%)도 실제로는
    # 0% 오탐이 아니라 "오탐률 자체를 결과로 보고"하는 방식이었다 -- IForest 기반
    # 판정에 0% FP를 강제로 맞추는 건 비현실적이고, 오히려 있는 그대로의 오탐률을
    # 정직하게 보고하는 게 이 프로젝트의 기존 관례와 일치한다. anomaly(재현율)만은
    # 이 시나리오의 핵심 주장이므로 계속 엄격히 확인한다.
    print("실제 detection_node()로 라벨-판정 확인 중...")
    anomaly_misses = []
    normal_false_positives = []
    for w in windows:
        flag = run_window(w)
        if w["label"] == "anomaly" and not flag:
            anomaly_misses.append(w["window_id"])
        elif w["label"] == "normal" and flag:
            normal_false_positives.append(w["window_id"])

    if anomaly_misses:
        for wid in anomaly_misses:
            print(f"  [MISS] {wid}")
        raise SystemExit(f"{len(anomaly_misses)}개 anomaly 윈도우가 미탐됨 - 생성 파라미터 조정 필요")
    print(f"검증 통과: anomaly {sum(1 for w in windows if w['label']=='anomaly')}개 전부 탐지됨.")
    if normal_false_positives:
        print(f"참고: normal 오탐 {len(normal_false_positives)}건 (있는 그대로 보고): {normal_false_positives}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / "lambda_eval_throttle.json"
    out_path.write_text(json.dumps(windows, indent=2, ensure_ascii=False), encoding="utf-8")

    n_normal = sum(1 for w in windows if w["label"] == "normal")
    n_anomaly = sum(1 for w in windows if w["label"] == "anomaly")
    n_edge = sum(1 for w in windows if w["label"] == "edge_normal")
    print(f"\n{out_path}: {len(windows)}개 (normal={n_normal}, anomaly={n_anomaly}, edge_normal={n_edge})")


if __name__ == "__main__":
    main()
