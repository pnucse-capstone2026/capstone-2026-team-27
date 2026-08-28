"""
playground/phase6_detection_node_e2e.py

Phase 6: detection_node() 자체를 배치로 돌리는 첫 E2E 평가 스크립트.

왜 필요한가:
  Phase 1~5, joint anomaly, tuning 스크립트는 전부 pipeline/detection_agent.py의
  _zscore_check / _score_with_model처럼 "포인트 단위" 저수준 함수를 직접 불러서
  평가했다. 그런데 실제 알림을 내는 건 detection_node()이고, 여기엔 그 저수준
  함수들에는 없는 두 가지가 더 들어간다: (1) OR 앙상블, (2) 지속성 체크(최근
  PERSISTENCE_WINDOW_POINTS개 시점이 전부 임계값을 넘어야 트리거 — 순간 노이즈
  필터링용). detection_node() 자체를 배치로 돌려본 적이 한 번도 없었다.

또 하나: Phase 1 데이터셋(playground/generate_eval_dataset.py)의 스파이크는
  전부 "마지막 1개 포인트만" 튀우는 방식이라(_spike_window), 지속성 체크와
  구조적으로 안 맞는다 — 이 데이터셋으로 persistence를 검증하면 recall이
  구조적으로 0%가 나온다(이미 확인함). 그래서 이 스크립트는 별도로:
    - sustained : 최근 PERSISTENCE_WINDOW_POINTS개가 지속적으로 튀는, 현실적인
                  인시던트 패턴 (진짜 잡아야 하는 케이스)
    - blip      : 마지막 1개만 튀는 순간 노이즈 (Phase 1과 동일한 패턴이지만,
                  여기서는 "지속성 체크가 이걸 걸러내는가"를 검증하는 용도로 씀 —
                  안 잡히는 게 정답)
    - normal    : 베이스라인만
  세 종류를 새로 만들어서 detection_node()에 그대로 흘려보낸다.

detection_node()는 호출할 때마다 IsolationForest 학습 버퍼를 갱신하는 부수효과가
있어서(pipeline/detection_agent.py의 _get_or_train_iforest), 샘플을 넣는 "순서"가
결과에 영향을 준다. 그래서 이 스크립트도 playground/validate_self_referential_buffer.py와
같은 방식으로 두 가지 순서로 검증한다: (1) 원래 순서(정상이 먼저 오는, 버퍼
워밍업에 유리한 순서) (2) 무작위로 섞은 순서(더 가혹한 조건).

첫 실행에서 정상/blip 오탐률이 낮지 않게 나왔는데(원래 순서 기준 normal 15%, blip
21.58%), 원인을 파보니 지속성 체크 로직 자체가 아니라 IForest 학습 버퍼가 아직
MAX_WINDOWS_PER_TYPE(30)만큼 안 찼을 때 생기는 콜드스타트 불안정성이었다 — 정상
샘플만 20개 넣어도 그 안에서 이미 IForest 점수가 0~1을 크게 오갔다. 그래서 "콜드
스타트 노이즈"와 "정상상태(버퍼가 가득 찬 뒤) persistence 성능"을 분리해서 보기
위해 웜업 옵션을 추가했다: 평가 샘플을 넣기 전에 순수 정상 윈도우를
MAX_WINDOWS_PER_TYPE만큼 리소스 타입별로 먼저 흘려보내(결과는 집계 안 함) 버퍼를
가득 채우고 여러 번 재학습이 일어난 뒤에 평가를 시작한다.

[실행 방법]
  프로젝트 루트에서: python playground/phase6_detection_node_e2e.py
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

import numpy as np

import pipeline.detection_agent as da
from playground.generate_eval_dataset import BASELINES, SPIKE_MULTIPLIER, WINDOW_SIZE, _normal_window

RESULT_PATH = PROJECT_ROOT / "playground" / "eval_outputs" / "phase6_detection_node_e2e_result.json"
MODEL_DIR = str(PROJECT_ROOT / ".tmp_phase6_models")

N_NORMAL_PER_TYPE = 20      # 리소스 타입당 정상 샘플 개수
N_SUSTAINED_PER_METRIC = 10 # 리소스 타입당, 지표 1개마다 지속형 스파이크 샘플 개수
N_BLIP_PER_METRIC = 10      # 리소스 타입당, 지표 1개마다 순간 노이즈(1포인트) 샘플 개수
N_WARMUP_PER_TYPE = da.MAX_WINDOWS_PER_TYPE  # 웜업용 정상 윈도우 개수 (버퍼를 가득 채우기 위해 실제 상수와 동기화)
RANDOM_SEED = 42
WARMUP_SEED_OFFSET = 900_000  # 평가용 normal 샘플과 시드가 겹치지 않도록 (완전히 같은 데이터를 두 번 세는 것 방지)


# ── 샘플 생성 ──────────────────────────────────────────────────────────────────

def _sustained_spike_window(rng: np.random.Generator, base: float, noise: float) -> list[float]:
    """최근 PERSISTENCE_WINDOW_POINTS개 포인트가 지속적으로 튀는 윈도우.
    generate_eval_dataset._spike_window와 동일한 배율(SPIKE_MULTIPLIER)을 쓰되,
    마지막 1개가 아니라 마지막 k개를 전부 튀운다."""
    window = base + rng.uniform(-noise, noise, size=WINDOW_SIZE)
    k = da.PERSISTENCE_WINDOW_POINTS
    window[-k:] = base * SPIKE_MULTIPLIER + rng.uniform(-noise, noise, size=k)
    return window.tolist()


def _blip_window(rng: np.random.Generator, base: float, noise: float) -> list[float]:
    """마지막 1개 포인트만 튀는 순간 노이즈 윈도우 (Phase 1의 _spike_window와 동일)."""
    window = base + rng.uniform(-noise, noise, size=WINDOW_SIZE)
    window[-1] = base * SPIKE_MULTIPLIER
    return window.tolist()


def _make_sample(
    resource_type: str, pattern: str, injected_metric: str | None, sample_idx: int
) -> dict:
    """pattern: 'normal' | 'sustained' | 'blip'
    expect_trigger: detection_node()가 "이렇게 판단해야 맞다"는 기대값 —
      Phase 1의 ground_truth_anomaly(포인트 단위 이상 주입 여부)와는 다른 개념.
      sustained만 True. normal/blip은 지속성 체크가 걸러내야 하므로 False."""
    seed = RANDOM_SEED + hash((resource_type, pattern, sample_idx)) % 1_000_000
    rng = np.random.default_rng(seed)

    metrics = {}
    for metric, (base, noise) in BASELINES[resource_type].items():
        if metric == injected_metric and pattern == "sustained":
            metrics[metric] = _sustained_spike_window(rng, base, noise)
        elif metric == injected_metric and pattern == "blip":
            metrics[metric] = _blip_window(rng, base, noise)
        else:
            metrics[metric] = _normal_window(rng, base, noise)

    return {
        "sample_id": f"{resource_type}_{pattern}_{injected_metric or 'none'}_{sample_idx:03d}",
        "resource_type": resource_type,
        "pattern": pattern,
        "injected_metric": injected_metric,
        "expect_trigger": pattern == "sustained",
        "raw_metrics": metrics,
    }


def generate_dataset() -> list[dict]:
    dataset: list[dict] = []
    for resource_type, metric_baselines in BASELINES.items():
        for i in range(N_NORMAL_PER_TYPE):
            dataset.append(_make_sample(resource_type, "normal", None, i))
        for metric in metric_baselines:
            for i in range(N_SUSTAINED_PER_METRIC):
                dataset.append(_make_sample(resource_type, "sustained", metric, i))
            for i in range(N_BLIP_PER_METRIC):
                dataset.append(_make_sample(resource_type, "blip", metric, i))
    return dataset


def generate_warmup_samples() -> list[dict]:
    """평가 시작 전 버퍼를 MAX_WINDOWS_PER_TYPE만큼 채우기 위한 순수 정상 윈도우.
    결과는 집계하지 않고 버퍼/모델 상태만 만드는 용도라 evaluation 샘플과 시드를
    겹치지 않게 분리한다(WARMUP_SEED_OFFSET)."""
    warmup: list[dict] = []
    for resource_type in BASELINES:
        for i in range(N_WARMUP_PER_TYPE):
            warmup.append(_make_sample(resource_type, "normal", None, WARMUP_SEED_OFFSET + i))
    return warmup


# ── detection_node 실행 ────────────────────────────────────────────────────────

def _reset_model_cache():
    if Path(MODEL_DIR).exists():
        shutil.rmtree(MODEL_DIR)


def _feed(samples: list[dict]) -> list[dict]:
    """samples를 순서 그대로 detection_node()에 하나씩 흘려보내고, 판단 결과를
    원본 샘플에 덧붙여 반환한다 (버퍼 리셋은 안 함 — 호출부가 관리)."""
    results = []
    for s in samples:
        state = {
            "resource_id": s["sample_id"],
            "resource_type": s["resource_type"],
            "raw_metrics": s["raw_metrics"],
        }
        out = da.detection_node(state)
        results.append({
            **s,
            "actual_anomaly_flag": out["anomaly_flag"],
            "actual_triggered_metrics": out["triggered_metrics"],
            "actual_zscore": out["anomaly_score_zscore"],
            "actual_iforest": out["anomaly_score_iforest"],
        })
    return results


def _buffer_stats() -> dict:
    """웜업 직후 버퍼가 리소스 타입별로 얼마나 찼는지 확인용 (진단 정보)."""
    buffer_by_type, pending_count = da._load_training_buffer()
    return {
        "pending_count": pending_count,
        "buffer_size_by_type": {rt: len(v) for rt, v in buffer_by_type.items()},
    }


# ── 버퍼 채택/제외 판정 사유 진단 ────────────────────────────────────────────────
# _get_or_train_iforest의 believed_normal 판정은 두 조건의 AND:
#   provisional_score < IFOREST_THRESHOLD * BUFFER_SCORE_MARGIN  (IForest 조건)
#   z_max            < Z_SCORE_THRESHOLD * BUFFER_ZSCORE_MARGIN  (Z-score 조건)
# 어느 쪽 때문에 탈락했는지는 detection_node 반환값만으로는 알 수 없어서(둘 다 내부
# 지역변수), _get_or_train_iforest가 남기는 "[iforest_buffer] 채택/제외" 로그 레코드를
# 가로채서(logging.Handler) 그 안의 실제 값(provisional_score, z_max)을 그대로 읽는다.
# ⚠️ _get_or_train_iforest를 또 부르면 버퍼가 중복 반영되므로, 절대 재호출하지 않고
# detection_node가 이미 하는 호출 1번에서 나오는 로그만 관찰한다.

class _BufferDecisionCapture(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.INFO)
        self.records: list[dict] = []

    def emit(self, record: logging.LogRecord) -> None:
        if "[iforest_buffer]" not in record.msg:
            return
        accepted = "채택" in record.msg
        excluded = "제외" in record.msg
        if not (accepted or excluded):
            return
        args = record.args or ()
        if len(args) < 3:
            return
        resource_type, score, z_max = args[0], args[1], args[2]
        self.records.append({
            "accepted": accepted,
            "resource_type": resource_type,
            "score": float(score),
            "z_max": float(z_max),
        })


def diagnose_buffer_decisions(samples: list[dict]) -> list[dict]:
    """samples를 detection_node에 흘려보내면서 버퍼 채택/제외 판정 로그를 그대로 수집한다.
    (샘플 목록의 리소스 타입별 첫 윈도우는 콜드스타트 경로라 이 로그 자체가 안 남는다 —
    캐시된 모델이 없어서 무조건 학습 데이터로 쓰이기 때문. 그래서 집계 모수는
    "리소스 타입당 (전체 개수 - 1)"이 된다.)"""
    _reset_model_cache()
    da.IFOREST_MODEL_DIR = MODEL_DIR

    logger = logging.getLogger("pipeline.detection_agent")
    prev_level = logger.level
    logger.setLevel(logging.INFO)
    capture = _BufferDecisionCapture()
    logger.addHandler(capture)
    try:
        _feed(samples)
    finally:
        logger.removeHandler(capture)
        logger.setLevel(prev_level)

    _reset_model_cache()
    return capture.records


def summarize_rejection_reasons(records: list[dict]) -> dict:
    score_limit = da.IFOREST_THRESHOLD * da.BUFFER_SCORE_MARGIN
    zmax_limit = da.Z_SCORE_THRESHOLD * da.BUFFER_ZSCORE_MARGIN

    by_type: dict[str, dict] = {}
    for r in records:
        d = by_type.setdefault(r["resource_type"], {
            "n_seen": 0, "n_accepted": 0, "n_rejected": 0,
            "rejected_score_only": 0, "rejected_zmax_only": 0, "rejected_both": 0,
        })
        d["n_seen"] += 1
        if r["accepted"]:
            d["n_accepted"] += 1
            continue
        d["n_rejected"] += 1
        score_fail = r["score"] >= score_limit
        zmax_fail = r["z_max"] >= zmax_limit
        if score_fail and zmax_fail:
            d["rejected_both"] += 1
        elif score_fail:
            d["rejected_score_only"] += 1
        elif zmax_fail:
            d["rejected_zmax_only"] += 1

    total = {
        "n_seen": sum(d["n_seen"] for d in by_type.values()),
        "n_accepted": sum(d["n_accepted"] for d in by_type.values()),
        "n_rejected": sum(d["n_rejected"] for d in by_type.values()),
        "rejected_score_only": sum(d["rejected_score_only"] for d in by_type.values()),
        "rejected_zmax_only": sum(d["rejected_zmax_only"] for d in by_type.values()),
        "rejected_both": sum(d["rejected_both"] for d in by_type.values()),
    }

    return {
        "thresholds": {
            "score_limit (IFOREST_THRESHOLD*BUFFER_SCORE_MARGIN)": round(score_limit, 4),
            "zmax_limit (Z_SCORE_THRESHOLD*BUFFER_ZSCORE_MARGIN)": round(zmax_limit, 4),
        },
        "total": total,
        "by_resource_type": by_type,
    }


def _print_rejection_summary(summary: dict) -> None:
    t = summary["total"]
    print(f"\n[버퍼 채택/제외 판정 사유] 관찰 대상 {t['n_seen']}건 "
          f"(리소스 타입당 첫 윈도우=콜드스타트라 로그 자체가 안 남아서 제외됨)")
    print(f"  채택={t['n_accepted']}  제외={t['n_rejected']}  "
          f"(제외 중 IForest만 탈락={t['rejected_score_only']}  "
          f"Z-score만 탈락={t['rejected_zmax_only']}  둘다 탈락={t['rejected_both']})")
    for rt, d in summary["by_resource_type"].items():
        print(f"  - {rt:<14} 관찰={d['n_seen']:<4} 채택={d['n_accepted']:<4} 제외={d['n_rejected']:<4} "
              f"(IForest만={d['rejected_score_only']:<3} Z-score만={d['rejected_zmax_only']:<3} "
              f"둘다={d['rejected_both']})")


def _run_through_detection_node(samples: list[dict], warmup_samples: list[dict] | None = None) -> dict:
    """모델 캐시를 리셋한 뒤, (있으면) warmup_samples를 먼저 흘려보내 버퍼를 채우고
    (결과는 집계 안 함), 그 다음 samples를 흘려보내 결과를 집계한다.
    반환: {"results": [...], "warmup_buffer_stats": {...} | None}
    """
    _reset_model_cache()
    da.IFOREST_MODEL_DIR = MODEL_DIR

    warmup_buffer_stats = None
    if warmup_samples:
        _feed(warmup_samples)
        warmup_buffer_stats = _buffer_stats()

    results = _feed(samples)

    _reset_model_cache()
    return {"results": results, "warmup_buffer_stats": warmup_buffer_stats}


# ── 집계 ──────────────────────────────────────────────────────────────────────

def _confusion(results: list[dict]) -> dict:
    tp = sum(1 for r in results if r["expect_trigger"] and r["actual_anomaly_flag"])
    fp = sum(1 for r in results if not r["expect_trigger"] and r["actual_anomaly_flag"])
    tn = sum(1 for r in results if not r["expect_trigger"] and not r["actual_anomaly_flag"])
    fn = sum(1 for r in results if r["expect_trigger"] and not r["actual_anomaly_flag"])
    n = len(results)
    accuracy = (tp + tn) / n if n else 0.0
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    return {
        "n": n, "tp": tp, "fp": fp, "tn": tn, "fn": fn,
        "accuracy": round(accuracy, 4), "precision": round(precision, 4),
        "recall": round(recall, 4), "f1": round(f1, 4),
    }


def _by_pattern(results: list[dict]) -> dict:
    out = {}
    for pattern in ("normal", "sustained", "blip"):
        subset = [r for r in results if r["pattern"] == pattern]
        n = len(subset)
        n_triggered = sum(1 for r in subset if r["actual_anomaly_flag"])
        out[pattern] = {
            "n": n,
            "n_triggered": n_triggered,
            "trigger_rate": round(n_triggered / n, 4) if n else 0.0,
        }
    return out


def _by_resource_type_sustained(results: list[dict]) -> dict:
    """sustained 케이스만 리소스 타입별 recall (Phase 4 스타일 breakdown과 비교하기 쉽게)."""
    out = {}
    for rt in BASELINES:
        subset = [r for r in results if r["pattern"] == "sustained" and r["resource_type"] == rt]
        n = len(subset)
        n_caught = sum(1 for r in subset if r["actual_anomaly_flag"])
        out[rt] = {"n": n, "n_caught": n_caught, "rate": round(n_caught / n, 4) if n else 0.0}
    return out


def _detector_breakdown(results: list[dict]) -> dict:
    """sustained 케이스 중 z-score/IForest 중 뭐가 잡았는지 (triggered_metrics 비어있으면
    IForest 단독으로 잡은 것 — detection_node는 OR 앙상블이라 둘 다 반환 안 함)."""
    sustained = [r for r in results if r["pattern"] == "sustained" and r["actual_anomaly_flag"]]
    caught_by_zscore = sum(1 for r in sustained if r["actual_triggered_metrics"])
    caught_by_iforest_only = sum(1 for r in sustained if not r["actual_triggered_metrics"])
    return {
        "n_sustained_caught": len(sustained),
        "caught_by_zscore": caught_by_zscore,
        "caught_by_iforest_only": caught_by_iforest_only,
    }


def _summarize(label: str, run_output: dict) -> dict:
    results = run_output["results"]
    summary = {
        "label": label,
        "confusion": _confusion(results),
        "by_pattern": _by_pattern(results),
        "by_resource_type_sustained": _by_resource_type_sustained(results),
        "detector_breakdown": _detector_breakdown(results),
        "warmup_buffer_stats": run_output["warmup_buffer_stats"],
    }
    c = summary["confusion"]
    print(f"\n[{label}] n={c['n']}  accuracy={c['accuracy']}  precision={c['precision']}  "
          f"recall={c['recall']}  f1={c['f1']}")
    for pattern, stats in summary["by_pattern"].items():
        print(f"  - {pattern:<10} n={stats['n']:<4} triggered={stats['n_triggered']:<4} "
              f"rate={stats['trigger_rate']}")
    db = summary["detector_breakdown"]
    print(f"  - sustained 잡은 경로: z-score={db['caught_by_zscore']}  "
          f"IForest단독={db['caught_by_iforest_only']} (총 {db['n_sustained_caught']}건)")
    if summary["warmup_buffer_stats"]:
        print(f"  - 웜업 후 버퍼 크기: {summary['warmup_buffer_stats']['buffer_size_by_type']}")
    return summary


def main() -> None:
    dataset = generate_dataset()
    warmup_samples = generate_warmup_samples()
    shuffled = dataset.copy()
    random.Random(RANDOM_SEED).shuffle(shuffled)

    print("=" * 70)
    print(f"Phase 6: detection_node() E2E 평가 (평가 샘플 {len(dataset)}개, "
          f"웜업 샘플 {len(warmup_samples)}개 — 리소스 타입당 {N_WARMUP_PER_TYPE}개)")
    print(f"  normal={sum(1 for s in dataset if s['pattern']=='normal')} / "
          f"sustained={sum(1 for s in dataset if s['pattern']=='sustained')} / "
          f"blip={sum(1 for s in dataset if s['pattern']=='blip')}")
    print("=" * 70)

    # 웜업 샘플(순수 정상)이 버퍼에서 왜 탈락하는지 사유별 집계 — 이전 실행에서
    # 리소스 타입당 30개 중 5~9개만 버퍼에 들어간 이유를 확인하기 위함.
    rejection_records = diagnose_buffer_decisions(warmup_samples)
    rejection_summary = summarize_rejection_reasons(rejection_records)
    _print_rejection_summary(rejection_summary)

    scenarios = [
        ("콜드스타트 / 원래 순서",   dataset,  None),
        ("콜드스타트 / 무작위 순서", shuffled, None),
        ("웜업 후 / 원래 순서",      dataset,  warmup_samples),
        ("웜업 후 / 무작위 순서",    shuffled, warmup_samples),
    ]

    summaries = {}
    for label, samples, warmup in scenarios:
        run_output = _run_through_detection_node(samples, warmup_samples=warmup)
        summaries[label] = _summarize(label, run_output)

    print("\n" + "=" * 70)
    print("콜드스타트 vs 웜업 후 비교 (accuracy / recall / normal 오탐률 / blip 오탐률)")
    print("=" * 70)
    for label, summary in summaries.items():
        c = summary["confusion"]
        normal_fpr = summary["by_pattern"]["normal"]["trigger_rate"]
        blip_fpr = summary["by_pattern"]["blip"]["trigger_rate"]
        print(f"  {label:<20} acc={c['accuracy']:<8} recall={c['recall']:<8} "
              f"normal오탐={normal_fpr:<8} blip오탐={blip_fpr}")

    result = {
        "config": {
            "n_normal_per_type": N_NORMAL_PER_TYPE,
            "n_sustained_per_metric": N_SUSTAINED_PER_METRIC,
            "n_blip_per_metric": N_BLIP_PER_METRIC,
            "n_warmup_per_type": N_WARMUP_PER_TYPE,
            "spike_multiplier": SPIKE_MULTIPLIER,
            "window_size": WINDOW_SIZE,
        },
        "warmup_rejection_reasons": rejection_summary,
        "scenarios": summaries,
    }

    RESULT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(RESULT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"\n저장 완료: {RESULT_PATH}")


if __name__ == "__main__":
    main()
