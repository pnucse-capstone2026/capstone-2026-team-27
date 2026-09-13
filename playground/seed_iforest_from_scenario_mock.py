"""
playground/seed_iforest_from_scenario_mock.py

seed_mock_iforest_buffer.py(순수 합성 생성기 기반)를 대체하는 2세대 시딩 스크립트.
이번엔 시나리오 기반 mock 파일(playground/mock_data/*_train.json)의 label="normal"
윈도우를 읽어서 학습시킨다 — EC2 좀비/Lambda 재시도폭증은 담당자가 실제 체크 함수로
검증한 데이터, Lambda(cost_spike)/AutoScaling(EDoS)/S3(대량다운로드)는 팀원이 제공.

RDS는 아직 아무도 mock 데이터를 안 만들어서 이번 시딩에서 제외 — 나중에 실제 RDS
리소스가 들어오면 기존 "낯선 타입" 콜드스타트 경로(_model_independent_seed_check)가
그대로 처리한다(추가 코드 불필요).

models/iforest_unified.pkl, iforest_unified_train_buffer.pkl을 덮어쓴다(완전 교체,
기존 내용과 무관). 실행 전 반드시 백업 확인 — 이번 실행 직전엔
models/iforest_unified*.pkl.synthetic_seed_bak로 이전(순수 합성) 상태를 남겨뒀음.

[실행 방법] 프로젝트 루트에서: python playground/seed_iforest_from_scenario_mock.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import numpy as np

import pipeline.detection_agent as da

MOCK_DATA_DIR = PROJECT_ROOT / "playground" / "mock_data"

# resource_type -> train 파일. RDS는 아직 데이터 없어서 제외.
#
# ⚠️ Lambda만 원본(lambda_train.json, 팀원 제공)이 아니라 throttle_count/
# async_event_age 베이스라인을 덧붙인 사본(lambda_train_with_throttle_baseline.json,
# _tmp_augment_lambda_train.py로 생성)을 쓴다 — 2026-09-14 실측으로 확인: 원본엔
# 이 두 컬럼이 아예 없어서(항상 0/mask=0) IsolationForest가 이 피처로 전혀 분할을
# 안 배웠고(decision_function이 throttle_count 0→100 변화에 반응 0 — 직접 확인),
# 결과적으로 스로틀 폭증 시나리오를 이 피처로는 절대 못 잡는 상태였다. invocation/
# error/duration/cost 분포는 원본과 완전히 동일(그대로 복사) — throttle 두 컬럼만
# seed_mock_iforest_buffer.make_lambda_window와 같은 근거(F-1 실측)로 추가.
TRAIN_FILES = {
    "EC2": "ec2_train.json",
    "Lambda": "lambda_train_with_throttle_baseline.json",
    "AutoScaling": "autoscaling_train.json",
    "S3": "s3_train.json",
}


def load_normal_windows(resource_type: str, filename: str) -> list[np.ndarray]:
    path = MOCK_DATA_DIR / filename
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    windows = []
    for w in data:
        if w["label"] != "normal":
            continue
        X = da.build_unified_feature_matrix(resource_type, w["raw_metrics"])
        windows.append(X)

    # MAX_WINDOWS_PER_TYPE(30)과 동일한 상한 — 정상 운영 중 FIFO가 유지하는
    # 버퍼 크기 불변식을 시딩 단계에서도 그대로 지킨다.
    if len(windows) > da.MAX_WINDOWS_PER_TYPE:
        windows = windows[: da.MAX_WINDOWS_PER_TYPE]
    return windows


def main() -> None:
    buffer_by_type: dict[str, list[np.ndarray]] = {}
    for resource_type, filename in TRAIN_FILES.items():
        windows = load_normal_windows(resource_type, filename)
        buffer_by_type[resource_type] = windows
        print(f"  {resource_type}: {filename}에서 normal {len(windows)}개 로드")

    combined = np.vstack([np.vstack(v) for v in buffer_by_type.values() if v])
    print(f"\n총 {combined.shape[0]}행({len(buffer_by_type)}개 타입)으로 iforest_unified.pkl 재학습 중...")
    da._fit_and_cache_unified(combined)
    da._save_training_buffer(buffer_by_type, pending_count=0)

    print(f"\n완료: {da.IFOREST_MODEL_DIR}/iforest_unified.pkl, "
          f"{da.IFOREST_MODEL_DIR}/iforest_unified_train_buffer.pkl 갱신됨")
    print("MOCK_SEED_BUFFER_FROZEN=True 유지 중 - 실데이터 자연 교체는 여전히 비활성.")
    print("RDS는 이번 시딩에 없음 - 실제 RDS 리소스는 콜드스타트 경로로 처리됨.")


if __name__ == "__main__":
    main()
