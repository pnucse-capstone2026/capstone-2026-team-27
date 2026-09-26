"""
run_scheduler.py — Phase E 오케스트레이션 진입점.

AWS CloudWatch에서 Detection=true 태그가 붙은 리소스를 찾아서 지표를 수집하고,
이상이 있는지 탐지한다.

⚠️ 안전 설계: 여기서는 탐지만 하고 끝난다. 실제 액션(Stop/Throttle 등)은 자동
   실행하지 않는다 — 이상이 발견된 리소스 목록만 출력한다. 실제로 전체 파이프라인
   (classification → decision → action → qa → logging)까지 이어서 돌리고 싶으면
   pipeline.graph.app.invoke(state)를 별도로, 명시적으로 호출해야 한다.

[실행 방법]
  python run_scheduler.py           # 1회만 실행하고 종료
  python run_scheduler.py --loop    # 관리자 설정의 폴링 주기(config/decision_policy.json)로 반복 실행 (Ctrl+C로 중단)
"""

from __future__ import annotations

import sys
import time

from config import decision_policy
from pipeline.orchestrator import run_detection_cycle


def run_once() -> list[dict]:
    print("=" * 70)
    print("탐지 사이클 시작 (디스커버리 → 지표 수집 → 순차 스캔)")
    print("=" * 70)

    anomalies = run_detection_cycle(resource_types=decision_policy.get_enabled_resource_types())

    print(f"\n이상 탐지: {len(anomalies)}건")
    for a in anomalies:
        print(
            f"  [{a['resource_type']}] {a['resource_id']}"
            f" | zscore={a['anomaly_score_zscore']}"
            f" | iforest={a['anomaly_score_iforest']}"
            f" | triggered={a['triggered_metrics']}"
        )
    print("=" * 70)
    return anomalies


if __name__ == "__main__":
    if "--loop" in sys.argv:
        print("관리자 설정의 폴링 주기로 반복 실행합니다 (Ctrl+C로 중단)")
        while True:
            run_once()
            poll_interval_seconds = decision_policy.get_polling_interval_minutes() * 60
            print(f"{poll_interval_seconds}초 대기...")
            time.sleep(poll_interval_seconds)
    else:
        run_once()
