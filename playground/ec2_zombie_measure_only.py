"""
playground/ec2_zombie_measure_only.py

ec2_zombie_real_pipeline_trial.py가 이미 시작+부하주입+대기까지 마쳐놓은 13대에
대해 "측정(measure)"과 "정리(cleanup)" 단계만 재실행한다. 배경: 3시간 대기를
로컬 프로세스로 버티는 방식이 메모리 부족으로 두 차례 종료되었으나, 인스턴스와
SSM 부하 자체는 로컬 프로세스와 무관하게 AWS 쪽에서 계속 살아있었으므로,
나이 가드(2.5시간) 통과 후 이 스크립트로 측정만 이어서 실행한다.
"""
from __future__ import annotations

import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from playground.ec2_zombie_real_pipeline_trial import (
    EC2_INSTANCES, RESULT_DIR, _setup_logging, _measure_one, cleanup, _setup_clients, logger,
)

_setup_logging()
ec2, ssm = _setup_clients()

# ⚠️ 2026-09-13 발견: measure()가 스레드마다 처음으로 boto3 클라이언트를 만드는데,
# detection-runtime 프로파일이 source_profile=default로 역할을 위임(assume role)하는
# 체인이라, 13개 스레드가 동시에 최초 자격증명 해석을 시도하면 botocore 내부 상태가
# 스레드-안전하지 않아 "Infinite loop in credential configuration detected"라는
# 가짜 순환 감지 오류가 간헐적으로 난다. 메인 스레드에서 한 번 미리 워밍업해서
# 캐시된 세션을 공유하게 하면 이 레이스를 피할 수 있다.
import boto3 as _boto3
_boto3.client("sts", region_name="ap-northeast-2").get_caller_identity()
logger.info("자격증명 워밍업 완료 (스레드 레이스 방지)")

logger.info("=== 측정 시작 (13대 병렬, bypass_approval_for_timing=True) ===")
results = []
with ThreadPoolExecutor(max_workers=len(EC2_INSTANCES)) as ex:
    futures = {
        ex.submit(_measure_one, iid, label, profile): iid
        for iid, label, profile in EC2_INSTANCES
    }
    for fut in as_completed(futures):
        results.append(fut.result())

order = [iid for iid, _, _ in EC2_INSTANCES]
results.sort(key=lambda r: order.index(r["resource_id"]))

RESULT_DIR.mkdir(parents=True, exist_ok=True)
out_path = RESULT_DIR / f"ec2_zombie_real_pipeline_trial_{datetime.now().strftime('%Y%m%d')}.json"
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=2)

summary = [{"resource_id": r["resource_id"], "label": r["label"], "profile": r["profile"],
            "anomaly_flag": r.get("anomaly_flag"), "anomaly_type": r.get("anomaly_type"),
            "action": r.get("selected_action"), "qa_passed": r.get("qa_passed")}
           for r in results]
logger.info("=== 결과 ===")
logger.info(json.dumps(summary, ensure_ascii=False, indent=2))
logger.info("결과 저장: %s", out_path)

n_normal_fp = sum(1 for r in results if r["label"] == "normal" and r.get("anomaly_flag"))
n_anomaly_detected = sum(1 for r in results if r["label"] == "anomaly" and r.get("anomaly_flag"))
logger.info("normal 오탐: %d/8, anomaly 정탐: %d/5", n_normal_fp, n_anomaly_detected)

logger.info("정리(전체 정지) 시작...")
cleanup(ec2, EC2_INSTANCES)
