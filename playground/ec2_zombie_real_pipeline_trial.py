"""
playground/ec2_zombie_real_pipeline_trial.py

EC2 "좀비 인스턴스" 시나리오의 실 AWS 종단 검증 (정상 8 / 이상 5,
run_3x_real_pipeline.py와 동일한 구조).

⚠️ 배경 (2026-09-13 조사로 확인된 문제 재발 방지):
   batch_pipeline_run.py의 9/9 18:39 UTC 실행에서 "normal" 라벨 8대 중
   처리된 5대 전부가 cost_inefficiency로 오탐됐다. 원인은 탐지 로직 결함이
   아니라 **타이밍**이었다 — 원래 부하 생성기가 11:40~14:42 UTC(3시간)만
   돌고 자동 종료됐는데, batch_pipeline_run.py는 그로부터 4시간 뒤에 실행돼서
   이미 13대 전부 유휴 상태로 수렴해 있었다(ec2_zombie_replay_trial.py
   docstring 참고). 그 원래 부하 생성 스크립트 자체는 리포지토리에 없어서
   (ec2_zombie_manifest.json도 없음) 이번에 새로 작성했다.

   이번엔 같은 실수를 피하려고 **부하를 계속 걸어둔 채로 그 안에서 측정까지
   끝낸다** — 부하 지속시간(STRESS_DURATION_SEC)을 측정 시점(WAIT_BEFORE_
   MEASURE_SEC)보다 넉넉히 길게 잡아서, 측정 파이프라인이 도는 동안(수 분~
   수십 분)에도 부하가 살아있도록 한다.

[부하 생성 방식]
   SSM(Systems Manager)으로 인스턴스 안에서 직접 셸 스크립트를 실행한다.
   "이상"(anomaly, silent/whisper) 5대는 아무 부하도 안 주고 그냥 켜두기만
   한다 — 진짜 좀비(켜져 있지만 아무것도 안 함)를 재현하는 것.
   "정상"(normal) 8대는 프로파일별로 다른 duty-cycle로 `yes` 프로세스를
   돌려서 차등 부하를 만든다(t3.micro, 2 vCPU 기준):
     light    : 1개 프로세스, 60s on / 240s off  (5분 평균 ~10%)
     moderate : 1개 프로세스, 150s on / 150s off (5분 평균 ~25%)
     heavy    : 2개 프로세스, 270s on / 30s off  (5분 평균 ~90%)
     bursty   : 2개 프로세스, 300s on / 600s off (15분 주기로 확 튀었다 꺼짐)
   EC2 유휴 체크(_low_utilization_check)는 윈도우 내 "peak"(최댓값) 기준이라,
   각 프로파일 다 최소 한 구간에서는 5% 임계값을 확실히 넘도록 설계했다.

⚠️ 신생 인스턴스 나이 가드: EC2 유휴 판정은 resource_age_seconds가
   _EC2_IDLE_WINDOW_HOURS(2.5시간) 미만이면 보류된다. 인스턴스를 정지 후
   재시작하면 LaunchTime이 그 시점으로 갱신되므로(실측 확인됨), 최소
   2.5시간을 기다려야 "이상"(anomaly) 5대도 정상적으로 판정 대상이 된다.
   WAIT_BEFORE_MEASURE_SEC=3시간으로 여유를 둔다.

⚠️ 실제 비용 발생: t3.micro 13대 x 4시간 정도 (대략 수백원 수준으로 추정).

[실행 방법]
  python playground/ec2_zombie_real_pipeline_trial.py --run
  python playground/ec2_zombie_real_pipeline_trial.py --cleanup   # 중간에 죽었을 때 수동 정리

[생성 파일]
  playground/eval_outputs/ec2_zombie_real_pipeline_trial_{날짜}.json
  playground/eval_outputs/logs/ec2_zombie_real_pipeline_trial_{시각}.log
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

import boto3

from playground.measure_pipeline_timing import measure

AWS_REGION = "ap-northeast-2"

# ⚠️ .env의 AWS_PROFILE(detection-runtime)은 최소권한이라 IAM instance-profile
# 연결/SSM 명령 실행 권한이 없다 — lambda_throttle_retry_trial.py와 동일한 이유로
# 이 스크립트의 "테스트 셋업" 전용 클라이언트는 SETUP_PROFILE을 명시적으로 쓴다.
SETUP_PROFILE = "default"
IAM_INSTANCE_PROFILE_NAME = "detection-test-ec2-ssm-role"

# 9/9 실험과 동일한 13대 (batch_pipeline_run.py의 EC2_INSTANCES와 동일)
EC2_INSTANCES = [
    ("i-00f27d6650869a74d", "anomaly", "silent"),
    ("i-094595e331b19be17", "anomaly", "silent"),
    ("i-0238a05593fbcf2f2", "anomaly", "whisper"),
    ("i-046cbf400dd6dc9d7", "anomaly", "whisper"),
    ("i-013a9d143009d1376", "anomaly", "whisper_more"),
    ("i-0e810265b88caeac5", "normal", "light"),
    ("i-0cd6cf73f56959d2e", "normal", "light"),
    ("i-07c77db4628d7e7ca", "normal", "moderate"),
    ("i-04cec80c3045da327", "normal", "moderate"),
    ("i-0380a0b372a973a5c", "normal", "heavy"),
    ("i-053230c6e903132f8", "normal", "heavy"),
    ("i-01aed37041b8e6946", "normal", "bursty"),
    ("i-0973aa83fa2ba0dd9", "normal", "bursty"),
]

STRESS_PROFILES = {
    "light":    {"n_proc": 1, "on_sec": 60, "off_sec": 240},
    "moderate": {"n_proc": 1, "on_sec": 150, "off_sec": 150},
    "heavy":    {"n_proc": 2, "on_sec": 270, "off_sec": 30},
    "bursty":   {"n_proc": 2, "on_sec": 300, "off_sec": 600},
}

STRESS_DURATION_SEC = 14400       # 4시간 — 측정 시점보다 넉넉히 길게
WAIT_BEFORE_MEASURE_SEC = 10800   # 3시간 — 2.5시간 나이 가드 통과 + 여유
SSM_REGISTER_TIMEOUT_SEC = 300

LOG_DIR = PROJECT_ROOT / "playground" / "eval_outputs" / "logs"
RESULT_DIR = PROJECT_ROOT / "playground" / "eval_outputs"

logger = logging.getLogger("ec2_zombie_real_pipeline_trial")


def _setup_logging() -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = LOG_DIR / f"ec2_zombie_real_pipeline_trial_{ts}.log"
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.addHandler(fh)
    logger.addHandler(sh)
    logger.info("로그 파일: %s", log_path)
    return log_path


def _setup_clients():
    sess = boto3.Session(profile_name=SETUP_PROFILE)
    return sess.client("ec2", region_name=AWS_REGION), sess.client("ssm", region_name=AWS_REGION)


def _build_stress_script(profile: str, duration_sec: int) -> str:
    p = STRESS_PROFILES[profile]
    n, on, off = p["n_proc"], p["on_sec"], p["off_sec"]
    spawn = "; ".join([f"(timeout {on} yes > /dev/null &)" for _ in range(n)])
    return (
        f"END=$(( $(date +%s) + {duration_sec} ));"
        f"while [ $(date +%s) -lt $END ]; do {spawn}; sleep {on}; sleep {off}; done"
    )


def _remove_iam_instance_profile(ec2, iid: str) -> None:
    """anomaly(유휴) 인스턴스에서 IAM instance profile을 떼어낸다 — SSM Agent가
    붙어있으면 하트비트 트래픽이 계속 발생해 진짜 방치된 인스턴스를 재현 못 한다
    (2026-09-13 실측으로 발견: peak_cpu는 0.2%대로 완벽히 유휴인데 network_io
    합산이 SSM 하트비트 트래픽만으로 임계값을 40% 초과해 idle_flag가 5/5 전부
    미탐되는 사고가 실제로 있었음)."""
    try:
        resp = ec2.describe_iam_instance_profile_associations(
            Filters=[{"Name": "instance-id", "Values": [iid]}]
        )
        assocs = resp.get("IamInstanceProfileAssociations", [])
        for a in assocs:
            if a["State"] in ("associated", "associating"):
                ec2.disassociate_iam_instance_profile(AssociationId=a["AssociationId"])
                logger.info("[%s] IAM instance profile 해제 완료 (%s)", iid, a["AssociationId"])
        if not assocs:
            logger.info("[%s] IAM instance profile 원래 없음 — 스킵", iid)
    except Exception as exc:
        logger.warning("[%s] IAM instance profile 해제 실패: %s", iid, exc)


def start_and_prepare(ec2, ssm, targets: list[tuple[str, str, str]]) -> None:
    """"normal" 라벨에만 IAM instance-profile을 연결해 SSM으로 부하를 걸 준비를 하고,
    "anomaly" 라벨은 반대로 IAM profile을 떼어내 진짜 방치된(SSM 미등록) 인스턴스
    상태를 재현한다. 이후 전체를 시작하고, "normal"만 SSM 등록을 기다린다.
    """
    for iid, label, profile in targets:
        if label != "normal":
            _remove_iam_instance_profile(ec2, iid)
            continue
        try:
            ec2.associate_iam_instance_profile(
                IamInstanceProfile={"Name": IAM_INSTANCE_PROFILE_NAME}, InstanceId=iid,
            )
            logger.info("[%s] IAM instance profile 연결 완료", iid)
        except ec2.exceptions.ClientError as exc:
            if "already associated" in str(exc).lower() or "IncorrectState" in str(exc):
                logger.info("[%s] IAM instance profile 이미 연결됨 — 스킵", iid)
            else:
                logger.warning("[%s] IAM instance profile 연결 실패: %s", iid, exc)

    ids = [iid for iid, _, _ in targets]
    ec2.start_instances(InstanceIds=ids)
    logger.info("전체 %d대 시작 요청 완료", len(ids))

    waiter = ec2.get_waiter("instance_running")
    waiter.wait(InstanceIds=ids)
    logger.info("전체 %d대 running 상태 도달", len(ids))

    # SSM 등록 대기는 "normal"만 — "anomaly"는 IAM profile이 없어서 영원히 등록 안 됨
    normal_ids = [iid for iid, label, _ in targets if label == "normal"]
    deadline = time.time() + SSM_REGISTER_TIMEOUT_SEC
    pending = set(normal_ids)
    while pending and time.time() < deadline:
        resp = ssm.describe_instance_information(
            Filters=[{"Key": "InstanceIds", "Values": list(pending)}]
        )
        for info in resp["InstanceInformationList"]:
            if info["PingStatus"] == "Online":
                pending.discard(info["InstanceId"])
                logger.info("[%s] SSM 등록 확인", info["InstanceId"])
        if pending:
            time.sleep(10)
    if pending:
        logger.warning("SSM 등록 시간 초과 — 아직 미등록: %s", pending)


def inject_load(ssm, targets: list[tuple[str, str, str]]) -> None:
    """"normal" 라벨 인스턴스에만 프로파일별 부하를 건다. "anomaly"는 그대로 둔다.

    ⚠️ 2026-09-13 버그로 발견: send_command의 최상위 TimeoutSeconds는 "명령이
    시작되길 기다리는 시간"이지 "실행 지속시간"이 아니다 — AWS-RunShellScript의
    실제 실행 시간 제한은 Parameters.executionTimeout(플러그인 레벨, 기본 3600초
    =1시간)이 따로 있는데 이걸 빠뜨려서, 4시간짜리 부하 스크립트가 1시간 만에
    TimedOut(SIGKILL)으로 강제 종료되는 사고가 실제로 났다(13대 전부 유휴로
    수렴 → 9/9와 동일한 타이밍 문제 재발). executionTimeout을 명시로 넘겨서
    STRESS_DURATION_SEC과 맞춘다.
    """
    for iid, label, profile in targets:
        if label != "normal":
            logger.info("[%s] anomaly(%s) — 부하 없음, 유휴 상태 유지", iid, profile)
            continue
        script = _build_stress_script(profile, STRESS_DURATION_SEC)
        resp = ssm.send_command(
            InstanceIds=[iid],
            DocumentName="AWS-RunShellScript",
            Parameters={
                "commands": [script],
                "executionTimeout": [str(STRESS_DURATION_SEC + 300)],
            },
            TimeoutSeconds=600,  # 명령 "시작"까지 기다리는 시간 — 실행시간과 무관, 여유만 둠
        )
        logger.info("[%s] 부하 주입 시작 (profile=%s, command_id=%s, %d초간, executionTimeout=%d)",
                    iid, profile, resp["Command"]["CommandId"], STRESS_DURATION_SEC,
                    STRESS_DURATION_SEC + 300)


def _measure_one(resource_id: str, label: str, profile: str) -> dict:
    logger.info("[측정 %s] measure() 시작 (label=%s, profile=%s)", resource_id, label, profile)
    try:
        result = measure(resource_id, "EC2", bypass_approval_for_timing=True)
        logger.info("[측정 %s] 완료 — anomaly_flag=%s, anomaly_type=%s, action=%s, qa_passed=%s",
                    resource_id, result.get("anomaly_flag"), result.get("anomaly_type"),
                    result.get("selected_action"), result.get("qa_passed"))
    except Exception as exc:
        logger.error("[측정 %s] 실패: %s", resource_id, exc)
        return {"resource_id": resource_id, "label": label, "profile": profile, "error": str(exc)}
    return {**result, "label": label, "profile": profile,
            "measured_at": datetime.now(timezone.utc).isoformat()}


def cleanup(ec2, targets: list[tuple[str, str, str]]) -> None:
    """측정이 끝난 뒤(또는 --cleanup으로 수동) 전부 정지한다.
    anomaly로 잡혀 이미 Stop된 인스턴스는 그대로 두면 되고(멱등), normal은
    이 함수가 명시적으로 정지시켜야 한다 — 아무도 자동으로 안 꺼주기 때문.
    """
    ids = [iid for iid, _, _ in targets]
    try:
        ec2.stop_instances(InstanceIds=ids)
        logger.info("전체 %d대 정지 요청 완료", len(ids))
    except Exception as exc:
        logger.warning("정지 요청 중 일부 실패(이미 정지 중일 수 있음): %s", exc)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="store_true", help="시작 + 부하주입 + 대기 + 측정 + 정리")
    parser.add_argument("--cleanup", action="store_true", help="13대 전부 정지만 수동 실행")
    args = parser.parse_args()

    if not args.run and not args.cleanup:
        parser.error("--run 또는 --cleanup 중 하나는 지정해야 함")

    _setup_logging()
    ec2, ssm = _setup_clients()

    if args.cleanup:
        cleanup(ec2, EC2_INSTANCES)
        return

    try:
        start_and_prepare(ec2, ssm, EC2_INSTANCES)
        inject_load(ssm, EC2_INSTANCES)

        logger.info("%d초(%.1f시간) 대기 — 나이 가드 통과 + 부하 누적...",
                    WAIT_BEFORE_MEASURE_SEC, WAIT_BEFORE_MEASURE_SEC / 3600)
        time.sleep(WAIT_BEFORE_MEASURE_SEC)

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

    finally:
        logger.info("정리(전체 정지) 시작...")
        cleanup(ec2, EC2_INSTANCES)


if __name__ == "__main__":
    main()
