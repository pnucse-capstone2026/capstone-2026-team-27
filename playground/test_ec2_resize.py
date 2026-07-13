# playground/test_ec2_resize.py
"""
실제 AWS EC2 인스턴스를 대상으로 Resize -> Rollback 흐름을 확인하는 수동 테스트 스크립트.

사전 조건:
  - .env에 INSTANCE_ID, AWS_DEFAULT_REGION, AWS 자격증명 설정
  - INSTANCE_ID로 지정한 인스턴스가 실행 중(running)이어야 함

실행: python playground/test_ec2_resize.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from pipeline.action_agent import take_snapshot, execute_action, rollback_action

INSTANCE_ID = os.getenv("INSTANCE_ID")
RESOURCE_TYPE = "EC2"
TARGET_INSTANCE_TYPE = "t3.small"

if not INSTANCE_ID:
    raise SystemExit("INSTANCE_ID 환경변수를 설정해주세요 (.env)")

print("=== 1. 스냅샷 저장 (현재 instance_type 기록) ===")
snapshot = take_snapshot(RESOURCE_TYPE, INSTANCE_ID)
print("snapshot:", snapshot)

print(f"\n=== 2. Resize 실행 ({snapshot['instance_type']} -> {TARGET_INSTANCE_TYPE}) ===")
result = execute_action(
    "Resize", RESOURCE_TYPE, INSTANCE_ID, target_instance_type=TARGET_INSTANCE_TYPE
)
print("result:", result)

print("\n=== 3. 콘솔 출력으로 결과 확인 (Resize 직후 스냅샷 재조회) ===")
after_resize_snapshot = take_snapshot(RESOURCE_TYPE, INSTANCE_ID)
print("after_resize_snapshot:", after_resize_snapshot)

print(f"\n=== 4. rollback_action으로 원래 타입({snapshot['instance_type']})으로 복원 ===")
rollback = rollback_action(RESOURCE_TYPE, INSTANCE_ID, snapshot)
print("rollback:", rollback)

print("\n=== 5. 최종 상태 확인 ===")
final_snapshot = take_snapshot(RESOURCE_TYPE, INSTANCE_ID)
print("final_snapshot:", final_snapshot)
