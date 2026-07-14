# playground/test_autoscaling_scaledown.py
"""
실제 AWS AutoScaling 그룹을 대상으로 ScaleDown -> Rollback 흐름을 확인하는
수동 테스트 스크립트.

사전 조건:
  - .env에 ASG_NAME, AWS_DEFAULT_REGION, AWS 자격증명 설정
  - ASG_NAME으로 지정한 AutoScaling 그룹이 존재해야 함

실행: python playground/test_autoscaling_scaledown.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from pipeline.action_agent import take_snapshot, execute_action, rollback_action

ASG_NAME = os.getenv("ASG_NAME")
RESOURCE_TYPE = "AutoScaling"

if not ASG_NAME:
    raise SystemExit("ASG_NAME 환경변수를 설정해주세요 (.env)")

print("=== 1. 스냅샷 저장 (현재 max_size, desired_capacity 기록) ===")
snapshot = take_snapshot(RESOURCE_TYPE, ASG_NAME)
print("snapshot:", snapshot)

print("\n=== 2. ScaleDown 실행 (MaxSize=2, DesiredCapacity=min(현재값, 2)) ===")
result = execute_action("ScaleDown", RESOURCE_TYPE, ASG_NAME)
print("result:", result)

print("\n=== 3. 콘솔 출력으로 결과 확인 (ScaleDown 직후 스냅샷 재조회) ===")
after_scaledown_snapshot = take_snapshot(RESOURCE_TYPE, ASG_NAME)
print("after_scaledown_snapshot:", after_scaledown_snapshot)

print(
    f"\n=== 4. rollback_action으로 원래 설정"
    f"(max_size={snapshot['max_size']}, desired_capacity={snapshot['desired_capacity']})으로 복원 ==="
)
rollback = rollback_action(RESOURCE_TYPE, ASG_NAME, snapshot)
print("rollback:", rollback)

print("\n=== 5. 최종 상태 확인 ===")
final_snapshot = take_snapshot(RESOURCE_TYPE, ASG_NAME)
print("final_snapshot:", final_snapshot)
