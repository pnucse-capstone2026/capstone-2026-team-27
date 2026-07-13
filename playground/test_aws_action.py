# playground/test_aws_action.py
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from pipeline.action_agent import take_snapshot, execute_action, rollback_action

INSTANCE_ID = os.getenv("INSTANCE_ID")
RESOURCE_TYPE = "EC2"

print("=== 1. 스냅샷 저장 ===")
snapshot = take_snapshot(RESOURCE_TYPE, INSTANCE_ID)
print("snapshot:", snapshot)

print("\n=== 2. Stop 액션 실행 ===")
result = execute_action("Stop", RESOURCE_TYPE, INSTANCE_ID)
print("result:", result)

print("\n=== 3. 롤백 (다시 시작) ===")
snapshot = {'instance_type': 't2.micro', 'state': 'running', 'security_group_ids': ['sg-0bc448534ddd6d14b']}
rollback = rollback_action(RESOURCE_TYPE, INSTANCE_ID, snapshot)
print("rollback:", rollback)