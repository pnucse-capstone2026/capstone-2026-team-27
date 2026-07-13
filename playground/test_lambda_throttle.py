# playground/test_lambda_throttle.py
"""
실제 AWS Lambda 함수를 대상으로 Throttle(동시성 제한) -> Rollback 흐름을 확인하는
수동 테스트 스크립트.

사전 조건:
  - .env에 LAMBDA_FUNCTION_NAME, AWS_DEFAULT_REGION, AWS 자격증명 설정
  - LAMBDA_FUNCTION_NAME으로 지정한 함수가 존재해야 함

실행: python playground/test_lambda_throttle.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from pipeline.action_agent import take_snapshot, execute_action, rollback_action

FUNCTION_NAME = os.getenv("LAMBDA_FUNCTION_NAME")
RESOURCE_TYPE = "Lambda"

if not FUNCTION_NAME:
    raise SystemExit("LAMBDA_FUNCTION_NAME 환경변수를 설정해주세요 (.env)")

print("=== 1. 스냅샷 저장 (현재 동시성 설정 기록) ===")
snapshot = take_snapshot(RESOURCE_TYPE, FUNCTION_NAME)
print("snapshot:", snapshot)

print("\n=== 2. Throttle 실행 (동시성 10으로 제한) ===")
result = execute_action("Throttle", RESOURCE_TYPE, FUNCTION_NAME)
print("result:", result)

print("\n=== 3. 콘솔 출력으로 결과 확인 (Throttle 직후 스냅샷 재조회) ===")
after_throttle_snapshot = take_snapshot(RESOURCE_TYPE, FUNCTION_NAME)
print("after_throttle_snapshot:", after_throttle_snapshot)

print(f"\n=== 4. rollback_action으로 원래 동시성 설정({snapshot['reserved_concurrency']})으로 복원 ===")
rollback = rollback_action(RESOURCE_TYPE, FUNCTION_NAME, snapshot)
print("rollback:", rollback)

print("\n=== 5. 최종 상태 확인 ===")
final_snapshot = take_snapshot(RESOURCE_TYPE, FUNCTION_NAME)
print("final_snapshot:", final_snapshot)
