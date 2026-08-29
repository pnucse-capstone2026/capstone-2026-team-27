"""
no_langgraph/raw_python/retry_policy.py (신규)

LangGraph 노드에 내장된 RetryPolicy(초기 대기시간, 배수 증가, 최대 대기시간,
지터, 재시도 대상 예외 필터, 최대 시도 횟수)와 동일한 기능을 손으로
구현한 것. "boto3 호출이 일시적 오류(예: 네트워크 순단)로 실패하면 지수
백오프로 자동 재시도"하는 요구사항에 쓴다.
"""

import random
import time
from functools import wraps


def retry_with_backoff(
    max_attempts: int = 3,
    initial_interval: float = 0.5,
    backoff_factor: float = 2.0,
    max_interval: float = 128.0,
    jitter: bool = True,
    retry_on: tuple[type[Exception], ...] = (ConnectionError,),
):
    """LangGraph의 RetryPolicy(max_attempts=..., initial_interval=..., ...)와
    같은 파라미터를 갖는 재시도 데코레이터."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            interval = initial_interval
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except retry_on:
                    if attempt == max_attempts:
                        raise
                    sleep_time = min(interval, max_interval)
                    if jitter:
                        sleep_time *= random.uniform(0.5, 1.5)
                    time.sleep(sleep_time)
                    interval *= backoff_factor
        return wrapper
    return decorator
