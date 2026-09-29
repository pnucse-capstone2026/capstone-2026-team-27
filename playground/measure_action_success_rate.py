"""
playground/measure_action_success_rate.py

action_agent.py가 실제 액션을 실행할 때마다 남기는 schema/logs/action_execution_log.jsonl을
집계해서 "Action 실행 성공률"을 계산한다.

NoAction/pending_approval은 애초에 실행 자체가 없었으므로 로그에 안 남는다(action_agent.py
_log_action_execution 참고) — 여기 집계되는 건 전부 "실제로 boto3 API를 호출 시도한 것"만.

[실행 방법]
  python playground/measure_action_success_rate.py
"""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

LOG_PATH = PROJECT_ROOT / "schema" / "logs" / "action_execution_log.jsonl"


def main() -> None:
    if not LOG_PATH.exists():
        print(f"{LOG_PATH} 없음 — 아직 실제 액션이 실행된 적이 없음.")
        return

    entries = []
    with open(LOG_PATH, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    if not entries:
        print("로그 파일은 있는데 항목이 없음.")
        return

    total = len(entries)
    success = sum(1 for e in entries if e.get("status") == "success")
    fail = total - success

    print(f"전체 실행 시도: {total}건")
    print(f"성공: {success}건, 실패: {fail}건")
    print(f"성공률: {success/total*100:.1f}%")

    print("\n=== 리소스 타입별 ===")
    by_type = Counter((e.get("resource_type"), e.get("status")) for e in entries)
    types = sorted(set(e.get("resource_type") for e in entries))
    for rt in types:
        t_total = sum(1 for e in entries if e.get("resource_type") == rt)
        t_success = sum(1 for e in entries if e.get("resource_type") == rt and e.get("status") == "success")
        print(f"  {rt}: {t_success}/{t_total} ({t_success/t_total*100:.1f}%)")

    print("\n=== 액션별 ===")
    actions = sorted(set(e.get("action") for e in entries))
    for act in actions:
        a_total = sum(1 for e in entries if e.get("action") == act)
        a_success = sum(1 for e in entries if e.get("action") == act and e.get("status") == "success")
        print(f"  {act}: {a_success}/{a_total} ({a_success/a_total*100:.1f}%)")

    failed_entries = [e for e in entries if e.get("status") != "success"]
    if failed_entries:
        print("\n=== 실패 상세 ===")
        for e in failed_entries:
            print(f"  {e.get('resource_type')}:{e.get('resource_id')} {e.get('action')} "
                  f"-> {e.get('status')} ({e.get('error')})")


if __name__ == "__main__":
    main()
