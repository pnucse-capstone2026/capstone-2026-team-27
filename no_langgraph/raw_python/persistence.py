"""
no_langgraph/raw_python/persistence.py  (신규)

LangGraph의 checkpointer가 대신해주던 일을 직접 구현한 것: "지금 state가
뭐고, 어디까지 진행했는지"를 durable storage에 저장했다가, 나중에(다른
프로세스 실행에서도) 그 지점부터 이어갈 수 있게 하는 코드.

여기서는 JSON 파일로 흉내낸다. 실제 운영에서는 DB 테이블이 필요하고,
동시성(같은 thread_id를 두 프로세스가 동시에 재개하려는 경우) 처리도
추가로 필요하다 — 그런 부분까지 LangGraph의 checkpointer는 이미 처리해
준다는 걸 감안해서 봐야 한다 (이 파일은 최소 기능만 구현).
"""

import dataclasses
import json
import os

from no_langgraph.raw_python.state import PipelineState

_STORE_DIR = os.path.join(os.path.dirname(__file__), "_checkpoints")


def save_checkpoint(thread_id: str, state: PipelineState, next_step: str) -> None:
    os.makedirs(_STORE_DIR, exist_ok=True)
    path = os.path.join(_STORE_DIR, f"{thread_id}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(
            {"state": dataclasses.asdict(state), "next_step": next_step},
            f, ensure_ascii=False, indent=2,
        )


def load_checkpoint(thread_id: str) -> tuple[PipelineState, str]:
    path = os.path.join(_STORE_DIR, f"{thread_id}.json")
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return PipelineState(**data["state"]), data["next_step"]


def delete_checkpoint(thread_id: str) -> None:
    path = os.path.join(_STORE_DIR, f"{thread_id}.json")
    if os.path.exists(path):
        os.remove(path)
