"""
no_langgraph/langgraph_version/demo.py

raw_python/demo.py와 완전히 동일한 3가지 시나리오, 동일한 assert로
LangGraph 버전을 검증한다. (같은 부품을 다르게 조립했을 때 결과가 같은지
확인하는 것 자체가 이 비교의 핵심.)

실행: 프로젝트 루트에서 `python -m no_langgraph.langgraph_version.demo`
"""

from no_langgraph.raw_python.state import PipelineState
from no_langgraph.langgraph_version.graph import app


def _run(resource_id: str) -> dict:
    """
    app.invoke()는 dataclass를 넣어도 dict를 반환한다 (LangGraph 내부적으로
    상태를 채널 dict로 관리하기 때문). raw_python은 dataclass를 그대로
    돌려주는 것과의 실제 차이점 중 하나 — COMPARISON.md에 적어둔다.
    """
    state = PipelineState(
        resource_id=resource_id,
        resource_type="EC2",
        raw_metrics={"cost": [0.5] * 27 + [3.0, 3.2, 3.5]},
    )
    print(f"\n{'='*70}\n실행: {resource_id}\n{'='*70}")
    return app.invoke(state)


def main():
    r1 = _run("i-normal")
    assert r1["rollback_count"] == 0 and r1["qa_passed"] is True

    r2 = _run("i-RECOVER1")
    assert r2["rollback_count"] == 1 and r2["qa_passed"] is True

    r3 = _run("i-FAIL")
    assert r3["rollback_count"] == 2 and r3["qa_passed"] is False

    print("\n모든 시나리오 통과 (rollback_count: 0 / 1 / 2) — raw_python과 동일한 결과")


if __name__ == "__main__":
    main()
