# no_langgraph

"LangGraph를 전혀 모른다고 가정하고" 클라우드 비용 이상 탐지 파이프라인을
순수 파이썬으로 처음부터 설계한 버전과, 같은 부품(stage 함수)을 LangGraph의
`StateGraph`로 다시 조립한 버전을 나란히 비교하기 위한 폴더.

기존 `pipeline/`, `schema/` 등 프로젝트 코드는 건드리지 않았다 — 이 폴더는
완전히 독립적인 데모이며, 각 stage는 실제 AWS/LLM/DB 호출 없는 더미 로직이다.

## 구조

```
no_langgraph/
├── raw_python/            # 1단계: 순수 Python (if/while만 사용)
│   ├── state.py           # 공유 상태 (dataclass)
│   ├── detection.py       # 개발자 A
│   ├── classification_qa.py   # 개발자 B (Classification + QA)
│   ├── decision_action.py     # 개발자 C (Decision + Action + rollback)
│   ├── logging_stage.py       # 담당 미배정
│   ├── orchestrator.py        # 담당 미배정 — 재시도/롤백 루프
│   └── demo.py                 # 실행 진입점
├── langgraph_version/     # 3단계: LangGraph StateGraph 버전
│   ├── graph.py            # raw_python의 stage 함수를 재사용, 조립만 다시 함
│   └── demo.py
├── DESIGN_NOTES.md        # 2단계: 설계 고민 / 버그 유형 / 팀 충돌 지점
└── COMPARISON.md          # 3단계: 실측 기반 정량 비교
```

## 실행

```bash
python -m no_langgraph.raw_python.demo
python -m no_langgraph.langgraph_version.demo
```

둘 다 동일한 3가지 시나리오(재시도 0회/1회/2회 소진)를 돌리고 결과가
같은지 assert로 확인한다.

## 읽는 순서

1. `raw_python/state.py`, `raw_python/orchestrator.py` — 순수 파이썬 설계
2. `DESIGN_NOTES.md` — 짜면서 겪은 고민 / 실제 버그 유형 / 팀 충돌 지점
3. `langgraph_version/graph.py` — 같은 로직을 StateGraph로 재조립
4. `COMPARISON.md` — 두 버전에 실제로 같은 변경을 적용해서 측정한 결과
