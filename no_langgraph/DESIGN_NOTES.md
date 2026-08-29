# 2단계 — 순수 파이썬 버전을 짜면서 부딪힌 문제들

`raw_python/` 코드를 실제로 짜면서 겪은 설계 고민을 정리한다. 미리 결론을
내려두고 끼워 맞춘 게 아니라, 코드를 짜는 순서대로 적었다.

---

## 1. 롤백-재시도 로직을 어디에 둘 것인가 — 선택지 3개

**후보 A. QA 함수(`run_qa`) 안에 재시도 판단까지 넣는다**
QA가 "실패했다"는 걸 가장 먼저 아는 함수니까 자연스러워 보인다. 그런데
QA가 "다시 Action을 부를지 말지"까지 정하려면 QA 함수가 Action 함수를
직접 호출해야 한다 → `classification_qa.py`(개발자 B)가 `decision_action.py`
(개발자 C)를 import해야 함 → **B가 C의 내부 구현을 알아야 하는 의존성**이
생긴다. B/C를 나눠서 개발하는 이유 자체가 무너진다. 기각.

**후보 B. Action 함수(`run_action`) 안에서 자기 자신을 재귀호출한다**
재시도 카운트를 어디서 멈출지 판단하려면 결국 QA 결과가 필요한데, QA는
Action *이후*에 실행되는 단계라 Action 안에서는 아직 QA 결과를 모른다.
Action이 스스로를 부르려면 QA까지 Action 안에서 호출해야 해서 후보 A와
같은 문제(C가 B를 알아야 함)가 생긴다. 기각.

**후보 C. 둘 다 모르는 제3의 장소(orchestrator)에 둔다 — 채택**
`orchestrator.py`가 A/B/C 세 사람의 함수를 모두 import해서 순서와 재시도
조건을 결정한다. B와 C는 서로의 존재를 몰라도 되고, 각자 orchestrator만
"이런 시그니처의 함수를 만들어 달라"고 요청받으면 된다.

**대가**: 이 orchestrator.py는 A/B/C 중 누구의 담당도 아니다. 요구사항에는
"개발자 A/B/C"만 명시돼 있었지 이 파일을 누가 쓸지는 안 정해져 있었다.
결국 세 사람의 인터페이스를 전부 알아야 하는 사람(보통 마지막에 합치는
사람)이 떠맡게 된다 — 이게 아래 4번 항목("동시 개발 충돌 지점")의 핵심이다.

---

## 2. state를 넘길 때 실제로 생긴 문제

`state.py`에서 dict 대신 dataclass를 선택한 이유를 코드를 짜다가 몸으로
느꼈다. 처음에 `run_qa`를 짜면서:

```python
def run_qa(state):
    result = state.action_result or {}
    state.qa_passed = result.get("status") == "success"
    return state
```

`action_result`가 dict였다면 `state["acton_result"]`처럼 오타를 내도
아무 경고 없이 `None`이 들어오고, `run_qa`는 그냥 `qa_passed = False`로
조용히 잘못된 결과를 낸다 — "QA가 실패했다"는 결과 자체는 그럴듯해 보이기
때문에 눈치채기 더 어렵다. dataclass는 `state.acton_result`라고 쓰면
그 즉시(런타임에, import/실행 시점에) `AttributeError`가 난다.

더 근본적인 문제: **"이 시점에 어떤 필드가 채워져 있어야 하는지"를 보장하는
장치가 dataclass를 쓰든 안 쓰든 원래 없다.** 예를 들어 `run_qa`는
`state.action_result`가 채워져 있다고 가정하는데, 만약 orchestrator가
실수로 `run_action`을 부르기 전에 `run_qa`를 불러버리면(순서를 잘못
짜면) `action_result`가 `None`인 채로 `run_qa`가 실행되고, 코드는 그냥
"조용히" `qa_passed = False`를 반환한다. **함수 호출 순서가 틀렸다는 걸
알려주는 에러가 어디에도 없다** — 3단계 비교에서 이 부분이 LangGraph와
가장 크게 갈리는 지점이다.

---

## 3. 이 구조에서 실제로 나는 버그 유형 — 재시도 임계값 중복

이번 프로젝트를 진행하면서 (이 데모 이전에) 실제로 겪은 사례를 그대로
재현할 수 있다. 재시도를 멈추는 조건을 두 군데에 손으로 옮겨 적어야 하는
상황에서 아래처럼 부등호 하나를 놓치는 실수가 실제로 나왔다:

```python
# 원본 의도: "2번까지는 재시도, 2번째도 실패하면 포기"
if state.rollback_count < 2:
    continue   # 재시도
break

# 손으로 옮겨 적다가 생긴 실수 (실제로 이 프로젝트에서 재현됨)
if state.rollback_count <= 2:   # < 대신 <= 를 씀
    continue   # 재시도
break
```

`<`를 `<=`로 잘못 옮기면 "2번까지 재시도"가 아니라 "3번까지 재시도"가
된다. 두 버전 다 문법적으로 완전히 정상이고, 정상적으로 실행되고, 결과도
그럴듯해 보인다 (그냥 롤백을 한 번 더 하는 것뿐이니까). 실제로 이 차이는
**두 구현을 나란히 놓고 실행 결과(rollback_count)를 비교하는 테스트를
따로 짜야만** 드러난다 — 코드 리뷰로 한눈에 잡아내기 어렵다 (부등호
하나 차이라서).

이 프로젝트의 `no_langgraph`에서도 이 위험은 그대로 존재한다:
`raw_python/orchestrator.py`의 `MAX_RETRY = 2`와
`langgraph_version/graph.py`의 `MAX_RETRY = 2`는 **값은 같지만 import로
연결돼 있지 않은 독립된 두 상수**다. 지금 당장은 둘 다 2로 맞아떨어지지만,
둘 중 하나만 나중에 바뀌면(예: LangGraph 버전만 3으로 바꾸고 raw_python은
깜빡하면) 아무 에러 없이 두 버전이 서로 다른 동작을 하게 된다.
(COMPARISON.md 비교4에서 이 부분을 실제로 확인한다.)

---

## 4. 팀 3명이 동시에 수정한다면 어디서 충돌이 날까

| 상황 | 충돌 지점 |
|---|---|
| B가 QA 판정 기준을 바꿈 (`classification_qa.py`) | A/C와는 파일이 겹치지 않아 git 충돌은 없음. 하지만 `run_qa`가 참조하는 `action_result`의 키 이름을 C가 이미 바꿔놨다면(예: `"status"` → `"result_status"`) **git은 충돌을 못 잡는다** — 파일이 다르니까. 실행해봐야 터진다. |
| C가 `run_action`의 반환 형태를 바꿈 (`decision_action.py`) | 위와 대칭적으로 동일한 문제. B/C 사이의 "약속"(action_result 스키마)이 코드 어디에도 강제돼 있지 않다. |
| **주인 없는 파일 두 개**: `orchestrator.py`, `logging_stage.py` | 재시도 규칙을 바꾸는 사람(보통 통합 담당자, 혹은 C — Action을 제일 잘 아니까)과 로깅 포맷을 바꾸는 사람(누구든)이 **같은 파일을 동시에 건드릴 가능성이 A/B/C 세 사람의 "자기 파일" 수정보다 높다**. 담당이 안 정해진 파일이 실제 git merge conflict가 가장 잘 나는 지점이다. |
| `state.py`에 새 필드를 추가 (예: 화이트리스트 단계용 `whitelisted`) | dataclass라서 A/B/C 세 사람 다 이 파일을 최소한 한 번은 다시 확인해야 한다 — 새 필드가 자기 파트와 관련 있는지 판단은 해야 하기 때문. dict였다면 "당장 안 쓰는 사람은 몰라도 되는" 대신, 오타/누락을 아무도 못 잡는 원래 문제가 그대로 남는다. |

**요약**: 이 구조에서 A/B/C 세 사람의 "자기 파일"끼리는 의외로 충돌이
적다 (파일이 다르니까). 진짜 문제는 (1) 파일 간 암묵적 계약
(action_result의 키 이름 같은)이 코드로 강제되지 않는다는 것과,
(2) orchestrator.py처럼 "주인 없는 조율자 파일"에 여러 사람의 요구사항이
몰린다는 것이다.
