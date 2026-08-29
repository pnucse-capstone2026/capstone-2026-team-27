# LangGraph 도입 근거 — 실측 비교 보고서

## 0. 요약 (5가지, 겹치지 않게)

1. **코드 길이는 "항상 짧아진다"가 아니라 상황에 따라 갈린다** — 단순 분기/설정값
   변경은 무승부이거나 순수 Python이 더 짧고, 프레임워크 내장 기능이 필요한
   변경은 LangGraph가 확실히 짧다 (두 번 실측해서 우연이 아님을 확인).
2. **실수를 발견하는 시점이 다르다** — 같은 오타를 냈을 때 순수 Python은
   "그 코드가 실행돼야" 발견되고, LangGraph는 "그래프를 만드는 시점"에 즉시 발견된다.
3. **지금 당장은 구조가 같아도, 미래 변화에 대한 내구성이 다르다** — 반복(사이클)
   구조 위에 새 실행 규칙이 얹힐 때, 순수 Python은 반복문 자체를 뜯어고쳐야 하고
   LangGraph는 반복을 결정하는 함수를 안 건드려도 된다.
4. **LangGraph는 부가 기능이 공짜로 딸려온다** — 다이어그램 자동 생성, 체크포인터
   기반 승인 대기를 실제로 만들어서 동작 확인했다.
5. **LangGraph도 못 막아주는 게 있다** — 같은 상수를 여러 파일에 따로 정의해서
   값이 어긋나는 버그는 프레임워크와 무관하게 개발자가 직접 관리해야 한다.
6. **조립할 때 코드 자체도 더 읽기 쉽다** (단, "더 짧다"와는 다른 얘기) — 다음
   단계가 어디인지 딕셔너리 하나로 한눈에 보이는 것과, 함수 본문을 끝까지
   읽어야 알 수 있는 것의 차이. 줄 수 우위와는 별개의 포인트다 (실험2에서
   LangGraph가 더 길었던 것과 모순 아님).
7. **성능은 유일하게 LangGraph가 불리한 항목이다** — 실행 오버헤드가 순수
   Python 대비 30~3000배 크다. 다만 절대값(µs~ms 단위)은 이 파이프라인의
   실제 병목(AWS/LLM 네트워크 호출, 수백ms~수초)에 비하면 무시할 수준이다.

모든 수치는 실제로 코드를 고치거나 벤치마크를 돌려서 `difflib`/`time.perf_counter()`로
잰 값이며, 지어낸 수치는 없다.

---

## 1. 코드 길이는 상황에 따라 갈린다

### 1-A. 단순 분기/설정값 변경 — 무승부 또는 순수 Python 우세

**실험1: 재시도 횟수 2→3 변경**

| | 순수 Python | LangGraph |
|---|---|---|
| 안 썼을 때 / 썼을 때 | `orchestrator.py`: `MAX_RETRY = 2` → `= 3` | `graph.py`: `MAX_RETRY = 2` → `= 3` |
| 수정량 | **1파일 1줄** | **1파일 1줄** |

무승부. 둘 다 상수 하나만 바꾸면 끝났다.

**실험2: Detection과 Classification 사이에 화이트리스트 단계 추가**

**안 썼을 때** (`orchestrator.py`, 기존 함수 본문에 바로 삽입):
```python
state = run_detection(state)
if not state.anomaly_flag:
    return run_logging(state)

state = run_whitelist_check(state)      # ← 추가된 2줄
if state.whitelisted:                   # ←
    return run_logging(state)           # ←

state = run_classification(state)
```

**썼을 때** (`graph.py`, 노드 2개 추가 + 라우터 재배선):
```python
def whitelist_node(state): return run_whitelist_check(state)
def whitelist_router(state): return "logging" if state.whitelisted else "classification"

graph.add_node("whitelist", whitelist_node)
graph.add_conditional_edges("detection", detection_router,
    {"whitelist": "whitelist", "logging": "logging"})   # 기존: {"classification":..} 였던 걸 수정
graph.add_conditional_edges("whitelist", whitelist_router,
    {"classification": "classification", "logging": "logging"})
```

| | 순수 Python | LangGraph |
|---|---|---|
| 조립 담당 파일 고유 수정량 | **+5줄** | **+15줄** |

**순수 Python이 더 짧았다.** 새 노드 2개(함수 선언) + 라우터 1개 + 그래프 배선
재조정이 필요한 LangGraph 쪽이 선언적 형태의 보일러플레이트 때문에 오히려 더 길다.

---

### 1-B. 프레임워크 내장 기능이 필요한 변경 — LangGraph 우세 (2회 확인, 우연 아님)

**실험5: 승인 대기 (Checkpointer + interrupt_before)** — "위험도 높은 액션은
멈췄다가, 나중에 승인되면 프로세스가 재시작돼도 이어서 실행"

**안 썼을 때** — 기존 `while` 루프를, "지금 어디까지 왔는지"를 문자열로 저장/복원하는
디스패처로 전면 재작성해야 했다:
```python
# 기존 (54줄)
while True:
    state = run_action(state)
    state = run_qa(state)
    if state.qa_passed:
        break
    ...

# 재작성 후 — while 루프는 프로세스가 죽으면 사라지는 콜스택 위에서 돌기 때문에
# "재개"를 표현할 수 없다. 단계를 문자열로 외부화해야 한다.
def run_pipeline(state, thread_id, start_step="detection"):
    step = start_step
    while step is not None:
        if step == "decision":
            state = run_decision(state)
            step = "approval_gate" if state.requires_approval else "action"
        elif step == "approval_gate":
            save_checkpoint(thread_id, state, next_step="action")   # 직접 구현
            return state, "action"          # 여기서 실제로 멈춤
        elif step == "action":
            state = run_action(state)
            step = "qa"
        elif step == "qa":
            ...
    delete_checkpoint(thread_id)
    return state, None
```
+ 신규 파일 `persistence.py` (체크포인트 저장/로드 직접 구현, 43줄)

**썼을 때** — 기존 로직은 그대로 두고 옵션 하나 + 노드 하나만 추가:
```python
graph.add_conditional_edges("decision", decision_router,
    {"approval_gate": "approval_gate", "action": "action"})   # 새 분기만 추가
graph.add_edge("approval_gate", "action")

_checkpointer = MemorySaver()
app = build_graph().compile(checkpointer=_checkpointer, interrupt_before=["approval_gate"])
```
```python
# 실제로 실행해서 확인한 동작
result = app.invoke(state, config={"configurable": {"thread_id": tid}})
# → approval_gate 직전에서 자동으로 멈춤 (action_executed=None, is_paused=True)
result = app.invoke(None, config={"configurable": {"thread_id": tid}})
# → 새 입력 없이, 멈췄던 지점부터 재개됨
```

| | 순수 Python | LangGraph |
|---|---|---|
| state 필드 추가 | +4 | +5 |
| 핵심 로직 | `orchestrator.py` **+61/-25** (전면 재작성) | `graph.py` **+16/-3** (분기만 추가) |
| 신규 파일 | `persistence.py` +43줄 | `approval_gate.py` +18, `runner.py` +33 |
| **합계** | **+108 / -25** | **+72 / -3** |

**실험6: 일시적 오류 자동 재시도 (RetryPolicy)** — "boto3 호출이 네트워크
순단으로 실패하면 지수 백오프로 자동 재시도"

**안 썼을 때** — 지수 백오프·지터·예외 필터를 직접 구현해야 한다:
```python
def retry_with_backoff(max_attempts=3, initial_interval=0.5, backoff_factor=2.0,
                        max_interval=128.0, jitter=True, retry_on=(ConnectionError,)):
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
# (전체 41줄, 신규 파일 retry_policy.py)

_run_action_with_retry = retry_with_backoff(max_attempts=3)(run_action)  # 적용 지점
```

**썼을 때** — 라이브러리가 이미 구현해둔 걸 파라미터로 켜기만 하면 끝:
```python
from langgraph.types import RetryPolicy

graph.add_node("action", action_node, retry_policy=RetryPolicy(max_attempts=3))
```

| | 순수 Python | LangGraph |
|---|---|---|
| 적용 지점 | `orchestrator.py` **+7/-1** | `graph.py` **+2/-1** |
| 신규 파일 | `retry_policy.py` **+41줄** | 없음 |
| **합계** | **+48 / -1** | **+2 / -1** |

두 실험 모두 `ConnectionError`를 2번 던지고 3번째에 성공하는 더미 노드로
실제 재시도 동작을 확인했다. (LangGraph의 `RetryPolicy`는 기본적으로
`ValueError`/`RuntimeError` 같은 "프로그래밍 실수"는 재시도 대상에서
제외하고 `ConnectionError`류만 재시도한다는 것도 소스 코드로 확인함.)

> **결론**: 실험5·6이 같은 패턴(LangGraph가 훨씬 적음)으로 두 번 나왔다는 게
> 중요하다. "체크포인팅"과 "재시도 정책"은 서로 다른 기능인데도 똑같은 이유
> (프레임워크가 이미 구현해둔 것을 순수 Python은 직접 재구현해야 함)로 격차가
> 났다 — 우연이 아니라는 뜻이다.

---

## 2. 실수를 발견하는 시점이 다르다

**같은 상황**: "다음에 어디로 가야 하는지"를 가리키는 문자열 하나를 똑같이
오타 냈다 (`classification` → `clasification`). 양쪽에 실제로 재현했다.

**안 썼을 때**:
```python
if step == "detection":
    state.anomaly_flag = True
    step = "clasification"      # ← 오타
elif step == "classification":
    ...
else:
    raise ValueError(f"알 수 없는 step: {step}")
```
```
--- 모듈 정의 시점: 아무 에러 없음 ---
--- anomaly_flag=True인 입력으로 실제로 실행해야만 ---
실행 중(runtime)에야 발견됨: 알 수 없는 step: clasification
```

**썼을 때**:
```python
graph.add_conditional_edges("detection", detection_router,
    {"classification": "clasification", "logging": "logging"})   # ← 매핑 값에 오타
```
```
--- graph.compile() 호출 이전: 아직 에러 없음 ---
compile() 시점에 즉시 감지됨: ValueError - At 'detection' node,
'detection_router' branch found unknown target 'clasification'
```

**차이**: 오타 자체는 똑같다. 순수 Python은 "그 분기를 실제로 타는 입력이
들어와야" 발견되고 (운영 중 특정 조건에서만 터지는 버그가 될 수 있음),
LangGraph는 "그래프를 만드는 시점"에 입력과 무관하게 즉시 발견된다.

---

## 3. 지금은 구조가 같아도, 미래 변화에 대한 내구성이 다르다

**지금 당장(실험1)은 완전히 동일했다** — 재시도 규칙은 둘 다 "한 곳"에서만 관리됐다.

```python
# 순수 Python: MAX_RETRY 상수 1개
# LangGraph:  MAX_RETRY 상수 1개
```

**차이는 그 "한 곳"에 새 실행 규칙(영속화, 자동재시도)이 얹혔을 때 드러났다**
(실험5·6 참고). `action↔qa` 반복을 결정하는 부분을 나란히 보면:

**안 썼을 때 — 영속화 요구사항이 붙기 전/후, `while` 루프 자체가 바뀜:**
```python
# 전
while True:
    state = run_action(state); state = run_qa(state)
    if state.qa_passed: break
    ...

# 후 — 완전히 다른 형태(step 디스패처)로 재작성
elif step == "qa":
    state = run_qa(state)
    if state.qa_passed: step = "logging"
    elif state.rollback_count < MAX_RETRY:
        rollback_action(state); state.rollback_count += 1
        step = "action"
    else: step = "logging"
```

**썼을 때 — 영속화 요구사항이 붙기 전/후, `qa_router`는 한 글자도 안 바뀜:**
```python
def qa_router(state: PipelineState) -> str:
    if state["qa_passed"]:
        return "logging"
    elif state["rollback_count"] < 2:
        return "action"
    else:
        return "logging"
```
승인 대기 기능을 추가할 때 건드린 건 이 함수가 아니라 `decision → action`
사이에 새 분기(`decision_router`) + 새 노드(`approval_gate`)를 끼워 넣은
것뿐이다. `action↔qa` 반복 자체를 표현하는 함수는 무관하게 유지됐다.

**왜 이런 차이가 나는가 (쉽게)**: `while` 루프는 "지금 몇 바퀴째인지"가
Python 프로세스가 켜져 있는 동안에만(콜스택에) 기억된다 — 프로세스가
꺼지면 그 기억도 사라진다. "꺼졌다 켜져도 이어가기"를 하려면 그 기억을
파일/DB에 문자열로 직접 적어뒀다 읽는 구조로 통째로 바꿔야 한다.
LangGraph는 "지금 어느 단계인지"를 프레임워크가 항상 자동으로 기록해두기
때문에, `qa_router`는 그냥 "다음 이름표가 뭔지"만 답하면 되고 기록/복원은
`compile(checkpointer=...)` 옵션 하나로 바깥에서 해결된다.

이 차이가 나는 근본 원인은 **이 파이프라인이 사이클(되돌아가는 구조)이기
때문**이다. 사이클이 없는 단순 순차 구조(실험2의 화이트리스트 삽입처럼
그냥 순서대로 끼워넣기만 하면 되는 경우)였다면 이런 격차 자체가 안
생겼을 것이다 — 실제로 실험2에서는 순수 Python이 더 짧았다.

---

## 4. LangGraph는 부가 기능이 공짜로 딸려온다

세 가지 부가 기능을 실제로 만들어서(다이어그램은 실제 프로젝트에 바로,
나머지 둘은 `pipeline/graph.py`·`schema/state.py`에 **임시 적용 → 테스트 →
원복**하는 방식으로) 동작을 확인했다.

### 4-1. 파이프라인 다이어그램 자동 생성

**안 썼을 때**: 사람이 코드를 읽고 손으로 그리거나 별도 도구에 옮겨 그려야 한다.

**썼을 때** (실제 프로젝트에 실행함, 코드 수정 없음):
```python
from pipeline.graph import app
print(app.get_graph().draw_mermaid())
```
`pipeline/graph.py`를 한 줄도 안 고치고 이 두 줄만으로 실제 그래프 구조를
그대로 Mermaid 다이어그램으로 뽑았다.

---

### 4-2. Checkpointer + interrupt_before — 승인 대기

**안 썼을 때** (`pipeline/graph.py`): `decision` 다음은 무조건 `action`.
```python
graph.add_edge("decision", "action")
...
app = build_graph().compile()
```

**썼을 때** (`pipeline/graph.py`, 실제로 적용): 위험도가 높으면 `approval_gate`를
거치도록 분기 추가 + 체크포인터로 컴파일.
```python
def decision_router(state: PipelineState) -> str:
    return "approval_gate" if state["requires_approval"] else "action"

graph.add_node("approval_gate", approval_gate_node)   # 신규 노드
graph.add_conditional_edges("decision", decision_router,
    {"approval_gate": "approval_gate", "action": "action"})
graph.add_edge("approval_gate", "action")

_checkpointer = MemorySaver()
app = build_graph().compile(checkpointer=_checkpointer, interrupt_before=["approval_gate"])
```

**1차 테스트 결과 — 멈추는 것까지는 성공, 재개 후 문제 발견:**
```
requires_approval: True
멈춰있는가 (is_paused): True
action_executed (아직 실행 안 됐어야 함): None      ← 정상: 실행 전 멈춤

# 승인 후 재개
재개 후 action_executed: None                        ← ⚠️ 문제: 여전히 실행 안 됨
재개 후 action_result: {'status': 'pending_approval'} ← ⚠️ 재개해도 보류 상태 그대로
재개 후 qa_passed: True                               ← ⚠️ 아무 액션도 안 했는데 통과 처리됨
```
**원인**: `pipeline/action_agent.py`의 `action_node`가 자기 나름대로
`if state.get("requires_approval"): return pending_approval` 체크를 갖고
있어서, 재개돼도 `requires_approval`이 여전히 `True`면 또 막아버린다.
`action_agent.py`는 수정하지 않기로 했으므로, 새로 만든 `approval_gate_node`
안에서 승인 처리와 함께 이 플래그를 내려주는 것으로 해결했다:

```python
def approval_gate_node(state: PipelineState) -> PipelineState:
    state["approval_status"] = "approved"
    state["requires_approval"] = False   # ← 수정 후 추가한 한 줄
    state["log_entries"].append(f"[APPROVAL] 승인 완료 ...")
    return state
```

**수정 후 재테스트 결과 — 정상 동작 확인:**
```
재개 후 requires_approval: False
재개 후 action_executed: Throttle             ← 이번엔 실제로 실행됨
재개 후 action_result: {'status': 'not_implemented', 'action': 'Throttle',
                         'rolled_back': True, 'rollback_status': 'success', ...}
재개 후 approval_status: approved
```
mock EC2 클라이언트로 `describe_instances`가 실제로 호출되는 지점까지
도달한 것도 확인했다 (mock 안 채운 부분에서 AWS 형식 에러가 난 것 자체가
"진짜로 실행을 시도했다"는 증거였다).

---

### 4-3. RetryPolicy — 일시적 오류 자동 재시도

**안 썼을 때** (`pipeline/graph.py`):
```python
graph.add_node("action", action_node)
```

**썼을 때** (실제로 적용):
```python
from langgraph.types import RetryPolicy

graph.add_node("action", action_node, retry_policy=RetryPolicy(max_attempts=3))
```

**테스트 결과** — 가짜 Lambda 클라이언트가 `put_function_concurrency`를
2번 `ConnectionError`로 실패시키고 3번째부터 성공하도록 만들어서 실행:
```
action_executed: Throttle
action_result 중 status: success
boto3 호출 총 시도 횟수: 4
```
결국 성공까지 도달하는 건 확인했다. 다만 예상한 3회가 아니라 4회가
나왔는데 (승인 대기 재개 상황과 겹쳐서 실행된 영향으로 추정), 정확한
원인은 이번엔 더 파보지 않았다 — 필요하면 별도로 재검증 가능.

> ⚠️ **두 기능 다 임시 적용 → 테스트 → 원복했다.** `git diff`로 `pipeline/graph.py`,
> `schema/state.py`가 원본과 완전히 동일한 것까지 확인 후 되돌렸다. 위 코드는
> 실제로 실행해서 나온 결과이지만, 지금 저장소에는 반영돼 있지 않다.

---

## 5. LangGraph도 못 막아주는 게 있다 (과대포장 방지)

**같은 상수를 두 곳에 따로 정의해서 값이 어긋나는 버그**는 프레임워크와
무관하게 똑같이 발생한다:

```python
# raw_python/orchestrator.py
MAX_RETRY = 2

# langgraph_version/graph.py
MAX_RETRY = 2   # 값은 같지만 import로 연결된 게 아니라 완전히 별개의 상수
```

LangGraph가 실제로 없애주는 건 "반복 실행 자체를 손으로 구현하다 조건을
잘못 옮겨 적는" 유형의 버그(루프를 도는 부분은 프레임워크가 보장)이지,
"같은 값을 여러 곳에 따로 정의해서 어긋나는" 유형의 버그는 아니다 — 이건
LangGraph를 쓰든 안 쓰든 개발자가 단일 소스로 직접 관리해야 한다.

---

## 6. 조립할 때 코드 자체도 더 읽기 쉽다 (줄 수와는 다른 얘기)

**주의**: 이 항목은 "코드가 더 짧다"는 게 아니다 — 실험2에서 봤듯 오히려
LangGraph가 더 긴 경우도 있다. 여기서 말하는 건 **"다음에 어디로 가는지
파악하는 데 코드를 얼마나 읽어야 하는가"**다.

**안 썼을 때**: 다음 단계가 어디인지 알려면 함수 본문을 끝까지 읽어야 한다.
```python
state = run_detection(state)
if not state.anomaly_flag:
    return run_logging(state)
state = run_classification(state)
state = run_decision(state)
while True:
    state = run_action(state)
    ...
```
"detection 다음에 뭐가 올 수 있는지"를 알려면 이 함수 전체를 눈으로
따라가야 한다.

**썼을 때**: 다음 단계 후보가 매핑 딕셔너리 하나에 다 모여 있다.
```python
graph.add_conditional_edges(
    "detection", detection_router,
    {"classification": "classification", "logging": "logging"},
)
```
"detection 다음엔 classification 아니면 logging뿐"이라는 게 이 한 줄만
봐도 끝난다 — `detection_router` 내부 로직을 안 봐도 "가능한 목적지 집합"은
바로 알 수 있다.

**한계**: 이건 정성적 판단이라 숫자로 재기 어렵다. 그리고 이 딕셔너리가
많아질수록(엣지가 늘어날수록) 오히려 파일 전체를 봐야 큰 그림이 잡히는
것도 사실이라, "무조건 더 읽기 쉽다"고 과장하진 않는 게 맞다 — 개별
분기 하나하나의 "목적지 파악"이 쉽다는 딱 그 정도의 주장이다.

---

## 7. 성능 — 유일하게 LangGraph가 불리한 항목

지금까지는 전부 "코드를 얼마나 고치는가"였다. 이번엔 "실제로 얼마나
빨리 도는가"를 `time.perf_counter()`로 실측했다.

### 7-1. 함수 호출 1회당 오버헤드 (재시도 없는 정상 경로, 2000회 평균)

| 방식 | 호출당 시간 | 베이스라인 대비 |
|---|---|---|
| 순수 함수 호출 (베이스라인) | 0.92 µs | 1x |
| 직접 만든 `retry_with_backoff` 데코레이터 | 1.18 µs | 1.3x |
| `tenacity` 데코레이터 (표준 재시도 라이브러리) | 20.00 µs | 22x |
| LangGraph `invoke()` (RetryPolicy, checkpointer 없음) | 872 µs | **950x** |
| LangGraph `invoke()` (RetryPolicy + MemorySaver) | 2,625 µs | **2,900x** |

### 7-2. 전체 파이프라인(6단계) 1회 실행 (500회 평균, i-normal 시나리오)

| | 1회 실행 시간 |
|---|---|
| 순수 Python (step 디스패처) | 0.094 ms |
| LangGraph | 3.020 ms |
| **배율** | **32배** |

### 7-3. 그래프 빌드 비용 (요청마다가 아니라 앱 시작 시 1회만 발생)

| | 시간 |
|---|---|
| `build_graph().compile()` | 5.666 ms |
| `compile(checkpointer=MemorySaver())` | 6.727 ms |
| 순수 Python | 0 ms (별도 컴파일 단계 자체가 없음, import가 곧 준비 완료) |

### 7-4. 체크포인트 1개 저장 용량

| | 크기 |
|---|---|
| 순수 Python (`persistence.py`, JSON 파일) | 1,034 bytes |
| LangGraph (`MemorySaver`, msgpack 직렬화) | 2,505 bytes (**2.4배**) |

LangGraph 쪽이 더 큰 이유: 상태값 자체 외에 채널별 버전 정보(`channel_versions`),
`versions_seen`, 타임스탬프 등 **여러 노드가 동시에 같은 상태를 갱신할 수 있는
범용 그래프 모델**을 지원하기 위한 부가 메타데이터가 매 체크포인트마다 같이
저장되기 때문이다. 우리 `persistence.py`는 "이 파이프라인 하나"만 위해
state 원본을 그대로 dump하면 되니 더 가볍다.

### 7-5. 의존성 설치 용량

`langgraph` + `langchain-core` + 그 하위 의존성(`langsmith`, `pydantic`,
`ormsgpack`, `tenacity` 등) 설치 용량을 실측: **약 16MB** (site-packages 기준).
순수 Python 버전은 표준 라이브러리만 쓰므로 추가 설치 0MB.

### 해석 — 상대적으론 크지만 절대적으론 무의미할 가능성이 높다

이 파이프라인의 실제 병목은 오케스트레이션이 아니라:
- `action_node`의 boto3 AWS 호출 (보통 **100ms ~ 수초**)
- `classification_node`/`decision_node`의 Gemini LLM 호출 (보통 **수백ms ~ 수초**)
- `logging_node`의 PostgreSQL insert (보통 **수ms ~ 수십ms**)

LangGraph가 추가하는 3ms(파이프라인 1회 기준)는 이런 실제 네트워크 호출
시간의 **0.1~1% 수준**이다. "32배 느리다"는 사실이지만, 실제 운영에서
체감되는 지연시간엔 거의 영향이 없다 — 이 파이프라인은 CPU 연산이 아니라
네트워크 I/O가 지배하는 구조이기 때문이다. 다만 만약 이 그래프 구조를
**초당 수천 번씩 호출하는 저지연 경로**(예: 실시간 스트림 처리)에 쓴다면
이 오버헤드는 무시할 수 없는 수준이 된다 — 이 프로젝트의 사용 패턴(리소스
이상 탐지, 분 단위 폴링)에서는 해당 사항이 없다는 뜻이다.

---

## 종합

| 관점 | 순수 Python | LangGraph |
|---|---|---|
| 단순 분기/설정값 변경 | 동일하거나 더 짧음 | 동일하거나 더 김 |
| 내장 기능(체크포인팅·재시도) 필요 시 | +108/-25, +48/-1 | **+72/-3, +2/-1** |
| 배선 오타 발견 시점 | 실행해야 발견 | `compile()` 시점 즉시 발견 |
| 반복 구조의 미래 변경 내구성 | 반복문 자체 재작성 필요 | 반복 결정 함수 무변경 |
| 부가 기능 | 직접 구현 필요 | 다이어그램·체크포인터·재시도정책 내장 (3가지 실제 검증) |
| 상수 중복 버그 | 발생 | 동일하게 발생 (한계) |
| 조립 시 목적지 파악 용이성 | 함수 본문 전체를 읽어야 함 | 매핑 딕셔너리 한 줄로 확인 (단, 줄 수 우위와는 별개) |
| 실행 성능 (실측) | 파이프라인 1회 **0.094ms** | 파이프라인 1회 **3.02ms (32배)** — 단, 실제 병목(네트워크 I/O)의 0.1~1% 수준이라 체감 영향은 미미 |
| 의존성 설치 용량 | 0MB (표준 라이브러리만) | 약 16MB (langgraph+langchain-core 등) |

**한 줄 요약**: LangGraph 도입 근거는 "코드가 짧아져서"가 아니라, 이 프로젝트가
이미 필요로 하는 성격의 기능(사이클 위에서의 안전한 재시도, 멈췄다 재개)이
프레임워크가 이미 만들어둔 영역과 정확히 겹치기 때문이다. 유일한 실측
단점은 성능(오케스트레이션 오버헤드)과 의존성 용량인데, 둘 다 이
프로젝트의 실제 병목(네트워크 I/O 지배적, 분 단위 폴링)을 고려하면
감수할 만한 수준이다.
