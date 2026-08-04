# 노드별 입력/출력 명세 (Node Contracts)

> 각 노드가 State에서 **읽는 필드**와 **채워야 하는 필드**를 명시한다.
> 담당자는 자신의 노드가 읽는 필드가 이전 노드에서 반드시 채워졌는지 확인하고 구현한다.

---

## Step 0 — 파이프라인 진입 (외부 주입)

| 필드 | 타입 | 설명 |
|---|---|---|
| `resource_id` | `str` | 예: `"i-0abc1234"` |
| `resource_type` | `Literal` | `EC2 / Lambda / S3 / RDS / AutoScaling` |
| `raw_metrics` | `dict` | 리소스 타입별 지표 목록 (아래 참고) |
| `timestamp` | `str` | ISO 8601, 수집 기준 시각 |

**리소스 타입별 `raw_metrics` 지표 목록** (CloudWatch 지표명 기준):

| resource_type | 지표 키 |
|---|---|
| EC2 | `cpu_utilization`, `network_in`, `network_out`, `cost` |
| Lambda | `invocation_count`, `error_count`, `duration_avg`, `cost` |
| S3 | `number_of_requests`, `bytes_downloaded`, `cost` |
| RDS | `cpu_utilization`, `database_connections`, `read_iops`, `write_iops`, `cost` |
| AutoScaling | `group_desired_capacity`, `group_in_service_instances`, `cost` |

- CloudWatch 지표: 1분 단위, 슬라이딩 윈도우 30분 = 리스트 30개 포인트
- `cost`: Cost Explorer 1시간 단위

초기값으로 반드시 함께 주입해야 하는 필드:

```python
{
    "anomaly_flag": False,
    "triggered_metrics": [],
    "candidate_actions": [],
    "requires_approval": False,
    "rollback_count": 0,
    "log_entries": [],
}
```

---

## Step 1 — Detection Agent

- **담당**: 박소영
- **읽는 필드**: `raw_metrics`, `resource_type`
- **채워야 하는 필드**

| 필드 | 타입 | 조건 |
|---|---|---|
| `anomaly_flag` | `bool` | Z-score OR Isolation Forest 중 하나라도 이상이면 True |
| `anomaly_score_zscore` | `Optional[float]` | 슬라이딩 윈도우 내 최대 Z-score. 이상 없으면 None |
| `anomaly_score_iforest` | `Optional[float]` | IF 이상 점수 [0.0, 1.0]. 이상 없으면 None |
| `triggered_metrics` | `list[str]` | 이상 감지된 지표명 목록. 없으면 `[]` |

- **알고리즘 기준**
  - Z-score: 임계값 k=3.0 초과 시 이상. `cost`, `network_in`, `invocation_count` 지표에 적용
  - Isolation Forest: 이상 점수 τ=0.6 초과 시 이상. 리소스 타입별 전체 지표 벡터에 적용. 24시간 주기 재학습
- **다음 분기**: `anomaly_flag == False` → Logging Agent로 바로 이동 (파이프라인 조기 종료)

---

## Step 2 — Classification Agent

- **담당**: 강지원
- **읽는 필드**: `anomaly_flag`, `triggered_metrics`, `raw_metrics`, `resource_type`
- **채워야 하는 필드**

| 필드 | 타입 | 조건 |
|---|---|---|
| `anomaly_type` | `Optional[Literal]` | `cost_inefficiency / cost_spike / risk_security` |
| `classification_reasoning` | `Optional[str]` | rule 처리 시 rule명, LLM 처리 시 LLM 근거 |
| `interim_action_taken` | `Optional[str]` | 임시조치 실행 시 내용, 없으면 None |

- **처리 전략**: Rule-based 선처리 → 모호한 케이스만 LLM 위임
- **주의**: `anomaly_flag == False`인 경우 이 노드에 도달하지 않음

---

## Step 3 — Decision Agent

- **담당**: 허소영
- **읽는 필드**: `anomaly_type`, `resource_type`, `resource_id`, `classification_reasoning`
- **채워야 하는 필드**

| 필드 | 타입 | 조건 |
|---|---|---|
| `candidate_actions` | `list[CandidateAction]` | 허용 집합 내 액션만 생성 (아래 참고) |
| `selected_action` | `Optional[Literal]` | 최고 score 액션 |
| `risk_level` | `Optional[Literal]` | `LOW / MED / HIGH`. 2단계 룰 적용 (아래 참고) |
| `requires_approval` | `bool` | `risk_level`이 MED 또는 HIGH이면 True |
| `decision_reasoning` | `Optional[str]` | 최종 선택 근거 |

- **허용 액션 집합** (LLM이 이 외의 액션 반환 시 자동으로 NoAction 대체):

| anomaly_type | 허용 액션 |
|---|---|
| `cost_inefficiency` | `NoAction, Stop, Stop+Schedule, Resize` |
| `cost_spike` | `NoAction, Throttle, Block, ScaleDown` |
| `risk_security` | `NoAction, Block, ScaleDown` |

- **점수 공식**: `Score = 0.5 × SavingRate - 0.3 × ImpactScore + 0.2 × StabilityScore`

- **SavingRate 산정 방식 (2026-07 중간보고 시점 갱신)**: `raw_metrics["cost"]` 시계열로부터
  결정론적으로 계산하는 것을 기본으로 한다 (LLM 추정은 cost 데이터가 없을 때만 예외적으로 사용).
  - `Stop` → 현재 평균 비용의 100% 제거로 간주
  - `Stop+Schedule` → 현재 평균 비용의 50% 제거로 간주 (듀티사이클 가정치)
  - `Resize` → EC2 온디맨드 단가표에서 현재 비용과 가장 가까운 tier를 역추정하고, 한 단계
    아래 tier와의 단가 차이로 계산
  - `Throttle` / `ScaleDown` → cost 윈도우를 기준선/최근 급증 구간으로 나눠 초과분(급증분)을
    절감 가능액으로 계산
  - `Block` → 보안 조치이므로 saving_rate를 인위적으로 만들지 않고 0.0으로 고정
  - `CandidateAction`에는 `estimated_saving_usd`(시간당 USD 절감 예상액) 필드가 함께
    기록된다. LLM fallback으로 산정된 경우는 근거 금액이 없으므로 0.0.
  - 상세 계산 로직/한계는 `pipeline/decision_agent.py` 파일 상단 주석 참고.

- **`risk_level` 2단계 판단 룰**:

```
1단계: anomaly_type 기반 기본값
  cost_inefficiency → LOW
  cost_spike        → MED
  risk_security     → HIGH

2단계: selected_action으로 상향 조정 (하향 없음)
  Stop+Schedule, Resize → 최소 MED
  Block                 → 최소 HIGH
```

---

## Step 4 — Action Agent

- **담당**: 허소영
- **읽는 필드**: `selected_action`, `resource_id`, `resource_type`, `requires_approval`
- **채워야 하는 필드**

| 필드 | 타입 | 조건 |
|---|---|---|
| `pre_action_snapshot` | `Optional[dict]` | 액션 실행 **전** 반드시 저장. 구조는 아래 참고 |
| `action_executed` | `Optional[str]` | 실제 실행한 액션명 |
| `action_result` | `Optional[dict]` | boto3 응답 원문 |

- **`pre_action_snapshot` 리소스별 구조** (모두 dict로 저장, 문자열 금지):

| resource_type | 저장 키 |
|---|---|
| EC2 | `instance_type`, `state`, `security_group_ids` |
| Lambda | `reserved_concurrency` (설정 없으면 -1) |
| S3 | `bucket_policy` (dict), `public_access_block` (dict) |
| RDS | `instance_class`, `multi_az` |
| AutoScaling | `max_size`, `desired_capacity` |

> **S3 주의**: boto3 `get_bucket_policy()["Policy"]`는 JSON 문자열로 반환됨.
> 저장 시 반드시 `json.loads()` 후 dict로 변환할 것. 그래야 롤백 시 `json.dumps()` 한 번으로 복원 가능.

- **주의**: `requires_approval == True` → 알림 전송 후 60분 대기 로직 선행
- **주의**: `selected_action == "NoAction"` → 스냅샷 저장 생략, QA Agent로 바로 이동

---

## Step 5 — QA Agent

- **담당**: 강지원
- **읽는 필드**: `action_result`, `resource_id`, `resource_type`, `pre_action_snapshot`, `rollback_count`
- **채워야 하는 필드**

| 필드 | 타입 | 조건 |
|---|---|---|
| `qa_passed` | `Optional[bool]` | SLA 전 항목 통과 시 True |
| `sla_check_result` | `Optional[SlaCheckResult]` | 항목별 통과 여부 + 실패 사유 |
| `rollback_count` | `int` | 롤백 실행 시 +1 |

- **분기 로직**:
  - `qa_passed == True` → Logging Agent
  - `qa_passed == False AND rollback_count < 2` → 롤백 후 Action Agent 재시도
  - `qa_passed == False AND rollback_count >= 2` → 관리자 알림 후 Logging Agent

---

## Step 6 — Logging Agent

- **담당**: 박소영
- **읽는 필드**: State 전체
- **채워야 하는 필드**

| 필드 | 타입 | 조건 |
|---|---|---|
| `log_entries` | `list[str]` | 각 단계 요약 로그 append |

- **Side-effect**: PostgreSQL `agent_runs`, `agent_steps`, `action_log` 테이블에 INSERT
- **Side-effect**: Grafana 대시보드 갱신용 메트릭 기록

---

## 필드 의존 관계 요약

```
[외부 주입]
  resource_id, resource_type, raw_metrics, timestamp
      │
      ▼
[Detection Agent]
  → anomaly_flag, triggered_metrics, anomaly_score_zscore, anomaly_score_iforest
      │
      │ anomaly_flag == False → ──────────────────────────────┐
      │ anomaly_flag == True  ↓                               │
      │                                                       │
[Classification Agent]                                        │
  → anomaly_type, classification_reasoning, interim_action_taken
      │                                                       │
      ▼                                                       │
[Decision Agent]                                              │
  → candidate_actions, selected_action, risk_level,           │
    requires_approval, decision_reasoning                     │
      │                                                       │
      ▼                                                       │
[Action Agent]                                                │
  → pre_action_snapshot, action_executed, action_result       │
      │                                                       │
      ▼                                                       │
[QA Agent]                                                    │
  → qa_passed, sla_check_result, rollback_count               │
      │                                                       │
      │ qa_passed == False, rollback_count < 2                │
      └──────────────────────→ [Action Agent 재시도]          │
      │                                                       │
      ▼ (통과 or rollback 2회 초과)                           │
[Logging Agent] ←──────────────────────────────────────────── ┘
  → log_entries  +  PostgreSQL / Grafana side-effect
```
