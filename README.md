# 클라우드 비용 이상 징후 탐지 LangGrpah

AWS 클라우드 비용 이상 징후를 탐지하고 자동으로 대응하는 LangGraph 기반 Multi-Agent 파이프라인

## 프로젝트 구조

```
langgraph_study/
├── pipeline/                    # 메인 파이프라인 코드
│   ├── graph.py                 # LangGraph 그래프 조립 및 라우팅
│   ├── detection_agent.py       # Step 1: 이상 탐지 Agent
│   ├── classification_agent.py  # Step 2: 이상 분류 Agent
│   ├── decision_agent.py        # Step 3: 액션 결정 Agent
│   ├── action_agent.py          # Step 4: 액션 실행 Agent
│   ├── QA_agent.py              # Step 5: SLA 검증 Agent
│   ├── logging_agent.py         # Step 6: Audit Log Agent
│   └── dummy_nodes.py           # 테스트용 더미 노드
│
├── schema/
│   └── state.py                 # 파이프라인 공유 State 스키마
├── models/
│   └── iforest_EC2.pkl          # Isolation Forest 학습 모델
├── playground/                  # 테스트 스크립트
│   ├── test_qa.py
│   ├── test_classification.py
│   ├── test_detection_logging_agents.py
│   ├── test_aws_connection.py
│   ├── test_decision_agent.py
│
│── run_dummy_pipeline.py
└── node_contracts.md            # 노드 계약서 (입출력 명세)
```

## 파이프라인 흐름

```
┌─────────────┐
│  Detection  │ ─── anomaly_flag=False ───────────────────┐
│   Agent     │                                           │
└─────┬───────┘                                           │
      │ anomaly_flag=True                                 │
      ▼                                                   │
┌─────────────────┐                                       │
│ Classification  │                                       │
│     Agent       │                                       │
└─────┬───────────┘                                       │
      │                                                   │
      ▼                                                   │
┌─────────────┐                                           │
│  Decision   │                                           │
│   Agent     │                                           │
└─────┬───────┘                                           │
      │                                                   │
      ▼                                                   │
┌─────────────┐     qa_passed=False                       │
│   Action    │ ◄───── (rollback_count < 2) ──────┐       │
│   Agent     │                                   │       │
└─────┬───────┘                                   │       │
      │                                           │       │
      ▼                                           │       │
┌─────────────┐                                   │       │
│     QA      │ ──────────────────────────────────┘       │
│   Agent     │                                           │
└─────┬───────┘                                           │
      │ qa_passed=True OR rollback_count >= 2             │
      ▼                                                   │
┌─────────────┐ ◄─────────────────────────────────────────┘
│  Logging    │
│   Agent     │
└─────────────┘
      │
      ▼
     END
```

## Agent 상세 설명

### Step 1: Detection Agent (`detection_agent.py`)
이상 탐지를 수행합니다.

**알고리즘:**
- **Z-score 탐지**: 비용, 네트워크 입력, 호출 횟수 지표에 적용 (k=3.0 임계값)
- **Isolation Forest 탐지**: 모든 지표를 다변량으로 분석 (τ=0.6 임계값)
- **OR 앙상블**: 둘 중 하나라도 트리거되면 이상으로 판정

**출력 필드:**
- `anomaly_flag`: 이상 여부
- `anomaly_score_zscore`: Z-score 최댓값
- `anomaly_score_iforest`: Isolation Forest 점수 (0~1)
- `triggered_metrics`: 트리거된 지표 목록

---

### Step 2: Classification Agent (`classification_agent.py`)
탐지된 이상 신호의 유형을 분류합니다.

**분류 유형:**
| 유형 | 설명 | 긴급도 |
|------|------|--------|
| `cost_inefficiency` | 좀비 리소스, 오버프로비저닝 | 낮음 |
| `cost_spike` | 트래픽/호출 폭증 | 높음 |
| `risk_security` | EDoS, DDoS, 비정상 접근 | 매우 높음 |

**처리 전략:**
1. **Rule-based**: 명확한 케이스는 규칙으로 즉시 분류
2. **LLM (Gemini)**: 모호한 케이스는 LLM에 위임

**출력 필드:**
- `anomaly_type`: 분류된 이상 유형
- `classification_reasoning`: 판단 근거
- `interim_action_taken`: 즉시 취한 임시 조치

---

### Step 3: Decision Agent (`nodes/decision_agent.py`)
대응 액션을 결정합니다.

**허용 액션 (anomaly_type별):**
| anomaly_type | 허용 액션 |
|--------------|-----------|
| `cost_inefficiency` | NoAction, Stop, Stop+Schedule, Resize |
| `cost_spike` | NoAction, Throttle, Block, ScaleDown |
| `risk_security` | NoAction, Block, ScaleDown |

**점수 계산:**
```
score = 0.5 × saving_rate - 0.3 × impact_score + 0.2 × stability_score
```

**saving_rate 산정 방식:** `raw_metrics["cost"]` 시계열에서 결정론적으로 계산하는 것이
기본이다 (LLM 추정은 cost 데이터가 부족할 때만 예외적으로 사용).
- `Stop`: 평균 비용의 100% 제거로 간주
- `Stop+Schedule`: 평균 비용의 50% 제거로 간주 (듀티사이클 가정)
- `Resize`: EC2 온디맨드 단가표에서 현재 비용과 가장 가까운 tier를 역추정 후, 한 단계
  낮은 tier와의 단가 차이로 계산
- `Throttle` / `ScaleDown`: cost 윈도우의 기준선 대비 최근 급증분을 절감 가능액으로 계산
- `Block`: 보안 조치이므로 saving_rate=0.0으로 고정

각 후보 액션에는 `estimated_saving_usd`(시간당 USD 절감 예상액)도 함께 기록된다.

**위험도 결정:**
- 기본값: `cost_inefficiency`→LOW, `cost_spike`→MED, `risk_security`→HIGH
- 액션별 상향: `Stop+Schedule`, `Resize`→최소 MED, `Block`→최소 HIGH

**출력 필드:**
- `candidate_actions`: 후보 액션 목록 (점수 포함)
- `selected_action`: 선택된 액션
- `risk_level`: LOW / MED / HIGH
- `requires_approval`: MED/HIGH인 경우 True

---

### Step 4: Action Agent (`nodes/action_agent.py`)
선택된 액션을 실제로 실행합니다.

**지원 리소스/액션:**
- EC2: Stop, Resize
- Lambda, S3, RDS, AutoScaling: 추후 확장 예정

**처리 흐름:**
1. `requires_approval=True` → 액션 보류 (pending_approval)
2. 액션 실행 전 스냅샷 저장 (롤백용)
3. 실제 액션 실행 (boto3)

**출력 필드:**
- `pre_action_snapshot`: 액션 전 상태 스냅샷
- `action_executed`: 실행된 액션명
- `action_result`: 실행 결과

---

### Step 5: QA Agent (`QA_agent.py`)
액션 수행 후 SLA 준수 여부를 검증합니다.

**검증 항목:**
| 항목 | 기준 |
|------|------|
| CPU SLA | 사용률 80% 이하 |
| 비용 SLA | 이전 대비 10% 이상 증가 없음 |
| 가용성 SLA | 액션 성공 완료 |

**분기 처리:**
- 검증 통과 → `qa_passed=True`, logging으로 이동
- 검증 실패 + `rollback_count < 2` → action으로 재시도
- 검증 실패 + `rollback_count >= 2` → 관리자 알림 후 logging으로 이동

**출력 필드:**
- `qa_passed`: 검증 통과 여부
- `sla_check_result`: 개별 SLA 검증 결과
- `rollback_count`: 롤백 시도 횟수

---

### Step 6: Logging Agent (`logging_agent.py`)
전체 파이프라인 실행 과정을 PostgreSQL Audit Log로 기록합니다.

**테이블 구조:**
| 테이블명 | 설명 |
|----------|------|
| `agent_runs` | 파이프라인 실행 1회 = 1행 |
| `agent_steps` | 각 단계별 실행 기록 |
| `action_log` | 액션 실행 상세 기록 (전/후 스냅샷) |

---

## State 스키마

`schema/state.py`에 정의된 `PipelineState` TypedDict를 모든 Agent가 공유합니다.

**리소스별 Metrics 구조:**
- `EC2Metrics`: cpu_utilization, network_in, network_out, cost
- `LambdaMetrics`: invocation_count, error_count, duration_avg, cost
- `S3Metrics`: number_of_requests, bytes_downloaded, cost
- `RDSMetrics`: cpu_utilization, database_connections, read_iops, write_iops, cost
- `AutoScalingMetrics`: group_desired_capacity, group_in_service_instances, cost

---

## 시작하기 (브랜치 Pull 후 테스트하기)

다른 브랜치를 pull 받아서 로컬에서 테스트해볼 때 순서입니다.

### 1. 저장소 clone (처음이라면)
```bash
git clone https://github.com/PNU-Detection/langgraph_study.git
cd langgraph_study
```

### 2. 브랜치 받기
```bash
git fetch origin
git checkout <브랜치명>   # 예: fix/decision-and-action-agent
```

### 3. 가상환경 생성 및 활성화
```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Mac/Linux
source venv/bin/activate
```

### 4. 의존성 설치
```bash
pip install -r requirements.txt
```

### 5. `.env` 파일 생성
`.env`는 `.gitignore`에 포함되어 git으로 공유되지 않으므로 직접 생성해야 합니다.
(AWS 키 등 민감 정보이므로 파일 자체를 공유하지 말고 팀 채널로 값만 전달할 것)

```bash
# LLM (Classification, Decision, QA Agent)
GEMINI_API_KEY=your_gemini_api_key

# AWS (Action Agent)
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=ap-northeast-2

# 테스트 대상 리소스 (playground/test_scenarios.py에서 사용)
INSTANCE_ID=i-0abc123def456
LAMBDA_FUNCTION_NAME=detection-test-lambda
ASG_NAME=detection-test-asg

# PostgreSQL (Logging Agent)
PGHOST=localhost
PGPORT=5432
PGDATABASE=cloud_anomaly_agent
PGUSER=postgres
PGPASSWORD=your_password
```

### 6. 시나리오 테스트 실행
```bash
python playground/test_scenarios.py
```

⚠️ **주의**
- 시나리오 1(좀비 리소스)은 `INSTANCE_ID`로 지정한 EC2 인스턴스를 **실제로 Stop시켰다가
  QA 실패 시 다시 Start**시킵니다. 실제 운영 중인 인스턴스가 아닌 테스트용 인스턴스로
  실행하세요.
- Gemini API 무료 티어는 하루 요청 20회 제한이 있어, 반복 실행 시 `429 RESOURCE_EXHAUSTED`
  로 rate limit에 걸릴 수 있습니다.
- DB 저장(Logging Agent)까지 확인하려면 로컬에 PostgreSQL이 실행 중이어야 하고,
  `cloud_anomaly_agent` 데이터베이스가 미리 생성되어 있어야 합니다.

---

## 실행 방법

```python
from pipeline.graph import app

# 초기 State 구성
initial_state = {
    "resource_id": "i-0abc123def456",
    "resource_type": "EC2",
    "raw_metrics": {
        "cpu_utilization": [10.0, 15.0, 12.0, ...],  # 30개 포인트
        "network_in": [...],
        "network_out": [...],
        "cost": [...],
    },
    "timestamp": "2024-01-01T00:00:00Z",
    "rollback_count": 0,
    "log_entries": [],
}

# 파이프라인 실행
result = app.invoke(initial_state)
print(result)
```

---

## 테스트

```bash
# 3가지 시나리오(좀비 리소스/Lambda 폭증/EDoS 의심) 전체 파이프라인 실행
python playground/test_scenarios.py

# QA Agent 테스트
python playground/test_qa.py

# Classification Agent 테스트
python playground/test_classification.py

# Detection + Logging Agent 테스트
python playground/test_detection_logging_agents.py

# 더미 파이프라인 실행
python playground/run_dummy_pipeline.py
```
