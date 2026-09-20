# 클라우드 이상탐지 및 자율대응 파이프라인 — 최종보고서 (팀 확정 구조 v2)

> 📦 이전 구조(v1)는 삭제하지 않고 [`FINAL_REPORT_OUTLINE_v1_archive.md`](FINAL_REPORT_OUTLINE_v1_archive.md)에 그대로 보관해뒀습니다.
> 특히 v1에 있었지만 v2엔 없는 "LangGraph 기술배경 별도 서술"과 "멘토피드백 중앙집중식 색인표"가 나중에 필요하면 그 파일에서 바로 가져다 쓸 수 있어요.

> 표기 규칙: `📎 근거자료`(본문에 반드시 인용/첨부할 사실적 근거), `📊 시각화자료`(넣어야 할 표/차트/스크린샷),
> `🔖 각주·미주`(출처/용어설명 필요 지점), `📌 멘토 의견 반영`(그 절 끝에 3~4문장으로 짧게 삽입할 대응 문구),
> `[TODO: 담당자]`(아직 채울 사람/데이터).
>
> **작성 규칙 (선배 기수 보고서 2편 실물 확인 후 반영)**
> 1. 각주는 출처 표기가 아니라 **용어 설명용**. 전문용어 처음 등장 시 위첨자 번호 + 페이지 하단 풀이.
> 2. 출처 표기는 두 갈래: (a) 외부 통계/그래프는 **그림 캡션에 직접** "그림 N. 기관, 「제목」, 연도." (b) 본문 인용은 `[번호]` → 7장에 IEEE 스타일 목록.
> 3. **멘토 의견 반영은 별도 챕터가 아니라, 관련 절 끝에 붙이는 3~4문장짜리 소절**. 아래 4곳에 위치 지정해둠.
> 4. **2장(연구배경)과 3장(연구내용)은 절대 안 겹쳐야 함**: 2장 = "이 보고서를 읽기 전에 알아야 할 개념/용어 정의"만. 3장 = "우리가 그 개념으로 실제 뭘 만들었는지". 2장에 "우리 아키텍처/우리 선택"이 들어가면 안 됨.

---

## 1. 서론

### 1.1 연구 배경
- 클라우드 비용 이상의 유형: 좀비(유휴) 리소스, 오버프로비저닝, EDoS, Lambda 재시도 폭증, S3 대량다운로드
- 왜 지금 이 문제가 중요한가(클라우드 지출 증가 추세, FinOps 관심 증가)

📎 근거자료: 클라우드 낭비 비용 규모 통계(Flexera State of the Cloud 등 1~2개)
🔖 각주·미주: `[번호]`로 인용 → 7장에 정리

### 1.2 기존 문제점 (기존 AWS Cost Explorer/Budgets 등 한계)

#### 1.2.1 수동 모니터링 대응의 한계
- AWS Cost Explorer/Budgets는 "가시화·알림"까지만 하고, 실제 조치는 사람이 판단해서 수행해야 함
- 대응 지연 = 비용 누적, 24/7 인력 모니터링 비용

#### 1.2.2 기존 FinOps 도구들의 한계 (및 차별점)
- Compute Optimizer/Trusted Advisor(탐지·추천까지), Kubecost/CloudHealth(가시화까지), Auto Scaling(실행은 하나 "이상탐지" 관점 정책 없음) — 대부분 탐지/추천 또는 실행 중 하나에 머무름
- 본 프로젝트는 탐지→분류→의사결정→승인→조치→검증→롤백을 하나의 파이프라인으로 엮은 것이 차별점

📎 근거자료: `playground/FINOPS_TOOL_COMPARISON.md` 비교표
📊 시각화자료: 기존 도구 vs 본 프로젝트 비교표(탐지/추천/실행/검증/롤백 ○×)
🔖 각주·미주: 각 도구 첫 등장 시 `[번호]` → 7장에 공식 문서 URL

> 📌 **멘토 의견 반영 ①** (이 절 끝에 삽입): "산학협력 멘토 의견을 반영하여, 기존 FinOps 도구(Compute Optimizer, Trusted Advisor, Kubecost 등)와의 비교 분석을 추가하였다."

### 1.3 연구 목표
- [TODO: 팀] 제안서 원문 인용 + 달성 내용 매핑
- **난이도를 명시적으로 서술** (채점자가 암묵적으로 알아주지 않음): 멀티에이전트 오케스트레이션, 실제 AWS API 실시간 연동, 승인 게이트를 포함한 상태 분기, 조치 후 실측 재검증·자동 롤백까지 구현해야 하는 복잡도

### 1.4 기대 효과
- 대응 시간 단축(수동 대비 detection~action까지 평균 [TODO]초)
- EDoS 등 보안성 이상에 대한 자동 완화(WAF)
- 승인 게이트를 통한 완전자동화 리스크 통제

📊 시각화자료: 파이프라인 단계별 소요시간 막대그래프(4.2.4에서 만든 것 재사용)

---

## 2. 연구 배경

> ⚠️ 이 장은 **개념/용어 정의만** 쓴다. "우리가 무엇을 만들었는지"는 절대 쓰지 않는다(그건 3장). 아래 2.1.2("멀티에이전트 파이프라인")는 일반론만 — "LangGraph로 우리가 구성한 파이프라인"은 3.1.1에만 쓴다.

### 2.1 이상탐지 이론적 배경 / 클라우드 비용 이상탐지 도메인 지식

#### 2.1.1 시나리오별 정의 및 공식 판정 근거

| 시나리오 | 정의 | 왜 문제가 되는가 | 공식 판정 기준 출처 |
|---|---|---|---|
| 좀비(유휴) 리소스 | 실행 중이지만 실질적으로 사용되지 않는 리소스 | 아무 가치도 만들어내지 않으면서 계속 과금됨 — 가장 순수한 형태의 낭비 | AWS Compute Optimizer "idle" 정의(피크 CPU ≤5%) |
| 오버프로비저닝 | 실제 필요량보다 훨씬 큰 스펙이 할당된 리소스 | 사용하지 않는 용량만큼 초과 비용이 지속적으로 발생함 | AWS Trusted Advisor 저활용 기준(10%) + 마진 |
| EDoS | 정상 트래픽처럼 보이는 대량 요청으로 과금을 유발하는 공격 | 방어 측이 공격과 정상적인 인기 급증을 구분하지 못하면, 시스템이 스스로 스케일업하여 공격자가 의도한 비용 폭증을 그대로 실현시켜줌 | 업계 일반 정의(EDoS, Economic Denial of Sustainability) |
| Lambda 재시도 폭증 | 함수 오류 발생 시 재시도가 반복되며 호출 횟수·비용이 기하급수적으로 증가하는 현상 | 정상 트래픽이 아닌데도 재시도 자체가 호출 수(=비용)를 계속 늘리며, 근본 원인(버그)이 해결되지 않는 한 방치 시 손실이 계속 커짐 | AWS Lambda 공식 문서의 재시도 동작(Retry Behavior) + DLQ 권장 설정 기준 |
| S3 대량다운로드 | 짧은 시간에 비정상적으로 많은 다운로드 요청·바이트가 발생해 데이터 유출이 의심되는 상황 | 기밀 데이터 유출 가능성이라는 보안 위험과, 그 자체로 발생하는 전송 비용 폭증이라는 비용 위험이 동시에 존재함 | AWS GuardDuty S3 익스필트레이션 탐지 기준(`Exfiltration:S3/AnomalousBehavior`) |

📎 근거자료: AWS 공식 문서(Compute Optimizer 가이드, Trusted Advisor 체크리스트)
🔖 각주·미주: 표의 출처를 `[번호]`로 → 7장에 URL+접속일

#### 2.1.2 멀티에이전트 파이프라인 (일반 개념)
- 멀티에이전트 시스템이란 무엇인가, LangGraph가 이런 그래프 기반 상태 오케스트레이션에 왜 쓰이는 계열의 기술인지(일반론)
- 규칙 기반 탐지 vs ML 기반 탐지(IsolationForest)의 일반적 트레이드오프(해석가능성 vs 복합패턴 탐지력)

📎 근거자료: LangGraph 공식 문서, 이상탐지 기법 비교 서베이 논문 1~2개
🔖 각주·미주: `[번호]` → 7장

---

## 3. 연구 내용

### 3.1 시스템 아키텍처 및 기술 스택

#### 3.1.1 전체 아키텍처 개요
- Detection → Classification → Decision → [Approval Gate] → Action → QA → (롤백 루프) → Logging
- 팀원별 역할이 아키텍처 위 어디를 맡았는지 표시(팀원A: Detection/Logging/DB/Grafana, 팀원B: QA/Rule Book/inbound_handlers, 본인: Decision/Action/Approval Gate/웹제어판/Slack)
- 노드별 정의표(입력/출력/역할)

📊 시각화자료: LangGraph 그래프 다이어그램(Mermaid), 노드별 정의표
📎 근거자료: `graph.py`, `schema/state.py`의 `PipelineState`

#### 3.1.2 기술 스택 선정 근거

| 기술 | 선택 근거 |
|---|---|
| PostgreSQL | 승인 게이트(interrupt) 체크포인터 + 관리자 계정/로그 저장 — 트랜잭션 일관성 필요 |
| LangGraph | 노드별 에이전트 분리 + 상태(State) 명시적 공유, 승인게이트·롤백 같은 분기를 그래프로 표현 |
| boto3 | mock이 아닌 실제 AWS API로 실측 검증 |
| Grafana | 팀원 기 구축 인프라 재활용, 패널 단위 시각화 |
| FastAPI | 웹 제어판 백엔드, 비동기 처리로 승인 대기 등 상태 조회 용이 |
| IsolationForest | 라벨 없는 이상탐지, 규칙으로 정의하기 어려운 복합 패턴 보완 |
| Z-score(persistence) | 순간 노이즈 오탐 방지, 연속 k회 조건으로 해석 가능성 확보 |

📎 근거자료: 각 기술의 대안 대비 장단점 한 줄씩(예: IsolationForest vs One-Class SVM)

### 3.2 이상탐지 모델 설계

#### 3.2.1 Isolation Forest, Z-score 정의 (설계 관점)
- 2.1.2에서 정의한 일반개념을 이어받아, **우리가 어떤 파라미터로 적용했는지**만 서술(정의 재서술 금지 — 2장 참조로 대체)
- Z-score persistence: k=3 연속 포인트 조건의 의미
- IsolationForest: contamination 등 하이퍼파라미터

#### 3.2.2 데이터 준비
- 실측 데이터 소스: CloudWatch 지표(CPU/Network/GroupDesiredCapacity 등)
- 실험용 리소스 생성 절차(EC2 duty-cycle 부하생성 systemd-run 기반, ASG 생성, group metrics collection 활성화) — **재현 가능하도록 절차 중심 서술**
- 변인통제 설계(`SCENARIO_VARIABLE_CONTROL.md` 표 인용)

📎 근거자료: `playground/SCENARIO_VARIABLE_CONTROL.md`, `ec2_overprovision_setup.py`/`autoscaling_edos_trial.py` 파라미터
📊 시각화자료: 변인통제 매트릭스(시나리오 × 독립/통제변인)

> ⚠️ 4.1(시나리오설계개요)과 겹치지 않게: 여기(3.2.2)는 **방법론/절차 상세**, 4.1은 **결과를 읽기 위한 1페이지 요약표**로 역할 분리.

#### 3.2.3 데이터 분석 및 파라미터 (임계값 선정 근거 포함)
- `EC2_IDLE_CPU_THRESHOLD_PCT=5.0`(Compute Optimizer idle 기준), `EC2_OVERPROVISION_CPU_THRESHOLD_PCT=20.0`(Trusted Advisor 10%+마진), `sustained_fraction`(baseline 80%/recent 20%, min 3pt, min_fraction 0.6) — 각각 **수식과 선택 이유**

📎 근거자료: AWS 공식 기준 인용, sustained_fraction 도입 계기(2026-09-11: 기존 방식이 CLF-001 경계에 걸려 무효화된 실측 사례)
📊 시각화자료: 임계치 밴드 다이어그램(CPU% 축 위 zombie/overprovisioned/normal), sustained_fraction 판정 시계열 예시
🔖 각주·미주: Compute Optimizer/Trusted Advisor `[번호]`

> 📌 **멘토 의견 반영 ②** (이 절 끝에 삽입): "산업체 멘토 의견을 반영하여, 기존 EC2 자원 낭비 탐지 로직에 더해 Compute Optimizer/Trusted Advisor 기준을 인용한 오버프로비저닝 임계치(5~20%)를 신규 정의하고, Stop과 구분되는 Resize 조치를 구현하였다."

#### 3.2.4 모델 학습용 데이터셋 구축 및 평가
- [TODO: 팀원A] IsolationForest 학습 데이터 구성(정상/이상 비율, 수집기간), contamination 값 근거
- **오버피팅/데이터 편향 논의**: 규칙 기반 임계치는 테스트 데이터로 역산하지 않고 AWS 공식 기준을 그대로 사용(데이터 누수 방지 근거) / IsolationForest는 학습-검증 데이터가 시간대·인스턴스 단위로 겹치지 않는지 [TODO: 팀원A 확인]
- production(실제 방식) vs iforest_only vs teammate_compat(구버전) 비교로 "규칙+지속성 결합이 IsolationForest 단독보다 나은 이유" 서술

📎 근거자료: `compute_metrics` confusion matrix 3방식 비교
📊 시각화자료: 3방식 비교 막대그래프(TP/FN/FP/TN) — 2026-09-12 EDoS 로그(production TP=5, iforest_only TP=0, teammate_compat TP=1) 강조

#### 3.2.5 최적화
- 성능: `describe_instances` 불필요 호출 제거(97s→0.005s), cost 필드 백필로 LLM 폴백/429 회피
- 비용: t3.micro 사용, 프리티어 고려, 사전 vCPU/EBS/인스턴스-시간 budget 계산
- 정확도: `_low_utilization_check()` 네트워크 AND조건 버그 수정 등 디버깅 히스토리(전/후 성능 비교 포함)

📎 근거자료: Before/After 타이밍 실측값, 커밋 diff
📊 시각화자료: Before/After 비교 막대그래프

### 3.3 자동화 대응 및 운영 시스템

#### 3.3.1 탐지-대응 파이프라인
- Rule Book/Classification Rules 구조, inbound_handlers 연동 — [TODO: 팀원B]
- 승인 대기(Approval Gate) 메커니즘 — 위험도(risk) 산정, 승인 임계값
- **중간보고서 대비 변경사항**: (예) CLF-001 재작성(`sustained_fraction` 도입), 화이트리스트 이벤트기간 예외 추가 등
- LLM 프롬프트 설계(분류/의사결정에 LLM 개입하는 지점과 프롬프트 구조)
- **규칙 자동 승격(Rule Promotion) 메커니즘**: Decision Agent의 LLM 프롬프트에 단순히
  선택된 액션만이 아니라, 그 판단 로직을 if-else 한 줄짜리 **pseudo_code**로 함께
  출력하도록 요청함(`pipeline/decision_agent.py`의 `_select_action_with_llm`). 이렇게
  모은 LLM 판단 로그(`schema/logs/llm_decision_log.jsonl`)를 여러 건 누적해 분석한
  결과, 같은 리소스타입×이상유형 조합에서 **액션과 pseudo_code가 모두 일관되게
  반복되는 패턴**은 매번 LLM을 호출할 필요 없이 Rule Book으로 승격해도 되는 후보임을
  확인했다. 이를 근거로 `pipeline/decision_pseudocode_promoter.py`(결정 규칙용)와
  `pipeline/rule_promoter.py`(분류 규칙용)를 구현하여, 액션 일관성·pseudo_code
  일관성이 임계값 이상인 패턴을 자동으로 찾아 승격 후보 대기 큐(`schema/logs/
  pending_rule_promotions.json`)에 등록한다 — 다만 **승인 없이 자동으로 규칙에
  반영하지는 않고**, 3.3.2절의 웹 제어판 승인 대기 목록을 통해 관리자가 최종 승인해야
  Rule Book에 실제 반영되도록 설계했다(오탐이 누적된 패턴이 그대로 규칙으로 굳어지는
  것을 방지하기 위함).
- 리소스/시나리오별 조치 매핑표:

| 리소스 | 이상유형 | 조치 |
|---|---|---|
| EC2(zombie) | 유휴 | Stop |
| EC2(overprovisioned) | 과대프로비저닝 | Resize |
| AutoScaling(EDoS) | 용량급증 | ScaleDown + WAF Rate-based Rule |
| Lambda | 재시도폭증 | Throttle |
| S3 | 대량다운로드 | Block |

- **QA 및 자동 롤백**: SLA 재검증 항목(과거 CPU만 → 지금 확장된 항목, [TODO: 팀원B]), EC2 Stop+SLA 임계값 몽키패치로 실제 AWS 상태변화까지 검증한 롤백 실동작 실험, WAF 자동 해제(`remove_waf_rate_based_rule`) 신규 배선
- 본 시스템은 시나리오별로 "복구(Recovery)"를 다음과 같이 판정한다:

| 시나리오 | 복구(Recovery) 정의 |
|---|---|
| 좀비(유휴) 리소스 | 리소스가 중지(Stop)되어 더 이상 과금이 발생하지 않는 상태 |
| 오버프로비저닝 | 적정 스펙으로 축소(Resize)된 이후에도 가용성 SLA(응답 지연·에러율 등)가 유지되는 상태 |
| Lambda 재시도 폭증 | 동시성 제한(Throttle) 적용 후 에러율이 임계값(50%) 아래로 떨어지고 호출 수가 평상시 수준으로 감소한 상태 |
| S3 대량다운로드 | 퍼블릭 접근 차단(Block) 적용 후 다운로드 요청량이 평상시 기준선 수준으로 감소한 상태 |
| AutoScaling EDoS | WAF Rate-based Rule 적용 및 ScaleDown 실행 후, ALB request_count가 기준선 수준으로 감소하고 인스턴스 수가 원래대로 복귀한 상태 |

📎 근거자료: 롤백 실험 로그(AWS API 응답), WAF Rate-based Rule 공식 문서 `[번호]`
📊 시각화자료: 롤백 전/후 상태 다이어그램

> 📌 **멘토 의견 반영 ③** (QA/롤백 서술 끝에 삽입): "산업체 멘토 의견을 반영하여, 과거 발생했던 롤백 이슈의 원인을 재조사하였다. 정확한 재현은 어려웠으나 [TODO: 조사결과 한줄요약]되는 것을 확인하였으며, 현재 설계(QA의 실측 재검증 + 자동 롤백)가 유사 상황을 완화함을 실험으로 확인하였다." — 근본원인을 100% 특정 못했다면 솔직히 "로그 보존기간 등으로 확정 못함"이라 쓰고 현재 설계의 완화 효과로 방점 이동.

#### 3.3.2 관제 및 알림 시스템
- 웹 제어판: 승인 대기 목록(액션 승인 게이트 + 3.3.1의 규칙 자동 승격 후보 승인 큐,
  두 종류를 함께 노출), 실행 이력 조회, **화이트리스트 관리 화면**(일반 리소스 제외 / 이벤트 기간 예외 두 모드, 각 모드의 실제 효과를 등록 전에 보여주는 UI로 개선) — [TODO: 본인] 화면 캡처
- Grafana 대시보드: 전체 파이프라인 성공률, 30일 정규화 비용 절감, 파이프라인 실행시간 패널
- Slack 알림: 트리거 시점, 메시지 포맷

📎 근거자료: 실제 화면
📊 시각화자료: 웹 제어판 스크린샷(승인대기/실행이력/화이트리스트), Grafana 스크린샷 3~4장, Slack 알림 캡처
🔖 각주·미주: "30일 기준 정규화" 계산식 각주로 설명

---

## 4. 연구 결과 분석 및 평가

### 4.1 시나리오 설계 개요
- 5개 시나리오 요약표(3.2.2 방법론의 결과만 1페이지로 리마인드 — 절차 상세는 3.2.2 참조로 대체, 여기서 재서술 금지)

📊 시각화자료: 시나리오 요약 표 1개

### 4.2 시나리오별 실행 결과 (5개 시나리오 end-to-end: 탐지→조치→QA→로깅)

#### 4.2.1 탐지 성능

| 시나리오 | TP | FN | FP | TN | Accuracy | Recall | Recall 95% CI |
|---|---|---|---|---|---|---|---|
| S3 대량다운로드 | 5 | 0 | 1 | 7 | 92.3% | 100% | [47.8%, 100.0%] |
| EC2 좀비 | 5 | 0 | 0 | 8 | 100% | 100% | [47.8%, 100.0%] |
| EC2 오버프로비저닝 | 5 | 0 | 0 | 8 | 100% | 100% | [47.8%, 100.0%] |
| Lambda 재시도폭증(호출/에러) | 5 | 0 | 1 | 7 | 92.3% | 100% | [47.8%, 100.0%] |
| Lambda 스로틀(재시도폭증, 별도 시나리오) | 8* | 0 | 0* | 5 | - | 100% | - |
| AutoScaling EDoS (n=15+8) | 10 | 5 | 1 | 7 | 73.9% | 66.7% | [38.4%, 88.2%] |

\* Lambda 스로틀의 "오탐 3건"은 재조사 결과 실제 이상(계정 동시성 공유로 인한 진짜 Throttle 다발)으로 재분류되어 순수 TP/FP 구분이 4.2.2에서 별도 설명됨.

📊 시각화자료: 혼동행렬 히트맵(시나리오별)

#### 4.2.2 전체 성능 (분류/조치/QA)

| 시나리오 | Classification 정확도 | 액션 실행 건수 | 액션 성공률 | QA 통과율 |
|---|---|---|---|---|
| EC2 오버프로비저닝 | 80% | 5건 | 80%(4/5) | 100% |
| EC2 좀비 | 100% | (측정 중, n=13 실험 진행) | | |
| S3 대량다운로드(Block) | 100% | 3건 | 100% | 100% |
| Lambda 스로틀(재시도폭증) | 88.9% | 8건 | 100% | 100% |
| AutoScaling EDoS(ScaleDown+WAF) | - | 8건 | 100%(ScaleDown 자체) | 0%(WAF 실패를 QA가 8/8 정확히 감지) |

**ScaleDown 1건 `not_implemented` 관련 버그 발견 및 수정**: EC2 오버프로비저닝 실험 중 `cost_spike`로 분류된 1건이 EC2에 구현되지 않은 `ScaleDown` 액션을 선택받아 실행되지 않은 채(`action_result.status="not_implemented"`) 남았다. 이 케이스 자체가 왜 필터링을 통과해 후보로 남았는지는 규명하지 못했으나(향후 과제), 더 중요한 2차 문제로 QA_agent.py의 `_apply_rule_based_qa()`가 `action_result.status=="failed"`만 실패로 처리하고 `"not_implemented"`는 놓쳐서 이 케이스가 `qa_passed=True`로 잘못 기록되는 버그를 발견·수정하였다(4.4절 참고).

**AutoScaling EDoS의 WAF 연동 실패**: ScaleDown 자체는 8/8 성공했으나, WAF Rate-based Rule 연동은 8/8 전부 `WAFUnavailableEntityException`으로 실패하였다. Web ACL 전파 지연(최대 5분 대기까지 실측 검증), IAM 권한 부족, ALB 리스너 누락, 리소스 이름 재사용, 조직 정책(SCP) 제약을 순서대로 배제하였으나 근본 원인은 특정하지 못했으며, AWS 계정 레벨의 WAF-ELB 교차 서비스 연동 문제로 추정된다. 다만 QA가 이 실패를 8/8 정확히 감지하여 자동 롤백을 수행함으로써, 조치 실패가 시스템 신뢰성에 영향을 주지 않도록 하는 안전장치가 실제로 작동함을 확인하였다.

**Lambda 스로틀 시나리오의 재분류**: 정상(normal) 3건이 트리거되어 confusion matrix상 오탐(FP)처럼 보였으나, CloudWatch 로그 재조사 결과 13개 테스트 함수가 계정 동시성 한도를 공유하면서 실제로 Throttle이 각각 37/93/71회 발생한 것으로 확인되어, 오탐이 아닌 진짜 이상(true positive)으로 재분류하였다.

📊 시각화자료: 단계별 성공률 스택 막대그래프

#### 4.2.3 비용
- 시나리오별: 시스템 미적용 시 예상 비용 vs 시스템 적용 후 실제 비용 vs 절감액
- EC2 오버프로비저닝 실측: 190.25 instance-hours ≈ $4.00 등 실측값 기재

📎 근거자료: Cost Explorer 스크린샷, 직접 계산한 instance-hours 근거
📊 시각화자료: 시나리오별 비용 비교 막대그래프

#### 4.2.4 실행 소요 시간
- 단계별(detection/classification/decision/action/qa/logging) 평균·표준편차·최소·최대

📊 시각화자료: 단계별 소요시간 박스플롯/에러바 (5개 시나리오 비교)
📎 근거자료: `batch_pipeline_replay__*.json`의 timings 블록

### 4.3 시나리오별 통계적 분석
- 5개 시나리오 **풀링**(raw TP/FN/FP/TN 합산 후 accuracy/recall/precision/FPR 재계산 — 비율을 평균내지 않음)
- Clopper-Pearson 95% 신뢰구간 (정규근사 대신 쓰는 이유)

📊 시각화자료: recall 95% CI 에러바 차트(5개 시나리오 + 전체풀링 한 번에)
🔖 각주·미주: "Clopper-Pearson"·"풀링 원칙" 첫 등장 지점에 용어설명 각주

> 📌 **멘토 의견 반영 ④** (이 절 도입부에 삽입): "산업체 멘토 의견을 반영하여, 정성적 서술을 넘어 Accuracy/Recall/Precision/오탐율을 Clopper-Pearson 95% 신뢰구간과 함께 정량적으로 제시하였다."

### 4.4 버그 발견 및 수정 이력
1. Decision Agent 불필요한 `describe_instances` 호출 제거
2. cost 필드 백필
3. t3.small 단가 미등록
4. SSM 백그라운드 프로세스 생존 문제(systemd-run)
5. `_low_utilization_check()` 네트워크 AND조건 로직 버그
6. EDoS 용량검증 이중계산 버그
7. `MOCK_SEED_BUFFER_FROZEN=True`로 인해 IForest가 실데이터로 한 번도 재학습되지 않던 버그(2026-09-13 발견) — 온라인 학습 정상화 후에도 "창 1개 학습" 콜드스타트로 인한 자기강화적 오탐 패턴이 재확인되어 워밍업 스크립트로 완화
8. AutoScaling EDoS ARN 파싱 버그(TargetGroup ARN을 LoadBalancer ARN과 동일한 방식으로 잘라 request_count가 항상 0으로 조회되던 문제)
9. QA_agent.py `_update_llm_log_with_qa_result()`가 Classification 필드(`matched_rule_id`)를 잘못 참조해 LLM 판단 로그 17건 전부 `qa_result=null`로 남던 버그
10. Decision Agent의 `ALLOWED_ACTIONS`가 리소스 타입 무관하게 적용되어 Lambda에 미구현 액션·잘못된 boto3 스펙이 노출되던 버그
11. QA_agent.py가 `action_result.status=="not_implemented"`(액션 미구현으로 실행 자체가 안 된 경우)를 `"failed"`와 구분하지 못해 `qa_passed=True`로 잘못 기록되던 버그(2026-09-14, EC2 오버프로비저닝 실측 중 발견)
12. AutoScaling ScaleDown+WAF 연동에서 `WAFUnavailableEntityException`이 재현성 있게(8/8) 발생 — 원인은 재시도 간격이 너무 짧았기 때문(2초→4초, 총 6초)으로, 별도 재현 실험으로 실제 전파 지연이 30초 내외임을 확인하고 재시도 간격을 30초로 늘려 수정함(2026-09-14). 이 1차 수정을 반영해 AutoScaling EDoS 시나리오를 실제로 재실행(n_anomaly=8, 2026-09-15 완료)한 결과, 이번에는 `WAFDuplicateItemException`(Web ACL 이름 충돌)으로 7/8이 재실패하고 1/8만 성공했다 — 원인은 Web ACL 이름이 초 단위 타임스탬프(`int(time.time())`)만으로 생성되어 ThreadPoolExecutor로 여러 리소스를 동시 처리할 때 같은 초에 이름이 겹쳤기 때문. 이름에 uuid 접미사를 추가하고, 이 수정 직후 확인된 동시 호출 시 WAFv2 API `ThrottlingException`에 대비해 boto3 클라이언트를 adaptive 재시도 모드로 변경함(2026-09-15). 두 수정을 반영한 뒤 실제 함수(`apply_waf_rate_based_rule`)로 실험과 동일한 동시성(n=8)을 재현해 8/8 성공을 확인함 — 다만 이 재검증은 함수 단위 재현실험이며, 이 2차 수정을 반영한 전체 파이프라인(탐지~QA) 재실행은 시간 제약상 수행하지 않음(향후 재실험 시 확정 수치로 갱신 필요).
13. AutoScaling EDoS v7 실험(23개 리소스를 ThreadPoolExecutor로 동시 처리)에서, 같은 리소스의 동일한 raw_metrics로 `detection_node()`를 연달아 두 번 호출했는데 anomaly_flag가 True→False로 바뀌는 사고가 실측으로 확인됨(2건). 원인은 IsolationForest 모델·학습버퍼를 pickle 파일로 읽고 쓰는 `_get_or_train_iforest()`에 락이 없어서, 같은 프로세스의 다른 스레드가 그 사이에 버퍼를 갱신·재학습해 파일을 덮어쓰면 바로 이어지는 두 번째 호출이 방금 바뀐 모델을 읽어버리는 동시성 레이스 컨디션이었음(2026-09-14 발견). `threading.Lock()`으로 해당 함수 전체를 임계구역으로 묶어 같은 프로세스 내 스레드 간 경합을 제거함(수정 완료, 재실험은 시간 제약상 보류 — 향후 연구 과제로 별도 기재).
14. `ec2_lambda_repeated_trial.py`(실험용 반복 측정 스크립트)의 `detect_both()`가 프로덕션 함수 `_low_utilization_check()`의 세 번째 반환값(`utilization_band`: "zombie"/"overprovisioned"/None을 구분)을 버리고, 절대 임계값이 조금이라도 걸리면 무조건 `absolute_kind="idle"`로 라벨링하던 버그를 EC2 좀비 v2 엣지케이스 실험 중 발견함(2026-09-14). 예를 들어 CPU 7%(오버프로비저닝 밴드)로 걸린 케이스도 "idle"로 잘못 표시됨. 실제 프로덕션 `detection_node()`는 처음부터 `utilization_band`를 `state["ec2_utilization_band"]`에 올바르게 저장하고 있어 이 버그의 영향을 받지 않았음(코드 확인으로 검증). `utilization_band` 값을 직접 분기해 `absolute_kind`를 "idle"/"overprovisioned"/"error_surge"/None으로 정확히 매핑하도록 수정함(수정 완료).
15. 위 14번 버그 발견 과정에서, EC2 좀비 v2 엣지케이스 실험의 "경계 정상(edge_normal)" 그룹이 CPU 사용률 7%를 목표로 설계되었으나, 이는 시스템 자체의 3구간 분류 정의(≤5% 좀비, 5~20% 오버프로비저닝, >20% 정상) 상 "오버프로비저닝" 밴드에 해당해 애초에 "정상"으로 라벨링하기에 부적절한 테스트 설계였음을 확인함. 즉 해당 그룹의 오탐(FP) 중 일부는 탐지 로직의 결함이 아니라 실험 설계 자체의 경계값 선정 오류에서 기인함(2026-09-14 발견, 코드 수정 아님 — 향후 재실험 시 "정상" 경계 케이스는 20% 초과 값을 사용하도록 설계 변경이 필요한 향후 연구 과제로 기재).
16. `QA_agent.py`의 `_apply_rule_based_qa()`가 모든 분기(Rule Book force_pass/force_fail, NoAction, 실행 실패, 액션 미구현, 일반 SLA 체크)에서 항상 값을 반환해 `None`을 반환하는 경로가 없었던 버그를 발견함(2026-09-14). 그 결과 `qa_node`의 `if rule_result is not None:` 조건이 항상 참이 되어, "모호한 케이스"를 처리하도록 설계된 LLM 기반 QA(`_call_llm_qa`, 가용성 등을 프롬프트로 판단)가 실제로는 한 번도 호출되지 못하는 죽은 코드였음을 확인함. 규칙 기반 체크가 판단할 데이터 자체가 부족해 낙관적으로 통과 처리되던 경우(트리거 지표 없음/비용 데이터 부족)를 모호한 케이스로 간주해 `None`을 반환하도록 수정하여, LLM 폴백 경로가 실제로 동작하도록 함(수정 및 재현 테스트로 검증 완료).

📎 근거자료: 각 버그 실측 발견 로그(구체적 수치)
📊 시각화자료: (선택) 버그 수정 전/후 비교표

---

## 5. 결론 및 향후 연구 방향

> 실물 보고서 형식: "1. 주제명: 2~3문장" 번호 나열형. 한계는 향후연구방향에 자연스럽게 녹임.

### 5.1 결론 (번호 나열형 — 초안)
1. **탐지-조치-검증 엔드투엔드 자동화**: 5개 시나리오에 대해 탐지→조치→실측 재검증→자동 롤백까지 하나의 파이프라인으로 구현·검증
2. **위험도 기반 승인 게이트**: 완전자동화 리스크를 human-in-the-loop로 통제
3. **WAF 자동 연동**: 기존 미구현 상태였던 EDoS 대응 WAF 적용/해제를 실제 배선·검증
4. [TODO] 팀원 A/B 성과 1줄씩 추가

### 5.2 향후 연구 방향
1. 진동형 EDoS 패턴 탐지(주기성 탐지 기법 추가)
2. IP 단위 블랙리스트(Athena 등 인프라 구축 후)
3. ROC-AUC/F1/MTTD/MTTR 추가, n 확대로 CI 축소
4. [TODO] `logging_node` 유니코드 인코딩 버그 수정
5. AutoScaling EDoS 재실험(4.4절 13번 동시성 레이스 컨디션 수정 반영 후 n=23 전량 액션/QA 확보)

📎 근거자료: 없음(주장 위주). "왜 안 했는지" 근거는 3~4장 재인용 정도로 충분

---

## 6. 구성원별 역할 및 개발 일정

> ⚠️ 이번 팀 초안에서 빠져 있던 **필수 항목**. 학교 지정 틀에 명시적으로 요구됨 — 반드시 추가.

| 구성원 | 담당 모듈 | 주요 기여 |
|---|---|---|
| 팀원 A | Detection Agent, Logging, DB, Grafana | [TODO] |
| 팀원 B | QA Agent, Rule Book(Classification), inbound_handlers | [TODO] |
| 본인 | Decision Agent, Action Agent, Approval Gate, 웹 제어판, Slack 알림 | EC2 오버프로비저닝 탐지/조치 신규 구현, WAF 연동 배선, 화이트리스트 UI 개선, 다수 실측 버그 수정, 반복실험 검증 |

- 개발 일정: [TODO] 제안~설계~구현~통합~실측검증 단계별 날짜

📊 시각화자료: 간트차트/타임라인 그래프
📎 근거자료: `git log --oneline` 날짜별 정리

---

## 7. 참고 문헌

> IEEE 스타일 번호목록. 논문: `[N] 저자, "제목," 학회/저널, pp., 연도.` / 공식문서·웹: `[N] 기관명. 문서명 [Online]. Available: URL (accessed 날짜).`

1. AWS. Compute Optimizer User Guide [Online]. Available: https://docs.aws.amazon.com/compute-optimizer/ (accessed [TODO]).
2. AWS. Trusted Advisor Best Practice Checklist [Online]. Available: https://aws.amazon.com/premiumsupport/technology/trusted-advisor/ (accessed [TODO]).
3. AWS. AWS WAF Rate-based rule statement [Online]. Available: https://docs.aws.amazon.com/waf/ (accessed [TODO]).
4. LangChain. LangGraph documentation [Online]. Available: https://langchain-ai.github.io/langgraph/ (accessed [TODO]).
5. C. J. Clopper and E. S. Pearson, "The use of confidence or fiducial limits illustrated in the case of the binomial," Biometrika, vol. 26, no. 4, pp. 404-413, 1934.
6. Kubecost. Kubecost documentation [Online]. Available: https://www.kubecost.com/ (accessed [TODO]).
7. [TODO] CloudHealth, Densify 등 나머지 FinOps 도구 공식 페이지
8. [TODO] F. T. Liu, K. M. Ting, Z.-H. Zhou, "Isolation Forest," ICDM 2008.
9. 김형철. 산학협력 자문의견서. 인스웨이브, 2026.

🔖 각주·미주 형식 통일: (1) 용어설명 각주는 페이지 하단 숫자 위첨자, (2) 출처 인용은 `[N]` + 위 목록, (3) 외부 통계 그림은 캡션에 직접 표기(이중등재 불필요)

---

## 전체 체크리스트 (제출 전)
- [ ] 6장(구성원별 역할/일정) 채워졌는가 — 이번에 새로 추가한 필수 항목
- [ ] 4곳의 "📌 멘토 의견 반영" 문구가 실제로 본문에 삽입됐는가
- [ ] 3.2.1~3.2.5 넘버링이 실제 문서에서도 순서대로 매겨졌는가 (초안 단계에서 3.2.2 중복 있었음)
- [ ] 4.1~4.4 넘버링 연속성 확인 (초안 단계에서 4.4 누락, 4.3→4.5로 건너뛴 적 있었음)
- [ ] 2장에 "우리가 무엇을 했는지"가 섞여 들어가지 않았는가 (정의만 있어야 함)
- [ ] 3.2.2(데이터준비, 방법론)와 4.1(시나리오설계개요, 요약표)이 내용 중복 없이 분업됐는가
- [ ] 모든 수치에 "실측/추정" 표시
- [ ] 4.3 풀링표를 EDoS 결과 반영해 재계산
- [ ] 시각화자료 전부 캡션(그림 번호 + 설명 1줄)
- [ ] 각주 번호와 참고문헌 목록 상호 대조
