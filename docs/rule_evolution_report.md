# Rule Book 자가진화 루프 구현 보고서

## 1. 개요

### 1.1 배경

| 문제 | 설명 |
|------|------|
| 사람이 설계한 규칙의 한계 | 초기 규칙은 예상 시나리오 기반으로 설계되어, 실제 운영 환경에서 예상치 못한 패턴에 대응하기 어려움 |
| LLM 비결정성 | 동일 입력에도 다른 판단을 내릴 수 있어, 반복 검증된 패턴을 규칙으로 명문화할 필요성 존재 |

### 1.2 목표

운영할수록 판단 정확도가 높아지는 **자가진화 시스템** 구축

---

## 2. 시스템 아키텍처

### 2.1 전체 흐름

```text
[파이프라인 실행]
       |
       v
[logging_node()]
  - agent_runs, agent_steps, action_log 기록
  - rule_stats_logger.record_rule_stats() 호출
    - win/lose 판단
    - rule_stats 테이블 UPSERT
       |
       v (n건 누적 감지)
[rule_evolution_engine.trigger_evolution()]
  - 저성능 규칙 탐지 (win_rate < 60%)
  - LLM 규칙 개선/비활성화 결정
  - 유사 규칙 통합 분석
  - 변경 전 백업 (rule_history/)
  - JSON 파일 자동 업데이트
  - reload_rules() 즉시 적용
```

### 2.2 컴포넌트 구성

| 컴포넌트 | 역할 |
|----------|------|
| `logging_node()` | 파이프라인 최종 단계, rule_stats 기록 트리거 |
| `rule_stats_logger` | win/lose 판단, DB 기록, 진화 트리거 감지 |
| `rule_evolution_engine` | 저성능 탐지, LLM 개선, 백업/롤백 |
| `rule_engine` | 규칙 로딩, 매칭, reload |

---

## 3. 핵심 구현 내용

### 3.1 Win/Lose 판단 기준

| 조건 | 설명 |
|------|------|
| `qa_passed = True` | QA 검증 통과 |
| `cost_ok = True` | 비용 절감 또는 유지 (10% 이상 증가 없음) |
| `availability_ok = True` | 가용성 유지 (액션 성공) |
| `rollback_count = 0` | 첫 시도에 성공 (롤백 없음) |

> **판단 로직**: 4가지 조건 모두 충족 시 Win, 하나라도 불충족 시 Lose

### 3.2 자가진화 트리거 조건

```python
# 마지막 진화 이후 n건(기본 3회) 이상 실행되면 트리거
runs_since_last_evolution = total_runs - last_evolution_run_count
trigger = runs_since_last_evolution >= 3
```

### 3.3 저성능 규칙 탐지

```sql
SELECT rule_id, rule_type, win_rate
FROM rule_stats
WHERE win_rate < 0.6    -- 60% 미만
  AND total_runs >= 3   -- 최소 3회 이상 실행
ORDER BY win_rate ASC
```

### 3.4 LLM 규칙 개선 프로세스

**입력**: 저성능 규칙 + 유사 규칙 정보

**LLM 분석 후 추천 결과**:

| 추천 | 설명 |
|------|------|
| `improve` | 조건/결과 수정으로 정확도 향상 |
| `disable` | 규칙 자체가 부적절하여 비활성화 |
| `merge` | 유사 규칙과 통합 |

**출력**: JSON 파일 자동 업데이트 → `reload_rules()` 즉시 적용

---

## 4. 구현 파일 목록

| 파일 | 역할 |
|------|------|
| `pipeline/rule_stats_logger.py` | win/lose 판단, rule_stats 테이블 UPSERT, 진화 트리거 감지 |
| `pipeline/rule_evolution_engine.py` | 저성능 규칙 탐지, LLM 개선, 유사 규칙 통합, 백업/롤백 |
| `pipeline/logging_agent.py` (수정) | rule_stats 기록 및 자가진화 트리거 호출 추가 |
| `schema/rules/rule_history/` | 규칙 변경 이력 백업 디렉토리 |
| `playground/test_rule_evolution.py` | 단위 테스트 |

---

## 5. 데이터베이스 스키마 확장

```sql
CREATE TABLE IF NOT EXISTS rule_stats (
    rule_id                   TEXT PRIMARY KEY,
    rule_type                 TEXT NOT NULL,        -- classification, qa, llm
    total_runs                INTEGER DEFAULT 0,    -- 총 실행 횟수
    total_wins                INTEGER DEFAULT 0,    -- 성공 횟수
    win_rate                  DOUBLE PRECISION,     -- 승률 (자동 계산)
    last_evolution_run_count  INTEGER DEFAULT 0,    -- 마지막 진화 시점의 total_runs
    last_evolution_at         TIMESTAMPTZ,          -- 마지막 진화 실행 시각
    updated_at                TIMESTAMPTZ DEFAULT now()
);
```

---

## 6. 안전장치

| 기능 | 설명 |
|------|------|
| 변경 전 백업 | `rule_history/{rule_type}_{timestamp}.json` 형태로 자동 저장 |
| 백업 보관 | 30일 이상 된 백업 자동 정리 |
| 롤백 기능 | `rollback_rules(rule_type, timestamp)` 함수로 특정 시점 복원 |
| 규칙 충돌 감지 | 동일 조건 + 다른 결과 규칙 쌍 자동 탐지 |
| 파일 락 | `filelock`을 사용한 동시성 제어 (10초 타임아웃) |

---

## 7. 설정값

| 항목 | 기본값 | 비고 |
|------|--------|------|
| 트리거 임계값 (n) | 3회 | 추후 5회로 조정 가능 |
| win_rate 임계값 | 60% | 2/3 이상 성공 필요 |
| 백업 보관 기간 | 30일 | 자동 정리 |
| 파일 락 타임아웃 | 10초 | 동시성 제어 |

---

## 8. 테스트 결과

| 테스트 | 설명 | 결과 |
|--------|------|------|
| `test_is_win` | win/lose 판단 로직 검증 | PASS |
| `test_get_rule_id_from_state` | 규칙 ID 추출 검증 | PASS |
| `test_evolution_trigger_check` | 진화 트리거 조건 검증 | PASS |
| `test_find_similar_rules` | 유사 규칙 탐지 검증 | PASS |
| `test_detect_rule_conflicts` | 규칙 충돌 감지 검증 | PASS |
| `test_backup_and_rollback` | 백업/롤백 기능 검증 | PASS |
| `test_integration_scenario` | 통합 시나리오 검증 | PASS |

---

## 9. 향후 확장 계획

| 단계 | 내용 |
|------|------|
| 웹 UI 연동 | 유저가 규칙 진화 이력 확인 및 수동 롤백 가능 |
| Slack/이메일 알림 | 규칙 변경 시 관리자 알림 |
| A/B 테스트 | 개선된 규칙을 일부 트래픽에만 적용 후 성능 비교 |
| Grafana 대시보드 | rule_stats 기반 규칙별 성능 시각화 |
