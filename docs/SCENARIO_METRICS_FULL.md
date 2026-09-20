# 시나리오별 전체 지표 정리 (2026-09-14 최종 재계산)

`playground/statistical_validation_report.py`(2026-09-14 EC2 좀비 참조 파일 버그 수정 후 재실행),
`playground/full_metrics_report.py`, `playground/full_pipeline_verification_report.py` 실행 결과를
그대로 옮긴 것. 계산 안 되는 항목은 값 대신 이유를 적었다(추정/생략 없음).

**버그 수정 확인**: EC2 좀비는 이제 `ec2_zombie_replay_trial__n8-5_scriptv1_20260910.json`(진짜
유휴 인스턴스 데이터, profile=silent/whisper/bursty 등)을 참조한다. 이전에는
`ec2_repeated_trial__n8-5_scriptv1_20260912.json`(profile=target_cpu_12.0pct/45.0pct — 사실상
오버프로비저닝 재측정 데이터)을 잘못 참조하고 있었다. 헤드라인 수치(Accuracy/Recall)는
우연히 동일해서 안 바뀌었지만, 방식별(iforest_only/teammate_compat) breakdown과 McNemar는
이번 수정으로 값이 바뀌었다(→ EC2 좀비는 이제 단일 방식만 존재해 McNemar 자체가 불가능).

**95% CI와 이항검정(vs 우연 50%)의 관계**: 둘은 완전히 같은 걸 두 번 쓰는 게 아니다.
CI는 "참값이 어느 범위에 있을 가능성이 높은가"(범위/불확실성)를 보여주고, 이항검정은
"이 결과가 동전던지기(50%)보다 유의하게 나은가"를 p-value로 공식 판정한다. 게다가
본 문서의 CI는 양측(two-sided) 95%인 반면 이항검정은 단측(one-sided, recall은
"greater", FPR은 "less")이라 기준 자체가 달라서, CI만으로 이항검정 결과를 그대로
유추할 수 없다. 그래서 아래 각 시나리오마다 둘 다 표기했다.

---

## 1. S3 대량다운로드

- 원본 파일: `s3_repeated_trial__window2.5h_objsize50kb_n8-5_scriptv5_20260910.json`
- 탐지 방식: production 1개만 저장됨(iforest_only/teammate_compat 없음)

**n_anomaly / n_normal**: 5 / 8

**Confusion Matrix**: TP=5, FN=0, FP=1, TN=7

**Point estimate**
| 지표 | 값 | 95% CI |
|---|---|---|
| Accuracy | 92.3% | [64.0%, 99.8%] |
| Recall | 100.0% | [47.8%, 100%] |
| Precision | 83.3% | (CI 미계산 — 스크립트가 precision CI는 안 뽑음, 필요시 추가 계산 가능) |
| FPR | 12.5% | [0.3%, 52.7%] |

**F1-score**: 0.909

**에이전트별 정확도**
| 탐지 | 분류 | 액션 | 검증(QA) |
|---|---|---|---|
| 92.3% | 100%(Rule Book 5/5) | 100%(3/3) | 100%(3/3) |

**이항검정(vs 우연 50%)**: Recall p=0.0312(유의함) / FPR p=0.0352(유의함)

**McNemar**: ❌ 불가 — 저장된 탐지 방식이 production 1개뿐이라 비교 대상이 없음

**실행 소요시간(에이전트별, n=3)**
| 단계 | 평균 | 표준편차 | 최소 | 최대 |
|---|---|---|---|---|
| detection | 0.026초 | 0.006초 | 0.018초 | 0.032초 |
| classification | 0.004초 | 0.006초 | 0.000초 | 0.013초 |
| decision | 0.016초 | 0.004초 | 0.011초 | 0.021초 |
| action | 0.308초 | 0.019초 | 0.295초 | 0.335초 |
| qa | 300.14초 | 0.014초 | 300.12초 | 300.16초 |
| logging | 0.007초 | 0.005초 | 0.003초 | 0.014초 |
| **total** | **300.63초** | 0.030초 | 300.60초 | 300.67초 |

**MTTD** = 0.026초 / **MTTR**(탐지+분류+결정+액션 평균, QA 대기 제외) = 평균 0.355초, 중앙값 0.347초 (n=3)

**훈련/평가 데이터 독립성**: 시나리오 전용 수치 없음(9절 참고, 시스템 공통 서술만 존재)

**결과분석**: S3는 절대임계값 없이 순수 통계(z-score+IForest OR게이트)만으로 탐지하는
시나리오라, Recall 100%가 나왔다는 것 자체가 통계 기반 탐지력을 보여주는 유의미한
결과다(이항검정 p=0.0312로 우연보다 유의). 다만 n=13(정상8/이상5)로 작아 CI가
[47.8%, 100%]로 넓다 — "100%가 항상 보장된다"가 아니라 "이 표본에서는 전부 맞았고,
더 큰 표본이면 참값이 이 범위 안일 것"으로 해석해야 한다. McNemar는 방식이 production
하나뿐이라 애초에 다른 탐지방식과 비교할 수 없는 시나리오다. 액션(Block)·QA는 별도의
3건짜리 소규모 실측이지만 100% 성공해 탐지→조치→검증 전체 체인이 실제로 작동함을
보여준다.

---

## 2. EC2 좀비(유휴 리소스)

- 원본 파일: `ec2_zombie_replay_trial__n8-5_scriptv1_20260910.json` (2026-09-14 참조 수정됨)
- 탐지 방식: production 1개만 저장됨

**n_anomaly / n_normal**: 5 / 8

**Confusion Matrix**: TP=5, FN=0, FP=0, TN=8

**Point estimate**
| 지표 | 값 | 95% CI |
|---|---|---|
| Accuracy | 100.0% | [75.3%, 100%] |
| Recall | 100.0% | [47.8%, 100%] |
| Precision | 100.0% | — |
| FPR | 0.0% | [0%, 36.9%] |

**F1-score**: 1.000

**에이전트별 정확도**
| 탐지 | 분류 | 액션 | 검증(QA) |
|---|---|---|---|
| 100% | 100%(LLM 5/5) | ❌ 탐지 전용 replay라 액션 실행 데이터 없음 | ❌ 위와 동일 |

**이항검정**: Recall p=0.0312(유의함) / FPR p=0.0039(유의함)

**McNemar**: ❌ 불가 — production 단일 방식만 존재(2026-09-14 이전엔 다른 시나리오 파일을
잘못 참조해서 3방식이 있는 것처럼 보였으나, 올바른 파일로 고치니 원래 단일 방식만 있었음)

**실행 소요시간**: ❌ 탐지 전용 replay 파일이라 `timings` 필드 자체가 없음. (참고: 지금 EC2
좀비 v2 재실험이 진행 중이며, 그 결과가 나오면 실제 액션+QA 포함 전체 파이프라인 데이터를
새로 얻을 수 있음)

**훈련/평가 데이터 독립성**: 시나리오 전용 수치 없음

**결과분석**: EC2 좀비는 CPU≤5%라는 **고정 절대임계값**으로 판정하는 시나리오라,
Recall 100%는 통계·ML 모델의 성능이 아니라 결정론적 규칙이 정확히 동작했다는 의미다
— 테스트 케이스가 임계값 경계(5% 근처)에서 충분히 떨어져 있으면 항상 맞을 수밖에
없는 구조이므로, 이 100%를 일반적인 탐지 성능 우수성으로 과대 해석하면 안 된다.
CI가 [47.8%, 100%]로 넓은 것도 "결정론적이라 항상 100%"가 아니라 n=13이 작아서
생기는 통계적 불확실성임을 함께 밝혀야 한다. 액션/QA 데이터가 없는 건 이 replay
데이터셋이 탐지 전용이기 때문이며, 지금 진행 중인 v2 실험이 이 시나리오 최초의
실제 액션(Stop) 실측이 된다.

---

## 3. EC2 오버프로비저닝

- 원본 파일: `ec2_overprovision_repeated_trial__converted.json`
- 탐지 방식: production / iforest_only / teammate_compat 3개 저장됨

**n_anomaly / n_normal**: 5 / 8 (전 방식 공통)

**Confusion Matrix**
| 방식 | TP | FN | FP | TN |
|---|---|---|---|---|
| production | 5 | 0 | 0 | 8 |
| iforest_only | 0 | 5 | 0 | 8 |
| teammate_compat | 1 | 4 | 0 | 8 |

**Point estimate (production 기준)**
| 지표 | 값 | 95% CI |
|---|---|---|
| Accuracy | 100.0% | [75.3%, 100%] |
| Recall | 100.0% | [47.8%, 100%] |
| Precision | 100.0% | — |
| FPR | 0.0% | [0%, 36.9%] |

**F1-score(방식별)**: production=1.000 / iforest_only=계산불가(TP=0) / teammate_compat=0.333

**에이전트별 정확도**
| 탐지 | 분류 | 액션 | 검증(QA) |
|---|---|---|---|
| 100% | 80%(LLM 4/5) | 80%(4/5) | 100%(5/5) |

**이항검정(production 기준)**: Recall p=0.0312(유의함) / FPR p=0.0039(유의함)

**McNemar**
| 비교 | b | c | p-value | 유의성 |
|---|---|---|---|---|
| iforest_only vs production | 0 | 5 | 0.0625 | 유의하지 않음(경계값에 가까움) |
| iforest_only vs teammate_compat | 0 | 1 | 1.0 | 유의하지 않음 |
| production vs teammate_compat | 4 | 0 | 0.125 | 유의하지 않음 |

**실행 소요시간(에이전트별)**
| 단계 | n | 평균 | 표준편차 | 최소 | 최대 |
|---|---|---|---|---|---|
| detection | 13 | 0.106초 | 0.031초 | 0.044초 | 0.164초 |
| classification | 5 | 16.94초 | 3.64초 | 10.50초 | 20.81초 |
| decision | 5 | 4.46초 | 8.29초 | 0.301초 | 21.05초 |
| action | 5 | 17.27초 | 10.42초 | 0.247초 | 33.15초 |
| qa | 5 | 301.13초 | 0.383초 | 300.73초 | 301.76초 |
| logging | 5 | 2.04초 | 0.009초 | 2.033초 | 2.056초 |
| **total** | 13 | 134.94초 | 166.37초 | 3.36초 | 350.33초 |

(classification~logging은 n=5 — 실제 액션까지 간 5건만 해당 단계를 거침. detection/total은 n=13 전체 표본 기준)

**MTTD** = 0.106초 / **MTTR**(탐지+분류+결정+액션 평균, QA 대기 제외) = 평균 38.78초, 중앙값 38.11초 (n=5)

**훈련/평가 데이터 독립성**: 시나리오 전용 수치 없음

**결과분석**: 이 시나리오도 좀비와 같은 절대임계값 계열(CPU 5~20%)이라 production
Recall 100%는 마찬가지로 결정론적 규칙의 성공이지 통계 모델의 일반화 성능이 아니다.
다만 McNemar 결과가 이 구조를 직접 증명한다 — **iforest_only 단독으로는 Recall
0%(TP=0/FN=5)**로 이 시나리오를 전혀 못 잡는다. 즉 절대임계값 컴포넌트가 없었다면
이 시나리오는 탐지 자체가 실패했을 것이므로, OR 게이트로 여러 판정 경로를 병렬
결합한 설계(3.1.1절)가 실제로 값어치를 한 사례다. 액션 성공률 80%(4/5)는 탐지
문제가 아니라 EC2에 미구현된 ScaleDown이 후보로 잘못 선택된 설계 결함이었고(4.4절
버그 11), QA는 여전히 100% 통과해 조치 자체(Resize)의 신뢰성은 높다.

---

## 4. Lambda 재시도폭증(구버전, 호출/에러 급증)

- 원본 파일: `lambda_repeated_trial__n8-5_scriptv1_20260909.json`
- 탐지 방식: production / iforest_only / teammate_compat 3개 저장됨

**n_anomaly / n_normal**: 5 / 8

**Confusion Matrix**
| 방식 | TP | FN | FP | TN |
|---|---|---|---|---|
| production | 5 | 0 | 1 | 7 |
| iforest_only | 5 | 0 | 1 | 7 |
| teammate_compat | 5 | 0 | 4 | 4 |

**Point estimate (production 기준)**
| 지표 | 값 | 95% CI |
|---|---|---|
| Accuracy | 92.3% | [64.0%, 99.8%] |
| Recall | 100.0% | [47.8%, 100%] |
| Precision | 83.3% | — |
| FPR | 12.5% | [0.3%, 52.7%] |

**F1-score(방식별)**: production=0.909 / iforest_only=0.909 / teammate_compat=0.714

**에이전트별 정확도**
| 탐지 | 분류 | 액션 | 검증(QA) |
|---|---|---|---|
| 92.3% | ❌ classification_accuracy 파일 없음(생성 안 됨) | ❌ 탐지 전용, 액션 데이터 없음 | ❌ 위와 동일 |

**이항검정(production 기준)**: Recall p=0.0312(유의함) / FPR p=0.0352(유의함)

**McNemar**
| 비교 | b | c | p-value | 비고 |
|---|---|---|---|---|
| iforest_only vs production | 0 | 0 | 계산불가(N/A) | 두 방식 완전히 동일 판정(불일치 쌍 없음) |
| iforest_only vs teammate_compat | 3 | 0 | 0.25 | 유의하지 않음 |
| production vs teammate_compat | 3 | 0 | 0.25 | 유의하지 않음 |

**실행 소요시간**: ❌ `elapsed_sec`(스크립트 전체 실행시간)만 있고 에이전트별 breakdown 없음

**훈련/평가 데이터 독립성**: 시나리오 전용 수치 없음

**결과분석**: 이 시나리오는 절대임계값 없이 3방식(production/iforest_only/
teammate_compat) 전부 Recall 100%를 기록했다는 점이 특징이다 — 세 방식이 완전히
동일하게 판정한 iforest_only vs production 조합은 McNemar 자체가 불가(불일치 쌍
없음)일 정도로 일관됐다. 다만 FPR은 방식별로 크게 갈린다(production/iforest_only
12.5% vs teammate_compat 50%) — 구버전 teammate_compat 방식이 정상 케이스를 훨씬
많이 오탐한다는 뜻으로, 이후 팀이 IForest 단독 판단으로 리팩터링한 결정(4.4절)이
타당했음을 뒷받침한다. 이 시나리오는 신버전(Lambda 스로틀)으로 대체되며 액션/QA
데이터 없이 탐지 성능 비교용으로만 남아있다.

---

## 5. Lambda 스로틀(429)/동시성 소진 재시도폭증 (신버전, 팀원 PR#40)

- 원본 파일: `team_results/lambda_throttle/clean_verification_20260914.json`
- 탐지 방식: production(`detected`) / iforest_only(`gate_iforest_triggered`) /
  zscore_only(`gate_zscore_triggered`) — **`statistical_validation_report.py`의 SCENARIOS
  목록에는 없어서 이번에 수동으로 별도 계산함**(스크립트에 정식으로 편입하려면 팀 파일
  포맷용 분기 하나를 추가해야 함)

**n_anomaly / n_normal**: 5 / 8

**Confusion Matrix**
| 방식 | TP | FN | FP | TN |
|---|---|---|---|---|
| production | 5 | 0 | 3 | 5 |
| iforest_only | 5 | 0 | 3 | 5 |
| zscore_only | 0 | 5 | 0 | 8 |

**Point estimate (production 기준)**
| 지표 | 값 | 95% CI |
|---|---|---|
| Accuracy | 76.9% | [46.2%, 95.0%] |
| Recall | 100.0% | [47.8%, 100%] |
| Precision | 62.5% | — |
| FPR | 37.5% | [8.5%, 75.5%] |

**F1-score**: production=0.769 / iforest_only=0.769 / zscore_only=계산불가(TP=0)

**에이전트별 정확도**
| 탐지 | 분류 | 액션 | 검증(QA) |
|---|---|---|---|
| 76.9% | 88.9%(Rule Book 8/9) | 100%(8/8) | 100%(8/8) |

**이항검정(production 기준)**: Recall p=0.03125(유의함) / FPR p=0.3633(**유의하지 않음** — 6개 시나리오 중 FPR이 우연과 통계적으로 구분 안 되는 유일한 경우)

**McNemar**
| 비교 | b | c | p-value | 비고 |
|---|---|---|---|---|
| production vs iforest_only | 0 | 0 | 계산불가(N/A) | 완전히 동일 판정 |
| production vs zscore_only | 5 | 3 | 0.727 | 유의하지 않음 |
| iforest_only vs zscore_only | 5 | 3 | 0.727 | 유의하지 않음 |

**실행 소요시간(에이전트별, n=13/8)**
| 단계 | 평균 | 표본수 |
|---|---|---|
| detection | 0.343초 | 13 |
| classification | ~0.00초(Rule Book만 매칭, LLM 미호출) | 8 |
| decision | 0.002초 | 8 |
| action | 0.888초 | 8 |
| qa | 300.93초 | 8 |
| logging | 0.090초 | 8 |
| **total** | **187.19초**(NoAction 포함 평균이라 낮게 나옴, 트리거된 8건만 보면 QA 대기 300초가 지배적) | 13 |

⚠️ 이 시나리오의 timing 데이터는 있지만 `full_metrics_report.py`의 집계 대상 파일 목록에
빠져 있어 아래 "통합 MTTD/MTTR"에는 반영되지 않았다.

**훈련/평가 데이터 독립성**: 시나리오 전용 수치 없음. 단, `note` 필드에 계정 동시성 공유로
인한 정상군 3건의 재분류 근거는 별도로 기록돼 있음(분류 정확도 산정에 반영됨).

**결과분석**: FPR 37.5%(3/8)만 보면 오탐이 심한 것처럼 보이지만, 그 이항검정이
6개 시나리오 중 유일하게 "유의하지 않음"(p=0.3633)으로 나온 이유가 바로 이 3건이다
— 재조사 결과 이 3건은 실제로 계정 동시성 한도를 공유한 다른 함수의 영향으로 진짜
Throttle이 37~93회씩 발생한 **진짜 이상**이었다(분류 정확도 88.9%에 반영). 즉
탐지기의 오탐이 아니라 애초에 "정상" 라벨 자체가 틀렸던 경우이므로, 원본 confusion
matrix의 FPR 수치를 그대로 "탐지 성능이 나쁘다"로 읽으면 안 된다. McNemar는
zscore_only가 Recall 0%로 이 시나리오를 전혀 못 잡는 것을 보여줘(iforest_only/
production만 100%), IForest가 이 시나리오의 실질적 탐지 주체임을 확인시켜준다.
액션(Throttle)·QA는 100% 성공했고, SHAP 기반 지표 기여도까지 전부 기록돼 해석
가능성도 확보됐다.

---

## 6. AutoScaling EDoS (v7, n=15+8)

- 원본 파일: `autoscaling_edos_traffic_trial__n8-15_scriptv4_20260913.json`
- 탐지 방식: production / iforest_only / zscore_only 3개 저장됨

**n_anomaly / n_normal**: 15 / 8

**Confusion Matrix**
| 방식 | TP | FN | FP | TN |
|---|---|---|---|---|
| production | 10 | 5 | 1 | 7 |
| iforest_only | 10 | 5 | 1 | 7 |
| zscore_only | 0 | 15 | 0 | 8 |

**Point estimate (production 기준)**
| 지표 | 값 | 95% CI |
|---|---|---|
| Accuracy | 73.9% | [51.6%, 89.8%] |
| Recall | 66.7% | [38.4%, 88.2%] |
| Precision | 90.9% | — |
| FPR | 12.5% | [0.3%, 52.7%] |

**F1-score**: production=0.769 / iforest_only=0.769 / zscore_only=계산불가(TP=0)

**에이전트별 정확도**
| 탐지 | 분류 | 액션(ScaleDown) | 검증(QA) |
|---|---|---|---|
| 73.9% | ❌ classification_accuracy 파일 없음 | 100%(8/8, ScaleDown만) — WAF 연동은 별도로 0%(8/8 실패) | 0%(8/8 실패, WAF 실패를 QA가 정확히 감지→자동 롤백 8/8) |

**이항검정(production 기준)**: Recall p=0.1509(**유의하지 않음** — 6개 시나리오 중 Recall이
우연과 통계적으로 구분 안 되는 유일한 경우, n이 가장 크지만 그만큼 성능도 낮게 나옴) / FPR
p=0.0352(유의함)

**McNemar**
| 비교 | b | c | p-value | 비고 |
|---|---|---|---|---|
| iforest_only vs production | 0 | 0 | 계산불가(N/A) | 완전히 동일 판정 |
| iforest_only vs zscore_only | 10 | 1 | **0.0117** | **유의함** — IsolationForest가 z-score 단독보다 유의하게 우수 |
| production vs zscore_only | 10 | 1 | **0.0117** | **유의함** |

**실행 소요시간**: ❌ `pipeline_result.timings` 필드 자체가 없음 — step_timings 실측 계측을
이 실험 이후에 코드에 추가해서, 이 실험 데이터에는 애초에 기록이 안 됨

**훈련/평가 데이터 독립성**: 이 시나리오가 v6(2026-09-13)에서 온라인 학습 구조로 인한
자기강화적 오탐이 실측으로 확인된 바로 그 사례다(9절 참고) — 6개 시나리오 중 유일하게
"이 시나리오에서 문제가 실제로 재현됐다"고 구체적으로 지목 가능한 케이스.

**결과분석**: Recall 66.7%는 6개 시나리오 중 가장 낮고, 이항검정도 유의하지 않음
(p=0.1509)으로 나왔지만 — 이건 표본이 가장 크기(n=23) 때문에 오히려 가장 신뢰할
수 있는 수치다. v5(n=10)에서는 Recall 80%로 더 높게 나왔었는데, 표본을 늘리자
점추정치는 낮아지고 대신 CI는 [38.4%, 88.2%]로 상대적으로 좁아졌다 — 작은 표본의
100%/80%보다 큰 표본의 66.7%가 통계적으로 더 믿을 만한 값이라는 걸 보여주는
좋은 사례다. McNemar(iforest_only vs zscore_only, p=0.0117)는 이 시나리오에서
z-score 단독으로는 거의 못 잡고(Recall 0%) IsolationForest가 실질적 탐지를
담당한다는 걸 통계적으로 확인해준다. 액션·QA 측면에서는 ScaleDown 자체는 8/8
성공했으나 WAF 연동이 8/8 전부 실패(계정 레벨 이슈로 추정, 미해결)했고, QA가 이
실패를 정확히 감지해 8/8 자동 롤백을 트리거함으로써 "일부 조치가 실패해도 전체
시스템은 안전하게 되돌아간다"는 안전장치가 실제로 검증됐다.

---

## 7. 풀링(전체 시나리오 통합) — S3 + EC2좀비 + EC2오버프로 + Lambda구버전 + AutoScaling EDoS

(Lambda 스로틀은 `statistical_validation_report.py`의 SCENARIOS 목록에 없어서 자동 풀링에서
빠져있음 — 수동으로 합치려면 TP=35,FN=5,FP=6,TN=42,n=88로 재계산 필요, 아래는 스크립트가
실제로 출력한 5개 시나리오 기준 풀링값)

**n_anomaly=35, n_normal=40, n=75**
**Confusion Matrix**: TP=30, FN=5, FP=3, TN=37

| 지표 | 값 | 95% CI |
|---|---|---|
| Accuracy | 89.3% | [80.1%, 95.3%] |
| Recall | 85.7% | [69.7%, 95.2%] |

F1-score: 0.882

**결과분석**: 개별 시나리오는 절대임계값 계열(좀비/오버프로/Lambda구버전)이 섞여 있어
100%가 여러 개 나오지만, 풀링은 원본 TP/FN/FP/TN을 합산한 뒤 재계산한 값이라 그런
개별 100%들에 표본이 눌려 과대평가되지 않는다. Accuracy 89.3%/Recall 85.7%는 절대
임계값 시나리오의 "쉬운 100%"와 통계 기반 시나리오의 "더 어려운 66.7~100%"가 섞인
현실적인 종합 지표로 봐야 한다. Lambda 스로틀이 빠진 5개 시나리오 기준값이라, 이걸
6개로 다시 합치면(TP=35,FN=5,FP=6,TN=42,n=88) 수치가 소폭 달라질 수 있음에 유의.

---

## 8. 통합 실행시간 / MTTD / MTTR

⚠️ **정정(2026-09-14)**: 이 절은 원래 `full_metrics_report.py`의 `TIMING_FILES` glob이
계산한 값(n=111)을 썼는데, `batch_pipeline_replay__EC2_*.json`을 glob으로 잡으면서
확정본(`..._20260912_134432.json`, n=13) 외에 관계없는 9/10 탐색용 실행분 4개까지
섞여 들어가 표본이 부풀려져 있었다(3절 EC2 오버프로비저닝 실행시간 각주에서 발견한
것과 동일한 버그). 아래는 실제로 confusion matrix/액션/QA 검증에 쓴 **정확히 3개
파일**(S3 `3x_real_pipeline_20260910_025234.json`, EC2 오버프로비저닝
`batch_pipeline_replay__EC2_20260912_134432.json`, Lambda 스로틀
`team_results/lambda_throttle/clean_verification_20260914.json`)의 원본 타이밍
샘플만 다시 합산한 값이다. **EC2 좀비·Lambda(구버전)·AutoScaling EDoS는 여전히
타이밍 데이터 자체가 없어 이 집계에서 빠진다**(각 절 참고).

총 표본: 29건(S3 3 + EC2오버프로비저닝 13 + Lambda스로틀 13)

| 단계 | n | 평균 | 표준편차 | 최소 | 최대 |
|---|---|---|---|---|---|
| detection | 29 | 0.204초 | 0.236초 | 0.011초 | 1.020초 |
| classification | 16 | 5.295초 | 8.111초 | 0.000초 | 20.809초 |
| decision | 16 | 1.399초 | 5.076초 | 0.002초 | 21.050초 |
| action | 16 | 5.899초 | 9.630초 | 0.247초 | 33.154초 |
| qa | 16 | 300.843초 | 0.411초 | 300.123초 | 301.756초 |
| logging | 16 | 0.685초 | 0.916초 | 0.003초 | 2.056초 |
| **total** | 29 | 175.502초 | 156.564초 | 0.912초 | 350.333초 |

(classification~logging은 n=16 — 실제 액션까지 도달한 샘플만: S3 3 + EC2오버프로 5 + Lambda스로틀 8. detection/total은 n=29 전체 표본 기준)

**MTTD(평균 탐지시간)** = 0.204초
**MTTR(탐지+분류+결정+액션 평균, QA 대기 제외)** = 평균 12.903초 / 중앙값 1.383초 (n=16)

**결과분석**: 평균(12.9초)과 중앙값(1.38초)이 크게 벌어지는 건 EC2 오버프로비저닝의
Resize 한 건이 LLM 분류 호출 지연으로 21초 넘게 걸린 이상치 때문이다(decision
표준편차 5.08초가 이를 반영). 즉 "보통은" 1.4초 안팎으로 끝나지만, LLM 호출이 걸리는
소수 케이스가 평균을 크게 끌어올린다 — 이런 이유로 평균과 중앙값을 같이 보고하는 게
중요하다. QA 단계(약 300초)가 전체 소요시간의 절대 다수를 차지하는 건 고정 대기
설계(3.3.3절, 지표 갱신 주기 5분 통일) 때문이며 최적화 여지가 없는 의도된 지연이다.

---

## 9. 훈련/평가 데이터 독립성 & 오버피팅 가능성

❌ **시나리오별 수치가 아니라 시스템 전체에 대한 정성적 서술 1건뿐**(`full_metrics_report.py`
QUALITATIVE_NOTES 그대로 인용):

> **훈련/평가 독립성**: `detection_agent.py`의 IsolationForest는 온라인 학습 구조라, '정상으로
> 판단된' 윈도우가 즉시 훈련 버퍼에 편입되고 모델이 재학습된다. EDoS 실험 패턴상 같은
> 리소스의 베이스라인(before) 구간이 먼저 버퍼에 들어간 뒤 그 리소스의 공격 후(after) 구간을
> 채점하므로, 완전한 훈련/평가 데이터 독립은 아니다. 실제로 v6(2026-09-13)에서 이 구조로
> 인해 모델이 [자기강화적 오탐 문제를 일으킨 것이 실측으로 확인되었다].
>
> **오버피팅 위험**: IsolationForest는 온라인 버퍼 크기가 작을 때(특히 1개 윈도우) 그 좁은
> 분산에 과적합되어, 이후 들어오는 정상 데이터조차 이상으로 오판하는 자기강화적 패턴이
> 실측으로 확인되었다(Phase 5, 창 1개 학습 = 정상 32.7% 오탐 문제와 동일 메커니즘).
> MAX_WINDOWS_PER_TYPE(FIFO)로 완화하나 근본적으로 온라인 학습 특유의 리스크다.

**구분해야 할 별개 데이터**: PDF 4.3.4절의 "held-out 재검증 오탐률 0%"는 이 6개 실측
시나리오가 아니라 `generate_eval_dataset.py`가 만든 435개 합성(목업) 평가 데이터셋 기준이다
— 실제 AWS n=13/n=23 실험과는 다른 데이터이므로 섞어서 인용하면 안 된다.

---

## 종합 표: 항목별 가용성 (6개 시나리오)

| 항목 | S3 | EC2좀비 | EC2오버프로 | Lambda구 | Lambda스로틀 | AutoScaling |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| n_anomaly/n_normal | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Confusion Matrix | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Accuracy+CI/Recall+CI/Precision/FPR+CI | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| F1-score | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 탐지 정확도 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 분류 정확도 | ✅ | ✅ | ✅ | ❌ | ✅ | ❌ |
| 액션 성공률 | ✅ | ❌ | ✅ | ❌ | ✅ | ✅ |
| QA 통과율 | ✅ | ❌ | ✅ | ❌ | ✅ | ✅ |
| 이항검정(Recall/FPR vs 50%) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| McNemar | ❌(방식 1개) | ❌(방식 1개, 수정 후 확정) | ✅ | ✅ | ✅(수동계산) | ✅ |
| 실행시간(에이전트별)/MTTD·MTTR | ✅ | ❌ | ✅ | ❌ | ✅ | ❌ |
| 훈련/평가 독립성(시나리오 전용) | ❌ | ❌ | ❌ | ❌ | ❌ | ❌(단, 이 시나리오가 실제 문제 재현 사례) |

**핵심 요약**: 9개 항목을 전부 채우는 시나리오는 없다. McNemar는 탐지 방식이 하나뿐인
S3·EC2좀비에서 구조적으로 불가능하고, 실행시간은 6개 중 3개(S3, EC2오버프로, Lambda스로틀)만 완전하며,
분류/액션/QA는 실제로 전체 파이프라인(액션까지) 실행한 4개 시나리오(S3/EC2오버프로/Lambda
스로틀/AutoScaling)에서만 가능하다. 훈련/평가 독립성은 애초에 시나리오별 지표가 아니라
시스템 전체의 구조적 특성에 대한 서술이다.
