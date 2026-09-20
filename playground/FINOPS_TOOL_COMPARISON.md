# 기존 FinOps 도구와의 비교

멘토(김형철, 인스웨이브) 자문의견서 지적사항: "AWS Cost Anomaly Detection, Kubecost,
CloudHealth, Densify 등 기존 상용/오픈소스 FinOps 도구와의 비교가 없다"에 대한 대응.
각 도구는 실제 문서/자료를 확인해 정리했다(2026-09 기준).

---

## 비교표

| 항목 | **우리 시스템** | AWS Cost Anomaly Detection | Kubecost | CloudHealth (VMware/Broadcom) | Densify | (참고) AWS Auto Scaling | (참고) AWS Compute Optimizer |
|---|---|---|---|---|---|---|---|
| **탐지 대상** | 리소스 사용량 지표(CPU/네트워크/호출/에러율/desired_capacity) + 비용 | **비용(청구 데이터)만** | K8s 비용(namespace/deployment/pod) | 비용(계정/리전/서비스별) | 리소스 사용률(rightsizing 전용) | 없음(탐지 기능 자체가 없음) | 리소스 사용률(rightsizing 전용) |
| **탐지 방식** | Z-score(통계) + IForest(ML 다변량) + 절대임계치(업계표준 인용) 3종 혼합, 시나리오별 주력 기법이 다름 | ML — 계절성/추세 반영한 지출 베이스라인 모델 | **고정임계치**(평균 대비 편차, lookback window 사용자 설정) | AI 기반 이상 탐지(구체적 알고리즘 비공개) | ML — 60일 단위 사용 패턴 학습 | 사용자가 설정한 target metric 기반 반응형 스케일링 | percentile(P99.5/P90)+headroom, 사용자 설정 가능 |
| **원인 분류(왜 이상인지)** | Rule Book(빠른 기지 패턴) + LLM(신규 패턴) 하이브리드로 anomaly_type(비용비효율/비용급증/보안위험) 자동 분류 | ❌ 없음 — 어느 서비스/계정에서 늘었는지만 표시 | ❌ 없음 | ❌ 없음(영향받은 서비스 표시까지만) | N/A(rightsizing 단일 목적) | N/A | N/A |
| **위험도 기반 승인 게이트** | ✅ LOW=자동, MED/HIGH=사람 승인 | 해당없음(액션 자체가 없음) | 해당없음 | 부분적(거버넌스 정책 기반 워크플로 있음, 세부 비공개) | ✅ "승인된 추천만 자동 적용" | 해당없음 | 해당없음(추천만, 실행 안 함) |
| **자동 조치 실행** | ✅ Stop/Resize/Throttle/Block/ScaleDown 등 실제 실행 | ❌ 알림만(이메일/SNS/Slack) | ⚠️ 부분적(request sizing, namespace turndown 자동화 있음 — K8s 범위 한정) | ⚠️ 부분적(보안/컴플라이언스 워크플로 위주, 비용 이상 자체의 자동 조치는 불명확) | ✅ 승인된 리사이징을 실제 환경에 자동 적용 | ✅ (단, "위험 판단"이 아니라 사전 설정된 스케일링만) | ❌ 추천만, 실행 안 함(별도 자동화 붙여야 함) |
| **사후 검증 + 자동 롤백** | ✅ SLA 기준 미달 시 스스로 롤백 | ❌ | ❌ | ❌ | ❌(자료상 확인 안 됨) | ❌ | ❌ |
| **보안 이상 대응** | ✅ S3 대량다운로드 Block, AutoScaling EDoS 의심 ScaleDown | ❌ | ❌ | 부분적(별도 컴플라이언스 체크 기능 있음, 비용 이상탐지와는 별개 기능) | ❌ | ❌(오히려 EDoS에 취약 — 아래 참고) | ❌ |
| **EDoS 시나리오 대응** | ✅ 부하 급증이 공격성인지 판단해 방어적으로 ScaleDown | ❌(비용 급증 "발생 후" 알림만, 사후적) | ❌ | ❌ | ❌ | ⚠️ **오히려 역효과** — 설계 목적 자체가 "부하 늘면 스케일업"이라 공격에 그대로 순응함 | ❌ |
| **적용 범위** | AWS 5종 리소스(EC2/Lambda/S3/RDS/AutoScaling), 단일 클라우드 | AWS 전 서비스(비용만) | Kubernetes 전용 | 멀티클라우드(AWS/Azure/GCP) | 멀티클라우드, VM/컨테이너 워크로드 | AWS EC2/ECS 등 | AWS EC2/Lambda/EBS 등 |
| **정량 검증 방식(공개 자료 기준)** | Clopper-Pearson 95% CI 포함 confusion matrix(TP/FN/FP/TN), n=13 반복실험 | 비공개(내부 ML 모델 성능 미공개) | 비공개 | 비공개 | "20~40% 절감" 등 사례 기반 수치(방법론 비공개) | 해당없음 | 비공개 |

---

## 핵심 차별점 요약 (보고서용 서술)

1. **원인을 분류하지 않는다 → 우리는 분류한다.** 위 4개 도구 전부 "무엇이 이상인지"는 보여줘도 "왜 이상인지(비용비효율/비용급증/보안위험)"를 자동으로 판단하지 않는다. 우리는 Rule Book + LLM으로 이 판단 자체를 자동화한다.

2. **대부분 추천/알림에서 멈춘다 → 우리는 실행하고 검증한다.** AWS Cost Anomaly Detection·CloudHealth는 알림, AWS Compute Optimizer는 추천만 한다. Densify가 유일하게 "승인 후 자동 적용"까지 하지만 rightsizing 단일 목적이라 보안/공격 대응이 없다. 우리는 실행 후 SLA 기준으로 **스스로 검증하고 롤백**까지 하는 게 이 중 어디에도 없다.

3. **Auto Scaling은 EDoS를 방어하지 못하고 오히려 악화시킨다.** 이건 "다른 도구보다 못한다"가 아니라 "이 문제 자체를 해결할 수 없는 구조"라는 근본적 차이다 — Auto Scaling의 설계 목적 자체가 "부하 증가 시 스케일업"이라, 의도적 비용 폭증 공격(EDoS)에는 순응하게 되어있다. 우리 시스템은 이 부하가 정상 트래픽인지 공격인지 판단해서 방어적으로 대응한다.

4. **위험도에 따라 자율성을 조절한다.** Densify를 제외한 나머지는 "완전 자동 아니면 완전 수동" 이분법이다. 우리는 낮은 위험은 자동, 높은 위험(Block/ScaleDown)은 사람 승인을 거치는 중간 지대를 갖는다.

5. **정량 검증의 투명성.** 위 4개 도구는 전부 자사 알고리즘/정확도를 공개하지 않는다(영업비밀). 우리는 실 AWS 환경에서 Clopper-Pearson 95% CI를 포함한 confusion matrix를 전부 공개적으로 검증했다 — 학술적 엄밀성 면에서는 상용 도구보다 오히려 투명하다.

---

## 정직하게 인정해야 할 한계

- 위 도구들은 **실제 프로덕션 환경, 수백~수천 개 계정 규모**에서 검증된 반면, 우리는 **직접 유도한 통제 실험(n=13)** 수준이다 — "상용 도구보다 낫다"가 아니라 "상용 도구가 못 하는 기능적 차별점이 있다"로 주장 범위를 한정해야 한다.
- Densify의 ML 기반 rightsizing(60일 패턴 학습)은 우리의 고정 임계치(5%/20%) 방식보다 정교하다 — 이 부분은 "아직 단순화된 근사치"임을 인정하고, 향후 과제로 명시하는 게 정직하다.
- Kubecost의 사용자 설정 가능한 lookback window/outlier threshold처럼, 우리도 임계치를 고정값이 아니라 조정 가능한 파라미터로 노출하면 실운영 적응력이 좋아질 것 — 이것도 향후 과제로 언급 가능.

Sources:
- [AWS Cost Anomaly Detection: The Ultimate Guide | nOps](https://www.nops.io/blog/aws-cost-anomaly-detection/)
- [AWS Cost Anomaly Detection | How It Works & Setup 2026](https://go-cloud.io/aws-cost-anomaly-detection/)
- [Anomaly Detection - IBM Documentation (Kubecost)](https://www.ibm.com/docs/en/SSW0JQG_2.x/using-kubecost/navigating-the-kubecost-ui/anomaly-detection.html)
- [IBM Kubecost - K8s Cost Monitoring - Apptio](https://www.apptio.com/products/kubecost/)
- [Cost Anomaly Detection - Broadcom Techdocs (CloudHealth)](https://techdocs.broadcom.com/us/en/vmware-tanzu/cloudhealth/tanzu-cloudhealth/saas/tnz-cloudhealth/using-and-managing-tanzu-cloudhealth-anomaly-detection.html)
- [Avoid Costly Surprises with AI/ML Based Anomaly Detection - VMware Cloud Management](https://blogs.vmware.com/management/2022/07/avoid-costly-surprises-with-ai-ml-based-anomaly-detection.html)
- [Densify Review: IT & Security AI Tool | SmarterWay.AI](https://smarterway.ai/tools/densify)
- [How to take advantage of Rightsizing recommendation preferences in Compute Optimizer](https://aws.amazon.com/blogs/aws-cloud-financial-management/how-to-take-advantage-of-rightsizing-recommendation-preferences-in-compute-optimizer/)
