"""
Classification Agent
--------------------
탐지된 이상 신호를 받아서 anomaly_type을 분류하고 임시 조치를 수행

처리 전략
1) Rule-based : 명확한 케이스는 규칙으로 즉시 분류 
2) LLM : 규칙으로 판단 불가한 모호한 케이스는 LLM에 위임
"""

import json
import re
from typing import Optional

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate
from schema.state import PipelineState, ALLOWED_ACTIONS

from dotenv import load_dotenv
load_dotenv()

llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash", temperature=0.1)

# 프롬프트 
prompt = ChatPromptTemplate.from_template("""
당신은 AWS 클라우드 비용 이상 징후를 분류하는 전문가입니다.
아래 정보를 바탕으로 이상 유형을 분류하고 JSON으로만 응답하세요.
마크다운 코드블록, 설명 텍스트 없이 JSON만 출력하세요.
 
## 입력 정보
- 리소스 타입: {resource_type}
- 이상이 감지된 지표: {triggered_metrics}
- Z-score: {anomaly_score_zscore}
- Isolation Forest 점수: {anomaly_score_iforest}
- 최근 지표 요약: {metrics_summary}
 
## 분류 기준
- cost_inefficiency: 낭비형. 좀비 리소스, 오버프로비저닝. 긴급도 낮음.
- cost_spike: 급증형. 트래픽 폭증, Lambda 호출 폭증. 긴급도 높음.
- risk_security: 위험형. EDoS, DDoS, 비정상 접근. 매우 민감.

## 판단 우선순위
- risk_security는 비정상 접근, 알 수 없는 IP, EDoS 등 명확한 보안 근거가 있을 때만 사용
- 보안 근거 없이 cpu + network 동시 급증만으로는 반드시 cost_spike로 분류
- 애매한 경우 cost_spike > cost_inefficiency 순으로 보수적으로 판단
 
## 응답 형식 (JSON만, 다른 텍스트 금지)
{{
  "anomaly_type": "cost_inefficiency" | "cost_spike" | "risk_security",
  "reasoning": "판단 근거를 2문장 이내로",
  "interim_action": "즉시 취할 임시조치를 10단어 이내로 (없으면 null)"
}}
""")
 
chain = prompt | llm
 
# 1) Rule-based
def _metrics_summary(raw_metrics: dict) -> dict:
    """각 지표의 최근값(마지막)과 평균을 요약."""
    summary = {}
    for key, values in raw_metrics.items():
        if isinstance(values, list) and values:
            summary[key] = {
                "latest": round(values[-1], 3),
                "mean":   round(sum(values) / len(values), 3),
            }
    return summary
 
 
def _apply_rules(state: PipelineState) -> Optional[tuple[str, str, Optional[str]]]:
    """
    Rule-based 선처리.
    명확하게 분류 가능한 케이스만 처리하고 (anomaly_type, reasoning, interim_action) 반환.
    모호하면 None 반환 → LLM으로 넘어감.
    """
    resource_type     = state["resource_type"]
    triggered_metrics = state["triggered_metrics"]
    raw_metrics       = state["raw_metrics"]
    summary           = _metrics_summary(raw_metrics)
 
    # AutoScaling: 인스턴스 수 급증 -> EDoS 의심 
    if resource_type == "AutoScaling":
        desired = summary.get("group_desired_capacity", {})
        if desired.get("latest", 0) > desired.get("mean", 0) * 2:
            return (
                "risk_security",
                "AutoScaling 인스턴스 수가 평균 대비 2배 이상 급증 → EDoS 의심",
                "AutoScaling 최대 인스턴스 수 임시 제한",
            )
 
    # Lambda: 호출 횟수 + 에러 동시 급증 -> 무한루프 또는 호출 폭증 
    if resource_type == "Lambda":
        if "invocation_count" in triggered_metrics and "error_count" in triggered_metrics:
            return (
                "cost_spike",
                "Lambda 호출 횟수와 에러 수 동시 급증 → 무한루프 또는 호출 폭증",
                "Lambda 동시성 임시 제한 적용",
            )
 
    # S3: 다운로드 급증 단독 -> 데이터 유출 의심 
    if resource_type == "S3":
        if triggered_metrics == ["bytes_downloaded"]:
            return (
                "risk_security",
                "S3 bytes_downloaded 단독 급증 → 비정상 데이터 유출 의심",
                "S3 퍼블릭 접근 임시 차단",
            )
 
    # EC2/RDS: cost만 단독 이상 -> 좀비 리소스 또는 오버프로비저닝 
    if resource_type in ("EC2", "RDS"):
        if triggered_metrics == ["cost"]:
            return (
                "cost_inefficiency",
                "비용 지표만 단독 이상, 성능 지표 정상 → 좀비 리소스 또는 오버프로비저닝",
                None,
            )
 
    # 규칙으로 판단 불가 -> LLM으로
    return None


# 2) LLM 호출 + 응답 파싱 
def _parse_llm_response(text: str) -> dict:
    """
    LLM 응답에서 JSON 추출.
    마크다운 코드블록이 섞여있어도 처리.
    파싱 실패 시 fallback 반환.
    """
    # LLM 응답 전처리 :  ```json ... ```  제거
    cleaned = re.sub(r"```(?:json)?", "", text).replace("```", "").strip()
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        return {
            "anomaly_type": None,
            "reasoning":    f"LLM 응답 파싱 실패: {text[:100]}",
            "interim_action": None,
        }
 
 
def _call_llm(state: PipelineState) -> tuple[Optional[str], str, Optional[str]]:
    """LLM 호출 후 (anomaly_type, reasoning, interim_action) 반환."""
    summary = _metrics_summary(state["raw_metrics"])
 
    for attempt in range(3):  # 최대 3회 재시도
        try:
            response = chain.invoke({
                "resource_type":           state["resource_type"],
                "triggered_metrics":       state["triggered_metrics"],
                "anomaly_score_zscore":    state["anomaly_score_zscore"],
                "anomaly_score_iforest":   state["anomaly_score_iforest"],
                "metrics_summary":         json.dumps(summary, ensure_ascii=False),
            })
            parsed = _parse_llm_response(response.content)
 
            anomaly_type = parsed.get("anomaly_type")
 
            # 유효하지 않은 anomaly_type이면 재시도
            valid_types = {"cost_inefficiency", "cost_spike", "risk_security"}
            if anomaly_type not in valid_types:
                continue
 
            return (
                anomaly_type,
                f"[LLM] {parsed.get('reasoning', '')}",
                parsed.get("interim_action"),
            )
 
        except Exception as e:
            if attempt == 2:
                return (
                    None,
                    f"LLM 호출 실패 (3회 재시도 초과): {e}",
                    None,
                )
 
    return (None, "LLM 응답에서 유효한 anomaly_type 추출 실패", None)
 
 
# 메인 노드 함수 
def classification_node(state: PipelineState) -> PipelineState:
    # Rule-based 선처리
    rule_result = _apply_rules(state)
 
    if rule_result is not None:
        anomaly_type, reasoning, interim_action = rule_result
        prefix = "[Rule] "
    else:
        # 모호한 케이스 -> LLM 위임
        anomaly_type, reasoning, interim_action = _call_llm(state)
        prefix = ""
 
    state["anomaly_type"]             = anomaly_type
    state["classification_reasoning"] = f"{prefix}{reasoning}"
    state["interim_action_taken"]     = interim_action
 
    return state
 