"""
Classification Agent
--------------------
탐지된 이상 신호를 받아서 anomaly_type을 분류하고 임시 조치를 수행

처리 전략
1) Rule-based : 명확한 케이스는 규칙으로 즉시 분류
2) LLM : 규칙으로 판단 불가한 모호한 케이스는 LLM에 위임
        → LLM 판단은 schema/logs/llm_classification_log.jsonl에 기록
"""

import json
import os
import re
import uuid
from datetime import datetime
from typing import Optional

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate
from schema.state import PipelineState, ALLOWED_ACTIONS
from pipeline.rule_engine import get_rule_engine

from dotenv import load_dotenv
load_dotenv()

# LLM 판단 로그 경로
LLM_LOG_PATH = os.path.join(os.path.dirname(__file__), "..", "schema", "logs", "llm_classification_log.jsonl")

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
 
 
def _apply_rules(state: PipelineState) -> Optional[tuple[str, str, Optional[str], Optional[str]]]:
    """
    Rule Book 기반 규칙 매칭.
    매칭되는 규칙이 있으면 (anomaly_type, reasoning, interim_action, rule_id) 반환.
    매칭되는 규칙이 없으면 None 반환 → LLM으로 넘어감.
    """
    engine = get_rule_engine()
    matched_rule = engine.match_classification_rules(state)

    if matched_rule is None:
        return None

    result = matched_rule.get("result", {})
    anomaly_type = result.get("anomaly_type")
    interim_action = result.get("interim_action")
    reasoning = engine.format_reasoning(matched_rule, state)
    rule_id = matched_rule.get("rule_id")

    return (anomaly_type, reasoning, interim_action, rule_id)


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
 
 
def _log_llm_judgment(state: PipelineState, anomaly_type: Optional[str],
                      reasoning: str, interim_action: Optional[str]) -> None:
    """
    LLM 판단 결과를 JSONL 파일에 기록.
    QA 결과(qa_passed, rollback_count)는 이후 QA Agent에서 trace_id로 연결하여 추가.
    """
    trace_id = state.get("trace_id")
    if not trace_id:
        return  # trace_id 없으면 로깅 스킵

    metrics_summary = _metrics_summary(state.get("raw_metrics", {}))

    log_entry = {
        # 추적 정보
        "trace_id": trace_id,
        "logged_at": datetime.utcnow().isoformat() + "Z",

        # 입력 (LLM 판단에 사용된 컨텍스트)
        "input": {
            "resource_id": state.get("resource_id"),
            "resource_type": state.get("resource_type"),
            "triggered_metrics": state.get("triggered_metrics", []),
            "metrics_summary": metrics_summary,  # latest + mean per metric
        },

        # LLM 출력
        "output": {
            "anomaly_type": anomaly_type,
            "interim_action": interim_action,
            "reasoning": reasoning,
        },

        # 분류 결과 (Rule Book 매칭 안 됨)
        "matched_rule_id": None,

        # QA 결과 (나중에 QA Agent가 채움)
        "qa_result": None,
    }

    try:
        os.makedirs(os.path.dirname(LLM_LOG_PATH), exist_ok=True)
        with open(LLM_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"[classification_agent] LLM 로그 기록 실패: {e}")


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
    # trace_id 생성 (없으면)
    if not state.get("trace_id"):
        state["trace_id"] = str(uuid.uuid4())

    # Rule Book 기반 규칙 매칭
    rule_result = _apply_rules(state)

    if rule_result is not None:
        anomaly_type, reasoning, interim_action, rule_id = rule_result
        prefix = f"[Rule:{rule_id}] "
        state["matched_rule_id"] = rule_id
    else:
        # 모호한 케이스 -> LLM 위임
        anomaly_type, reasoning, interim_action = _call_llm(state)
        prefix = ""
        state["matched_rule_id"] = None

        # LLM 판단 로깅 (규칙 승격 분석용)
        _log_llm_judgment(state, anomaly_type, reasoning, interim_action)

    state["anomaly_type"]             = anomaly_type
    state["classification_reasoning"] = f"{prefix}{reasoning}"
    state["interim_action_taken"]     = interim_action

    return state
 