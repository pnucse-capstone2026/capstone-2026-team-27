"""
Gemini API 호출 유틸
====================
여러 에이전트(decision_agent, classification_agent, QA_agent)가 각자
ChatGoogleGenerativeAI를 직접 호출하고 있는데, 무료 티어 쿼터(일일 20회 등)를
하나의 키가 다 쓰면 그 자리에서 실패한다. 이 모듈은 .env에 등록된 여러 개의
Gemini API 키를 순서대로 시도하다가, 429(RESOURCE_EXHAUSTED)를 받으면 다음
키로 넘어가는 call_gemini() 하나만 제공한다.

컨텍스트 유지가 필요 없는 단발성 프롬프트 호출 전용이다 (대화 히스토리 없음).
매 호출마다 키 후보를 처음부터 다시 시도하므로, 특정 키가 그 시점에 이미
소진돼 있어도 다음 호출에서 자동으로 다음 키부터 시작하지는 않는다 (단순
순환이 아니라 "현재 요청 하나를 끝까지 처리하기 위한" 재시도 순서다).
"""

from __future__ import annotations

import os
import logging

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

GEMINI_MODEL = "gemini-2.5-flash"

# GEMINI_KEY_1, GEMINI_KEY_2, GEMINI_KEY_3 ... 처럼 번호가 끊기지 않는 동안 계속 읽는다.
_GEMINI_KEY_ENV_PREFIX = "GEMINI_KEY_"


def _load_gemini_keys() -> list[str]:
    """
    입력: 없음 (.env의 GEMINI_KEY_1, GEMINI_KEY_2, GEMINI_KEY_3 ... 사용)
    출력: 설정된 키 문자열 리스트 (순서 보장), 하나도 없으면 빈 리스트
    """
    keys: list[str] = []
    i = 1
    while True:
        key = os.getenv(f"{_GEMINI_KEY_ENV_PREFIX}{i}")
        if not key:
            break
        keys.append(key)
        i += 1
    return keys


def _is_quota_exhausted(exc: Exception) -> bool:
    """
    입력: exc (call 중 발생한 예외)
    출력: 429(RESOURCE_EXHAUSTED) 에러인지 여부

    langchain_google_genai는 이를 GoogleGenerativeAIError로 감싸서
    "... (RESOURCE_EXHAUSTED): 429 RESOURCE_EXHAUSTED. ..." 형태의 문자열을
    담아 던진다. 버전에 따라 예외 클래스가 달라질 수 있어 문자열 매칭으로
    판별한다 (429/RESOURCE_EXHAUSTED가 아닌 에러는 키 순환 대상이 아니므로
    그대로 올린다).
    """
    text = str(exc)
    return "RESOURCE_EXHAUSTED" in text or " 429" in text or text.startswith("429")


def call_gemini(prompt: str, temperature: float = 0.1) -> str:
    """
    입력: prompt (단일 문자열, 대화 히스토리 없는 단발성 호출), temperature
    출력: LLM 응답 텍스트 (response.content)

    .env의 GEMINI_KEY_1 → GEMINI_KEY_2 → GEMINI_KEY_3 ... 순서로 시도한다.
    현재 키가 429(RESOURCE_EXHAUSTED)를 반환하면 다음 키로 넘어가고,
    429가 아닌 다른 에러(네트워크 오류, 잘못된 요청 등)는 키를 바꿔도 의미가
    없으므로 즉시 그대로 올린다. 등록된 키를 전부 시도했는데도 모두
    쿼터 소진이면 RuntimeError를 발생시킨다.

    기존 에이전트에서 ChatGoogleGenerativeAI를 직접 만들어 호출하던 부분을
    이 함수 호출 하나로 교체할 수 있도록, prompt str -> response str
    인터페이스만 노출한다.
    """
    from langchain_google_genai import ChatGoogleGenerativeAI
    from langchain_core.messages import HumanMessage

    keys = _load_gemini_keys()
    if not keys:
        raise RuntimeError(
            f"{_GEMINI_KEY_ENV_PREFIX}1, {_GEMINI_KEY_ENV_PREFIX}2, ... 중 "
            ".env에 설정된 Gemini API 키가 하나도 없습니다."
        )

    last_exc: Exception | None = None
    for idx, key in enumerate(keys, start=1):
        try:
            llm = ChatGoogleGenerativeAI(
                model=GEMINI_MODEL,
                google_api_key=key,
                temperature=temperature,
            )
            response = llm.invoke([HumanMessage(content=prompt)])
            return response.content
        except Exception as exc:  # noqa: BLE001 - 429 여부 판별 후 그대로 재발생 or 순환
            if not _is_quota_exhausted(exc):
                raise
            logger.warning(
                "%s%d 쿼터 소진(429) - 다음 키로 전환 (%d/%d)",
                _GEMINI_KEY_ENV_PREFIX, idx, idx, len(keys),
            )
            last_exc = exc
            continue

    raise RuntimeError(
        f"{_GEMINI_KEY_ENV_PREFIX}1 ~ {_GEMINI_KEY_ENV_PREFIX}{len(keys)} "
        f"전부 쿼터 소진(429)으로 호출 실패"
    ) from last_exc
