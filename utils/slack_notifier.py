"""
Slack Incoming Webhook으로 알림을 보낸다.

알림 대상:
  - requires_approval == True 로 승인 대기가 생겼을 때 (risk_level MED/HIGH)
  - QA 롤백이 2회를 초과해서 더 이상 자동 재시도하지 않을 때 (rollback_exhausted)

   알림 전송 실패(webhook 만료, 네트워크 오류 등)가 파이프라인 실행 자체를 막으면 안 되므로, 
   예외를 절대 밖으로 던지지 않고 콘솔에만 원인을 남긴다
   (logging_node의 DB 저장 실패 처리와 같은 원칙)
"""

from __future__ import annotations

import os

import requests
from dotenv import load_dotenv

load_dotenv()

_TIMEOUT_SECONDS = 5


def send_slack_alert(text: str) -> None:
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        print("[slack_notifier] SLACK_WEBHOOK_URL이 .env에 없어 알림을 건너뜁니다.")
        return

    try:
        resp = requests.post(webhook_url, json={"text": text}, timeout=_TIMEOUT_SECONDS)
        resp.raise_for_status()
    except Exception as exc:
        # requests 예외의 repr()에는 요청 URL(=웹훅 비밀값)이 그대로 포함되는 경우가 많아
        # 원문 그대로 찍지 않고, URL을 가린 채로만 원인을 남긴다.
        safe_reason = str(exc).replace(webhook_url, "<webhook-url-redacted>")
        print(f"[slack_notifier] 알림 전송 실패 — 원인: {safe_reason}")
