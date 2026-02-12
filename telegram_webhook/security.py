"""Webhook signature verification."""

import hmac
import hashlib
import logging
from typing import Dict

from config import USE_HMAC, CHATWOOT_WEBHOOK_SECRET, CHATWOOT_WEBHOOK_TOKEN, TELEGRAM_SECRET_TOKEN

logger = logging.getLogger("tg-cw-bridge")


def verify_telegram_secret(headers: Dict[str, str]) -> bool:
    """Verify Telegram webhook secret token (X-Telegram-Bot-Api-Secret-Token header)."""
    if not TELEGRAM_SECRET_TOKEN:
        # No secret configured — skip verification (development mode)
        return True
    got = headers.get("X-Telegram-Bot-Api-Secret-Token") or headers.get("x-telegram-bot-api-secret-token") or ""
    return hmac.compare_digest(got, TELEGRAM_SECRET_TOKEN)


def verify_chatwoot_webhook(raw: bytes, headers: Dict[str, str]) -> bool:
    if USE_HMAC:
        if not CHATWOOT_WEBHOOK_SECRET:
            return False
        got = headers.get("X-Chatwoot-Webhook-Signature", "") or headers.get("X-Chatwoot-Signature", "")
        want = hmac.new(CHATWOOT_WEBHOOK_SECRET.encode(), raw, hashlib.sha256).hexdigest()
        return hmac.compare_digest(got, want)

    if CHATWOOT_WEBHOOK_TOKEN:
        return hmac.compare_digest(headers.get("X-Webhook-Token", ""), CHATWOOT_WEBHOOK_TOKEN)

    return True
