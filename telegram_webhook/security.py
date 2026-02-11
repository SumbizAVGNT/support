import hmac
import hashlib
import logging
from flask import Request
from config import USE_HMAC, CHATWOOT_WEBHOOK_SECRET, CHATWOOT_WEBHOOK_TOKEN, TELEGRAM_SECRET_TOKEN

logger = logging.getLogger("telegram_webhook")

def verify_telegram_secret(req: Request) -> bool:
    if not TELEGRAM_SECRET_TOKEN:
        return True
    got = (req.headers.get("X-Telegram-Bot-Api-Secret-Token") or "").strip()
    ok = (got == TELEGRAM_SECRET_TOKEN)
    if not ok:
        logger.warning("Invalid Telegram webhook secret token")
    return ok

def verify_chatwoot_webhook(req: Request):
    """Returns (ok: bool, http_code: int)"""
    raw = req.get_data(cache=False) or b""
    if USE_HMAC:
        if not CHATWOOT_WEBHOOK_SECRET:
            logger.error("USE_HMAC=true but CHATWOOT_WEBHOOK_SECRET missing")
            return False, 401
        got = req.headers.get("X-Chatwoot-Webhook-Signature", "")
        want = hmac.new(CHATWOOT_WEBHOOK_SECRET.encode(), raw, hashlib.sha256).hexdigest()
        ok = hmac.compare_digest(got, want)
        if not ok:
            logger.warning("Invalid Chatwoot webhook signature")
            return False, 401
        return True, 200
    else:
        expected = CHATWOOT_WEBHOOK_TOKEN
        if not expected:
            # no verification (осознанно)
            return True, 200
        got = req.headers.get("X-Webhook-Token", "")
        ok = (got == expected)
        if not ok:
            logger.warning("Invalid Chatwoot webhook shared token")
            return False, 401
        return True, 200
