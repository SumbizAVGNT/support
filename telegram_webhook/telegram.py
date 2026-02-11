from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Optional, Dict
import logging

from utils import SESSION, safe_json
from config import TELEGRAM_BOT_TOKEN, CHATWOOT_TELEGRAM_INBOX_ID, DEFAULT_TIMEOUT

logger = logging.getLogger("telegram_webhook")

TG_API_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

def tg_api(method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{TG_API_BASE}/{method}"
    r = SESSION.post(url, json=payload, timeout=DEFAULT_TIMEOUT)
    js = safe_json(r)
    if not r.ok:
        logger.warning("Telegram API %s failed: %s", method, js)
    return js

def tg_send_message(chat_id: int, text: str, reply_to_message_id: Optional[int] = None) -> None:
    payload = {"chat_id": chat_id, "text": text}
    if reply_to_message_id:
        payload["reply_to_message_id"] = reply_to_message_id
    tg_api("sendMessage", payload)

@dataclass
class ParsedUpdate:
    kind: str                  # "message" | "callback" | "unknown"
    chat_id: int = 0
    text: str = ""
    display_name: str = ""
    inbox_id: int = CHATWOOT_TELEGRAM_INBOX_ID
    _callback_id: Optional[str] = None

    def answer_callback(self):
        if self.kind != "callback" or not self._callback_id:
            return
        tg_api("answerCallbackQuery", {"callback_query_id": self._callback_id})

def parse_update_basic(upd: Dict[str, Any]) -> ParsedUpdate:
    # message / edited_message
    msg = upd.get("message") or upd.get("edited_message")
    if msg:
        chat = msg.get("chat", {})
        chat_id = int(chat.get("id"))
        from_user = msg.get("from", {}) or {}
        username = from_user.get("username") or str(chat_id)
        first_name = from_user.get("first_name") or username
        text = msg.get("text") or ""
        return ParsedUpdate(kind="message", chat_id=chat_id, text=text, display_name=first_name)

    # callback_query
    cq = upd.get("callback_query")
    if cq:
        chat_id = int((cq.get("from") or {}).get("id"))
        return ParsedUpdate(kind="callback", chat_id=chat_id, _callback_id=cq.get("id"))

    return ParsedUpdate(kind="unknown")
