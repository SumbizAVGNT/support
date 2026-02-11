from __future__ import annotations
from typing import Any, Dict, Optional, List
import logging

from utils import SESSION, safe_json
from config import (
    CHATWOOT_BASE_URL, CHATWOOT_ACCOUNT_ID, CHATWOOT_API_TOKEN,
    CW_ACCESS_TOKEN, CW_CLIENT, CW_UID, DEFAULT_TIMEOUT,
)
from telegram import tg_send_message
from dedupe import DEDUP

logger = logging.getLogger("telegram_webhook")

def _headers() -> Dict[str, str]:
    if CHATWOOT_API_TOKEN:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "api_access_token": CHATWOOT_API_TOKEN,
        }
    h = {"Accept": "application/json", "Content-Type": "application/json"}
    if CW_ACCESS_TOKEN and CW_CLIENT and CW_UID:
        h.update({"access-token": CW_ACCESS_TOKEN, "client": CW_CLIENT, "uid": CW_UID})
    return h

def _url(path: str) -> str:
    base = f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}"
    return f"{base}{path}"

def cw_get(path: str, **kw):
    return SESSION.get(_url(path), headers=_headers(), timeout=DEFAULT_TIMEOUT, **kw)

def cw_post(path: str, **kw):
    return SESSION.post(_url(path), headers=_headers(), timeout=DEFAULT_TIMEOUT, **kw)

def ensure_contact_and_inbox(inbox_id: int, source_id: str, name: str, email: str) -> int:
    """Create/find contact and idempotently link to inbox."""
    resp = cw_post("/contacts", json={"inbox_id": inbox_id, "source_id": source_id, "name": name, "email": email})
    if resp.status_code in (200, 201):
        js = safe_json(resp)
        cid = js.get("id") or (js.get("data") or {}).get("id")
        logger.info("contact created id=%s", cid)
        contact_id = int(cid)
    elif resp.status_code == 422:
        # fallback search by email
        s = cw_get("/contacts/search", params={"q": email})
        sjs = safe_json(s)
        payload = (sjs.get("payload") or [])
        if not payload:
            raise RuntimeError("Contact exists (422) but search returned empty payload")
        contact_id = int(payload[0].get("id"))
        logger.info("contact found by search id=%s", contact_id)
    else:
        resp.raise_for_status()
        raise RuntimeError("unreachable")

    # link inbox idempotently
    cw_post(f"/contacts/{contact_id}/contact_inboxes", json={"inbox_id": inbox_id, "source_id": source_id})
    return contact_id

def _extract_payload_list(j: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not isinstance(j, dict):
        return []
    if isinstance(j.get("payload"), list):
        return j["payload"]
    data = j.get("data") or {}
    if isinstance(data.get("payload"), list):
        return data["payload"]
    return []

def find_or_create_conversation(inbox_id: int, contact_id: int, source_id: str) -> int:
    """Reuse any open/pending/snoozed/on_hold conversation in the inbox, otherwise create."""
    r = cw_get(f"/contacts/{contact_id}/conversations")
    payload = _extract_payload_list(safe_json(r))
    for item in payload:
        try:
            if int(item.get("inbox_id") or 0) == inbox_id and (item.get("status") in {"open", "pending", "snoozed", "on_hold"}):
                cid = int(item["id"])
                logger.info("reusing conversation id=%s", cid)
                return cid
        except Exception:
            continue

    # create new
    r2 = cw_post("/conversations", json={"source_id": source_id, "inbox_id": inbox_id, "contact_id": contact_id})
    js = safe_json(r2)
    conv_id = js.get("id") or (js.get("data") or {}).get("id")
    if not conv_id:
        raise RuntimeError(f"Failed to create conversation: {js}")
    logger.info("conversation created id=%s", conv_id)
    return int(conv_id)

def post_message_to_conversation(conversation_id: int, content: str, is_private: bool = False) -> None:
    body = {"content": content or "", "message_type": 1, "private": bool(is_private)}
    r = cw_post(f"/conversations/{conversation_id}/messages", json=body)
    if not r.ok:
        logger.warning("Failed to post message to conversation %s: %s", conversation_id, r.text)

def handle_chatwoot_event(evt: Dict[str, Any]) -> None:
    """Forward agent (outgoing) messages to Telegram chat based on source_id."""
    # dedupe
    if "id" in evt:
        key = f"evt:{evt['id']}"
    elif "message" in evt:
        key = f"msg:{evt['message'].get('id')}:{evt.get('event')}"
    else:
        key = f"raw:{hash(str(evt))}"
    if not DEDUP.add(key):
        logger.info("Duplicate webhook event skipped: %s", key)
        return

    event = evt.get("event") or evt.get("name")
    message = evt.get("message") or {}
    content = evt.get("content") or message.get("content") or ""
    sender_type = (message.get("sender_type") or "").lower()

    conv = evt.get("conversation") or message.get("conversation") or {}
    contact_inbox = conv.get("contact_inbox") or {}
    source_id = str(contact_inbox.get("source_id") or "").strip()

    # need tg chat id
    if not source_id.isdigit():
        return
    chat_id = int(source_id)

    # forward only agent/user messages
    if event in {"message_created", "message_updated"} and sender_type in {"user", "agent"}:
        if content:
            tg_send_message(chat_id, content)
            logger.info("Forwarded agent message to Telegram chat_id=%s", chat_id)
