#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram ↔ Chatwoot bridge (FastAPI + aiogram v3)
— без Telegram WebApp, только бот и кнопки.

Версия 5.5
- FIX: multipart -> message_type="incoming" (вместо "0"), private="false"
- IMG: корректные MIME/расширения, чтобы скриншоты рендерились как изображения
- AVATAR: подтягиваем аватар TG при создании/поиске контакта
- CLOSE: уведомление о закрытии (несколько вариантов событий) + восстановление chat_id по conv_id
- UI: более информативное меню и подсказки
"""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import hmac
import io
import json
import logging
import mimetypes
import os
import sqlite3
import time
import zlib
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import httpx
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse, StreamingResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import (
    Update, Message, CallbackQuery,
    InlineKeyboardButton, InlineKeyboardMarkup,
)

# ============================== ENV ==============================

APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
APP_PORT = int(os.getenv("APP_PORT", "5501"))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_SECRET_TOKEN = os.getenv(
    "TELEGRAM_WEBHOOK_SECRET_TOKEN",
    os.getenv("TELEGRAM_SECRET_TOKEN", "")
).strip()
TELEGRAM_WEBHOOK_PUBLIC_URL = os.getenv("TELEGRAM_WEBHOOK_PUBLIC_URL", "").rstrip("/")
TELEGRAM_ALLOWED_UPDATES = [
    u.strip()
    for u in os.getenv(
        "TELEGRAM_ALLOWED_UPDATES",
        "message,edited_message,callback_query"
    ).split(",")
    if u.strip()
]

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN is required")

CHATWOOT_BASE_URL = os.getenv("CHATWOOT_BASE_URL", "").rstrip("/")
CHATWOOT_ACCOUNT_ID = os.getenv("CHATWOOT_ACCOUNT_ID", "1").strip()
CHATWOOT_INBOX_ID = int(os.getenv("CHATWOOT_INBOX_ID", os.getenv("CHATWOOT_TELEGRAM_INBOX_ID", "5")))
CHATWOOT_API_TOKEN = os.getenv("CHATWOOT_API_TOKEN", "").strip()
CW_ACCESS_TOKEN = os.getenv("CHATWOOT_ACCESS_TOKEN", "").strip()
CW_CLIENT = os.getenv("CHATWOOT_CLIENT", "").strip()
CW_UID = os.getenv("CHATWOOT_UID", "").strip()

if not CHATWOOT_BASE_URL:
    raise RuntimeError("CHATWOOT_BASE_URL is required")

USE_HMAC = os.getenv("USE_HMAC", "false").lower() == "true"
CHATWOOT_WEBHOOK_SECRET = os.getenv(
    "CHATWOOT_HMAC_TOKEN",
    os.getenv("CHATWOOT_WEBHOOK_SECRET", "")
).strip()
CHATWOOT_WEBHOOK_TOKEN = os.getenv("CHATWOOT_WEBHOOK_TOKEN", "").strip()

FILE_PROXY_PUBLIC_BASE = os.getenv("FILE_PROXY_PUBLIC_BASE", "").rstrip("/")

AGENT_USER = os.getenv("AGENT_USER", "").strip()
AGENT_PASS = os.getenv("AGENT_PASS", "").strip()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
DB_PATH = os.getenv("STATE_DB", "state.db")

# ============================== LOGGING ==============================

def _setup_logging():
    fmt = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
    logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO), format=fmt)

_setup_logging()
logger = logging.getLogger("tg-cw-bridge")

# ============================== HTTP ==============================

HTTP_TIMEOUT = httpx.Timeout(30, connect=10)
HTTP = httpx.AsyncClient(
    timeout=HTTP_TIMEOUT,
    limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
    headers={"User-Agent": "telegram-chatwoot-bridge/5.5"}
)

# ============================== AIROGRAM ==============================

try:
    from aiogram.client.default import DefaultBotProperties
    BOT = Bot(
        token=TELEGRAM_BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
except Exception:
    BOT = Bot(token=TELEGRAM_BOT_TOKEN, parse_mode=ParseMode.HTML)

DP = Dispatcher()
ROUTER = Router()
DP.include_router(ROUTER)

# ============================== DB ==============================

def _db():
    conn = sqlite3.connect(DB_PATH, timeout=10, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute(
        """CREATE TABLE IF NOT EXISTS sessions(
            chat_id INTEGER PRIMARY KEY,
            contact_id INTEGER,
            conversation_id INTEGER,
            nickname TEXT
        );"""
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS closures(
           id INTEGER PRIMARY KEY AUTOINCREMENT,
           chat_id INTEGER,
           conversation_id INTEGER,
           closed_at INTEGER
        );"""
    )
    conn.execute("CREATE INDEX IF NOT EXISTS closures_chat_idx ON closures(chat_id, closed_at DESC);")
    conn.execute("CREATE INDEX IF NOT EXISTS sessions_conv_idx ON sessions(conversation_id);")
    return conn

def db_get_session(chat_id: int) -> Dict[str, Any]:
    with _db() as conn:
        row = conn.execute(
            "SELECT contact_id, conversation_id, nickname FROM sessions WHERE chat_id=?",
            (chat_id,)
        ).fetchone()
    if not row:
        return {"contact_id": None, "conversation_id": None, "nickname": None}
    return {"contact_id": row[0], "conversation_id": row[1], "nickname": row[2]}

def db_upsert_session(
    chat_id: int,
    contact_id: Optional[int],
    conversation_id: Optional[int],
    nickname: Optional[str]
):
    with _db() as conn:
        conn.execute(
            """INSERT INTO sessions(chat_id, contact_id, conversation_id, nickname)
               VALUES(?,?,?,?)
               ON CONFLICT(chat_id) DO UPDATE SET
                 contact_id=excluded.contact_id,
                 conversation_id=excluded.conversation_id,
                 nickname=COALESCE(excluded.nickname, sessions.nickname)""",
            (chat_id, contact_id, conversation_id, nickname)
        )

def db_set_conv(chat_id: int, conv_id: Optional[int]):
    s = db_get_session(chat_id)
    old = s.get("conversation_id")
    db_upsert_session(chat_id, s["contact_id"], conv_id, s["nickname"])
    if old:
        try:
            CONV2CHAT.pop(int(old), None)
        except Exception:
            pass
    if conv_id:
        CONV2CHAT[int(conv_id)] = int(chat_id)

def db_set_nickname(chat_id: int, nickname: Optional[str]):
    s = db_get_session(chat_id)
    db_upsert_session(chat_id, s["contact_id"], s["conversation_id"], nickname)

def db_add_closure(chat_id: int, conversation_id: int, closed_at: int):
    with _db() as conn:
        conn.execute(
            "INSERT INTO closures(chat_id, conversation_id, closed_at) VALUES(?,?,?)",
            (chat_id, conversation_id, closed_at)
        )

def db_get_last_closure(chat_id: int) -> Optional[Tuple[int, int]]:
    with _db() as conn:
        row = conn.execute(
            "SELECT conversation_id, closed_at FROM closures WHERE chat_id=? "
            "ORDER BY closed_at DESC LIMIT 1",
            (chat_id,)
        ).fetchone()
    return (row[0], row[1]) if row else None

def db_get_chat_by_conversation(conv_id: int) -> Optional[int]:
    with _db() as conn:
        row = conn.execute(
            "SELECT chat_id FROM sessions WHERE conversation_id=?",
            (conv_id,)
        ).fetchone()
    return int(row[0]) if row and row[0] is not None else None

# ============================== MEMORY SESS ==============================

SESS: Dict[int, Dict[str, Any]] = {}
CONV2CHAT: Dict[int, int] = {}  # conv_id -> chat_id

def sess(chat_id: int) -> Dict[str, Any]:
    s = SESS.get(chat_id)
    if s:
        return s
    p = db_get_session(chat_id)
    if p.get("conversation_id"):
        try:
            CONV2CHAT[int(p["conversation_id"])] = int(chat_id)
        except Exception:
            pass
    SESS[chat_id] = {
        "nickname": p.get("nickname"),
        "awaiting_nickname": False,
        "contact_id": p.get("contact_id"),
        "conversation_id": p.get("conversation_id"),
    }
    return SESS[chat_id]

# ============================== DEDUP ==============================

class LRUSet:
    def __init__(self, capacity: int = 4096):
        self.capacity = capacity
        self._data: OrderedDict[str, None] = OrderedDict()

    def add(self, key: str) -> bool:
        if key in self._data:
            self._data.move_to_end(key)
            return False
        self._data[key] = None
        if len(self._data) > self.capacity:
            self._data.popitem(last=False)
        return True

DEDUP = LRUSet()

# ============================== CHATWOOT CLIENT ==============================

def _auth_mode() -> str:
    if CHATWOOT_API_TOKEN:
        return "api_token"
    if CW_ACCESS_TOKEN and CW_CLIENT and CW_UID:
        return "devise"
    return "none"

class ChatwootClient:
    def __init__(self, base_url: str, account_id: str):
        self.base = base_url.rstrip("/")
        self.acc = account_id

    def _headers(self) -> Dict[str, str]:
        h = {"Accept": "application/json"}
        if CHATWOOT_API_TOKEN:
            h["api_access_token"] = CHATWOOT_API_TOKEN
        elif CW_ACCESS_TOKEN and CW_CLIENT and CW_UID:
            h.update(
                {
                    "access-token": CW_ACCESS_TOKEN,
                    "client": CW_CLIENT,
                    "uid": CW_UID,
                }
            )
        return h

    def _url(self, path: str) -> str:
        return f"{self.base}/api/v1/accounts/{self.acc}{path}"

    async def get(self, path: str, **kw) -> httpx.Response:
        return await HTTP.get(self._url(path), headers=self._headers(), **kw)

    async def post(self, path: str, **kw) -> httpx.Response:
        headers = self._headers()
        if "json" in kw:
            headers["Content-Type"] = "application/json"
        return await HTTP.post(self._url(path), headers=headers, **kw)

    async def patch(self, path: str, **kw) -> httpx.Response:
        headers = self._headers()
        if "json" in kw:
            headers["Content-Type"] = "application/json"
        return await HTTP.patch(self._url(path), headers=headers, **kw)

    async def post_multipart(
        self,
        path: str,
        data: Dict[str, Any],
        files: List[Tuple[str, Tuple[str, io.BytesIO, str]]],
    ) -> httpx.Response:
        return await HTTP.post(
            self._url(path),
            headers=self._headers(),
            data=data,
            files=files,
            timeout=httpx.Timeout(60, connect=10),
        )

CW = ChatwootClient(CHATWOOT_BASE_URL, CHATWOOT_ACCOUNT_ID)

async def cw_update_contact_name(contact_id: Optional[int], name: str):
    if not contact_id:
        return
    try:
        await CW.patch(f"/contacts/{contact_id}", json={"name": name})
    except Exception:
        logger.exception("failed to patch contact name")

# ============================== TG API HELPERS ==============================

def _tg_api_url(method: str) -> str:
    return f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"

async def tg_api(method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        r = await HTTP.post(_tg_api_url(method), json=payload)
        return r.json() if r.content else {}
    except Exception:
        logger.exception("tg_api error")
        return {}

async def tg_get_file_info(file_id: str) -> Optional[dict]:
    js = await tg_api("getFile", {"file_id": file_id})
    return js.get("result") if isinstance(js, dict) else None

async def tg_get_profile_photo_file_id(user_id: int) -> Optional[str]:
    js = await tg_api("getUserProfilePhotos", {"user_id": user_id, "limit": 1})
    try:
        photos = (js or {}).get("result", {}).get("photos") or []
        if not photos:
            return None
        best = photos[0][-1]
        return best.get("file_id")
    except Exception:
        return None

def tg_file_direct_url(file_path: str) -> str:
    return f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{file_path}"

def tgfile_public_url(file_id: str, filename: Optional[str] = None) -> str:
    if FILE_PROXY_PUBLIC_BASE:
        q = f"?fn={quote(filename)}" if filename else ""
        return f"{FILE_PROXY_PUBLIC_BASE}/tgfile/{quote(file_id)}{q}"
    return ""

# ============================== CONTACT & CONV ==============================

async def _prepare_avatar_from_tg(
    tg_user: Optional[dict],
) -> Tuple[Optional[str], Optional[Tuple[str, io.BytesIO, str]]]:
    if not tg_user:
        return None, None
    uid = tg_user.get("id")
    if not isinstance(uid, int):
        return None, None
    file_id = await tg_get_profile_photo_file_id(uid)
    if not file_id:
        return None, None
    if FILE_PROXY_PUBLIC_BASE:
        return tgfile_public_url(file_id, filename="avatar.jpg"), None
    fn, buf, mime = await _download_tg_file(file_id)
    if (mime or "").startswith("image/"):
        base, ext = os.path.splitext(fn)
        if mime == "image/jpeg" and ext.lower() not in {".jpg", ".jpeg"}:
            fn = base + ".jpg"
        if mime == "image/png" and ext.lower() != ".png":
            fn = base + ".png"
    return None, (fn, buf, mime)

async def ensure_contact_and_inbox(
    inbox_id: int,
    source_id: str,
    name: str,
    email: str,
    tg_user: Optional[dict] = None,
) -> int:
    avatar_url, avatar_file = await _prepare_avatar_from_tg(tg_user)
    body = {
        "inbox_id": inbox_id,
        "source_id": source_id,
        "name": name,
        "email": email,
    }

    try:
        if avatar_url:
            resp = await CW.post("/contacts", json={**body, "avatar_url": avatar_url})
        elif avatar_file:
            resp = await CW.post_multipart(
                "/contacts", data=body, files=[("avatar", avatar_file)]
            )
        else:
            resp = await CW.post("/contacts", json=body)
    except Exception:
        logger.exception("ensure_contact POST failed")
        raise

    if resp.status_code in (200, 201):
        js = resp.json() or {}
        return int(
            js.get("id")
            or (js.get("payload") or {}).get("contact", {}).get("id")
            or 0
        )

    if resp.status_code == 422:
        s = await CW.get("/contacts/search", params={"q": email})
        payload = (s.json() or {}).get("payload") or []
        if not payload:
            raise RuntimeError("Contact exists but not found")
        cid = int(payload[0].get("id"))
        try:
            if avatar_url:
                await CW.patch(f"/contacts/{cid}", json={"avatar_url": avatar_url})
            elif avatar_file:
                await CW.patch(
                    f"/contacts/{cid}", data={}, files=[("avatar", avatar_file)]
                )
        except Exception:
            logger.exception("patch avatar failed")
        return cid

    resp.raise_for_status()
    raise RuntimeError("ensure_contact failed")

async def _ensure_open_conversation(
    inbox_id: int,
    contact_id: int,
    source_id: str,
) -> int:
    lst = await CW.get(f"/contacts/{contact_id}/conversations")
    js = lst.json() or {}
    items = js.get("payload") or js.get("data") or []
    for it in items or []:
        try:
            if (it.get("status") or "").lower() != "open":
                continue
            cur_inbox = int(
                (it.get("inbox") or {}).get("id")
                or it.get("inbox_id")
                or -1
            )
            if cur_inbox != inbox_id:
                continue
            sid = str(
                ((it.get("meta") or {}).get("sender") or {})
                .get("additional_attributes", {})
                .get("source_id")
                or (it.get("contact_inbox") or {}).get("source_id")
                or ""
            )
            if sid and sid != source_id:
                continue
            return int(it.get("id"))
        except Exception:
            continue
    resp = await CW.post(
        "/conversations",
        json={
            "source_id": source_id,
            "inbox_id": inbox_id,
            "contact_id": contact_id,
        },
    )
    resp.raise_for_status()
    return int((resp.json() or {}).get("id"))

# ============================== TG → CW (ФАЙЛЫ И СООБЩЕНИЯ) ==============================

async def _download_tg_file(file_id: str) -> Tuple[str, io.BytesIO, str]:
    info = await tg_get_file_info(file_id)
    if not info or not info.get("file_path"):
        raise RuntimeError("Telegram getFile failed")
    file_path = info["file_path"]
    url = tg_file_direct_url(file_path)
    guessed = mimetypes.guess_type(file_path)[0] or "application/octet-stream"
    filename = os.path.basename(file_path)
    buf = io.BytesIO()
    async with HTTP.stream("GET", url) as r:
        r.raise_for_status()
        async for chunk in r.aiter_bytes():
            buf.write(chunk)
    buf.seek(0)
    return filename, buf, guessed

def _guess_image_mime_from_name(
    filename: str,
    fallback: str = "application/octet-stream",
) -> str:
    mime = mimetypes.guess_type(filename or "")[0]
    if not mime:
        if filename.lower().endswith((".jpg", ".jpeg")):
            return "image/jpeg"
        if filename.lower().endswith(".png"):
            return "image/png"
        if filename.lower().endswith(".webp"):
            return "image/webp"
    return mime or fallback

async def cw_post_incoming_multipart(
    conversation_id: int,
    content: str,
    uploads: List[Tuple[str, io.BytesIO, str]],
) -> None:
    data = {
        "content": content or " ",
        "message_type": "incoming",
        "private": "false",
    }
    files = [("attachments[]", (fn, buf, ct)) for (fn, buf, ct) in uploads]
    resp = await CW.post_multipart(
        f"/conversations/{conversation_id}/messages",
        data=data,
        files=files,
    )
    if not resp.is_success:
        logger.warning(
            "CW incoming multipart failed: %s", resp.text[:400]
        )

async def post_incoming_message_to_conversation(
    conversation_id: int,
    content: str,
    attachments: Optional[List[dict]] = None,
) -> None:
    if attachments:
        files: List[Tuple[str, io.BytesIO, str]] = []
        for f in attachments:
            try:
                file_id = f.get("file_id")
                if file_id:
                    fn, buf, mime = await _download_tg_file(file_id)
                else:
                    url = f.get("url")
                    fn = f.get("file_name") or "file"
                    mime = f.get("mime") or _guess_image_mime_from_name(fn)
                    r = await HTTP.get(url)
                    r.raise_for_status()
                    buf = io.BytesIO(r.content)

                if (mime or "").startswith("image/"):
                    base, ext = os.path.splitext(fn or "image")
                    if mime == "image/jpeg" and ext.lower() not in {".jpg", ".jpeg"}:
                        fn = base + ".jpg"
                    elif mime == "image/png" and ext.lower() != ".png":
                        fn = base + ".png"
                    elif mime == "image/webp" and ext.lower() != ".webp":
                        fn = base + ".webp"

                files.append(
                    (fn or "file", buf, mime or "application/octet-stream")
                )
            except Exception:
                logger.exception("attach download failed")

        text_for_media = content if content else " "
        if files:
            await cw_post_incoming_multipart(conversation_id, text_for_media, files)
        else:
            await CW.post(
                f"/conversations/{conversation_id}/messages",
                json={
                    "content": content or "",
                    "message_type": 0,
                    "private": False,
                },
            )
        return

    resp = await CW.post(
        f"/conversations/{conversation_id}/messages",
        json={"content": content or "", "message_type": 0, "private": False},
    )
    if not resp.is_success:
        logger.warning(
            "CW incoming json failed: %s", resp.text[:400]
        )

# ============================== CHATWOOT WEBHOOK → TG ==============================

def extract_source_id(evt: Dict[str, Any]) -> Optional[str]:
    paths = [
        ["message", "conversation", "contact_inbox", "source_id"],
        ["conversation", "contact_inbox", "source_id"],
        ["message", "content_attributes", "source_id"],
        ["content_attributes", "source_id"],
        ["meta", "sender", "additional_attributes", "source_id"],
        ["additional_attributes", "source_id"],
    ]
    for path in paths:
        cur: Any = evt
        ok = True
        for k in path:
            if isinstance(cur, dict) and k in cur:
                cur = cur[k]
            else:
                ok = False
                break
        if ok and isinstance(cur, (str, int)):
            s = str(cur).strip()
            if s:
                return s
    data = evt.get("data")
    if isinstance(data, list) and data:
        return extract_source_id(data[0])
    return None

def _resolve_agent_name(evt: Dict[str, Any]) -> str:
    msg = evt.get("message") or {}
    s_top = evt.get("sender") or {}
    s_msg = msg.get("sender") or {}
    for cand in (s_msg, s_top, evt.get("user") or {}):
        name = (cand.get("available_name") or cand.get("name") or "").strip()
        if name:
            return name
    return "Поддержка"

def is_agent_outgoing(evt: Dict[str, Any]) -> bool:
    ev = (evt.get("event") or "").lower()
    if ev not in {"message_created", "message_updated"}:
        return False
    msg = evt.get("message") or {}
    nested_sender_type = (msg.get("sender_type") or "").lower()
    top_sender_type = (((evt.get("sender") or {}).get("type")) or "").lower()
    nested_message_type = msg.get("message_type")
    top_message_type = evt.get("message_type")
    if nested_sender_type == "contact" or top_sender_type == "contact":
        return False
    if nested_sender_type in {"user", "agent"}:
        return True
    if isinstance(nested_message_type, int) and nested_message_type == 1:
        return True
    if isinstance(top_message_type, int) and top_message_type == 1:
        return True
    if isinstance(top_message_type, str) and top_message_type.lower() == "outgoing":
        return True
    return False

async def _send_attachment_to_telegram(
    chat_id: int,
    att: dict,
    caption: Optional[str] = None,
    with_caption: bool = False,
) -> None:
    url = (
        att.get("data_url")
        or att.get("download_url")
        or att.get("file_url")
        or ""
    )
    if not url:
        return
    ct = (att.get("file_type") or att.get("content_type") or "").lower()
    if "image" in ct:
        payload = {"chat_id": chat_id, "photo": url}
        if with_caption and caption:
            payload["caption"] = caption
        await tg_api("sendPhoto", payload)
    else:
        payload = {"chat_id": chat_id, "document": url}
        if with_caption and caption:
            payload["caption"] = caption
        await tg_api("sendDocument", payload)

def _conv_display_id(evt: Dict[str, Any]) -> Optional[int]:
    for cand in (
        (evt.get("conversation") or {}).get("display_id"),
        evt.get("display_id"),
        ((evt.get("message") or {}).get("conversation") or {}).get("display_id"),
    ):
        if cand is not None:
            try:
                return int(cand)
            except Exception:
                continue
    return None

def _conv_id_from_event(evt: Dict[str, Any]) -> Optional[int]:
    conv = evt.get("conversation") or {}
    candidates = [
        evt.get("conversation_id"),
        evt.get("conversationId"),
        conv.get("id"),
        ((evt.get("message") or {}).get("conversation") or {}).get("id"),
        evt.get("id")
        if ((evt.get("event") or "").lower().startswith("conversation_"))
        else None,
    ]
    for cand in candidates:
        if cand is not None:
            try:
                return int(cand)
            except Exception:
                continue
    return None

def _status_changed_to_resolved(evt: Dict[str, Any]) -> bool:
    changed = evt.get("changed_attributes") or []
    try:
        if isinstance(changed, dict):
            cur = (changed.get("status") or {}).get("current_value")
            return isinstance(cur, str) and cur.lower() == "resolved"
        for item in changed:
            cur = (item.get("status") or {}).get("current_value")
            if isinstance(cur, str) and cur.lower() == "resolved":
                return True
    except Exception:
        pass
    conv = evt.get("conversation") or {}
    st = (conv.get("status") or "").lower()
    return st == "resolved"

def _get_status_now(evt: Dict[str, Any]) -> Optional[str]:
    for cand in (
        evt.get("status"),
        evt.get("current_status"),
        (evt.get("conversation") or {}).get("status"),
        ((evt.get("message") or {}).get("conversation") or {}).get("status"),
    ):
        if isinstance(cand, str) and cand:
            return cand
    changed = evt.get("changed_attributes")
    try:
        if isinstance(changed, dict):
            node = changed.get("status") or {}
            val = node.get("current_value") or node.get("to")
            if val:
                return val
        elif isinstance(changed, list):
            for it in changed:
                node = (it or {}).get("status") or {}
                val = node.get("current_value") or node.get("to")
                if val:
                    return val
    except Exception:
        pass
    return None

def _looks_like_close_activity_text(text: str) -> bool:
    t = (text or "").strip().lower()
    if not t:
        return False
    tokens = [
        "resolved the conversation",
        "marked conversation as resolved",
        "conversation was resolved",
        "closed the conversation",
        "conversation closed",
        "завершил диалог",
        "завершила диалог",
        "закрыл диалог",
        "закрыла диалог",
        "пометил разговор как решённый",
        "пометила разговор как решённый",
        "разговор помечен как решённый",
        "диалог завершён",
        "диалог завершен",
    ]
    return any(s in t for s in tokens)

async def _resolve_chat_id(
    conv_id: Optional[int],
    source_id_hint: Optional[str],
) -> Optional[int]:
    if source_id_hint and source_id_hint.isdigit():
        return int(source_id_hint)
    if not conv_id:
        return None
    cid = CONV2CHAT.get(int(conv_id))
    if cid:
        return int(cid)
    cid = db_get_chat_by_conversation(int(conv_id))
    if cid:
        CONV2CHAT[int(conv_id)] = int(cid)
        return int(cid)
    try:
        resp = await CW.get(f"/conversations/{int(conv_id)}")
        if resp.is_success:
            j = resp.json() or {}
            src = str(
                ((j.get("conversation") or {}).get("contact_inbox") or {}).get(
                    "source_id"
                )
                or (j.get("contact_inbox") or {}).get("source_id")
                or (((j.get("meta") or {}).get("sender") or {}).get(
                    "additional_attributes"
                )
                    or {}).get("source_id")
                or ""
            ).strip()
            if src and src.isdigit():
                return int(src)
    except Exception:
        logger.exception("resolve chat_id via API failed")
    return None

async def _notify_and_close(chat_id: int, conv_id: int, ticket_no: Optional[int]):
    if not DEDUP.add(f"close-notify:{conv_id}"):
        return
    s = sess(chat_id)
    s["conversation_id"] = None
    db_set_conv(chat_id, None)
    db_add_closure(chat_id, conv_id, int(time.time()))
    try:
        await BOT.send_message(
            chat_id,
            f"🔒 <b>Тикет #{ticket_no or conv_id} закрыт</b>\n"
            f"Если появятся вопросы — нажмите «🆕 Новый тикет» и начните новый диалог.",
        )
    except Exception:
        logger.exception("send close notify failed")

async def handle_chatwoot_event(
    evt: Dict[str, Any],
    dedup_hint: Optional[str] = None,
) -> None:
    if "id" in evt:
        key = f"evt:{evt['id']}"
    elif "message" in evt and isinstance(evt["message"], dict):
        key = f"msg:{evt['message'].get('id')}:{evt.get('event')}"
    else:
        try:
            key = f"raw:{hash(json.dumps(evt, sort_keys=True))}"
        except Exception:
            key = f"raw:{time.time_ns()}"
    if dedup_hint:
        key = f"{key}:{dedup_hint}"
    if not DEDUP.add(key):
        logger.info("Duplicate webhook skipped: %s", key)
        return

    ev = (evt.get("event") or "").lower()
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            "CW webhook: event=%s, status_now=%s, conv_id=%s, display_id=%s",
            ev,
            _get_status_now(evt),
            _conv_id_from_event(evt),
            _conv_display_id(evt),
        )

    # 1) Сообщения агента → TG
    if ev in {"message_created", "message_updated"} and is_agent_outgoing(evt):
        source_id = extract_source_id(evt) or ""
        if not source_id.isdigit():
            return
        chat_id = int(source_id)
        agent = _resolve_agent_name(evt)
        prefix = f"👩‍💻 Агент {agent}:"
        message = evt.get("message") or {}
        content = (
            evt.get("content")
            or message.get("content")
            or (
                message.get("processed_message_content")
                if isinstance(message.get("processed_message_content"), str)
                else ""
            )
            or ""
        ).strip()
        raw_atts = evt.get("attachments") or message.get("attachments") or []
        if content:
            try:
                await BOT.send_message(chat_id, f"{prefix}\n{content}")
            except TelegramBadRequest:
                await tg_api(
                    "sendMessage",
                    {"chat_id": chat_id, "text": f"{prefix}\n{content}"},
                )
        if raw_atts:
            for i, att in enumerate(raw_atts):
                try:
                    await _send_attachment_to_telegram(
                        chat_id,
                        att,
                        caption=prefix,
                        with_caption=(i == 0 and not content),
                    )
                except Exception:
                    logger.exception("send attachment TG failed")
        return

    # 2) Изменение статуса беседы → уведомление о закрытии
    if ev in {"conversation_status_changed", "conversation_updated", "conversation_resolved"}:
        st_now = (_get_status_now(evt) or "").lower()
        if not st_now and ev == "conversation_updated" and _status_changed_to_resolved(evt):
            st_now = "resolved"
        if st_now == "resolved":
            source_id = extract_source_id(evt)
            conv_id = _conv_id_from_event(evt)
            ticket_no = _conv_display_id(evt) or conv_id
            chat_id = await _resolve_chat_id(conv_id, source_id)
            if chat_id and conv_id:
                await _notify_and_close(chat_id, int(conv_id), ticket_no)
            else:
                logger.warning(
                    "cannot resolve chat_id for closure: ev=%s conv_id=%s src=%s",
                    ev,
                    conv_id,
                    source_id,
                )
            return

    # 3) Activity-сообщение закрытия
    if ev == "message_created":
        msg = evt.get("message") or {}
        content = (evt.get("content") or msg.get("content") or "") or ""
        status_now = (_get_status_now(evt) or "").lower()
        if status_now == "resolved" or _looks_like_close_activity_text(content):
            source_id = extract_source_id(evt)
            conv_id = _conv_id_from_event(evt)
            ticket_no = _conv_display_id(evt) or conv_id
            chat_id = await _resolve_chat_id(conv_id, source_id)
            if chat_id and conv_id:
                await _notify_and_close(chat_id, int(conv_id), ticket_no)
            else:
                logger.warning(
                    "cannot resolve chat_id from activity close: conv_id=%s src=%s",
                    conv_id,
                    source_id,
                )
            return

# ============================== FASTAPI APP ==============================

app = FastAPI(title="Telegram ↔ Chatwoot bridge", version="5.5")
security = HTTPBasic()

def require_agent(credentials: HTTPBasicCredentials = Depends(security)):
    if not AGENT_USER:  # dev
        return True
    correct = (
        credentials is not None
        and hmac.compare_digest(credentials.username, AGENT_USER)
        and hmac.compare_digest(credentials.password, AGENT_PASS)
    )
    if not correct:
        raise HTTPException(
            status_code=401,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
        )
    return True

@app.on_event("startup")
async def _startup():
    try:
        info = await BOT.get_webhook_info()
        url = (info.url or "").rstrip("/")
        if TELEGRAM_WEBHOOK_PUBLIC_URL and url != f"{TELEGRAM_WEBHOOK_PUBLIC_URL}/telegram/webhook":
            await BOT.set_webhook(
                url=f"{TELEGRAM_WEBHOOK_PUBLIC_URL}/telegram/webhook",
                # секрет выключен
                secret_token=None,
                allowed_updates=TELEGRAM_ALLOWED_UPDATES or None,
                drop_pending_updates=False,
                max_connections=40,
            )
            logger.info(
                "Webhook установлен: %s",
                f"{TELEGRAM_WEBHOOK_PUBLIC_URL}/telegram/webhook",
            )
        else:
            logger.info(
                "Webhook уже установлен: %s (pending=%s)",
                url or "-",
                info.pending_update_count,
            )
    except Exception as e:
        logger.warning("Не удалось проверить/установить webhook: %s", e)

@app.on_event("shutdown")
async def _shutdown_event():
    try:
        await HTTP.aclose()
    except Exception:
        pass
    try:
        await BOT.session.close()
    except Exception:
        pass

@app.get("/")
async def root():
    return {
        "ok": True,
        "name": "telegram-chatwoot-bridge",
        "ver": "5.5",
    }

@app.get("/healthz")
async def healthz():
    return {
        "ok": True,
        "auth_mode": _auth_mode(),
        "inbox_id": CHATWOOT_INBOX_ID,
        "db": DB_PATH,
        "ts": int(time.time()),
    }

# -------- Telegram webhook endpoint --------

def verify_telegram_secret(headers: Dict[str, str]) -> bool:
    # Полностью отключили проверку секрета Telegram
    return True

@app.post("/telegram/webhook")
async def telegram_webhook(request: Request):
    if not verify_telegram_secret(request.headers):
        return JSONResponse({"status": "unauthorized"}, status_code=401)
    data = await request.json()
    try:
        update = Update.model_validate(data)
    except Exception:
        logger.exception(
            "Update parse failed: %s",
            (
                json.dumps(data, ensure_ascii=False)[:600]
                if isinstance(data, dict)
                else str(data)
            ),
        )
        return JSONResponse({"ok": True})
    try:
        await DP.feed_update(BOT, update)
    except Exception:
        logger.exception("DP.feed_update error")
    return JSONResponse({"ok": True})

# -------- Chatwoot webhook endpoint (+ алиас /webhook) --------

def load_chatwoot_json_from_raw(
    raw: bytes,
    content_encoding: Optional[str],
) -> Dict[str, Any]:
    enc = (content_encoding or "").lower().strip()
    try:
        if enc == "gzip":
            raw = gzip.decompress(raw)
        elif enc in ("deflate", "zlib"):
            try:
                raw = zlib.decompress(raw)
            except zlib.error:
                raw = zlib.decompress(raw, -zlib.MAX_WBITS)
    except Exception:
        logger.exception("decompress failed")
    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception:
        logger.exception("parse json failed")
        return {}

def verify_chatwoot_webhook_from_raw(
    raw: bytes,
    headers: Dict[str, str],
) -> bool:
    if USE_HMAC:
        if not CHATWOOT_WEBHOOK_SECRET:
            return False
        got = (
            headers.get("X-Chatwoot-Webhook-Signature", "")
            or headers.get("X-Chatwoot-Signature", "")
        )
        want = hmac.new(
            CHATWOOT_WEBHOOK_SECRET.encode(),
            raw,
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(got, want)
    expected = CHATWOOT_WEBHOOK_TOKEN
    if not expected:
        return True
    got = headers.get("X-Webhook-Token", "")
    return got == expected

@app.post("/chatwoot/webhook")
async def chatwoot_webhook(request: Request):
    raw = await request.body()
    headers = {k: v for k, v in request.headers.items()}
    if not verify_chatwoot_webhook_from_raw(raw, headers):
        return JSONResponse({"status": "unauthorized"}, status_code=401)
    evt = load_chatwoot_json_from_raw(raw, headers.get("Content-Encoding"))
    try:
        req_id = (
            headers.get("X-Request-Id")
            or headers.get("Fly-Request-Id")
            or str(time.time_ns())
        )
        await handle_chatwoot_event(evt, dedup_hint=req_id)
    except Exception:
        logger.exception("webhook handle error")
    return JSONResponse({"ok": True})

@app.post("/webhook")
async def chatwoot_webhook_alias(request: Request):
    return await chatwoot_webhook(request)

# -------- Публичный прокси для файлов Telegram (опционально) --------

@app.get("/tgfile/{file_id}")
async def tgfile(file_id: str, fn: Optional[str] = None):
    info = await tg_get_file_info(file_id)
    if not info or not info.get("file_path"):
        return PlainTextResponse("Not found", status_code=404)
    file_path = info["file_path"]
    upstream = tg_file_direct_url(file_path)
    guessed = mimetypes.guess_type(file_path)[0] or "application/octet-stream"
    filename = fn or os.path.basename(file_path)

    async def gen():
        async with HTTP.stream("GET", upstream) as r:
            r.raise_for_status()
            async for chunk in r.aiter_bytes():
                yield chunk

    headers = {
        "Content-Type": guessed,
        "Content-Disposition": "inline; filename*=UTF-8''" + quote(filename),
        "Cache-Control": "public, max-age=31536000, immutable",
    }
    return StreamingResponse(gen(), headers=headers)

# ============================== UI / KEYBOARDS ==============================

CB_CREATE = "create_ticket"
CB_EDIT_NICK = "edit_nick"
CB_PROMPT_NICK = "prompt_nick"
CB_USE_USERNAME = "use_username"
CB_HELP = "help"
CB_ATTACH_HOWTO = "attach_howto"
CB_STATUS = "status"

def tg_inline_kb(rows: List[List[Tuple[str, str]]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=t, callback_data=d) for (t, d) in row]
            for row in rows
        ]
    )

def kb_need_nick(username: Optional[str]) -> InlineKeyboardMarkup:
    rows: List[List[Tuple[str, str]]] = []
    if username:
        rows.append([(f"🔁 Использовать @{username}", CB_USE_USERNAME)])
    rows.append([("✏️ Ввести никнейм вручную", CB_PROMPT_NICK)])
    rows.append([("❓ Помощь", CB_HELP)])
    return tg_inline_kb(rows)

def kb_main(chat_id: int) -> InlineKeyboardMarkup:
    s = sess(chat_id)
    conv = s.get("conversation_id")
    rows: List[List[Tuple[str, str]]] = []
    if conv:
        rows.append([(f"💬 Диалог #{conv}", CB_STATUS)])
    else:
        rows.append([("🆕 Новый тикет", CB_CREATE)])
    rows.append([("📎 Как прислать файл/скрин", CB_ATTACH_HOWTO)])
    rows.append([("📊 Статус", CB_STATUS), ("✏️ Никнейм", CB_EDIT_NICK)])
    rows.append([("❓ Помощь", CB_HELP)])
    return tg_inline_kb(rows)

async def _send_menu(chat_id: int, username: Optional[str]):
    s = sess(chat_id)
    if s.get("nickname"):
        await BOT.send_message(
            chat_id,
            "📌 <b>Меню</b>\n"
            "— Создайте тикет или продолжайте текущий диалог.\n"
            "— Можно отправлять текст, фото, видео и файлы (скриншоты рендерятся как изображения).",
            reply_markup=kb_main(chat_id),
        )
    else:
        await BOT.send_message(
            chat_id,
            "👋 <b>Добро пожаловать!</b>\n"
            "Перед началом укажите никнейм — как к вам обращаться.",
            reply_markup=kb_need_nick(username),
        )

async def _start_flow(chat_id: int, from_user: dict):
    s = sess(chat_id)
    if s.get("nickname"):
        await BOT.send_message(
            chat_id,
            "✨ <b>Привет!</b> Чем можем помочь?",
            reply_markup=kb_main(chat_id),
        )
    else:
        s["awaiting_nickname"] = True
        await BOT.send_message(
            chat_id,
            "👋 <b>Привет!</b> Укажите никнейм — как к вам обращаться.",
            reply_markup=kb_need_nick((from_user.get("username") or "").strip()),
        )

async def _set_nickname(chat_id: int, from_user: dict, nick: str):
    nick = nick.strip()[:64].strip("@ ")
    if not nick:
        await BOT.send_message(
            chat_id,
            "⚠️ Никнейм не распознан. Отправьте один текст — желаемый ник.",
        )
        return
    s = sess(chat_id)
    s["nickname"] = nick
    s["awaiting_nickname"] = False
    db_set_nickname(chat_id, nick)
    if s.get("contact_id"):
        await cw_update_contact_name(s["contact_id"], nick)
    await BOT.send_message(
        chat_id,
        f"✅ Готово! Ваш никнейм: <b>@{nick}</b>",
    )
    await _send_menu(chat_id, (from_user.get("username") or "").strip())

# ============================== СБОР ВЛОЖЕНИЙ ИЗ TG ==============================

def _push(
    files: List[dict],
    file_id: str,
    fn: Optional[str],
    mime: Optional[str],
):
    if not file_id:
        return
    url = tgfile_public_url(file_id, filename=fn)
    files.append(
        {
            "url": url,
            "file_id": file_id,
            "file_name": fn or "file",
            "mime": mime or _guess_image_mime_from_name(fn or "file"),
        }
    )

def _collect_tg_attachments(msg: dict) -> Tuple[str, List[dict]]:
    text = (msg.get("caption") or msg.get("text") or "").strip()
    files: List[dict] = []
    if msg.get("photo"):
        best = msg["photo"][-1]
        _push(files, best.get("file_id"), "photo.jpg", "image/jpeg")
    if msg.get("document"):
        d = msg["document"]
        fname = d.get("file_name") or "document"
        mime = d.get("mime_type") or _guess_image_mime_from_name(fname)
        if (mime or "").startswith("image/"):
            base, ext = os.path.splitext(fname)
            if mime == "image/jpeg" and ext.lower() not in {".jpg", ".jpeg"}:
                fname = base + ".jpg"
            if mime == "image/png" and ext.lower() != ".png":
                fname = base + ".png"
            if mime == "image/webp" and ext.lower() != ".webp":
                fname = base + ".webp"
        _push(files, d.get("file_id"), fname, mime)
    if msg.get("video"):
        v = msg["video"]
        _push(files, v.get("file_id"), "video.mp4", v.get("mime_type") or "video/mp4")
    if msg.get("audio"):
        a = msg["audio"]
        _push(
            files,
            a.get("file_id"),
            a.get("file_name") or "audio.mp3",
            a.get("mime_type") or "audio/mpeg",
        )
    if msg.get("voice"):
        vc = msg["voice"]
        _push(
            files,
            vc.get("file_id"),
            "voice.ogg",
            vc.get("mime_type") or "audio/ogg",
        )
    if msg.get("sticker"):
        st = msg["sticker"]
        mime = "image/webp" if (st.get("is_video") is False) else "video/webm"
        name = "sticker.webp" if (st.get("is_video") is False) else "sticker.webm"
        _push(files, st.get("file_id"), name, mime)
    return text, files

# ============================== ХЕНДЛЕРЫ TG ==============================

@ROUTER.message(F.text.startswith("/start"))
async def h_start(message: Message):
    chat_id = message.chat.id
    from_user = message.from_user.model_dump() if message.from_user else {}
    await _start_flow(chat_id, from_user)
    # prewarm contact (+ аватар)
    try:
        s = sess(chat_id)
        name_hint = (
            s.get("nickname")
            or (from_user.get("username") or "")
            or (from_user.get("first_name") or "")
            or str(chat_id)
        )
        contact_id = await ensure_contact_and_inbox(
            CHATWOOT_INBOX_ID,
            str(chat_id),
            name_hint,
            f"{chat_id}@telegram",
            tg_user=from_user,
        )
        if contact_id and not s.get("contact_id"):
            s["contact_id"] = contact_id
            db_upsert_session(
                chat_id,
                contact_id,
                s.get("conversation_id"),
                s.get("nickname"),
            )
    except Exception:
        logger.exception("ensure_contact on /start failed")

@ROUTER.message(F.text.startswith("/nick"))
async def h_nick(message: Message):
    chat_id = message.chat.id
    from_user = message.from_user.model_dump() if message.from_user else {}
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) == 2 and parts[1].strip():
        await _set_nickname(chat_id, from_user, parts[1])
    else:
        s = sess(chat_id)
        s["awaiting_nickname"] = True
        await BOT.send_message(
            chat_id,
            "✏️ Отправьте новый никнейм одним сообщением.",
        )

@ROUTER.message(F.text.startswith("/status"))
async def h_status(message: Message):
    chat_id = message.chat.id
    await _send_status(chat_id)

async def _send_status(chat_id: int):
    s = sess(chat_id)
    conv = s.get("conversation_id")
    last = db_get_last_closure(chat_id)
    if last:
        last_conv, last_ts = last
        ts = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(last_ts))
        last_line = f"Последний закрытый тикет: #{last_conv} в {ts}."
    else:
        last_line = "Закрытых тикетов пока нет."
    current_line = f"Текущий тикет: {('#'+str(conv)) if conv else 'нет (ещё не открыт)'}"
    await BOT.send_message(
        chat_id,
        f"📊 <b>Статус</b>\n{current_line}\n{last_line}",
        reply_markup=kb_main(chat_id),
    )

@ROUTER.callback_query(F.data == CB_USE_USERNAME)
async def cb_use_username(c: CallbackQuery):
    chat = c.message.chat if c.message else None
    if not chat:
        return await c.answer()
    chat_id = chat.id
    from_user = c.from_user.model_dump() if c.from_user else {}
    username = (c.from_user.username or "").strip() if c.from_user else ""
    s = sess(chat_id)
    if username:
        await _set_nickname(chat_id, from_user, username)
    else:
        s["awaiting_nickname"] = True
        await BOT.send_message(
            chat_id,
            "У вас нет username в Telegram. Отправьте желаемый ник одним сообщением.",
        )
    await c.answer()

@ROUTER.callback_query(F.data.in_({CB_PROMPT_NICK, CB_EDIT_NICK}))
async def cb_prompt_nick(c: CallbackQuery):
    chat = c.message.chat if c.message else None
    if not chat:
        return await c.answer()
    s = sess(chat.id)
    s["awaiting_nickname"] = True
    await BOT.send_message(
        chat.id,
        "✏️ Отправьте новый никнейм одним сообщением.",
    )
    await c.answer()

@ROUTER.callback_query(F.data == CB_CREATE)
async def cb_create_ticket(c: CallbackQuery):
    chat = c.message.chat if c.message else None
    if not chat:
        return await c.answer()
    chat_id = chat.id
    from_user = c.from_user.model_dump() if c.from_user else {}
    s = sess(chat_id)
    username = (c.from_user.username or "").strip() if c.from_user else ""
    if not s.get("nickname"):
        s["awaiting_nickname"] = True
        await BOT.send_message(
            chat_id,
            "Сначала задайте никнейм.",
            reply_markup=kb_need_nick(username),
        )
        return await c.answer()
    try:
        contact_id = s.get("contact_id")
        if not contact_id:
            display_name = s.get("nickname") or username or str(chat_id)
            contact_id = await ensure_contact_and_inbox(
                CHATWOOT_INBOX_ID,
                str(chat_id),
                display_name,
                f"{chat_id}@telegram",
                tg_user=from_user,
            )
            s["contact_id"] = contact_id
            db_upsert_session(
                chat_id,
                contact_id,
                s.get("conversation_id"),
                s.get("nickname"),
            )
        conv_id = await _ensure_open_conversation(
            CHATWOOT_INBOX_ID,
            int(contact_id),
            str(chat_id),
        )
        s["conversation_id"] = conv_id
        db_set_conv(chat_id, conv_id)
        await BOT.send_message(
            chat_id,
            f"✅ <b>Тикет открыт: #{conv_id}</b>\n"
            "Пишите сообщение или прикрепляйте файлы.\n"
            "Подсказка по файлам — кнопка ниже 👇",
            reply_markup=kb_main(chat_id),
        )
    except Exception:
        logger.exception("create via callback failed")
        await BOT.send_message(
            chat_id,
            "❌ Не удалось открыть обращение. Попробуйте позже.",
        )
    await c.answer()

@ROUTER.callback_query(F.data == CB_ATTACH_HOWTO)
async def cb_attach_howto(c: CallbackQuery):
    chat = c.message.chat if c.message else None
    if not chat:
        return await c.answer()
    text = (
        "📎 <b>Как прислать файл/скрин</b>\n"
        "• Нажмите скрепку в Telegram.\n"
        "• Для <b>скриншотов</b> предпочтительно «Фото/Видео» — тогда увидим превью.\n"
        "• Можно отправлять как файл (PDF/DOC/ZIP и т.п.).\n"
        "• Максимальный размер — по правилам Telegram."
    )
    await BOT.send_message(chat.id, text, reply_markup=kb_main(chat.id))
    await c.answer()

@ROUTER.callback_query(F.data == CB_HELP)
async def cb_help(c: CallbackQuery):
    chat = c.message.chat if c.message else None
    if not chat:
        return await c.answer()
    text = (
        "❓ <b>Помощь</b>\n"
        "— <b>🆕 Новый тикет</b> — создаёт обращение в поддержку.\n"
        "— <b>📎 Как прислать файл/скрин</b> — подсказки по вложениям.\n"
        "— <b>📊 Статус</b> — показывает текущий диалог и последний закрытый тикет.\n"
        "— <b>✏️ Никнейм</b> — как к вам обращаться в чате.\n"
        "Мы отвечаем прямо здесь, сообщения от агента помечены «👩‍💻 Агент …»."
    )
    await BOT.send_message(chat.id, text, reply_markup=kb_main(chat.id))
    await c.answer()

@ROUTER.callback_query(F.data == CB_STATUS)
async def cb_status(c: CallbackQuery):
    chat = c.message.chat if c.message else None
    if not chat:
        return await c.answer()
    await _send_status(chat.id)
    await c.answer()

@ROUTER.message()
async def h_fallback(message: Message):
    chat_id = message.chat.id
    from_user = message.from_user.model_dump() if message.from_user else {}
    text = (message.text or "").strip()
    s = sess(chat_id)

    if s.get("awaiting_nickname"):
        if text:
            await _set_nickname(chat_id, from_user, text)
            try:
                await BOT.delete_message(chat_id, message.message_id)
            except Exception:
                pass
        else:
            await BOT.send_message(
                chat_id,
                "⚠️ Никнейм должен быть текстом.",
            )
        return

    if not s.get("nickname"):
        username = (from_user.get("username") or "").strip()
        await BOT.send_message(
            chat_id,
            "Пожалуйста, укажите никнейм.",
            reply_markup=kb_need_nick(username),
        )
        return

    if s.get("conversation_id"):
        try:
            caption_or_text, files = _collect_tg_attachments(message.model_dump())
            content = caption_or_text or text or ""
            await post_incoming_message_to_conversation(
                int(s["conversation_id"]),
                content,
                attachments=files if files else None,
            )
        except Exception:
            logger.exception("forward user message failed")
            await BOT.send_message(
                chat_id,
                "❌ Не удалось отправить сообщение. Попробуйте ещё раз.",
            )
        return

    await BOT.send_message(
        chat_id,
        "Чтобы начать переписку с поддержкой — создайте тикет.",
        reply_markup=kb_main(chat_id),
    )

# ============================== MAIN ==============================

if __name__ == "__main__":
    import uvicorn

    logger.info("Starting app on %s:%s", APP_HOST, APP_PORT)
    uvicorn.run("webhook_server:app", host=APP_HOST, port=APP_PORT, reload=False)
