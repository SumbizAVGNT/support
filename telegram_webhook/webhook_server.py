#!/usr/bin/env python3
"""Telegram <-> Chatwoot bridge (FastAPI + aiogram v3) — v6.0 refactored."""

from __future__ import annotations

import gzip
import io
import json
import logging
import mimetypes
import os
import time
import zlib
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import hmac
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

from config import (
    APP_HOST, APP_PORT, LOG_LEVEL, TELEGRAM_BOT_TOKEN,
    TELEGRAM_WEBHOOK_PUBLIC_URL, TELEGRAM_ALLOWED_UPDATES,
    CHATWOOT_INBOX_ID, AGENT_USER, AGENT_PASS,
    FILE_PROXY_PUBLIC_BASE,
)
from database import (
    init_db, get_session, upsert_session, set_conversation,
    set_nickname, add_closure, get_last_closure, get_chat_by_conversation,
)
from dedupe import DEDUP
from security import verify_telegram_secret, verify_chatwoot_webhook
import chatwoot as cw
from telegram import (
    tg_api, tg_get_file_info, tg_get_profile_photo_file_id,
    tg_file_direct_url, tgfile_public_url, guess_image_mime,
)

# ============================== LOGGING ==============================

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("tg-cw-bridge")

# ============================== AIOGRAM ==============================

try:
    from aiogram.client.default import DefaultBotProperties
    BOT = Bot(token=TELEGRAM_BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
except Exception:
    BOT = Bot(token=TELEGRAM_BOT_TOKEN, parse_mode=ParseMode.HTML)

DP = Dispatcher()
ROUTER = Router()
DP.include_router(ROUTER)

# ============================== INIT DB ==============================

init_db()

# ============================== MEMORY SESSION ==============================

SESS: Dict[int, Dict[str, Any]] = {}
CONV2CHAT: Dict[int, int] = {}


def sess(chat_id: int) -> Dict[str, Any]:
    s = SESS.get(chat_id)
    if s:
        return s
    p = get_session(chat_id)
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

# ============================== TG FILE HELPERS ==============================


async def _download_tg_file(file_id: str) -> Tuple[str, io.BytesIO, str]:
    info = await tg_get_file_info(file_id)
    if not info or not info.get("file_path"):
        raise RuntimeError("Telegram getFile failed")
    file_path = info["file_path"]
    url = tg_file_direct_url(file_path)
    guessed = mimetypes.guess_type(file_path)[0] or "application/octet-stream"
    filename = os.path.basename(file_path)
    buf = io.BytesIO()
    async with cw.HTTP.stream("GET", url) as r:
        r.raise_for_status()
        async for chunk in r.aiter_bytes():
            buf.write(chunk)
    buf.seek(0)
    return filename, buf, guessed


def _fix_image_extension(fn: str, mime: str) -> str:
    if not (mime or "").startswith("image/"):
        return fn
    base, ext = os.path.splitext(fn or "image")
    if mime == "image/jpeg" and ext.lower() not in {".jpg", ".jpeg"}:
        return base + ".jpg"
    if mime == "image/png" and ext.lower() != ".png":
        return base + ".png"
    if mime == "image/webp" and ext.lower() != ".webp":
        return base + ".webp"
    return fn

# ============================== CONTACT & CONV ==============================


async def _prepare_avatar_from_tg(tg_user: Optional[dict]) -> Tuple[Optional[str], Optional[Tuple[str, io.BytesIO, str]]]:
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
    fn = _fix_image_extension(fn, mime)
    return None, (fn, buf, mime)


async def ensure_contact_and_inbox(
    inbox_id: int, source_id: str, name: str, email: str, tg_user: Optional[dict] = None,
) -> int:
    avatar_url, avatar_file = await _prepare_avatar_from_tg(tg_user)
    body = {"inbox_id": inbox_id, "source_id": source_id, "name": name, "email": email}

    try:
        if avatar_url:
            resp = await cw.post("/contacts", json={**body, "avatar_url": avatar_url})
        elif avatar_file:
            resp = await cw.post_multipart("/contacts", data=body, files=[("avatar", avatar_file)])
        else:
            resp = await cw.post("/contacts", json=body)
    except Exception:
        logger.exception("ensure_contact POST failed")
        raise

    if resp.status_code in (200, 201):
        js = resp.json() or {}
        return int(js.get("id") or (js.get("payload") or {}).get("contact", {}).get("id") or 0)

    if resp.status_code == 422:
        s = await cw.get("/contacts/search", params={"q": email})
        payload = (s.json() or {}).get("payload") or []
        if not payload:
            raise RuntimeError("Contact exists but not found")
        cid = int(payload[0].get("id"))
        try:
            if avatar_url:
                await cw.patch(f"/contacts/{cid}", json={"avatar_url": avatar_url})
            elif avatar_file:
                await cw.patch(f"/contacts/{cid}", data={}, files=[("avatar", avatar_file)])
        except Exception:
            logger.exception("patch avatar failed")
        return cid

    resp.raise_for_status()
    raise RuntimeError("ensure_contact failed")


async def _ensure_open_conversation(inbox_id: int, contact_id: int, source_id: str) -> int:
    lst = await cw.get(f"/contacts/{contact_id}/conversations")
    js = lst.json() or {}
    items = js.get("payload") or js.get("data") or []
    for it in items or []:
        try:
            if (it.get("status") or "").lower() != "open":
                continue
            cur_inbox = int((it.get("inbox") or {}).get("id") or it.get("inbox_id") or -1)
            if cur_inbox != inbox_id:
                continue
            return int(it.get("id"))
        except Exception:
            continue
    resp = await cw.post("/conversations", json={"source_id": source_id, "inbox_id": inbox_id, "contact_id": contact_id})
    resp.raise_for_status()
    return int((resp.json() or {}).get("id"))

# ============================== TG -> CW MESSAGES ==============================


async def cw_post_incoming_multipart(conversation_id: int, content: str, uploads: List[Tuple[str, io.BytesIO, str]]):
    data = {"content": content or " ", "message_type": "incoming", "private": "false"}
    files = [("attachments[]", (fn, buf, ct)) for (fn, buf, ct) in uploads]
    resp = await cw.post_multipart(f"/conversations/{conversation_id}/messages", data=data, files=files)
    if not resp.is_success:
        logger.warning("CW incoming multipart failed: %s", resp.text[:400])


async def post_incoming_message(conversation_id: int, content: str, attachments: Optional[List[dict]] = None):
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
                    mime = f.get("mime") or guess_image_mime(fn)
                    r = await cw.HTTP.get(url)
                    r.raise_for_status()
                    buf = io.BytesIO(r.content)
                fn = _fix_image_extension(fn, mime)
                files.append((fn or "file", buf, mime or "application/octet-stream"))
            except Exception:
                logger.exception("attach download failed")

        if files:
            await cw_post_incoming_multipart(conversation_id, content or " ", files)
        else:
            await cw.post(f"/conversations/{conversation_id}/messages", json={"content": content or "", "message_type": 0, "private": False})
        return

    resp = await cw.post(f"/conversations/{conversation_id}/messages", json={"content": content or "", "message_type": 0, "private": False})
    if not resp.is_success:
        logger.warning("CW incoming json failed: %s", resp.text[:400])

# ============================== CHATWOOT WEBHOOK -> TG ==============================


def _extract_source_id(evt: Dict[str, Any]) -> Optional[str]:
    paths = [
        ["message", "conversation", "contact_inbox", "source_id"],
        ["conversation", "contact_inbox", "source_id"],
        ["message", "content_attributes", "source_id"],
        ["meta", "sender", "additional_attributes", "source_id"],
    ]
    for path in paths:
        cur: Any = evt
        for k in path:
            if isinstance(cur, dict) and k in cur:
                cur = cur[k]
            else:
                cur = None
                break
        if cur is not None and isinstance(cur, (str, int)):
            s = str(cur).strip()
            if s:
                return s
    return None


def _resolve_agent_name(evt: Dict[str, Any]) -> str:
    msg = evt.get("message") or {}
    for cand in (msg.get("sender") or {}, evt.get("sender") or {}, evt.get("user") or {}):
        name = (cand.get("available_name") or cand.get("name") or "").strip()
        if name:
            return name
    return "Поддержка"


def _is_agent_outgoing(evt: Dict[str, Any]) -> bool:
    ev = (evt.get("event") or "").lower()
    if ev not in {"message_created", "message_updated"}:
        return False
    msg = evt.get("message") or {}
    sender_type = (msg.get("sender_type") or "").lower()
    top_sender = (((evt.get("sender") or {}).get("type")) or "").lower()
    if sender_type == "contact" or top_sender == "contact":
        return False
    if sender_type in {"user", "agent"}:
        return True
    mt = msg.get("message_type") or evt.get("message_type")
    if isinstance(mt, int) and mt == 1:
        return True
    if isinstance(mt, str) and mt.lower() == "outgoing":
        return True
    return False


async def _download_chatwoot_file(url: str, max_retries: int = 4) -> Tuple[bytes, str]:
    """Download a file from Chatwoot with retry + exponential backoff."""
    import asyncio
    last_exc: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            resp = await cw.HTTP.get(url, follow_redirects=True)
            if resp.status_code == 404 and attempt < max_retries - 1:
                wait = 2 ** attempt
                logger.warning("Chatwoot file 404, retry %d in %ds: %s", attempt + 1, wait, url)
                await asyncio.sleep(wait)
                continue
            resp.raise_for_status()
            ct = resp.headers.get("content-type", "application/octet-stream")
            return resp.content, ct
        except Exception as exc:
            last_exc = exc
            if attempt < max_retries - 1:
                wait = 2 ** attempt
                logger.warning("Chatwoot file download error (attempt %d), retry in %ds: %s", attempt + 1, wait, exc)
                await asyncio.sleep(wait)
            else:
                raise
    raise last_exc or RuntimeError("download failed")


async def _send_attachment_to_telegram(chat_id: int, att: dict, caption: Optional[str] = None, with_caption: bool = False):
    url = att.get("data_url") or att.get("download_url") or att.get("file_url") or ""
    if not url:
        return
    ct = (att.get("file_type") or att.get("content_type") or "").lower()

    # Download the file first (Chatwoot ActiveStorage URLs require following redirects
    # and may 404 briefly due to race conditions)
    try:
        file_data, content_type = await _download_chatwoot_file(url)
    except Exception:
        logger.exception("Failed to download attachment from Chatwoot: %s", url)
        return

    filename = os.path.basename(url.split("?")[0].split("/")[-1]) or "file"

    data = {"chat_id": str(chat_id)}
    if with_caption and caption:
        data["caption"] = caption

    if "image" in ct:
        field_name = "photo"
        method = "sendPhoto"
    else:
        field_name = "document"
        method = "sendDocument"

    from telegram import TG_API_BASE
    files = {field_name: (filename, file_data, content_type)}
    try:
        r = await cw.HTTP.post(f"{TG_API_BASE}/{method}", data=data, files=files)
        if r.status_code != 200:
            logger.error("Telegram %s failed %d: %s", method, r.status_code, r.text[:500])
    except Exception:
        logger.exception("Telegram upload %s failed", method)


def _conv_id_from_event(evt: Dict[str, Any]) -> Optional[int]:
    conv = evt.get("conversation") or {}
    for cand in [evt.get("conversation_id"), conv.get("id"), ((evt.get("message") or {}).get("conversation") or {}).get("id")]:
        if cand is not None:
            try:
                return int(cand)
            except Exception:
                continue
    ev = (evt.get("event") or "").lower()
    if ev.startswith("conversation_") and evt.get("id") is not None:
        try:
            return int(evt["id"])
        except Exception:
            pass
    return None


def _conv_display_id(evt: Dict[str, Any]) -> Optional[int]:
    for cand in [
        (evt.get("conversation") or {}).get("display_id"),
        evt.get("display_id"),
        ((evt.get("message") or {}).get("conversation") or {}).get("display_id"),
    ]:
        if cand is not None:
            try:
                return int(cand)
            except Exception:
                continue
    return None


def _status_resolved(evt: Dict[str, Any]) -> bool:
    changed = evt.get("changed_attributes")
    try:
        if isinstance(changed, dict):
            return (changed.get("status") or {}).get("current_value", "").lower() == "resolved"
        if isinstance(changed, list):
            return any((item.get("status") or {}).get("current_value", "").lower() == "resolved" for item in changed)
    except Exception:
        pass
    return (((evt.get("conversation") or {}).get("status") or "").lower() == "resolved")


def _get_status_now(evt: Dict[str, Any]) -> Optional[str]:
    for cand in [
        evt.get("status"), evt.get("current_status"),
        (evt.get("conversation") or {}).get("status"),
        ((evt.get("message") or {}).get("conversation") or {}).get("status"),
    ]:
        if isinstance(cand, str) and cand:
            return cand
    return None


def _looks_like_close_text(text: str) -> bool:
    t = (text or "").strip().lower()
    if not t:
        return False
    tokens = [
        "resolved the conversation", "marked conversation as resolved",
        "closed the conversation", "conversation closed",
        "завершил диалог", "завершила диалог", "закрыл диалог", "закрыла диалог",
        "диалог завершён", "диалог завершен",
    ]
    return any(s in t for s in tokens)


async def _resolve_chat_id(conv_id: Optional[int], source_id_hint: Optional[str]) -> Optional[int]:
    if source_id_hint and source_id_hint.isdigit():
        return int(source_id_hint)
    if not conv_id:
        return None
    cid = CONV2CHAT.get(int(conv_id))
    if cid:
        return int(cid)
    cid = get_chat_by_conversation(int(conv_id))
    if cid:
        CONV2CHAT[int(conv_id)] = int(cid)
        return int(cid)
    try:
        resp = await cw.get(f"/conversations/{int(conv_id)}")
        if resp.is_success:
            j = resp.json() or {}
            src = str(
                ((j.get("conversation") or {}).get("contact_inbox") or {}).get("source_id")
                or (j.get("contact_inbox") or {}).get("source_id")
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
    set_conversation(chat_id, None)
    CONV2CHAT.pop(conv_id, None)
    add_closure(chat_id, conv_id, int(time.time()))
    try:
        await BOT.send_message(
            chat_id,
            f"<b>Тикет #{ticket_no or conv_id} закрыт</b>\n"
            f"Если появятся вопросы — нажмите «Новый тикет» и начните новый диалог.",
        )
    except Exception:
        logger.exception("send close notify failed")


async def handle_chatwoot_event(evt: Dict[str, Any], dedup_hint: Optional[str] = None):
    if "id" in evt:
        key = f"evt:{evt['id']}"
    elif "message" in evt and isinstance(evt["message"], dict):
        key = f"msg:{evt['message'].get('id')}:{evt.get('event')}"
    else:
        key = f"raw:{time.time_ns()}"
    if dedup_hint:
        key = f"{key}:{dedup_hint}"
    if not DEDUP.add(key):
        return

    ev = (evt.get("event") or "").lower()

    # 1) Agent messages -> TG
    if ev in {"message_created", "message_updated"} and _is_agent_outgoing(evt):
        source_id = _extract_source_id(evt) or ""
        if not source_id.isdigit():
            return
        chat_id = int(source_id)
        agent = _resolve_agent_name(evt)
        prefix = f"Агент {agent}:"
        message = evt.get("message") or {}
        content = (evt.get("content") or message.get("content") or "").strip()
        raw_atts = evt.get("attachments") or message.get("attachments") or []
        if content:
            try:
                await BOT.send_message(chat_id, f"{prefix}\n{content}")
            except TelegramBadRequest:
                await tg_api("sendMessage", {"chat_id": chat_id, "text": f"{prefix}\n{content}"})
        for i, att in enumerate(raw_atts):
            try:
                await _send_attachment_to_telegram(chat_id, att, caption=prefix, with_caption=(i == 0 and not content))
            except Exception:
                logger.exception("send attachment to TG failed")
        return

    # 2) Conversation status change
    if ev in {"conversation_status_changed", "conversation_updated", "conversation_resolved"}:
        st_now = (_get_status_now(evt) or "").lower()
        if not st_now and ev == "conversation_updated" and _status_resolved(evt):
            st_now = "resolved"
        if st_now == "resolved":
            source_id = _extract_source_id(evt)
            conv_id = _conv_id_from_event(evt)
            ticket_no = _conv_display_id(evt) or conv_id
            chat_id = await _resolve_chat_id(conv_id, source_id)
            if chat_id and conv_id:
                await _notify_and_close(chat_id, int(conv_id), ticket_no)
            return

    # 3) Activity message about closure
    if ev == "message_created":
        msg = evt.get("message") or {}
        content = (evt.get("content") or msg.get("content") or "").strip()
        status_now = (_get_status_now(evt) or "").lower()
        if status_now == "resolved" or _looks_like_close_text(content):
            source_id = _extract_source_id(evt)
            conv_id = _conv_id_from_event(evt)
            ticket_no = _conv_display_id(evt) or conv_id
            chat_id = await _resolve_chat_id(conv_id, source_id)
            if chat_id and conv_id:
                await _notify_and_close(chat_id, int(conv_id), ticket_no)

# ============================== FASTAPI APP ==============================

app = FastAPI(title="Telegram-Chatwoot Bridge", version="6.0")
security_scheme = HTTPBasic()


def require_agent(credentials: HTTPBasicCredentials = Depends(security_scheme)):
    if not AGENT_USER:
        return True
    correct = (
        credentials is not None
        and hmac.compare_digest(credentials.username, AGENT_USER)
        and hmac.compare_digest(credentials.password, AGENT_PASS)
    )
    if not correct:
        raise HTTPException(status_code=401, detail="Unauthorized", headers={"WWW-Authenticate": "Basic"})
    return True


@app.on_event("startup")
async def _startup():
    try:
        info = await BOT.get_webhook_info()
        url = (info.url or "").rstrip("/")
        target = f"{TELEGRAM_WEBHOOK_PUBLIC_URL}/telegram/webhook"
        if TELEGRAM_WEBHOOK_PUBLIC_URL and url != target:
            await BOT.set_webhook(
                url=target, secret_token=None,
                allowed_updates=TELEGRAM_ALLOWED_UPDATES or None,
                drop_pending_updates=False, max_connections=40,
            )
            logger.info("Webhook set: %s", target)
        else:
            logger.info("Webhook OK: %s (pending=%s)", url or "-", info.pending_update_count)
    except Exception as e:
        logger.warning("Webhook setup failed: %s", e)


@app.on_event("shutdown")
async def _shutdown():
    await cw.close_http()
    try:
        await BOT.session.close()
    except Exception:
        pass


@app.get("/")
async def root():
    return {"ok": True, "name": "telegram-chatwoot-bridge", "ver": "6.0"}


@app.get("/healthz")
async def healthz():
    return {"ok": True, "auth_mode": cw.auth_mode(), "inbox_id": CHATWOOT_INBOX_ID, "ts": int(time.time())}


# -------- Telegram webhook --------

@app.post("/telegram/webhook")
async def telegram_webhook(request: Request):
    if not verify_telegram_secret(dict(request.headers)):
        return JSONResponse({"status": "unauthorized"}, status_code=401)
    data = await request.json()
    try:
        update = Update.model_validate(data)
    except Exception:
        logger.exception("Update parse failed")
        return JSONResponse({"ok": True})
    try:
        await DP.feed_update(BOT, update)
    except Exception:
        logger.exception("DP.feed_update error")
    return JSONResponse({"ok": True})


# -------- Chatwoot webhook --------

@app.post("/chatwoot/webhook")
async def chatwoot_webhook(request: Request):
    raw = await request.body()
    headers = dict(request.headers)
    if not verify_chatwoot_webhook(raw, headers):
        return JSONResponse({"status": "unauthorized"}, status_code=401)

    enc = (headers.get("content-encoding") or "").lower().strip()
    try:
        if enc == "gzip":
            raw = gzip.decompress(raw)
        elif enc in ("deflate", "zlib"):
            raw = zlib.decompress(raw)
    except Exception:
        pass

    try:
        evt = json.loads(raw.decode("utf-8")) if raw else {}
    except Exception:
        evt = {}

    try:
        req_id = headers.get("x-request-id") or str(time.time_ns())
        await handle_chatwoot_event(evt, dedup_hint=req_id)
    except Exception:
        logger.exception("webhook handle error")
    return JSONResponse({"ok": True})


@app.post("/webhook")
async def chatwoot_webhook_alias(request: Request):
    return await chatwoot_webhook(request)


# -------- TG file proxy --------

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
        async with cw.HTTP.stream("GET", upstream) as r:
            r.raise_for_status()
            async for chunk in r.aiter_bytes():
                yield chunk

    return StreamingResponse(
        gen(),
        headers={
            "Content-Type": guessed,
            "Content-Disposition": "inline; filename*=UTF-8''" + quote(filename),
            "Cache-Control": "public, max-age=31536000, immutable",
        },
    )

# ============================== UI / KEYBOARDS ==============================

CB_CREATE = "create_ticket"
CB_EDIT_NICK = "edit_nick"
CB_PROMPT_NICK = "prompt_nick"
CB_USE_USERNAME = "use_username"
CB_HELP = "help"
CB_ATTACH_HOWTO = "attach_howto"
CB_STATUS = "status"


def _kb(rows: List[List[Tuple[str, str]]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=t, callback_data=d) for (t, d) in row] for row in rows]
    )


def kb_need_nick(username: Optional[str]) -> InlineKeyboardMarkup:
    rows: List[List[Tuple[str, str]]] = []
    if username:
        rows.append([(f"Использовать @{username}", CB_USE_USERNAME)])
    rows.append([("Ввести никнейм вручную", CB_PROMPT_NICK)])
    rows.append([("Помощь", CB_HELP)])
    return _kb(rows)


def kb_main(chat_id: int) -> InlineKeyboardMarkup:
    s = sess(chat_id)
    conv = s.get("conversation_id")
    rows: List[List[Tuple[str, str]]] = []
    if conv:
        rows.append([(f"Диалог #{conv}", CB_STATUS)])
    else:
        rows.append([("Новый тикет", CB_CREATE)])
    rows.append([("Как прислать файл/скрин", CB_ATTACH_HOWTO)])
    rows.append([("Статус", CB_STATUS), ("Никнейм", CB_EDIT_NICK)])
    rows.append([("Помощь", CB_HELP)])
    return _kb(rows)


async def _send_menu(chat_id: int, username: Optional[str]):
    s = sess(chat_id)
    if s.get("nickname"):
        await BOT.send_message(
            chat_id,
            "<b>Меню</b>\n"
            "— Создайте тикет или продолжайте текущий диалог.\n"
            "— Можно отправлять текст, фото, видео и файлы.",
            reply_markup=kb_main(chat_id),
        )
    else:
        await BOT.send_message(
            chat_id,
            "<b>Добро пожаловать!</b>\n"
            "Перед началом укажите никнейм — как к вам обращаться.",
            reply_markup=kb_need_nick(username),
        )


async def _start_flow(chat_id: int, from_user: dict):
    s = sess(chat_id)
    if s.get("nickname"):
        await BOT.send_message(chat_id, "<b>Привет!</b> Чем можем помочь?", reply_markup=kb_main(chat_id))
    else:
        s["awaiting_nickname"] = True
        await BOT.send_message(
            chat_id,
            "<b>Привет!</b> Укажите никнейм — как к вам обращаться.",
            reply_markup=kb_need_nick((from_user.get("username") or "").strip()),
        )


async def _set_nickname(chat_id: int, from_user: dict, nick: str):
    nick = nick.strip()[:64].strip("@ ")
    if not nick:
        await BOT.send_message(chat_id, "Никнейм не распознан. Отправьте желаемый ник.")
        return
    s = sess(chat_id)
    s["nickname"] = nick
    s["awaiting_nickname"] = False
    set_nickname(chat_id, nick)
    if s.get("contact_id"):
        await cw.update_contact_name(s["contact_id"], nick)
    await BOT.send_message(chat_id, f"Готово! Ваш никнейм: <b>@{nick}</b>")
    await _send_menu(chat_id, (from_user.get("username") or "").strip())

# ============================== TG ATTACHMENT COLLECTOR ==============================


def _push(files: List[dict], file_id: str, fn: Optional[str], mime: Optional[str]):
    if not file_id:
        return
    url = tgfile_public_url(file_id, filename=fn)
    files.append({"url": url, "file_id": file_id, "file_name": fn or "file", "mime": mime or guess_image_mime(fn or "file")})


def _collect_tg_attachments(msg: dict) -> Tuple[str, List[dict]]:
    text = (msg.get("caption") or msg.get("text") or "").strip()
    files: List[dict] = []
    if msg.get("photo"):
        _push(files, msg["photo"][-1].get("file_id"), "photo.jpg", "image/jpeg")
    if msg.get("document"):
        d = msg["document"]
        fname = d.get("file_name") or "document"
        mime = d.get("mime_type") or guess_image_mime(fname)
        fname = _fix_image_extension(fname, mime)
        _push(files, d.get("file_id"), fname, mime)
    if msg.get("video"):
        v = msg["video"]
        _push(files, v.get("file_id"), "video.mp4", v.get("mime_type") or "video/mp4")
    if msg.get("audio"):
        a = msg["audio"]
        _push(files, a.get("file_id"), a.get("file_name") or "audio.mp3", a.get("mime_type") or "audio/mpeg")
    if msg.get("voice"):
        vc = msg["voice"]
        _push(files, vc.get("file_id"), "voice.ogg", vc.get("mime_type") or "audio/ogg")
    if msg.get("sticker"):
        st = msg["sticker"]
        is_video = st.get("is_video", False)
        _push(files, st.get("file_id"), "sticker.webm" if is_video else "sticker.webp", "video/webm" if is_video else "image/webp")
    return text, files

# ============================== TG HANDLERS ==============================


@ROUTER.message(F.text.startswith("/start"))
async def h_start(message: Message):
    chat_id = message.chat.id
    from_user = message.from_user.model_dump() if message.from_user else {}
    await _start_flow(chat_id, from_user)
    try:
        s = sess(chat_id)
        name_hint = s.get("nickname") or (from_user.get("username") or "") or (from_user.get("first_name") or "") or str(chat_id)
        contact_id = await ensure_contact_and_inbox(CHATWOOT_INBOX_ID, str(chat_id), name_hint, f"{chat_id}@telegram", tg_user=from_user)
        if contact_id and not s.get("contact_id"):
            s["contact_id"] = contact_id
            upsert_session(chat_id, contact_id, s.get("conversation_id"), s.get("nickname"))
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
        sess(chat_id)["awaiting_nickname"] = True
        await BOT.send_message(chat_id, "Отправьте новый никнейм одним сообщением.")


@ROUTER.message(F.text.startswith("/status"))
async def h_status(message: Message):
    await _send_status(message.chat.id)


async def _send_status(chat_id: int):
    s = sess(chat_id)
    conv = s.get("conversation_id")
    last = get_last_closure(chat_id)
    last_line = f"Последний закрытый тикет: #{last[0]} в {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(last[1]))}." if last else "Закрытых тикетов пока нет."
    current_line = f"Текущий тикет: {('#' + str(conv)) if conv else 'нет (ещё не открыт)'}"
    await BOT.send_message(chat_id, f"<b>Статус</b>\n{current_line}\n{last_line}", reply_markup=kb_main(chat_id))


@ROUTER.callback_query(F.data == CB_USE_USERNAME)
async def cb_use_username(c: CallbackQuery):
    chat = c.message.chat if c.message else None
    if not chat:
        return await c.answer()
    from_user = c.from_user.model_dump() if c.from_user else {}
    username = (c.from_user.username or "").strip() if c.from_user else ""
    if username:
        await _set_nickname(chat.id, from_user, username)
    else:
        sess(chat.id)["awaiting_nickname"] = True
        await BOT.send_message(chat.id, "У вас нет username. Отправьте ник одним сообщением.")
    await c.answer()


@ROUTER.callback_query(F.data.in_({CB_PROMPT_NICK, CB_EDIT_NICK}))
async def cb_prompt_nick(c: CallbackQuery):
    chat = c.message.chat if c.message else None
    if not chat:
        return await c.answer()
    sess(chat.id)["awaiting_nickname"] = True
    await BOT.send_message(chat.id, "Отправьте новый никнейм одним сообщением.")
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
        await BOT.send_message(chat_id, "Сначала задайте никнейм.", reply_markup=kb_need_nick(username))
        return await c.answer()
    try:
        contact_id = s.get("contact_id")
        if not contact_id:
            display_name = s.get("nickname") or username or str(chat_id)
            contact_id = await ensure_contact_and_inbox(CHATWOOT_INBOX_ID, str(chat_id), display_name, f"{chat_id}@telegram", tg_user=from_user)
            s["contact_id"] = contact_id
            upsert_session(chat_id, contact_id, s.get("conversation_id"), s.get("nickname"))
        conv_id = await _ensure_open_conversation(CHATWOOT_INBOX_ID, int(contact_id), str(chat_id))
        s["conversation_id"] = conv_id
        set_conversation(chat_id, conv_id)
        CONV2CHAT[conv_id] = chat_id
        await BOT.send_message(
            chat_id,
            f"<b>Тикет открыт: #{conv_id}</b>\n"
            "Пишите сообщение или прикрепляйте файлы.",
            reply_markup=kb_main(chat_id),
        )
    except Exception:
        logger.exception("create via callback failed")
        await BOT.send_message(chat_id, "Не удалось открыть обращение. Попробуйте позже.")
    await c.answer()


@ROUTER.callback_query(F.data == CB_ATTACH_HOWTO)
async def cb_attach_howto(c: CallbackQuery):
    chat = c.message.chat if c.message else None
    if not chat:
        return await c.answer()
    await BOT.send_message(
        chat.id,
        "<b>Как прислать файл/скрин</b>\n"
        "Нажмите скрепку в Telegram.\n"
        "Для скриншотов лучше «Фото/Видео» — будет превью.\n"
        "Можно отправлять PDF/DOC/ZIP.",
        reply_markup=kb_main(chat.id),
    )
    await c.answer()


@ROUTER.callback_query(F.data == CB_HELP)
async def cb_help(c: CallbackQuery):
    chat = c.message.chat if c.message else None
    if not chat:
        return await c.answer()
    await BOT.send_message(
        chat.id,
        "<b>Помощь</b>\n"
        "— <b>Новый тикет</b> — создаёт обращение в поддержку.\n"
        "— <b>Как прислать файл/скрин</b> — подсказки по вложениям.\n"
        "— <b>Статус</b> — показывает текущий диалог.\n"
        "— <b>Никнейм</b> — как к вам обращаться.\n"
        "Сообщения от агента помечены «Агент ...».",
        reply_markup=kb_main(chat.id),
    )
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
            await BOT.send_message(chat_id, "Никнейм должен быть текстом.")
        return

    if not s.get("nickname"):
        username = (from_user.get("username") or "").strip()
        await BOT.send_message(chat_id, "Пожалуйста, укажите никнейм.", reply_markup=kb_need_nick(username))
        return

    if s.get("conversation_id"):
        try:
            caption_or_text, files = _collect_tg_attachments(message.model_dump())
            content = caption_or_text or text or ""
            await post_incoming_message(int(s["conversation_id"]), content, attachments=files if files else None)
        except Exception:
            logger.exception("forward user message failed")
            await BOT.send_message(chat_id, "Не удалось отправить сообщение. Попробуйте ещё раз.")
        return

    await BOT.send_message(chat_id, "Чтобы начать переписку с поддержкой — создайте тикет.", reply_markup=kb_main(chat_id))

# ============================== MAIN ==============================

if __name__ == "__main__":
    import uvicorn
    logger.info("Starting app on %s:%s", APP_HOST, APP_PORT)
    uvicorn.run("webhook_server:app", host=APP_HOST, port=APP_PORT, reload=False)
