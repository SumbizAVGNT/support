"""Telegram API helpers."""

from __future__ import annotations

import logging
import mimetypes
import os
from typing import Any, Dict, Optional
from urllib.parse import quote

from config import TELEGRAM_BOT_TOKEN, FILE_PROXY_PUBLIC_BASE
from chatwoot import HTTP

logger = logging.getLogger("tg-cw-bridge")

TG_API_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"


async def tg_api(method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        r = await HTTP.post(f"{TG_API_BASE}/{method}", json=payload)
        return r.json() if r.content else {}
    except Exception:
        logger.exception("tg_api error: %s", method)
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
        return photos[0][-1].get("file_id")
    except Exception:
        return None


def tg_file_direct_url(file_path: str) -> str:
    return f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{file_path}"


def tgfile_public_url(file_id: str, filename: Optional[str] = None) -> str:
    if FILE_PROXY_PUBLIC_BASE:
        q = f"?fn={quote(filename)}" if filename else ""
        return f"{FILE_PROXY_PUBLIC_BASE}/tgfile/{quote(file_id)}{q}"
    return ""


def guess_image_mime(filename: str, fallback: str = "application/octet-stream") -> str:
    mime = mimetypes.guess_type(filename or "")[0]
    if not mime:
        lower = (filename or "").lower()
        if lower.endswith((".jpg", ".jpeg")):
            return "image/jpeg"
        if lower.endswith(".png"):
            return "image/png"
        if lower.endswith(".webp"):
            return "image/webp"
    return mime or fallback
