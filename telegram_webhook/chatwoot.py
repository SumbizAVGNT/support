"""Chatwoot API client - async with httpx."""

from __future__ import annotations

import io
import logging
from typing import Any, Dict, List, Optional, Tuple

import httpx

from config import (
    CHATWOOT_BASE_URL,
    CHATWOOT_ACCOUNT_ID,
    CHATWOOT_API_TOKEN,
    CW_ACCESS_TOKEN,
    CW_CLIENT,
    CW_UID,
    CHATWOOT_INBOX_ID,
)

logger = logging.getLogger("tg-cw-bridge")

HTTP_TIMEOUT = httpx.Timeout(30, connect=10)
HTTP = httpx.AsyncClient(
    timeout=HTTP_TIMEOUT,
    limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
    headers={"User-Agent": "telegram-chatwoot-bridge/6.0"},
)


def auth_mode() -> str:
    if CHATWOOT_API_TOKEN:
        return "api_token"
    if CW_ACCESS_TOKEN and CW_CLIENT and CW_UID:
        return "devise"
    return "none"


def _headers() -> Dict[str, str]:
    h = {"Accept": "application/json"}
    if CHATWOOT_API_TOKEN:
        h["api_access_token"] = CHATWOOT_API_TOKEN
    elif CW_ACCESS_TOKEN and CW_CLIENT and CW_UID:
        h.update({"access-token": CW_ACCESS_TOKEN, "client": CW_CLIENT, "uid": CW_UID})
    return h


def _url(path: str) -> str:
    return f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}{path}"


async def get(path: str, **kw) -> httpx.Response:
    return await HTTP.get(_url(path), headers=_headers(), **kw)


async def post(path: str, **kw) -> httpx.Response:
    headers = _headers()
    if "json" in kw:
        headers["Content-Type"] = "application/json"
    return await HTTP.post(_url(path), headers=headers, **kw)


async def patch(path: str, **kw) -> httpx.Response:
    headers = _headers()
    if "json" in kw:
        headers["Content-Type"] = "application/json"
    return await HTTP.patch(_url(path), headers=headers, **kw)


async def post_multipart(
    path: str,
    data: Dict[str, Any],
    files: List[Tuple[str, Tuple[str, io.BytesIO, str]]],
) -> httpx.Response:
    return await HTTP.post(
        _url(path),
        headers=_headers(),
        data=data,
        files=files,
        timeout=httpx.Timeout(60, connect=10),
    )


async def update_contact_name(contact_id: Optional[int], name: str):
    if not contact_id:
        return
    try:
        await patch(f"/contacts/{contact_id}", json={"name": name})
    except Exception:
        logger.exception("failed to patch contact name")


async def close_http():
    try:
        await HTTP.aclose()
    except Exception:
        pass
