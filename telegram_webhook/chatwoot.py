"""Chatwoot API client - async with httpx, with automatic token refresh."""

from __future__ import annotations

import asyncio
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
    CW_PASSWORD,
    CHATWOOT_INBOX_ID,
)

logger = logging.getLogger("tg-cw-bridge")

HTTP_TIMEOUT = httpx.Timeout(30, connect=10)
HTTP = httpx.AsyncClient(
    timeout=HTTP_TIMEOUT,
    limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
    headers={"User-Agent": "telegram-chatwoot-bridge/6.0"},
)

# --------------- Mutable token state ---------------

_tokens: Dict[str, str] = {
    "access-token": CW_ACCESS_TOKEN,
    "client": CW_CLIENT,
    "uid": CW_UID,
}
_token_lock = asyncio.Lock()


def auth_mode() -> str:
    if CHATWOOT_API_TOKEN:
        return "api_token"
    if _tokens["access-token"] and _tokens["client"] and _tokens["uid"]:
        return "devise"
    return "none"


def _headers() -> Dict[str, str]:
    h = {"Accept": "application/json"}
    if CHATWOOT_API_TOKEN:
        h["api_access_token"] = CHATWOOT_API_TOKEN
    elif _tokens["access-token"] and _tokens["client"] and _tokens["uid"]:
        h.update({
            "access-token": _tokens["access-token"],
            "client": _tokens["client"],
            "uid": _tokens["uid"],
        })
    return h


def _url(path: str) -> str:
    return f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}{path}"


# --------------- Token rotation & refresh ---------------

def _update_tokens_from_response(resp: httpx.Response) -> None:
    """Extract rotated Devise tokens from response headers."""
    new_token = resp.headers.get("access-token", "").strip()
    new_client = resp.headers.get("client", "").strip()
    new_uid = resp.headers.get("uid", "").strip()
    if new_token and new_client and new_uid:
        changed = (
            new_token != _tokens["access-token"]
            or new_client != _tokens["client"]
            or new_uid != _tokens["uid"]
        )
        if changed:
            _tokens["access-token"] = new_token
            _tokens["client"] = new_client
            _tokens["uid"] = new_uid
            logger.info("Devise tokens rotated from response headers")


async def _sign_in() -> bool:
    """Re-authenticate with Chatwoot via /auth/sign_in."""
    email = _tokens["uid"]
    if not CW_PASSWORD or not email:
        logger.error("Cannot re-authenticate: CHATWOOT_PASSWORD or CW_UID not configured")
        return False
    try:
        url = f"{CHATWOOT_BASE_URL}/auth/sign_in"
        resp = await HTTP.post(url, json={"email": email, "password": CW_PASSWORD})
        if resp.status_code == 200:
            data = resp.json() or {}
            d = data.get("data") or data
            new_token = (
                resp.headers.get("access-token", "").strip()
                or str(d.get("access_token") or "").strip()
            )
            new_client = resp.headers.get("client", "").strip()
            new_uid = (
                resp.headers.get("uid", "").strip()
                or str(d.get("uid") or email).strip()
            )
            if new_token:
                _tokens["access-token"] = new_token
                if new_client:
                    _tokens["client"] = new_client
                if new_uid:
                    _tokens["uid"] = new_uid
                logger.info("Re-authenticated with Chatwoot successfully")
                return True
            logger.error("sign_in 200 but no access-token in response")
        else:
            logger.error("Chatwoot sign_in failed: %s %s", resp.status_code, resp.text[:300])
    except Exception:
        logger.exception("Chatwoot sign_in exception")
    return False


def _reset_file_positions(kw: dict) -> None:
    """Seek all BytesIO objects back to start for request retry."""
    for item in kw.get("files") or []:
        if isinstance(item, (tuple, list)) and len(item) >= 2:
            file_tuple = item[1] if isinstance(item[1], (tuple, list)) else item
            for part in (file_tuple if isinstance(file_tuple, (tuple, list)) else [file_tuple]):
                if isinstance(part, io.BytesIO):
                    part.seek(0)


async def _request_with_refresh(method: str, url: str, **kw) -> httpx.Response:
    """Make an HTTP request; on 401 with Devise auth, refresh tokens and retry."""
    headers = kw.pop("headers", _headers())
    resp = await getattr(HTTP, method)(url, headers=headers, **kw)

    if resp.status_code != 401 or CHATWOOT_API_TOKEN:
        _update_tokens_from_response(resp)
        return resp

    logger.warning("Got 401 from %s %s, attempting token refresh", method.upper(), url)

    async with _token_lock:
        current_headers = _headers()
        if current_headers.get("access-token") != headers.get("access-token"):
            # Another coroutine already refreshed tokens
            _reset_file_positions(kw)
            resp2 = await getattr(HTTP, method)(url, headers=_headers(), **kw)
            _update_tokens_from_response(resp2)
            return resp2

        if not await _sign_in():
            return resp  # Return original 401 if refresh failed

    # Retry with new tokens
    _reset_file_positions(kw)
    resp2 = await getattr(HTTP, method)(url, headers=_headers(), **kw)
    _update_tokens_from_response(resp2)
    return resp2


# --------------- Public API (same interface as before) ---------------

async def get(path: str, **kw) -> httpx.Response:
    return await _request_with_refresh("get", _url(path), **kw)


async def post(path: str, **kw) -> httpx.Response:
    headers = _headers()
    if "json" in kw:
        headers["Content-Type"] = "application/json"
    return await _request_with_refresh("post", _url(path), headers=headers, **kw)


async def patch(path: str, **kw) -> httpx.Response:
    headers = _headers()
    if "json" in kw:
        headers["Content-Type"] = "application/json"
    return await _request_with_refresh("patch", _url(path), headers=headers, **kw)


async def post_multipart(
    path: str,
    data: Dict[str, Any],
    files: List[Tuple[str, Tuple[str, io.BytesIO, str]]],
) -> httpx.Response:
    return await _request_with_refresh(
        "post", _url(path),
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
