import asyncio
import os
import io
import mimetypes
import logging
import threading
from typing import Optional, Dict, Any, List

import aiohttp
import requests as _requests_lib
import nextcord
from dotenv import load_dotenv
from datetime import datetime, timezone

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

load_dotenv()

logger = logging.getLogger("discord_webhook")

MAX_FILES_PER_MESSAGE = 10
MAX_FILE_SIZE = int(os.getenv("MAX_ATTACHMENT_SIZE_MB", "50")) * 1024 * 1024

# Reusable connector for aiohttp sessions
_connector: Optional[aiohttp.TCPConnector] = None


def _get_connector() -> aiohttp.TCPConnector:
    global _connector
    if _connector is None or _connector.closed:
        _connector = aiohttp.TCPConnector(limit=30, ttl_dns_cache=300)
    return _connector


# --------------- Mutable token state ---------------

_tokens: Dict[str, str] = {
    "access-token": os.getenv("CHATWOOT_ACCESS_TOKEN", ""),
    "client": os.getenv("CHATWOOT_CLIENT", ""),
    "uid": os.getenv("CHATWOOT_UID", ""),
}
_CW_PASSWORD = os.getenv("CHATWOOT_PASSWORD", os.getenv("CW_PASSWORD", "")).strip()
_CHATWOOT_BASE_URL = os.getenv("CHATWOOT_BASE_URL", "").rstrip("/")
_token_lock = threading.Lock()


def get_chatwoot_headers() -> Dict[str, str]:
    return {
        "access-token": _tokens["access-token"],
        "client": _tokens["client"],
        "uid": _tokens["uid"],
    }


def _update_tokens_from_headers(headers: dict) -> None:
    """Extract rotated Devise tokens from response headers."""
    new_token = (headers.get("access-token") or "").strip()
    new_client = (headers.get("client") or "").strip()
    new_uid = (headers.get("uid") or "").strip()
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


def refresh_tokens_sync() -> bool:
    """Re-authenticate with Chatwoot via /auth/sign_in (synchronous)."""
    email = _tokens["uid"]
    if not _CW_PASSWORD or not email:
        logger.error("Cannot re-authenticate: CHATWOOT_PASSWORD or CW_UID not configured")
        return False
    try:
        url = f"{_CHATWOOT_BASE_URL}/auth/sign_in"
        resp = _requests_lib.post(url, json={"email": email, "password": _CW_PASSWORD}, timeout=30)
        if resp.status_code == 200:
            data = resp.json() or {}
            d = data.get("data") or data
            new_token = (
                (resp.headers.get("access-token") or "").strip()
                or str(d.get("access_token") or "").strip()
            )
            new_client = (resp.headers.get("client") or "").strip()
            new_uid = (
                (resp.headers.get("uid") or "").strip()
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


async def refresh_tokens_async() -> bool:
    """Re-authenticate with Chatwoot via /auth/sign_in (async)."""
    email = _tokens["uid"]
    if not _CW_PASSWORD or not email:
        logger.error("Cannot re-authenticate: CHATWOOT_PASSWORD or CW_UID not configured")
        return False
    try:
        url = f"{_CHATWOOT_BASE_URL}/auth/sign_in"
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(connector=_get_connector(), connector_owner=False, timeout=timeout) as session:
            async with session.post(url, json={"email": email, "password": _CW_PASSWORD}) as resp:
                if resp.status == 200:
                    data = await resp.json() or {}
                    d = data.get("data") or data
                    new_token = (
                        (resp.headers.get("access-token") or "").strip()
                        or str(d.get("access_token") or "").strip()
                    )
                    new_client = (resp.headers.get("client") or "").strip()
                    new_uid = (
                        (resp.headers.get("uid") or "").strip()
                        or str(d.get("uid") or email).strip()
                    )
                    if new_token:
                        _tokens["access-token"] = new_token
                        if new_client:
                            _tokens["client"] = new_client
                        if new_uid:
                            _tokens["uid"] = new_uid
                        logger.info("Re-authenticated with Chatwoot successfully (async)")
                        return True
                    logger.error("sign_in 200 but no access-token in response")
                else:
                    body = await resp.text()
                    logger.error("Chatwoot sign_in failed: %s %s", resp.status, body[:300])
    except Exception:
        logger.exception("Chatwoot sign_in exception (async)")
    return False


async def send_chatwoot_message(
    conversation_id: int,
    content: str,
    message_type: str = "incoming",
    attachments: Optional[List[Any]] = None,
) -> bool:
    url = (
        f"{os.getenv('CHATWOOT_BASE_URL')}"
        f"/api/v1/accounts/{os.getenv('CHATWOOT_ACCOUNT_ID')}/conversations/{conversation_id}/messages"
    )
    try:
        # Pre-download attachments so we can retry the API call if needed
        downloaded_files: List[dict] = []
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(connector=_get_connector(), connector_owner=False, timeout=timeout) as session:
            if attachments:
                for attachment in attachments:
                    attachment_url = getattr(attachment, "url", None) or (
                        attachment.get("url") if isinstance(attachment, dict) else None
                    )
                    if not attachment_url:
                        continue
                    filename = getattr(attachment, "filename", None) or (
                        attachment.get("filename") if isinstance(attachment, dict) else "file"
                    )
                    content_type = getattr(attachment, "content_type", None) or (
                        attachment.get("content_type") if isinstance(attachment, dict) else None
                    ) or mimetypes.guess_type(filename or "")[0] or "application/octet-stream"

                    file_data = None
                    for attempt in range(3):
                        try:
                            async with session.get(attachment_url) as resp:
                                if resp.status == 200:
                                    file_data = await resp.read()
                                    break
                                elif resp.status == 404 and attempt < 2:
                                    await asyncio.sleep(2 ** attempt)
                                    continue
                                else:
                                    break
                        except Exception:
                            if attempt < 2:
                                await asyncio.sleep(2 ** attempt)
                    if file_data:
                        downloaded_files.append({"data": file_data, "filename": filename, "content_type": content_type})

            def _build_form() -> aiohttp.FormData:
                form = aiohttp.FormData()
                form.add_field("content", content or "")
                form.add_field("message_type", message_type)
                form.add_field("private", "false")
                for f in downloaded_files:
                    form.add_field("attachments[]", f["data"], filename=f["filename"], content_type=f["content_type"])
                return form

            headers = get_chatwoot_headers()
            headers.pop("Content-Type", None)

            async with session.post(url, data=_build_form(), headers=headers) as resp:
                _update_tokens_from_headers(dict(resp.headers))
                if 200 <= resp.status < 300:
                    return True
                if resp.status != 401:
                    logger.warning("[send_chatwoot_message] failed: status=%s", resp.status)
                    return False

            # Got 401 — try to refresh tokens and retry
            logger.warning("[send_chatwoot_message] got 401, refreshing tokens")
            if not await refresh_tokens_async():
                return False

            headers = get_chatwoot_headers()
            headers.pop("Content-Type", None)
            async with session.post(url, data=_build_form(), headers=headers) as resp:
                _update_tokens_from_headers(dict(resp.headers))
                if 200 <= resp.status < 300:
                    return True
                logger.warning("[send_chatwoot_message] retry failed: status=%s", resp.status)
                return False
    except Exception as e:
        logger.error("[send_chatwoot_message] exception: %s", e)
        return False


def _guess_is_image(filename: str, url: str, ctype: Optional[str]) -> bool:
    if ctype and ctype.lower().startswith("image/"):
        return True
    if not filename and url:
        filename = os.path.basename(url.split("?")[0])
    ext = os.path.splitext(filename or "")[1].lower()
    if ext in {".png", ".jpg", ".jpeg", ".gif", ".webp"}:
        return True
    guessed = mimetypes.guess_type(url or filename or "")[0]
    return bool(guessed and guessed.startswith("image/"))


async def _try_fetch_file(url: str, filename: Optional[str] = None) -> Optional[nextcord.File]:
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(connector=_get_connector(), connector_owner=False, timeout=timeout) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    logger.warning("[fetch_file] status=%s url=%s", resp.status, url)
                    return None
                data = await resp.read()
                if not data:
                    return None
                if len(data) > MAX_FILE_SIZE:
                    logger.warning("[fetch_file] file too large (%d bytes) url=%s", len(data), url)
                    return None
                fname = filename or os.path.basename(url.split("?")[0]) or "file"
                if "." not in fname:
                    ctype = (resp.headers.get("Content-Type") or "").split(";")[0].strip()
                    ext = mimetypes.guess_extension(ctype) or ".bin"
                    fname = fname + ext
                return nextcord.File(io.BytesIO(data), filename=fname)
    except Exception as e:
        logger.error("[fetch_file] exception: %s url=%s", e, url)
    return None


async def send_discord_message(
    user: nextcord.User,
    content: Optional[str],
    *,
    agent_name: Optional[str] = None,
    agent_avatar_download_url: Optional[str] = None,
    agent_avatar_external_url: Optional[str] = None,
    attachments: Optional[List[Dict[str, Any]]] = None,
    embed_color: int = 0x9B59B6,
    timezone_str: Optional[str] = None,
    show_timestamp: bool = True,
) -> bool:
    try:
        text = (content or "").strip() or "\u200b"
        embed = nextcord.Embed(description=text, colour=nextcord.Colour(embed_color))

        header = f"Агент Поддержки {agent_name}".strip() if agent_name else "Агент Поддержки"

        if show_timestamp:
            now_utc = datetime.now(timezone.utc)
            if timezone_str and ZoneInfo:
                try:
                    now_local = now_utc.astimezone(ZoneInfo(timezone_str))
                except Exception:
                    now_local = now_utc
            else:
                now_local = now_utc
            embed.timestamp = now_local
            embed.set_footer(text=now_local.strftime("%d.%m.%Y %H:%M %Z"))

        essential_files: List[nextcord.File] = []
        other_files: List[nextcord.File] = []
        avatar_file: Optional[nextcord.File] = None
        image_for_embed: Optional[nextcord.File] = None

        # Fetch avatar
        if agent_avatar_download_url:
            avatar_file = await _try_fetch_file(agent_avatar_download_url, filename="agent_avatar")
        if not avatar_file and agent_avatar_external_url:
            avatar_file = await _try_fetch_file(agent_avatar_external_url, filename="agent_avatar")

        # Set author header with avatar
        if avatar_file:
            embed.set_author(name=header, icon_url=f"attachment://{avatar_file.filename}")
            essential_files.append(avatar_file)
        elif agent_avatar_external_url and agent_avatar_external_url.startswith("https://"):
            embed.set_author(name=header, icon_url=agent_avatar_external_url)
        else:
            embed.set_author(name=header)

        # Process attachments
        if attachments:
            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(connector=_get_connector(), connector_owner=False, timeout=timeout) as session:
                for att in attachments:
                    url = att.get("url") or att.get("file_url")
                    if not url:
                        continue
                    filename = att.get("filename") or "file"
                    ctype_att = (att.get("type") or att.get("content_type") or "").split(";")[0].strip()
                    is_image = _guess_is_image(filename, url, ctype_att)

                    # Retry with exponential backoff for 404s (ActiveStorage race condition)
                    data = None
                    resp_headers = {}
                    for attempt in range(4):
                        try:
                            async with session.get(url) as resp:
                                if resp.status == 404 and attempt < 3:
                                    wait = 2 ** attempt
                                    logger.warning("Attachment 404, retry %d in %ds: %s", attempt + 1, wait, url)
                                    await asyncio.sleep(wait)
                                    continue
                                if resp.status != 200:
                                    break
                                data = await resp.read()
                                resp_headers = resp.headers
                                break
                        except Exception as dl_err:
                            if attempt < 3:
                                wait = 2 ** attempt
                                logger.warning("Attachment download error (attempt %d), retry in %ds: %s", attempt + 1, wait, dl_err)
                                await asyncio.sleep(wait)
                            else:
                                logger.error("Attachment download failed after retries: %s", dl_err)

                    if not data:
                        continue
                    if len(data) > MAX_FILE_SIZE:
                        logger.warning("[send_discord_message] attachment too large (%d bytes), skipped: %s", len(data), url)
                        continue
                    if "." not in filename:
                        ctype = (resp_headers.get("Content-Type") or "").split(";")[0].strip()
                        guessed_ext = mimetypes.guess_extension(ctype) or ".bin"
                        filename = filename + guessed_ext
                    file_obj = nextcord.File(io.BytesIO(data), filename=filename)
                    if is_image and image_for_embed is None:
                        image_for_embed = file_obj
                    else:
                        other_files.append(file_obj)

        if image_for_embed:
            essential_files.append(image_for_embed)
            embed.set_image(url=f"attachment://{image_for_embed.filename}")

        # Send embed with essential files
        if essential_files:
            await user.send(embed=embed, files=essential_files[:MAX_FILES_PER_MESSAGE])
        else:
            await user.send(embed=embed)

        # Send remaining files in batches
        while other_files:
            batch = other_files[:MAX_FILES_PER_MESSAGE]
            other_files = other_files[MAX_FILES_PER_MESSAGE:]
            await user.send(files=batch)

        return True
    except Exception as e:
        logger.error("[send_discord_message] exception: %s", e)
        return False
