import asyncio
import os
import io
import mimetypes
import logging
from typing import Optional, Dict, Any, List

import aiohttp
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

# Reusable connector for aiohttp sessions
_connector: Optional[aiohttp.TCPConnector] = None


def _get_connector() -> aiohttp.TCPConnector:
    global _connector
    if _connector is None or _connector.closed:
        _connector = aiohttp.TCPConnector(limit=30, ttl_dns_cache=300)
    return _connector


def get_chatwoot_headers() -> Dict[str, str]:
    return {
        "access-token": os.getenv("CHATWOOT_ACCESS_TOKEN", ""),
        "client": os.getenv("CHATWOOT_CLIENT", ""),
        "uid": os.getenv("CHATWOOT_UID", ""),
    }


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
        form = aiohttp.FormData()
        form.add_field("content", content or "")
        form.add_field("message_type", message_type)
        form.add_field("private", "false")

        headers = get_chatwoot_headers()
        headers.pop("Content-Type", None)

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
                        form.add_field(
                            "attachments[]",
                            file_data,
                            filename=filename,
                            content_type=content_type,
                        )

            async with session.post(url, data=form, headers=headers) as resp:
                if 200 <= resp.status < 300:
                    return True
                logger.warning("[send_chatwoot_message] failed: status=%s", resp.status)
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
