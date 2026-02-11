# utils.py
import os
import io
import mimetypes
from typing import Optional, Dict, Any, List

import aiohttp
import nextcord
from dotenv import load_dotenv

from datetime import datetime, timezone
try:
    # Python 3.9+
    from zoneinfo import ZoneInfo  # type: ignore
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

load_dotenv()

# Discord ограничение: не более 10 файлов на одно сообщение
MAX_FILES_PER_MESSAGE = 10


def get_chatwoot_headers() -> Dict[str, str]:
    return {
        "access-token": os.getenv("CHATWOOT_ACCESS_TOKEN"),
        "client": os.getenv("CHATWOOT_CLIENT"),
        "uid": os.getenv("CHATWOOT_UID"),
    }


async def send_chatwoot_message(
    conversation_id: int,
    content: str,
    message_type: str = "incoming",
    attachments: Optional[List[Dict[str, Any]]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Отправка сообщения в Chatwoot, с поддержкой multipart для вложений.
    """
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
        headers.pop("Content-Type", None)  # aiohttp сам выставит multipart

        async with aiohttp.ClientSession() as session:
            if attachments:
                for attachment in attachments:
                    attachment_url = (
                        getattr(attachment, "url", None)
                        or attachment.get("url")
                        or attachment.get("file_url")
                    )
                    if not attachment_url:
                        continue

                    filename = (
                        getattr(attachment, "filename", None)
                        or attachment.get("filename")
                        or "file"
                    )
                    content_type = (
                        getattr(attachment, "content_type", None)
                        or attachment.get("content_type")
                        or mimetypes.guess_type(filename)[0]
                        or "application/octet-stream"
                    )

                    async with session.get(attachment_url) as resp:
                        if resp.status == 200:
                            file_data = await resp.read()
                            if not file_data:
                                continue
                            form.add_field(
                                "attachments[]",
                                file_data,
                                filename=filename,
                                content_type=content_type,
                            )

        async with aiohttp.ClientSession() as session2:
            async with session2.post(url, data=form, headers=headers) as resp:
                if 200 <= resp.status < 300:
                    return await resp.json()
                else:
                    print(f"[send_chatwoot_message] failed: status={resp.status}")
                    try:
                        print(await resp.text())
                    except Exception:
                        pass
                    return None

    except Exception as e:
        print(f"[send_chatwoot_message] exception: {e}")
        return None


async def send_discord_message(
    user: nextcord.User,
    content: Optional[str],
    *,
    agent_name: Optional[str] = None,
    # URL для скачивания (внутренний прокси — бот скачивает файл)
    agent_avatar_download_url: Optional[str] = None,
    # Публичный HTTPS URL (фоллбэк для icon_url, если скачивание не удалось)
    agent_avatar_external_url: Optional[str] = None,
    attachments: Optional[List[Dict[str, Any]]] = None,
    # Параметры оформления/поведения:
    show_header_image: bool = False,          # крупная шапка-изображение из аватара (обычно не нужно)
    embed_color: int = 0x9B59B6,              # фиолетовый по умолчанию
    timezone_str: Optional[str] = None,       # например: "Europe/Moscow"
    show_timestamp: bool = True,              # ставить подпись даты/времени
) -> bool:
    """
    Отправляем EMBED:
      - author.name = "Агент Поддержки {никнейм}"
      - цвет embed (левая полоса) = embed_color (фиолетовый по умолчанию)
      - (опционально) крупная шапка из аватара (show_header_image)
      - иконка автора = аватар
      - первый image/* из attachments вставляется внутрь embed (после текста)
      - все прочие вложения — отдельными сообщениями следом

    Примечание: цвет заголовка (текста) автора Discord менять не позволяет — только цвет самого embed.
    """
    try:
        # === Текст и базовый embed ===
        text = (content or "").strip() or "\u200b"
        embed = nextcord.Embed(description=text, colour=nextcord.Colour(embed_color))

        header = f"Агент Поддержки {agent_name}".strip() if agent_name else "Агент Поддержки"

        # === Временная подпись ===
        if show_timestamp:
            now_utc = datetime.now(timezone.utc)
            if timezone_str and ZoneInfo:
                try:
                    tz = ZoneInfo(timezone_str)  # type: ignore
                    now_local = now_utc.astimezone(tz)
                except Exception:
                    now_local = now_utc
            else:
                now_local = now_utc

            embed.timestamp = now_local
            # Явная подпись — чтобы в любом клиенте читалось одинаково
            embed.set_footer(text=now_local.strftime("%d.%m.%Y %H:%M %Z"))

        essential_files: List[nextcord.File] = []  # то, что надо приложить к embed (аватар/картинка для set_image)
        other_files: List[nextcord.File] = []      # все остальные вложения (уйдут отдельными сообщениями)
        avatar_file: Optional[nextcord.File] = None
        image_for_embed: Optional[nextcord.File] = None  # первый image/* из attachments

        print(
            "[send_discord_message] "
            f"agent_name={agent_name!r} "
            f"avatar_download={agent_avatar_download_url!r} "
            f"avatar_external={agent_avatar_external_url!r}"
        )

        # === Утилитки ===
        def _guess_is_image(filename: str, url: str, ctype: Optional[str]) -> bool:
            if ctype and ctype.lower().startswith("image/"):
                return True
            if not filename and url:
                filename = os.path.basename(url.split("?")[0])
            ext = os.path.splitext(filename)[1].lower()
            if ext in {".png", ".jpg", ".jpeg", ".gif", ".webp"}:
                return True
            guessed = mimetypes.guess_type(url or filename)[0]
            return bool(guessed and guessed.startswith("image/"))

        async def _try_fetch_file(url: str, filename: Optional[str] = None) -> Optional[nextcord.File]:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            data = await resp.read()
                            if not data:
                                print("[send_discord_message] file body empty")
                                return None
                            if not filename:
                                # Пытаемся вывести имя из URL или content-type
                                fname = os.path.basename(url.split("?")[0]) or "file"
                            else:
                                fname = filename
                            # добиваем расширение, если его нет
                            if "." not in fname:
                                ctype = (resp.headers.get("Content-Type") or "").split(";")[0].strip()
                                ext = mimetypes.guess_extension(ctype) or mimetypes.guess_extension(
                                    mimetypes.guess_type(url)[0] or ""
                                ) or ".bin"
                                fname = fname + ext
                            return nextcord.File(io.BytesIO(data), filename=fname)
                        print(f"[send_discord_message] fetch failed status={resp.status} url={url}")
            except Exception as e:
                print(f"[send_discord_message] fetch exception: {e} url={url}")
            return None

        # === 1) Пробуем скачать аватар (как файл — чтобы дать icon_url через attachment://) ===
        if agent_avatar_download_url:
            avatar_file = await _try_fetch_file(agent_avatar_download_url, filename="agent_avatar")

        if not avatar_file and agent_avatar_external_url:
            avatar_file = await _try_fetch_file(agent_avatar_external_url, filename="agent_avatar")

        # === 2) Заголовок автора + опциональная шапка из аватара ===
        if avatar_file:
            embed.set_author(name=header, icon_url=f"attachment://{avatar_file.filename}")
            essential_files.append(avatar_file)
            if show_header_image:
                # Если включена "шапка", используем аватар как большое изображение сверху.
                embed.set_image(url=f"attachment://{avatar_file.filename}")
        else:
            # Фоллбэк на внешний HTTPS URL (иконка у автора)
            if agent_avatar_external_url and agent_avatar_external_url.startswith("https://"):
                embed.set_author(name=header, icon_url=agent_avatar_external_url)
                if show_header_image:
                    embed.set_image(url=agent_avatar_external_url)
            else:
                embed.set_author(name=header)

        # === 3) Разбор пользовательских вложений.
        # Первый image/* (скриншот) пойдёт в embed.set_image (если шапка не занята),
        # прочие вложения уйдут отдельными файлами после embed.
        if attachments:
            async with aiohttp.ClientSession() as session:
                for att in attachments:
                    url = att.get("url") or att.get("file_url")
                    if not url:
                        continue

                    filename = att.get("filename") or "file"
                    ctype_att = (att.get("type") or att.get("content_type") or "").split(";")[0].strip()
                    is_image = _guess_is_image(filename, url, ctype_att)

                    async with session.get(url) as resp:
                        if resp.status != 200:
                            print(f"[send_discord_message] attachment fetch failed {filename} status={resp.status}")
                            continue
                        data = await resp.read()
                        if not data:
                            print(f"[send_discord_message] attachment empty: {filename}")
                            continue

                        # Гарантируем расширение
                        if "." not in filename:
                            ctype = (resp.headers.get("Content-Type") or "").split(";")[0].strip()
                            guessed_ext = mimetypes.guess_extension(ctype) or mimetypes.guess_extension(
                                mimetypes.guess_type(url)[0] or ""
                            ) or ".bin"
                            filename = filename + guessed_ext

                        file_obj = nextcord.File(io.BytesIO(data), filename=filename)

                        # Если это картинка и в embed ещё нет изображения (или шапка не занята),
                        # кладём её внутрь embed, иначе — как отдельный файл после.
                        if is_image and image_for_embed is None and not (show_header_image and avatar_file):
                            image_for_embed = file_obj
                        else:
                            other_files.append(file_obj)

        # Вставляем картинку (скриншот) внутрь embed ПОСЛЕ текста
        if image_for_embed:
            essential_files.append(image_for_embed)
            embed.set_image(url=f"attachment://{image_for_embed.filename}")

        # === 4) Отправка: embed + только нужные файлы (аватар/картинка embed)
        if essential_files:
            await user.send(embed=embed, files=essential_files[:MAX_FILES_PER_MESSAGE])
            print(f"[send_discord_message] sent embed with {len(essential_files)} embedded files "
                  f"(avatar/image); queued other files: {len(other_files)}")
        else:
            await user.send(embed=embed)
            print(f"[send_discord_message] sent embed without files; queued other files: {len(other_files)}")

        # === 5) Остальные файлы — отдельными сообщениями (батчами по 10)
        while other_files:
            batch = other_files[:MAX_FILES_PER_MESSAGE]
            other_files = other_files[MAX_FILES_PER_MESSAGE:]
            await user.send(files=batch)
            print(f"[send_discord_message] sent follow-up batch size={len(batch)}")

        return True

    except Exception as e:
        print(f"[send_discord_message] exception: {e}")
        return False
