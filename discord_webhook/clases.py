import nextcord
from nextcord.ui import Modal, TextInput, View, Button
from nextcord.ext import commands
import aiohttp
import os
import asyncio
import logging
import json
from datetime import datetime

from database import (
    get_session_by_discord_id,
    get_conversation_status,
    mark_message_processed,
    is_message_processed,
)
from utils import send_chatwoot_message

logger = logging.getLogger("discord_webhook")


class ProblemReportButtonView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @nextcord.ui.button(
        label="Сообщить о проблеме",
        style=nextcord.ButtonStyle.primary,
        custom_id="problem_report_button",
    )
    async def problem_report_button(self, button: Button, interaction: nextcord.Interaction):
        await interaction.response.send_modal(ProblemReportModal())


class ProblemReportModal(Modal):
    def __init__(self):
        super().__init__(
            title="Сообщить о проблеме",
            timeout=300,
            custom_id="problem_report_modal",
        )
        self.nickname = TextInput(
            label="Ваш никнейм",
            placeholder="Введите ваш никнейм или имя",
            min_length=2,
            max_length=32,
            required=True,
        )
        self.add_item(self.nickname)

        self.problem_description = TextInput(
            label="Опишите проблему",
            style=nextcord.TextInputStyle.paragraph,
            placeholder="Подробно опишите вашу проблему...",
            min_length=10,
            max_length=1024,
            required=True,
        )
        self.add_item(self.problem_description)

    async def callback(self, interaction: nextcord.Interaction):
        discord_user_id = str(interaction.user.id)
        try:
            if self._has_active_ticket(discord_user_id):
                await interaction.response.send_message(
                    "У вас уже есть активный тикет. Используйте его для общения с поддержкой.",
                    ephemeral=True,
                )
                return

            # Defer immediately — ticket creation takes several seconds
            await interaction.response.defer(ephemeral=True)

            response, used_url = await self._create_support_ticket(discord_user_id)
            await self._handle_ticket_creation_response(interaction, response, discord_user_id, used_url)
        except Exception as e:
            logger.error("Modal callback error: %s", e, exc_info=True)
            await self._send_error_response(interaction)

    def _has_active_ticket(self, discord_user_id: str) -> bool:
        session = get_session_by_discord_id(discord_user_id)
        return bool(session and get_conversation_status(session[3]))

    def _candidate_urls(self) -> list[str]:
        env_url = os.getenv("FLASK_SERVER_URL", "").strip()
        if not env_url:
            env_url = "http://webhook:5500/create_contact"
        if not env_url.rstrip("/").endswith("/create_contact"):
            env_url = env_url.rstrip("/") + "/create_contact"

        candidates = [env_url]
        fallbacks = [
            "http://webhook:5500/create_contact",
            "http://localhost:5500/create_contact",
        ]
        seen = {env_url}
        for u in fallbacks:
            if u not in seen:
                candidates.append(u)
                seen.add(u)
        return candidates

    async def _create_support_ticket(self, discord_user_id: str):
        payload = {
            "name": self.nickname.value,
            "email": f"{discord_user_id}@discord",
            "discord_user": discord_user_id,
            "problem_text": self.problem_description.value,
        }
        candidates = self._candidate_urls()
        timeout = aiohttp.ClientTimeout(total=15)
        last_response = None
        last_url = None

        async with aiohttp.ClientSession(timeout=timeout) as session_http:
            for url in candidates:
                try:
                    logger.info("[create_contact] POST %s", url)
                    resp = await session_http.post(url, json=payload)
                    last_response = resp
                    last_url = url
                    if resp.status == 404:
                        logger.warning("[create_contact] %s -> 404, trying next", url)
                        continue
                    return resp, url
                except asyncio.TimeoutError:
                    logger.error("[create_contact] %s -> timeout", url)
                except aiohttp.ClientError as e:
                    logger.error("[create_contact] %s -> error: %s", url, e)
        return last_response, last_url

    async def _handle_ticket_creation_response(self, interaction, response, discord_user_id, used_url):
        if response is None:
            await interaction.followup.send("Сервер недоступен. Попробуйте позже.", ephemeral=True)
            return

        status = response.status
        body_bytes = await response.read()
        body_snippet = (body_bytes.decode(errors="replace")[:500]) if body_bytes else ""
        logger.info("[create_contact] url=%s status=%s body=%s", used_url, status, body_snippet)

        if status == 404:
            await interaction.followup.send("Ошибка сервера (404). Проверьте адрес сервиса.", ephemeral=True)
            return
        if not (200 <= status < 300):
            await interaction.followup.send(f"Ошибка сервера ({status})", ephemeral=True)
            return

        try:
            data = json.loads(body_bytes.decode("utf-8"))
        except Exception:
            await interaction.followup.send("Некорректный ответ сервера.", ephemeral=True)
            return

        if data.get("success"):
            conversation_id = data.get("conversation_id")
            mark_message_processed(f"ticket_created_{discord_user_id}")
            await interaction.followup.send(f"Запрос #{conversation_id} создан!", ephemeral=True)

            embed = nextcord.Embed(
                title=f"Информация о вашем запросе #{conversation_id}",
                color=nextcord.Color.blurple(),
                timestamp=datetime.utcnow(),
            )
            embed.add_field(name="Никнейм", value=self.nickname.value, inline=False)
            embed.add_field(name="Номер тикета", value=str(conversation_id), inline=True)
            embed.add_field(name="Описание проблемы", value=self.problem_description.value, inline=False)
            try:
                user = await interaction.user.create_dm()
                await user.send(embed=embed)
            except Exception as e:
                logger.error("Failed to send DM with ticket info: %s", e)
        else:
            await interaction.followup.send(
                f"Ошибка: {data.get('error', 'Неизвестная ошибка')}", ephemeral=True
            )

    async def _send_error_response(self, interaction: nextcord.Interaction):
        msg = "Ошибка соединения с сервером. Повторите попытку позже."
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message(msg, ephemeral=True)
            else:
                await interaction.followup.send(msg, ephemeral=True)
        except Exception as e:
            logger.error("Failed to send error response: %s", e)


class SupportBot(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.lock = asyncio.Lock()
        self.WARNING_MESSAGES = {
            "no_ticket": "Сначала создайте запрос через /support",
            "ticket_closed": "Этот тикет уже закрыт. Создайте новый запрос через /support",
            "send_error": "Ошибка отправки в поддержку",
        }

    @commands.Cog.listener()
    async def on_message(self, message):
        if message.author.bot:
            return
        if not isinstance(message.channel, nextcord.DMChannel):
            return
        try:
            async with self.lock:
                await self._process_user_message(message)
        except Exception as e:
            logger.error("Message processing error: %s", e, exc_info=True)

    async def _process_user_message(self, message):
        message_id = str(message.id)
        discord_user_id = str(message.author.id)

        if is_message_processed(message_id):
            return

        session = get_session_by_discord_id(discord_user_id)
        if not session:
            await self._send_warning_if_needed(message.channel, "no_ticket")
            return

        conversation_id = session[3]
        if not get_conversation_status(conversation_id):
            await self._send_warning_if_needed(message.channel, "ticket_closed")
            return

        mark_message_processed(message_id)
        ok = await send_chatwoot_message(conversation_id, message.content, attachments=message.attachments)
        if not ok:
            await self._send_warning_if_needed(message.channel, "send_error")

    async def _send_warning_if_needed(self, channel, message_key):
        text = self.WARNING_MESSAGES.get(message_key)
        if not text:
            return
        async for msg in channel.history(limit=5):
            if msg.author == self.bot.user and msg.content == text:
                return
        await channel.send(text)


def setup(bot):
    bot.add_cog(SupportBot(bot))
