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

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ProblemReportButtonView(View):
    def __init__(self):
        super().__init__(timeout=None)  # кнопка не пропадет

    @nextcord.ui.button(
        label="Сообщить о проблеме",
        style=nextcord.ButtonStyle.primary,
        custom_id="problem_report_button"
    )
    async def problem_report_button(self, button: Button, interaction: nextcord.Interaction):
        modal = ProblemReportModal()
        await interaction.response.send_modal(modal)


class ProblemReportModal(Modal):
    def __init__(self):
        super().__init__(
            title="Сообщить о проблеме",
            timeout=300,
            custom_id="problem_report_modal"
        )

        self.nickname = TextInput(
            label="Ваш никнейм",
            placeholder="Введите ваш никнейм или имя",
            min_length=2,
            max_length=32,
            required=True
        )
        self.add_item(self.nickname)

        self.problem_description = TextInput(
            label="Опишите проблему",
            style=nextcord.TextInputStyle.paragraph,
            placeholder="Подробно опишите вашу проблему...",
            min_length=10,
            max_length=1024,
            required=True
        )
        self.add_item(self.problem_description)

    async def callback(self, interaction: nextcord.Interaction):
        discord_user_id = str(interaction.user.id)

        try:
            # Проверка активного тикета
            if self._has_active_ticket(discord_user_id):
                await interaction.response.send_message(
                    "⚠️ У вас уже есть активный тикет. Пожалуйста, используйте его для общения с поддержкой.",
                    ephemeral=True
                )
                return

            # Создание нового тикета
            response, used_url = await self._create_support_ticket(discord_user_id)
            await self._handle_ticket_creation_response(interaction, response, discord_user_id, used_url)

        except Exception as e:
            logger.error(f"Ошибка при обработке модалки: {e}", exc_info=True)
            await self._send_error_response(interaction)

    def _has_active_ticket(self, discord_user_id: str) -> bool:
        """Проверяет наличие активного тикета у пользователя."""
        session = get_session_by_discord_id(discord_user_id)
        return bool(session and get_conversation_status(session[3]))

    def _candidate_urls(self) -> list[str]:
        env_url = os.getenv("FLASK_SERVER_URL", "").strip()

        
        if env_url.startswith("https://chatwoot.teighto.net"):
            env_url = env_url.replace("https://chatwoot.teighto.net", "https://discordbot.teighto.net")

        candidates = []
        if env_url:
            
            if not env_url.rstrip("/").endswith("/create_contact"):
                env_url = env_url.rstrip("/") + "/create_contact"
            candidates.append(env_url)

       
        candidates += [
            "https://discordbot.teighto.net/create_contact",
            "http://webhook:5500/create_contact",
            "http://localhost:5500/create_contact",
        ]
        # уберем дубликаты, сохраняя порядок
        seen = set()
        uniq = []
        for u in candidates:
            if u not in seen:
                uniq.append(u)
                seen.add(u)
        return uniq

    async def _create_support_ticket(self, discord_user_id: str):
       
        payload = {
            "name": self.nickname.value,
            "email": f"{discord_user_id}@discord",
            "discord_user": discord_user_id,
            "problem_text": self.problem_description.value
        }

        candidates = self._candidate_urls()
        timeout = aiohttp.ClientTimeout(total=15)
        last_response = None
        last_url = None

        async with aiohttp.ClientSession(timeout=timeout) as session_http:
            for url in candidates:
                try:
                    logger.info(f"[create_contact] POST {url}")
                    resp = await session_http.post(url, json=payload)
                    last_response = resp
                    last_url = url

                    # если это 404 — попробуем следующий кандидат
                    if resp.status == 404:
                        logger.warning(f"[create_contact] {url} → 404 Not Found, пробую следующий")
                        continue

                    # на любой не-404 возвращаем сразу (пусть обработается выше)
                    return resp, url

                except asyncio.TimeoutError:
                    logger.error(f"[create_contact] {url} → timeout")
                except aiohttp.ClientError as e:
                    logger.error(f"[create_contact] {url} → client error: {e}")

        # если добрались сюда — либо все были 404, либо ошибки/таймауты
        return last_response, last_url

    async def _handle_ticket_creation_response(
        self,
        interaction: nextcord.Interaction,
        response: aiohttp.ClientResponse | None,
        discord_user_id: str,
        used_url: str | None
    ):
        if response is None:
            await interaction.response.send_message(
                "⚠️ Сервер недоступен. Попробуйте позже.",
                ephemeral=True
            )
            return

        status = response.status
        
        body_bytes = await response.read()
        body_snippet = (body_bytes.decode(errors="replace")[:500]) if body_bytes else ""
        logger.info(f"[create_contact] used_url={used_url} status={status} body={body_snippet}")

        # 404 — почти наверняка неверный домен/путь (раньше он и был на chatwoot.*)
        if status == 404:
            await interaction.response.send_message(
                "⚠️ Ошибка сервера (404). Проверьте адрес сервиса создания тикета. "
                "Он должен быть вида https://discordbot.teighto.net/create_contact",
                ephemeral=True
            )
            return

        
        if not (200 <= status < 300):
            await interaction.response.send_message(
                f"⚠️ Ошибка сервера ({status})",
                ephemeral=True
            )
            return

        
        try:
            data = json.loads(body_bytes.decode("utf-8"))
        except Exception:
            await interaction.response.send_message(
                "⚠️ Некорректный ответ сервера.",
                ephemeral=True
            )
            return

        if data.get('success'):
            conversation_id = data.get('conversation_id')

            mark_message_processed(f"ticket_created_{discord_user_id}")

            # Ответ с подтверждением
            await interaction.response.send_message(
                f"✅ Запрос #{conversation_id} создан!",
                ephemeral=True
            )

            # ЛС пользователю
            embed = nextcord.Embed(
                title=f"Информация о вашем запросе #{conversation_id}",
                color=nextcord.Color.blurple(),  # фиолетовый
                timestamp=datetime.utcnow()
            )
            embed.add_field(name="Никнейм", value=self.nickname.value, inline=False)
            embed.add_field(name="Номер тикета", value=str(conversation_id), inline=True)
            embed.add_field(name="Описание проблемы", value=self.problem_description.value, inline=False)

            try:
                user = await interaction.user.create_dm()
                await user.send(embed=embed)
            except Exception as e:
                logger.error(f"Не удалось отправить ЛС с информацией о запросе: {e}")
        else:
            await interaction.response.send_message(
                f"❌ Ошибка: {data.get('error', 'Неизвестная ошибка')}",
                ephemeral=True
            )

    async def _send_error_response(self, interaction: nextcord.Interaction):
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "⚠️ Ошибка соединения с сервером.",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    "⚠️ Ошибка соединения с сервером.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Failed to send error response: {e}")


class SupportBot(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.lock = asyncio.Lock()
        self.WARNING_MESSAGES = {
            'no_ticket': "ℹ️ Сначала создайте запрос через /support",
            'ticket_closed': "🔒 Этот тикет уже закрыт. Создайте новый запрос через /support",
            'send_error': "⚠️ Ошибка отправки в поддержку"
        }

    @commands.Cog.listener()
    async def on_message(self, message):
        # Игнорируем ботов
        if message.author.bot:
            return

        # Только DM
        if not isinstance(message.channel, nextcord.DMChannel):
            return

        try:
            async with self.lock:
                await self._process_user_message(message)
        except Exception as e:
            logger.error(f"Ошибка обработки сообщения: {e}", exc_info=True)

    async def _process_user_message(self, message):
        message_id = str(message.id)
        discord_user_id = str(message.author.id)

        # антидубликат
        if is_message_processed(message_id):
            logger.info(f"Message {message_id} already processed, skipping.")
            return

        session = get_session_by_discord_id(discord_user_id)
        if not session:
            await self._send_warning_if_needed(message.channel, 'no_ticket')
            return

        conversation_id = session[3]
        if not get_conversation_status(conversation_id):
            await self._send_warning_if_needed(message.channel, 'ticket_closed')
            return

        mark_message_processed(message_id)

        # Отправка в Chatwoot (вложения поддерживаются в utils.send_chatwoot_message)
        ok = await send_chatwoot_message(conversation_id, message.content, attachments=message.attachments)
        if not ok:
            await self._send_warning_if_needed(message.channel, 'send_error')

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
