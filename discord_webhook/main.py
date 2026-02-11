import nextcord
from nextcord.ext import commands
from clases import ProblemReportModal, setup, ProblemReportButtonView
from dotenv import load_dotenv
import os
import logging
from database import init_db, get_session_by_discord_id, get_conversation_status

load_dotenv()

logger = logging.getLogger("discord_webhook")

init_db()

intents = nextcord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)


async def send_or_update_support_message(bot: commands.Bot):
    channel_id = int(os.getenv("DISCORD_CHANNEL_ID", "0"))
    if not channel_id:
        logger.error("DISCORD_CHANNEL_ID not set")
        return

    channel = bot.get_channel(channel_id)
    if channel is None:
        logger.error("Channel %s not found", channel_id)
        return

    embed = nextcord.Embed(
        title="Возникли проблемы? Пиши — мы поможем!",
        description=(
            "Если возникли проблемы со входом в игру или с донат-услугами, "
            "если что-то сломалось, не запускается или работает не так, "
            "как должно — пиши нам и мы с удовольствием поможем\n\n"
            "**Команда /support** — написать сообщение в поддержку\n\n"
            "**Рабочее время нашей техподдержки:**\n"
            "Будние дни: с 12:00 до 22:00 по МСК\n"
            "Выходные дни: с 14:00 до 20:00 МСК"
        ),
        color=nextcord.Color.blue(),
    )
    banner_url = os.getenv("DISCORD_BANNER_URL", "")
    if banner_url:
        embed.set_image(url=banner_url)

    view = ProblemReportButtonView()

    async for message in channel.history(limit=50):
        if message.components:
            for comp in message.components:
                for item in comp.children:
                    if hasattr(item, "custom_id") and item.custom_id == "problem_report_button":
                        await message.edit(embed=embed, view=view)
                        logger.info("Updated existing support message")
                        return

    await channel.send(embed=embed, view=view)
    logger.info("Sent new support message")


@bot.event
async def on_ready():
    logger.info("Bot %s ready", bot.user)
    setup(bot)
    await send_or_update_support_message(bot)


@bot.slash_command(description="Сообщить о проблеме")
async def support(interaction: nextcord.Interaction):
    await interaction.response.send_modal(ProblemReportModal())


@bot.slash_command(description="Проверить статус тикета")
async def ticket_status(interaction: nextcord.Interaction):
    session = get_session_by_discord_id(str(interaction.user.id))
    if session:
        if get_conversation_status(session[3]):
            await interaction.response.send_message(
                f"Ваш тикет #{session[3]} активен", ephemeral=True
            )
        else:
            await interaction.response.send_message(
                f"Ваш тикет #{session[3]} закрыт", ephemeral=True
            )
    else:
        await interaction.response.send_message("У вас нет активных тикетов", ephemeral=True)


if __name__ == "__main__":
    bot.run(os.getenv("DISCORD_BOT_TOKEN"))
