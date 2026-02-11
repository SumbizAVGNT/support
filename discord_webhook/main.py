import nextcord
from nextcord.ext import commands
from clases import ProblemReportModal, setup, ProblemReportButtonView
from dotenv import load_dotenv
import os
from database import init_db, get_session_by_discord_id, get_conversation_status

load_dotenv()
init_db()

intents = nextcord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)

async def send_or_update_support_message(bot: commands.Bot):
    channel_id = int(os.getenv("DISCORD_CHANNEL_ID"))
    channel = bot.get_channel(channel_id)
    if channel is None:
        print(f"Канал с ID {channel_id} не найден!")
        return

    # Текст для Embed
    embed = nextcord.Embed(
        title="Возникли проблемы? Пиши — мы поможем! 🙏",
        description=(
            "Если возникли проблемы со входом в игру или с донат-услугами, если что-то сломалось, не запускается или работает не так, как должно — пиши нам и мы с удовольствием поможем 😇❤️\n\n"
            "**Команда /support** — написать сообщение в поддержку\n\n"
            "**Рабочее время нашей техподдержки:**\n"
            "Будние дни: с 12:00 до 22:00 по МСК\n"
            "Выходные дни: с 14:00 до 20:00 МСК"
        ),
        color=nextcord.Color.blue()
    )
    # Пример места для картинки — добавь свою ссылку или оставь пустым
    embed.set_image(url="https://media.discordapp.net/attachments/1377006686446948372/1433567258126319926/banner2.png?ex=6905290c&is=6903d78c&hm=f8e001c7cc7f48d7d6cc81e8724d32e3c0836323a5afab04547acc39ac6d8bcc&=&format=webp&quality=lossless&width=976&height=305")  # Замени на свою картинку или удали

    view = ProblemReportButtonView()

    # Проверяем последние 50 сообщений канала на наше сообщение (по custom_id кнопки)
    async for message in channel.history(limit=50):
        if message.components:
            for comp in message.components:
                for item in comp.children:
                    if hasattr(item, "custom_id") and item.custom_id == "problem_report_button":
                        # Обновляем сообщение (embed и view)
                        await message.edit(embed=embed, view=view)
                        print("Обновлено существующее сообщение с кнопкой.")
                        return

    # Если не нашли — отправляем новое
    await channel.send(embed=embed, view=view)
    print("Отправлено новое сообщение с кнопкой.")

@bot.event
async def on_ready():
    print(f'Бот {bot.user} готов!')
    setup(bot)
    await send_or_update_support_message(bot)
    print("2331")

@bot.slash_command(description="Сообщить о проблеме")
async def support(interaction: nextcord.Interaction):
    await interaction.response.send_modal(ProblemReportModal())

@bot.slash_command(description="Проверить статус тикета")
async def ticket_status(interaction: nextcord.Interaction):
    session = get_session_by_discord_id(str(interaction.user.id))
    if session:
        if get_conversation_status(session[3]):
            await interaction.response.send_message(
                f"✅ Ваш тикет #{session[3]} активен",
                ephemeral=True
            )
        else:
            await interaction.response.send_message(
                f"🔒 Ваш тикет #{session[3]} закрыт",
                ephemeral=True
            )
    else:
        await interaction.response.send_message(
            "ℹ️ У вас нет активных тикетов",
            ephemeral=True
        )

if __name__ == "__main__":
    bot.run(os.getenv('DISCORD_BOT_TOKEN'))