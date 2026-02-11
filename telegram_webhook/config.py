import os

APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
APP_PORT = int(os.getenv("APP_PORT", "5501"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_SECRET_TOKEN = os.getenv("TELEGRAM_SECRET_TOKEN", "").strip()
CHATWOOT_TELEGRAM_INBOX_ID = int(os.getenv("CHATWOOT_TELEGRAM_INBOX_ID", "5"))

assert TELEGRAM_BOT_TOKEN, "TELEGRAM_BOT_TOKEN is required"

# Chatwoot base
CHATWOOT_BASE_URL = os.getenv("CHATWOOT_BASE_URL", "").rstrip("/")
CHATWOOT_ACCOUNT_ID = int(os.getenv("CHATWOOT_ACCOUNT_ID", "1"))
assert CHATWOOT_BASE_URL, "CHATWOOT_BASE_URL is required"

# Chatwoot auth: API token (реком.) или devise заголовки
CHATWOOT_API_TOKEN = os.getenv("CHATWOOT_API_TOKEN", "").strip()
CW_ACCESS_TOKEN = os.getenv("CHATWOOT_ACCESS_TOKEN", "").strip()
CW_CLIENT = os.getenv("CHATWOOT_CLIENT", "").strip()
CW_UID = os.getenv("CHATWOOT_UID", "").strip()

# Webhook verification
USE_HMAC = os.getenv("USE_HMAC", "false").lower() == "true"
CHATWOOT_WEBHOOK_SECRET = os.getenv("CHATWOOT_WEBHOOK_SECRET", "").strip()
CHATWOOT_WEBHOOK_TOKEN = os.getenv("CHATWOOT_WEBHOOK_TOKEN", "").strip()

# HTTP
DEFAULT_TIMEOUT = 15
USER_AGENT = "telegram-chatwoot-bridge/1.0"
