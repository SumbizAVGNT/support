"""Centralized configuration loaded from environment variables."""

import os

# App
APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
APP_PORT = int(os.getenv("APP_PORT", "5501"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
DB_PATH = os.getenv("STATE_DB", "state.db")

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_SECRET_TOKEN = os.getenv(
    "TELEGRAM_WEBHOOK_SECRET_TOKEN",
    os.getenv("TELEGRAM_SECRET_TOKEN", ""),
).strip()
TELEGRAM_WEBHOOK_PUBLIC_URL = os.getenv("TELEGRAM_WEBHOOK_PUBLIC_URL", "").rstrip("/")
TELEGRAM_ALLOWED_UPDATES = [
    u.strip()
    for u in os.getenv("TELEGRAM_ALLOWED_UPDATES", "message,edited_message,callback_query").split(",")
    if u.strip()
]

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN is required")

# Chatwoot
CHATWOOT_BASE_URL = os.getenv("CHATWOOT_BASE_URL", "").rstrip("/")
CHATWOOT_ACCOUNT_ID = os.getenv("CHATWOOT_ACCOUNT_ID", "1").strip()
CHATWOOT_INBOX_ID = int(os.getenv("CHATWOOT_INBOX_ID", os.getenv("CHATWOOT_TELEGRAM_INBOX_ID", "5")))
CHATWOOT_API_TOKEN = os.getenv("CHATWOOT_API_TOKEN", "").strip()
CW_ACCESS_TOKEN = os.getenv("CHATWOOT_ACCESS_TOKEN", "").strip()
CW_CLIENT = os.getenv("CHATWOOT_CLIENT", "").strip()
CW_UID = os.getenv("CHATWOOT_UID", "").strip()
CW_PASSWORD = os.getenv("CHATWOOT_PASSWORD", os.getenv("CW_PASSWORD", "")).strip()

if not CHATWOOT_BASE_URL:
    raise RuntimeError("CHATWOOT_BASE_URL is required")

# Webhook verification
USE_HMAC = os.getenv("USE_HMAC", "false").lower() == "true"
CHATWOOT_WEBHOOK_SECRET = os.getenv("CHATWOOT_HMAC_TOKEN", os.getenv("CHATWOOT_WEBHOOK_SECRET", "")).strip()
CHATWOOT_WEBHOOK_TOKEN = os.getenv("CHATWOOT_WEBHOOK_TOKEN", "").strip()

# File proxy
FILE_PROXY_PUBLIC_BASE = os.getenv("FILE_PROXY_PUBLIC_BASE", "").rstrip("/")

# Agent auth (optional)
AGENT_USER = os.getenv("AGENT_USER", "").strip()
AGENT_PASS = os.getenv("AGENT_PASS", "").strip()
