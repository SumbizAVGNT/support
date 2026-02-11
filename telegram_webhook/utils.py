import logging
import requests
from config import DEFAULT_TIMEOUT, USER_AGENT

logger = logging.getLogger("telegram_webhook")

SESSION = requests.Session()
SESSION.headers["User-Agent"] = USER_AGENT
# timeout задаём на каждом запросе явно

def safe_json(resp: requests.Response):
    try:
        return resp.json()
    except Exception:
        logger.warning("Non-JSON response: %s", (resp.text or "")[:500])
        return {}
