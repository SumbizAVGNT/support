import os
import json
import time
import uuid
import hmac
import hashlib
import mimetypes
import asyncio
import logging
import urllib.parse
import requests
import threading
from flask import Flask, request, jsonify, Response, g
from flask_cors import CORS

from main import bot
from database import (
    db_connection, init_db, get_or_create_session,
    get_session_by_conversation_id, get_session_by_contact_id,
    close_session, mark_message_processed, is_message_processed,
    cleanup_old_messages, DATABASE_NAME,
)
from utils import send_discord_message, get_chatwoot_headers

# ---------------------------------------------------------------------------- #
# Logging
# ---------------------------------------------------------------------------- #
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_JSON = os.getenv("LOG_JSON", "false").lower() == "true"
LOG_BODY_MAX = int(os.getenv("LOG_BODY_MAX", "2000"))
REDACT_KEYS = {
    k.strip().lower()
    for k in os.getenv(
        "REDACT_KEYS",
        "authorization,api_access_token,access-token,client,uid,token,secret,password",
    ).split(",")
}

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("discord_webhook")


def _redact_headers(h: dict) -> dict:
    if not h:
        return {}
    return {k: ("***" if k.lower() in REDACT_KEYS else v) for k, v in h.items()}


def _cut_body(b) -> str:
    if b is None:
        return ""
    if isinstance(b, bytes):
        b = b.decode(errors="replace")
    return b[:LOG_BODY_MAX] + ("...(truncated)" if len(b) > LOG_BODY_MAX else "")


def _json_log(ev: str, **fields):
    if LOG_JSON:
        print(json.dumps({"event": ev, **fields}, ensure_ascii=False))
    else:
        parts = [f"[{ev}]"] + [f"{k}={v}" for k, v in fields.items()]
        logger.info(" ".join(parts))


# ---------------------------------------------------------------------------- #
# Flask
# ---------------------------------------------------------------------------- #
app = Flask(__name__)
CORS(app)

# ---------------------------------------------------------------------------- #
# Config from env
# ---------------------------------------------------------------------------- #
CHATWOOT_HMAC_TOKEN = (os.getenv("CHATWOOT_HMAC_TOKEN") or "").encode()
CHATWOOT_BASE_URL = os.getenv("CHATWOOT_BASE_URL", "").rstrip("/")
CHATWOOT_ACCOUNT_ID = os.getenv("CHATWOOT_ACCOUNT_ID", "1")
CHATWOOT_INBOX_ID = int(os.getenv("CHATWOOT_INBOX_ID", "1"))
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "").strip()

PUBLIC_HOST = os.getenv("PUBLIC_HOST", CHATWOOT_BASE_URL).rstrip("/")
HOST_REWRITE_MAP = os.getenv("HOST_REWRITE_MAP", "")
_REWRITE_PAIRS: list[tuple[str, str]] = []
for pair in (HOST_REWRITE_MAP or "").split(","):
    pair = pair.strip()
    if "->" in pair:
        src, dst = pair.split("->", 1)
        _REWRITE_PAIRS.append((src.rstrip("/"), dst.rstrip("/")))

FILE_PROXY_FETCH_BASE = os.getenv("FILE_PROXY_FETCH_BASE", "http://127.0.0.1:5500").rstrip("/")
FILE_PROXY_PUBLIC_BASE = os.getenv("FILE_PROXY_PUBLIC_BASE", "").rstrip("/")
CF_ACCESS_CLIENT_ID = os.getenv("CF_ACCESS_CLIENT_ID", "")
CF_ACCESS_CLIENT_SECRET = os.getenv("CF_ACCESS_CLIENT_SECRET", "")
INTERNAL_FETCH_BASE = os.getenv("INTERNAL_FETCH_BASE", CHATWOOT_BASE_URL).rstrip("/")


# ---------------------------------------------------------------------------- #
# Middleware
# ---------------------------------------------------------------------------- #
@app.before_request
def _before_request():
    g._ts = time.perf_counter()
    g._rid = request.headers.get("X-Request-Id") or uuid.uuid4().hex[:12]
    if logger.isEnabledFor(logging.DEBUG):
        _json_log(
            "http_in",
            rid=g._rid,
            method=request.method,
            path=request.path,
        )


@app.after_request
def _after_request(resp: Response):
    try:
        dur_ms = int((time.perf_counter() - getattr(g, "_ts", time.perf_counter())) * 1000)
        if logger.isEnabledFor(logging.DEBUG):
            _json_log("http_out", rid=getattr(g, "_rid", "-"), status=resp.status_code, duration_ms=dur_ms)
        resp.headers["X-Request-Id"] = getattr(g, "_rid", "-")
    except Exception:
        pass
    return resp


# ---------------------------------------------------------------------------- #
# Security
# ---------------------------------------------------------------------------- #
def verify_chatwoot_signature(req) -> bool:
    if not CHATWOOT_HMAC_TOKEN:
        return True
    sig = req.headers.get("X-Chatwoot-Signature", "")
    try:
        body = req.get_data(cache=False) or b""
        digest = hmac.new(CHATWOOT_HMAC_TOKEN, body, hashlib.sha256).hexdigest()
        return hmac.compare_digest(digest, sig)
    except Exception:
        return False


def _check_admin_auth() -> bool:
    if not ADMIN_TOKEN:
        return False
    auth = request.headers.get("Authorization", "")
    return auth == f"Bearer {ADMIN_TOKEN}"


# ---------------------------------------------------------------------------- #
# URL utilities
# ---------------------------------------------------------------------------- #
def _public_netloc() -> str:
    return urllib.parse.urlparse(PUBLIC_HOST).netloc


def _normalize_to_public(u: str) -> str:
    if not u:
        return u
    for src, dst in _REWRITE_PAIRS:
        if u.startswith(src):
            u = dst + u[len(src):]
    parsed = urllib.parse.urlparse(u)
    host = parsed.hostname or ""
    if host in {"localhost", "127.0.0.1"} or host.startswith("172."):
        pub = urllib.parse.urlparse(PUBLIC_HOST)
        u = urllib.parse.urlunparse((pub.scheme, pub.netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))
    return u


def _rewrite_public_to_internal(u: str) -> str:
    if not u:
        return u
    pub = urllib.parse.urlparse(PUBLIC_HOST)
    url = urllib.parse.urlparse(u)
    if url.netloc == pub.netloc:
        internal = urllib.parse.urlparse(INTERNAL_FETCH_BASE)
        return urllib.parse.urlunparse((internal.scheme, internal.netloc, url.path, url.params, url.query, url.fragment))
    return u


def get_filename_from_url(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    filename = os.path.basename(parsed.path)
    return filename if filename else "file"


def _public_proxy_url(raw_url: str) -> str:
    if not raw_url or not FILE_PROXY_PUBLIC_BASE:
        return ""
    normalized = _normalize_to_public(raw_url)
    return f"{FILE_PROXY_PUBLIC_BASE}/proxy/file?url={urllib.parse.quote(normalized, safe='')}&v={int(time.time())}"


def _internal_proxy_url(raw_url: str) -> str:
    if not raw_url:
        return ""
    normalized = _normalize_to_public(raw_url)
    return f"{FILE_PROXY_FETCH_BASE}/proxy/file?url={urllib.parse.quote(normalized, safe='')}&v={int(time.time())}"


# ---------------------------------------------------------------------------- #
# Chatwoot API
# ---------------------------------------------------------------------------- #
_http_session = requests.Session()
_http_session.headers["User-Agent"] = "discord-chatwoot-bridge/2.0"


def make_chatwoot_request(method, url, json_data=None):
    headers = get_chatwoot_headers()
    resp = _http_session.request(method, url, json=json_data, headers=headers, timeout=30)
    resp.raise_for_status()
    try:
        return resp.json()
    except ValueError:
        return {"raw": resp.text}


# Agent cache
_AGENTS_CACHE: dict[int, dict] = {}
_AGENTS_CACHE_TS: float = 0.0
_AGENTS_TTL: float = 60.0


def _refresh_agents_cache():
    global _AGENTS_CACHE, _AGENTS_CACHE_TS
    try:
        url = f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/agents"
        data = make_chatwoot_request("GET", url)
        agents = data if isinstance(data, list) else data.get("payload") or data.get("agents") or []
        cache = {}
        for a in agents:
            aid = a.get("id")
            if aid is not None:
                cache[int(aid)] = {
                    "id": int(aid),
                    "name": a.get("name") or a.get("available_name"),
                    "avatar_url": a.get("avatar_url") or a.get("thumbnail") or "",
                }
        _AGENTS_CACHE = cache
        _AGENTS_CACHE_TS = time.time()
        logger.info("[agents_cache] loaded %d agents", len(cache))
    except Exception as e:
        logger.warning("[agents_cache] load failed: %s", e)


def _get_agent_info(agent_id: int) -> dict | None:
    if not _AGENTS_CACHE or time.time() - _AGENTS_CACHE_TS > _AGENTS_TTL:
        _refresh_agents_cache()
    return _AGENTS_CACHE.get(int(agent_id))


# ---------------------------------------------------------------------------- #
# Attachments
# ---------------------------------------------------------------------------- #
def extract_attachments(raw_attachments):
    attachments = []
    for att in raw_attachments or []:
        try:
            data_url = att.get("data_url", "")
            filename = att.get("file_name") or get_filename_from_url(data_url)
            file_type = (att.get("file_type") or "").lower()
            if data_url:
                normalized = _normalize_to_public(data_url)
                proxy_url = f"{FILE_PROXY_FETCH_BASE}/proxy/file?url={urllib.parse.quote(normalized, safe='')}"
                attachments.append({
                    "url": proxy_url,
                    "filename": filename,
                    "content_type": file_type,
                    "original_url": data_url,
                })
        except Exception as e:
            logger.error("Error processing attachment: %s", e)
    return attachments


# ---------------------------------------------------------------------------- #
# File proxy
# ---------------------------------------------------------------------------- #
def _upstream_headers() -> dict:
    h = {"User-Agent": "FileProxy/2.0", "Accept": "*/*", "Host": _public_netloc()}
    if CF_ACCESS_CLIENT_ID and CF_ACCESS_CLIENT_SECRET:
        h["CF-Access-Client-Id"] = CF_ACCESS_CLIENT_ID
        h["CF-Access-Client-Secret"] = CF_ACCESS_CLIENT_SECRET
    return h


@app.route("/proxy/file", methods=["GET"])
def proxy_file():
    raw_url = request.args.get("url", "").strip()
    if not raw_url:
        return jsonify({"error": "No URL provided"}), 400
    try:
        public_url = _normalize_to_public(raw_url)
        upstream_url = _rewrite_public_to_internal(public_url)

        # Retry with exponential backoff for 404s (ActiveStorage race condition)
        max_retries = 4
        last_status = None
        response = None
        for attempt in range(max_retries):
            response = _http_session.get(upstream_url, stream=True, timeout=30, headers=_upstream_headers())
            last_status = response.status_code
            if last_status == 404 and attempt < max_retries - 1:
                wait = 2 ** attempt
                logger.warning("Proxy file 404, retry %d in %ds: %s", attempt + 1, wait, upstream_url)
                time.sleep(wait)
                continue
            break

        if response is None or response.status_code >= 400:
            return jsonify({"error": f"upstream {last_status}"}), 502

        body = response.content
        if not body:
            return jsonify({"error": "Empty file"}), 502

        content_type = response.headers.get("Content-Type", "application/octet-stream")
        cd = response.headers.get("Content-Disposition", "")
        filename = cd.split("filename=")[1].strip().strip('"') if "filename=" in cd else get_filename_from_url(public_url)

        return Response(
            body,
            content_type=content_type,
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "Access-Control-Allow-Origin": "*",
            },
        )
    except requests.exceptions.Timeout:
        return jsonify({"error": "Download timeout"}), 504
    except requests.exceptions.RequestException as e:
        return jsonify({"error": str(e)}), 502
    except Exception as e:
        logger.error("Proxy error: %s", e, exc_info=True)
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------- #
# Chatwoot helpers
# ---------------------------------------------------------------------------- #
def search_or_create_contact(name, email, phone=None):
    search_url = f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/contacts/search?q={urllib.parse.quote(email)}"
    result = make_chatwoot_request("GET", search_url)
    contacts = result.get("payload", [])
    if contacts:
        cid = contacts[0].get("id") or contacts[0].get("contact", {}).get("id")
        if cid:
            return cid
    payload = {"name": name, "email": email, "phone_number": phone if phone and phone.startswith("+") else None}
    contact_url = f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/contacts"
    result = make_chatwoot_request("POST", contact_url, payload)
    contact_id = result.get("payload", {}).get("contact", {}).get("id") or result.get("id")
    if not contact_id:
        raise ValueError("Failed to create contact in Chatwoot")
    return contact_id


def create_conversation(contact_id, source_id):
    payload = {"source_id": source_id, "inbox_id": CHATWOOT_INBOX_ID, "contact_id": contact_id}
    url = f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations"
    result = make_chatwoot_request("POST", url, payload)
    conv_id = result.get("id") or result.get("conversation", {}).get("id")
    if not conv_id:
        raise ValueError(f"Failed to create conversation: {result}")
    return conv_id


def send_chatwoot_message_sync(conversation_id, content):
    url = f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations/{conversation_id}/messages"
    payload = {"content": content, "message_type": 0, "private": False}
    make_chatwoot_request("POST", url, payload)


# ---------------------------------------------------------------------------- #
# Healthcheck
# ---------------------------------------------------------------------------- #
@app.route("/healthz", methods=["GET"])
def healthz():
    return jsonify({"ok": True, "public_host": PUBLIC_HOST})


# ---------------------------------------------------------------------------- #
# Async helpers (Discord)
# ---------------------------------------------------------------------------- #
async def notify_user_about_closed_ticket(user_id: int, ticket_id: int):
    try:
        user = await bot.fetch_user(user_id)
        await send_discord_message(
            user=user,
            content=f"Ваш тикет #{ticket_id} был закрыт. Если у вас есть новые вопросы, создайте новый запрос через /support",
        )
    except Exception as e:
        logger.error("Error sending close notification: %s", e)


async def fetch_and_send(discord_user_id, content, attachments, agent_name, avatar_dl, avatar_ext):
    try:
        user = await bot.fetch_user(discord_user_id)
        await send_discord_message(
            user=user,
            content=content or "",
            agent_name=(agent_name or "").strip() or None,
            agent_avatar_download_url=(avatar_dl or "").strip() or None,
            agent_avatar_external_url=(avatar_ext or "").strip() or None,
            attachments=attachments or [],
        )
    except Exception as e:
        logger.error("Error in fetch_and_send: %s", e)


# ---------------------------------------------------------------------------- #
# Webhook
# ---------------------------------------------------------------------------- #
@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        if not verify_chatwoot_signature(request):
            return jsonify({"status": "unauthorized"}), 401

        data = request.get_json(silent=True) or {}
        event = data.get("event")

        if event not in ["message_created", "message_updated", "conversation_updated"]:
            return jsonify({"status": "ignored"}), 200

        if event in ["message_created", "message_updated"]:
            message_type = data.get("message_type")
            message_id = data.get("id")

            if message_type != "outgoing":
                return jsonify({"status": "not outgoing"}), 200

            if is_message_processed(message_id):
                return jsonify({"status": "duplicate"}), 200
            mark_message_processed(message_id)

            content = data.get("content") or ""
            conversation = data.get("conversation") or {}
            contact_id = (conversation.get("contact_inbox") or {}).get("contact_id")

            # Resolve agent
            assignee_id = conversation.get("assignee_id") or (conversation.get("meta") or {}).get("assignee_id")
            sender = data.get("sender") or {}
            agent_id = assignee_id or (data.get("user") or {}).get("id") or sender.get("id")

            agent_name = None
            raw_avatar = None
            if agent_id:
                info = _get_agent_info(int(agent_id))
                if info:
                    agent_name = (info.get("name") or "").strip() or None
                    raw_avatar = info.get("avatar_url") or ""

            if not agent_name:
                agent_name = (sender.get("name") or (data.get("user") or {}).get("name") or "").strip() or "Support"
            if not raw_avatar:
                raw_avatar = sender.get("avatar_url") or sender.get("avatar") or (data.get("user") or {}).get("avatar_url") or ""

            avatar_dl = _internal_proxy_url(raw_avatar) if raw_avatar else None
            avatar_ext = _public_proxy_url(raw_avatar) if raw_avatar else None

            if not contact_id:
                return jsonify({"status": "no contact_id"}), 400

            session = get_session_by_contact_id(contact_id)
            if not session:
                return jsonify({"status": "no discord session"}), 200

            discord_user_id = session[1]
            attachments = extract_attachments(data.get("attachments"))

            asyncio.run_coroutine_threadsafe(
                fetch_and_send(int(discord_user_id), content, attachments, agent_name, avatar_dl, avatar_ext),
                bot.loop,
            )
            return jsonify({"status": "forwarded"}), 200

        elif event == "conversation_updated":
            changed_attrs = data.get("changed_attributes") or []
            conversation_id = data.get("id")
            contact_id = (data.get("contact_inbox") or {}).get("contact_id")
            if not contact_id:
                return jsonify({"status": "no contact_id"}), 400

            if any((attr.get("status") or {}).get("current_value") == "resolved" for attr in changed_attrs):
                session = get_session_by_conversation_id(conversation_id)
                if session:
                    discord_user_id = session[1]
                    if close_session(conversation_id):
                        asyncio.run_coroutine_threadsafe(
                            notify_user_about_closed_ticket(int(discord_user_id), conversation_id),
                            bot.loop,
                        )
                        return jsonify({"status": "closed"}), 200
                return jsonify({"status": "no session"}), 200

        return jsonify({"status": "ignored"}), 200
    except Exception as e:
        logger.error("Webhook error: %s", e, exc_info=True)
        return jsonify({"status": "error"}), 500


# ---------------------------------------------------------------------------- #
# Admin
# ---------------------------------------------------------------------------- #
@app.route("/admin/clear_db", methods=["POST"])
def clear_database():
    if not _check_admin_auth():
        return jsonify({"error": "Unauthorized"}), 401
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            tables = ["processed_messages", "sessions", "ticket_history"]
            for table in tables:
                cur.execute(f"DELETE FROM {table}")
            conn.commit()
        return jsonify({"status": "success", "message": "Database cleared"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/admin/db_status", methods=["GET"])
def db_status():
    if not _check_admin_auth():
        return jsonify({"error": "Unauthorized"}), 401
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            tables = ["sessions", "processed_messages", "ticket_history"]
            status = {}
            for table in tables:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                status[table] = cur.fetchone()[0]
        return jsonify({"status": "success", "database": DATABASE_NAME, "table_counts": status})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/admin/reset_sessions", methods=["POST"])
def reset_sessions():
    if not _check_admin_auth():
        return jsonify({"error": "Unauthorized"}), 401
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM sessions")
            cur.execute("DELETE FROM processed_messages")
            conn.commit()
        return jsonify({"status": "success", "message": "Sessions cleared"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------- #
# Contact creation + avatar sync
# ---------------------------------------------------------------------------- #
def _discord_get_user(discord_user_id: str) -> dict | None:
    token = os.getenv("DISCORD_BOT_TOKEN", "").strip()
    if not token or not discord_user_id:
        return None
    try:
        r = _http_session.get(
            f"https://discord.com/api/v10/users/{discord_user_id}",
            headers={"Authorization": f"Bot {token}"},
            timeout=15,
        )
        if r.status_code == 200:
            return r.json()
        logger.warning("[avatar] discord api -> %s", r.status_code)
    except Exception as e:
        logger.warning("[avatar] discord api error: %s", e)
    return None


def _discord_avatar_url_and_hash(user_json: dict, discord_user_id: str) -> tuple[str | None, str | None]:
    if not user_json:
        return None, None
    avatar = user_json.get("avatar")
    if avatar:
        ext = "gif" if str(avatar).startswith("a_") else "png"
        return f"https://cdn.discordapp.com/avatars/{discord_user_id}/{avatar}.{ext}?size=256", str(avatar)
    disc = user_json.get("discriminator", "0")
    try:
        idx = int(disc) % 5
    except Exception:
        try:
            idx = (int(discord_user_id) >> 22) % 6
        except Exception:
            idx = 0
    return f"https://cdn.discordapp.com/embed/avatars/{idx}.png", f"default_{idx}"


def _chatwoot_update_contact_avatar(contact_id: int, avatar_url: str, avatar_hash: str) -> bool:
    try:
        url = f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/contacts/{contact_id}"
        make_chatwoot_request("PUT", url, {"avatar_url": avatar_url, "custom_attributes": {"discord_avatar_hash": avatar_hash}})
        return True
    except Exception as e:
        logger.warning("[avatar] update failed: %s", e)
        return False


def _create_contact_impl(data: dict):
    missing = [f for f in ["name", "email"] if not (str(data.get(f) or "").strip())]
    if missing:
        raise ValueError(f"Missing required fields: {', '.join(missing)}")

    name = data["name"].strip()
    email = data["email"].strip()
    phone = (data.get("phone_number") or "").strip() or None
    discord_user = (data.get("discord_user") or "").strip() or None
    problem_text = (data.get("problem_text") or "").strip()

    contact_id = search_or_create_contact(name, email, phone)
    conversation_id = create_conversation(contact_id, discord_user or email)
    session_id = get_or_create_session(discord_user, contact_id, conversation_id)

    # Avatar sync (best-effort)
    try:
        if discord_user:
            user_json = _discord_get_user(discord_user)
            if user_json:
                avatar_url, avatar_hash = _discord_avatar_url_and_hash(user_json, discord_user)
                if avatar_url and avatar_hash:
                    _chatwoot_update_contact_avatar(contact_id, avatar_url, avatar_hash)
    except Exception as e:
        logger.warning("[avatar] sync skipped: %s", e)

    welcome_msg = f"Диалог создан для {name} ({email}).\nПроблема: {problem_text or 'Не указана'}"
    try:
        send_chatwoot_message_sync(conversation_id, welcome_msg)
    except Exception as e:
        logger.warning("[create_contact] welcome message failed: %s", e)

    return {"success": True, "conversation_id": conversation_id, "contact_id": contact_id, "session_id": session_id}


@app.post("/create_contact")
def create_contact():
    try:
        data = request.get_json(silent=True) or {}
        result = _create_contact_impl(data)
        return jsonify(result), 200
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        logger.error("[create_contact] error: %s", e, exc_info=True)
        return jsonify({"success": False, "error": "Internal server error"}), 500


@app.post("/api/create_contact")
def create_contact_api():
    return create_contact()


# ---------------------------------------------------------------------------- #
# Periodic cleanup
# ---------------------------------------------------------------------------- #
def _periodic_cleanup():
    import time as _time
    while True:
        _time.sleep(3600)  # every hour
        try:
            cleanup_old_messages(days=7)
        except Exception:
            pass


# ---------------------------------------------------------------------------- #
# Run
# ---------------------------------------------------------------------------- #
def run_flask():
    app.run(host="0.0.0.0", port=5500)


if __name__ == "__main__":
    init_db()
    cleanup_thread = threading.Thread(target=_periodic_cleanup, daemon=True)
    cleanup_thread.start()
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    bot.run(os.getenv("DISCORD_BOT_TOKEN"))
