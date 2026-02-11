# webhook_server.py
import os
import io
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
import aiohttp
import threading
from flask import Flask, request, jsonify, Response, g
from flask_cors import CORS

from main import bot
from database import (
    db_connection, init_db, get_or_create_session,
    get_session_by_conversation_id, get_session_by_contact_id,
    close_session, mark_message_processed, is_message_processed,
    DATABASE_NAME
)
from utils import send_discord_message, get_chatwoot_headers

# ------------------------------------------------------------------------------ #
# Логи
# ------------------------------------------------------------------------------ #
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_JSON = os.getenv("LOG_JSON", "false").lower() == "true"
LOG_BODY_MAX = int(os.getenv("LOG_BODY_MAX", "2000"))
REDACT_KEYS = {k.strip().lower() for k in os.getenv(
    "REDACT_KEYS",
    "authorization,api_access_token,access-token,client,uid,token,secret,password"
).split(",")}

logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
logger = logging.getLogger("discord_webhook")


def _redact_headers(h: dict) -> dict:
    if not h:
        return {}
    safe = {}
    for k, v in h.items():
        lk = k.lower()
        safe[k] = "***redacted***" if lk in REDACT_KEYS else v
    return safe


def _cut_body(b: bytes | str | None) -> str:
    if b is None:
        return ""
    if isinstance(b, bytes):
        b = b.decode(errors="replace")
    return b[:LOG_BODY_MAX] + ("…(truncated)" if len(b) > LOG_BODY_MAX else "")


def _json_log(ev: str, **fields):
    if LOG_JSON:
        print(json.dumps({"event": ev, **fields}, ensure_ascii=False))
    else:
        parts = [f"[{ev}]"] + [f"{k}={v}" for k, v in fields.items()]
        logger.info(" ".join(parts))


# ------------------------------------------------------------------------------ #
# Flask
# ------------------------------------------------------------------------------ #
app = Flask(__name__)
CORS(app)

# ------------------------------------------------------------------------------ #
# ENV / CONST
# ------------------------------------------------------------------------------ #
CHATWOOT_HMAC_TOKEN = (os.getenv("CHATWOOT_HMAC_TOKEN") or "").encode()

def get_chatwoot_base_url():
    return os.getenv('CHATWOOT_BASE_URL')

def get_chatwoot_account_id():
    return os.getenv('CHATWOOT_ACCOUNT_ID')

def get_chatwoot_inbox_id():
    return int(os.getenv("CHATWOOT_INBOX_ID", 0))

PUBLIC_HOST = (os.getenv("PUBLIC_HOST") or "https://chatwoot.teighto.net").rstrip("/")
HOST_REWRITE_MAP = os.getenv("HOST_REWRITE_MAP", f"https://172.18.0.1->{PUBLIC_HOST}")
_REWRITE_PAIRS: list[tuple[str, str]] = []
for pair in (HOST_REWRITE_MAP or "").split(","):
    pair = pair.strip()
    if "->" in pair:
        src, dst = pair.split("->", 1)
        _REWRITE_PAIRS.append((src.rstrip("/"), dst.rstrip("/")))

# База прокси для БОТА (скачивание, без CF)
FILE_PROXY_FETCH_BASE = (os.getenv("FILE_PROXY_FETCH_BASE") or "http://127.0.0.1:5500").rstrip("/")
# Публичная база прокси (HTTPS) — как фоллбэк для embed URL
FILE_PROXY_PUBLIC_BASE = (os.getenv("FILE_PROXY_PUBLIC_BASE") or "https://webhook.teighto.net").rstrip("/")

CF_ACCESS_CLIENT_ID = os.getenv("CF_ACCESS_CLIENT_ID", "")
CF_ACCESS_CLIENT_SECRET = os.getenv("CF_ACCESS_CLIENT_SECRET", "")
INTERNAL_FETCH_BASE = (os.getenv("INTERNAL_FETCH_BASE") or "http://127.0.0.1:5001").rstrip("/")


# ------------------------------------------------------------------------------ #
# Middleware
# ------------------------------------------------------------------------------ #
@app.before_request
def _before_request():
    g._ts = time.perf_counter()
    g._rid = request.headers.get("X-Request-Id") or uuid.uuid4().hex[:12]
    try:
        _json_log(
            "http_in",
            rid=g._rid,
            method=request.method,
            path=request.path,
            query=request.query_string.decode(),
            headers=_redact_headers(dict(request.headers)),
            body=_cut_body(request.get_data(cache=True))
        )
    except Exception:
        logger.exception("failed to log http_in")


@app.after_request
def _after_request(resp: Response):
    try:
        dur_ms = int((time.perf_counter() - getattr(g, "_ts", time.perf_counter())) * 1000)
        _json_log(
            "http_out",
            rid=getattr(g, "_rid", "-"),
            status=resp.status_code,
            duration_ms=dur_ms,
            headers=_redact_headers(dict(resp.headers)),
            body=_cut_body(resp.get_data())
        )
        resp.headers["X-Request-Id"] = getattr(g, "_rid", "-")
    except Exception:
        logger.exception("failed to log http_out")
    return resp


# ------------------------------------------------------------------------------ #
# Безопасность
# ------------------------------------------------------------------------------ #
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


# ------------------------------------------------------------------------------ #
# URL utils
# ------------------------------------------------------------------------------ #
def _public_netloc() -> str:
    parsed = urllib.parse.urlparse(PUBLIC_HOST)
    return parsed.netloc

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

def get_extension(file_type: str, filename: str) -> str:
    if '.' in filename:
        return filename[filename.rfind('.'):]
    ext = mimetypes.guess_extension(file_type)
    if ext:
        return ext
    if 'png' in file_type: return '.png'
    if 'jpeg' in file_type or 'jpg' in file_type: return '.jpg'
    if 'gif' in file_type: return '.gif'
    if 'pdf' in file_type: return '.pdf'
    return '.bin'

def _public_proxy_url(raw_url: str) -> str:
    if not raw_url:
        return ""
    normalized = _normalize_to_public(raw_url)
    return f"{FILE_PROXY_PUBLIC_BASE}/proxy/file?url={urllib.parse.quote(normalized, safe='')}&v={int(time.time())}"

def _internal_proxy_url(raw_url: str) -> str:
    if not raw_url:
        return ""
    normalized = _normalize_to_public(raw_url)
    return f"{FILE_PROXY_FETCH_BASE}/proxy/file?url={urllib.parse.quote(normalized, safe='')}&v={int(time.time())}"


def make_chatwoot_request(method, url, json_data=None):
    headers = get_chatwoot_headers()
    ts = time.perf_counter()
    rid = getattr(g, "_rid", uuid.uuid4().hex[:12])
    try:
        _json_log("cw_out", rid=rid, method=method, url=url, headers=_redact_headers(headers),
                  body=_cut_body(json.dumps(json_data, ensure_ascii=False)) if json_data else "")
        resp = requests.request(method, url, json=json_data, headers=headers, timeout=30)
        dur_ms = int((time.perf_counter() - ts) * 1000)
        text = resp.text or ""
        _json_log("cw_in", rid=rid, status=resp.status_code, duration_ms=dur_ms,
                  headers=_redact_headers(dict(resp.headers)), body=_cut_body(text))
        resp.raise_for_status()
        try:
            return resp.json()
        except ValueError:
            return {"raw": text}
    except requests.exceptions.RequestException as e:
        logger.exception(f"Chatwoot API request failed: {e}")
        raise


# ---------------------- Кэш агентов и функции получения аватара ----------------
_AGENTS_CACHE: dict[int, dict] = {}
_AGENTS_CACHE_TS: float = 0.0
_AGENTS_TTL: float = 60.0  # 1 мин. кэш

def _refresh_agents_cache():
    global _AGENTS_CACHE, _AGENTS_CACHE_TS
    try:
        url = f"{get_chatwoot_base_url()}/api/v1/accounts/{get_chatwoot_account_id()}/agents"
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
        print(f"[agents_cache] loaded {len(cache)} agents")
    except Exception as e:
        print(f"[agents_cache] load failed: {e}")

def _get_agent_info(agent_id: int) -> dict | None:
    now = time.time()
    if not _AGENTS_CACHE or now - _AGENTS_CACHE_TS > _AGENTS_TTL:
        _refresh_agents_cache()
    return _AGENTS_CACHE.get(int(agent_id))


# ------------------------------------------------------------------------------ #
# Attachments mapping
# ------------------------------------------------------------------------------ #
def extract_attachments(raw_attachments):
    attachments = []
    for att in (raw_attachments or []):
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
                    "original_url": data_url
                })
                logger.info(f"Processed attachment: name={filename} type={file_type} -> {proxy_url}")
        except Exception as e:
            logger.error(f"Error processing attachment: {e}")
    return attachments


# ------------------------------------------------------------------------------ #
# File proxy
# ------------------------------------------------------------------------------ #
def _upstream_headers() -> dict:
    h = {
        "User-Agent": "Mozilla/5.0 (compatible; FileProxy/1.0)",
        "Accept": "*/*",
        "Host": _public_netloc(),
    }
    if CF_ACCESS_CLIENT_ID and CF_ACCESS_CLIENT_SECRET:
        h["CF-Access-Client-Id"] = CF_ACCESS_CLIENT_ID
        h["CF-Access-Client-Secret"] = CF_ACCESS_CLIENT_SECRET
    return h


@app.route('/proxy/file', methods=['GET'])
def proxy_file():
    try:
        raw_url = request.args.get('url', '').strip()
        if not raw_url:
            return jsonify({"error": "No URL provided"}), 400

        public_url = _normalize_to_public(raw_url)
        upstream_url = _rewrite_public_to_internal(public_url)

        logger.info(f"Proxying file: raw={raw_url} -> public={public_url} -> upstream={upstream_url}")

        t0 = time.perf_counter()
        response = requests.get(upstream_url, stream=True, timeout=30, headers=_upstream_headers())
        body = response.content
        dur_ms = int((time.perf_counter() - t0) * 1000)
        _json_log("file_proxy_upstream",
                  url=upstream_url, status=response.status_code,
                  bytes=len(body or b""), duration_ms=dur_ms,
                  ct=response.headers.get('Content-Type', '-'))

        if response.status_code >= 400:
            text = body.decode(errors="replace") if body else ""
            return Response(
                json.dumps({"error": f"upstream {response.status_code}", "detail": text[:500]}, ensure_ascii=False),
                status=502, mimetype="application/json"
            )

        if len(body) == 0:
            logger.error("Downloaded file is empty")
            return jsonify({"error": "Empty file"}), 502

        content_type = response.headers.get('Content-Type', 'application/octet-stream')
        cd = response.headers.get('Content-Disposition', '')
        if 'filename=' in (cd or ''):
            filename = cd.split('filename=')[1].strip().strip('"')
        else:
            filename = get_filename_from_url(public_url)

        return Response(
            body, content_type=content_type,
            headers={'Content-Disposition': f'attachment; filename="{filename}"',
                     'Access-Control-Allow-Origin': '*'}
        )

    except requests.exceptions.Timeout:
        logger.error("Timeout while downloading file")
        return jsonify({"error": "Download timeout"}), 504
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error: {e}")
        return jsonify({"error": str(e)}), 502
    except Exception as e:
        logger.error(f"Proxy error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


# ------------------------------------------------------------------------------ #
# Chatwoot helpers
# ------------------------------------------------------------------------------ #
def search_or_create_contact(name, email, phone=None):
    search_url = f"{get_chatwoot_base_url()}/api/v1/accounts/{get_chatwoot_account_id()}/contacts/search?q={urllib.parse.quote(email)}"
    result = make_chatwoot_request('GET', search_url)
    contacts = result.get('payload', [])
    if contacts:
        cid = contacts[0].get('id') or contacts[0].get('contact', {}).get('id')
        if cid:
            return cid

    payload = {"name": name, "email": email, "phone_number": phone if phone and phone.startswith('+') else None}
    contact_url = f"{get_chatwoot_base_url()}/api/v1/accounts/{get_chatwoot_account_id()}/contacts"
    result = make_chatwoot_request('POST', contact_url, payload)
    contact_id = result.get("payload", {}).get("contact", {}).get("id") or result.get("id")
    if not contact_id:
        raise ValueError("Failed to create contact in Chatwoot")
    return contact_id


def create_conversation(contact_id, source_id):
    payload = {"source_id": source_id, "inbox_id": get_chatwoot_inbox_id(), "contact_id": contact_id}
    url = f"{get_chatwoot_base_url()}/api/v1/accounts/{get_chatwoot_account_id()}/conversations"
    result = make_chatwoot_request('POST', url, payload)
    conv_id = result.get("id") or result.get("conversation", {}).get("id")
    if not conv_id:
        raise ValueError(f"Failed to create conversation: {result}")
    return conv_id


def send_chatwoot_message(conversation_id, content):
    url = f"{get_chatwoot_base_url()}/api/v1/accounts/{get_chatwoot_account_id()}/conversations/{conversation_id}/messages"
    payload = {"content": content, "message_type": 0, "private": False}
    make_chatwoot_request('POST', url, payload)


# ------------------------------------------------------------------------------ #
# Healthcheck
# ------------------------------------------------------------------------------ #
@app.route('/healthz', methods=['GET'])
def healthz():
    return jsonify({
        "ok": True,
        "public_host": PUBLIC_HOST,
        "file_proxy_fetch_base": FILE_PROXY_FETCH_BASE,
        "file_proxy_public_base": FILE_PROXY_PUBLIC_BASE,
        "internal_fetch_base": INTERNAL_FETCH_BASE
    })


# ------------------------------------------------------------------------------ #
# Async helpers (Discord)
# ------------------------------------------------------------------------------ #
async def notify_user_about_closed_ticket(user_id: int, ticket_id: int):
    try:
        user = await bot.fetch_user(user_id)
        await send_discord_message(
            user=user,
            content=f"🔒 Ваш тикет #{ticket_id} был закрыт. Если у вас есть новые вопросы, создайте новый запрос через /support"
        )
    except Exception as e:
        logger.error(f"Ошибка при отправке уведомления о закрытии тикета: {e}")


async def fetch_and_send(
    discord_user_id: int,
    content: str | None,
    attachments: list | None,
    agent_name: str | None,
    agent_avatar_download_url: str | None,
    agent_avatar_external_url: str | None
):
    try:
        user = await bot.fetch_user(discord_user_id)
        await send_discord_message(
            user=user,
            content=content or "",
            agent_name=(agent_name or "").strip() or None,
            agent_avatar_download_url=(agent_avatar_download_url or "").strip() or None,
            agent_avatar_external_url=(agent_avatar_external_url or "").strip() or None,
            attachments=attachments or []
        )
    except Exception as e:
        logger.error(f"Error in fetch_and_send: {e}")


# ------------------------------------------------------------------------------ #
# Webhook
# ------------------------------------------------------------------------------ #
@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        if not verify_chatwoot_signature(request):
            logger.warning("Invalid Chatwoot webhook signature")
            return jsonify({"status": "unauthorized"}), 401

        data = request.get_json(silent=True) or {}
        rid = getattr(g, "_rid", "-")
        _json_log("webhook_event", rid=rid, event=data.get('event'),
                  message_type=data.get('message_type'), message_id=data.get('id'),
                  attachments_count=len(data.get('attachments') or []))

        event = data.get('event')
        if event not in ['message_created', 'message_updated', 'conversation_updated']:
            logger.info(f"Ignoring event type: {event}")
            return jsonify({"status": "ignored event type"}), 200

        if event in ['message_created', 'message_updated']:
            sender = (data.get('sender') or {})
            message_type = data.get('message_type')
            message_id = data.get('id')

            # Только исходящие сообщения агента
            if message_type != 'outgoing':
                logger.info("Ignoring non-outgoing message")
                return jsonify({"status": "not outgoing"}), 200

            if is_message_processed(message_id):
                logger.info(f"Duplicate message {message_id} ignored")
                return jsonify({"status": "message already processed"}), 200
            mark_message_processed(message_id)

            content = data.get('content') or ""
            conversation = (data.get('conversation') or {})
            contact_id = (conversation.get('contact_inbox') or {}).get('contact_id')

            # Определяем агента (предпочтительно по assignee_id)
            assignee_id = (conversation or {}).get('assignee_id') or (conversation.get('meta') or {}).get('assignee_id')
            agent_id = assignee_id or (data.get('user') or {}).get('id') or sender.get('id')

            agent_name = None
            raw_avatar = None

            if agent_id:
                info = _get_agent_info(int(agent_id))
                if info:
                    agent_name = (info.get("name") or "").strip() or None
                    raw_avatar = info.get("avatar_url") or ""
                    print(f"[webhook] agent resolved by id={agent_id}: name={agent_name!r} avatar={raw_avatar!r}")
                else:
                    print(f"[webhook] agent not found in cache: id={agent_id}")

            # Fallback по payload
            if not agent_name:
                agent_name = (sender.get('name') or (data.get('user') or {}).get('name') or "").strip() or "Support"
            if not raw_avatar:
                raw_avatar = (sender.get('avatar_url') or sender.get('avatar') or (data.get('user') or {}).get('avatar_url') or "")

            # Готовим ссылки для аватара
            agent_avatar_download_url = _internal_proxy_url(raw_avatar) if raw_avatar else None
            agent_avatar_external_url = _public_proxy_url(raw_avatar) if raw_avatar else None

            if not contact_id:
                logger.warning("No contact_id in message event")
                return jsonify({"status": "error - no contact_id"}), 400

            session = get_session_by_contact_id(contact_id)
            if not session:
                logger.info("No active discord session for contact")
                return jsonify({"status": "no discord session"}), 200

            discord_user_id = session[1]
            attachments = extract_attachments(data.get('attachments'))

            _json_log("dispatch_to_discord", rid=rid, discord_user_id=discord_user_id,
                      content_len=len(content), attachments=len(attachments))

            asyncio.run_coroutine_threadsafe(
                fetch_and_send(
                    int(discord_user_id),
                    content,
                    attachments,
                    agent_name,
                    agent_avatar_download_url,
                    agent_avatar_external_url
                ),
                bot.loop
            )
            logger.info(f"Forwarded message {message_id} to Discord user {discord_user_id}")
            return jsonify({"status": "message forwarded"}), 200

        elif event == 'conversation_updated':
            changed_attrs = data.get('changed_attributes') or []
            conversation_id = data.get('id')
            contact_id = (data.get('contact_inbox') or {}).get('contact_id')
            if not contact_id:
                return jsonify({"status": "error - no contact_id"}), 400

            # Закрытие диалога
            if any((attr.get('status') or {}).get('current_value') == 'resolved' for attr in changed_attrs):
                session = get_session_by_conversation_id(conversation_id)
                if session:
                    discord_user_id = session[1]
                    if close_session(conversation_id):
                        asyncio.run_coroutine_threadsafe(
                            notify_user_about_closed_ticket(int(discord_user_id), conversation_id),
                            bot.loop
                        )
                        return jsonify({"status": "session closed"}), 200
                return jsonify({"status": "no active session found"}), 200

        return jsonify({"status": "ignored event"}), 200

    except Exception as e:
        logger.error(f"Webhook processing error: {e}", exc_info=True)
        return jsonify({"status": "error", "error": str(e)}), 500


# ------------------------------------------------------------------------------ #
# Admin
# ------------------------------------------------------------------------------ #
@app.route('/admin/clear_db', methods=['POST'])
def clear_database():
    try:
        auth = request.headers.get('Authorization')
        if auth != 'Bearer cleanup_temp_token':
            return jsonify({"error": "Unauthorized"}), 401

        with db_connection() as conn:
            cursor = conn.cursor()
            tables = ['processed_messages', 'sessions', 'ticket_history']
            for table in tables:
                cursor.execute(f"DELETE FROM {table}")
                logger.warning(f"Cleared table: {table} - {cursor.rowcount} rows deleted")
            conn.commit()

        logger.warning("DATABASE CLEARED - All data has been removed")
        return jsonify({"status": "success", "message": "Database cleared successfully", "tables_cleared": tables})

    except Exception as e:
        logger.error(f"Error clearing database: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/admin/db_status', methods=['GET'])
def db_status():
    try:
        auth = request.headers.get('Authorization')
        if auth != 'Bearer cleanup_temp_token':
            return jsonify({"error": "Unauthorized"}), 401

        with db_connection() as conn:
            cursor = conn.cursor()
            tables = ['sessions', 'processed_messages', 'ticket_history']
            status = {}
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                status[table] = cursor.fetchone()[0]

        return jsonify({"status": "success", "database": DATABASE_NAME, "table_counts": status})

    except Exception as e:
        logger.error(f"Error getting DB status: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/admin/reset_sessions', methods=['POST'])
def reset_sessions():
    try:
        auth = request.headers.get('Authorization')
        if auth != 'Bearer cleanup_temp_token':
            return jsonify({"error": "Unauthorized"}), 401

        with db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM sessions")
            cursor.execute("DELETE FROM processed_messages")
            conn.commit()

        logger.warning("SESSIONS CLEARED - All sessions removed")
        return jsonify({"status": "success", "message": "Sessions cleared"})

    except Exception as e:
        logger.error(f"Error clearing sessions: {e}")
        return jsonify({"error": str(e)}), 500




# ------------------------------------------------------------------------------ 
# Эндпойнты для создания контакта/диалога (исправление 404 и синхронизация аватара Discord)
# ------------------------------------------------------------------------------ 

def _discord_get_user(discord_user_id: str) -> dict | None:
    """Получить JSON пользователя Discord через REST API (без nextcord), чтобы взять avatar hash."""
    token = os.getenv("DISCORD_BOT_TOKEN", "").strip()
    if not token or not discord_user_id:
        return None
    url = f"https://discord.com/api/v10/users/{discord_user_id}"
    try:
        r = requests.get(url, headers={"Authorization": f"Bot {token}"}, timeout=15)
        if r.status_code == 200:
            return r.json()
        logger.warning(f"[avatar] discord api {url} -> {r.status_code} {r.text[:200]}")
    except Exception as e:
        logger.warning(f"[avatar] discord api error: {e}")
    return None


def _discord_avatar_url_and_hash(user_json: dict, discord_user_id: str) -> tuple[str | None, str | None]:
    """Построить CDN-URL аватара и вернуть (url, hash)."""
    if not user_json:
        return None, None
    avatar = user_json.get("avatar")
    if avatar:
        # animated?
        ext = "gif" if str(avatar).startswith("a_") else "png"
        return f"https://cdn.discordapp.com/avatars/{discord_user_id}/{avatar}.{ext}?size=256", str(avatar)
    # дефолтный аватар — безопасный запасной вариант
    # сначала пробуем discriminator, затем fallback по сдвигу id
    disc = user_json.get("discriminator", "0")
    try:
        idx = int(disc) % 5
    except Exception:
        try:
            idx = (int(discord_user_id) >> 22) % 6
        except Exception:
            idx = 0
    return f"https://cdn.discordapp.com/embed/avatars/{idx}.png", f"default_{idx}"


def _chatwoot_get_contact(contact_id: int) -> dict | None:
    """Получить карточку контакта из Chatwoot (для сравнения хеша)."""
    try:
        url = f"{get_chatwoot_base_url()}/api/v1/accounts/{get_chatwoot_account_id()}/contacts/{contact_id}"
        data = make_chatwoot_request("GET", url)
        # ответ может быть сразу контактом или в payload.contact
        if isinstance(data, dict):
            if "payload" in data:
                return data["payload"].get("contact") or data["payload"]
            return data
    except Exception as e:
        logger.warning(f"[avatar] get contact failed: {e}")
    return None


def _chatwoot_update_contact_avatar(contact_id: int, avatar_url: str, avatar_hash: str) -> bool:
    """Обновить аватар и сохранить hash в custom_attributes.discord_avatar_hash."""
    try:
        url = f"{get_chatwoot_base_url()}/api/v1/accounts/{get_chatwoot_account_id()}/contacts/{contact_id}"
        payload = {
            "avatar_url": avatar_url,
            "custom_attributes": {
                "discord_avatar_hash": avatar_hash
            }
        }
        _ = make_chatwoot_request("PUT", url, payload)
        logger.info(f"[avatar] contact {contact_id} avatar updated")
        return True
    except Exception as e:
        logger.warning(f"[avatar] update contact failed: {e}")
        return False


def _create_contact_impl(data: dict):
    """
    Базовая реализация: создать/найти контакт, создать беседу, записать сессию.
    Плюс: подтянуть аватар Discord и синхронизировать его в Chatwoot.
    """

    # Локальная валидация (без внешних helper'ов)
    def _require_fields(payload: dict, fields: list[str]):
        missing = [f for f in fields if not (str(payload.get(f) or "").strip())]
        if missing:
            raise ValueError(f"Missing required fields: {', '.join(missing)}")

    _require_fields(data, ['name', 'email'])

    name = data['name'].strip()
    email = data['email'].strip()
    phone = (data.get('phone_number') or "").strip() or None
    discord_user = (data.get('discord_user') or "").strip() or None
    problem_text = (data.get('problem_text') or '').strip()

    # Создаём/находим контакт и разговор
    contact_id = search_or_create_contact(name, email, phone)
    conversation_id = create_conversation(contact_id, discord_user or email)
    session_id = get_or_create_session(discord_user, contact_id, conversation_id)
    logger.info(f"[create_contact] session_id={session_id} contact_id={contact_id} conversation_id={conversation_id}")

    # --- Синхронизация аватара из Discord (best-effort) ---
    try:
        if discord_user:
            user_json = _discord_get_user(discord_user)
            avatar_url, avatar_hash = _discord_avatar_url_and_hash(user_json, discord_user) if user_json else (None, None)
            if avatar_url and avatar_hash:
                contact = _chatwoot_get_contact(contact_id) or {}
                current_hash = ((contact.get("custom_attributes") or {}).get("discord_avatar_hash")) or ""
                if current_hash != avatar_hash:
                    _chatwoot_update_contact_avatar(contact_id, avatar_url, avatar_hash)
                else:
                    logger.info(f"[avatar] contact {contact_id} avatar up-to-date ({avatar_hash})")
    except Exception as e:
        logger.warning(f"[avatar] sync skipped due error: {e}")

    # Приветственное сообщение в чат
    welcome_msg = f"Диалог создан для {name} ({email}).\nПроблема: {problem_text or 'Не указана'}"
    try:
        send_chatwoot_message(conversation_id, welcome_msg)
    except Exception as e:
        logger.warning(f"[create_contact] failed to post welcome message: {e}")

    return {
        "success": True,
        "conversation_id": conversation_id,
        "contact_id": contact_id,
        "session_id": session_id
    }


@app.post('/create_contact')
def create_contact():
    try:
        data = request.get_json(silent=True) or {}
        logger.info(f"Received data for create_contact")
        result = _create_contact_impl(data)
        # Возвращаем 200 OK (не 201), чтобы клиент не считал это ошибкой
        return jsonify(result), 200
    except ValueError as e:
        logger.warning(f"[create_contact] Validation error: {e}")
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        logger.error(f"[create_contact] Unexpected error: {e}", exc_info=True)
        return jsonify({"success": False, "error": "Internal server error"}), 500


# Алиасы
@app.post('/api/create_contact')
def create_contact_api():
    return create_contact()


@app.post('/create-contact')
def create_contact_dash():
    return create_contact()


# ------------------------------------------------------------------------------ #
# Run
# ------------------------------------------------------------------------------ #
def run_flask():
    app.run(host='0.0.0.0', port=5500)


if __name__ == '__main__':
    init_db()
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    bot.run(os.getenv('DISCORD_BOT_TOKEN'))
