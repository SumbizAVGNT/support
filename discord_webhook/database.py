import sqlite3
import logging
from contextlib import contextmanager

logger = logging.getLogger("discord_webhook")

DATABASE_NAME = "chatwoot_discord.db"


@contextmanager
def db_connection():
    conn = sqlite3.connect(DATABASE_NAME, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL;")
    try:
        yield conn
    finally:
        conn.close()


def init_db():
    with db_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                discord_user_id TEXT NOT NULL,
                chatwoot_contact_id INTEGER NOT NULL,
                chatwoot_conversation_id INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(discord_user_id, chatwoot_conversation_id)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS processed_messages (
                message_id TEXT PRIMARY KEY,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ticket_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                discord_user_id TEXT NOT NULL,
                chatwoot_conversation_id INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                closed_at TIMESTAMP,
                status TEXT DEFAULT 'open'
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sessions_discord_id ON sessions(discord_user_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sessions_contact_id ON sessions(chatwoot_contact_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sessions_conv_id ON sessions(chatwoot_conversation_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ticket_history_discord ON ticket_history(discord_user_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_processed_messages_ts ON processed_messages(processed_at);")
        conn.commit()


def get_or_create_session(discord_user_id, chatwoot_contact_id, chatwoot_conversation_id):
    with db_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO sessions (discord_user_id, chatwoot_contact_id, chatwoot_conversation_id) VALUES (?, ?, ?)",
            (discord_user_id, chatwoot_contact_id, chatwoot_conversation_id),
        )
        conn.commit()
        cur.execute(
            "SELECT id FROM sessions WHERE discord_user_id = ? AND chatwoot_conversation_id = ?",
            (discord_user_id, chatwoot_conversation_id),
        )
        row = cur.fetchone()
        return row[0] if row else None


def get_session_by_conversation_id(conversation_id):
    with db_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM sessions WHERE chatwoot_conversation_id = ?", (conversation_id,))
        return cur.fetchone()


def get_session_by_discord_id(discord_user_id):
    with db_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM sessions WHERE discord_user_id = ? ORDER BY created_at DESC LIMIT 1",
            (discord_user_id,),
        )
        return cur.fetchone()


def get_session_by_contact_id(contact_id):
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT * FROM sessions WHERE chatwoot_contact_id = ? ORDER BY created_at DESC LIMIT 1",
                (contact_id,),
            )
            return cur.fetchone()
    except Exception as e:
        logger.error("DB error in get_session_by_contact_id: %s", e)
        return None


def close_session(conversation_id):
    with db_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO ticket_history (discord_user_id, chatwoot_conversation_id, closed_at, status)
               SELECT discord_user_id, chatwoot_conversation_id, CURRENT_TIMESTAMP, 'closed'
               FROM sessions WHERE chatwoot_conversation_id = ?""",
            (conversation_id,),
        )
        cur.execute("DELETE FROM sessions WHERE chatwoot_conversation_id = ?", (conversation_id,))
        conn.commit()
        return cur.rowcount > 0


def get_conversation_status(conversation_id):
    with db_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM sessions WHERE chatwoot_conversation_id = ?", (conversation_id,))
        return cur.fetchone() is not None


def mark_message_processed(message_id):
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("INSERT OR IGNORE INTO processed_messages (message_id) VALUES (?)", (str(message_id),))
            conn.commit()
    except (ValueError, sqlite3.Error) as e:
        logger.error("DB error in mark_message_processed: %s", e)


def is_message_processed(message_id):
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM processed_messages WHERE message_id = ?", (str(message_id),))
            return cur.fetchone() is not None
    except (ValueError, sqlite3.Error) as e:
        logger.error("DB error in is_message_processed: %s", e)
        return False


def cleanup_old_messages(days=7):
    """Remove processed messages older than N days to keep DB lean."""
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "DELETE FROM processed_messages WHERE processed_at < datetime('now', ?)",
                (f"-{days} days",),
            )
            deleted = cur.rowcount
            conn.commit()
            if deleted:
                logger.info("Cleaned up %d old processed messages", deleted)
    except Exception as e:
        logger.error("Cleanup error: %s", e)
