"""SQLite database layer for session and closure tracking."""

import sqlite3
import logging
from typing import Any, Dict, Optional, Tuple

from config import DB_PATH

logger = logging.getLogger("tg-cw-bridge")


def _connect():
    conn = sqlite3.connect(DB_PATH, timeout=10, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def init_db():
    conn = _connect()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sessions(
            chat_id INTEGER PRIMARY KEY,
            contact_id INTEGER,
            conversation_id INTEGER,
            nickname TEXT
        );
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS closures(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER,
            conversation_id INTEGER,
            closed_at INTEGER
        );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS closures_chat_idx ON closures(chat_id, closed_at DESC);")
    conn.execute("CREATE INDEX IF NOT EXISTS sessions_conv_idx ON sessions(conversation_id);")
    conn.close()


def get_session(chat_id: int) -> Dict[str, Any]:
    conn = _connect()
    row = conn.execute(
        "SELECT contact_id, conversation_id, nickname FROM sessions WHERE chat_id=?", (chat_id,)
    ).fetchone()
    conn.close()
    if not row:
        return {"contact_id": None, "conversation_id": None, "nickname": None}
    return {"contact_id": row[0], "conversation_id": row[1], "nickname": row[2]}


def upsert_session(chat_id: int, contact_id: Optional[int], conversation_id: Optional[int], nickname: Optional[str]):
    conn = _connect()
    conn.execute(
        """INSERT INTO sessions(chat_id, contact_id, conversation_id, nickname)
           VALUES(?,?,?,?)
           ON CONFLICT(chat_id) DO UPDATE SET
             contact_id=excluded.contact_id,
             conversation_id=excluded.conversation_id,
             nickname=COALESCE(excluded.nickname, sessions.nickname)""",
        (chat_id, contact_id, conversation_id, nickname),
    )
    conn.close()


def set_conversation(chat_id: int, conv_id: Optional[int]):
    s = get_session(chat_id)
    upsert_session(chat_id, s["contact_id"], conv_id, s["nickname"])


def set_nickname(chat_id: int, nickname: Optional[str]):
    s = get_session(chat_id)
    upsert_session(chat_id, s["contact_id"], s["conversation_id"], nickname)


def add_closure(chat_id: int, conversation_id: int, closed_at: int):
    conn = _connect()
    conn.execute(
        "INSERT INTO closures(chat_id, conversation_id, closed_at) VALUES(?,?,?)",
        (chat_id, conversation_id, closed_at),
    )
    conn.close()


def get_last_closure(chat_id: int) -> Optional[Tuple[int, int]]:
    conn = _connect()
    row = conn.execute(
        "SELECT conversation_id, closed_at FROM closures WHERE chat_id=? ORDER BY closed_at DESC LIMIT 1",
        (chat_id,),
    ).fetchone()
    conn.close()
    return (row[0], row[1]) if row else None


def get_chat_by_conversation(conv_id: int) -> Optional[int]:
    conn = _connect()
    row = conn.execute("SELECT chat_id FROM sessions WHERE conversation_id=?", (conv_id,)).fetchone()
    conn.close()
    return int(row[0]) if row and row[0] is not None else None
