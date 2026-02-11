import sqlite3
from contextlib import contextmanager
import logging

DATABASE_NAME = 'chatwoot_discord.db'

def init_db():
    with db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                discord_user_id TEXT NOT NULL,
                chatwoot_contact_id INTEGER NOT NULL,
                chatwoot_conversation_id INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(discord_user_id, chatwoot_conversation_id)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_messages (
                message_id TEXT PRIMARY KEY,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ticket_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                discord_user_id TEXT NOT NULL,
                chatwoot_conversation_id INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                closed_at TIMESTAMP,
                status TEXT DEFAULT 'open'
            )
        ''')
        conn.commit()


# ----------------- Database Functions ------------------

@contextmanager
def get_db_connection():
    """Контекстный менеджер для подключения к SQLite"""
    conn = sqlite3.connect('webhook_sessions.db')
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

def init_database():
    """Инициализация базы данных при запуске"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Таблица сессий
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                discord_user_id TEXT NOT NULL,
                contact_id INTEGER NOT NULL,
                conversation_id INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(discord_user_id, contact_id, conversation_id)
            )
        ''')
        
        # Таблица обработанных сообщений (для избежания дубликатов)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id INTEGER NOT NULL UNIQUE,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
@contextmanager
def db_connection():
    conn = sqlite3.connect(DATABASE_NAME)
    try:
        yield conn
    finally:
        conn.close()

def get_or_create_session(discord_user_id, chatwoot_contact_id, chatwoot_conversation_id):
    with db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR IGNORE INTO sessions 
            (discord_user_id, chatwoot_contact_id, chatwoot_conversation_id)
            VALUES (?, ?, ?)
        ''', (discord_user_id, chatwoot_contact_id, chatwoot_conversation_id))
        conn.commit()

        cursor.execute('''
            SELECT id FROM sessions WHERE discord_user_id = ? AND chatwoot_conversation_id = ?
        ''', (discord_user_id, chatwoot_conversation_id))
        row = cursor.fetchone()
        return row[0] if row else None

def get_session_by_conversation_id(conversation_id):
    with db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM sessions 
            WHERE chatwoot_conversation_id = ?
        ''', (conversation_id,))
        return cursor.fetchone()

def get_session_by_discord_id(discord_user_id):
    with db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM sessions 
            WHERE discord_user_id = ?
            ORDER BY created_at DESC
            LIMIT 1
        ''', (discord_user_id,))
        return cursor.fetchone()

def get_session_by_contact_id(contact_id):
    try:
        with db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM sessions 
                WHERE chatwoot_contact_id = ?
                ORDER BY created_at DESC
                LIMIT 1
            ''', (contact_id,))
            return cursor.fetchone()
    except Exception as e:
        logging.error(f"DB error in get_session_by_contact_id: {e}")
        return None

def close_session(conversation_id):
    """Закрывает сессию по ID разговора"""
    with db_connection() as conn:
        cursor = conn.cursor()

        # Переносим в историю
        cursor.execute('''
            INSERT INTO ticket_history 
            (discord_user_id, chatwoot_conversation_id, closed_at, status)
            SELECT discord_user_id, chatwoot_conversation_id, CURRENT_TIMESTAMP, 'closed'
            FROM sessions WHERE chatwoot_conversation_id = ?
        ''', (conversation_id,))

        # Удаляем из активных
        cursor.execute('''
            DELETE FROM sessions 
            WHERE chatwoot_conversation_id = ?
        ''', (conversation_id,))

        conn.commit()
        return cursor.rowcount > 0

def get_conversation_status(conversation_id):
    """Проверяет наличие активной сессии для разговора"""
    with db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT 1 FROM sessions 
            WHERE chatwoot_conversation_id = ?
        ''', (conversation_id,))
        return cursor.fetchone() is not None

def mark_message_processed(message_id):
    try:
        with db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR IGNORE INTO processed_messages (message_id)
                VALUES (?)
            ''', (str(message_id),))
            conn.commit()
    except (ValueError, sqlite3.Error) as e:
        print(f"Database error in mark_message_processed: {e}")

def is_message_processed(message_id):
    try:
        with db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT 1 FROM processed_messages 
                WHERE message_id = ?
            ''', (str(message_id),))
            return cursor.fetchone() is not None
    except (ValueError, sqlite3.Error) as e:
        print(f"Database error in is_message_processed: {e}")
        return False