import sqlite3
import os

DB_NAME = "pipeline.db"

def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    
    # Queue Items Table
    c.execute('''
        CREATE TABLE IF NOT EXISTS queue_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            external_id TEXT UNIQUE,
            item_type TEXT NOT NULL,
            payload TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TEXT NOT NULL
        )
    ''')
    
    # Issues Table
    c.execute('''
        CREATE TABLE IF NOT EXISTS issues (
            id INTEGER PRIMARY KEY,
            number INTEGER,
            title TEXT,
            state TEXT,
            user_login TEXT,
            is_pull_request INTEGER,
            created_at TEXT,
            closed_at TEXT,
            lead_time_hours REAL,
            raw_payload TEXT
        )
    ''')
    
    conn.commit()
    conn.close()
    print(f"Database {DB_NAME} initialized.")

if __name__ == "__main__":
    init_db()
