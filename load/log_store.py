import os
import sqlite3
from datetime import datetime

# Reuse the same DB file used by extract logs so all run history is centralized
DB_DIR = os.path.join(os.path.dirname(__file__), '..', 'logs')
DB_PATH = os.path.join(DB_DIR, 'extract_runs.db')

CREATE_TABLE_SQL = '''
CREATE TABLE IF NOT EXISTS load_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    rows_inserted INTEGER,
    message TEXT,
    csv_path TEXT
);
'''


def _ensure_db():
    os.makedirs(DB_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(CREATE_TABLE_SQL)
        conn.commit()
    finally:
        conn.close()


def insert_load(started_at=None, finished_at=None, status='success', rows_inserted=None, message=None, csv_path=None):
    _ensure_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    if started_at is None:
        started_at = datetime.now().isoformat(sep=' ', timespec='seconds')
    if finished_at is None:
        finished_at = started_at
    cur.execute(
        "INSERT INTO load_runs (started_at, finished_at, status, rows_inserted, message, csv_path) VALUES (?, ?, ?, ?, ?, ?)",
        (started_at, finished_at, status, rows_inserted, message, csv_path)
    )
    conn.commit()
    conn.close()
