import os
import sqlite3
from datetime import datetime

DB_DIR = os.path.join(os.path.dirname(__file__), '..', 'logs')
DB_PATH = os.path.join(DB_DIR, 'extract_runs.db')

CREATE_TABLE_SQL = '''
CREATE TABLE IF NOT EXISTS extract_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    attempts INTEGER DEFAULT 0,
    message TEXT,
    csv_path TEXT,
    row_count INTEGER
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


def start_run(attempts=0):
    """Insert a running run record and return its id."""
    _ensure_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    started_at = datetime.now().isoformat(sep=' ', timespec='seconds')
    cur.execute("INSERT INTO extract_runs (started_at, status, attempts) VALUES (?, ?, ?)",
                (started_at, 'running', attempts))
    run_id = cur.lastrowid
    conn.commit()
    conn.close()
    return run_id


def update_run(run_id, status, message=None, csv_path=None, row_count=None):
    """Update the run record with finish info."""
    _ensure_db()
    finished_at = datetime.now().isoformat(sep=' ', timespec='seconds')
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "UPDATE extract_runs SET finished_at = ?, status = ?, message = ?, csv_path = ?, row_count = ? WHERE id = ?",
        (finished_at, status, message, csv_path, row_count, run_id)
    )
    conn.commit()
    conn.close()


def insert_final(status, attempts=0, message=None, csv_path=None, row_count=None):
    """Convenience insert if you don't call start_run first."""
    _ensure_db()
    started_at = datetime.now().isoformat(sep=' ', timespec='seconds')
    finished_at = started_at
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO extract_runs (started_at, finished_at, status, attempts, message, csv_path, row_count) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (started_at, finished_at, status, attempts, message, csv_path, row_count)
    )
    conn.commit()
    conn.close()
