import os
import sqlite3
from datetime import datetime

# === Database path ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "control.db")

# === SQL: create process log table ===
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS process_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    process_name TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    message TEXT
);
"""


def _get_conn():
    """Ensure DB exists and return connection."""
    os.makedirs(BASE_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute(CREATE_TABLE_SQL)
    conn.commit()
    return conn



# START PROCESS
def start_process(process_name, message="Starting"):
    """
    Ghi log bắt đầu một process.
    Return: id của log row.
    """
    conn = _get_conn()
    cur = conn.cursor()

    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cur.execute(
        """
        INSERT INTO process_log (process_name, status, started_at, message)
        VALUES (?, 'running', ?, ?)
        """,
        (process_name, started_at, message)
    )
    log_id = cur.lastrowid

    conn.commit()
    conn.close()
    return log_id



#  END PROCESS

def end_process(log_id, status, message=None):
    """
    Update trạng thái khi process kết thúc.
    status = 'success' hoặc 'fail'
    """
    conn = _get_conn()
    cur = conn.cursor()

    ended_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cur.execute(
        """
        UPDATE process_log
        SET status = ?, ended_at = ?, message = ?
        WHERE id = ?
        """,
        (status, ended_at, message, log_id)
    )

    conn.commit()
    conn.close()



#  GET LATEST STATUS
def get_latest_status(process_name):
    """
    Lấy status mới nhất của một process: success/fail/running/None
    """
    conn = _get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT status
        FROM process_log
        WHERE process_name = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (process_name,)
    )
    row = cur.fetchone()
    conn.close()

    return row[0] if row else None



# CHECK IF CURRENT PROCESS CAN RUN

def can_run(process_name, required_previous):
    """
    Kiểm tra process hiện tại có được phép chạy hay không.

    Example:
        can_run("transform", "extract")
        can_run("load", "transform")
    """
    prev_status = get_latest_status(required_previous)

    if prev_status == "success":
        return True
    return False



# HELPERS FOR ETL
def log_success(log_id, message="Completed"):
    end_process(log_id, "success", message)


def log_fail(log_id, message="Failed"):
    end_process(log_id, "fail", message)
