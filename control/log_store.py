# log_store.py (MySQL version)
# Ghi log ETL vào MySQL thay vì SQLite

import pymysql
from datetime import datetime
import os
from dotenv import load_dotenv
from pathlib import Path

# Load .env
BASE_DIR = Path(__file__).resolve().parents[1]
env_path = BASE_DIR / ".env"
load_dotenv(env_path)

DB_CONFIG = {
    "host": os.getenv("DB_CONTROL_HOST", "127.0.0.1"),
    "user": os.getenv("DB_CONTROL_USER", "root"),
    "password": os.getenv("DB_CONTROL_PASS", ""),
    "database": os.getenv("DB_CONTROL_NAME", "control"),
    "port": int(os.getenv("DB_CONTROL_PORT", 3306)),
}

def connect():
    return pymysql.connect(
        host=DB_CONFIG["host"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        database=DB_CONFIG["database"],
        port=DB_CONFIG["port"],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True
    )


# ================================
# Hàm ghi log ETL
# ================================

def start_process(process_name: str, message: str = None) -> int:
    """Tạo một log mới với status = 'running'."""
    conn = connect()
    with conn.cursor() as cur:
        sql = """
            INSERT INTO process_logs (process_name, status, message, start_time)
            VALUES (%s, 'running', %s, %s)
        """
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cur.execute(sql, (process_name, message, now))
        cur.execute("SELECT LAST_INSERT_ID() AS id")
        log_id = cur.fetchone()["id"]
    conn.close()
    return log_id


def log_success(log_id: int, message: str = None):
    """Update một log thành công."""
    conn = connect()
    with conn.cursor() as cur:
        sql = """
            UPDATE process_logs
            SET status='success',
                message=%s,
                end_time=%s
            WHERE id=%s
        """
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cur.execute(sql, (message, now, log_id))
    conn.close()


def log_fail(log_id: int, message: str = None):
    """Update một log thất bại."""
    conn = connect()
    with conn.cursor() as cur:
        sql = """
            UPDATE process_logs
            SET status='failed',
                message=%s,
                end_time=%s
            WHERE id=%s
        """
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cur.execute(sql, (message, now, log_id))
    conn.close()


def get_latest_status(process_name: str) -> str:
    """Lấy status mới nhất của một process."""
    conn = connect()
    with conn.cursor() as cur:
        sql = """
            SELECT status
            FROM process_logs
            WHERE process_name = %s
            ORDER BY id DESC
            LIMIT 1
        """
        cur.execute(sql, (process_name,))
        row = cur.fetchone()
    conn.close()

    if not row:
        return None
    return row["status"]
