# db_connect.py
# FLOW 3.0.x – Kết nối MySQL với logging đầy đủ

import os
import pymysql
from dotenv import load_dotenv
from pathlib import Path
from load.load_logger import logger

# FLOW 3.0.2: load file .env từ thư mục cha
BASE_DIR = Path(__file__).resolve().parents[1]
env_path = BASE_DIR / ".env"
load_dotenv(env_path)

# FLOW 3.0.3: cấu hình DB từ env
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASS", "123456"),
    "database": os.getenv("DB_NAME", "laptop_dw"),
    "port": int(os.getenv("DB_PORT", "3306")),
}

# FLOW 3.0.4: hàm kết nối MySQL có log
def mysql_connect():
    logger.info("[DB] Đang kết nối MySQL...")
    try:
        conn = pymysql.connect(
            host=DB_CONFIG["host"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            database=DB_CONFIG["database"],
            port=DB_CONFIG["port"],
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
            connect_timeout=10
        )
        logger.info("[DB] Kết nối MySQL thành công")
        return conn
    except Exception as e:
        logger.error(f"[DB] Kết nối MySQL thất bại: {e}", exc_info=True)
        raise