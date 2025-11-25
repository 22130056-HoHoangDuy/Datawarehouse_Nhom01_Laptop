# db_connect.py
import os
import pymysql
from dotenv import load_dotenv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
env_path = BASE_DIR / ".env"
load_dotenv(env_path)

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASS", ""),
    "database": os.getenv("DB_NAME", "laptop_dw"),
    "port": int(os.getenv("DB_PORT", "3306")),
}

# Mart database
DB_MART_CONFIG = {
    "host": os.getenv("DB_MART_HOST", "127.0.0.1"),
    "user": os.getenv("DB_MART_USER", "root"),
    "password": os.getenv("DB_MART_PASS", ""),
    "database": os.getenv("DB_MART_NAME", "datamart"),
    "port": int(os.getenv("DB_MART_PORT", "3306")),
}

def mysql_connect():
    return pymysql.connect(
        host=DB_CONFIG["host"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        database=DB_CONFIG["database"],
        port=DB_CONFIG["port"],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
        connect_timeout=5
    )

def mysql_connect_mart():
    """Connect to Mart database"""
    return pymysql.connect(
        host=DB_MART_CONFIG["host"],
        user=DB_MART_CONFIG["user"],
        password=DB_MART_CONFIG["password"],
        database=DB_MART_CONFIG["database"],
        port=DB_MART_CONFIG["port"],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
        connect_timeout=5
    )
