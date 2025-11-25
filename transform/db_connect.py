import os
import pymysql
from dotenv import load_dotenv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
env_path = BASE_DIR / ".env"
load_dotenv(env_path)

def staging_connect():
    return pymysql.connect(
        host=os.getenv("DB_STAGING_HOST", "localhost"),
        user=os.getenv("DB_STAGING_USER", "root"),
        password=os.getenv("DB_STAGING_PASS", ""),
        database=os.getenv("DB_STAGING_NAME", "staging"),
        port=int(os.getenv("DB_STAGING_PORT", 3306)),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )
