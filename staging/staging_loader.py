# staging_loader.py
import os
import pandas as pd
import pymysql
from dotenv import load_dotenv
from pathlib import Path

# Load .env
BASE_DIR = Path(__file__).resolve().parents[1]
env_path = BASE_DIR / ".env"
load_dotenv(env_path)

DB_STAGING_HOST = os.getenv("DB_STAGING_HOST", "localhost")
DB_STAGING_USER = os.getenv("DB_STAGING_USER", "root")
DB_STAGING_PASS = os.getenv("DB_STAGING_PASS", "")
DB_STAGING_NAME = os.getenv("DB_STAGING_NAME", "staging")
DB_STAGING_PORT = int(os.getenv("DB_STAGING_PORT", "3306"))

STAGING_TABLE = "staging_laptop_raw"


def staging_connect():
    """Kết nối tới DB STAGING."""
    conn = pymysql.connect(
        host=DB_STAGING_HOST,
        user=DB_STAGING_USER,
        password=DB_STAGING_PASS,
        database=DB_STAGING_NAME,
        port=DB_STAGING_PORT,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False
    )
    return conn


def ensure_staging_table(conn):
    """Tạo bảng staging_laptop_raw nếu chưa có (đúng schema đã dùng)."""
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {STAGING_TABLE} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        brand VARCHAR(100),
        product_name VARCHAR(255),
        price INT,
        currency VARCHAR(10),
        source VARCHAR(50),
        url TEXT,
        timestamp DATETIME,
        sold_count INT
    );
    """
    with conn.cursor() as cur:
        cur.execute(ddl)


def load_to_staging(csv_path: str):
    # Đọc CSV
    df = pd.read_csv(csv_path)

    # Ép kiểu numeric an toàn
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0).astype(int)
    df["sold_count"] = pd.to_numeric(df["sold_count"], errors="coerce").fillna(0).astype(int)

    # Xử lý NaN cho toàn bộ DF 
    df = df.replace({pd.NA: None, pd.NaT: None, "": None}).where(pd.notnull(df), None)

    conn = staging_connect()
    print(f"[STAGING] Using DB: {DB_STAGING_NAME}")

    ensure_staging_table(conn)

    with conn.cursor() as cur:
        # Xóa dữ liệu cũ
        cur.execute(f"TRUNCATE TABLE {STAGING_TABLE}")

        sql = f"""
            INSERT INTO {STAGING_TABLE}
                (brand, product_name, price, currency, source, url, timestamp, sold_count)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """

        rows = 0
        for _, r in df.iterrows():
            cur.execute(sql, (
                r["brand"],
                r["product_name"],
                int(r["price"]),
                r["currency"],
                r["source"],
                r["url"],
                r["timestamp"],
                int(r["sold_count"]) if r["sold_count"] is not None else None,
            ))
            rows += 1

    conn.commit()
    conn.close()
    return rows
