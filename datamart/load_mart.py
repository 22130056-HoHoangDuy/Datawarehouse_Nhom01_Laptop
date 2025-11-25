# load_mart.py
import logging
from datetime import datetime
import pandas as pd

from load.db_connect import mysql_connect

logger = logging.getLogger("load")

def ensure_mart_schema(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mart_brand_summary (
                brand_name VARCHAR(100),
                total_products INT,
                avg_price INT,
                min_price INT,
                max_price INT,
                latest_update DATETIME
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS mart_source_summary (
                source_name VARCHAR(100),
                total_products INT,
                avg_price INT,
                min_price INT,
                max_price INT,
                latest_update DATETIME
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS mart_daily_price_stats (
                crawl_date DATE,
                total_products INT,
                avg_price INT,
                min_price INT,
                max_price INT
            );
        """)

    conn.commit()


def load_datamart():
    conn = mysql_connect()
    ensure_mart_schema(conn)

    logger.info("=== BẮT ĐẦU LOAD DATA MART ===")

    # đọc fact + dim
    df = pd.read_sql("""
        SELECT 
            f.price, f.sold_count, f.timestamp,
            b.brand_name,
            s.source_name,
            t.crawl_date
        FROM fact_sales f
        JOIN dim_brand b ON f.brand_id = b.brand_id
        JOIN dim_source s ON f.source_id = s.source_id
        JOIN dim_time t ON f.time_id = t.time_id
    """, conn)

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ====== MART 1: BRAND SUMMARY ======
    brand_df = df.groupby("brand_name").agg(
        total_products=("price", "count"),
        avg_price=("price", "mean"),
        min_price=("price", "min"),
        max_price=("price", "max")
    ).reset_index()
    brand_df["latest_update"] = now

    with conn.cursor() as cur:
        cur.execute("DELETE FROM mart_brand_summary")
        for _, r in brand_df.iterrows():
            cur.execute("""
                INSERT INTO mart_brand_summary
                    (brand_name, total_products, avg_price, min_price, max_price, latest_update)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                r["brand_name"],
                int(r["total_products"]),
                int(r["avg_price"]),
                int(r["min_price"]),
                int(r["max_price"]),
                now
            ))

    # ====== MART 2: SOURCE SUMMARY ======
    src_df = df.groupby("source_name").agg(
        total_products=("price", "count"),
        avg_price=("price", "mean"),
        min_price=("price", "min"),
        max_price=("price", "max")
    ).reset_index()
    src_df["latest_update"] = now

    with conn.cursor() as cur:
        cur.execute("DELETE FROM mart_source_summary")
        for _, r in src_df.iterrows():
            cur.execute("""
                INSERT INTO mart_source_summary
                    (source_name, total_products, avg_price, min_price, max_price, latest_update)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                r["source_name"],
                int(r["total_products"]),
                int(r["avg_price"]),
                int(r["min_price"]),
                int(r["max_price"]),
                now
            ))

    # ====== MART 3: DAILY PRICE STATS ======
    date_df = df.groupby("crawl_date").agg(
        total_products=("price", "count"),
        avg_price=("price", "mean"),
        min_price=("price", "min"),
        max_price=("price", "max")
    ).reset_index()

    with conn.cursor() as cur:
        cur.execute("DELETE FROM mart_daily_price_stats")
        for _, r in date_df.iterrows():
            cur.execute("""
                INSERT INTO mart_daily_price_stats
                    (crawl_date, total_products, avg_price, min_price, max_price)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                str(r["crawl_date"]),
                int(r["total_products"]),
                int(r["avg_price"]),
                int(r["min_price"]),
                int(r["max_price"])
            ))

    conn.commit()
    conn.close()

    logger.info("=== HOÀN TẤT LOAD DATA MART ===")
