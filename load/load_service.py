# load_service.py
import os
from datetime import datetime
from load.db_connect import mysql_connect
from load.schema import ensure_schema
from load.dim_loader import upsert_dim, upsert_dim_time
from load.fact_loader import load_fact_sales

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

def run_load(df):
    print("[LOAD] Kết nối MySQL...")
    conn = mysql_connect()
    cur = conn.cursor()

    try:
        print("[LOAD] Kiểm tra Schema...")
        ensure_schema(conn)

        print("[LOAD] UPSERT Dimensions...")
        brands = sorted(set(df["brand"]))
        sources = sorted(set(df["source"]))
        products = sorted(set(df["product_name"]))

        brand_ids = upsert_dim(cur, "dim_brand",  "brand_name",  brands)
        source_ids = upsert_dim(cur, "dim_source", "source_name", sources)
        product_ids = upsert_dim(cur, "dim_product","product_name", products)

        # DIM TIME
        time_pairs = sorted(set((d, int(h)) for d, h in zip(df["crawl_date"], df["crawl_hour"])))
        time_ids = upsert_dim_time(cur, time_pairs)

        conn.commit()

        print("[LOAD] Nạp FACT...")
        rows = load_fact_sales(cur, df, brand_ids, source_ids, product_ids, time_ids)

        conn.commit()
        print(f"[LOAD] Thành công: {rows} rows fact_sales")

        return rows

    except Exception as e:
        print(f"[LOAD] ERROR: {e}")
        conn.rollback()
        return 0

    finally:
        cur.close()
        conn.close()
