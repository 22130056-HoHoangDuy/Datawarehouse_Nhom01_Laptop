# staging_loader.py
import pandas as pd
from load.db_connect import mysql_connect
import math

STAGING_TABLE = "staging_laptop_raw"

def clean_value(val):
    """Chuyển NaN thành None để MySQL chấp nhận"""
    if isinstance(val, float) and math.isnan(val):
        return None
    return val

def load_to_staging(csv_path: str):
    df = pd.read_csv(csv_path)

    conn = mysql_connect()
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {STAGING_TABLE}")

        sql = f"""
            INSERT INTO {STAGING_TABLE}
                (brand, product_name, price, currency, source, url, timestamp, sold_count)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """

        rows = 0
        for _, r in df.iterrows():
            cur.execute(sql, (
                clean_value(r["brand"]),
                clean_value(r["product_name"]),
                None if pd.isna(r["price"]) else int(r["price"]),
                clean_value(r["currency"]),
                clean_value(r["source"]),
                clean_value(r["url"]),
                clean_value(r["timestamp"]),
                clean_value(r["sold_count"])
            ))
            rows += 1

    conn.commit()
    conn.close()
    return rows
