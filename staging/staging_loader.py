# staging_loader.py
import pandas as pd
from load.db_connect import mysql_connect

STAGING_TABLE = "staging_laptop_raw"

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
                r["brand"],
                r["product_name"],
                int(r["price"]),
                r["currency"],
                r["source"],
                r["url"],
                r["timestamp"],
                r["sold_count"]
            ))
            rows += 1

    conn.commit()
    conn.close()
    return rows
