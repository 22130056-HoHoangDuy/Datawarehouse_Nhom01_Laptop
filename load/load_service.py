import os
from datetime import datetime

OUT_DIR = "data_output"
os.makedirs(OUT_DIR, exist_ok=True)


def _save_csv(df):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(OUT_DIR, f"clean_laptop_{timestamp}.csv")
    try:
        import pandas as pd
        if hasattr(df, 'to_csv'):
            df.to_csv(out_path, index=False, encoding='utf-8-sig')
            rows = len(df)
        else:
            pd.DataFrame(df).to_csv(out_path, index=False, encoding='utf-8-sig')
            rows = len(df)
    except Exception:
        with open(out_path, 'w', encoding='utf-8') as f:
            f.write(str(df))
        rows = 0
    return out_path, rows


def _load_to_mysql(df):
    # Load DB config from env or .env
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except Exception:
        pass
    db_host = os.environ.get('DB_HOST')
    db_user = os.environ.get('DB_USER')
    db_pass = os.environ.get('DB_PASS')
    db_name = os.environ.get('DB_NAME')
    db_port = os.environ.get('DB_PORT', '3306')
    if not all([db_host, db_user, db_pass, db_name]):
        raise RuntimeError('DB config not provided')

    try:
        import mysql.connector
        conn = mysql.connector.connect(host=db_host, user=db_user, password=db_pass, database=db_name, port=int(db_port))
        cur = conn.cursor()
        # create raw_products table if not exists
        create_sql = '''
        CREATE TABLE IF NOT EXISTS raw_products (
            id INT AUTO_INCREMENT PRIMARY KEY,
            url VARCHAR(1000) NOT NULL,
            brand VARCHAR(255),
            product_name TEXT,
            price DECIMAL(18,2),
            currency VARCHAR(10),
            source VARCHAR(255),
            ts DATETIME,
            sold_count INT,
            csv_path VARCHAR(1024),
            inserted_at DATETIME,
            UNIQUE KEY uq_url (url(500))
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        '''
        cur.execute(create_sql)
        conn.commit()

        # prepare insert with upsert on url
        insert_sql = ("INSERT INTO raw_products (url, brand, product_name, price, currency, source, ts, sold_count, csv_path, inserted_at) "
                      "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                      "ON DUPLICATE KEY UPDATE brand=VALUES(brand), product_name=VALUES(product_name), price=VALUES(price), "
                      "currency=VALUES(currency), source=VALUES(source), ts=VALUES(ts), sold_count=VALUES(sold_count), csv_path=VALUES(csv_path), inserted_at=VALUES(inserted_at)")

        rows = 0
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for item in (df.to_dict(orient='records') if hasattr(df, 'to_dict') else df):
            url = item.get('url')
            brand = item.get('brand')
            pname = item.get('product_name')
            price = item.get('price')
            currency = item.get('currency')
            source = item.get('source')
            ts = item.get('timestamp') or item.get('ts')
            sold = item.get('sold_count')
            cur.execute(insert_sql, (url, brand, pname, price, currency, source, ts, sold, None, now))
            rows += 1
        conn.commit()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        raise


def run_load(df):
    """Load cleaned data: try to push into MySQL (if DB config present), otherwise save CSV.
    Returns number of rows inserted/handled.
    """
    # First save CSV as archive
    out_path, saved_rows = _save_csv(df)

    # then attempt DB load if config available
    try:
        rows = _load_to_mysql(df)
        print(f"run_load: inserted {rows} rows into DB and saved CSV -> {out_path}")
        # record load run into DB (if log store available)
        try:
            from .log_store import insert_load
            insert_load(status='success', rows_inserted=rows, csv_path=out_path)
        except Exception:
            pass
        return rows
    except Exception as e:
        print(f"run_load DB error: {e}. Falling back to CSV only. Saved rows: {saved_rows} -> {out_path}")
        try:
            from .log_store import insert_load
            insert_load(status='failure', message=str(e), rows_inserted=saved_rows, csv_path=out_path)
        except Exception:
            pass
        return saved_rows
