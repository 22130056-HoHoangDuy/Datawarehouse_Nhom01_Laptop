# dim_loader.py
ID_COLS = {
    "dim_brand": "brand_id",
    "dim_source": "source_id",
    "dim_product": "product_id",
}

def upsert_dim(cur, table, col, values):
    """
    Upsert DIM đúng chuẩn → LẤY ID CHUẨN kể cả insert hoặc existed
    """
    id_col = ID_COLS[table]

    insert_sql = f"""
        INSERT INTO {table} ({col})
        VALUES (%s)
        ON DUPLICATE KEY UPDATE {col} = VALUES({col})
    """

    select_sql = f"SELECT {id_col} FROM {table} WHERE {col}=%s"

    ids = {}

    for v in values:
        # luôn insert trước
        cur.execute(insert_sql, (v,))
        # sau đó lấy ID đúng
        cur.execute(select_sql, (v,))
        row = cur.fetchone()
        ids[v] = row[id_col]

    return ids


def upsert_dim_time(cur, pairs):
    """
    Insert hoặc lấy time_id đúng cách.
    """
    insert_sql = """
        INSERT INTO dim_time (crawl_date, crawl_hour)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE crawl_date = VALUES(crawl_date)
    """

    select_sql = """
        SELECT time_id FROM dim_time WHERE crawl_date=%s AND crawl_hour=%s
    """

    ids = {}

    for d, h in pairs:
        cur.execute(insert_sql, (d, h))
        cur.execute(select_sql, (d, h))
        row = cur.fetchone()
        ids[(d, h)] = row["time_id"]

    return ids
