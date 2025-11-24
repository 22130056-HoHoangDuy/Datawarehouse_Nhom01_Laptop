# dim_loader.py
ID_COLS = {
    "dim_brand":  "brand_id",
    "dim_source": "source_id",
    "dim_product": "product_id",
}

def upsert_dim(cur, table, col, values):
    """
    Upsert danh sách dim (brand, source, product)
    """
    id_col = ID_COLS[table]
    sql = f"""
        INSERT INTO {table} ({col})
        VALUES (%s)
        ON DUPLICATE KEY UPDATE
            {col} = VALUES({col}),
            {id_col} = LAST_INSERT_ID({id_col})
    """

    ids = {}
    for v in values:
        cur.execute(sql, (v,))
        ids[v] = cur.lastrowid
    return ids

def upsert_dim_time(cur, pairs):
    """
    pairs = [(crawl_date, crawl_hour), ...]
    """
    sql = """
        INSERT INTO dim_time (crawl_date, crawl_hour)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE
            time_id = LAST_INSERT_ID(time_id)
    """
    ids = {}
    for d, h in pairs:
        cur.execute(sql, (d, h))
        ids[(d, h)] = cur.lastrowid
    return ids
