# dim_loader.py
# FLOW 3.2.1

ID_COLS = {
    "dim_brand":  "brand_id",
    "dim_source": "source_id",
    "dim_product": "product_id",
}

# 3.2.2: upsert dim
def upsert_dim(cur, table, col, values):
    id_col = ID_COLS[table]
    sql = f"""
        INSERT INTO {table} ({col})
        VALUES (%s)
        ON DUPLICATE KEY UPDATE
            {col} = VALUES({col}),
            {id_col} = LAST_INSERT_ID({id_col})
    """
    ids = {}
    for v in values:  # 3.2.3
        cur.execute(sql, (v,))  # 3.2.4
        ids[v] = cur.lastrowid  # 3.2.5
    return ids

# 3.2.6: upsert dim_time
def upsert_dim_time(cur, pairs):
    sql = """
        INSERT INTO dim_time (crawl_date, crawl_hour)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE
            time_id = LAST_INSERT_ID(time_id)
    """
    ids = {}
    for d, h in pairs:  # 3.2.7
        cur.execute(sql, (d, h))  # 3.2.8
        ids[(d, h)] = cur.lastrowid  # 3.2.9
    return ids
