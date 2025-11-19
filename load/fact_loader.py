from tqdm import tqdm
import pandas as pd

def load_fact_sales(cur, df, brand_ids, source_ids, product_ids, time_ids):
    """
    Nạp dữ liệu vào fact_sales với cơ chế UPSERT.
    Nếu trùng (product_id, source_id, time_id) -> UPDATE.
    """

    sql = """
        INSERT INTO fact_sales
            (product_id, brand_id, source_id, time_id, price, sold_count, `timestamp`)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            price = VALUES(price),
            sold_count = VALUES(sold_count),
            `timestamp` = VALUES(`timestamp`);
    """

    rows = 0

    for _, r in tqdm(df.iterrows(), total=len(df), desc="Loading fact_sales"):
        try:
            b_id = brand_ids.get(str(r["brand"]).upper())
            s_id = source_ids.get(str(r["source"]).lower())
            p_id = product_ids.get(str(r["product_name"]))
            t_id = time_ids.get((str(r["crawl_date"]), int(r["crawl_hour"])))

            # Bỏ qua các dòng không có DIM id
            if not all([b_id, s_id, p_id, t_id]):
                continue

            # Chuẩn hóa sold_count
            sc = r.get("sold_count")
            sold_count = None
            if pd.notna(sc):
                s = str(sc).lower().replace("k", "000").replace(",", "")
                try:
                    sold_count = int(float(s))
                except:
                    sold_count = None

            # Thực thi UPSERT
            cur.execute(sql, (
                int(p_id),
                int(b_id),
                int(s_id),
                int(t_id),
                int(r["price"]),
                sold_count,
                str(r["timestamp"])
            ))

            rows += 1

        except Exception as e:
            print(f"Lỗi nạp dòng: {e}")
            continue

    return rows