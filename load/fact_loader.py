# fact_loader.py
# FLOW 3.3.1 – Load dữ liệu vào fact_sales

from tqdm import tqdm
import pandas as pd
from load.load_logger import logger

# FLOW 3.3.2: hàm load fact_sales
def load_fact_sales(cur, df, brand_ids, source_ids, product_ids, time_ids):
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
    skipped = 0

    logger.info(f"FLOW 3.3.3: Bắt đầu load {len(df)} dòng vào fact_sales")

    for idx, r in tqdm(df.iterrows(), total=len(df), desc="Loading fact_sales"):
        try:
            # FLOW 3.3.4 - 3.3.7: lấy ID từ các dim
            b_id = brand_ids.get(str(r["brand"]).strip().upper())
            s_id = source_ids.get(str(r["source"]).strip().lower())
            p_id = product_ids.get(str(r["product_name"]).strip())
            t_key = (str(r["crawl_date"]).split(" ")[0], int(r["crawl_hour"]))
            t_id = time_ids.get(t_key)

            # FLOW 3.3.8: kiểm tra đủ khóa ngoại không
            if not all([b_id, s_id, p_id, t_id]):
                skipped += 1
                if skipped <= 10:
                    logger.warning(f"SKIP dòng {idx}: thiếu khóa ngoại - brand={r['brand']}, source={r['source']}, product={r['product_name']}, time={t_key}")
                continue

            # FLOW 3.3.9 - 3.3.10: xử lý sold_count
            sold_count = None
            sc = r.get("sold_count")
            if pd.notna(sc):
                s = str(sc).lower().replace("k", "000").replace(",", "").replace(".", "")
                try:
                    sold_count = int(float(s))
                except:
                    logger.debug(f"sold_count không parse được: '{sc}'")

            # FLOW 3.3.11: execute insert/update
            cur.execute(sql, (
                int(p_id), int(b_id), int(s_id), int(t_id),
                int(float(r["price"])) if pd.notna(r["price"]) else 0,
                sold_count,
                str(r["timestamp"]) if pd.notna(r["timestamp"]) else None
            ))
            rows += 1  # FLOW 3.3.12

        except Exception as e:
            logger.error(f"FLOW 3.3.13: Lỗi insert dòng {idx}: {e} | Data: {dict(r)}", exc_info=True)
            skipped += 1
            continue

    logger.info(f"fact_sales hoàn tất: {rows} dòng thành công, {skipped} dòng bị bỏ qua")
    return rows  # FLOW 3.3.14