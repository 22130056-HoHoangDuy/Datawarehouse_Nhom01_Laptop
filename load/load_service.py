# load_service.py
# FLOW 3.4.1 – Service chính điều phối toàn bộ quá trình LOAD

from load.db_connect import mysql_connect
from load.schema import ensure_schema
from load.dim_loader import upsert_dim, upsert_dim_time
from load.fact_loader import load_fact_sales
from load.load_logger import logger
import pandas as pd

def run_load(df: pd.DataFrame):
    if df is None or df.empty:
        logger.error("FLOW 3.4.0: DataFrame rỗng → không thực hiện load")
        return 0

    logger.info("="*60)
    logger.info("FLOW 3.4.1: BẮT ĐẦU QUÁ TRÌNH LOAD")
    logger.info(f"Số bản ghi nhận được: {len(df):,}")
    logger.info("="*60)

    conn = None
    cur = None
    try:
        # FLOW 3.4.4 - 3.4.5: kết nối DB
        conn = mysql_connect()
        cur = conn.cursor()

        # FLOW 3.4.7: đảm bảo schema
        ensure_schema(conn)

        # FLOW 3.4.8 - 3.4.16: upsert dimensions
        logger.info("FLOW 3.4.8: UPSERT Dimensions...")
        brands = sorted({str(x).strip().upper() for x in df["brand"] if pd.notna(x)})
        sources = sorted({str(x).strip().lower() for x in df["source"] if pd.notna(x)})
        products = sorted({str(x).strip() for x in df["product_name"] if pd.notna(x)})

        brand_ids = upsert_dim(cur, "dim_brand", "brand_name", brands)
        source_ids = upsert_dim(cur, "dim_source", "source_name", sources)
        product_ids = upsert_dim(cur, "dim_product", "product_name", products)

        time_pairs = sorted({
            (str(d).split(" ")[0], int(h))
            for d, h in zip(df["crawl_date"], df["crawl_hour"])
            if pd.notna(d) and pd.notna(h)
        })
        time_ids = upsert_dim_time(cur, time_pairs)

        conn.commit()
        logger.info(f"Dimension upsert thành công: {len(brand_ids)} brand, {len(source_ids)} source, {len(product_ids)} product, {len(time_ids)} time")

        # FLOW 3.4.19: load fact
        logger.info("FLOW 3.4.19: Bắt đầu load fact_sales...")
        rows = load_fact_sales(cur, df, brand_ids, source_ids, product_ids, time_ids)
        conn.commit()

        logger.info(f"FLOW 3.4.21: LOAD HOÀN TẤT – {rows:,} bản ghi fact_sales")
        logger.info("="*60)
        return rows

    except Exception as e:
        logger.error(f"FLOW 3.4.23: LỖI NGHIÊM TRỌNG trong quá trình LOAD: {e}", exc_info=True)
        if conn:
            conn.rollback()
            logger.info("FLOW 3.4.24: Đã rollback transaction")
        return 0

    finally:
        if cur: cur.close()
        if conn: conn.close()
        logger.debug("FLOW 3.4.27: Đã đóng kết nối MySQL")