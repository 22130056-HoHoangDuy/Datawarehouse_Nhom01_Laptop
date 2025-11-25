# load_service.py
import os
import logging
from datetime import datetime
import pandas as pd

from load.db_connect import mysql_connect
from load.schema import ensure_schema
from load.dim_loader import upsert_dim, upsert_dim_time
from load.fact_loader import load_fact_sales

# ==== IMPORT LOG CONTROL ====
from control.log_store import (
    start_process,
    log_success,
    log_fail,
    get_latest_status
)

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

def _build_logger():
    logger = logging.getLogger("load")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)

    logfile = os.path.join(LOG_DIR, f"load_{datetime.now().strftime('%Y%m%d')}.log")
    fh = logging.FileHandler(logfile, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger

logger = _build_logger()


def run_load(clean_csv_path: str):
    """
    Load vào Data Warehouse:
        - Kiểm tra Transform success
        - Ghi log vào db.control
        - Tải dim + fact
    """

    # ===========================================
    # 🔥 1. Check Transform trước
    # ===========================================
    prev_status = get_latest_status("transform")
    if prev_status != "success":
        logger.error("⛔ Transform chưa success (status=%s) → Load KHÔNG CHẠY", prev_status)
        raise RuntimeError("Load bị chặn vì Transform thất bại.")

    # ===========================================
    # 🔥 2. Log START vào db.control
    # ===========================================
    log_id = start_process("load", "Load started")
    logger.info("=== BẮT ĐẦU LOAD ===")

    try:
        if not os.path.exists(clean_csv_path):
            raise FileNotFoundError(f"File clean CSV không tồn tại: {clean_csv_path}")

        df = pd.read_csv(clean_csv_path)
        logger.info("Đọc %d dòng sạch để load DW", len(df))

        conn = mysql_connect()
        ensure_schema(conn)

        with conn.cursor() as cur:
            # ====== Load DIM ======
            logger.info("Upsert DIM...")
            brand_ids = upsert_dim(cur, "dim_brand", "brand_name", df["brand"].astype(str).str.upper().unique())
            source_ids = upsert_dim(cur, "dim_source", "source_name", df["source"].astype(str).str.lower().unique())
            product_ids = upsert_dim(cur, "dim_product", "product_name", df["product_name"].astype(str).unique())

            date_hour_pairs = list(zip(df["crawl_date"], df["crawl_hour"]))
            time_ids = upsert_dim_time(cur, date_hour_pairs)

            # ====== Load FACT ======
            logger.info("Upsert FACT...")
            rows = load_fact_sales(cur, df, brand_ids, source_ids, product_ids, time_ids)

        conn.commit()
        conn.close()

        logger.info("✔ Load thành công %d dòng", rows)
        logger.info("=== HOÀN TẤT LOAD ===")

        # ===========================================
        # 🔥 3. Log SUCCESS
        # ===========================================
        log_success(log_id, f"Load success: {rows} rows")

        logger.info(f"FLOW 3.4.21: LOAD HOÀN TẤT – {rows:,} bản ghi fact_sales")
        logger.info("="*60)
        return rows

    except Exception as exc:
        logger.exception("❌ Load lỗi: %s", exc)

        # ===========================================
        # 🔥 4. Log FAIL
        # ===========================================
        log_fail(log_id, f"Load failed: {exc}")
        raise
