# transform_service.py
import logging
import os
import pandas as pd
import re
from datetime import datetime
from transform.clean_transform import clean_dataframe
from transform.db_connect import staging_connect

from control.log_store import (
    start_process,
    get_latest_status,
    log_success,
    log_fail
)

LOG_DIR = "logs"
OUTPUT_DIR = "data_output"
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

BRAND_WHITELIST = [
    "DELL", "ASUS", "ACER", "HP", "LENOVO", "MSI",
    "APPLE", "MACBOOK", "RAZER", "GIGABYTE", "HUAWEI",
    "MICROSOFT", "SAMSUNG", "XIAOMI", "REALME"
]

REMOVE_KEYWORDS = [
    r"top ", r"top\d", "top 10", 
    "nên mua", "có nên", "là gì", "hướng dẫn",
    "review", "so sánh", "tư vấn", "gì", "?", "!"
]

# ==== LOGGER ====
def _build_logger():
    logger = logging.getLogger("transform")
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    log_path = os.path.join(LOG_DIR, f"transform_{datetime.now().strftime('%Y%m%d')}.log")

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger

logger = _build_logger()


# ======================================================================
#                           RUN TRANSFORM
# ======================================================================

def run_transform(csv_path: str, *, return_output_path=False):

    # 1. CHECK EXTRACT
    prev = get_latest_status("extract")
    if prev != "success":
        raise RuntimeError("Extract chưa success → dừng Transform")

    log_id = start_process("transform", "Transform started")
    logger.info("=== BẮT ĐẦU TRANSFORM ===")

    try:
        # 2. LOAD FROM STAGING
        conn = staging_connect()
        df = pd.read_sql("SELECT * FROM staging_laptop_raw", conn, dtype=str)
        conn.close()

        # Remove header rows accidentally inserted
        df = df[df["brand"] != "brand"]
        df = df[df["product_name"] != "product_name"]

        # Drop NULL brand or NULL product_name
        df = df.dropna(subset=["brand", "product_name"])

        # CAST to string
        df["brand"] = df["brand"].astype(str)
        df["product_name"] = df["product_name"].astype(str)

        logger.info("Đọc từ STAGING %d dòng", len(df))

        # 3. REMOVE NON-LAPTOP CONTENT
        # remove brand == NULL or rác
        df = df[df["brand"].str.upper().isin(BRAND_WHITELIST)]

        # remove bài viết tin tức – an toàn regex (escape)
        for kw in REMOVE_KEYWORDS:
            safe_kw = re.escape(kw)
            df = df[~df["product_name"].str.contains(safe_kw, case=False, na=False)]

        # remove product_name quá ngắn (<5)
        df = df[df["product_name"].str.len() > 5]

        # drop column id nếu có
        if "id" in df.columns:
            df = df.drop(columns=["id"])

        # chuẩn hóa tên cột
        df.columns = [c.strip().lower() for c in df.columns]

        # 4. CLEAN RULES
        df_clean = clean_dataframe(df)
        logger.info("Clean xong: %d dòng hợp lệ", len(df_clean))

        # 5. SAVE CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = os.path.join(OUTPUT_DIR, f"clean_laptop_{timestamp}.csv")
        df_clean.to_csv(out_path, index=False, encoding="utf-8-sig")
        logger.info("Lưu clean CSV → %s", out_path)

        log_success(log_id, "Transform success")
        if return_output_path:
            return df_clean, out_path
        return df_clean

    except Exception as exc:
        logger.exception("Transform lỗi: %s", exc)
        log_fail(log_id, f"Transform failed: {exc}")
        raise
