# transform_service.py
import logging
import os
import pandas as pd
from datetime import datetime
from typing import Tuple, Union

from transform.clean_transform import clean_dataframe
from load.db_connect import mysql_connect   # ❗ ADD THIS

# ==== IMPORT LOG CONTROL ====
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

def _build_logger() -> logging.Logger:
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


def run_transform(csv_path: str, *, return_output_path: bool = False):
    """
    Transform dùng Staging → Clean → Clean CSV output
    """

    # ===== 1. Kiểm tra Extract =====
    prev = get_latest_status("extract")
    if prev != "success":
        logger.error("Extract chưa success (status=%s) → Transform dừng", prev)
        raise RuntimeError("Transform bị chặn vì Extract thất bại.")

    # ===== 2. Ghi log START =====
    log_id = start_process("transform", "Transform started")
    logger.info("=== BẮT ĐẦU TRANSFORM ===")

    try:
        # ===== 3. Đọc dữ liệu từ STAGING =====
        conn = mysql_connect()
        df = pd.read_sql("SELECT * FROM staging_laptop_raw", conn)
        conn.close()

        logger.info("Đọc từ STAGING %d dòng", len(df))

        # Optional: drop cột ID nếu có
        if "id" in df.columns:
            df = df.drop(columns=["id"])

        df.columns = [c.strip().lower() for c in df.columns]

        # ===== 4. Clean =====
        df_clean = clean_dataframe(df)
        logger.info("Clean xong: %d dòng hợp lệ", len(df_clean))

        # ===== 5. Ghi CSV output =====
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = os.path.join(OUTPUT_DIR, f"clean_laptop_{timestamp}.csv")
        df_clean.to_csv(out_path, index=False, encoding="utf-8-sig")

        logger.info("Lưu clean CSV → %s", out_path)
        logger.info("=== HOÀN TẤT TRANSFORM ===")

        # ===== 6. Log SUCCESS =====
        log_success(log_id, "Transform success")

        if return_output_path:
            return df_clean, out_path
        return df_clean

    except Exception as exc:
        logger.exception("Transform lỗi: %s", exc)
        log_fail(log_id, f"Transform failed: {exc}")
        raise
