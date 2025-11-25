import os
import logging
from datetime import datetime
import pandas as pd

from transform.clean_transform import clean_dataframe
from control.log_store import start_process, log_success, log_fail

LOG_DIR = "logs"
OUTPUT_DIR = "data_output"
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

def _build_logger():
    logger = logging.getLogger("transform")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    log_path = os.path.join(LOG_DIR, f"transform_{datetime.now().strftime('%Y%m%d')}.log")
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger.addHandler(sh)
    return logger

logger = _build_logger()

def run_transform(csv_path=None, *, return_output_path=False):
    """Transform dữ liệu từ staging hoặc từ CSV."""
    
    # BẮT ĐẦU GHI LOG
    log_id = start_process("transform", "Transform started")
    logger.info("=== BẮT ĐẦU TRANSFORM ===")

    try:
        # --- 1. Đọc dữ liệu ---
        if csv_path:
            logger.info("Đọc CSV raw: %s", csv_path)
            df = pd.read_csv(csv_path)
        else:
            raise ValueError("Transform hiện yêu cầu csv_path")

        if df.empty:
            raise ValueError("DataFrame trống, không thể transform")

        # --- 2. Drop ID nếu có ---
        if "id" in df.columns:
            df = df.drop(columns=["id"])

        # --- 3. Chuẩn hóa tên cột ---
        df.columns = [c.strip().lower() for c in df.columns]

        # --- 4. Clean ---
        df_clean = clean_dataframe(df)
        logger.info("Clean xong: %d dòng hợp lệ", len(df_clean))

        # --- 5. Xuất CSV ---
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = os.path.join(OUTPUT_DIR, f"clean_laptop_{ts}.csv")
        df_clean.to_csv(out_path, index=False, encoding="utf-8-sig")
        logger.info("Lưu clean CSV → %s", out_path)

        # --- 6. Ghi log thành công ---
        log_success(log_id, "Transform success")

        if return_output_path:
            return df_clean, out_path
        return df_clean

    except Exception as e:
        logger.exception("Transform thất bại: %s", e)
        log_fail(log_id, str(e))
        raise
