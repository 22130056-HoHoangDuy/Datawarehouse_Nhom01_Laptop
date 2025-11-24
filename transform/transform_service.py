# transform_service.py
import logging
import os
import pandas as pd
from datetime import datetime
from typing import Tuple, Union

import pandas as pd

from transform.clean_transform import clean_dataframe

LOG_DIR = "logs"
OUTPUT_DIR = "data_output"
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)


def _build_logger() -> logging.Logger:
    """Create a module-level logger that mirrors other pipeline logs."""
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


def run_transform(csv_path: str, *, return_output_path: bool = False) -> Union[pd.DataFrame, Tuple[pd.DataFrame, str]]:
    """
    Nhận CSV từ Extract → trả ra DataFrame sạch để Load.

    Args:
        csv_path: Đường dẫn tới file CSV raw.
        return_output_path: Nếu True, trả về cả đường dẫn file clean đã lưu.

    Returns:
        DataFrame sạch (và đường dẫn file clean nếu `return_output_path=True`).
    """
    if not os.path.exists(csv_path):
        logger.error("Không tìm thấy file CSV: %s", csv_path)
        raise FileNotFoundError(f"Không tìm thấy file CSV: {csv_path}")

    logger.info("=== BẮT ĐẦU TRANSFORM ===")
    logger.info("Đọc dữ liệu từ: %s", csv_path)

    df = pd.read_csv(csv_path)
    logger.info("Đọc thành công %d dòng, %d cột", len(df), len(df.columns))

    df.columns = [c.strip().lower() for c in df.columns]
    logger.debug("Tên cột chuẩn hóa: %s", df.columns.tolist())

    logger.info("Bắt đầu làm sạch dữ liệu...")
    df_clean = clean_dataframe(df)
    logger.info("Làm sạch xong, giữ lại %d dòng hợp lệ", len(df_clean))

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(OUTPUT_DIR, f"clean_laptop_{timestamp}.csv")
    df_clean.to_csv(out_path, index=False, encoding="utf-8-sig")
    logger.info("Đã lưu dữ liệu sạch → %s", out_path)
    logger.info("=== HOÀN TẤT TRANSFORM ===")

    if return_output_path:
        return df_clean, out_path
    return df_clean
