#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script chạy riêng bước Transform với logging cấu trúc giống run_daily.bat.
"""

import os
import sys
from pathlib import Path
from typing import Optional

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from transform.transform_service import run_transform, logger  # noqa: E402

RAW_PATTERN = "raw_laptop_*.csv"


def find_latest_raw_csv() -> Optional[str]:
    """Tìm file raw mới nhất trong data_output giống như automation."""
    data_dir = ROOT_DIR / "data_output"
    if not data_dir.exists():
        return None
    raw_files = list(data_dir.glob(RAW_PATTERN))
    if not raw_files:
        return None
    return str(max(raw_files, key=lambda path: path.stat().st_mtime))


def main():
    if len(sys.argv) > 1:
        csv_path = sys.argv[1]
    else:
        csv_path = find_latest_raw_csv()
        if not csv_path:
            logger.error("Không tìm thấy file raw trong data_output (pattern %s)", RAW_PATTERN)
            sys.exit(1)
        logger.info("Không truyền tham số → dùng file raw mới nhất: %s", csv_path)

    logger.info("=== BẮT ĐẦU CHẠY TRANSFORM (run_transform.py) ===")
    try:
        df_clean, output_path = run_transform(csv_path, return_output_path=True)
        logger.info("✅ Transform thành công: %d dòng sạch → %s", len(df_clean), output_path)
    except Exception as exc:
        logger.exception("❌ Transform thất bại: %s", exc)
        sys.exit(1)

    logger.info("=== KẾT THÚC CHẠY TRANSFORM (run_transform.py) ===")
    sys.exit(0)


if __name__ == "__main__":
    main()
