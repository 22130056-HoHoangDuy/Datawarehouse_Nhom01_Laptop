#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Transform Standalone Script
Chạy transform độc lập với logging chi tiết và retry mechanism (max 5 lần)
Tương thích Python 3.13.7
"""

import os
import sys
import logging
import traceback
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

# Thêm thư mục gốc vào PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from transform.clean_transform import clean_dataframe

# ==================== CONFIGURATION ====================
MAX_RETRIES = 5
RETRY_DELAY = 2  # seconds
LOG_DIR = "logs"
OUTPUT_DIR = "data_output"
LOG_FILE_FORMAT = "transform_standalone_{date}.log"

# ==================== SETUP LOGGING ====================
def setup_logging() -> logging.Logger:
    """Thiết lập logging chi tiết"""
    os.makedirs(LOG_DIR, exist_ok=True)
    
    log_date = datetime.now().strftime("%Y%m%d")
    log_file = os.path.join(LOG_DIR, LOG_FILE_FORMAT.format(date=log_date))
    
    # Tạo logger
    logger = logging.getLogger("TransformStandalone")
    logger.setLevel(logging.DEBUG)
    
    # Xóa handlers cũ nếu có
    logger.handlers.clear()
    
    # File handler - ghi tất cả log vào file
    file_handler = logging.FileHandler(
        log_file, 
        mode='a', 
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)
    
    # Console handler - chỉ hiển thị INFO trở lên
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    console_handler.setFormatter(console_formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

# ==================== UTILITY FUNCTIONS ====================
def find_latest_raw_file() -> Optional[str]:
    """Tìm file raw CSV mới nhất trong data_output"""
    output_path = Path(OUTPUT_DIR)
    if not output_path.exists():
        return None
    
    raw_files = list(output_path.glob("raw_laptop_*.csv"))
    if not raw_files:
        return None
    
    # Sắp xếp theo thời gian modified, lấy file mới nhất
    latest_file = max(raw_files, key=lambda p: p.stat().st_mtime)
    return str(latest_file)

def validate_csv_file(csv_path: str, logger: logging.Logger) -> bool:
    """Kiểm tra file CSV có hợp lệ không"""
    if not os.path.exists(csv_path):
        logger.error(f"File không tồn tại: {csv_path}")
        return False
    
    if not csv_path.lower().endswith('.csv'):
        logger.error(f"File không phải CSV: {csv_path}")
        return False
    
    try:
        file_size = os.path.getsize(csv_path)
        if file_size == 0:
            logger.error(f"File rỗng: {csv_path}")
            return False
        logger.info(f"Kích thước file: {file_size:,} bytes")
        return True
    except Exception as e:
        logger.error(f"Lỗi kiểm tra file: {e}")
        return False

def read_csv_with_retry(csv_path: str, logger: logging.Logger, max_retries: int = 3) -> Optional[pd.DataFrame]:
    """Đọc CSV với retry mechanism"""
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Đang đọc CSV (lần thử {attempt}/{max_retries})...")
            df = pd.read_csv(csv_path, encoding='utf-8-sig')
            logger.info(f"Đọc thành công: {len(df)} dòng, {len(df.columns)} cột")
            logger.debug(f"Các cột: {list(df.columns)}")
            return df
        except UnicodeDecodeError:
            try:
                logger.warning(f"Lỗi encoding UTF-8, thử encoding khác...")
                df = pd.read_csv(csv_path, encoding='latin-1')
                logger.info(f"Đọc thành công với encoding latin-1: {len(df)} dòng")
                return df
            except Exception as e:
                logger.error(f"Lỗi encoding (lần thử {attempt}/{max_retries}): {e}")
                if attempt < max_retries:
                    time.sleep(RETRY_DELAY)
        except Exception as e:
            logger.error(f"Lỗi đọc CSV (lần thử {attempt}/{max_retries}): {e}")
            logger.debug(traceback.format_exc())
            if attempt < max_retries:
                time.sleep(RETRY_DELAY)
    
    return None

# ==================== MAIN TRANSFORM FUNCTION ====================
def run_transform_once(csv_path: str, logger: logging.Logger) -> Optional[pd.DataFrame]:
    """Chạy transform một lần"""
    try:
        # Bước 1: Đọc CSV
        logger.info("=" * 60)
        logger.info("BƯỚC 1: ĐỌC FILE CSV")
        logger.info("=" * 60)
        logger.info(f"Đường dẫn file: {csv_path}")
        
        df = read_csv_with_retry(csv_path, logger)
        if df is None:
            raise Exception("Không thể đọc file CSV")
        
        initial_rows = len(df)
        logger.info(f"Số dòng ban đầu: {initial_rows:,}")
        logger.info(f"Số cột: {len(df.columns)}")
        
        # Bước 2: Chuẩn hóa tên cột
        logger.info("=" * 60)
        logger.info("BƯỚC 2: CHUẨN HÓA TÊN CỘT")
        logger.info("=" * 60)
        old_columns = list(df.columns)
        df.columns = [c.strip().lower() for c in df.columns]
        logger.debug(f"Cột cũ: {old_columns}")
        logger.debug(f"Cột mới: {list(df.columns)}")
        
        # Bước 3: Kiểm tra cột bắt buộc
        logger.info("=" * 60)
        logger.info("BƯỚC 3: KIỂM TRA CỘT BẮT BUỘC")
        logger.info("=" * 60)
        REQUIRED_COLS = ['brand', 'product_name', 'price', 'currency', 'source', 'timestamp', 'sold_count']
        missing_cols = [col for col in REQUIRED_COLS if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Thiếu các cột bắt buộc: {missing_cols}")
        logger.info(f"Tất cả cột bắt buộc đã có: {REQUIRED_COLS}")
        
        # Bước 4: Thống kê dữ liệu trước khi clean
        logger.info("=" * 60)
        logger.info("BƯỚC 4: THỐNG KÊ DỮ LIỆU TRƯỚC KHI CLEAN")
        logger.info("=" * 60)
        logger.info(f"Số dòng: {len(df):,}")
        logger.info(f"Số dòng có brand: {df['brand'].notna().sum():,}")
        logger.info(f"Số dòng có product_name: {df['product_name'].notna().sum():,}")
        logger.info(f"Số dòng có price: {df['price'].notna().sum():,}")
        logger.info(f"Số dòng có price > 0: {(df['price'].notna() & (df['price'] > 0)).sum():,}")
        logger.info(f"Số dòng có sold_count: {df['sold_count'].notna().sum():,}")
        logger.debug(f"Giá trị null theo cột:\n{df[REQUIRED_COLS].isnull().sum()}")
        
        # Bước 5: Clean và Transform
        logger.info("=" * 60)
        logger.info("BƯỚC 5: CLEAN VÀ TRANSFORM DỮ LIỆU")
        logger.info("=" * 60)
        logger.info("Bắt đầu clean_dataframe...")
        
        df_clean = clean_dataframe(df)
        
        final_rows = len(df_clean)
        removed_rows = initial_rows - final_rows
        logger.info(f"Hoàn tất clean_dataframe")
        logger.info(f"Số dòng sau khi clean: {final_rows:,}")
        logger.info(f"Số dòng đã loại bỏ: {removed_rows:,} ({removed_rows/initial_rows*100:.2f}%)")
        
        # Bước 6: Thống kê sau khi clean
        logger.info("=" * 60)
        logger.info("BƯỚC 6: THỐNG KÊ DỮ LIỆU SAU KHI CLEAN")
        logger.info("=" * 60)
        if final_rows > 0:
            logger.info(f"Số brand duy nhất: {df_clean['brand'].nunique()}")
            logger.info(f"Số source duy nhất: {df_clean['source'].nunique()}")
            logger.info(f"Số product duy nhất: {df_clean['product_name'].nunique()}")
            logger.info(f"Giá trung bình: {df_clean['price'].mean():,.0f} VND")
            logger.info(f"Giá thấp nhất: {df_clean['price'].min():,} VND")
            logger.info(f"Giá cao nhất: {df_clean['price'].max():,} VND")
            logger.info(f"Số dòng có sold_count: {df_clean['sold_count'].notna().sum():,}")
            if df_clean['sold_count'].notna().sum() > 0:
                logger.info(f"Số lượng bán trung bình: {df_clean['sold_count'].mean():,.0f}")
            
            # Thống kê theo brand
            logger.debug("Thống kê theo brand:")
            brand_counts = df_clean['brand'].value_counts()
            for brand, count in brand_counts.head(10).items():
                logger.debug(f"  {brand}: {count} sản phẩm")
            
            # Thống kê theo source
            logger.debug("Thống kê theo source:")
            source_counts = df_clean['source'].value_counts()
            for source, count in source_counts.items():
                logger.debug(f"  {source}: {count} sản phẩm")
        
        # Bước 7: Lưu file output
        logger.info("=" * 60)
        logger.info("BƯỚC 7: LƯU FILE OUTPUT")
        logger.info("=" * 60)
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(OUTPUT_DIR, f"clean_laptop_{timestamp}.csv")
        
        df_clean.to_csv(output_path, index=False, encoding='utf-8-sig')
        file_size = os.path.getsize(output_path)
        logger.info(f"Đã lưu file: {output_path}")
        logger.info(f"Kích thước file: {file_size:,} bytes")
        
        logger.info("=" * 60)
        logger.info("TRANSFORM HOÀN TẤT THÀNH CÔNG!")
        logger.info("=" * 60)
        
        return df_clean
        
    except Exception as e:
        logger.error(f"Lỗi trong quá trình transform: {e}")
        logger.error(traceback.format_exc())
        raise

# ==================== MAIN FUNCTION WITH RETRY ====================
def main():
    """Hàm main với retry mechanism"""
    logger = setup_logging()
    
    logger.info("=" * 60)
    logger.info("TRANSFORM STANDALONE SCRIPT - BẮT ĐẦU")
    logger.info("=" * 60)
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Pandas version: {pd.__version__}")
    logger.info(f"Thời gian bắt đầu: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Max retries: {MAX_RETRIES}")
    
    # Xác định file CSV input
    if len(sys.argv) > 1:
        csv_path = sys.argv[1]
        logger.info(f"File CSV từ tham số dòng lệnh: {csv_path}")
    else:
        csv_path = find_latest_raw_file()
        if csv_path:
            logger.info(f"Tự động tìm file raw mới nhất: {csv_path}")
        else:
            logger.error("Không tìm thấy file raw CSV trong data_output/")
            logger.error("Cách sử dụng: python transform_standalone.py [đường_dẫn_file.csv]")
            sys.exit(1)
    
    # Validate file
    if not validate_csv_file(csv_path, logger):
        sys.exit(1)
    
    # Chạy transform với retry
    last_exception = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info("")
            logger.info("=" * 60)
            logger.info(f"LẦN THỬ {attempt}/{MAX_RETRIES}")
            logger.info("=" * 60)
            
            result = run_transform_once(csv_path, logger)
            
            if result is not None and len(result) > 0:
                logger.info("")
                logger.info("=" * 60)
                logger.info("THÀNH CÔNG!")
                logger.info("=" * 60)
                logger.info(f"Tổng số dòng đã transform: {len(result):,}")
                sys.exit(0)
            else:
                raise Exception("Transform trả về DataFrame rỗng hoặc None")
                
        except Exception as e:
            last_exception = e
            logger.error(f"Lỗi ở lần thử {attempt}/{MAX_RETRIES}: {e}")
            logger.debug(traceback.format_exc())
            
            if attempt < MAX_RETRIES:
                logger.info(f"Đợi {RETRY_DELAY} giây trước khi thử lại...")
                time.sleep(RETRY_DELAY)
            else:
                logger.error("=" * 60)
                logger.error("ĐÃ HẾT SỐ LẦN THỬ!")
                logger.error("=" * 60)
    
    # Nếu đến đây thì đã thất bại
    logger.error(f"Transform thất bại sau {MAX_RETRIES} lần thử")
    logger.error(f"Lỗi cuối cùng: {last_exception}")
    sys.exit(1)

if __name__ == "__main__":
    main()

