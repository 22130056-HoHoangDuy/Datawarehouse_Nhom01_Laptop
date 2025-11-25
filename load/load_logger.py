# load_logger.py
# FLOW 3.0.1 – Cấu hình logger chính cho toàn bộ module LOAD
# Tất cả log sẽ ghi vào logs/load.log

import logging
import os
from logging.handlers import RotatingFileHandler

# FLOW 3.0.2: đảm bảo thư mục logs tồn tại
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "load.log")

# FLOW 3.0.3: tạo logger riêng cho module LOAD
logger = logging.getLogger("LOAD")
logger.setLevel(logging.DEBUG)
logger.propagate = False

# FLOW 3.0.4: định dạng log đẹp
formatter = logging.Formatter(
    "%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# FLOW 3.0.5: handler ghi file (xoay vòng 5MB)
file_handler = RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=5, encoding="utf-8")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

# FLOW 3.0.6: handler in ra console (chỉ từ INFO trở lên)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

# FLOW 3.0.7: tránh thêm handler nhiều lần khi reload module
if not logger.handlers:
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

# FLOW 3.0.8: log khởi động
logger.debug("Logger module LOAD đã được khởi tạo thành công – log file: logs/load.log")