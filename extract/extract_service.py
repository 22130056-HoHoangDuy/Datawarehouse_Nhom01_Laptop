# extract_service.py
import os
import logging
import pandas as pd
from datetime import datetime
from extract.sitemap_utils import fetch_sitemap_urls
from extract.page_parser import parse_product_page

LOG_DIR = "logs"
OUTPUT_DIR = "data_output"
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

logger = logging.getLogger("extract")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(os.path.join(LOG_DIR, f"extract_{datetime.now().strftime('%Y%m%d')}.log"), encoding="utf-8")
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)


# ==============================
#      URL FILTER (CHUẨN)
# ==============================
PRODUCT_KEYWORDS = [
    "/laptop/",          # link sản phẩm chính
]

BLOCK_KEYWORDS = [
    "/hoi-dap/",
    "/tin-tuc/",
    "/khuyen-mai/",
    "/tra-gop/",
    "/#",
    "?",
]

def is_valid_product_url(url: str) -> bool:
    """Chỉ lấy link sản phẩm thật"""
    if not isinstance(url, str):
        return False

    url = url.lower()

    # Loại bỏ query / anchor
    if "?" in url or "#" in url:
        return False

    # Không lấy các page hỏi đáp / tin tức
    for bad in BLOCK_KEYWORDS:
        if bad in url:
            return False

    # Chỉ chấp nhận link dạng /laptop/xxx
    return url.startswith("https://www.thegioididong.com/laptop/")


# ==============================
#      CRAWL WEBSITE
# ==============================
def extract_thegioididong():
    logger.info("Đang crawl nguồn: thegioidididong")

    urls = fetch_sitemap_urls("https://www.thegioididong.com/", logger)

    # Lọc chính xác URL sản phẩm laptop
    valid_urls = [u for u in urls if is_valid_product_url(u)]
    logger.info(f"[thegioidididong] Tổng URL hợp lệ: {len(valid_urls)}")

    data = []
    for url in valid_urls:
        item = parse_product_page(url, "thegioididong")
        if item:
            data.append(item)

    return data


# ==============================
#      RUN EXTRACT
# ==============================
def run_extract(return_output_path=False):
    logger.info("=== BẮT ĐẦU QUÁ TRÌNH EXTRACT ===")

    try:
        all_rows = extract_thegioididong()

        if not all_rows:
            logger.error("KHÔNG có dữ liệu hợp lệ sau khi crawl!")
            return None

        df = pd.DataFrame(all_rows)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = os.path.join(OUTPUT_DIR, f"raw_laptop_{ts}.csv")
        df.to_csv(out_path, index=False, encoding="utf-8-sig")

        logger.info(f"Hoàn tất EXTRACT → {out_path}")

        if return_output_path:
            return df, out_path
        return df

    except Exception as exc:
        logger.exception("Extract bị lỗi: %s", exc)
        return None
