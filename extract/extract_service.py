# extract_service.py
import os, logging
import pandas as pd
from datetime import datetime
from extract.crawler import harvest_site, SITES

OUT_DIR = "data_output"
LOG_DIR = "logs"
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(LOG_DIR, f"extract_{datetime.now().strftime('%Y%m%d')}.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8"
)

def run_extract():
    all_data = []
    logging.info("=== BẮT ĐẦU QUÁ TRÌNH EXTRACT ===")

    for site in SITES.keys():
        logging.info(f"Đang crawl nguồn: {site}")
        try:
            items = harvest_site(site)
            all_data.extend(items)
        except Exception as e:
            logging.error(f"Lỗi crawl {site}: {e}")

    if not all_data:
        logging.error("KHÔNG lấy được dữ liệu!!!")
        return None

    df = pd.DataFrame(all_data).drop_duplicates(subset=["url"]).reset_index(drop=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    csv_path = os.path.join(OUT_DIR, f"raw_laptop_{timestamp}.csv")
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    logging.info(f"Hoàn tất EXTRACT, tổng {len(df)} sản phẩm → {csv_path}")
    return csv_path
