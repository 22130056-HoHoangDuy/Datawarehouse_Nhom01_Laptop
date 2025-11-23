# =========================================
# File: extract/crawler.py
# =========================================
from __future__ import annotations

import logging
import re
from typing import Dict, Iterable, List, Set

from bs4 import BeautifulSoup

from extract.extract_utils import http_get
from extract.page_parser import parse_product_page

# Chỉ crawl Thế Giới Di Động
SITES: Dict[str, dict] = {
    "thegioididong": {
        "base": "https://www.thegioididong.com",
        "category": "https://www.thegioididong.com/laptop",
        # Có thể thêm sort/filter tại đây nếu bạn muốn (ví dụ o=17 là sort theo bán chạy)
        "params": {"o": "17"},  # tuỳ chọn
    }
}


def harvest_site(site_key: str) -> List[dict]:
    if site_key not in SITES:
        raise ValueError(f"Unknown site: {site_key}")
    if site_key == "thegioididong":
        return harvest_tgdd(SITES[site_key])
    raise ValueError(f"No harvester implemented for: {site_key}")


def _build_list_url(base_url: str, page: int, extra_params: Dict[str, str] | None) -> str:
    # TGDD dùng tham số ?pi={page} cho pagination; giữ nguyên các params khác nếu có.
    q = "&".join([*(f"{k}={v}" for k, v in (extra_params or {}).items()), f"pi={page}"])
    if "?" in base_url:
        return f"{base_url}&{q}"
    return f"{base_url}?{q}"


def _extract_product_urls_from_list(html: str, site_base: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")

    # Cách 1: danh sách cổ điển .listproduct > li > a
    urls: List[str] = []
    for a in soup.select(".listproduct a, ul.listproduct a"):
        href = a.get("href")
        if not href:
            continue
        # TGDD hay trả href dạng "/laptop/some-slug"
        if href.startswith("/"):
            href = site_base.rstrip("/") + href
        if "/laptop/" in href:
            urls.append(href)

    # Cách 2: phòng trường hợp layout khác, tìm qua data-link / data-href
    if not urls:
        for el in soup.select("[data-href], [data-link]"):
            href = el.get("data-href") or el.get("data-link")
            if href and "/laptop/" in href:
                if href.startswith("/"):
                    href = site_base.rstrip("/") + href
                urls.append(href)

    # Loại bỏ query & fragment rác
    clean: List[str] = []
    for u in urls:
        u = re.sub(r"#.*$", "", u)
        u = re.sub(r"\?.*$", "", u)
        if u not in clean:
            clean.append(u)
    return clean


def harvest_tgdd(conf: dict, max_pages: int = 100, max_products: int = 1000) -> List[dict]:
    base = conf["base"]
    cat = conf["category"]
    params = conf.get("params", {})

    seen: Set[str] = set()
    product_urls: List[str] = []

    for page in range(1, max_pages + 1):
        list_url = _build_list_url(cat, page, params)
        r = http_get(list_url)
        if not r:
            logging.warning("Bỏ qua trang #%s (lỗi GET): %s", page, list_url)
            break

        urls = _extract_product_urls_from_list(r.text, base)
        # Lọc URL mới
        new_urls = [u for u in urls if u not in seen]
        if not new_urls:
            logging.info("Trang #%s không có URL mới → dừng phân trang.", page)
            break

        for u in new_urls:
            if u not in seen:
                seen.add(u)
                product_urls.append(u)

        logging.info("Trang #%s: thu được %d URL (tổng %d)", page, len(new_urls), len(product_urls))

        if len(product_urls) >= max_products:
            logging.info("Đạt ngưỡng an toàn max_products=%d → dừng.", max_products)
            break

    # Crawl chi tiết sản phẩm
    results: List[dict] = []
    for idx, url in enumerate(product_urls, 1):
        item = parse_product_page(url, "thegioididong")
        if item:
            results.append(item)
        else:
            logging.debug("Bỏ qua (parse None): %s", url)

        if idx % 20 == 0:
            logging.info("Đã parse %d/%d sản phẩm TGDD...", idx, len(product_urls))

    logging.info("TGDD hoàn tất: %d/%d sản phẩm hợp lệ.", len(results), len(product_urls))
    return results


# =========================================
# File: extract/extract_service.py  (giữ nguyên logic, chỉ crawl TGDD vì SITES chỉ còn 1)
# =========================================
from __future__ import annotations

import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Optional

import pandas as pd

from extract.crawler import SITES, harvest_site

OUT_DIR = "data_output"
LOG_DIR = "logs"
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)


def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("extract")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    log_path = os.path.join(LOG_DIR, f"extract_{datetime.now().strftime('%Y%m%d')}.log")
    fh = RotatingFileHandler(log_path, maxBytes=2_000_000, backupCount=5, encoding="utf-8")
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    fh.setFormatter(fmt)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


logger = _setup_logger()


def _timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def run_extract() -> Optional[str]:
    all_data = []
    logger.info("=== BẮT ĐẦU QUÁ TRÌNH EXTRACT (TGDD ONLY) ===")

    for site in SITES.keys():
        logger.info("Đang crawl nguồn: %s", site)
        try:
            items = harvest_site(site)
            if not items:
                logger.warning("Không có dữ liệu từ nguồn: %s", site)
                continue
            all_data.extend(items)
            logger.info("-> Thu được %d bản ghi từ %s", len(items), site)
        except Exception as e:
            logger.exception("Lỗi crawl %s: %s", site, e)

    if not all_data:
        logger.error("KHÔNG lấy được dữ liệu!!!")
        return None

    df = pd.DataFrame(all_data).drop_duplicates(subset=["url"]).reset_index(drop=True)
    preferred_cols = [
        "brand",
        "product_name",
        "price",
        "currency",
        "source",
        "url",
        "timestamp",
        "sold_count",
    ]
    cols = [c for c in preferred_cols if c in df.columns] + [c for c in df.columns if c not in preferred_cols]
    df = df[cols]

    csv_path = os.path.join(OUT_DIR, f"raw_laptop_{_timestamp()}.csv")
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    logger.info("Hoàn tất EXTRACT, tổng %d sản phẩm → %s", len(df), csv_path)
    return csv_path


if __name__ == "__main__":
    run_extract()
