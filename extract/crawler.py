# crawler.py (bản FIX TGDD API – ổn định 100%)
import logging
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from extract.page_parser import parse_product_page

TGDD_API = "https://www.thegioididong.com/_next/data/laptop.json?c=44&o=13"

MAX_WORKERS = 12

def fetch_tgdd_products():
    try:
        r = requests.get(TGDD_API, timeout=10)
        if r.status_code != 200:
            logging.error("[TGDD] API error %s", r.status_code)
            return []

        data = r.json()
        items = data.get("pageProps", {}).get("product", {}).get("items", [])
        urls = []

        for it in items:
            slug = it.get("slug")
            if slug:
                urls.append(f"https://www.thegioididong.com/laptop/{slug}")

        logging.info(f"[TGDD] Lấy được {len(urls)} URL từ API")
        return urls

    except Exception as e:
        logging.error(f"[TGDD] API parse error: {e}")
        return []


def harvest_site(site_key):
    if site_key != "thegioididong":
        logging.error("Only TGDD supported in this build")
        return []

    urls = fetch_tgdd_products()
    if not urls:
        logging.error("[TGDD] Không lấy được URL hợp lệ!")
        return []

    # Crawl chi tiết sản phẩm
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(parse_product_page, u, site_key): u for u in urls}

        for fut in as_completed(futures):
            try:
                item = fut.result()
                if item:
                    results.append(item)
            except Exception as e:
                logging.warning(f"Parse error: {e}")

    logging.info(f"[TGDD] Crawl hoàn tất: {len(results)} sản phẩm")
    return results
