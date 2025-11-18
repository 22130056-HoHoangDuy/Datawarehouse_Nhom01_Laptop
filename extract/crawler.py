# crawler.py
import logging
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from extract.extract_utils import http_get
from extract.page_parser import parse_product_page

MAX_PER_SITE = 120
MAX_WORKERS = 5

SITES = {
    "cellphones": {
        "seed_categories": ["https://cellphones.com.vn/laptop/"],
        "sitemap": "https://cellphones.com.vn/sitemap/sitemap_index.xml"
    },
    "thegioididong": {
        "seed_categories": ["https://www.thegioididong.com/laptop"],
        "sitemap": "https://www.thegioididong.com/newsitemap/sitemap-product"
    },
    "gearvn": {
        "seed_categories": [
            "https://gearvn.com/collections/laptop",
            "https://gearvn.com/collections/laptop-gaming"
        ],
        "sitemap": None
    }
}

def harvest_site(site_key):
    cfg = SITES[site_key]
    urls = set()

    # Crawl sitemap nếu có
    if cfg.get("sitemap"):
        r = http_get(cfg["sitemap"])
        if r:
            soup = BeautifulSoup(r.text, "xml")
            for loc in soup.find_all("loc"):
                u = loc.get_text().strip()
                if any(k in u.lower() for k in ["laptop", "macbook", "product"]):
                    urls.add(u.split("?")[0])
                if len(urls) >= MAX_PER_SITE: break

    # Crawl seed categories
    for seed in cfg["seed_categories"]:
        r = http_get(seed)
        if not r: continue
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.find_all("a", href=True):
            full = urljoin(seed, a["href"]).split("?")[0]
            if any(k in full.lower() for k in ["laptop", "macbook", "product", "collection"]):
                urls.add(full)
            if len(urls) >= MAX_PER_SITE: break
        if len(urls) >= MAX_PER_SITE: break

    urls = list(urls)[:MAX_PER_SITE]
    logging.info(f"[{site_key}] Found {len(urls)} URLs")

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(parse_product_page, u, site_key): u for u in urls}
        for fut in tqdm(as_completed(futures), total=len(futures), desc=f"Crawling {site_key}"):
            try:
                item = fut.result()
                if item: results.append(item)
            except Exception as e:
                logging.warning(f"Parse error: {e}")

    return results
