# crawler.py
import logging
import os
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from extract.extract_utils import http_get
from extract.page_parser import parse_product_page

# Optional Playwright-based renderer (faster for JS-heavy pages)
try:
    from extract.render_crawler import render_html
    USE_RENDERER = True
except Exception:
    render_html = None
    USE_RENDERER = False

MAX_PER_SITE = 1000
MAX_WORKERS = 12
PAGINATION_LIMIT = 60

SITES = {
    "thegioididong": {
        "seed_categories": ["https://www.thegioididong.com/laptop#c=44&o=13&pi=21"],
        "sitemap": "https://www.thegioididong.com/newsitemap/sitemap-product"
    }
}

# Allow override via environment
if os.environ.get('EXTRACT_TGDD_SEED'):
    override = os.environ.get('EXTRACT_TGDD_SEED')
    logging.info(f"Overriding thegioididong seed with EXTRACT_TGDD_SEED={override}")
    SITES['thegioididong']['seed_categories'] = [override]


def harvest_site(site_key):
    cfg = SITES[site_key]
    urls = set()

    # Crawl sitemap nếu có
    if cfg.get("sitemap"):
        r = http_get(cfg["sitemap"])
        if r:
            soup = BeautifulSoup(r.text, "xml")
            locs = [loc.get_text().strip() for loc in soup.find_all("loc")]

            logging.info(f"[{site_key}] Sitemap contains {len(locs)} loc entries")

            exclude_tokens = ["tin-tuc", "news", "khuyen", "search", "tag"]

            for u in locs:
                lu = u.lower()
                if "thegioididong.com" not in lu:
                    continue
                if any(ex in lu for ex in exclude_tokens):
                    continue
                urls.add(u.split("?")[0])
                if len(urls) >= MAX_PER_SITE:
                    break

    # Crawl seed page (fallback)
    for seed in cfg["seed_categories"]:
        pages = [seed]

        if site_key == "thegioididong":
            if "#" in seed:
                logging.info(f"[{site_key}] Using single fragment seed page: {seed}")
            else:
                for pnum in range(2, PAGINATION_LIMIT + 1):
                    pages.append(f"{seed}?p={pnum}")
                    pages.append(f"{seed}#c=44&o=13&pi={pnum}")

        for idx, page_url in enumerate(pages):
            logging.info(f"[{site_key}] Fetching category page {idx+1}/{len(pages)}: {page_url}")

            # Try renderer first
            soup = None
            if USE_RENDERER:
                try:
                    html = render_html(page_url)
                    if html:
                        soup = BeautifulSoup(html, "lxml")
                except Exception:
                    pass

            if soup is None:
                r = http_get(page_url)
                if not r:
                    continue
                soup = BeautifulSoup(r.text, "lxml")

            anchors = soup.find_all("a", href=True)

            added = 0
            for a in anchors:
                full = urljoin(page_url, a["href"]).split("?")[0]
                if any(k in full.lower() for k in ["laptop", "macbook", "product"]):
                    if full not in urls:
                        urls.add(full)
                        added += 1
                if len(urls) >= MAX_PER_SITE:
                    break

            logging.info(f"[{site_key}] Added {added} URLs from page {idx+1}")

            if len(urls) >= MAX_PER_SITE:
                break
        if len(urls) >= MAX_PER_SITE:
            break

    urls = list(urls)[:MAX_PER_SITE]
    logging.info(f"[{site_key}] Found total {len(urls)} URLs")
    for u in urls[:20]:
        logging.info(f"[{site_key}] Sample: {u}")

    # Parse
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {
            ex.submit(parse_product_page, u, site_key): u for u in urls
        }
        for fut in tqdm(as_completed(futures), total=len(futures), desc=f"Crawling {site_key}"):
            try:
                item = fut.result()
                if item:
                    results.append(item)
            except Exception as e:
                logging.warning(f"Parse error: {e}")

    return results
