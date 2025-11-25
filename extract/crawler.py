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
        # Use the specific filtered page which contains all laptop products as requested
        # Default to the fragment page with pi=21 (matches user's working seed)
        "seed_categories": ["https://www.thegioididong.com/laptop#c=44&o=13&pi=21"],
        "sitemap": "https://www.thegioididong.com/newsitemap/sitemap-product"
    }
}

# Allow overriding the TGDD seed via environment variable EXTRACT_TGDD_SEED
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
            # sample some sitemap entries for debugging
            for s in locs[:20]:
                logging.info(f"[{site_key}] Sitemap sample: {s}")

            # Looser filter: accept locs under the site domain and avoid obvious non-product paths
            exclude_tokens = ["tin-tuc", "news", "khuyen-mai", "khuyenmai", "tag", "search", "tim-kiem", "khuyenmai"]
            for u in locs:
                lu = u.lower()
                if "thegioididong.com" not in lu:
                    continue
                if any(ex in lu for ex in exclude_tokens):
                    continue
                urls.add(u.split("?")[0])
                if len(urls) >= MAX_PER_SITE:
                    break

    # Crawl seed categories (with pagination for some sites)
    for seed in cfg["seed_categories"]:
        pages = [seed]
        # If the seed contains a fragment (client-side filtered page), only fetch that page once
        # Also, when running in test mode (EXTRACT_FORCE_FAIL) avoid generating many pagination pages
        if site_key == "thegioididong":
            force_fail = os.environ.get('EXTRACT_FORCE_FAIL', '').lower() in ('1', 'true', 'yes')
            if "#" in seed or force_fail:
                logging.info(f"[{site_key}] Using single fragment seed page: {seed}")
            else:
                # otherwise keep generating pagination variants as before
                for pnum in range(2, PAGINATION_LIMIT + 1):
                    pages.append(f"{seed}?p={pnum}")
                    pages.append(f"{seed}#c=44&o=13&pi={pnum}")
                logging.info(f"[{site_key}] Generated {len(pages)} category page URLs (including fragments)")

        for idx, page_url in enumerate(pages):
            logging.info(f"[{site_key}] Fetching category page {idx+1}/{len(pages)}: {page_url}")
            # If Playwright renderer is available, use it for TGDD to get client-rendered product lists
            if USE_RENDERER and site_key == "thegioididong":
                try:
                    html = render_html(page_url)
                    if not html:
                        logging.debug(f"Renderer returned no HTML for {page_url}, falling back to http_get")
                        r = http_get(page_url)
                        if not r:
                            logging.info(f"[{site_key}] Failed to fetch page (renderer+http) {page_url}")
                            continue
                        soup = BeautifulSoup(r.text, "lxml")
                    else:
                        soup = BeautifulSoup(html, "lxml")
                except Exception as e:
                    logging.warning(f"Renderer error for {page_url}: {e}")
                    # fallback to http_get
                    r = http_get(page_url)
                    if not r:
                        logging.info(f"[{site_key}] Failed to fetch page (renderer error + http) {page_url}")
                        continue
                    soup = BeautifulSoup(r.text, "lxml")
            else:
                r = http_get(page_url)
                if not r:
                    logging.info(f"[{site_key}] Failed to fetch page (http) {page_url}")
                    continue
                soup = BeautifulSoup(r.text, "lxml")
            anchors = soup.find_all("a", href=True)
            logging.info(f"[{site_key}] Page {idx+1} anchors found: {len(anchors)}")
            added_on_page = 0
            for a in anchors:
                full = urljoin(page_url, a["href"]).split("?")[0]
                if any(k in full.lower() for k in ["laptop", "macbook", "product", "collection"]):
                    if full not in urls:
                        urls.add(full)
                        added_on_page += 1
                if len(urls) >= MAX_PER_SITE:
                    break
            logging.info(f"[{site_key}] Added {added_on_page} candidate URLs from page {idx+1}")
            if len(urls) >= MAX_PER_SITE:
                break
        if len(urls) >= MAX_PER_SITE:
            break

    urls = list(urls)[:MAX_PER_SITE]
    logging.info(f"[{site_key}] Found {len(urls)} URLs")
    # Log a short sample to help debugging discovery quality
    sample = urls[:20]
    for u in sample:
        logging.info(f"[{site_key}] Sample URL: {u}")

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
