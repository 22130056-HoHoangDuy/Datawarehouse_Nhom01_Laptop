# crawler.py
import logging
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
        "seed_categories": ["https://www.thegioididong.com/laptop"],
        "sitemap": "https://www.thegioididong.com/newsitemap/sitemap-product"
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
        if site_key == "thegioididong":
            # TGDD uses server-side '?p=' in some routes, but also many category filters are client-side
            # and rely on fragment/hash params like '#c=44&o=13&pi=21'. Generate both variants so
            # the renderer can apply client-side filters and load full product lists.
            for pnum in range(2, PAGINATION_LIMIT + 1):
                pages.append(f"{seed}?p={pnum}")
                # fragment-style pagination/filtering (client-side)
                pages.append(f"{seed}#c=44&o=13&pi={pnum}")

            logging.info(f"[{site_key}] Generated {len(pages)} category page URLs (including fragments)")

        for page_url in pages:
            # If Playwright renderer is available, use it for TGDD to get client-rendered product lists
            if USE_RENDERER and site_key == "thegioididong":
                try:
                    html = render_html(page_url)
                    if not html:
                        logging.debug(f"Renderer returned no HTML for {page_url}, falling back to http_get")
                        r = http_get(page_url)
                        if not r:
                            continue
                        soup = BeautifulSoup(r.text, "lxml")
                    else:
                        soup = BeautifulSoup(html, "lxml")
                except Exception as e:
                    logging.warning(f"Renderer error for {page_url}: {e}")
                    # fallback to http_get
                    r = http_get(page_url)
                    if not r:
                        continue
                    soup = BeautifulSoup(r.text, "lxml")
            else:
                r = http_get(page_url)
                if not r:
                    continue
                soup = BeautifulSoup(r.text, "lxml")

            for a in soup.find_all("a", href=True):
                full = urljoin(page_url, a["href"]).split("?")[0]
                if any(k in full.lower() for k in ["laptop", "macbook", "product", "collection"]):
                    urls.add(full)
                if len(urls) >= MAX_PER_SITE:
                    break
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
