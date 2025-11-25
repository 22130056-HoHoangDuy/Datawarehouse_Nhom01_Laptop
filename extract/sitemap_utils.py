import logging
from bs4 import BeautifulSoup
from extract.extract_utils import http_get

def fetch_sitemap_urls(sitemap_url: str, limit: int = 1000) -> list:
    """
    Đọc sitemap → trả về danh sách URL sản phẩm hợp lệ.
    """
    urls = []
    r = http_get(sitemap_url)
    if not r:
        logging.error(f"[SITEMAP] Không tải được sitemap: {sitemap_url}")
        return urls

    try:
        soup = BeautifulSoup(r.text, "xml")
        locs = [loc.get_text().strip() for loc in soup.find_all("loc")]
        logging.info(f"[SITEMAP] Sitemap có {len(locs)} mục")

        exclude_tokens = ["tin-tuc", "news", "khuyen-mai", "search", "tag"]

        for u in locs:
            lu = u.lower()
            if any(ex in lu for ex in exclude_tokens):
                continue
            urls.append(u.split("?")[0])
            if len(urls) >= limit:
                break

    except Exception as e:
        logging.error(f"[SITEMAP] Parse lỗi: {e}")

    return urls
