# page_parser.py
import json
import logging
import warnings
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from datetime import datetime
from urllib.parse import urlparse
from extract.extract_utils import http_get, parse_price, parse_sold_number

# OPTIONAL renderer
try:
    from extract.render_crawler import render_html
    PAGE_USE_RENDERER = True
except:
    render_html = None
    PAGE_USE_RENDERER = False

# ===== WHITELIST BRAND =====
BRAND_WHITELIST = {
    "dell", "asus", "acer", "hp", "lenovo", "msi", "apple", "macbook",
    "razer", "gigabyte", "huawei", "microsoft", "samsung", "xiaomi", "realme"
}

# ===== KEYWORDS MUST CONTAIN =====
VALID_PATTERNS = [
    "/laptop/",
    "/laptop-",
    "may-tinh-xach-tay",
    "/macbook",
]

# ===== URL NEED TO BE BLOCKED COMPLETELY =====
BLOCKED_PATTERNS = [
    "/hoi-dap/",
    "/tin-tuc/",
    "/bai-viet/",
    "/tu-van/",
    "/kinh-nghiem/",
    "/tra-gop/",
    "/khuyen-mai/",
    "/search",
]


def is_valid_product_url(url: str) -> bool:
    """Chỉ chấp nhận URL của product laptop, loại bỏ bài viết."""
    low = url.lower()

    # loại domain khác TGDD
    if "thegioididong.com" not in low:
        return False

    # loại bài viết
    if any(p in low for p in BLOCKED_PATTERNS):
        return False

    # phải chứa từ khóa laptop/product
    return any(p in low for p in VALID_PATTERNS)


def extract_jsonld(soup):
    for s in soup.find_all("script", type="application/ld+json"):
        try:
            txt = (s.string or "").strip()
            data = json.loads(txt)
            if isinstance(data, dict) and data.get("@type") == "Product":
                return data
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and item.get("@type") == "Product":
                        return item
        except:
            pass
    return None


def normalize_brand_value(b):
    if isinstance(b, dict):
        return (b.get("name") or "").strip().upper()
    if isinstance(b, list):
        return str(b[0]).strip().upper() if b else ""
    if isinstance(b, str):
        return b.strip().upper()
    return ""


def parse_product_page(url, source):
    # Bộ lọc URL ngay TỪ ĐẦU → tránh phí crawler
    if not is_valid_product_url(url):
        logging.info(f"DROP invalid_url: {url}")
        return None

    r = http_get(url)
    if not r:
        return None

    warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

    try:
        soup = BeautifulSoup(r.text, "lxml")
    except:
        soup = BeautifulSoup(r.text, "html.parser")

    jd = extract_jsonld(soup)

    brand, name, price = "", "", None
    currency = "VND"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sold_count = None

    # === JSON-LD ===
    if jd:
        brand = normalize_brand_value(jd.get("brand"))
        name = jd.get("name") or jd.get("headline") or ""

        offers = jd.get("offers") or {}
        if isinstance(offers, dict):
            price = parse_price(str(offers.get("price") or offers.get("priceSpecification", {}).get("price")))

    # === fallback title ===
    if not name:
        title = soup.find("h1") or soup.find("title")
        name = title.get_text(strip=True) if title else ""

    # === PRICE extraction fallback ===
    if not price:
        price_el = soup.select_one(".price, .product-price, .gia, .box-price-present")
        if price_el:
            price = parse_price(price_el.get_text(strip=True))

    # META tag
    if not price:
        m = soup.find("meta", property="product:price:amount") or soup.find("meta", attrs={"itemprop": "price"})
        if m and m.get("content"):
            price = parse_price(m["content"])

    # data-price
    if not price:
        el = soup.find(attrs={"data-price": True})
        if el:
            price = parse_price(el.get("data-price"))

    # script price
    if not price:
        import re
        txt = "\n".join([s.string or "" for s in soup.find_all("script")])
        m = re.search(r'"price"\s*:\s*"([\d\.,]+)"', txt)
        if m:
            price = parse_price(m.group(1))

    # RENDERER fallback
    if (not price or not brand) and PAGE_USE_RENDERER and "thegioididong" in url:
        try:
            html2 = render_html(url)
            if html2:
                soup2 = BeautifulSoup(html2, "html.parser")
                jd2 = extract_jsonld(soup2)
                if jd2:
                    brand = normalize_brand_value(jd2.get("brand"))
                    name = jd2.get("name", name)
                    offers = jd2.get("offers") or {}
                    if isinstance(offers, dict):
                        price = parse_price(str(offers.get("price")))
        except:
            pass

    # detect brand in name
    if not brand:
        for b in BRAND_WHITELIST:
            if b in name.lower():
                brand = b.upper()
                break

    # SOLD COUNT (TGDD)
    el = soup.find("span", class_="quantity-sale")
    if el:
        sold_count = parse_sold_number(el.get_text(strip=True))

    # LOẠI SẢN PHẨM KHÔNG CÓ GIÁ
    if not price or price <= 0:
        logging.info(f"DROP no_price: {url}")
        return None

    return {
        "brand": brand,
        "product_name": name.strip(),
        "price": price,
        "currency": currency,
        "source": source,
        "url": url.strip(),
        "timestamp": timestamp,
        "sold_count": sold_count,
    }
