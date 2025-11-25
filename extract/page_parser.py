# page_parser.py
import json
import logging
import warnings
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from datetime import datetime
from extract.extract_utils import http_get, parse_price, parse_sold_number

try:
    from extract.render_crawler import render_html
    PAGE_USE_RENDERER = True
except:
    render_html = None
    PAGE_USE_RENDERER = False

BRAND_WHITELIST = {
    "dell","asus","acer","hp","lenovo","msi","apple","macbook",
    "razer","gigabyte","huawei","microsoft","samsung","xiaomi","realme"
}

def extract_jsonld(soup):
    for s in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads((s.string or "").strip())
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
    """Fix brand dạng list hoặc dict"""
    if isinstance(b, dict):
        return (b.get("name") or "").upper()
    if isinstance(b, list):
        try:
            return str(b[0]).upper()
        except:
            return ""
    if isinstance(b, str):
        return b.upper()
    return ""

def parse_product_page(url, source):
    r = http_get(url)
    if not r:
        return None

    warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

    try:
        soup = BeautifulSoup(r.text, "lxml")
    except:
        soup = BeautifulSoup(r.text, "html.parser")

    jd = extract_jsonld(soup)

    brand = ""
    name = ""
    price = None
    currency = "VND"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sold_count = None

    # ===== JSON-LD =====
    if jd:
        brand = normalize_brand_value(jd.get("brand"))
        name = jd.get("name") or jd.get("headline") or ""

        offers = jd.get("offers")
        if isinstance(offers, dict):
            raw_price = offers.get("price") or offers.get("priceSpecification", {}).get("price")
            price = parse_price(str(raw_price))

    # ===== Fallback name =====
    if not name:
        title = soup.find("h1") or soup.find("title")
        if title:
            name = title.get_text(strip=True)

    # ===== Price fallback =====
    if not price:
        price_el = soup.select_one(".box-price-present, .price, .product-price")
        if price_el:
            price = parse_price(price_el.get_text(strip=True))

    if not price:
        import re
        scripts = soup.find_all("script")
        txt = "\n".join([s.string or "" for s in scripts])
        m = re.search(r'"price"\s*:\s*"([\d\.,]+)"', txt)
        if m:
            price = parse_price(m.group(1))

    # ===== Renderer fallback =====
    if (not price or not brand) and PAGE_USE_RENDERER and "thegioididong" in url:
        try:
            html2 = render_html(url)
            if html2:
                soup2 = BeautifulSoup(html2, "html.parser")
                jd2 = extract_jsonld(soup2)
                if jd2:
                    jd = jd2
                    brand = normalize_brand_value(jd2.get("brand"))
                if not price:
                    el2 = soup2.select_one(".box-price-present")
                    if el2:
                        price = parse_price(el2.get_text(strip=True))
        except:
            pass

    # ===== Brand từ tên =====
    if not brand:
        lower = name.lower()
        for b in BRAND_WHITELIST:
            if b in lower:
                brand = b.upper()
                break

    # ===== Sold =====
    el = soup.find("span", class_="quantity-sale")
    if el:
        sold_count = parse_sold_number(el.get_text(strip=True))

    # ===== DROP invalid =====
    if not price or price <= 0:
        logging.info(f"DROP no_price: {url}")
        return None

    if len(name.strip()) == 0:
        logging.info(f"DROP noname: {url}")
        return None

    return {
        "brand": brand,
        "product_name": name.strip(),
        "price": price,
        "currency": currency,
        "source": source,
        "url": url,
        "timestamp": timestamp,
        "sold_count": sold_count,
    }
