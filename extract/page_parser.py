# page_parser.py
import json
import logging
import warnings
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from datetime import datetime
from extract.extract_utils import http_get, parse_price, parse_sold_number
# Optional renderer for JS-rendered product pages
try:
    from extract.render_crawler import render_html
    PAGE_USE_RENDERER = True
except Exception:
    render_html = None
    PAGE_USE_RENDERER = False

BRAND_WHITELIST = {
    "dell", "asus", "acer", "hp", "lenovo", "msi", "apple", "macbook",
    "razer", "gigabyte", "huawei", "microsoft", "samsung", "xiaomi", "realme"
}

LAP_KEYWORDS = [
    "laptop", "macbook", "notebook", "thinkpad", "vivobook",
    "tuf", "rog", "legion", "pavilion", "zenbook", "ideapad"
]

def extract_jsonld(soup):
    for s in soup.find_all("script", type="application/ld+json"):
        try:
            txt = s.string or ""
            data = json.loads(txt.strip())
            if isinstance(data, dict) and data.get("@type") == "Product":
                return data
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and item.get("@type") == "Product":
                        return item
        except:
            pass
    return None

def parse_product_page(url, source):
    r = http_get(url)
    if not r:
        return None

    # Prefer lxml parser (more robust for mixed/XML-like pages); fall back to html.parser.
    # Also silence the XMLParsedAsHTMLWarning which can be noisy for some responses.
    warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
    try:
        soup = BeautifulSoup(r.text, "lxml")
    except Exception:
        soup = BeautifulSoup(r.text, "html.parser")
    jd = extract_jsonld(soup)

    brand, name, price, currency = "", "", None, "VND"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sold_count = None

    if jd:
        name = jd.get("name") or jd.get("headline") or ""
        b = jd.get("brand")
        if isinstance(b, dict): brand = b.get("name", "")
        elif isinstance(b, str): brand = b
        
        offers = jd.get("offers") or {}
        if isinstance(offers, dict):
            price = parse_price(str(offers.get("price") or offers.get("priceSpecification", {}).get("price")))

    if not name:
        title = soup.find("h1") or soup.find("title")
        name = title.get_text(strip=True) if title else ""

    if not price:
        price_el = soup.select_one(".price, .product-price, .price-value, .gia, .box-price-present")
        if price_el:
            price = parse_price(price_el.get_text(strip=True))

    # If price/JSON-LD missing and Playwright renderer is available, try rendering the product page
    if (not price or not jd) and PAGE_USE_RENDERER and "thegioididong" in url:
        try:
            html2 = render_html(url)
            if html2:
                soup2 = BeautifulSoup(html2, "html.parser")
                if not jd:
                    jd = extract_jsonld(soup2)
                    if jd and not price:
                        offers = jd.get("offers") or {}
                        if isinstance(offers, dict):
                            price = parse_price(str(offers.get("price") or offers.get("priceSpecification", {}).get("price")))
                if not price:
                    price_el2 = soup2.select_one(".price, .product-price, .price-value, .gia, .box-price-present")
                    if price_el2:
                        price = parse_price(price_el2.get_text(strip=True))
        except Exception:
            pass

    if not brand:
        for b in BRAND_WHITELIST:
            if b.lower() in name.lower():
                brand = b.upper()
                break

    # parse sold number
    if "thegioididong" in url:
        el = soup.find("span", class_="quantity-sale")
        if el: sold_count = parse_sold_number(el.get_text(strip=True))
    elif "gearvn" in url:
        el = soup.select_one(".product-quantity-sold, .productView-soldCount")
        if el: sold_count = parse_sold_number(el.get_text(strip=True))

    if not price or price <= 0:
        logging.info(f"DROP no_price: {url}")
        return None

    # If we have JSON-LD Product data, accept it even if name lacks keyword
    if jd:
        # jd exists and earlier we extracted name/brand/price from it, so accept
        pass
    else:
        # allow if URL pattern clearly indicates a laptop product (TGDD)
        has_kw = any(k in name.lower() for k in LAP_KEYWORDS)
        sure_product = ("/laptop-" in url) or ("/may-tinh-xach-tay-" in url) or ("/laptop/" in url)
        if not has_kw and not sure_product:
            logging.info(f"DROP not_laptop_like: {name} ({url})")
            return None

    return {
        "brand": brand,
        "product_name": name.strip(),
        "price": price,
        "currency": currency,
        "source": source,
        "url": url.strip(),
        "timestamp": timestamp,
        "sold_count": sold_count
    }
