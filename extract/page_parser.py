# page_parser.py
import json
from bs4 import BeautifulSoup
from datetime import datetime
from extract.extract_utils import http_get, parse_price, parse_sold_number

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
            data = json.loads(s.string.strip())
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
    if not r: return None

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

    if not price or price <= 0: return None
    if not any(k in name.lower() for k in LAP_KEYWORDS): return None

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
