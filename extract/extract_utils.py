# extract_utils.py
import random, time, requests, logging, re
from datetime import datetime

REQUEST_TIMEOUT = 15
DELAY_BETWEEN_REQUESTS = 1.5

def random_headers():
    uas = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119 Safari/537.36"
    ]
    return {"User-Agent": random.choice(uas), "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8"}

def http_get(url):
    try:
        r = requests.get(url, headers=random_headers(), timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            time.sleep(DELAY_BETWEEN_REQUESTS + random.random() * 0.5)
            r.encoding = r.apparent_encoding
            return r
    except Exception as e:
        logging.warning(f"Lỗi GET {url}: {e}")
    return None

def parse_price(text):
    if not text: return None
    text = text.split('-')[0]
    match = re.search(r'[\d,.]+', text)
    if not match: return None

    num_str = match.group(0).replace(".", "").replace(",", "")
    try:
        val = int(num_str)
        if val > 100_000_000: val //= 10
        if val < 5_000_000: return None
        return val
    except:
        return None

def parse_sold_number(text):
    if not text: return None
    text = text.lower().replace("đã bán", "").strip()
    text = text.replace(",", ".").replace(" ", "")
    try:
        if "k" in text:
            return int(float(text.replace("k", "")) * 1000)
        return int(float(text))
    except:
        return None
