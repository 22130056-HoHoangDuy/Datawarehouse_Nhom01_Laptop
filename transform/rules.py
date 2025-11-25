# rules.py
import pandas as pd

def normalize_brand(x: str):
    if not isinstance(x, str):
        return "UNKNOWN"
    return x.strip().upper()

def normalize_source(x: str):
    if not isinstance(x, str):
        return "unknown"
    return x.strip().lower()

def normalize_product_name(x: str):
    if not isinstance(x, str):
        return ""
    return x.strip()

def clean_sold_count(val):
    """
    Chuẩn hóa sold_count giống ETL gốc.
    Ví dụ: "1.5k" → 1500, "2,000" → 2000
    """
    if pd.isna(val):
        return None

    s = str(val).lower().replace(" ", "").replace(",", "").replace(".", "")
    try:
        if "k" in s:
            return int(float(s.replace("k", "")) * 1000)
        return int(float(s))
    except:
        return None

def safe_price(x):
    try:
        s = str(x).strip().replace("₫","").replace(".","").replace(",","")
        v = int(s)
        return v if v > 0 else None
    except:
        return None

