import pandas as pd
import re
import ast
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def normalize_brand(x):
    if isinstance(x, list) and x:
        return str(x[0]).strip().upper()
    if isinstance(x, str):
        x = x.strip()
        if x.startswith("[") and x.endswith("]"):
            try:
                lst = ast.literal_eval(x)
                if lst: return str(lst[0]).strip().upper()
            except: return "UNKNOWN"
        return x.upper() if x else "UNKNOWN"
    return "UNKNOWN"

def normalize_source(x):
    return str(x).strip().lower() if isinstance(x, str) and x.strip() else "unknown"

def normalize_product_name(x):
    return str(x).strip() if isinstance(x, str) and x.strip() else "UNKNOWN_PRODUCT"

def safe_price(x):
    if pd.isna(x) or str(x).strip() == "":
        return 0
    try:
        digits = re.sub(r"[^\d]", "", str(x))
        return int(digits) if digits else 0
    except:
        return 0

def clean_sold_count(x):
    if pd.isna(x) or x == "":
        return 0
    s = str(x).lower().replace(" ", "").replace(",", "").replace(".", "")
    try:
        if "k" in s:
            return int(float(s.replace("k",""))*1000)
        return int(float(s))
    except:
        return 0

REQUIRED_COLS = ['brand','product_name','price','currency','source','timestamp','sold_count']

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # Check các cột
    for col in REQUIRED_COLS:
        if col not in df.columns:
            raise ValueError(f"Thiếu cột bắt buộc: {col}")

    df["brand"] = df["brand"].apply(normalize_brand)
    df["product_name"] = df["product_name"].apply(normalize_product_name)
    df["price"] = df["price"].apply(safe_price)
    df["sold_count"] = df["sold_count"].apply(clean_sold_count)
    df["source"] = df["source"].apply(normalize_source)

    # timestamp → crawl_date / crawl_hour
    ts = pd.to_datetime(df["timestamp"], errors="coerce")
    now = datetime.now()
    df["crawl_date"] = ts.dt.date.astype(str).fillna(now.date().isoformat())
    df["crawl_hour"] = ts.dt.hour.fillna(now.hour).astype(int)

    return df.reset_index(drop=True)
