# clean_transform.py
import pandas as pd
from datetime import datetime
from transform.rules import (
    normalize_brand,
    normalize_source,
    normalize_product_name,
    clean_sold_count,
    safe_price
)

REQUIRED_COLS = ['brand', 'product_name', 'price', 'currency', 'source', 'timestamp', 'sold_count']

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # Kiểm tra cột bắt buộc
    for col in REQUIRED_COLS:
        if col not in df.columns:
            raise ValueError(f"Thiếu cột bắt buộc: {col}")

    # Chuẩn hóa từng trường
    df["brand"] = df["brand"].apply(normalize_brand)
    df["source"] = df["source"].apply(normalize_source)
    df["product_name"] = df["product_name"].apply(normalize_product_name)
    df["price"] = df["price"].apply(safe_price)
    df["sold_count"] = df["sold_count"].apply(clean_sold_count)

    # Loại giá lỗi hoặc NaN
    df = df[df["price"].notna()].copy()

    # Chuẩn hóa timestamp → tách ngày + giờ
    ts_series = pd.to_datetime(df["timestamp"], errors="coerce")
    now = datetime.now()
    df["crawl_date"] = ts_series.dt.date.astype(str).fillna(now.date().isoformat())
    df["crawl_hour"] = ts_series.dt.hour.fillna(now.hour).astype(int)

    # Loại dòng thiếu product_name
    df = df[df["product_name"].str.len() > 0]

    # Reset index
    df = df.reset_index(drop=True)

    return df
