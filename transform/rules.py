import pandas as pd
import re
import ast  # dùng để convert string list thành list thực

def normalize_brand(x):
    """
    Nếu là list hoặc string dạng list, lấy phần tử đầu tiên,
    nếu string bình thường thì strip + upper,
    nếu None hoặc rỗng thì trả 'UNKNOWN'.
    """
    if isinstance(x, list) and x:
        return str(x[0]).strip().upper()
    if isinstance(x, str):
        x = x.strip()
        # Nếu string có dạng list, chuyển thành list thực
        if x.startswith("[") and x.endswith("]"):
            try:
                x_list = ast.literal_eval(x)
                if isinstance(x_list, list) and x_list:
                    return str(x_list[0]).strip().upper()
            except:
                pass
        elif x:
            return x.upper()
    return "UNKNOWN"

def normalize_source(x):
    if not isinstance(x, str) or not x.strip():
        return "unknown"
    return x.strip().lower()

def normalize_product_name(x):
    if not isinstance(x, str) or not x.strip():
        return "UNKNOWN_PRODUCT"
    return x.strip()

def clean_sold_count(val):
    if pd.isna(val) or val == "":
        return 0
    s = str(val).lower().replace(" ", "").replace(",", "").replace(".", "")
    try:
        if "k" in s:
            return int(float(s.replace("k", "")) * 1000)
        return int(float(s))
    except:
        return 0

def safe_price(x):
    if pd.isna(x) or str(x).strip() == "":
        return 0
    try:
        digits_only = re.sub(r"[^\d]", "", str(x))
        return int(digits_only) if digits_only else 0
    except:
        return 0
