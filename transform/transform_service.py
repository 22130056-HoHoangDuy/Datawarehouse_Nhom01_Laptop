# transform_service.py
import os
import pandas as pd
from datetime import datetime
from transform.clean_transform import clean_dataframe

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

def run_transform(csv_path: str):
    """
    Nhận CSV từ Extract → trả ra DataFrame sạch để Load.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Không tìm thấy file CSV: {csv_path}")

    print(f"[TRANSFORM] Đọc CSV: {csv_path}")
    df = pd.read_csv(csv_path)
    df.columns = [c.strip().lower() for c in df.columns]

    # Làm sạch
    print("[TRANSFORM] Làm sạch dữ liệu...")
    df_clean = clean_dataframe(df)

    print(f"[TRANSFORM] Hoàn tất. Giữ lại {len(df_clean)} dòng hợp lệ.")

    # Lưu optional file transform (phục vụ debug)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join("data_output", f"clean_laptop_{ts}.csv")
    df_clean.to_csv(out_path, index=False, encoding="utf-8-sig")

    print(f"[TRANSFORM] Lưu dữ liệu sạch → {out_path}")

    return df_clean
