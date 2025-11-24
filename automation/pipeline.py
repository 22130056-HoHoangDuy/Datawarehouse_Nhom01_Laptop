# pipeline.py

import sys, os

# Thêm ROOT vào PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extract.extract_service import run_extract
from transform.transform_service import run_transform
from load.load_service import run_load


def main():
    print("=== PIPELINE START ===")

    # 1. Extract
    print("\n[1] Extracting data...")
    csv_raw = run_extract()
    if not csv_raw:
        print("Extract failed. Stopping pipeline.")
        sys.exit(1)

    # 2. Transform
    print("\n[2] Transforming data...")
    df_clean = run_transform(csv_raw)
    if df_clean is None or len(df_clean) == 0:
        print("Transform produced no data. Stopping pipeline.")
        sys.exit(2)

    # 3. Load
    print("\n[3] Loading to Data Warehouse...")
    rows = run_load(df_clean)
    if rows <= 0:
        print("Load failed or inserted 0 rows.")
        sys.exit(3)

    print("\n=== PIPELINE FINISHED SUCCESSFULLY ===")
    sys.exit(0)


if __name__ == "__main__":
    main()
