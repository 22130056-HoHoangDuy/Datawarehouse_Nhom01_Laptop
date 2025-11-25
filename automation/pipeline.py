# pipeline.py

import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extract.extract_service import run_extract
from staging.staging_loader import load_to_staging
from transform.transform_service import run_transform
from load.load_service import run_load
from datamart.load_mart import load_datamart

def main():
    print("=== PIPELINE START ===")

    # 1) EXTRACT
    print("\n[1] Extracting data...")
    csv_raw = run_extract()
    if not csv_raw:
        print("Extract failed. Pipeline stopped.")
        sys.exit(1)

    # 2) LOAD TO STAGING
    print("\n[2] Loading raw CSV → Staging...")
    count = load_to_staging(csv_raw)
    print(f"Staging loaded: {count} rows")

    # 3) TRANSFORM
    print("\n[3] Transforming data from Staging...")
    df_clean, clean_csv_path = run_transform(csv_raw, return_output_path=True)
    if df_clean is None or len(df_clean) == 0:
        print("Transform produced no data. Pipeline stopped.")
        sys.exit(2)

    # 4) LOAD DATA WAREHOUSE
    print("\n[4] Loading into Data Warehouse...")
    rows = run_load(clean_csv_path)
    if rows <= 0:
        print("Load failed. Pipeline stopped.")
        sys.exit(3)

    print(f"Loaded {rows} rows into DW.")

    # 5) LOAD DATA MART
    print("\n[5] Building Data Marts...")
    load_datamart()

    print("\n=== PIPELINE FINISHED SUCCESSFULLY ===")
    sys.exit(0)


if __name__ == "__main__":
    main()
