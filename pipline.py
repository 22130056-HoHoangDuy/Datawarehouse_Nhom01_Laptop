# pipeline.py
import sys
import os
from extract.extract_service import run_extract
from transform.transform_service import run_transform
from load.load_service import run_load

def main():
    print("=== PIPELINE START ===")

    # 1. Extract
    print("\n[1] Extracting data...")
    # Read retry and email settings from environment (optional)
    try:
        max_retries = int(os.environ.get("EXTRACT_MAX_RETRIES", "3"))
    except Exception:
        max_retries = 3

    # Read SMTP/email settings from environment variables (DO NOT hardcode credentials)
    smtp_user = os.environ.get("EXTRACT_SMTP_USER")
    smtp_pass = os.environ.get("EXTRACT_SMTP_PASS")
    smtp_server = os.environ.get("EXTRACT_SMTP_SERVER")
    smtp_port = os.environ.get("EXTRACT_SMTP_PORT")
    to_email = os.environ.get("EXTRACT_NOTIFY_EMAIL")

    email_notify = all([smtp_user, smtp_pass, smtp_server, smtp_port, to_email])
    email_config = None
    if email_notify:
        email_config = {
            'to_email': to_email,
            'smtp_server': smtp_server,
            'smtp_port': int(smtp_port),
            'smtp_user': smtp_user,
            'smtp_pass': smtp_pass
        }

    csv_raw = run_extract(max_retries=max_retries, email_notify=email_notify, email_config=email_config)
    if not csv_raw:
        print("❌ Extract failed. Stopping pipeline.")
        sys.exit(1)

    # 2. Transform
    print("\n[2] Transforming data...")
    df_clean = run_transform(csv_raw)
    if df_clean is None or len(df_clean) == 0:
        print("❌ Transform produced no data. Stopping pipeline.")
        sys.exit(2)

    # 3. Load
    print("\n[3] Loading to Data Warehouse...")
    rows = run_load(df_clean)
    if rows <= 0:
        print("❌ Load failed or inserted 0 rows.")
        sys.exit(3)

    print("\n=== PIPELINE FINISHED SUCCESSFULLY ===")
    sys.exit(0)

if __name__ == "__main__":
    main()
