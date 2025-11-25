# extract_service.py
import os, logging, smtplib
import pandas as pd
from datetime import datetime
from extract.crawler import harvest_site, SITES
# Try to load local hardcoded config (optional)
from email.mime.text import MIMEText
try:
    from .local_config import DEFAULT_SMTP as LOCAL_DEFAULT_SMTP
except Exception:
    LOCAL_DEFAULT_SMTP = None
try:
    from .log_store import start_run, update_run
except Exception:
    # If import fails, define no-op fallbacks
    def start_run(attempts=0):
        return None
    def update_run(run_id, status, message=None, csv_path=None, row_count=None):
        return None

OUT_DIR = "data_output"
LOG_DIR = "logs"
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(LOG_DIR, f"extract_{datetime.now().strftime('%Y%m%d')}.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8"
)

# Also stream INFO+ logs to console so progress is visible when running interactively
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logging.getLogger().addHandler(console_handler)

def send_error_email(subject, body, to_email, smtp_server, smtp_port, smtp_user, smtp_pass):
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = smtp_user
    msg['To'] = to_email
    try:
        with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, [to_email], msg.as_string())
        logging.info(f"Đã gửi email thông báo lỗi tới {to_email}")
    except Exception as e:
        logging.error(f"Lỗi gửi email: {e}")

def run_extract(max_retries=3, email_notify=False, email_config=None):
    """Run extract. Only retry when an error occurs. On success returns csv path.

    Behavior changed: do one extraction attempt; if it fails, retry up to max_retries-1 more times.
    """
    attempts_done = 0
    last_err = None
    # Support a test mode to force failure (useful to validate retry logic)
    force_fail = os.environ.get('EXTRACT_FORCE_FAIL', '').lower() in ('1', 'true', 'yes')

    # If email_notify requested but no explicit config provided, fall back to local config
    if email_notify and (email_config is None) and LOCAL_DEFAULT_SMTP:
        email_config = LOCAL_DEFAULT_SMTP

    # record run start in DB (if available)
    run_id = start_run(attempts=0)

    while True:
        attempts_done += 1
        # update attempts in DB if possible
        try:
            if run_id:
                update_run(run_id, status='running', message=f'Attempt {attempts_done}')
        except Exception:
            pass
        logging.info(f"=== BẮT ĐẦU QUÁ TRÌNH EXTRACT (thử lần {attempts_done}) ===")
        all_data = []
        # Honor optional environment variable to restrict crawling to a single site
        only_site = os.environ.get('EXTRACT_ONLY_SITE')
        sites_to_iterate = [only_site] if (only_site and only_site in SITES) else list(SITES.keys())
        if only_site and only_site not in SITES:
            logging.warning(f"EXTRACT_ONLY_SITE={only_site} but site not configured; ignoring restriction")

        for site in sites_to_iterate:
            logging.info(f"Đang crawl nguồn: {site}")
            try:
                if force_fail:
                    logging.info("EXTRACT_FORCE_FAIL is set -> forcing no data for this attempt (test mode)")
                    items = []
                else:
                    from time import perf_counter
                    t0 = perf_counter()
                    items = harvest_site(site)
                    t1 = perf_counter()
                    duration = t1 - t0
                    logging.info(f"Hoàn tất crawl {site}: tìm được {len(items)} URLs, thời gian {duration:.1f}s")
                all_data.extend(items)
            except Exception as e:
                logging.error(f"Lỗi crawl {site} (thử lần {attempts_done}): {e}")
                last_err = f"Lỗi crawl {site}: {e}"

        # If no data collected, consider this attempt a failure and possibly retry
        if not all_data:
            logging.error(f"KHÔNG lấy được dữ liệu (thử lần {attempts_done})!!!")
            last_err = last_err or "KHÔNG lấy được dữ liệu!!!"
            if attempts_done >= max_retries:
                logging.error(f"Đạt giới hạn {max_retries} lần thử, dừng và báo lỗi.")
                break
            else:
                logging.info(f"Sẽ thử lại (lần tiếp theo {attempts_done+1}/{max_retries}) sau 3s...")
                import time; time.sleep(3)
                continue

        # Try to write CSV; treat failures here as attempt failure and retry
        try:
            df = pd.DataFrame(all_data).drop_duplicates(subset=["url"]).reset_index(drop=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_path = os.path.join(OUT_DIR, f"raw_laptop_{timestamp}.csv")
            df.to_csv(csv_path, index=False, encoding="utf-8-sig")
            logging.info(f"Hoàn tất EXTRACT, tổng {len(df)} sản phẩm → {csv_path}")
            # If requested, send a success notification email using the existing helper
            if email_notify and email_config:
                try:
                    subject = "[Extract Pipeline] Hoàn tất extract"
                    body = f"Pipeline extract hoàn tất thành công. Tổng {len(df)} sản phẩm. File: {csv_path}"
                    send_error_email(subject, body,
                                     email_config['to_email'],
                                     email_config['smtp_server'],
                                     email_config['smtp_port'],
                                     email_config['smtp_user'],
                                     email_config['smtp_pass'])
                    logging.info(f"Đã gửi email thông báo hoàn tất tới {email_config['to_email']}")
                except Exception as e:
                    logging.error(f"Lỗi khi gửi email hoàn tất: {e}")
            # update DB as success
            try:
                if run_id:
                    update_run(run_id, status='success', message='Extract completed', csv_path=csv_path, row_count=len(df))
                else:
                    # fallback insert
                    try:
                        from .log_store import insert_final
                        insert_final(status='success', attempts=attempts_done, message='Extract completed', csv_path=csv_path, row_count=len(df))
                    except Exception:
                        pass
            except Exception:
                pass
            return csv_path
        except Exception as e:
            logging.error(f"Lỗi ghi file CSV (thử lần {attempts_done}): {e}")
            last_err = f"Lỗi ghi file CSV: {e}"
            if attempts_done >= max_retries:
                logging.error(f"Đạt giới hạn {max_retries} lần thử, dừng và báo lỗi.")
                break
            logging.info(f"Sẽ thử lại (lần tiếp theo {attempts_done+1}/{max_retries}) sau 3s...")
            import time; time.sleep(3)
            continue
    # Nếu quá số lần retry, gửi email nếu cấu hình
    if email_notify and email_config:
        subject = "[Extract Pipeline] Lỗi quá số lần chạy lại"
        body = f"Pipeline extract đã chạy lại {max_retries} lần nhưng vẫn lỗi.\nLỗi cuối cùng: {last_err}"
        send_error_email(subject, body,
                        email_config['to_email'],
                        email_config['smtp_server'],
                        email_config['smtp_port'],
                        email_config['smtp_user'],
                        email_config['smtp_pass'])
    return None
