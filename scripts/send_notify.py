"""Send a one-off notification email using the extract_service helper.

Usage (PowerShell):
$env:EXTRACT_SMTP_USER = "your_email@example.com"
$env:EXTRACT_SMTP_PASS = "APP_PASSWORD"
$env:EXTRACT_SMTP_SERVER = "smtp.gmail.com"
$env:EXTRACT_SMTP_PORT = "465"
$env:EXTRACT_NOTIFY_EMAIL = "notify_to@example.com"
py scripts\send_notify.py

The script sends a subject/body preset message if no args provided.
"""
import os
import sys
from extract.extract_service import send_error_email

DEFAULT_SUBJECT = "[Alert] Extract system failure"
DEFAULT_BODY = "extract hệ thống dataWH đã lỗi vui lòng kiểm tra"

def main():
    smtp_user = os.environ.get("EXTRACT_SMTP_USER")
    smtp_pass = os.environ.get("EXTRACT_SMTP_PASS")
    smtp_server = os.environ.get("EXTRACT_SMTP_SERVER")
    smtp_port = os.environ.get("EXTRACT_SMTP_PORT")
    to_email = os.environ.get("EXTRACT_NOTIFY_EMAIL")

    if not all([smtp_user, smtp_pass, smtp_server, smtp_port, to_email]):
        print("Missing SMTP configuration. Please set EXTRACT_SMTP_USER, EXTRACT_SMTP_PASS, EXTRACT_SMTP_SERVER, EXTRACT_SMTP_PORT, EXTRACT_NOTIFY_EMAIL environment variables.")
        sys.exit(1)

    subject = DEFAULT_SUBJECT
    body = DEFAULT_BODY
    # optional CLI args: subject, body
    if len(sys.argv) > 1:
        subject = sys.argv[1]
    if len(sys.argv) > 2:
        body = sys.argv[2]

    try:
        send_error_email(subject, body, to_email, smtp_server, int(smtp_port), smtp_user, smtp_pass)
        print(f"Notification sent to {to_email}")
    except Exception as e:
        print(f"Failed to send notification: {e}")
        sys.exit(2)

if __name__ == '__main__':
    main()
