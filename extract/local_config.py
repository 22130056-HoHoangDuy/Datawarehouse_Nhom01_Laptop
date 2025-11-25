"""
Local configuration for extract/email. THIS FILE CONTAINS SENSITIVE CREDENTIALS.
Only use for a private, non-shared environment. Do NOT commit to a public repo.

Edit values below to match your SMTP/account settings.
"""

DEFAULT_SMTP = {
    # SMTP server settings
    'smtp_server': 'smtp.gmail.com',
    'smtp_port': 465,
    # Email account to send from (change to your account)
    'smtp_user': '22130314@st.hcmuaf.edu.vn',
    # App password or account password (hardcoded here per request)
    'smtp_pass': 'pmpv eycb bxqh eewc',
    # Notification recipient
    'to_email': 'tungekko113@gmail.com',
}
