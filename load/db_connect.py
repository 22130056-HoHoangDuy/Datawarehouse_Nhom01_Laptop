"""Database connection helpers for loaders.
Provides a simple `mysql_connect()` that reads DB_* env vars or .env.
"""
import os

def _load_env():
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except Exception:
        pass


def mysql_connect():
    """Return a mysql.connector connection using env vars: DB_HOST, DB_USER, DB_PASS, DB_NAME, DB_PORT
    Raises RuntimeError if config missing or ImportError if connector not installed.
    """
    _load_env()
    db_host = os.environ.get('DB_HOST')
    db_user = os.environ.get('DB_USER')
    db_pass = os.environ.get('DB_PASS')
    db_name = os.environ.get('DB_NAME')
    db_port = os.environ.get('DB_PORT', '3306')
    if not all([db_host, db_user, db_pass, db_name]):
        raise RuntimeError('DB config not provided in environment')
    try:
        import mysql.connector
    except Exception as e:
        raise ImportError('mysql-connector-python is required to connect to MySQL: ' + str(e))
    conn = mysql.connector.connect(host=db_host, user=db_user, password=db_pass, database=db_name, port=int(db_port))
    return conn
