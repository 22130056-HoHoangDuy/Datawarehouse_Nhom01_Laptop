import os
import sqlite3

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'logs', 'extract_runs.db')
if not os.path.exists(DB_PATH):
    print(f"No DB found at {DB_PATH}")
    raise SystemExit(1)

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
cur.execute('SELECT id, started_at, finished_at, status, rows_inserted, message, csv_path FROM load_runs ORDER BY id DESC LIMIT 20')
rows = cur.fetchall()
if not rows:
    print('No load run records found')
else:
    print(f'Found {len(rows)} load records (most recent first):')
    for r in rows:
        print('\n'.join([
            f'ID: {r[0]}',
            f'Started: {r[1]}',
            f'Finished: {r[2]}',
            f'Status: {r[3]}',
            f'Rows inserted: {r[4]}',
            f'Message: {r[5]}',
            f'CSV: {r[6]}',
            '---'
        ]))
conn.close()
