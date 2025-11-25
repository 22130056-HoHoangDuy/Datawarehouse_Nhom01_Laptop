import os
import sys
import time

# ensure project root in path
proj_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if proj_root not in sys.path:
    sys.path.insert(0, proj_root)

# monkeypatch harvest_site to avoid real crawling
from extract import crawler

def fake_harvest_site(site):
    print(f"fake_harvest_site called for site={site}")
    return [
        {'url': f'https://example.com/{site}/1', 'title': 'Test Product 1', 'price': 100},
        {'url': f'https://example.com/{site}/2', 'title': 'Test Product 2', 'price': 200},
    ]

# Apply monkeypatch
import extract.extract_service as es
es.harvest_site = fake_harvest_site

# Build email config from local_config fallback (None -> use local_config in extract_service)

# Run extract -> transform -> load
from extract.extract_service import run_extract
from transform.transform_service import run_transform
from load.load_service import run_load

print('Running extract (mock) -> transform -> load (local log)')
csv_raw = run_extract(max_retries=1, email_notify=True, email_config=None)
print('raw csv:', csv_raw)
if not csv_raw:
    print('extract failed')
    sys.exit(1)

df_clean = run_transform(csv_raw)
print('clean rows:', len(df_clean) if df_clean is not None else None)
rows = run_load(df_clean)
print('rows inserted/handled by load:', rows)

# show logs
print('\n-- Extract logs --')
os.system('py scripts\show_extract_logs.py')
print('\n-- Load logs --')
os.system('py scripts\show_load_logs.py')

print('\nDone')
