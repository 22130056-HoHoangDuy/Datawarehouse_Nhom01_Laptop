import os
import sys
import time

# Ensure project root on path
proj_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if proj_root not in sys.path:
    sys.path.insert(0, proj_root)

# Force extract to fail
os.environ['EXTRACT_FORCE_FAIL'] = '1'

from extract import extract_service as es

print('Running run_extract with EXTRACT_FORCE_FAIL=1 to force failure and trigger error email...')
csv_path = es.run_extract(max_retries=2, email_notify=True, email_config=None)
print('run_extract returned:', csv_path)
if not csv_path:
    print('As expected, extract failed and error email should have been sent (if SMTP valid in local_config).')
else:
    print('Unexpected: extract succeeded')

# give logs a moment
time.sleep(1)
