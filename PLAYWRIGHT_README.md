Playwright integration

This project can optionally use Playwright to render JS-heavy pages (e.g., Thegioididong category pages) so the crawler sees client-rendered product lists and prices.

Install steps (Windows PowerShell):

```powershell
pip install -r requ.txt
# Then install browser binaries for playwright
python -m playwright install
# or, with new playwright CLI
# playwright install chromium
```

Usage:
- The crawler will automatically try to import the renderer. If Playwright is installed and browsers are present, TGDD category pages will be fetched via Playwright automatically.
- If Playwright is not available, the crawler falls back to the existing HTTP GET implementation.

Notes:
- Playwright adds significant dependencies and increases runtime cost per fetched page; prefer using it only for category pages where server returns a skeleton HTML.
- On CI or headless servers, ensure required dependencies for Playwright (browsers and some OS packages) are available.

