"""Playwright-based renderer helper.

Provides `render_html(url, ...)` which returns the page HTML after JS rendering.
Requires `playwright` package and browser binaries installed (see PLAYWRIGHT_README.md).
"""
import logging
import time
from random import random

try:
    from playwright.sync_api import sync_playwright
except Exception as e:
    sync_playwright = None


def render_html(url, wait_until='networkidle', timeout=15000, headless=True):
    """Render the page and return HTML string.

    Returns None on failure.
    """
    if sync_playwright is None:
        logging.warning("Playwright not installed; render_html unavailable.")
        return None

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
                ),
                viewport={"width": 1200, "height": 900},
            )
            page = context.new_page()
            page.goto(url, wait_until=wait_until, timeout=timeout)
            # small jitter to let client scripts settle
            time.sleep(0.5 + random() * 0.5)
            html = page.content()
            browser.close()
            return html
    except Exception as e:
        logging.warning(f"Playwright render failed for {url}: {e}")
        return None
