"""
Async Playwright helper for JS-heavy Pakistani supermarket sites.
Provides a context-manager that launches a headless Chromium browser.
"""

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

from playwright.async_api import (
    async_playwright,
    Browser,
    BrowserContext,
    Page,
    Playwright,
)

import config

logger = logging.getLogger("playwright_helper")


@asynccontextmanager
async def browser_context(headless: bool = True) -> AsyncGenerator[BrowserContext, None]:
    """Yield a Playwright BrowserContext with sensible defaults."""
    pw: Playwright = await async_playwright().start()
    browser: Browser = await pw.chromium.launch(headless=headless)
    ctx: BrowserContext = await browser.new_context(
        user_agent=config.USER_AGENT,
        viewport={"width": 1366, "height": 768},
        locale="en-US",
        java_script_enabled=True,
    )
    try:
        yield ctx
    finally:
        await ctx.close()
        await browser.close()
        await pw.stop()


async def get_page_content(
    ctx: BrowserContext,
    url: str,
    wait_selector: Optional[str] = None,
    timeout: int = 30_000,
) -> str:
    """Navigate to *url*, optionally wait for a CSS selector, return HTML.

    If *wait_selector* is given but not found within *timeout*, the function
    logs a debug warning and still returns the page HTML rather than raising.
    This prevents a single missing selector from silently killing the entire
    Playwright fallback for a category.
    """
    page: Page = await ctx.new_page()
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
        if wait_selector:
            try:
                await page.wait_for_selector(wait_selector, timeout=timeout)
            except Exception:
                # Selector not present — content may still have products under
                # a different CSS class; proceed with what loaded.
                logger.debug(
                    "wait_for_selector(%r) timed out on %s — continuing anyway.",
                    wait_selector, url,
                )
        await page.wait_for_timeout(2000)
        return await page.content()
    finally:
        await page.close()



