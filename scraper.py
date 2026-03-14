"""
scraper.py — Playwright-based Pinterest analytics scraper.

Launches a persistent Chromium context so login cookies survive between
runs. Intercepts Pinterest's internal XHR analytics responses — never
scrapes the DOM for metrics.
"""

import asyncio
import json
import logging
import re
from datetime import datetime, timezone
from typing import Optional

from playwright.async_api import async_playwright, Page, Response

import database

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
ANALYTICS_ENDPOINT_PATTERN = r"PinAnalyticsResource"
BROWSER_SESSION_DIR = ".browser_session"
PIN_DELAY = 2  # seconds between pins to avoid rate limiting

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

async def login(page: Page, email: str, password: str) -> None:
    """Navigate to /login/, fill credentials, wait for redirect."""
    await page.goto("https://www.pinterest.com/login/")
    await page.fill('input[name="id"]', email)
    await page.fill('input[name="password"]', password)
    await page.click('button[type="submit"]')
    # Wait until we leave /login/
    await page.wait_for_url(re.compile(r"^(?!.*\/login\/).*$"), timeout=30_000)
    logger.info("Login successful")


# ---------------------------------------------------------------------------
# Analytics response parsing
# ---------------------------------------------------------------------------

def parse_analytics_response(body: dict, pin_id: str) -> Optional[dict]:
    """Extract metrics from Pinterest's PinAnalyticsResource XHR payload.

    Expected shape:
        {
          "resource_response": {
            "data": {
              "lifetime_metrics": {
                "IMPRESSION": <int>,
                "SAVE":       <int>,
                "PIN_CLICK":  <int>
              }
            }
          }
        }

    Returns a metrics dict ready for database.insert_metrics, or None if the
    payload doesn't match the expected shape.
    """
    try:
        metrics_data = (
            body["resource_response"]["data"]["lifetime_metrics"]
        )
        impressions = int(metrics_data.get("IMPRESSION", 0))
        saves = int(metrics_data.get("SAVE", 0))
        clicks = int(metrics_data.get("PIN_CLICK", 0))
    except (KeyError, TypeError, ValueError):
        logger.warning("Unexpected analytics payload shape for pin %s", pin_id)
        return None

    engagement_rate = (
        (saves + clicks) / impressions * 100 if impressions > 0 else 0.0
    )

    return {
        "pin_id": pin_id,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "impressions": impressions,
        "saves": saves,
        "clicks": clicks,
        "engagement_rate": round(engagement_rate, 4),
    }


# ---------------------------------------------------------------------------
# DOM metadata
# ---------------------------------------------------------------------------

async def scrape_pin_metadata(page: Page, pin_url: str) -> dict:
    """Return pin identity dict scraped from the DOM.

    Gracefully falls back to '' for any element that times out.
    """
    pin_id = pin_url.rstrip("/").split("/")[-1]

    async def safe_text(selector: str) -> str:
        try:
            loc = page.locator(selector).first
            return (await loc.inner_text(timeout=5_000)).strip()
        except Exception:
            return ""

    async def safe_attr(selector: str, attr: str) -> str:
        try:
            loc = page.locator(selector).first
            return (await loc.get_attribute(attr, timeout=5_000)) or ""
        except Exception:
            return ""

    title = await safe_text("h1[data-test-id='pin-title']")
    description = await safe_text("[data-test-id='pin-description']")
    image_url = await safe_attr("img[src*='pinimg.com']", "src")

    return {
        "pin_id": pin_id,
        "url": pin_url,
        "title": title,
        "description": description,
        "image_url": image_url,
    }


# ---------------------------------------------------------------------------
# Per-pin scrape
# ---------------------------------------------------------------------------

async def scrape_pin(page: Page, pin_url: str) -> bool:
    """Full scrape for one pin. Returns True on success."""
    pin_id = pin_url.rstrip("/").split("/")[-1]
    captured: list[dict] = []

    async def handle_response(response: Response) -> None:
        if re.search(ANALYTICS_ENDPOINT_PATTERN, response.url, re.IGNORECASE):
            try:
                body = await response.json()
                metrics = parse_analytics_response(body, pin_id)
                if metrics:
                    captured.append(metrics)
            except Exception as exc:
                logger.debug("Could not parse analytics response: %s", exc)

    page.on("response", handle_response)

    try:
        await page.goto(pin_url, wait_until="domcontentloaded", timeout=30_000)
        # Allow XHR requests a moment to arrive
        await asyncio.sleep(3)

        metadata = await scrape_pin_metadata(page, pin_url)
        database.upsert_pin(metadata)

        if captured:
            database.insert_metrics(captured[0])
            logger.info("Scraped pin %s — metrics captured", pin_id)
        else:
            logger.warning("Scraped pin %s — no analytics XHR intercepted", pin_id)

        return True
    except Exception as exc:
        logger.error("Failed to scrape pin %s: %s", pin_id, exc)
        return False
    finally:
        page.remove_listener("response", handle_response)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def run_scraper(pin_urls: list[str], email: str, password: str) -> None:
    """Async entry point called by pipeline.py."""
    database.init_db()

    async with async_playwright() as pw:
        context = await pw.chromium.launch_persistent_context(
            user_data_dir=BROWSER_SESSION_DIR,
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )

        page = await context.new_page()

        # Log in only when not already authenticated
        await page.goto("https://www.pinterest.com/pin/318770479893190821/")
        if "/login/" in page.url or "pinterest.com" not in page.url:
            await login(page, email, password)

        for pin_url in pin_urls:
            await scrape_pin(page, pin_url)
            await asyncio.sleep(PIN_DELAY)

        await context.close()

    logger.info("Scraper finished — %d pins processed", len(pin_urls))
