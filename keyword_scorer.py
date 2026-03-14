"""
keyword_scorer.py — TF-IDF + Pinterest Trends keyword health scorer.

For every pin in the DB, extracts the top-10 TF-IDF keywords from its
description, fetches relative search volume from Pinterest Trends via
Playwright XHR interception, computes a health score, and writes results
back to the keywords table (full refresh per pin per run).

Entry point for pipeline.py: run_keyword_scoring()
"""

import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Callable, Optional

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer

from playwright.async_api import async_playwright, Page, Response

import database

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BROWSER_SESSION_DIR = ".browser_session"
TREND_ENDPOINT_PATTERN = r"TrendingSearches"
TOP_N_KEYWORDS = 10

TFIDF_WEIGHT = 0.4
TREND_WEIGHT = 0.6

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Health score
# ---------------------------------------------------------------------------

def compute_health(tfidf: float, trend_vol: float) -> float:
    """health = tfidf * 0.4 + (trend_vol / 100) * 0.6  (range 0–1)."""
    return round(tfidf * TFIDF_WEIGHT + (trend_vol / 100.0) * TREND_WEIGHT, 6)


# ---------------------------------------------------------------------------
# TF-IDF extraction
# ---------------------------------------------------------------------------

def extract_top_keywords(descriptions: list[str]) -> list[list[tuple[str, float]]]:
    """Return top-N (keyword, tfidf_score) pairs for each description.

    Builds a single TF-IDF matrix across the full corpus so scores are
    comparable across pins.  Tokens shorter than 3 characters and English
    stopwords are removed.
    """
    if not any(d.strip() for d in descriptions):
        return [[] for _ in descriptions]

    vectorizer = TfidfVectorizer(
        stop_words="english",
        token_pattern=r"(?u)\b[a-zA-Z]{3,}\b",
    )
    matrix = vectorizer.fit_transform(descriptions)
    feature_names: np.ndarray = vectorizer.get_feature_names_out()

    results: list[list[tuple[str, float]]] = []
    for i in range(matrix.shape[0]):
        row = matrix[i].toarray()[0]
        paired = sorted(
            zip(feature_names, row), key=lambda x: x[1], reverse=True
        )
        top = [(kw, float(score)) for kw, score in paired[:TOP_N_KEYWORDS] if score > 0]
        results.append(top)
    return results


# ---------------------------------------------------------------------------
# Pinterest Trends scraping
# ---------------------------------------------------------------------------

def _parse_trend_volume(body: dict, keyword: str) -> float:
    """Extract relative search volume (0–100) from a TrendingSearches payload.

    Pinterest's trends XHR returns a structure like:
        {
          "resource_response": {
            "data": [
              {"term": "...", "volume": 72, ...},
              ...
            ]
          }
        }
    Falls back to 0.0 when the shape is unexpected.
    """
    try:
        items = body["resource_response"]["data"]
        if not isinstance(items, list):
            return 0.0
        kw_lower = keyword.lower()
        for item in items:
            if str(item.get("term", "")).lower() == kw_lower:
                return float(min(max(item.get("volume", 0), 0), 100))
        # Keyword not found in results — use the top item's volume as a proxy
        if items:
            return float(min(max(items[0].get("volume", 0), 0), 100))
    except (KeyError, TypeError, ValueError, AttributeError):
        pass
    return 0.0


async def fetch_trend_volume(
    page: Page, keyword: str, cache: dict[str, float]
) -> float:
    """Return the Pinterest Trends relative volume for *keyword* (0–100).

    Results are cached in *cache* for the lifetime of the run so each
    keyword is scraped at most once.
    """
    if keyword in cache:
        return cache[keyword]

    captured: list[dict] = []

    async def handle_response(response: Response) -> None:
        if re.search(TREND_ENDPOINT_PATTERN, response.url, re.IGNORECASE):
            try:
                body = await response.json()
                captured.append(body)
            except Exception:
                pass

    page.on("response", handle_response)
    vol = 0.0
    try:
        await page.goto(
            f"https://www.pinterest.com/trends/?term={keyword}",
            wait_until="domcontentloaded",
            timeout=20_000,
        )
        await asyncio.sleep(2)

        if captured:
            vol = _parse_trend_volume(captured[0], keyword)
        else:
            logger.warning("No TrendingSearches XHR captured for keyword '%s'", keyword)
    except Exception as exc:
        logger.warning("Trend fetch failed for '%s': %s", keyword, exc)
    finally:
        page.remove_listener("response", handle_response)

    cache[keyword] = vol
    return vol


# ---------------------------------------------------------------------------
# DB writes
# ---------------------------------------------------------------------------

def _refresh_keywords_for_pin(pin_id: str, rows: list[dict], db_path: str) -> None:
    """Delete existing keyword rows for *pin_id* then bulk-insert *rows*."""
    insert_sql = """
        INSERT INTO keywords (pin_id, keyword, tfidf, trend_vol, health, scored_at)
        VALUES (:pin_id, :keyword, :tfidf, :trend_vol, :health, :scored_at)
    """
    with database.get_conn(db_path) as conn:
        conn.execute("DELETE FROM keywords WHERE pin_id = ?", (pin_id,))
        if rows:
            conn.executemany(insert_sql, rows)


# ---------------------------------------------------------------------------
# Core async runner
# ---------------------------------------------------------------------------

async def _score_pins_async(
    db_path: str = database.DB_PATH,
    trend_fetcher: Optional[Callable] = None,
) -> None:
    """Score all pins. *trend_fetcher* may be injected for testing.

    Signature of trend_fetcher: async (page, keyword, cache) -> float
    """
    pins = database.get_pins(db_path=db_path)
    if not pins:
        logger.info("No pins found — keyword scoring skipped.")
        return

    descriptions = [p.get("description") or "" for p in pins]
    per_pin_keywords = extract_top_keywords(descriptions)

    # Collect every unique keyword we'll need to score
    all_keywords: set[str] = {
        kw for kws in per_pin_keywords for kw, _ in kws
    }
    logger.info(
        "Scoring %d pins — %d unique keywords to look up",
        len(pins),
        len(all_keywords),
    )

    cache: dict[str, float] = {}
    _fetcher = trend_fetcher or fetch_trend_volume

    scored_at = datetime.now(timezone.utc).isoformat()

    async with async_playwright() as pw:
        context = await pw.chromium.launch_persistent_context(
            user_data_dir=BROWSER_SESSION_DIR,
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        page = await context.new_page()

        for pin, top_kws in zip(pins, per_pin_keywords):
            pin_id: str = pin["pin_id"]
            rows: list[dict] = []

            for keyword, tfidf_score in top_kws:
                trend_vol = await _fetcher(page, keyword, cache)
                health = compute_health(tfidf_score, trend_vol)
                rows.append(
                    {
                        "pin_id": pin_id,
                        "keyword": keyword,
                        "tfidf": tfidf_score,
                        "trend_vol": trend_vol,
                        "health": health,
                        "scored_at": scored_at,
                    }
                )

            _refresh_keywords_for_pin(pin_id, rows, db_path)
            logger.info("Scored pin %s — %d keywords written", pin_id, len(rows))

        await context.close()


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_keyword_scoring(
    db_path: str = database.DB_PATH,
    trend_fetcher: Optional[Callable] = None,
) -> None:
    """Synchronous entry point called by pipeline.py."""
    database.init_db(db_path=db_path)
    asyncio.run(_score_pins_async(db_path=db_path, trend_fetcher=trend_fetcher))
    logger.info("Keyword scoring complete.")
