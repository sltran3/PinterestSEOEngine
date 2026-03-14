"""
pipeline.py — Top-level orchestrator.

Called by scheduler.py via run_pipeline(). Reads credentials and pin URLs
from environment variables, then runs each stage in sequence.

Stages 2–4 are stubbed as commented imports; uncomment each line as the
corresponding file is built.
"""

import asyncio
import logging
import os

from dotenv import load_dotenv

import scraper
import keyword_scorer
import ab_engine
import dashboard

logger = logging.getLogger(__name__)

load_dotenv()  # loads .env into os.environ before anything reads from it

# ---------------------------------------------------------------------------
# Config — read once at import time so tests can monkeypatch os.environ
# ---------------------------------------------------------------------------
PINTEREST_EMAIL = os.environ.get("PINTEREST_EMAIL", "")
PINTEREST_PASSWORD = os.environ.get("PINTEREST_PASSWORD", "")
_RAW_URLS = os.environ.get("PINTEREST_PIN_URLS", "https://www.pinterest.com/pin/318770479893190821/")


def _parse_pin_urls(raw: str) -> list[str]:
    """Split comma-separated URLs, strip whitespace, drop empty strings."""
    return [u.strip() for u in raw.split(",") if u.strip()]


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_pipeline() -> None:
    """Execute every pipeline stage in sequence."""
    email = os.environ.get("PINTEREST_EMAIL", PINTEREST_EMAIL)
    password = os.environ.get("PINTEREST_PASSWORD", PINTEREST_PASSWORD)
    pin_urls = _parse_pin_urls(os.environ.get("PINTEREST_PIN_URLS", _RAW_URLS))

    if not email or not password:
        raise EnvironmentError(
            "PINTEREST_EMAIL and PINTEREST_PASSWORD must be set."
        )
    if not pin_urls:
        logger.warning("PINTEREST_PIN_URLS is empty — nothing to scrape.")
        return

    logger.info("Pipeline starting — %d pins to process", len(pin_urls))

    # Stage 1: scrape
    asyncio.run(scraper.run_scraper(pin_urls, email, password))
    logger.info("Stage 1 (scrape) complete")

    # Stage 2: keyword scoring
    keyword_scorer.run_keyword_scoring()
    logger.info("Stage 2 (keyword scoring) complete")

    # Stage 3: A/B testing
    ab_engine.run_ab_engine()
    logger.info("Stage 3 (A/B engine) complete")

    # Stage 4: dashboard
    dashboard.render_dashboard()
    logger.info("Stage 4 (dashboard) complete")

    logger.info("Pipeline finished successfully")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    run_pipeline()
