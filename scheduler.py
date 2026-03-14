"""
scheduler.py — APScheduler-based daily cron runner.

Process entrypoint: python scheduler.py

Runs run_pipeline() every day at RUN_HOUR:RUN_MINUTE with up to 3 attempts
and exponential backoff (60 → 120 → 240 s). Failures beyond all retries are
appended to logs/errors.log.
"""

import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from pipeline import run_pipeline

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
RUN_HOUR = 3
RUN_MINUTE = 0

MAX_ATTEMPTS = 3
BACKOFF_SECONDS = [60, 120, 240]

LOGS_DIR = Path("logs")
LOG_FILE = LOGS_DIR / "pipeline.log"
ERROR_LOG = LOGS_DIR / "errors.log"

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
LOGS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE),
    ],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Retry wrapper
# ---------------------------------------------------------------------------

def run_pipeline_with_retry() -> None:
    """Run run_pipeline() up to MAX_ATTEMPTS times with exponential backoff."""
    last_exc: Exception | None = None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            logger.info("Pipeline attempt %d/%d", attempt, MAX_ATTEMPTS)
            run_pipeline()
            logger.info("Pipeline succeeded on attempt %d", attempt)
            return
        except Exception as exc:
            last_exc = exc
            logger.error(
                "Attempt %d failed: %s", attempt, exc, exc_info=True
            )
            if attempt < MAX_ATTEMPTS:
                wait = BACKOFF_SECONDS[attempt - 1]
                logger.info("Retrying in %d seconds…", wait)
                time.sleep(wait)

    # All retries exhausted
    timestamp = datetime.now(timezone.utc).isoformat()
    message = f"{timestamp} — all {MAX_ATTEMPTS} attempts failed. Last error: {last_exc}\n"
    logger.critical("All retry attempts exhausted. See %s for details.", ERROR_LOG)
    with ERROR_LOG.open("a") as fh:
        fh.write(message)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    scheduler = BlockingScheduler()
    scheduler.add_job(
        run_pipeline_with_retry,
        trigger=CronTrigger(hour=RUN_HOUR, minute=RUN_MINUTE),
        id="daily_pinterest_pipeline",
        name="Daily Pinterest SEO pipeline",
    )
    logger.info(
        "Scheduler started — pipeline runs daily at %02d:%02d UTC",
        RUN_HOUR,
        RUN_MINUTE,
    )
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped")


if __name__ == "__main__":
    main()
