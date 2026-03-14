"""
ab_engine.py — A/B test engine for Pinterest pin variants.

Groups pins sharing a variant_group id, collects their engagement_rate
time-series, runs a Welch t-test after the 14-day experiment window, and
writes the winner back to the ab_variants table.

Entry point for pipeline.py: run_ab_engine()
"""

import logging
from datetime import datetime, timedelta, timezone

from scipy import stats

import database

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
EXPERIMENT_WINDOW_DAYS = 14
MIN_DAYS = 7
MIN_OBSERVATIONS = 7
SIGNIFICANCE_THRESHOLD = 0.05

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------

def _parse_ts(ts_str: str) -> datetime:
    """Parse an ISO-8601 string to a timezone-aware datetime (UTC if naive)."""
    dt = datetime.fromisoformat(ts_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _days_span(timestamps: list[str]) -> float:
    """Return the number of days between the earliest and latest timestamp."""
    if len(timestamps) < 2:
        return 0.0
    dts = [_parse_ts(ts) for ts in timestamps]
    return (max(dts) - min(dts)).total_seconds() / 86400.0


# ---------------------------------------------------------------------------
# Group evaluation
# ---------------------------------------------------------------------------

def _declare_winner(
    group_id: str,
    winning_pin_id: str,
    losing_pin_id: str,
    p_value: float,
    evaluated_at: str,
    db_path: str,
) -> None:
    """Write winner=1 to the winning variant and winner=0 to the loser."""
    sql = """
        UPDATE ab_variants
        SET winner = ?, p_value = ?, evaluated_at = ?
        WHERE pin_id = ? AND variant_group = ?
    """
    with database.get_conn(db_path) as conn:
        conn.execute(sql, (1, p_value, evaluated_at, winning_pin_id, group_id))
        conn.execute(sql, (0, p_value, evaluated_at, losing_pin_id, group_id))


def _evaluate_group(group_id: str, db_path: str) -> None:
    """Run the full evaluation pipeline for one variant group."""
    variants = database.get_ab_variants_for_group(group_id, db_path=db_path)

    if len(variants) != 2:
        logger.warning(
            "Group %s: expected 2 variants, found %d — skipping",
            group_id, len(variants),
        )
        return

    variant_map = {v["variant"]: v for v in variants}
    if "A" not in variant_map or "B" not in variant_map:
        logger.warning("Group %s: missing variant A or B — skipping", group_id)
        return

    pin_a = variant_map["A"]["pin_id"]
    pin_b = variant_map["B"]["pin_id"]

    metrics_a = database.get_metrics_for_pin(pin_a, db_path=db_path)
    metrics_b = database.get_metrics_for_pin(pin_b, db_path=db_path)

    # Guard: minimum number of observations
    if len(metrics_a) < MIN_OBSERVATIONS or len(metrics_b) < MIN_OBSERVATIONS:
        logger.warning(
            "Group %s: insufficient observations (A=%d, B=%d, need %d) — skipping",
            group_id, len(metrics_a), len(metrics_b), MIN_OBSERVATIONS,
        )
        return

    timestamps_a = [r["scraped_at"] for r in metrics_a]
    timestamps_b = [r["scraped_at"] for r in metrics_b]

    # Guard: both variants must span at least MIN_DAYS
    span_a = _days_span(timestamps_a)
    span_b = _days_span(timestamps_b)
    if span_a < MIN_DAYS or span_b < MIN_DAYS:
        logger.warning(
            "Group %s: insufficient time span (A=%.1f days, B=%.1f days, need %d) — skipping",
            group_id, span_a, span_b, MIN_DAYS,
        )
        return

    # Guard: experiment window must have elapsed
    all_timestamps = timestamps_a + timestamps_b
    earliest = min(_parse_ts(ts) for ts in all_timestamps)
    window_end = earliest + timedelta(days=EXPERIMENT_WINDOW_DAYS)
    now = datetime.now(timezone.utc)
    if now < window_end:
        logger.info(
            "Group %s: experiment window not yet complete (ends %s) — skipping",
            group_id, window_end.isoformat(),
        )
        return

    # Welch t-test
    rates_a = [r["engagement_rate"] for r in metrics_a]
    rates_b = [r["engagement_rate"] for r in metrics_b]
    result = stats.ttest_ind(rates_a, rates_b, equal_var=False)
    p_value = float(result.pvalue)
    evaluated_at = datetime.now(timezone.utc).isoformat()

    if p_value < SIGNIFICANCE_THRESHOLD:
        mean_a = sum(rates_a) / len(rates_a)
        mean_b = sum(rates_b) / len(rates_b)
        winning_variant = "A" if mean_a > mean_b else "B"
        winning_pin_id = variant_map[winning_variant]["pin_id"]
        losing_pin_id = variant_map["B" if winning_variant == "A" else "A"]["pin_id"]

        _declare_winner(
            group_id, winning_pin_id, losing_pin_id, p_value, evaluated_at, db_path
        )
        logger.info(
            "Group %s: variant %s wins (mean A=%.4f, mean B=%.4f, p=%.6f)",
            group_id, winning_variant, mean_a, mean_b, p_value,
        )
    else:
        logger.info(
            "No significant winner yet for group %s (p=%.4f)", group_id, p_value
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_ab_engine(db_path: str = database.DB_PATH) -> None:
    """Evaluate all A/B variant groups and write winners to ab_variants."""
    database.init_db(db_path=db_path)

    with database.get_conn(db_path) as conn:
        rows = conn.execute(
            "SELECT DISTINCT variant_group FROM ab_variants"
        ).fetchall()
    groups = [r["variant_group"] for r in rows]

    if not groups:
        logger.info("No variant groups found — A/B engine skipped.")
        return

    logger.info("Evaluating %d variant group(s)", len(groups))
    for group_id in groups:
        _evaluate_group(group_id, db_path)
    logger.info("A/B engine complete.")
