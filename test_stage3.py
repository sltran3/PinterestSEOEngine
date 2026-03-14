"""
test_stage3.py — pytest tests for ab_engine.py (Stage 3).

Uses a real temp-file SQLite DB.  Two variant groups are seeded:
  - "group_clear"  — variant A clearly wins (high vs low engagement)
  - "group_noise"  — no significant difference between A and B

Timestamps are placed 15–1 days in the past so the 14-day experiment
window has already elapsed and the window-check passes without any
clock manipulation.
"""

from datetime import datetime, timedelta, timezone

import pytest

import database
import ab_engine


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts(days_ago: int) -> str:
    """Return a UTC ISO-8601 string for *days_ago* days before now."""
    dt = datetime.now(timezone.utc) - timedelta(days=days_ago)
    return dt.isoformat()


def _seed_pin(db_path: str, pin_id: str, title: str = "") -> None:
    database.upsert_pin(
        {
            "pin_id": pin_id,
            "url": f"https://pinterest.com/pin/{pin_id}/",
            "title": title,
            "description": "",
            "image_url": "",
        },
        db_path=db_path,
    )


def _seed_metrics(
    db_path: str,
    pin_id: str,
    engagement_rates: list[float],
    start_days_ago: int = 15,
) -> None:
    """Insert one metrics row per rate, spaced one day apart, starting
    *start_days_ago* days in the past."""
    for i, rate in enumerate(engagement_rates):
        database.insert_metrics(
            {
                "pin_id": pin_id,
                "scraped_at": _ts(start_days_ago - i),
                "impressions": 1000,
                "saves": int(rate * 5),
                "clicks": int(rate * 5),
                "engagement_rate": rate,
            },
            db_path=db_path,
        )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

# Engagement series: A dominates B unambiguously
_CLEAR_A = [16.0, 17.0, 15.5, 18.0, 16.5, 17.5, 15.0, 16.0, 18.5, 17.0, 16.0, 15.5, 17.0, 16.5]
_CLEAR_B = [2.0,  1.5,  2.5,  1.0,  2.0,  3.0,  1.5,  2.0,  1.0,  2.5,  2.0,  1.5,  1.0,  2.5]

# Engagement series: virtually identical — should not produce a significant result
_NOISE_A = [10.0, 10.5, 10.0, 9.5, 10.0, 10.5, 10.0, 9.5, 10.0, 10.5, 10.0, 9.5, 10.0, 10.5]
_NOISE_B = [10.5, 10.0, 9.5, 10.0, 10.5, 10.0, 9.5, 10.0, 10.5, 10.0, 9.5, 10.0, 10.5, 10.0]


@pytest.fixture()
def db_path(tmp_path):
    path = str(tmp_path / "test_stage3.db")
    database.init_db(db_path=path)

    # Pins
    for pin_id in ["clear_a", "clear_b", "noise_a", "noise_b"]:
        _seed_pin(path, pin_id)

    # Variants
    for row in [
        {"pin_id": "clear_a", "variant_group": "group_clear", "variant": "A"},
        {"pin_id": "clear_b", "variant_group": "group_clear", "variant": "B"},
        {"pin_id": "noise_a", "variant_group": "group_noise", "variant": "A"},
        {"pin_id": "noise_b", "variant_group": "group_noise", "variant": "B"},
    ]:
        database.insert_ab_variant(row, db_path=path)

    # Metrics — 14 daily data points, starting 15 days ago
    _seed_metrics(path, "clear_a", _CLEAR_A)
    _seed_metrics(path, "clear_b", _CLEAR_B)
    _seed_metrics(path, "noise_a", _NOISE_A)
    _seed_metrics(path, "noise_b", _NOISE_B)

    return path


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestClearWinner:
    def test_winner_is_set_for_clear_group(self, db_path):
        ab_engine.run_ab_engine(db_path=db_path)
        variants = database.get_ab_variants_for_group("group_clear", db_path=db_path)
        winners = [v for v in variants if v["winner"] == 1]
        assert len(winners) == 1

    def test_variant_a_is_the_winner(self, db_path):
        ab_engine.run_ab_engine(db_path=db_path)
        variants = database.get_ab_variants_for_group("group_clear", db_path=db_path)
        variant_map = {v["variant"]: v for v in variants}
        assert variant_map["A"]["winner"] == 1
        assert variant_map["B"]["winner"] == 0

    def test_p_value_is_significant(self, db_path):
        ab_engine.run_ab_engine(db_path=db_path)
        variants = database.get_ab_variants_for_group("group_clear", db_path=db_path)
        for v in variants:
            assert v["p_value"] is not None
            assert v["p_value"] < ab_engine.SIGNIFICANCE_THRESHOLD

    def test_evaluated_at_is_set(self, db_path):
        ab_engine.run_ab_engine(db_path=db_path)
        variants = database.get_ab_variants_for_group("group_clear", db_path=db_path)
        for v in variants:
            assert v["evaluated_at"] is not None


class TestNoSignificantWinner:
    def test_winner_unset_for_noise_group(self, db_path):
        ab_engine.run_ab_engine(db_path=db_path)
        variants = database.get_ab_variants_for_group("group_noise", db_path=db_path)
        for v in variants:
            assert v["winner"] == 0, (
                f"Expected winner=0 for '{v['variant']}', got {v['winner']}"
            )

    def test_evaluated_at_unset_for_noise_group(self, db_path):
        """Non-significant groups are not written to at all."""
        ab_engine.run_ab_engine(db_path=db_path)
        variants = database.get_ab_variants_for_group("group_noise", db_path=db_path)
        for v in variants:
            assert v["evaluated_at"] is None


class TestGuards:
    def test_skips_group_with_too_few_observations(self, tmp_path):
        path = str(tmp_path / "few_obs.db")
        database.init_db(db_path=path)
        for pin_id in ["few_a", "few_b"]:
            _seed_pin(path, pin_id)
        database.insert_ab_variant(
            {"pin_id": "few_a", "variant_group": "g_few", "variant": "A"}, db_path=path
        )
        database.insert_ab_variant(
            {"pin_id": "few_b", "variant_group": "g_few", "variant": "B"}, db_path=path
        )
        # Only 3 observations each (below MIN_OBSERVATIONS=7)
        _seed_metrics(path, "few_a", [15.0, 16.0, 14.0])
        _seed_metrics(path, "few_b", [2.0, 3.0, 1.5])

        ab_engine.run_ab_engine(db_path=path)
        variants = database.get_ab_variants_for_group("g_few", db_path=path)
        assert all(v["winner"] == 0 for v in variants)

    def test_skips_group_with_insufficient_time_span(self, tmp_path):
        path = str(tmp_path / "short_span.db")
        database.init_db(db_path=path)
        for pin_id in ["span_a", "span_b"]:
            _seed_pin(path, pin_id)
        database.insert_ab_variant(
            {"pin_id": "span_a", "variant_group": "g_span", "variant": "A"}, db_path=path
        )
        database.insert_ab_variant(
            {"pin_id": "span_b", "variant_group": "g_span", "variant": "B"}, db_path=path
        )
        # 8 observations but all on the same day → span = 0 days
        for pin_id, rate in [("span_a", 15.0), ("span_b", 2.0)]:
            for j in range(8):
                database.insert_metrics(
                    {
                        "pin_id": pin_id,
                        "scraped_at": f"2024-01-01T{j:02d}:00:00+00:00",
                        "impressions": 1000,
                        "saves": 50,
                        "clicks": 30,
                        "engagement_rate": rate,
                    },
                    db_path=path,
                )

        ab_engine.run_ab_engine(db_path=path)
        variants = database.get_ab_variants_for_group("g_span", db_path=path)
        assert all(v["winner"] == 0 for v in variants)

    def test_skips_group_when_window_not_elapsed(self, tmp_path):
        path = str(tmp_path / "recent.db")
        database.init_db(db_path=path)
        for pin_id in ["rec_a", "rec_b"]:
            _seed_pin(path, pin_id)
        database.insert_ab_variant(
            {"pin_id": "rec_a", "variant_group": "g_rec", "variant": "A"}, db_path=path
        )
        database.insert_ab_variant(
            {"pin_id": "rec_b", "variant_group": "g_rec", "variant": "B"}, db_path=path
        )
        # 8 observations starting only 8 days ago → window ends in the future
        _seed_metrics(path, "rec_a", [15.0] * 8, start_days_ago=8)
        _seed_metrics(path, "rec_b", [2.0] * 8,  start_days_ago=8)

        ab_engine.run_ab_engine(db_path=path)
        variants = database.get_ab_variants_for_group("g_rec", db_path=path)
        assert all(v["winner"] == 0 for v in variants)

    def test_empty_variant_table_is_a_noop(self, tmp_path):
        path = str(tmp_path / "empty.db")
        database.init_db(db_path=path)
        ab_engine.run_ab_engine(db_path=path)  # must not raise
        assert database.get_all_ab_variants(db_path=path) == []


class TestIdempotency:
    def test_rerunning_does_not_change_winner(self, db_path):
        ab_engine.run_ab_engine(db_path=db_path)
        variants_first = {
            v["variant"]: v["winner"]
            for v in database.get_ab_variants_for_group("group_clear", db_path=db_path)
        }
        ab_engine.run_ab_engine(db_path=db_path)
        variants_second = {
            v["variant"]: v["winner"]
            for v in database.get_ab_variants_for_group("group_clear", db_path=db_path)
        }
        assert variants_first == variants_second
