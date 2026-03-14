"""
test_stage2.py — pytest tests for keyword_scorer.py (Stage 2).

Playwright trend scraping is replaced with a fixed mock so tests are
hermetic and fast. Uses a real temp-file SQLite DB.
"""

import asyncio
import pytest

import database
import keyword_scorer


# ---------------------------------------------------------------------------
# Sample corpus
# ---------------------------------------------------------------------------

PINS = [
    {
        "pin_id": "pin001",
        "url": "https://pinterest.com/pin/pin001/",
        "title": "Minimalist living room",
        "description": (
            "minimalist living room decor ideas with neutral palette "
            "simple furniture and clean lines for modern home design"
        ),
        "image_url": "",
    },
    {
        "pin_id": "pin002",
        "url": "https://pinterest.com/pin/pin002/",
        "title": "Healthy smoothie bowls",
        "description": (
            "healthy smoothie bowl recipes with fresh fruit toppings "
            "granola seeds and almond butter for nutritious breakfast ideas"
        ),
        "image_url": "",
    },
    {
        "pin_id": "pin003",
        "url": "https://pinterest.com/pin/pin003/",
        "title": "Outdoor garden layout",
        "description": (
            "small outdoor garden layout ideas raised beds container plants "
            "vertical garden design and seasonal flower arrangement tips"
        ),
        "image_url": "",
    },
]

# Fixed trend volumes returned by the mock (keyword → 0-100 float)
MOCK_TREND_VOLUMES: dict[str, float] = {
    # pin001 keywords
    "minimalist": 80.0,
    "living": 60.0,
    "room": 55.0,
    "decor": 70.0,
    "neutral": 40.0,
    "palette": 30.0,
    "simple": 50.0,
    "furniture": 65.0,
    "clean": 45.0,
    "lines": 20.0,
    "modern": 75.0,
    "home": 90.0,
    "design": 85.0,
    # pin002 keywords
    "healthy": 88.0,
    "smoothie": 72.0,
    "bowl": 55.0,
    "recipes": 60.0,
    "fresh": 50.0,
    "fruit": 65.0,
    "toppings": 30.0,
    "granola": 45.0,
    "seeds": 25.0,
    "almond": 40.0,
    "butter": 35.0,
    "nutritious": 38.0,
    "breakfast": 70.0,
    "ideas": 55.0,
    # pin003 keywords
    "outdoor": 68.0,
    "garden": 80.0,
    "layout": 42.0,
    "raised": 35.0,
    "beds": 40.0,
    "container": 30.0,
    "plants": 75.0,
    "vertical": 50.0,
    "seasonal": 45.0,
    "flower": 70.0,
    "arrangement": 38.0,
    "tips": 55.0,
    "small": 60.0,
}


async def _mock_fetcher(page, keyword: str, cache: dict) -> float:
    """Return fixed volumes; falls back to 0.0 for unknown keywords."""
    if keyword in cache:
        return cache[keyword]
    vol = MOCK_TREND_VOLUMES.get(keyword, 0.0)
    cache[keyword] = vol
    return vol


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------

@pytest.fixture()
def db_path(tmp_path):
    path = str(tmp_path / "test_stage2.db")
    database.init_db(db_path=path)
    for pin in PINS:
        database.upsert_pin(pin, db_path=path)
    return path


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestKeywordScorerPopulatesTable:
    def test_keywords_table_populated(self, db_path):
        keyword_scorer.run_keyword_scoring(db_path=db_path, trend_fetcher=_mock_fetcher)
        rows = database.get_all_keywords(db_path=db_path)
        assert len(rows) > 0

    def test_every_pin_has_keywords(self, db_path):
        keyword_scorer.run_keyword_scoring(db_path=db_path, trend_fetcher=_mock_fetcher)
        for pin in PINS:
            rows = database.get_keywords_for_pin(pin["pin_id"], db_path=db_path)
            assert len(rows) > 0, f"pin {pin['pin_id']} has no keywords"

    def test_at_most_top_n_keywords_per_pin(self, db_path):
        keyword_scorer.run_keyword_scoring(db_path=db_path, trend_fetcher=_mock_fetcher)
        for pin in PINS:
            rows = database.get_keywords_for_pin(pin["pin_id"], db_path=db_path)
            assert len(rows) <= keyword_scorer.TOP_N_KEYWORDS


class TestHealthScoreValues:
    def test_health_score_formula(self, db_path):
        """health = tfidf * 0.4 + (trend_vol / 100) * 0.6"""
        keyword_scorer.run_keyword_scoring(db_path=db_path, trend_fetcher=_mock_fetcher)
        rows = database.get_all_keywords(db_path=db_path)
        for row in rows:
            expected = keyword_scorer.compute_health(row["tfidf"], row["trend_vol"])
            assert row["health"] == pytest.approx(expected, rel=1e-5), (
                f"health mismatch for '{row['keyword']}': "
                f"got {row['health']}, expected {expected}"
            )

    def test_health_score_range(self, db_path):
        keyword_scorer.run_keyword_scoring(db_path=db_path, trend_fetcher=_mock_fetcher)
        rows = database.get_all_keywords(db_path=db_path)
        for row in rows:
            assert 0.0 <= row["health"] <= 1.0, (
                f"health out of [0,1] for '{row['keyword']}': {row['health']}"
            )

    def test_known_health_score(self, db_path):
        """Spot-check one keyword whose tfidf and trend_vol are deterministic."""
        keyword_scorer.run_keyword_scoring(db_path=db_path, trend_fetcher=_mock_fetcher)
        rows = database.get_all_keywords(db_path=db_path)
        by_kw = {r["keyword"]: r for r in rows}

        # "garden" only appears in pin003 — trend_vol mocked at 80.0
        if "garden" in by_kw:
            row = by_kw["garden"]
            expected = keyword_scorer.compute_health(row["tfidf"], 80.0)
            assert row["health"] == pytest.approx(expected, rel=1e-5)

    def test_zero_trend_vol_uses_only_tfidf(self):
        """When trend_vol is 0, health equals tfidf * TFIDF_WEIGHT."""
        h = keyword_scorer.compute_health(tfidf=0.5, trend_vol=0.0)
        assert h == pytest.approx(0.5 * keyword_scorer.TFIDF_WEIGHT, rel=1e-6)

    def test_full_trend_vol_contribution(self):
        """When tfidf is 0, health equals TREND_WEIGHT."""
        h = keyword_scorer.compute_health(tfidf=0.0, trend_vol=100.0)
        assert h == pytest.approx(keyword_scorer.TREND_WEIGHT, rel=1e-6)


class TestFullRefresh:
    def test_rerun_replaces_existing_keywords(self, db_path):
        """Second run should produce the same count, not double-insert."""
        keyword_scorer.run_keyword_scoring(db_path=db_path, trend_fetcher=_mock_fetcher)
        first_count = len(database.get_all_keywords(db_path=db_path))

        keyword_scorer.run_keyword_scoring(db_path=db_path, trend_fetcher=_mock_fetcher)
        second_count = len(database.get_all_keywords(db_path=db_path))

        assert first_count == second_count

    def test_stale_keywords_removed_on_refresh(self, db_path):
        """If a pin's description changes, old keywords should not linger."""
        keyword_scorer.run_keyword_scoring(db_path=db_path, trend_fetcher=_mock_fetcher)
        first_kws = {
            r["keyword"]
            for r in database.get_keywords_for_pin("pin001", db_path=db_path)
        }

        # Update pin001 with a completely different description
        database.upsert_pin(
            {
                "pin_id": "pin001",
                "url": "https://pinterest.com/pin/pin001/",
                "title": "Different",
                "description": "yoga meditation wellness mindfulness breathing",
                "image_url": "",
            },
            db_path=db_path,
        )
        keyword_scorer.run_keyword_scoring(db_path=db_path, trend_fetcher=_mock_fetcher)
        second_kws = {
            r["keyword"]
            for r in database.get_keywords_for_pin("pin001", db_path=db_path)
        }

        assert second_kws != first_kws


class TestTrendCache:
    def test_cache_prevents_duplicate_fetches(self, db_path):
        """Each unique keyword should be fetched exactly once."""
        call_log: list[str] = []

        async def counting_fetcher(page, keyword: str, cache: dict) -> float:
            if keyword not in cache:
                call_log.append(keyword)
            return await _mock_fetcher(page, keyword, cache)

        keyword_scorer.run_keyword_scoring(
            db_path=db_path, trend_fetcher=counting_fetcher
        )
        # No keyword should appear more than once in the call log
        assert len(call_log) == len(set(call_log)), (
            f"Duplicate fetches detected: {[k for k in call_log if call_log.count(k) > 1]}"
        )


class TestEdgeCases:
    def test_pin_with_empty_description_produces_no_keywords(self, tmp_path):
        path = str(tmp_path / "empty_desc.db")
        database.init_db(db_path=path)
        database.upsert_pin(
            {
                "pin_id": "empty1",
                "url": "https://pinterest.com/pin/empty1/",
                "title": "No description",
                "description": "",
                "image_url": "",
            },
            db_path=path,
        )
        keyword_scorer.run_keyword_scoring(db_path=path, trend_fetcher=_mock_fetcher)
        rows = database.get_keywords_for_pin("empty1", db_path=path)
        assert rows == []

    def test_scored_at_is_stored(self, db_path):
        keyword_scorer.run_keyword_scoring(db_path=db_path, trend_fetcher=_mock_fetcher)
        rows = database.get_all_keywords(db_path=db_path)
        for row in rows:
            assert row["scored_at"], f"scored_at missing for '{row['keyword']}'"
