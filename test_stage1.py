"""
test_stage1.py — pytest tests for database.py (Stage 1).

Uses a real temp-file SQLite DB (not :memory:) to exercise all functions
through the public API.
"""

import os
import tempfile

import pytest

import database


# ---------------------------------------------------------------------------
# Fixture: isolated DB file for each test
# ---------------------------------------------------------------------------

@pytest.fixture()
def db_path(tmp_path):
    path = str(tmp_path / "test_pinterest_seo.db")
    database.init_db(db_path=path)
    return path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pin(pin_id="abc123", url="https://pinterest.com/pin/abc123/",
         title="My Pin", description="A great pin", image_url="https://i.pinimg.com/img.jpg"):
    return dict(
        pin_id=pin_id,
        url=url,
        title=title,
        description=description,
        image_url=image_url,
    )


def _metrics(pin_id="abc123", scraped_at="2024-01-01T00:00:00+00:00",
             impressions=1000, saves=50, clicks=30, engagement_rate=8.0):
    return dict(
        pin_id=pin_id,
        scraped_at=scraped_at,
        impressions=impressions,
        saves=saves,
        clicks=clicks,
        engagement_rate=engagement_rate,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestSchema:
    def test_all_four_tables_exist(self, db_path):
        with database.get_conn(db_path) as conn:
            rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            ).fetchall()
        names = {r["name"] for r in rows}
        names.discard("sqlite_sequence")
        assert {"pins", "pin_metrics", "keywords", "pin_keywords", "ab_variants"} == names


class TestUpsertPin:
    def test_insert(self, db_path):
        database.upsert_pin(_pin(), db_path=db_path)
        pins = database.get_pins(db_path=db_path)
        assert len(pins) == 1
        assert pins[0]["pin_id"] == "abc123"
        assert pins[0]["title"] == "My Pin"

    def test_update_title_on_conflict(self, db_path):
        database.upsert_pin(_pin(title="Original"), db_path=db_path)
        database.upsert_pin(_pin(title="Updated"), db_path=db_path)
        pins = database.get_pins(db_path=db_path)
        assert len(pins) == 1
        assert pins[0]["title"] == "Updated"

    def test_update_description_on_conflict(self, db_path):
        database.upsert_pin(_pin(description="First"), db_path=db_path)
        database.upsert_pin(_pin(description="Second"), db_path=db_path)
        pins = database.get_pins(db_path=db_path)
        assert pins[0]["description"] == "Second"

    def test_update_image_url_on_conflict(self, db_path):
        database.upsert_pin(_pin(image_url="https://old.jpg"), db_path=db_path)
        database.upsert_pin(_pin(image_url="https://new.jpg"), db_path=db_path)
        pins = database.get_pins(db_path=db_path)
        assert pins[0]["image_url"] == "https://new.jpg"


class TestInsertMetrics:
    def test_stores_correct_engagement_rate(self, db_path):
        database.upsert_pin(_pin(), db_path=db_path)
        database.insert_metrics(_metrics(engagement_rate=8.0), db_path=db_path)
        rows = database.get_metrics_for_pin("abc123", db_path=db_path)
        assert len(rows) == 1
        assert rows[0]["engagement_rate"] == pytest.approx(8.0)

    def test_stores_impressions_saves_clicks(self, db_path):
        database.upsert_pin(_pin(), db_path=db_path)
        database.insert_metrics(
            _metrics(impressions=500, saves=25, clicks=10), db_path=db_path
        )
        rows = database.get_metrics_for_pin("abc123", db_path=db_path)
        assert rows[0]["impressions"] == 500
        assert rows[0]["saves"] == 25
        assert rows[0]["clicks"] == 10


class TestDuplicateMetrics:
    def test_duplicate_pin_id_scraped_at_silently_ignored(self, db_path):
        database.upsert_pin(_pin(), db_path=db_path)
        m = _metrics(scraped_at="2024-06-01T12:00:00+00:00", engagement_rate=5.0)
        database.insert_metrics(m, db_path=db_path)
        # Second insert with same (pin_id, scraped_at) — different values but must be ignored
        database.insert_metrics({**m, "engagement_rate": 99.0}, db_path=db_path)
        rows = database.get_metrics_for_pin("abc123", db_path=db_path)
        assert len(rows) == 1
        assert rows[0]["engagement_rate"] == pytest.approx(5.0)


class TestGetMetricsOrdering:
    def test_returns_rows_ordered_oldest_first(self, db_path):
        database.upsert_pin(_pin(), db_path=db_path)
        timestamps = [
            "2024-03-01T00:00:00+00:00",
            "2024-01-01T00:00:00+00:00",
            "2024-02-01T00:00:00+00:00",
        ]
        for i, ts in enumerate(timestamps):
            database.insert_metrics(
                _metrics(scraped_at=ts, engagement_rate=float(i)), db_path=db_path
            )
        rows = database.get_metrics_for_pin("abc123", db_path=db_path)
        returned_ts = [r["scraped_at"] for r in rows]
        assert returned_ts == sorted(returned_ts)

    def test_returns_only_rows_for_requested_pin(self, db_path):
        database.upsert_pin(_pin(pin_id="pin1", url="https://pinterest.com/pin/pin1/"), db_path=db_path)
        database.upsert_pin(_pin(pin_id="pin2", url="https://pinterest.com/pin/pin2/"), db_path=db_path)
        database.insert_metrics(_metrics(pin_id="pin1", scraped_at="2024-01-01T00:00:00+00:00"), db_path=db_path)
        database.insert_metrics(_metrics(pin_id="pin2", scraped_at="2024-01-01T00:00:00+00:00"), db_path=db_path)
        rows = database.get_metrics_for_pin("pin1", db_path=db_path)
        assert all(r["pin_id"] == "pin1" for r in rows)
        assert len(rows) == 1
