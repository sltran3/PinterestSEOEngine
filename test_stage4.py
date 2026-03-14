"""
test_stage4.py — pytest tests for dashboard.py (Stage 4).

Seeds the DB with 2 pins (14 days of metrics each, 3 keywords each) and
one evaluated A/B group, then calls render_dashboard() and asserts outputs.
"""

import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import database
import dashboard


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------

def _ts(days_ago: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days_ago)).isoformat()


def _seed_pin(db_path, pin_id, title, description=""):
    database.upsert_pin(
        {
            "pin_id": pin_id,
            "url": f"https://pinterest.com/pin/{pin_id}/",
            "title": title,
            "description": description,
            "image_url": "",
        },
        db_path=db_path,
    )


def _seed_metrics(db_path, pin_id, rates, start_days_ago=15):
    for i, rate in enumerate(rates):
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


def _seed_keywords(db_path, pin_id, keywords):
    """Insert keyword rows directly (bypasses Playwright trend scraper)."""
    scored_at = _ts(1)
    with database.get_conn(db_path) as conn:
        for kw, tfidf, trend_vol, health in keywords:
            conn.execute(
                """
                INSERT OR IGNORE INTO keywords
                    (pin_id, keyword, tfidf, trend_vol, health, scored_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (pin_id, kw, tfidf, trend_vol, health, scored_at),
            )


def _seed_ab_group(db_path, pin_a_id, pin_b_id, group_id, winner_pin_id):
    """Insert ab_variant rows and mark the winner directly."""
    database.insert_ab_variant(
        {"pin_id": pin_a_id, "variant_group": group_id, "variant": "A"},
        db_path=db_path,
    )
    database.insert_ab_variant(
        {"pin_id": pin_b_id, "variant_group": group_id, "variant": "B"},
        db_path=db_path,
    )
    evaluated_at = _ts(0)
    with database.get_conn(db_path) as conn:
        conn.execute(
            "UPDATE ab_variants SET winner=1, p_value=0.001, evaluated_at=? "
            "WHERE pin_id=? AND variant_group=?",
            (evaluated_at, winner_pin_id, group_id),
        )
        loser_id = pin_b_id if winner_pin_id == pin_a_id else pin_a_id
        conn.execute(
            "UPDATE ab_variants SET winner=0, p_value=0.001, evaluated_at=? "
            "WHERE pin_id=? AND variant_group=?",
            (evaluated_at, loser_id, group_id),
        )


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------

@pytest.fixture()
def seeded_db(tmp_path):
    path = str(tmp_path / "test_stage4.db")
    database.init_db(db_path=path)

    _seed_pin(path, "pin_alpha", "Minimalist Home Decor Ideas for 2024")
    _seed_pin(path, "pin_beta",  "Healthy Smoothie Bowl Recipes")

    # 14 daily data points starting 15 days ago → window elapsed
    _seed_metrics(path, "pin_alpha", [15.0 + i * 0.2 for i in range(14)])
    _seed_metrics(path, "pin_beta",  [8.0  + i * 0.1 for i in range(14)])

    # 3 keywords per pin
    _seed_keywords(path, "pin_alpha", [
        ("minimalist", 0.45, 80.0, 0.66),
        ("decor",      0.35, 70.0, 0.56),
        ("home",       0.30, 90.0, 0.66),
    ])
    _seed_keywords(path, "pin_beta", [
        ("smoothie",  0.50, 72.0, 0.632),
        ("healthy",   0.40, 88.0, 0.688),
        ("breakfast", 0.30, 65.0, 0.51),
    ])

    # One A/B group — pin_alpha wins
    _seed_ab_group(path, "pin_alpha", "pin_beta", "group_01", "pin_alpha")

    return path


# ---------------------------------------------------------------------------
# Output path fixture
# ---------------------------------------------------------------------------

@pytest.fixture()
def out_dir(tmp_path):
    return str(tmp_path / "output")


# ---------------------------------------------------------------------------
# File creation tests
# ---------------------------------------------------------------------------

class TestOutputFiles:
    def test_dashboard_png_created(self, seeded_db, out_dir):
        dashboard.render_dashboard(db_path=seeded_db, output_dir=out_dir)
        assert (Path(out_dir) / "dashboard.png").exists()

    def test_dashboard_csv_created(self, seeded_db, out_dir):
        dashboard.render_dashboard(db_path=seeded_db, output_dir=out_dir)
        assert (Path(out_dir) / "dashboard_summary.csv").exists()

    def test_png_is_nonempty(self, seeded_db, out_dir):
        dashboard.render_dashboard(db_path=seeded_db, output_dir=out_dir)
        assert (Path(out_dir) / "dashboard.png").stat().st_size > 0

    def test_output_dir_created_if_missing(self, seeded_db, tmp_path):
        deep_dir = str(tmp_path / "nested" / "output")
        dashboard.render_dashboard(db_path=seeded_db, output_dir=deep_dir)
        assert Path(deep_dir).is_dir()


# ---------------------------------------------------------------------------
# CSV content tests
# ---------------------------------------------------------------------------

class TestCSVContent:
    def _read_csv(self, out_dir):
        with open(Path(out_dir) / "dashboard_summary.csv", newline="") as f:
            return list(csv.DictReader(f))

    def test_csv_row_count(self, seeded_db, out_dir):
        """2 pins × 3 keywords = 6 data rows."""
        dashboard.render_dashboard(db_path=seeded_db, output_dir=out_dir)
        rows = self._read_csv(out_dir)
        assert len(rows) == 6

    def test_csv_has_required_columns(self, seeded_db, out_dir):
        dashboard.render_dashboard(db_path=seeded_db, output_dir=out_dir)
        rows = self._read_csv(out_dir)
        expected = {"pin_id", "pin_title", "keyword", "tfidf", "trend_vol", "health", "mean_engagement_rate"}
        assert expected == set(rows[0].keys())

    def test_csv_pin_ids_match_seeded_pins(self, seeded_db, out_dir):
        dashboard.render_dashboard(db_path=seeded_db, output_dir=out_dir)
        rows = self._read_csv(out_dir)
        pin_ids = {r["pin_id"] for r in rows}
        assert pin_ids == {"pin_alpha", "pin_beta"}

    def test_csv_title_truncated_to_40_chars(self, seeded_db, out_dir):
        dashboard.render_dashboard(db_path=seeded_db, output_dir=out_dir)
        rows = self._read_csv(out_dir)
        for row in rows:
            assert len(row["pin_title"]) <= dashboard.TITLE_MAX_CHARS

    def test_csv_health_scores_are_numeric(self, seeded_db, out_dir):
        dashboard.render_dashboard(db_path=seeded_db, output_dir=out_dir)
        rows = self._read_csv(out_dir)
        for row in rows:
            assert float(row["health"]) >= 0.0

    def test_csv_mean_engagement_rate_nonzero(self, seeded_db, out_dir):
        dashboard.render_dashboard(db_path=seeded_db, output_dir=out_dir)
        rows = self._read_csv(out_dir)
        for row in rows:
            assert float(row["mean_engagement_rate"]) > 0.0


# ---------------------------------------------------------------------------
# Edge case: empty DB
# ---------------------------------------------------------------------------

class TestEmptyDB:
    def test_no_crash_on_empty_db(self, tmp_path):
        path = str(tmp_path / "empty.db")
        out = str(tmp_path / "out")
        database.init_db(db_path=path)
        dashboard.render_dashboard(db_path=path, output_dir=out)
        # No output files expected — function exits early with a warning
        assert not (Path(out) / "dashboard.png").exists()

    def test_no_ab_data_renders_placeholder(self, tmp_path):
        path = str(tmp_path / "no_ab.db")
        out = str(tmp_path / "out")
        database.init_db(db_path=path)
        _seed_pin(path, "pin1", "Test Pin")
        _seed_metrics(path, "pin1", [10.0] * 7)
        dashboard.render_dashboard(db_path=path, output_dir=out)
        assert (Path(out) / "dashboard.png").exists()


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------

class TestIdempotency:
    def test_rerender_overwrites_files(self, seeded_db, out_dir):
        dashboard.render_dashboard(db_path=seeded_db, output_dir=out_dir)
        mtime_png_1 = (Path(out_dir) / "dashboard.png").stat().st_mtime
        mtime_csv_1 = (Path(out_dir) / "dashboard_summary.csv").stat().st_mtime

        import time; time.sleep(0.05)
        dashboard.render_dashboard(db_path=seeded_db, output_dir=out_dir)
        mtime_png_2 = (Path(out_dir) / "dashboard.png").stat().st_mtime
        mtime_csv_2 = (Path(out_dir) / "dashboard_summary.csv").stat().st_mtime

        assert mtime_png_2 >= mtime_png_1
        assert mtime_csv_2 >= mtime_csv_1
