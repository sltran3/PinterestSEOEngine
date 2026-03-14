"""
database.py — SQLite schema manager and data access layer.
All other modules must import from this file; raw sqlite3 connections
must never be opened elsewhere.
"""

import sqlite3
import contextlib
from typing import Optional

DB_PATH = 'pinterest_seo.db'

SCHEMA = """
CREATE TABLE IF NOT EXISTS pins (
    pin_id      TEXT PRIMARY KEY,
    url         TEXT NOT NULL,
    title       TEXT DEFAULT '',
    description TEXT DEFAULT '',
    image_url   TEXT DEFAULT '',
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pin_metrics (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    pin_id          TEXT NOT NULL REFERENCES pins(pin_id),
    scraped_at      TIMESTAMP NOT NULL,
    impressions     INTEGER DEFAULT 0,
    saves           INTEGER DEFAULT 0,
    clicks          INTEGER DEFAULT 0,
    engagement_rate REAL DEFAULT 0.0,
    UNIQUE(pin_id, scraped_at)
);

CREATE TABLE IF NOT EXISTS keywords (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    pin_id    TEXT      NOT NULL REFERENCES pins(pin_id),
    keyword   TEXT      NOT NULL,
    tfidf     REAL      DEFAULT 0.0,
    trend_vol REAL      DEFAULT 0.0,
    health    REAL      DEFAULT 0.0,
    scored_at TIMESTAMP NOT NULL,
    UNIQUE(pin_id, keyword)
);

CREATE TABLE IF NOT EXISTS pin_keywords (
    pin_id     TEXT NOT NULL REFERENCES pins(pin_id),
    keyword_id INTEGER NOT NULL REFERENCES keywords(id),
    PRIMARY KEY (pin_id, keyword_id)
);

CREATE TABLE IF NOT EXISTS ab_variants (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    pin_id        TEXT      NOT NULL REFERENCES pins(pin_id),
    variant_group TEXT      NOT NULL,
    variant       TEXT      NOT NULL CHECK(variant IN ('A', 'B')),
    winner        INTEGER   DEFAULT 0,
    p_value       REAL,
    evaluated_at  TIMESTAMP,
    UNIQUE(pin_id, variant_group)
);
"""


@contextlib.contextmanager
def get_conn(db_path: str = DB_PATH):
    """Context manager that yields a configured sqlite3 connection.

    Enables WAL mode and foreign key enforcement. Commits on clean exit,
    rolls back on any exception.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: str = DB_PATH) -> None:
    """Create all tables. Safe to call on every startup."""
    with get_conn(db_path) as conn:
        conn.executescript(SCHEMA)


def upsert_pin(pin: dict, db_path: str = DB_PATH) -> None:
    """Insert a pin or update title/description/image_url on conflict."""
    sql = """
        INSERT INTO pins (pin_id, url, title, description, image_url)
        VALUES (:pin_id, :url, :title, :description, :image_url)
        ON CONFLICT(pin_id) DO UPDATE SET
            title       = excluded.title,
            description = excluded.description,
            image_url   = excluded.image_url
    """
    with get_conn(db_path) as conn:
        conn.execute(sql, pin)


def insert_metrics(metrics: dict, db_path: str = DB_PATH) -> None:
    """Insert a metrics row. Silently ignores duplicate (pin_id, scraped_at)."""
    sql = """
        INSERT OR IGNORE INTO pin_metrics
            (pin_id, scraped_at, impressions, saves, clicks, engagement_rate)
        VALUES
            (:pin_id, :scraped_at, :impressions, :saves, :clicks, :engagement_rate)
    """
    with get_conn(db_path) as conn:
        conn.execute(sql, metrics)


def get_pins(db_path: str = DB_PATH) -> list[dict]:
    """Return all rows from pins as plain dicts."""
    with get_conn(db_path) as conn:
        rows = conn.execute("SELECT * FROM pins").fetchall()
    return [dict(r) for r in rows]


def get_metrics_for_pin(pin_id: str, db_path: str = DB_PATH) -> list[dict]:
    """Return metrics for one pin ordered by scraped_at ASC."""
    sql = """
        SELECT * FROM pin_metrics
        WHERE pin_id = ?
        ORDER BY scraped_at ASC
    """
    with get_conn(db_path) as conn:
        rows = conn.execute(sql, (pin_id,)).fetchall()
    return [dict(r) for r in rows]


def get_keywords_for_pin(pin_id: str, db_path: str = DB_PATH) -> list[dict]:
    """Return keyword rows for one pin ordered by health DESC."""
    sql = """
        SELECT * FROM keywords
        WHERE pin_id = ?
        ORDER BY health DESC
    """
    with get_conn(db_path) as conn:
        rows = conn.execute(sql, (pin_id,)).fetchall()
    return [dict(r) for r in rows]


def get_all_keywords(db_path: str = DB_PATH) -> list[dict]:
    """Return all rows from keywords."""
    with get_conn(db_path) as conn:
        rows = conn.execute("SELECT * FROM keywords").fetchall()
    return [dict(r) for r in rows]


def insert_ab_variant(variant: dict, db_path: str = DB_PATH) -> None:
    """Insert an A/B variant row. Silently ignores duplicate (pin_id, variant_group)."""
    sql = """
        INSERT OR IGNORE INTO ab_variants (pin_id, variant_group, variant)
        VALUES (:pin_id, :variant_group, :variant)
    """
    with get_conn(db_path) as conn:
        conn.execute(sql, variant)


def get_ab_variants_for_group(group_id: str, db_path: str = DB_PATH) -> list[dict]:
    """Return both variant rows for a group ordered by variant (A then B)."""
    sql = """
        SELECT * FROM ab_variants
        WHERE variant_group = ?
        ORDER BY variant ASC
    """
    with get_conn(db_path) as conn:
        rows = conn.execute(sql, (group_id,)).fetchall()
    return [dict(r) for r in rows]


def get_all_ab_variants(db_path: str = DB_PATH) -> list[dict]:
    """Return all rows from ab_variants."""
    with get_conn(db_path) as conn:
        rows = conn.execute("SELECT * FROM ab_variants").fetchall()
    return [dict(r) for r in rows]
