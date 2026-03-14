"""api.py — FastAPI server for the Pinterest SEO dashboard.

Reads from pinterest_seo.db via database.get_conn() and exposes JSON endpoints.
CORS is enabled for http://localhost:5173 (Vite dev server).
Pipeline runs are dispatched to a background thread; state lives in-process only.
"""

import re
import threading
from datetime import datetime, timezone
from typing import Optional

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import database
import pipeline

# ── App setup ──────────────────────────────────────────────────────────────────

app = FastAPI(title="Pinterest SEO API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Pipeline state (in-memory, process lifetime only) ─────────────────────────

_pipeline_state: dict = {
    "status": "idle",   # idle | running | success | error
    "last_run": None,
    "error": None,
}
_pipeline_lock = threading.Lock()

# ── Helpers ───────────────────────────────────────────────────────────────────


def _round_floats(d: dict) -> dict:
    """Round float values to 4 decimal places."""
    return {k: round(v, 4) if isinstance(v, float) else v for k, v in d.items()}


def _extract_pin_id(url: str) -> Optional[str]:
    """Extract numeric pin id from a Pinterest URL."""
    m = re.search(r"/pin/(\d+)", url)
    return m.group(1) if m else None


# ── Pins ──────────────────────────────────────────────────────────────────────


class PinCreateRequest(BaseModel):
    url: str


@app.get("/api/pins")
def list_pins():
    """Return all tracked pins."""
    return [_round_floats(p) for p in database.get_pins()]


@app.post("/api/pins", status_code=201)
def add_pin(body: PinCreateRequest):
    """Add a pin by URL, extracting the pin_id from the path."""
    pin_id = _extract_pin_id(body.url)
    if not pin_id:
        raise HTTPException(422, detail="Could not extract pin_id from URL")
    database.upsert_pin({
        "pin_id": pin_id,
        "url": body.url,
        "title": "",
        "description": "",
        "image_url": "",
    })
    pins = database.get_pins()
    pin = next((p for p in pins if p["pin_id"] == pin_id), None)
    return _round_floats(pin) if pin else {"pin_id": pin_id}


@app.delete("/api/pins/{pin_id}", status_code=204)
def delete_pin(pin_id: str):
    """Delete a pin and all its associated data."""
    with database.get_conn() as conn:
        row = conn.execute("SELECT 1 FROM pins WHERE pin_id = ?", (pin_id,)).fetchone()
        if not row:
            raise HTTPException(404, detail="Pin not found")
        # Delete in FK dependency order
        conn.execute("DELETE FROM pin_keywords WHERE pin_id = ?", (pin_id,))
        conn.execute("DELETE FROM keywords WHERE pin_id = ?", (pin_id,))
        conn.execute("DELETE FROM pin_metrics WHERE pin_id = ?", (pin_id,))
        conn.execute("DELETE FROM ab_variants WHERE pin_id = ?", (pin_id,))
        conn.execute("DELETE FROM pins WHERE pin_id = ?", (pin_id,))


# ── Metrics ───────────────────────────────────────────────────────────────────


@app.get("/api/metrics/summary")
def metrics_summary():
    """Return aggregate stats across all pins."""
    pins = database.get_pins()
    if not pins:
        return {
            "total_pins": 0,
            "avg_engagement_rate": 0.0,
            "total_impressions": 0,
            "total_saves": 0,
            "best_pin": None,
        }

    total_impressions = 0
    total_saves = 0
    all_rates: list[float] = []
    best_pin = None
    best_rate = -1.0

    for pin in pins:
        metrics = database.get_metrics_for_pin(pin["pin_id"])
        if metrics:
            rates = [m["engagement_rate"] for m in metrics]
            avg = sum(rates) / len(rates)
            all_rates.extend(rates)
            total_impressions += sum(m["impressions"] for m in metrics)
            total_saves += sum(m["saves"] for m in metrics)
            if avg > best_rate:
                best_rate = avg
                best_pin = pin

    return {
        "total_pins": len(pins),
        "avg_engagement_rate": round(sum(all_rates) / len(all_rates), 4) if all_rates else 0.0,
        "total_impressions": total_impressions,
        "total_saves": total_saves,
        "best_pin": _round_floats(best_pin) if best_pin else None,
    }


@app.get("/api/pins/{pin_id}/metrics")
def pin_metrics(pin_id: str):
    """Return time-series metrics for one pin (scraped_at ASC)."""
    with database.get_conn() as conn:
        row = conn.execute("SELECT 1 FROM pins WHERE pin_id = ?", (pin_id,)).fetchone()
    if not row:
        raise HTTPException(404, detail="Pin not found")
    metrics = database.get_metrics_for_pin(pin_id)
    return [_round_floats(m) for m in metrics]


# ── Keywords ──────────────────────────────────────────────────────────────────


@app.get("/api/keywords")
def list_keywords():
    """Return top 15 keywords by health score with pin title joined."""
    with database.get_conn() as conn:
        rows = conn.execute("""
            SELECT k.id, k.pin_id, k.keyword, k.tfidf, k.trend_vol, k.health, k.scored_at,
                   p.title AS pin_title
            FROM keywords k
            JOIN pins p ON p.pin_id = k.pin_id
            ORDER BY k.health DESC
            LIMIT 15
        """).fetchall()
    return [_round_floats(dict(r)) for r in rows]


# ── A/B variants ──────────────────────────────────────────────────────────────


class ABCreateRequest(BaseModel):
    pin_id: str
    variant_group: str
    variant: str          # 'A' or 'B'
    title: Optional[str] = None
    description: Optional[str] = None


@app.get("/api/ab")
def list_ab():
    """Return all A/B groups with per-variant mean engagement rate."""
    variants = database.get_all_ab_variants()
    groups: dict[str, list] = {}
    for v in variants:
        metrics = database.get_metrics_for_pin(v["pin_id"])
        rates = [m["engagement_rate"] for m in metrics]
        mean_rate = round(sum(rates) / len(rates), 4) if rates else 0.0
        entry = {**_round_floats(v), "mean_engagement_rate": mean_rate}
        groups.setdefault(v["variant_group"], []).append(entry)
    return [{"variant_group": g, "variants": vs} for g, vs in groups.items()]


@app.post("/api/ab", status_code=201)
def create_ab(body: ABCreateRequest):
    """Register a new A/B variant. Optionally updates the pin's title/description."""
    if body.variant not in ("A", "B"):
        raise HTTPException(422, detail="variant must be 'A' or 'B'")

    with database.get_conn() as conn:
        row = conn.execute("SELECT * FROM pins WHERE pin_id = ?", (body.pin_id,)).fetchone()
        if not row:
            raise HTTPException(404, detail="Pin not found")
        if body.title or body.description:
            new_title = body.title if body.title else row["title"]
            new_desc = body.description if body.description else row["description"]
            conn.execute(
                "UPDATE pins SET title = ?, description = ? WHERE pin_id = ?",
                (new_title, new_desc, body.pin_id),
            )

    database.insert_ab_variant({
        "pin_id": body.pin_id,
        "variant_group": body.variant_group,
        "variant": body.variant,
    })
    return {"ok": True}


# ── Pipeline control ──────────────────────────────────────────────────────────


def _run_pipeline_thread() -> None:
    """Target for the background pipeline thread."""
    with _pipeline_lock:
        _pipeline_state["status"] = "running"
        _pipeline_state["error"] = None
    try:
        pipeline.run_pipeline()
        with _pipeline_lock:
            _pipeline_state["status"] = "success"
            _pipeline_state["last_run"] = datetime.now(timezone.utc).isoformat()
    except Exception as exc:
        with _pipeline_lock:
            _pipeline_state["status"] = "error"
            _pipeline_state["error"] = str(exc)
            _pipeline_state["last_run"] = datetime.now(timezone.utc).isoformat()


@app.post("/api/run")
def trigger_run():
    """Start the pipeline in a background thread. Returns 409 if already running."""
    with _pipeline_lock:
        if _pipeline_state["status"] == "running":
            raise HTTPException(409, detail="Pipeline is already running")
    thread = threading.Thread(target=_run_pipeline_thread, daemon=True)
    thread.start()
    return {"ok": True}


@app.get("/api/run/status")
def run_status():
    """Return current pipeline status."""
    with _pipeline_lock:
        return dict(_pipeline_state)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
