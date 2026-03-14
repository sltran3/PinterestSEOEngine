"""
dashboard.py — Matplotlib dashboard renderer.

Produces:
  dashboard.png          — three-subplot figure saved to output_dir
  dashboard_summary.csv  — keyword-level summary table saved to output_dir

Entry point for pipeline.py: render_dashboard()
"""

import csv
import logging
from datetime import datetime
from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # headless — must be set before importing pyplot
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec

import database

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
FIGURE_SIZE = (18, 12)
FIGURE_DPI = 150
TOP_N_KEYWORDS = 15
TITLE_MAX_CHARS = 40
WINNER_EDGE_COLOR = "#DAA520"
WINNER_EDGE_WIDTH = 2

OUTPUT_PNG = "dashboard.png"
OUTPUT_CSV = "dashboard_summary.csv"

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_ts(ts_str: str) -> datetime:
    """Parse ISO-8601 string to a naive datetime (matplotlib-friendly)."""
    dt = datetime.fromisoformat(ts_str)
    # Strip timezone so matplotlib date locators work without offset issues
    return dt.replace(tzinfo=None)


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _build_pin_colors(pins: list[dict]) -> dict[str, str]:
    """Map pin_id → color string drawn from the default prop cycle."""
    cycle = plt.rcParams["axes.prop_cycle"].by_key()["color"]
    return {p["pin_id"]: cycle[i % len(cycle)] for i, p in enumerate(pins)}


# ---------------------------------------------------------------------------
# Subplot 1 — Engagement rate over time
# ---------------------------------------------------------------------------

def _plot_engagement(ax: plt.Axes, pins: list[dict], pin_colors: dict, db_path: str) -> None:
    ax.set_title("Engagement Rate Over Time", fontweight="bold")
    ax.set_ylabel("Engagement rate (%)")

    for pin in pins:
        metrics = database.get_metrics_for_pin(pin["pin_id"], db_path=db_path)
        if not metrics:
            continue
        dates = [_parse_ts(m["scraped_at"]) for m in metrics]
        rates = [m["engagement_rate"] for m in metrics]
        label = (pin.get("title") or pin["pin_id"])[:TITLE_MAX_CHARS]
        ax.plot(dates, rates, label=label, color=pin_colors[pin["pin_id"]], marker="o", markersize=3)

    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.setp(ax.get_xticklabels(), rotation=30, ha="right")
    ax.legend(loc="upper left", fontsize=8)
    ax.grid(axis="y", linestyle="--", alpha=0.4)


# ---------------------------------------------------------------------------
# Subplot 2 — Keyword health scores
# ---------------------------------------------------------------------------

def _plot_keywords(ax: plt.Axes, pin_colors: dict, db_path: str) -> None:
    ax.set_title("Top 15 Keyword Health Scores", fontweight="bold")
    ax.set_xlabel("Keyword health score (TF-IDF x trend volume)")

    all_keywords = database.get_all_keywords(db_path=db_path)
    all_keywords.sort(key=lambda k: k["health"], reverse=True)
    top = all_keywords[:TOP_N_KEYWORDS]

    if not top:
        ax.text(0.5, 0.5, "No keyword data yet", transform=ax.transAxes,
                ha="center", va="center", color="grey")
        return

    # Reverse so highest-ranked bar appears at the top
    labels = [k["keyword"] for k in reversed(top)]
    healths = [k["health"] for k in reversed(top)]
    colors = [pin_colors.get(k["pin_id"], "#888888") for k in reversed(top)]

    bars = ax.barh(labels, healths, color=colors)
    ax.bar_label(bars, fmt="%.3f", padding=3, fontsize=7)
    ax.set_xlim(0, max(healths) * 1.2 if healths else 1)
    ax.grid(axis="x", linestyle="--", alpha=0.4)


# ---------------------------------------------------------------------------
# Subplot 3 — A/B test results
# ---------------------------------------------------------------------------

def _plot_ab(ax: plt.Axes, pin_colors: dict, db_path: str) -> None:
    ax.set_title("A/B Test Results", fontweight="bold")
    ax.set_ylabel("Mean engagement rate (%)")

    all_variants = database.get_all_ab_variants(db_path=db_path)
    # Only show groups that have been evaluated (winner has been declared)
    evaluated: dict[str, dict[str, dict]] = {}
    for v in all_variants:
        if v.get("evaluated_at"):
            gid = v["variant_group"]
            evaluated.setdefault(gid, {})[v["variant"]] = v

    if not evaluated:
        ax.text(0.5, 0.5, "No evaluated A/B groups yet", transform=ax.transAxes,
                ha="center", va="center", color="grey")
        return

    group_ids = sorted(evaluated.keys())
    bar_width = 0.35
    x_positions = range(len(group_ids))

    for i, gid in enumerate(group_ids):
        variants = evaluated[gid]
        for j, variant_label in enumerate(("A", "B")):
            if variant_label not in variants:
                continue
            v = variants[variant_label]
            metrics = database.get_metrics_for_pin(v["pin_id"], db_path=db_path)
            mean_rate = _mean([m["engagement_rate"] for m in metrics])

            bar_x = i + (j - 0.5) * bar_width
            is_winner = v["winner"] == 1
            color = pin_colors.get(v["pin_id"], "#888888")

            bar = ax.bar(
                bar_x, mean_rate, bar_width,
                color=color,
                edgecolor=WINNER_EDGE_COLOR if is_winner else "none",
                linewidth=WINNER_EDGE_WIDTH if is_winner else 0,
                label=f"Variant {variant_label}",
            )
            ax.annotate(
                f"{mean_rate:.2f}",
                xy=(bar_x, mean_rate),
                xytext=(0, 4),
                textcoords="offset points",
                ha="center", va="bottom", fontsize=8,
            )

    ax.set_xticks(list(x_positions))
    ax.set_xticklabels(group_ids, rotation=15, ha="right")

    # Deduplicated legend entries
    handles, labels_seen = [], []
    for handle, label in zip(*ax.get_legend_handles_labels()):
        if label not in labels_seen:
            handles.append(handle)
            labels_seen.append(label)
    ax.legend(handles, labels_seen, fontsize=8)
    ax.grid(axis="y", linestyle="--", alpha=0.4)


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------

def _export_csv(pins: list[dict], db_path: str, output_path: Path) -> None:
    """Write dashboard_summary.csv — one row per keyword per pin."""
    # Pre-compute mean engagement rate per pin
    pin_means: dict[str, float] = {}
    for pin in pins:
        metrics = database.get_metrics_for_pin(pin["pin_id"], db_path=db_path)
        pin_means[pin["pin_id"]] = _mean([m["engagement_rate"] for m in metrics])

    pin_titles: dict[str, str] = {
        p["pin_id"]: (p.get("title") or p["pin_id"])[:TITLE_MAX_CHARS]
        for p in pins
    }

    all_keywords = database.get_all_keywords(db_path=db_path)

    fieldnames = [
        "pin_id", "pin_title", "keyword",
        "tfidf", "trend_vol", "health", "mean_engagement_rate",
    ]

    with output_path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for kw in all_keywords:
            pid = kw["pin_id"]
            writer.writerow({
                "pin_id": pid,
                "pin_title": pin_titles.get(pid, pid),
                "keyword": kw["keyword"],
                "tfidf": round(kw["tfidf"], 6),
                "trend_vol": round(kw["trend_vol"], 2),
                "health": round(kw["health"], 6),
                "mean_engagement_rate": round(pin_means.get(pid, 0.0), 4),
            })

    logger.info("CSV written to %s (%d rows)", output_path, len(all_keywords))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def render_dashboard(
    db_path: str = database.DB_PATH,
    output_dir: str = ".",
) -> None:
    """Build dashboard.png and dashboard_summary.csv in *output_dir*."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    pins = database.get_pins(db_path=db_path)
    if not pins:
        logger.warning("No pins found — dashboard skipped.")
        return

    pin_colors = _build_pin_colors(pins)

    # Layout: full-width time-series on top, keywords + A/B side-by-side below
    fig = plt.figure(figsize=FIGURE_SIZE)
    gs = gridspec.GridSpec(2, 2, figure=fig, height_ratios=[1, 1.1], hspace=0.45, wspace=0.35)

    ax1 = fig.add_subplot(gs[0, :])   # row 0, full width
    ax2 = fig.add_subplot(gs[1, 0])   # row 1, left
    ax3 = fig.add_subplot(gs[1, 1])   # row 1, right

    _plot_engagement(ax1, pins, pin_colors, db_path)
    _plot_keywords(ax2, pin_colors, db_path)
    _plot_ab(ax3, pin_colors, db_path)

    fig.suptitle("Pinterest SEO Dashboard", fontsize=16, fontweight="bold", y=1.01)

    png_path = out / OUTPUT_PNG
    fig.savefig(png_path, dpi=FIGURE_DPI, bbox_inches="tight")
    plt.close(fig)
    logger.info("Dashboard saved to %s", png_path)

    csv_path = out / OUTPUT_CSV
    _export_csv(pins, db_path, csv_path)
