# Pinterest SEO Analytics Engine

A automated analytics pipeline that scrapes your own Pinterest pin performance data daily, scores keywords using TF-IDF, runs A/B tests on pin descriptions, and visualises everything in a dashboard.

Built with Python, Playwright, SQLite, scikit-learn, scipy, and Matplotlib. Optional web dashboard with FastAPI + React.

---

## What it does

Pinterest shows you analytics if you log in manually — but you'd have to check every day and track it yourself. This project automates that entirely.

Every night at 3am it:

1. **Scrapes your pins** — logs into Pinterest as you, visits each pin's analytics page, and saves impressions, saves, clicks, and engagement rate to a local database
2. **Scores your keywords** — analyses your pin descriptions using TF-IDF weighted by Pinterest Trends volume to find which keywords are both distinctive and trending
3. **Evaluates A/B tests** — compares pin variants you've written (different titles or descriptions for the same content) and declares a winner using a statistical significance test
4. **Generates a dashboard** — produces a `dashboard.png` with engagement trends over time, keyword health scores, and A/B test results

> **Important:** This only works on pins you own on a Pinterest Business account. Pinterest only shows analytics data to the pin's owner.

---

## Project structure

```
pinterestSEOproj/
  database.py          # SQLite schema and data access layer
  scraper.py           # Playwright browser automation
  keyword_scorer.py    # TF-IDF keyword analysis
  ab_engine.py         # A/B test statistical evaluation
  dashboard.py         # Matplotlib chart generation
  pipeline.py          # Orchestrates all 4 stages in sequence
  scheduler.py         # APScheduler daily cron runner
  api.py               # FastAPI backend (optional web dashboard)
  frontend/            # React frontend (optional web dashboard)
  requirements.txt
  pinterest_seo.db     # created automatically on first run
  .browser_session/    # created automatically on first login
  logs/
    pipeline.log
    errors.log
```

---

## How it works

### The data flow

```
Pinterest (your account)
        ↓
  Playwright scraper         logs in, intercepts XHR, saves to SQLite
        ↓
  SQLite database            pinterest_seo.db — one new row per pin per day
        ↓
  Keyword scorer             TF-IDF × trend volume = keyword health score
        ↓
  A/B engine                 Welch t-test → declares winner if p < 0.05
        ↓
  Matplotlib dashboard       dashboard.png + dashboard_summary.csv
```

### Why Playwright?

Pinterest builds its pages with JavaScript — the analytics data isn't in the raw HTML. Playwright launches a real Chromium browser, waits for the page to fully load, and intercepts the internal API call Pinterest makes to populate the stats panel. This gives us clean structured JSON directly instead of scraping the DOM.

### Why SQLite?

No server, no setup. SQLite is a single file (`pinterest_seo.db`) that Python can read and write to directly. Every daily run appends a new row to the `metrics` table — after 30 days you have 30 data points per pin, which is enough to see trends.

### How keyword scoring works

TF-IDF finds words that are distinctive to a specific pin compared to all your other pins. A generic word like "recipe" that appears everywhere scores low. A specific phrase like "sourdough discard" that only appears in one pin scores high. That score is then multiplied by the keyword's current search volume from Pinterest Trends. High score = use this keyword more.

### How A/B testing works

You write two versions of a pin (same image, different title or description) and tag them as the same `variant_group`. The engine collects engagement rate data for both over 14 days and runs a Welch t-test. If `p < 0.05` it has statistical confidence that one version is genuinely better and marks it as the winner.

---

## Setup

### Requirements

- Python 3.11+
- Node.js 18+ (only if using the web dashboard)
- A Pinterest Business account
- Your own Pinterest pins with analytics enabled

### 1. Clone and create a virtual environment

```bash
git clone <your-repo-url>
cd pinterestSEOproj
python3 -m venv venv
source venv/bin/activate
```

> Run `source venv/bin/activate` every time you open a new terminal window.

### 2. Install dependencies

```bash
pip install -r requirements.txt
python -m playwright install chromium
```

### 3. Set environment variables

```bash
export PINTEREST_EMAIL="you@email.com"
export PINTEREST_PASSWORD="yourpassword"
export PINTEREST_PIN_URLS="https://www.pinterest.com/pin/ID1/,https://www.pinterest.com/pin/ID2/"
```

`PINTEREST_PIN_URLS` is a comma-separated list of your pin URLs. These are the pins the scraper will track.

---

## Running the project

### Test run (recommended first time)

Before starting the scheduler, run the pipeline once manually to confirm everything works:

```bash
python pipeline.py
```

Watch the terminal output. You should see it scrape each pin, score keywords, evaluate A/B groups, and write `dashboard.png`. Open it to verify:

```bash
open dashboard.png
```

### Start the daily scheduler

Once the manual run looks good, start the cron job:

```bash
python scheduler.py
```

This runs forever, firing the full pipeline every day at 3am. To keep it running after you close the terminal:

```bash
nohup python scheduler.py > logs/pipeline.log 2>&1 &
```

### Debugging the scraper

If the scraper isn't capturing data, set `HEADLESS = False` in `scraper.py` to watch the browser navigate Pinterest in real time. This makes it easy to see if login failed or if Pinterest changed their page layout.

---

## Optional: Web dashboard

Instead of opening `dashboard.png` manually you can run a live web dashboard.

### Start the API

```bash
python api.py
```

### Start the frontend

```bash
cd frontend
npm install
npm run dev
```

Open `http://localhost:5173`. The dashboard shows all your charts live, lets you add and remove pins, set up A/B tests, and trigger a manual pipeline run.

---

## Running tests

```bash
pytest test_stage1.py -v
pytest test_stage2.py -v
pytest test_stage3.py -v
pytest test_stage4.py -v
```

Tests use a temporary database and mock out all Playwright calls so they run offline in under a second.

---

## A/B testing your pins

To run an A/B test, create two pins on Pinterest with the same image but different titles or descriptions. Then register them as a variant group using the web dashboard or directly in the database:

```python
from database import get_conn
from datetime import datetime, timezone

with get_conn() as conn:
    conn.execute("""
        INSERT INTO ab_variants (pin_id, variant_group, variant, title, description, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, ("YOUR_PIN_ID_A", "test_group_1", "A", "Title version A", "Description A",
          datetime.now(timezone.utc).isoformat()))
```

After 14 days of daily scraping the A/B engine will automatically declare a winner when the data reaches statistical significance (`p < 0.05`).

---

## Tech stack

| Component | Technology | Purpose |
|---|---|---|
| Browser automation | Playwright | Logs into Pinterest, intercepts XHR |
| Database | SQLite | Stores all time-series data locally |
| Keyword analysis | scikit-learn TF-IDF | Scores keywords by distinctiveness |
| A/B testing | scipy Welch t-test | Statistical significance testing |
| Visualisation | Matplotlib | Generates dashboard.png |
| Scheduler | APScheduler | Runs pipeline daily at 3am |
| API | FastAPI | Serves data to the web dashboard |
| Frontend | React + Tailwind + Recharts | Interactive web dashboard |