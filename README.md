# Newsfeed (Labor Market Tracker)

## Run the site locally

From the repo root:

```sh
python3 -m http.server 8000
```

Then open `http://localhost:8000/` (or `http://localhost:8000/index.html`).

## Refresh the BLS / ADP / Revelio comparison files

```sh
source .venv/bin/activate
FRED_API_KEY="..." python3 scripts/fetch_bls_revisions.py
```

This updates files like `data/raw/employment_naics.csv`, `data/raw/bls_revisions.csv`, and JSONs under `data/`.

Lightweight refresh (Alt Compare employment only, skip revisions):

```sh
source .venv/bin/activate
FRED_API_KEY="..." python3 scripts/fetch_bls_revisions.py --employment-only
```

## Build the Kalshi “Market Impact” dataset

### Demo dataset (no network)

```sh
python3 scripts/fetch_kalshi_impact.py --demo
```

### Live dataset (requires Kalshi API credentials + network)

```sh
# Preferred: API key id + private key (.key file)
KALSHI_KEY_ID="..." KALSHI_PRIVATE_KEY_PATH="/path/to/kalshi.key" python3 scripts/fetch_kalshi_impact.py --start-month 2025-07 --end-month 2025-12

# Alternative: bearer token (if you have one)
KALSHI_TOKEN="..." python3 scripts/fetch_kalshi_impact.py --start-month 2025-07 --end-month 2025-12
```

Optional: customize horizons / window (e.g., add a 1-day horizon by fetching a longer window):

```sh
KALSHI_TOKEN="..." python3 scripts/fetch_kalshi_impact.py --start-month 2025-07 --end-month 2025-12 --post-minutes 1440 --horizons 5,30,60,240,1440
```

Optional: add actual/expected values via a CSV (see `data/kalshi_impact_events.example.csv`):

```sh
KALSHI_TOKEN="..." python3 scripts/fetch_kalshi_impact.py --events-csv data/kalshi_impact_events.example.csv
```

The website page `impact.html` reads `data/kalshi_impact.json` (including per-event + per-source summaries).

## Build the Polymarket “Market Impact” dataset

```sh
python3 scripts/fetch_polymarket_impact.py --start-month 2025-07 --end-month 2025-12
```

This writes `data/polymarket_impact.json`. In `impact.html`, switch Provider to `Polymarket (NFP)` to view it.
