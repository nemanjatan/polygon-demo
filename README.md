# Polygon.io Candle + Indicators Aligned Export

This tool fetches candle (OHLCV) data and indicator series from Polygon.io, aligns them to a New York session-aware time grid, merges indicators into candles, and exports JSON similar to `2_expected_json_rough-sample.json`.

## Setup

1. Python 3.11+
2. Install deps:

```bash
pip install -r requirements.txt
```

3. Set your API key:

```bash
export POLYGON_API_KEY=YOUR_KEY
```

## Usage

```bash
python fetch_polygon.py \
  --symbol TSLA \
  --from "2025-10-30 20:00:00 -0400" \
  --config 1_input_config.yaml \
  --output out.json
```

- `--symbol`: Ticker, e.g., TSLA or FPGL
- `--from`: Datetime string in local NY offset or UTC (`YYYY-MM-DD HH:MM:SS ±HHMM` or `Z`)
- `--config`: YAML defining timeframes and indicators (see `1_input_config.yaml`)
- `--output`: Path to write the JSON

## Notes
- Aligns to continuous time grids per timeframe, filling missing candles with `null` prices and `0` volume.
- Tries Polygon indicators first; if unavailable, falls back to local computation (EMA, RSI, MACD) for supported indicators.
- Sessions (Pre, Regular, After) are computed using America/New_York timezone.

## API (optional)

Run a small HTTP server exposing the same functionality for your frontend.

```bash
pip install -r requirements.txt
export POLYGON_API_KEY=YOUR_KEY
uvicorn api:create_app --host 0.0.0.0 --port 8000 --reload
```

See `docs/API.md` for endpoints, examples, and request/response schemas.
