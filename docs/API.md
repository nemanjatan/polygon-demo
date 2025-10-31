# Polygon Export API — Quick Guide

This backend wraps the existing scripts into a small HTTP API our frontend can consume. It fetches candles, computes/attaches indicators, aligns to a New York session-aware time grid, and returns a FE-friendly JSON structure identical to our `out.json` export.

## Run locally

```bash
pip install -r requirements.txt
export POLYGON_API_KEY=YOUR_KEY
uvicorn api:create_app --host 0.0.0.0 --port 8000 --reload
```

- Interactive docs: `http://localhost:8000/docs`
- Health check: `GET /health`

## Endpoints

### GET /v1/market_status
- **Query params**: `at` (datetime string; ISO8601 or e.g. `2025-10-30 20:00:00 -0400`)
- **Response**:
```json
{
  "as_of": "2025-10-30 20:00:00 -0400",
  "market_status": "Open",
  "market_session": "After-Hours",
  "timezone": "America/New_York"
}
```

### GET /v1/time_grid
- **Query params**: `end`, `timeframe` (e.g., `1m`, `5m`, `1h`), `count` (default 50)
- **Response**:
```json
{
  "end_aligned": "2025-10-30 20:00:00 -0400",
  "timestamps": ["2025-10-30 19:12:00 -0400", "..."]
}
```

### POST /v1/export
Runs the candle + indicators flow and returns the export JSON our FE can render.

- **Body**:
```json
{
  "symbol": "TSLA",
  "as_of": "2025-10-30 20:00:00 -0400",
  "api_key": null,
  "config": {
    "max_candles_limit": 200,
    "config": {
      "1m": [
        {"name": "ema10", "indicator": "ema", "params": {"window_size": 10}},
        {"name": "rsi14", "indicator": "rsi", "params": {"window_size": 14}},
        {"name": "macd", "indicator": "macd", "params": {"short_window_size": 12, "long_window_size": 26, "signal_window_size": 9}}
      ]
    }
  }
}
```
- **Auth**: If `api_key` is omitted, the service uses `POLYGON_API_KEY` from environment.
- **Response (abridged)**:
```json
{
  "version": "1.1.0",
  "as_of_utc": "2025-10-31 00:00:00 UTC",
  "as_of_edt": "2025-10-30 20:00:00 -0400",
  "source": "polygon.io",
  "ticker": "TSLA",
  "market_status": "Open",
  "market_session": "After-Hours",
  "timezone": "America/New_York",
  "frames": {
    "1m": [
      {
        "timestamp": "2025-10-30 19:12:00 -0400",
        "open": 199.12,
        "high": 199.5,
        "low": 198.7,
        "close": 199.0,
        "volume": 12345,
        "ema10": 198.9,
        "rsi14": 55.1,
        "macd_value": 0.12,
        "macd_signal": 0.10,
        "macd_histogram": 0.02
      }
    ]
  }
}
```

## Indicator support
Indicators are computed locally for reliability:
- **EMA**: `indicator: "ema"`, params: `{ "window_size": number }`
- **RSI**: `indicator: "rsi"`, params: `{ "window_size": number }`
- **MACD**: `indicator: "macd"`, params: `{ "short_window_size", "long_window_size", "signal_window_size" }`

Returned indicator columns are merged into each frame row. Missing values are `null`.

## Timeframes and alignment
- Timeframes: strings like `1m`, `5m`, `1h`, `1d`
- Data is snapped to timeframe boundaries in `America/New_York` and aligned to a continuous time grid. Missing candles keep `open/high/low/close = null`, `volume = 0` to preserve spacing.

## Request examples

cURL:
```bash
curl -X POST http://localhost:8000/v1/export \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "TSLA",
    "as_of": "2025-10-30 20:00:00 -0400",
    "config": {
      "max_candles_limit": 200,
      "config": {
        "1m": [
          {"name": "ema10", "indicator": "ema", "params": {"window_size": 10}},
          {"name": "rsi14", "indicator": "rsi", "params": {"window_size": 14}},
          {"name": "macd", "indicator": "macd", "params": {"short_window_size": 12, "long_window_size": 26, "signal_window_size": 9}}
        ]
      }
    }
  }'
```

Fetch (browser/FE):
```typescript
const res = await fetch("/v1/export", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({
    symbol: "TSLA",
    as_of: "2025-10-30 20:00:00 -0400",
    config: {
      max_candles_limit: 200,
      config: {
        "1m": [
          { name: "ema10", indicator: "ema", params: { window_size: 10 } },
          { name: "rsi14", indicator: "rsi", params: { window_size: 14 } },
          { name: "macd", indicator: "macd", params: { short_window_size: 12, long_window_size: 26, signal_window_size: 9 } }
        ]
      }
    }
  })
});
const data = await res.json();
```

## Files of interest
- Backend entry: `api.py`
- Core logic reused from CLI: `fetch_polygon.py`, `merge.py`, `ny_sessions.py`, `polygon_client.py`

## Notes
- Set `POLYGON_API_KEY` or pass `api_key` in the request body.
- OpenAPI docs auto-generated at `/docs` for easy testing and schema inspection.
- Add CORS if serving from a different origin (can be configured in `api.py`).

