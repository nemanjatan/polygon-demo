from __future__ import annotations

import argparse
import json
from datetime import datetime
from typing import Dict, List

import pandas as pd
import pytz
import yaml
from dateutil import parser as dtparser
from dotenv import load_dotenv

from ny_sessions import align_to_boundary_ny, classify_session, market_status, to_ny
from polygon_client import PolygonDataClient
from merge import align_candles_to_grid, attach_indicators, frame_to_export_rows


# Load .env if present
load_dotenv()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch candles + indicators and export aligned JSON")
    p.add_argument("--symbol", required=True)
    p.add_argument("--from", dest="from_dt", required=True, help="Datetime string, e.g. '2025-10-30 20:00:00 -0400'")
    p.add_argument("--config", required=True, help="YAML config file")
    p.add_argument("--output", required=True, help="Output JSON path")
    p.add_argument("--api-key", dest="api_key", default=None, help="Polygon API key (or set POLYGON_API_KEY env)")
    return p.parse_args()


def load_config(path: str) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict) and "config" in item:
                return item
        # fallback to first list item if no explicit 'config'
        return raw[0]
    return raw


def main():
    args = parse_args()
    cfg = load_config(args.config)

    symbol: str = args.symbol.upper()
    as_of_ny: datetime = to_ny(dtparser.parse(args.from_dt))

    api_key = args.api_key
    if not api_key:
        import os
        api_key = os.environ.get("POLYGON_API_KEY")
    if not api_key:
        raise SystemExit("POLYGON_API_KEY not provided.")

    client = PolygonDataClient(api_key)

    max_candles_limit: int = int(cfg.get("max_candles_limit", 200))
    frames_cfg: Dict[str, List[Dict]] = cfg["config"]

    export = {
        "version": "1.1.0",
        "as_of_utc": to_ny(as_of_ny).astimezone(pytz.UTC).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "as_of_edt": to_ny(as_of_ny).strftime("%Y-%m-%d %H:%M:%S %z"),
        "source": "polygon.io",
        "ticker": symbol,
        "market_status": market_status(as_of_ny),
        "market_session": classify_session(as_of_ny),
        "timezone": "America/New_York",
        "frames": {},
    }

    for timeframe, indicators in frames_cfg.items():
        per_frame_limit = max(
            [int(ind.get("candle_limit", max_candles_limit)) for ind in indicators] + [max_candles_limit]
        )
        end_aligned = align_to_boundary_ny(as_of_ny, timeframe)

        candles = client.fetch_aggregates(symbol, timeframe, end_aligned, per_frame_limit)

        # Build grid and align
        from ny_sessions import generate_time_grid
        grid = generate_time_grid(end_aligned, per_frame_limit, timeframe)
        base_df = align_candles_to_grid(grid, candles)

        # Indicator series
        indicators_map: Dict[str, pd.Series | pd.DataFrame] = {}
        # Provide candles for fallback local computation
        fallback_df = base_df.copy()
        for ind in indicators:
            name = ind["name"]
            kind = ind["indicator"]
            params = ind.get("params", {})
            series = client.fetch_indicator_series(
                symbol=symbol,
                timeframe=timeframe,
                indicator=kind,
                params=params,
                limit=per_frame_limit,
                candles_for_fallback=fallback_df,
            )
            # Name columns consistently
            if isinstance(series, pd.DataFrame):
                indicators_map.update({col: series[col] for col in series.columns})
            else:
                indicators_map[name] = series.rename(name)

        merged = attach_indicators(base_df, indicators_map)
        export_rows = frame_to_export_rows(merged, tz_label="EDT")
        export["frames"][timeframe] = export_rows

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(export, f, indent=2)


if __name__ == "__main__":
    main()
