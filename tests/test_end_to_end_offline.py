import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytz
import yaml

import fetch_polygon as fp
from polygon_client import PolygonDataClient, Candle
from ny_sessions import align_to_boundary_ny

NY = pytz.timezone("America/New_York")


def test_end_to_end_offline(tmp_path, monkeypatch):
    # Prepare config
    cfg = {
        "max_candles_limit": 10,
        "config": {
            "1m": [
                {"name": "rsi14", "indicator": "rsi", "params": {"window_size": 14}, "candle_limit": 5}
            ],
            "5m": [
                {"name": "ema10", "indicator": "ema", "params": {"window_size": 10}, "candle_limit": 4},
                {"name": "macd", "indicator": "macd", "params": {"short_window_size": 12, "long_window_size": 26, "signal_window_size": 9}, "candle_limit": 4},
            ],
        },
    }
    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(yaml.safe_dump(cfg), encoding="utf-8")

    # Monkeypatch PolygonDataClient to avoid network
    candles_map = {}

    def fake_fetch_aggs(symbol, timeframe, end_ny, limit):
        if timeframe == "1m":
            base = align_to_boundary_ny(NY.localize(datetime(2025, 10, 30, 10, 5, 0)), "1m")
            candles = [
                Candle(base.replace(minute=1), 1, 2, 0.5, 1.5, 100),
                Candle(base.replace(minute=2), 2, 3, 1.5, 2.5, 200),
                Candle(base.replace(minute=4), 4, 5, 3.5, 4.5, 400),
            ]
        else:
            base = align_to_boundary_ny(NY.localize(datetime(2025, 10, 30, 10, 0, 0)), "5m")
            candles = [
                Candle(base, 10, 12, 9, 11, 1000),
                Candle(base.replace(minute=5), 11, 13, 10, 12, 1100),
                Candle(base.replace(minute=10), 12, 14, 11, 13, 1200),
                Candle(base.replace(minute=15), 13, 15, 12, 14, 1300),
            ]
        return candles[-limit:]

    def fake_fetch_indicator(symbol, timeframe, indicator, params, limit, candles_for_fallback):
        # Force local computation branch by returning computed series
        if indicator == "macd":
            # Return DataFrame with macd columns by delegating to local compute via client method
            # But import internal compute is not public; use simplified local calc
            close = candles_for_fallback["close"].fillna(method="ffill").fillna(method="bfill")
            fast = close.ewm(span=12, adjust=False).mean()
            slow = close.ewm(span=26, adjust=False).mean()
            macd = fast - slow
            signal = macd.ewm(span=9, adjust=False).mean()
            hist = macd - signal
            return pd.DataFrame({"macd_value": macd, "macd_signal": signal, "macd_histogram": hist}).tail(limit)
        if indicator == "ema":
            close = candles_for_fallback["close"].fillna(method="ffill").fillna(method="bfill")
            return close.ewm(span=int(params.get("window_size", 10)), adjust=False).mean().tail(limit)
        if indicator == "rsi":
            # simple constant to verify merge
            return pd.Series([50.0] * len(candles_for_fallback), index=candles_for_fallback.index).tail(limit)
        raise AssertionError("unexpected indicator")

    monkeypatch.setattr(PolygonDataClient, "fetch_aggregates", staticmethod(fake_fetch_aggs))
    monkeypatch.setattr(PolygonDataClient, "fetch_indicator_series", staticmethod(fake_fetch_indicator))

    out_path = tmp_path / "out.json"

    # Run main using environment variable key bypass (won't be used due to monkeypatch)
    args = [
        "--symbol",
        "TSLA",
        "--from",
        "2025-10-30 10:07:23 -0400",
        "--config",
        str(cfg_path),
        "--output",
        str(out_path),
        "--api-key",
        "DUMMY",
    ]

    # Parse and run through main by simulating CLI
    import sys
    old_argv = sys.argv
    try:
        sys.argv = [old_argv[0]] + args
        fp.main()
    finally:
        sys.argv = old_argv

    data = json.loads(out_path.read_text(encoding="utf-8"))
    assert data["ticker"] == "TSLA"
    assert "1m" in data["frames"]
    assert "5m" in data["frames"]
    # Ensure indicators present
    row_5m = data["frames"]["5m"][0]
    assert "ema10" in row_5m or "macd_value" in row_5m
