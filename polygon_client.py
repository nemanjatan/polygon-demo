from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Iterable

import numpy as np
import pandas as pd

# Prefer Polygon (as per task), fallback to Massive (rebrand)
try:
    from polygon import RESTClient  # type: ignore
    _CLIENT_KIND = "polygon"
except Exception:  # pragma: no cover
    from massive import RESTClient  # type: ignore
    _CLIENT_KIND = "massive"


@dataclass
class Candle:
    ts_ny: datetime
    open: Optional[float]
    high: Optional[float]
    low: Optional[float]
    close: Optional[float]
    volume: Optional[float]


class PolygonDataClient:
    def __init__(self, api_key: str):
        self.client = RESTClient(api_key=api_key)

    def fetch_aggregates(
        self,
        symbol: str,
        timeframe: str,
        end_ny: datetime,
        limit: int,
    ) -> List[Candle]:
        multiplier, timespan = self._parse_tf(timeframe)
        # Use end_ny to derive a back window
        end_utc = pd.Timestamp(end_ny).tz_convert("UTC").to_pydatetime()
        lookback = self._tf_to_timedelta(timeframe) * (limit * 4)
        start_utc = end_utc - lookback

        rows: List[Candle] = []
        if hasattr(self.client, "list_aggs"):
            # Massive style
            from_str = start_utc.strftime("%Y-%m-%d")
            to_str = end_utc.strftime("%Y-%m-%d")
            aggs_iter: Iterable[Any] = self.client.list_aggs(
                ticker=symbol,
                multiplier=multiplier,
                timespan=timespan,
                from_=from_str,
                to=to_str,
                limit=50000,
            )
            for a in aggs_iter:
                ts = pd.to_datetime(getattr(a, "timestamp"), unit="ms", utc=True).tz_convert("America/New_York").to_pydatetime()
                rows.append(Candle(ts, a.open, a.high, a.low, a.close, float(getattr(a, "volume", 0) or 0)))
        else:
            # Polygon style
            aggs = self.client.get_aggs(
                ticker=symbol,
                multiplier=multiplier,
                timespan=timespan,
                from_=start_utc.isoformat(),
                to=end_utc.isoformat(),
                limit=limit * 5,
                sort="desc",
            )
            for a in aggs:
                ts = pd.to_datetime(a.timestamp, unit="ms", utc=True).tz_convert("America/New_York").to_pydatetime()
                rows.append(Candle(ts, a.open, a.high, a.low, a.close, float(a.volume) if a.volume is not None else 0.0))

        rows.sort(key=lambda r: r.ts_ny)
        return rows[-limit:]

    def fetch_indicator_series(
        self,
        symbol: str,
        timeframe: str,
        indicator: str,
        params: Dict,
        limit: int,
        candles_for_fallback: Optional[pd.DataFrame] = None,
    ) -> pd.Series:
        # Use local computation for reliability
        if candles_for_fallback is None:
            raise RuntimeError("Indicator fallback requires candles_for_fallback")
        return self._compute_indicator_local(indicator, params, candles_for_fallback).tail(limit)

    # --- internals ---

    def _compute_indicator_local(self, indicator: str, params: Dict, candles_df: pd.DataFrame) -> pd.Series:
        if indicator.lower() == "rsi":
            window = int(params.get("window_size", 14))
            return _rsi(candles_df["close"].astype(float), window).rename(f"rsi{window}")
        if indicator.lower() == "ema":
            window = int(params.get("window_size", 10))
            return candles_df["close"].astype(float).ewm(span=window, adjust=False).mean().rename(f"ema{window}")
        if indicator.lower() == "macd":
            short_w = int(params.get("short_window_size", 12))
            long_w = int(params.get("long_window_size", 26))
            signal_w = int(params.get("signal_window_size", 9))
            fast = candles_df["close"].astype(float).ewm(span=short_w, adjust=False).mean()
            slow = candles_df["close"].astype(float).ewm(span=long_w, adjust=False).mean()
            macd = fast - slow
            signal = macd.ewm(span=signal_w, adjust=False).mean()
            hist = macd - signal
            out = pd.DataFrame({
                "macd_value": macd,
                "macd_signal": signal,
                "macd_histogram": hist,
            })
            return out  # type: ignore
        raise ValueError(f"Unsupported indicator: {indicator}")

    @staticmethod
    def _parse_tf(tf: str):
        if tf.endswith("s"):
            return int(tf[:-1]), "second"
        if tf.endswith("m"):
            return int(tf[:-1]), "minute"
        if tf.endswith("h"):
            return int(tf[:-1]), "hour"
        if tf.endswith("d"):
            return int(tf[:-1]), "day"
        raise ValueError(f"Unsupported timeframe: {tf}")

    @staticmethod
    def _tf_to_timedelta(tf: str):
        if tf.endswith("s"):
            return timedelta(seconds=int(tf[:-1]))
        if tf.endswith("m"):
            return timedelta(minutes=int(tf[:-1]))
        if tf.endswith("h"):
            return timedelta(hours=int(tf[:-1]))
        if tf.endswith("d"):
            return timedelta(days=int(tf[:-1]))
        raise ValueError(f"Unsupported timeframe: {tf}")


def _rsi(prices: pd.Series, window: int) -> pd.Series:
    delta = prices.diff()
    gain = (delta.clip(lower=0)).ewm(alpha=1/window, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1/window, adjust=False).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi
