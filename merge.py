from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd

from ny_sessions import generate_time_grid
from polygon_client import Candle


def align_candles_to_grid(
    grid_ny: List[datetime],
    candles: List[Candle],
) -> pd.DataFrame:
    # Build DataFrame indexed by NY timestamps
    df = pd.DataFrame(
        [
            {
                "timestamp": c.ts_ny,
                "open": c.open,
                "high": c.high,
                "low": c.low,
                "close": c.close,
                "volume": c.volume,
            }
            for c in candles
        ]
    ).set_index("timestamp").sort_index()

    grid_df = pd.DataFrame(index=pd.Index(grid_ny, name="timestamp"))
    out = grid_df.join(df, how="left")
    # Missing candles: volume=0, prices=None to indicate missing
    out["volume"] = out["volume"].fillna(0)
    return out


def attach_indicators(
    base_df: pd.DataFrame,
    indicators: Dict[str, pd.Series | pd.DataFrame],
) -> pd.DataFrame:
    df = base_df.copy()
    for name, series in indicators.items():
        if isinstance(series, pd.DataFrame):
            # multiple columns like macd
            df = df.join(series)
        else:
            df[name] = series
    return df


def frame_to_export_rows(df: pd.DataFrame, tz_label: str) -> List[Dict]:
    rows: List[Dict] = []
    # Determine indicator columns once
    base_cols = {"open", "high", "low", "close", "volume"}
    indicator_cols = [c for c in df.columns if c not in base_cols]

    for ts, r in df.iterrows():
        row = {
            "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S %z").replace("+0000", "UTC") if tz_label == "UTC" else ts.strftime("%Y-%m-%d %H:%M:%S %z"),
            "open": _round_or_none(r.get("open")),
            "high": _round_or_none(r.get("high")),
            "low": _round_or_none(r.get("low")),
            "close": _round_or_none(r.get("close")),
            "volume": _round_or_none(r.get("volume")),
        }
        # include indicator columns explicitly; null when missing
        for col in indicator_cols:
            val = r.get(col)
            if pd.isna(val):
                row[col] = None
            else:
                row[col] = _round_or_none(val)
        rows.append(row)
    return rows


def _round_or_none(v: Optional[float]) -> Optional[float]:
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    try:
        return round(float(v), 3)
    except Exception:
        return None
