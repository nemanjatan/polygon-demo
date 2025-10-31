from datetime import datetime, timedelta
import pandas as pd
import pytz

from merge import align_candles_to_grid, attach_indicators, frame_to_export_rows
from polygon_client import Candle

NY = pytz.timezone("America/New_York")


def _ts(h, m):
    return NY.localize(datetime(2025, 10, 30, h, m, 0))


def test_align_candles_to_grid_with_gaps():
    grid = [_ts(10, i) for i in range(0, 5)]  # 10:00..10:04
    candles = [
        Candle(_ts(10, 0), 1, 2, 0.5, 1.5, 100),
        # missing 10:01
        Candle(_ts(10, 2), 2, 3, 1.5, 2.5, 200),
        Candle(_ts(10, 4), 4, 5, 3.5, 4.5, 400),
    ]
    df = align_candles_to_grid(grid, candles)
    assert len(df) == 5
    # volume filled to 0 on missing
    assert df.loc[_ts(10, 1), "volume"] == 0
    assert pd.isna(df.loc[_ts(10, 1), "open"])  # missing price stays NaN


def test_attach_indicators_and_export_rows():
    grid = [_ts(10, i) for i in range(0, 3)]
    candles = [
        Candle(_ts(10, 0), 1, 2, 0.5, 1.5, 100),
        Candle(_ts(10, 1), 2, 3, 1.5, 2.5, 200),
        Candle(_ts(10, 2), 3, 4, 2.5, 3.5, 300),
    ]
    base = align_candles_to_grid(grid, candles)

    # single series
    rsi = pd.Series([50.0, 51.0, 52.0], index=base.index, name="rsi14")
    # multi-column (macd)
    macd = pd.DataFrame(
        {
            "macd_value": [0.1, 0.2, 0.3],
            "macd_signal": [0.05, 0.1, 0.15],
            "macd_histogram": [0.05, 0.1, 0.15],
        },
        index=base.index,
    )
    merged = attach_indicators(base, {"rsi14": rsi, "macd": macd})

    rows = frame_to_export_rows(merged, tz_label="EDT")
    assert len(rows) == 3
    assert set(["rsi14", "macd_value", "macd_signal", "macd_histogram"]).issubset(rows[0].keys())
