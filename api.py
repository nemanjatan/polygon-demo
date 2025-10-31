from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
import os

import pandas as pd
from dateutil import parser as dtparser
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from merge import align_candles_to_grid, attach_indicators, frame_to_export_rows
from ny_sessions import (
    align_to_boundary_ny,
    classify_session,
    generate_time_grid,
    market_status,
    to_ny,
)
from polygon_client import PolygonDataClient


app = FastAPI(title="Polygon Export API", version="1.0.0")

# CORS (configurable via ALLOW_ORIGINS comma-separated env var)
allowed_origins_env = os.environ.get("ALLOW_ORIGINS", "")
if allowed_origins_env.strip():
    allowed_origins = [o.strip() for o in allowed_origins_env.split(",") if o.strip()]
else:
    allowed_origins = [
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class IndicatorConfig(BaseModel):
    name: str
    indicator: str
    params: Dict[str, Any] = Field(default_factory=dict)
    candle_limit: Optional[int] = None


class ExportConfig(BaseModel):
    max_candles_limit: int = 200
    config: Dict[str, List[IndicatorConfig]]


class ExportRequest(BaseModel):
    symbol: str
    as_of: str = Field(description="Datetime string, e.g. '2025-10-30 20:00:00 -0400' or ISO8601")
    config: ExportConfig
    api_key: Optional[str] = None


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/v1/market_status")
def get_market_status(at: Optional[str] = None) -> Dict[str, str]:
    if at is None:
        dt = datetime.now()
    else:
        try:
            dt = dtparser.parse(at)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid datetime: {e}")
    ny = to_ny(dt)
    return {
        "as_of": ny.strftime("%Y-%m-%d %H:%M:%S %z"),
        "market_status": market_status(ny),
        "market_session": classify_session(ny),
        "timezone": "America/New_York",
    }


@app.get("/v1/time_grid")
def get_time_grid(end: str, timeframe: str, count: int = 50) -> Dict[str, Any]:
    try:
        end_dt = dtparser.parse(end)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid end datetime: {e}")
    end_aligned = align_to_boundary_ny(end_dt, timeframe)
    grid = generate_time_grid(end_aligned, count, timeframe)
    return {
        "end_aligned": end_aligned.strftime("%Y-%m-%d %H:%M:%S %z"),
        "timestamps": [ts.strftime("%Y-%m-%d %H:%M:%S %z") for ts in grid],
    }


@app.post("/v1/export")
def export_data(req: ExportRequest) -> Dict[str, Any]:
    symbol = req.symbol.upper()
    try:
        as_of_ny: datetime = to_ny(dtparser.parse(req.as_of))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid as_of datetime: {e}")

    api_key = req.api_key
    if not api_key:
        import os
        api_key = os.environ.get("POLYGON_API_KEY")
    if not api_key:
        raise HTTPException(status_code=400, detail="POLYGON_API_KEY not provided.")

    client = PolygonDataClient(api_key)

    max_candles_limit: int = int(req.config.max_candles_limit)
    frames_cfg = req.config.config

    export: Dict[str, Any] = {
        "version": "1.1.0",
        "as_of_utc": to_ny(as_of_ny).astimezone(pd.Timestamp("now", tz="UTC").tz).strftime("%Y-%m-%d %H:%M:%S UTC"),
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
            [int(ind.candle_limit or max_candles_limit) for ind in indicators] + [max_candles_limit]
        )
        end_aligned = align_to_boundary_ny(as_of_ny, timeframe)

        candles = client.fetch_aggregates(symbol, timeframe, end_aligned, per_frame_limit)

        grid = generate_time_grid(end_aligned, per_frame_limit, timeframe)
        base_df = align_candles_to_grid(grid, candles)

        indicators_map: Dict[str, Any] = {}
        fallback_df = base_df.copy()
        for ind in indicators:
            series = client.fetch_indicator_series(
                symbol=symbol,
                timeframe=timeframe,
                indicator=ind.indicator,
                params=ind.params,
                limit=per_frame_limit,
                candles_for_fallback=fallback_df,
            )
            if isinstance(series, pd.DataFrame):
                indicators_map.update({col: series[col] for col in series.columns})
            else:
                indicators_map[ind.name] = series.rename(ind.name)

        merged = attach_indicators(base_df, indicators_map)
        export_rows = frame_to_export_rows(merged, tz_label="EDT")
        export["frames"][timeframe] = export_rows

    return export


# Convenience: uvicorn entrypoint
def create_app() -> FastAPI:
    return app


