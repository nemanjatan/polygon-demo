from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, time
from typing import Iterable, List

import pytz

NY_TZ = pytz.timezone("America/New_York")
UTC_TZ = pytz.UTC


@dataclass(frozen=True)
class SessionWindow:
    name: str  # Pre-Market, Regular, After-Hours
    start: time
    end: time


PRE_MARKET = SessionWindow("Pre-Market", time(4, 0), time(9, 30))
REGULAR = SessionWindow("Regular", time(9, 30), time(16, 0))
AFTER_HOURS = SessionWindow("After-Hours", time(16, 0), time(20, 0))

SESSIONS_ORDERED = [PRE_MARKET, REGULAR, AFTER_HOURS]


def ensure_aware(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        # Assume input is NY local if naive
        return NY_TZ.localize(dt)
    return dt


def to_ny(dt: datetime) -> datetime:
    return ensure_aware(dt).astimezone(NY_TZ)


def to_utc(dt: datetime) -> datetime:
    return ensure_aware(dt).astimezone(UTC_TZ)


def classify_session(dt: datetime) -> str:
    ny = to_ny(dt)
    t = ny.timetz().replace(tzinfo=None)
    for s in SESSIONS_ORDERED:
        if s.start <= t < s.end:
            return s.name
    return "Closed"


def market_status(dt: datetime) -> str:
    sess = classify_session(dt)
    return "Open" if sess in {PRE_MARKET.name, REGULAR.name, AFTER_HOURS.name} else "Closed"


def generate_time_grid(
    end_inclusive_ny: datetime,
    count: int,
    timeframe: str,
) -> List[datetime]:
    # end_inclusive is aligned to the timeframe boundary in NY tz
    end_ny = to_ny(end_inclusive_ny)
    delta = _timeframe_to_timedelta(timeframe)
    grid: List[datetime] = []
    cur = end_ny
    for _ in range(count):
        grid.append(cur)
        cur = cur - delta
    grid.reverse()
    return grid


def align_to_boundary_ny(dt: datetime, timeframe: str) -> datetime:
    ny = to_ny(dt)
    delta = _timeframe_to_timedelta(timeframe)
    # Snap down to boundary
    seconds = int(delta.total_seconds())
    epoch = int(ny.timestamp())
    snapped_epoch = (epoch // seconds) * seconds
    return datetime.fromtimestamp(snapped_epoch, NY_TZ)


def _timeframe_to_timedelta(tf: str) -> timedelta:
    if tf.endswith("s"):
        return timedelta(seconds=int(tf[:-1]))
    if tf.endswith("m"):
        return timedelta(minutes=int(tf[:-1]))
    if tf.endswith("h"):
        return timedelta(hours=int(tf[:-1]))
    if tf.endswith("d"):
        return timedelta(days=int(tf[:-1]))
    raise ValueError(f"Unsupported timeframe: {tf}")
