from datetime import datetime
import pytz

from ny_sessions import classify_session, market_status, align_to_boundary_ny, generate_time_grid

NY = pytz.timezone("America/New_York")


def test_classify_and_market_status():
    pre = NY.localize(datetime(2025, 10, 30, 5, 0, 0))
    reg = NY.localize(datetime(2025, 10, 30, 10, 0, 0))
    aft = NY.localize(datetime(2025, 10, 30, 17, 0, 0))
    closed = NY.localize(datetime(2025, 10, 30, 21, 0, 0))

    assert classify_session(pre) == "Pre-Market"
    assert classify_session(reg) == "Regular"
    assert classify_session(aft) == "After-Hours"
    assert classify_session(closed) == "Closed"

    assert market_status(pre) == "Open"
    assert market_status(reg) == "Open"
    assert market_status(aft) == "Open"
    assert market_status(closed) == "Closed"


def test_align_to_boundary_and_grid():
    dt = NY.localize(datetime(2025, 10, 30, 10, 7, 23))
    m1 = align_to_boundary_ny(dt, "1m")
    assert m1.minute == 7 and m1.second == 0

    m5 = align_to_boundary_ny(dt, "5m")
    assert m5.minute == 5 and m5.second == 0

    grid = generate_time_grid(m5, 5, "1m")
    assert len(grid) == 5
    # last is m5, previous 4 are consecutive minutes
    assert grid[-1] == m5
    for i in range(1, 5):
        assert (grid[-i] - grid[-i-1]).total_seconds() == 60
