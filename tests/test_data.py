from __future__ import annotations

from datetime import date, timedelta


def make_stock_rows(num_weeks: int = 70) -> list[dict]:
    symbols = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "SBIN"]
    base_close = {
        "RELIANCE": 120.0,
        "TCS": 200.0,
        "INFY": 150.0,
        "HDFCBANK": 90.0,
        "ICICIBANK": 110.0,
        "SBIN": 80.0,
    }
    rows: list[dict] = []
    start = date(2024, 1, 1)
    for i in range(num_weeks):
        dt = start + timedelta(days=7 * i)
        dt_s = dt.isoformat()
        for s in symbols:
            if s == "RELIANCE":
                close = base_close[s] + i * 0.4 + ((i % 6) - 3) * 0.6
            elif s == "TCS":
                close = base_close[s] - i * 0.3 + ((i % 4) - 1.5) * 0.9
            elif s == "INFY":
                close = base_close[s] + ((-1) ** i) * 3.0 + i * 0.1
            elif s == "HDFCBANK":
                close = base_close[s] + (i % 8) * 0.5
            elif s == "ICICIBANK":
                close = base_close[s] + i * 0.2 + ((i % 5) - 2) * 0.7
            else:
                close = base_close[s] + ((i % 3) - 1) * 1.8
            close = max(1.0, round(close, 4))
            open_ = round(close * (1.0 + (0.002 if i % 2 == 0 else -0.002)), 4)
            high = round(max(open_, close) * 1.02, 4)
            low = round(min(open_, close) * 0.98, 4)
            vol = 300_000 + (i * 3_000) + (hash(s) % 20_000)
            rows.append(
                {
                    "symbol": s,
                    "date": dt_s,
                    "open": open_,
                    "high": high,
                    "low": low,
                    "close": close,
                    "volume": vol,
                }
            )
    return rows


def make_index_rows(num_weeks: int = 70, index_name: str = "NIFTY_50") -> list[dict]:
    rows: list[dict] = []
    start = date(2024, 1, 1)
    for i in range(num_weeks):
        dt = start + timedelta(days=7 * i)
        close = 1000.0 + (i * 1.7) + ((i % 5) - 2) * 4.0
        open_ = close * (1.0 + (0.001 if i % 2 == 0 else -0.001))
        high = max(open_, close) * 1.01
        low = min(open_, close) * 0.99
        rows.append(
            {
                "symbol": index_name,
                "date": dt.isoformat(),
                "open": round(open_, 4),
                "high": round(high, 4),
                "low": round(low, 4),
                "close": round(close, 4),
                "volume": 1_000_000 + i * 10_000,
            }
        )
    return rows


def make_positions() -> list[dict]:
    return [
        {"position_id": "p1", "symbol": "RELIANCE", "entry_date": "2024-03-18", "entry_price": 122.0},
        {"position_id": "p2", "symbol": "TCS", "entry_date": "2024-03-18", "entry_price": 197.0},
        {"position_id": "p3", "symbol": "INFY", "entry_date": "2024-03-18", "entry_price": 151.0},
    ]


def tiny_rows_for_entry_timing() -> list[dict]:
    # One clear down move at 2024-01-08 to trigger entry on next bar (2024-01-15).
    dates = ["2024-01-01", "2024-01-08", "2024-01-15", "2024-01-22", "2024-01-29", "2024-02-05"]
    closes = [100.0, 88.0, 90.0, 78.0, 79.0, 81.0]
    opens = [100.0, 90.0, 91.0, 80.0, 79.0, 82.0]
    rows: list[dict] = []
    for dt, open_, close in zip(dates, opens, closes):
        rows.append(
            {
                "symbol": "RELIANCE",
                "date": dt,
                "open": open_,
                "high": max(open_, close) * 1.01,
                "low": min(open_, close) * 0.99,
                "close": close,
                "volume": 1_000_000,
            }
        )
    return rows


def rows_with_halted_symbol_after_entry() -> list[dict]:
    rows: list[dict] = []

    aaa_dates = ["2024-01-01", "2024-01-08", "2024-01-15"]
    aaa_opens = [100.0, 90.0, 85.0]
    aaa_closes = [100.0, 88.0, 86.0]
    for dt, open_, close in zip(aaa_dates, aaa_opens, aaa_closes):
        rows.append(
            {
                "symbol": "AAA",
                "date": dt,
                "open": open_,
                "high": max(open_, close) * 1.01,
                "low": min(open_, close) * 0.99,
                "close": close,
                "volume": 1_000_000,
            }
        )

    # Keep market calendar extending further while AAA stops printing rows.
    bbb_dates = ["2024-01-01", "2024-01-08", "2024-01-15", "2024-01-22", "2024-01-29"]
    bbb_closes = [50.0, 51.0, 52.0, 53.0, 54.0]
    for dt, close in zip(bbb_dates, bbb_closes):
        rows.append(
            {
                "symbol": "BBB",
                "date": dt,
                "open": close,
                "high": close * 1.01,
                "low": close * 0.99,
                "close": close,
                "volume": 800_000,
            }
        )
    return rows
