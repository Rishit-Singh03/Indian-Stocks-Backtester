from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from math import sqrt
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from app.clickhouse import ClickHouseClient, sql_string, validate_identifier
from app.config import get_settings


settings = get_settings()
app = FastAPI(title="Stock Dashboard API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ch = ClickHouseClient(
    base_url=settings.clickhouse_url,
    user=settings.clickhouse_user,
    password=settings.clickhouse_password,
    timeout=45,
)


def parse_date_or_default(value: str | None, default_value: date) -> date:
    if not value:
        return default_value
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid date: {value}") from exc


def split_symbols(symbols: str, max_count: int | None = None) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in symbols.split(","):
        value = item.strip().upper()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    if not out:
        raise HTTPException(status_code=400, detail="Provide at least one symbol/index.")
    if max_count is not None and len(out) > max_count:
        raise HTTPException(status_code=400, detail=f"Too many symbols. Max allowed: {max_count}")
    return out


def sql_string_list(values: list[str]) -> str:
    return ", ".join(sql_string(value) for value in values)


def table_name(which: Literal["stock", "index", "ticker"]) -> str:
    if which == "stock":
        return validate_identifier(settings.prices_table)
    if which == "index":
        return validate_identifier(settings.index_table)
    return validate_identifier(settings.ticker_table)


def load_series_rows(
    symbols: list[str],
    universe: Literal["stock", "index"],
    interval: Literal["1d", "1w"],
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    db = validate_identifier(settings.clickhouse_database)
    target = table_name("stock" if universe == "stock" else "index")
    symbol_column = "symbol" if universe == "stock" else "index_name"
    list_sql = sql_string_list(symbols)
    start_sql = sql_string(start_date.isoformat())
    end_sql = sql_string(end_date.isoformat())

    if interval == "1d":
        query = f"""
SELECT
    {symbol_column} AS symbol,
    date,
    open,
    high,
    low,
    close,
    volume
FROM {db}.{target}
WHERE {symbol_column} IN ({list_sql})
  AND date BETWEEN {start_sql} AND {end_sql}
ORDER BY symbol, date
FORMAT JSONEachRow
""".strip()
        return ch.query_rows(query)

    # Weekly rollup from daily bars.
    query = f"""
SELECT
    symbol,
    week_start AS date,
    argMin(open, date) AS open,
    max(high) AS high,
    min(low) AS low,
    argMax(close, date) AS close,
    sum(volume) AS volume
FROM
(
    SELECT
        {symbol_column} AS symbol,
        date,
        open,
        high,
        low,
        close,
        volume,
        toDate(toStartOfWeek(date, 1)) AS week_start
    FROM {db}.{target}
    WHERE {symbol_column} IN ({list_sql})
      AND date BETWEEN {start_sql} AND {end_sql}
)
GROUP BY symbol, week_start
ORDER BY symbol, week_start
FORMAT JSONEachRow
""".strip()
    return ch.query_rows(query)


def corr(x: list[float], y: list[float]) -> float | None:
    n = len(x)
    if n < 2 or n != len(y):
        return None
    mx = sum(x) / n
    my = sum(y) / n
    num = sum((a - mx) * (b - my) for a, b in zip(x, y))
    denx = sqrt(sum((a - mx) ** 2 for a in x))
    deny = sqrt(sum((b - my) ** 2 for b in y))
    if denx == 0 or deny == 0:
        return None
    return num / (denx * deny)


@app.get("/api/v1/health")
def health() -> dict[str, Any]:
    try:
        rows = ch.query_rows("SELECT 1 AS ok FORMAT JSONEachRow")
        return {"status": "ok", "clickhouse": bool(rows and rows[0].get("ok") == 1)}
    except Exception as exc:  # noqa: BLE001
        return {"status": "degraded", "clickhouse": False, "error": str(exc)}


@app.get("/api/v1/search")
def search(q: str = Query(..., min_length=1), limit: int = Query(20, ge=1, le=100)) -> dict[str, Any]:
    db = validate_identifier(settings.clickhouse_database)
    ticker_tbl = table_name("ticker")
    index_tbl = table_name("index")
    q_sql = sql_string(f"%{q}%")
    stock_limit = max(1, int(limit * 0.7))
    index_limit = max(1, limit - stock_limit)

    stock_query = f"""
SELECT
    'stock' AS type,
    symbol AS code,
    company_name AS name,
    bse_code AS meta
FROM {db}.{ticker_tbl}
WHERE exchange = 'BSE'
  AND status = 'ACTIVE'
  AND (
      lowerUTF8(symbol) LIKE lowerUTF8({q_sql})
      OR lowerUTF8(company_name) LIKE lowerUTF8({q_sql})
      OR lowerUTF8(bse_code) LIKE lowerUTF8({q_sql})
  )
ORDER BY symbol
LIMIT {stock_limit}
FORMAT JSONEachRow
""".strip()
    index_query = f"""
SELECT
    'index' AS type,
    index_name AS code,
    index_name AS name,
    any(provider_ticker) AS meta
FROM {db}.{index_tbl}
GROUP BY index_name
HAVING lowerUTF8(index_name) LIKE lowerUTF8({q_sql})
ORDER BY index_name
LIMIT {index_limit}
FORMAT JSONEachRow
""".strip()

    try:
        stock_rows = ch.query_rows(stock_query)
        index_rows = ch.query_rows(index_query)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    combined = (stock_rows + index_rows)[:limit]
    return {"query": q, "count": len(combined), "results": combined}


@app.get("/api/v1/indexes/snapshot")
def indexes_snapshot(on_date: str | None = None) -> dict[str, Any]:
    db = validate_identifier(settings.clickhouse_database)
    index_tbl = table_name("index")
    target_date = parse_date_or_default(on_date, datetime.now(timezone.utc).date())
    target_sql = sql_string(target_date.isoformat())
    query = f"""
SELECT index_name, date, close
FROM {db}.{index_tbl}
WHERE date <= {target_sql}
ORDER BY index_name, date DESC
LIMIT 2 BY index_name
FORMAT JSONEachRow
""".strip()
    try:
        rows = ch.query_rows(query)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["index_name"])].append(row)

    items: list[dict[str, Any]] = []
    for name in sorted(grouped.keys()):
        values = sorted(grouped[name], key=lambda r: str(r["date"]), reverse=True)
        latest = values[0]
        prev = values[1] if len(values) > 1 else None
        close = float(latest["close"])
        prev_close = float(prev["close"]) if prev is not None else None
        abs_change = close - prev_close if prev_close is not None else None
        pct_change = ((close / prev_close) - 1.0) * 100.0 if prev_close not in (None, 0.0) else None
        items.append(
            {
                "index_name": name,
                "date": str(latest["date"]),
                "close": close,
                "prev_close": prev_close,
                "abs_change": abs_change,
                "pct_change": pct_change,
            }
        )
    return {"as_of_date": target_date.isoformat(), "count": len(items), "items": items}


@app.get("/api/v1/series")
def series(
    symbols: str = Query(..., description="Comma-separated symbols or index names."),
    universe: Literal["stock", "index"] = Query("stock"),
    interval: Literal["1d", "1w"] = Query("1w"),
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, Any]:
    names = split_symbols(symbols)
    today = datetime.now(timezone.utc).date()
    end_d = parse_date_or_default(end_date, today)
    default_start = end_d - timedelta(days=365)
    start_d = parse_date_or_default(start_date, default_start)
    if start_d > end_d:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")

    try:
        rows = load_series_rows(names, universe, interval, start_d, end_d)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["symbol"])].append(
            {
                "date": str(row["date"]),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": int(row["volume"]),
            }
        )
    result = [{"symbol": symbol, "points": grouped.get(symbol, [])} for symbol in names]
    return {
        "universe": universe,
        "interval": interval,
        "start_date": start_d.isoformat(),
        "end_date": end_d.isoformat(),
        "series": result,
    }


@app.get("/api/v1/ohlcv/{symbol}")
def ohlcv(
    symbol: str,
    universe: Literal["stock", "index"] = Query("stock"),
    interval: Literal["1d", "1w"] = Query("1w"),
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, Any]:
    name = symbol.strip().upper()
    if not name:
        raise HTTPException(status_code=400, detail="symbol is required")

    today = datetime.now(timezone.utc).date()
    end_d = parse_date_or_default(end_date, today)
    start_d = parse_date_or_default(start_date, end_d - timedelta(days=365))
    if start_d > end_d:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")

    try:
        rows = load_series_rows([name], universe, interval, start_d, end_d)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    points = [
        {
            "date": str(row["date"]),
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": int(row["volume"]),
        }
        for row in rows
    ]
    return {
        "symbol": name,
        "universe": universe,
        "interval": interval,
        "start_date": start_d.isoformat(),
        "end_date": end_d.isoformat(),
        "points": points,
    }


@app.get("/api/v1/compare")
def compare(
    symbols: str = Query(..., description="Comma-separated symbols or index names."),
    universe: Literal["stock", "index"] = Query("stock"),
    interval: Literal["1d", "1w"] = Query("1w"),
    normalized_base: float = Query(100.0, gt=0.0, le=1000000.0),
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, Any]:
    names = split_symbols(symbols, max_count=6)
    if len(names) < 2:
        raise HTTPException(status_code=400, detail="Provide at least two symbols/index names.")
    today = datetime.now(timezone.utc).date()
    end_d = parse_date_or_default(end_date, today)
    start_d = parse_date_or_default(start_date, end_d - timedelta(days=365))
    if start_d > end_d:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")

    try:
        rows = load_series_rows(names, universe, interval, start_d, end_d)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    grouped: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["symbol"])].append((str(row["date"]), float(row["close"])))

    series_out: list[dict[str, Any]] = []
    for symbol in names:
        points = sorted(grouped.get(symbol, []), key=lambda t: t[0])
        if not points:
            series_out.append({"symbol": symbol, "normalized": []})
            continue
        base = points[0][1]
        if base == 0:
            series_out.append({"symbol": symbol, "normalized": []})
            continue
        normalized = [{"date": d, "value": (v / base) * normalized_base} for d, v in points]
        period_return_pct = ((points[-1][1] / base) - 1.0) * 100.0
        series_out.append(
            {
                "symbol": symbol,
                "base_close": base,
                "normalized_base": normalized_base,
                "period_return_pct": period_return_pct,
                "normalized": normalized,
            }
        )
    return {
        "universe": universe,
        "interval": interval,
        "normalized_base": normalized_base,
        "start_date": start_d.isoformat(),
        "end_date": end_d.isoformat(),
        "series": series_out,
    }


@app.get("/api/v1/correlation")
def correlation(
    symbols: str = Query(..., description="Comma-separated symbols or index names."),
    universe: Literal["stock", "index"] = Query("stock"),
    interval: Literal["1d", "1w"] = Query("1w"),
    window: int = Query(52, ge=10, le=1000),
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, Any]:
    names = split_symbols(symbols, max_count=6)
    if len(names) < 2:
        raise HTTPException(status_code=400, detail="Provide at least two symbols/index names.")

    today = datetime.now(timezone.utc).date()
    end_d = parse_date_or_default(end_date, today)
    default_days = max(365, window * (7 if interval == "1w" else 2))
    start_d = parse_date_or_default(start_date, end_d - timedelta(days=default_days))
    if start_d > end_d:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")

    try:
        rows = load_series_rows(names, universe, interval, start_d, end_d)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    closes: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for row in rows:
        closes[str(row["symbol"])].append((str(row["date"]), float(row["close"])))
    for symbol in closes:
        closes[symbol].sort(key=lambda t: t[0])

    returns_map: dict[str, dict[str, float]] = {}
    for symbol in names:
        points = closes.get(symbol, [])
        ret: dict[str, float] = {}
        for i in range(1, len(points)):
            prev_date, prev_close = points[i - 1]
            curr_date, curr_close = points[i]
            _ = prev_date
            if prev_close == 0:
                continue
            ret[curr_date] = (curr_close / prev_close) - 1.0
        if window > 0 and len(ret) > window:
            keep_dates = sorted(ret.keys())[-window:]
            ret = {k: ret[k] for k in keep_dates}
        returns_map[symbol] = ret

    matrix: list[dict[str, Any]] = []
    for a in names:
        for b in names:
            da = returns_map.get(a, {})
            db = returns_map.get(b, {})
            common_dates = sorted(set(da.keys()).intersection(db.keys()))
            x = [da[d] for d in common_dates]
            y = [db[d] for d in common_dates]
            value = corr(x, y)
            matrix.append(
                {
                    "symbol_a": a,
                    "symbol_b": b,
                    "correlation": value,
                    "observations": len(common_dates),
                }
            )

    return {
        "universe": universe,
        "interval": interval,
        "window": window,
        "start_date": start_d.isoformat(),
        "end_date": end_d.isoformat(),
        "matrix": matrix,
    }
