from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from backfill_bse_prices import (
    clickhouse_query,
    env_or_dotenv,
    load_dotenv,
    make_session,
    validate_identifier,
)


ACTION_TYPES = {"SPLIT", "BONUS", "DIVIDEND", "RIGHTS", "MERGER", "DEMERGER", "OTHER"}


def normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def pick_field(row: dict[str, str], candidates: list[str]) -> str:
    normalized = {normalize_key(k): v for k, v in row.items()}
    for candidate in candidates:
        value = normalized.get(normalize_key(candidate), "").strip()
        if value:
            return value
    return ""


def parse_date(value: str) -> str:
    text = value.strip()
    if not text:
        return ""
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d", "%d %b %Y", "%d %B %Y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    raise ValueError(f"Unsupported date format: {value!r}")


def parse_float(value: str, default: float = 1.0) -> float:
    text = value.strip().replace(",", "")
    if not text:
        return default
    return float(text)


def ensure_phase1_tables(
    *,
    session: Any,
    clickhouse_url: str,
    user: str,
    password: str,
    timeout: int,
    database: str,
    raw_prices_table: str,
    corporate_actions_table: str,
    adjusted_prices_table: str,
) -> None:
    db = validate_identifier(database)
    raw_tbl = validate_identifier(raw_prices_table)
    actions_tbl = validate_identifier(corporate_actions_table)
    adjusted_tbl = validate_identifier(adjusted_prices_table)

    create_db = f"CREATE DATABASE IF NOT EXISTS {db}"
    create_actions = f"""
CREATE TABLE IF NOT EXISTS {db}.{actions_tbl}
(
    exchange LowCardinality(String) DEFAULT 'BSE',
    symbol String,
    bse_code String,
    action_type LowCardinality(String),
    ratio_from Float64 DEFAULT 1.0,
    ratio_to Float64 DEFAULT 1.0,
    ex_date Date,
    announcement_date Date,
    source String,
    notes String,
    fetched_at_utc DateTime,
    ingested_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(ex_date)
ORDER BY (exchange, symbol, ex_date, action_type, source)
""".strip()
    create_adjusted = f"""
CREATE TABLE IF NOT EXISTS {db}.{adjusted_tbl}
(
    exchange LowCardinality(String),
    symbol String,
    bse_code String,
    company_name String,
    date Date,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    adj_factor Float64,
    volume UInt64,
    source LowCardinality(String),
    fetched_at_utc DateTime,
    adjusted_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(adjusted_at)
PARTITION BY toYYYYMM(date)
ORDER BY (exchange, bse_code, date)
""".strip()

    for stmt in (create_db, create_actions, create_adjusted):
        clickhouse_query(
            session=session,
            url=clickhouse_url,
            query=stmt,
            user=user,
            password=password,
            timeout=timeout,
        )

    # Early fail if raw table is missing.
    clickhouse_query(
        session=session,
        url=clickhouse_url,
        query=f"EXISTS TABLE {db}.{raw_tbl} FORMAT JSONEachRow",
        user=user,
        password=password,
        timeout=timeout,
    )


def load_actions_csv(
    *,
    csv_path: Path,
    fetched_at_utc: str,
) -> list[dict[str, Any]]:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    rows: list[dict[str, Any]] = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError("CSV has no header row.")

        for raw_row in reader:
            row = {str(k): str(v or "").strip() for k, v in raw_row.items() if k is not None}
            symbol = pick_field(row, ["symbol", "ticker", "security_id", "scrip_id"]).upper().replace(" ", "")
            bse_code = re.sub(r"\D", "", pick_field(row, ["bse_code", "security_code", "scrip_code", "sc_code"]))
            action_type = pick_field(row, ["action_type", "corporate_action", "event"]).upper()
            ex_date_raw = pick_field(row, ["ex_date", "effective_date", "date"])
            announcement_raw = pick_field(row, ["announcement_date", "ann_date", "declared_date"])
            ratio_from_raw = pick_field(row, ["ratio_from", "from", "old", "numerator"])
            ratio_to_raw = pick_field(row, ["ratio_to", "to", "new", "denominator"])
            source = pick_field(row, ["source", "origin"]) or str(csv_path.name)
            notes = pick_field(row, ["notes", "remark", "remarks", "description"])

            if not action_type:
                action_type = "OTHER"
            if action_type not in ACTION_TYPES:
                action_type = "OTHER"
            if not ex_date_raw:
                continue
            ex_date = parse_date(ex_date_raw)
            announcement_date = parse_date(announcement_raw) if announcement_raw else ex_date

            ratio_from = parse_float(ratio_from_raw, default=1.0)
            ratio_to = parse_float(ratio_to_raw, default=1.0)
            if action_type in {"SPLIT", "BONUS"} and (ratio_from <= 0 or ratio_to <= 0):
                continue
            if not symbol and not bse_code:
                continue

            rows.append(
                {
                    "exchange": "BSE",
                    "symbol": symbol,
                    "bse_code": bse_code,
                    "action_type": action_type,
                    "ratio_from": ratio_from,
                    "ratio_to": ratio_to,
                    "ex_date": ex_date,
                    "announcement_date": announcement_date,
                    "source": source,
                    "notes": notes,
                    "fetched_at_utc": fetched_at_utc,
                }
            )
    return rows


def insert_actions(
    *,
    session: Any,
    clickhouse_url: str,
    user: str,
    password: str,
    timeout: int,
    database: str,
    corporate_actions_table: str,
    rows: list[dict[str, Any]],
) -> int:
    if not rows:
        return 0
    db = validate_identifier(database)
    tbl = validate_identifier(corporate_actions_table)
    query = (
        f"INSERT INTO {db}.{tbl} "
        "(exchange,symbol,bse_code,action_type,ratio_from,ratio_to,ex_date,announcement_date,source,notes,fetched_at_utc) "
        "FORMAT JSONEachRow"
    )
    by_month: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        ex_date = str(row.get("ex_date", ""))
        month_key = ex_date[:7] if len(ex_date) >= 7 else "unknown"
        by_month.setdefault(month_key, []).append(row)

    inserted = 0
    for month_key in sorted(by_month.keys()):
        chunk = by_month[month_key]
        payload = "\n".join(json.dumps(row, ensure_ascii=False) for row in chunk).encode("utf-8")
        clickhouse_query(
            session=session,
            url=clickhouse_url,
            query=query,
            user=user,
            password=password,
            timeout=max(timeout, 120),
            data=payload,
            content_type="application/json",
        )
        inserted += len(chunk)
    return inserted


def first_day_of_month(day: date) -> date:
    return day.replace(day=1)


def next_month(day: date) -> date:
    if day.month == 12:
        return day.replace(year=day.year + 1, month=1, day=1)
    return day.replace(month=day.month + 1, day=1)


def month_end(day: date) -> date:
    return next_month(day) - timedelta(days=1)


def fetch_raw_min_max_date(
    *,
    session: Any,
    clickhouse_url: str,
    user: str,
    password: str,
    timeout: int,
    database: str,
    raw_prices_table: str,
) -> tuple[date | None, date | None]:
    return fetch_table_min_max_date(
        session=session,
        clickhouse_url=clickhouse_url,
        user=user,
        password=password,
        timeout=timeout,
        database=database,
        table_name=raw_prices_table,
        date_column="date",
    )


def fetch_table_min_max_date(
    *,
    session: Any,
    clickhouse_url: str,
    user: str,
    password: str,
    timeout: int,
    database: str,
    table_name: str,
    date_column: str,
) -> tuple[date | None, date | None]:
    db = validate_identifier(database)
    tbl = validate_identifier(table_name)
    date_col = validate_identifier(date_column)
    query = f"SELECT min({date_col}) AS min_date, max({date_col}) AS max_date FROM {db}.{tbl} FORMAT JSONEachRow"
    text = clickhouse_query(
        session=session,
        url=clickhouse_url,
        query=query,
        user=user,
        password=password,
        timeout=timeout,
    ).strip()
    if not text:
        return None, None
    row = json.loads(text.splitlines()[0])
    min_raw = str(row.get("min_date") or "").strip()
    max_raw = str(row.get("max_date") or "").strip()
    min_date = datetime.strptime(min_raw, "%Y-%m-%d").date() if min_raw else None
    max_date = datetime.strptime(max_raw, "%Y-%m-%d").date() if max_raw else None
    return min_date, max_date


def week_start(day: date) -> date:
    return day - timedelta(days=day.weekday())


def rebuild_adjusted_prices(
    *,
    session: Any,
    clickhouse_url: str,
    user: str,
    password: str,
    timeout: int,
    database: str,
    raw_prices_table: str,
    corporate_actions_table: str,
    adjusted_prices_table: str,
    start_date: str,
    end_date: str,
    truncate_first: bool,
) -> None:
    db = validate_identifier(database)
    raw_tbl = validate_identifier(raw_prices_table)
    actions_tbl = validate_identifier(corporate_actions_table)
    adjusted_tbl = validate_identifier(adjusted_prices_table)

    if truncate_first:
        clickhouse_query(
            session=session,
            url=clickhouse_url,
            query=f"TRUNCATE TABLE {db}.{adjusted_tbl}",
            user=user,
            password=password,
            timeout=timeout,
        )

    user_start = datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else None
    user_end = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else None
    min_raw, max_raw = fetch_raw_min_max_date(
        session=session,
        clickhouse_url=clickhouse_url,
        user=user,
        password=password,
        timeout=timeout,
        database=database,
        raw_prices_table=raw_prices_table,
    )
    effective_start = user_start or min_raw
    effective_end = user_end or max_raw
    if effective_start is None or effective_end is None:
        return
    if effective_start > effective_end:
        return

    month_cursor = first_day_of_month(effective_start)
    end_month = first_day_of_month(effective_end)
    while month_cursor <= end_month:
        chunk_start = max(effective_start, month_cursor)
        chunk_end = min(effective_end, month_end(month_cursor))
        date_filter = f"p.date BETWEEN '{chunk_start.isoformat()}' AND '{chunk_end.isoformat()}'"

        query = f"""
INSERT INTO {db}.{adjusted_tbl}
(exchange,symbol,bse_code,company_name,date,open,high,low,close,adj_factor,volume,source,fetched_at_utc)
WITH actions AS
(
    SELECT
        bse_code,
        upperUTF8(symbol) AS symbol_key,
        ex_date,
        argMax(ratio_from, ingested_at) AS ratio_from_latest,
        argMax(ratio_to, ingested_at) AS ratio_to_latest
    FROM {db}.{actions_tbl}
    WHERE upperUTF8(action_type) IN ('SPLIT', 'BONUS')
    GROUP BY bse_code, symbol_key, ex_date
    HAVING ratio_from_latest > 0
       AND ratio_to_latest > 0
)
SELECT
    p.exchange,
    p.symbol,
    p.bse_code,
    p.company_name,
    p.date,
    p.open * p.adj_factor AS open,
    p.high * p.adj_factor AS high,
    p.low * p.adj_factor AS low,
    p.close * p.adj_factor AS close,
    p.adj_factor,
    toUInt64(round(p.volume / greatest(p.adj_factor, 1e-12))) AS volume,
    'ADJUSTED_SPLIT_BONUS' AS source,
    now() AS fetched_at_utc
FROM
(
    SELECT
        p.exchange,
        p.symbol,
        p.bse_code,
        p.company_name,
        p.date,
        p.open,
        p.high,
        p.low,
        p.close,
        p.volume,
        exp(
            sum(
                if(
                    a.ratio_from_latest > 0 AND a.ratio_to_latest > 0,
                    log(greatest(a.ratio_from_latest / a.ratio_to_latest, 1e-12)),
                    0.0
                )
            )
        ) AS adj_factor
    FROM {db}.{raw_tbl} p
    LEFT JOIN actions a
        ON (
            (a.bse_code != '' AND a.bse_code = p.bse_code)
            OR (a.bse_code = '' AND a.symbol_key = upperUTF8(p.symbol))
        )
       AND p.date < a.ex_date
    WHERE {date_filter}
    GROUP BY
        p.exchange, p.symbol, p.bse_code, p.company_name, p.date,
        p.open, p.high, p.low, p.close, p.volume
) p
ORDER BY p.exchange, p.bse_code, p.date
""".strip()

        clickhouse_query(
            session=session,
            url=clickhouse_url,
            query=query,
            user=user,
            password=password,
            timeout=max(timeout, 300),
        )
        month_cursor = next_month(month_cursor)


def ensure_phase1_aggregation_tables(
    *,
    session: Any,
    clickhouse_url: str,
    user: str,
    password: str,
    timeout: int,
    database: str,
    weekly_prices_table: str,
    monthly_prices_table: str,
    universe_snapshot_table: str,
) -> None:
    db = validate_identifier(database)
    weekly_tbl = validate_identifier(weekly_prices_table)
    monthly_tbl = validate_identifier(monthly_prices_table)
    universe_tbl = validate_identifier(universe_snapshot_table)

    create_weekly = f"""
CREATE TABLE IF NOT EXISTS {db}.{weekly_tbl}
(
    exchange LowCardinality(String),
    symbol String,
    bse_code String,
    company_name String,
    week_start Date,
    week_end Date,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume UInt64,
    avg_daily_volume Float64,
    bars UInt16,
    source LowCardinality(String),
    fetched_at_utc DateTime,
    ingested_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(week_start)
ORDER BY (exchange, bse_code, week_start)
""".strip()

    create_monthly = f"""
CREATE TABLE IF NOT EXISTS {db}.{monthly_tbl}
(
    exchange LowCardinality(String),
    symbol String,
    bse_code String,
    company_name String,
    month_start Date,
    month_end Date,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume UInt64,
    avg_daily_volume Float64,
    bars UInt16,
    source LowCardinality(String),
    fetched_at_utc DateTime,
    ingested_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(month_start)
ORDER BY (exchange, bse_code, month_start)
""".strip()

    create_universe = f"""
CREATE TABLE IF NOT EXISTS {db}.{universe_tbl}
(
    week_start Date,
    exchange LowCardinality(String),
    symbol String,
    bse_code String,
    company_name String,
    is_active UInt8,
    avg_volume_20d Float64,
    last_close Float64,
    last_trade_date Date,
    source LowCardinality(String),
    fetched_at_utc DateTime,
    ingested_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(week_start)
ORDER BY (week_start, exchange, bse_code)
""".strip()

    for stmt in (create_weekly, create_monthly, create_universe):
        clickhouse_query(
            session=session,
            url=clickhouse_url,
            query=stmt,
            user=user,
            password=password,
            timeout=timeout,
        )


def rebuild_weekly_prices(
    *,
    session: Any,
    clickhouse_url: str,
    user: str,
    password: str,
    timeout: int,
    database: str,
    adjusted_prices_table: str,
    weekly_prices_table: str,
    start_date: str,
    end_date: str,
    truncate_first: bool,
) -> tuple[date | None, date | None]:
    db = validate_identifier(database)
    adjusted_tbl = validate_identifier(adjusted_prices_table)
    weekly_tbl = validate_identifier(weekly_prices_table)

    if truncate_first:
        clickhouse_query(
            session=session,
            url=clickhouse_url,
            query=f"TRUNCATE TABLE {db}.{weekly_tbl}",
            user=user,
            password=password,
            timeout=timeout,
        )

    user_start = datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else None
    user_end = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else None
    min_src, max_src = fetch_table_min_max_date(
        session=session,
        clickhouse_url=clickhouse_url,
        user=user,
        password=password,
        timeout=timeout,
        database=database,
        table_name=adjusted_prices_table,
        date_column="date",
    )
    effective_start = user_start or min_src
    effective_end = user_end or max_src
    if effective_start is None or effective_end is None or effective_start > effective_end:
        return None, None

    week_min = week_start(effective_start)
    week_max = week_start(effective_end)
    month_cursor = first_day_of_month(week_min)
    end_month = first_day_of_month(week_max)
    while month_cursor <= end_month:
        chunk_start = month_cursor
        chunk_end = month_end(month_cursor)
        query = f"""
INSERT INTO {db}.{weekly_tbl}
(exchange,symbol,bse_code,company_name,week_start,week_end,open,high,low,close,volume,avg_daily_volume,bars,source,fetched_at_utc)
SELECT
    p.exchange,
    p.symbol,
    p.bse_code,
    argMax(p.company_name, p.date) AS company_name,
    toDate(toStartOfWeek(p.date, 1)) AS week_start,
    max(p.date) AS week_end,
    argMin(p.open, p.date) AS open,
    max(p.high) AS high,
    min(p.low) AS low,
    argMax(p.close, p.date) AS close,
    toUInt64(sum(p.volume)) AS volume,
    avg(toFloat64(p.volume)) AS avg_daily_volume,
    toUInt16(count()) AS bars,
    'WEEKLY_FROM_ADJUSTED' AS source,
    now() AS fetched_at_utc
FROM {db}.{adjusted_tbl} p
WHERE p.date BETWEEN '{effective_start.isoformat()}' AND '{effective_end.isoformat()}'
  AND toDate(toStartOfWeek(p.date, 1)) BETWEEN '{chunk_start.isoformat()}' AND '{chunk_end.isoformat()}'
GROUP BY p.exchange, p.symbol, p.bse_code, week_start
ORDER BY p.exchange, p.bse_code, week_start
""".strip()
        clickhouse_query(
            session=session,
            url=clickhouse_url,
            query=query,
            user=user,
            password=password,
            timeout=max(timeout, 300),
        )
        month_cursor = next_month(month_cursor)
    return effective_start, effective_end


def rebuild_monthly_prices(
    *,
    session: Any,
    clickhouse_url: str,
    user: str,
    password: str,
    timeout: int,
    database: str,
    adjusted_prices_table: str,
    monthly_prices_table: str,
    start_date: str,
    end_date: str,
    truncate_first: bool,
) -> tuple[date | None, date | None]:
    db = validate_identifier(database)
    adjusted_tbl = validate_identifier(adjusted_prices_table)
    monthly_tbl = validate_identifier(monthly_prices_table)

    if truncate_first:
        clickhouse_query(
            session=session,
            url=clickhouse_url,
            query=f"TRUNCATE TABLE {db}.{monthly_tbl}",
            user=user,
            password=password,
            timeout=timeout,
        )

    user_start = datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else None
    user_end = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else None
    min_src, max_src = fetch_table_min_max_date(
        session=session,
        clickhouse_url=clickhouse_url,
        user=user,
        password=password,
        timeout=timeout,
        database=database,
        table_name=adjusted_prices_table,
        date_column="date",
    )
    effective_start = user_start or min_src
    effective_end = user_end or max_src
    if effective_start is None or effective_end is None or effective_start > effective_end:
        return None, None

    month_cursor = first_day_of_month(effective_start)
    end_month = first_day_of_month(effective_end)
    while month_cursor <= end_month:
        query = f"""
INSERT INTO {db}.{monthly_tbl}
(exchange,symbol,bse_code,company_name,month_start,month_end,open,high,low,close,volume,avg_daily_volume,bars,source,fetched_at_utc)
SELECT
    p.exchange,
    p.symbol,
    p.bse_code,
    argMax(p.company_name, p.date) AS company_name,
    toDate(toStartOfMonth(p.date)) AS month_start,
    max(p.date) AS month_end,
    argMin(p.open, p.date) AS open,
    max(p.high) AS high,
    min(p.low) AS low,
    argMax(p.close, p.date) AS close,
    toUInt64(sum(p.volume)) AS volume,
    avg(toFloat64(p.volume)) AS avg_daily_volume,
    toUInt16(count()) AS bars,
    'MONTHLY_FROM_ADJUSTED' AS source,
    now() AS fetched_at_utc
FROM {db}.{adjusted_tbl} p
WHERE p.date BETWEEN '{effective_start.isoformat()}' AND '{effective_end.isoformat()}'
  AND toDate(toStartOfMonth(p.date)) = '{month_cursor.isoformat()}'
GROUP BY p.exchange, p.symbol, p.bse_code, month_start
ORDER BY p.exchange, p.bse_code, month_start
""".strip()
        clickhouse_query(
            session=session,
            url=clickhouse_url,
            query=query,
            user=user,
            password=password,
            timeout=max(timeout, 300),
        )
        month_cursor = next_month(month_cursor)
    return effective_start, effective_end


def rebuild_universe_snapshot(
    *,
    session: Any,
    clickhouse_url: str,
    user: str,
    password: str,
    timeout: int,
    database: str,
    adjusted_prices_table: str,
    universe_snapshot_table: str,
    start_date: str,
    end_date: str,
    truncate_first: bool,
) -> tuple[date | None, date | None]:
    db = validate_identifier(database)
    adjusted_tbl = validate_identifier(adjusted_prices_table)
    universe_tbl = validate_identifier(universe_snapshot_table)

    if truncate_first:
        clickhouse_query(
            session=session,
            url=clickhouse_url,
            query=f"TRUNCATE TABLE {db}.{universe_tbl}",
            user=user,
            password=password,
            timeout=timeout,
        )

    user_start = datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else None
    user_end = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else None
    min_src, max_src = fetch_table_min_max_date(
        session=session,
        clickhouse_url=clickhouse_url,
        user=user,
        password=password,
        timeout=timeout,
        database=database,
        table_name=adjusted_prices_table,
        date_column="date",
    )
    effective_start = user_start or min_src
    effective_end = user_end or max_src
    if effective_start is None or effective_end is None or effective_start > effective_end:
        return None, None

    week_min = week_start(effective_start)
    week_max = week_start(effective_end)
    month_cursor = first_day_of_month(week_min)
    end_month = first_day_of_month(week_max)
    lookback_start = effective_start - timedelta(days=90)
    while month_cursor <= end_month:
        chunk_start = month_cursor
        chunk_end = month_end(month_cursor)
        base_start = max(lookback_start, chunk_start - timedelta(days=90))
        base_end = min(effective_end, chunk_end + timedelta(days=6))
        query = f"""
INSERT INTO {db}.{universe_tbl}
(week_start,exchange,symbol,bse_code,company_name,is_active,avg_volume_20d,last_close,last_trade_date,source,fetched_at_utc)
WITH base AS
(
    SELECT
        p.exchange,
        p.symbol,
        p.bse_code,
        p.company_name,
        p.date,
        p.close,
        toFloat64(p.volume) AS volume,
        toDate(toStartOfWeek(p.date, 1)) AS bucket_week_start,
        avg(toFloat64(p.volume)) OVER (
            PARTITION BY p.exchange, p.bse_code
            ORDER BY p.date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS avg_volume_20d
    FROM {db}.{adjusted_tbl} p
    WHERE p.date BETWEEN '{base_start.isoformat()}' AND '{base_end.isoformat()}'
)
SELECT
    bucket_week_start AS week_start,
    exchange,
    symbol,
    bse_code,
    argMax(company_name, date) AS company_name,
    toUInt8(1) AS is_active,
    argMax(avg_volume_20d, date) AS avg_volume_20d,
    argMax(close, date) AS last_close,
    max(date) AS last_trade_date,
    'UNIVERSE_SNAPSHOT_FROM_ADJUSTED' AS source,
    now() AS fetched_at_utc
FROM base
WHERE bucket_week_start BETWEEN '{chunk_start.isoformat()}' AND '{chunk_end.isoformat()}'
  AND date BETWEEN '{effective_start.isoformat()}' AND '{effective_end.isoformat()}'
GROUP BY bucket_week_start, exchange, symbol, bse_code
ORDER BY bucket_week_start, exchange, bse_code
""".strip()
        clickhouse_query(
            session=session,
            url=clickhouse_url,
            query=query,
            user=user,
            password=password,
            timeout=max(timeout, 300),
        )
        month_cursor = next_month(month_cursor)
    return effective_start, effective_end


def validate_adjustments(
    *,
    session: Any,
    clickhouse_url: str,
    user: str,
    password: str,
    timeout: int,
    database: str,
    raw_prices_table: str,
    corporate_actions_table: str,
    adjusted_prices_table: str,
    limit: int,
) -> list[dict[str, Any]]:
    db = validate_identifier(database)
    raw_tbl = validate_identifier(raw_prices_table)
    actions_tbl = validate_identifier(corporate_actions_table)
    adjusted_tbl = validate_identifier(adjusted_prices_table)
    query = f"""
SELECT
    ca.symbol,
    ca.bse_code,
    ca.ex_date,
    ca.ratio_from,
    ca.ratio_to,
    anyIf(raw.close, raw.date = ca.ex_date - 1) AS raw_prev_close,
    anyIf(raw.close, raw.date = ca.ex_date) AS raw_ex_close,
    anyIf(adj.close, adj.date = ca.ex_date - 1) AS adj_prev_close,
    anyIf(adj.close, adj.date = ca.ex_date) AS adj_ex_close
FROM {db}.{actions_tbl} ca
LEFT JOIN {db}.{raw_tbl} raw
    ON raw.symbol = ca.symbol
   AND raw.date IN (ca.ex_date - 1, ca.ex_date)
LEFT JOIN {db}.{adjusted_tbl} adj
    ON adj.symbol = ca.symbol
   AND adj.date IN (ca.ex_date - 1, ca.ex_date)
WHERE upperUTF8(ca.action_type) IN ('SPLIT', 'BONUS')
GROUP BY ca.symbol, ca.bse_code, ca.ex_date, ca.ratio_from, ca.ratio_to
ORDER BY ca.ex_date DESC, ca.symbol
LIMIT {max(1, int(limit))}
FORMAT JSONEachRow
""".strip()
    text = clickhouse_query(
        session=session,
        url=clickhouse_url,
        query=query,
        user=user,
        password=password,
        timeout=timeout,
    )
    out: list[dict[str, Any]] = []
    for line in text.splitlines():
        line = line.strip()
        if line:
            out.append(json.loads(line))
    return out


def parse_args() -> argparse.Namespace:
    dotenv = load_dotenv(Path(".env"))
    parser = argparse.ArgumentParser(description="Phase 1 data hardening utilities.")
    parser.add_argument("--clickhouse-url", default=env_or_dotenv(dotenv, "CLICKHOUSE_URL", default="http://localhost:8123"))
    parser.add_argument("--database", default=env_or_dotenv(dotenv, "CLICKHOUSE_DATABASE", default="market"))
    parser.add_argument("--raw-prices-table", default=env_or_dotenv(dotenv, "OHLCV_TABLE", default="DailyPricesBhavcopy"))
    parser.add_argument("--corporate-actions-table", default=env_or_dotenv(dotenv, "CORPORATE_ACTIONS_TABLE", default="CorporateActions"))
    parser.add_argument("--adjusted-prices-table", default=env_or_dotenv(dotenv, "ADJUSTED_PRICES_TABLE", default="AdjustedDailyPrices"))
    parser.add_argument("--weekly-prices-table", default=env_or_dotenv(dotenv, "WEEKLY_PRICES_TABLE", default="WeeklyPrices"))
    parser.add_argument("--monthly-prices-table", default=env_or_dotenv(dotenv, "MONTHLY_PRICES_TABLE", default="MonthlyPrices"))
    parser.add_argument("--universe-snapshot-table", default=env_or_dotenv(dotenv, "UNIVERSE_SNAPSHOT_TABLE", default="UniverseSnapshot"))
    parser.add_argument("--user", default=env_or_dotenv(dotenv, "CLICKHOUSE_USER", "CH_USER", default="default"))
    parser.add_argument("--password", default=env_or_dotenv(dotenv, "CLICKHOUSE_PASSWORD", "CH_PASSWORD", default=""))
    parser.add_argument("--timeout", type=int, default=60)

    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-tables", help="Create CorporateActions and AdjustedDailyPrices tables.")

    load = sub.add_parser("load-actions", help="Load corporate actions from CSV.")
    load.add_argument("--csv", required=True, help="Path to corporate actions CSV.")

    rebuild = sub.add_parser("rebuild-adjusted", help="Rebuild adjusted prices table.")
    rebuild.add_argument("--start-date", default="", help="Optional start date YYYY-MM-DD.")
    rebuild.add_argument("--end-date", default="", help="Optional end date YYYY-MM-DD.")
    rebuild.add_argument("--truncate-first", action="store_true", help="Truncate adjusted table before insert.")

    sub.add_parser("init-agg-tables", help="Create WeeklyPrices, MonthlyPrices, and UniverseSnapshot tables.")

    rebuild_agg = sub.add_parser("rebuild-aggregates", help="Rebuild weekly/monthly/universe aggregates from adjusted prices.")
    rebuild_agg.add_argument("--start-date", default="", help="Optional start date YYYY-MM-DD.")
    rebuild_agg.add_argument("--end-date", default="", help="Optional end date YYYY-MM-DD.")
    rebuild_agg.add_argument("--truncate-first", action="store_true", help="Truncate selected aggregate tables before insert.")
    rebuild_agg.add_argument("--skip-weekly", action="store_true", help="Skip WeeklyPrices rebuild.")
    rebuild_agg.add_argument("--skip-monthly", action="store_true", help="Skip MonthlyPrices rebuild.")
    rebuild_agg.add_argument("--skip-universe", action="store_true", help="Skip UniverseSnapshot rebuild.")

    validate = sub.add_parser("validate-splits", help="Sample split/bonus continuity checks.")
    validate.add_argument("--limit", type=int, default=20)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    session = make_session()

    ensure_phase1_tables(
        session=session,
        clickhouse_url=args.clickhouse_url,
        user=args.user,
        password=args.password,
        timeout=args.timeout,
        database=args.database,
        raw_prices_table=args.raw_prices_table,
        corporate_actions_table=args.corporate_actions_table,
        adjusted_prices_table=args.adjusted_prices_table,
    )

    if args.command == "init-tables":
        print(
            json.dumps(
                {
                    "status": "ok",
                    "database": args.database,
                    "corporate_actions_table": args.corporate_actions_table,
                    "adjusted_prices_table": args.adjusted_prices_table,
                },
                indent=2,
            )
        )
        return

    if args.command == "load-actions":
        fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        rows = load_actions_csv(csv_path=Path(args.csv), fetched_at_utc=fetched_at)
        inserted = insert_actions(
            session=session,
            clickhouse_url=args.clickhouse_url,
            user=args.user,
            password=args.password,
            timeout=args.timeout,
            database=args.database,
            corporate_actions_table=args.corporate_actions_table,
            rows=rows,
        )
        print(
            json.dumps(
                {
                    "status": "ok",
                    "csv": str(Path(args.csv).resolve()),
                    "rows_parsed": len(rows),
                    "rows_inserted": inserted,
                    "target": f"{args.database}.{args.corporate_actions_table}",
                },
                indent=2,
            )
        )
        return

    if args.command == "rebuild-adjusted":
        rebuild_adjusted_prices(
            session=session,
            clickhouse_url=args.clickhouse_url,
            user=args.user,
            password=args.password,
            timeout=args.timeout,
            database=args.database,
            raw_prices_table=args.raw_prices_table,
            corporate_actions_table=args.corporate_actions_table,
            adjusted_prices_table=args.adjusted_prices_table,
            start_date=args.start_date,
            end_date=args.end_date,
            truncate_first=bool(args.truncate_first),
        )
        print(
            json.dumps(
                {
                    "status": "ok",
                    "target": f"{args.database}.{args.adjusted_prices_table}",
                    "source": f"{args.database}.{args.raw_prices_table}",
                    "actions": f"{args.database}.{args.corporate_actions_table}",
                    "start_date": args.start_date or None,
                    "end_date": args.end_date or None,
                    "truncate_first": bool(args.truncate_first),
                },
                indent=2,
            )
        )
        return

    if args.command == "init-agg-tables":
        ensure_phase1_aggregation_tables(
            session=session,
            clickhouse_url=args.clickhouse_url,
            user=args.user,
            password=args.password,
            timeout=args.timeout,
            database=args.database,
            weekly_prices_table=args.weekly_prices_table,
            monthly_prices_table=args.monthly_prices_table,
            universe_snapshot_table=args.universe_snapshot_table,
        )
        print(
            json.dumps(
                {
                    "status": "ok",
                    "database": args.database,
                    "weekly_prices_table": args.weekly_prices_table,
                    "monthly_prices_table": args.monthly_prices_table,
                    "universe_snapshot_table": args.universe_snapshot_table,
                },
                indent=2,
            )
        )
        return

    if args.command == "rebuild-aggregates":
        ensure_phase1_aggregation_tables(
            session=session,
            clickhouse_url=args.clickhouse_url,
            user=args.user,
            password=args.password,
            timeout=args.timeout,
            database=args.database,
            weekly_prices_table=args.weekly_prices_table,
            monthly_prices_table=args.monthly_prices_table,
            universe_snapshot_table=args.universe_snapshot_table,
        )
        do_weekly = not bool(args.skip_weekly)
        do_monthly = not bool(args.skip_monthly)
        do_universe = not bool(args.skip_universe)
        if not (do_weekly or do_monthly or do_universe):
            raise ValueError("All aggregate targets are skipped. Remove at least one --skip-* flag.")

        weekly_range: tuple[date | None, date | None] = (None, None)
        monthly_range: tuple[date | None, date | None] = (None, None)
        universe_range: tuple[date | None, date | None] = (None, None)

        if do_weekly:
            weekly_range = rebuild_weekly_prices(
                session=session,
                clickhouse_url=args.clickhouse_url,
                user=args.user,
                password=args.password,
                timeout=args.timeout,
                database=args.database,
                adjusted_prices_table=args.adjusted_prices_table,
                weekly_prices_table=args.weekly_prices_table,
                start_date=args.start_date,
                end_date=args.end_date,
                truncate_first=bool(args.truncate_first),
            )
        if do_monthly:
            monthly_range = rebuild_monthly_prices(
                session=session,
                clickhouse_url=args.clickhouse_url,
                user=args.user,
                password=args.password,
                timeout=args.timeout,
                database=args.database,
                adjusted_prices_table=args.adjusted_prices_table,
                monthly_prices_table=args.monthly_prices_table,
                start_date=args.start_date,
                end_date=args.end_date,
                truncate_first=bool(args.truncate_first),
            )
        if do_universe:
            universe_range = rebuild_universe_snapshot(
                session=session,
                clickhouse_url=args.clickhouse_url,
                user=args.user,
                password=args.password,
                timeout=args.timeout,
                database=args.database,
                adjusted_prices_table=args.adjusted_prices_table,
                universe_snapshot_table=args.universe_snapshot_table,
                start_date=args.start_date,
                end_date=args.end_date,
                truncate_first=bool(args.truncate_first),
            )

        print(
            json.dumps(
                {
                    "status": "ok",
                    "source": f"{args.database}.{args.adjusted_prices_table}",
                    "weekly": {
                        "enabled": do_weekly,
                        "target": f"{args.database}.{args.weekly_prices_table}" if do_weekly else None,
                        "start_date": weekly_range[0].isoformat() if weekly_range[0] else None,
                        "end_date": weekly_range[1].isoformat() if weekly_range[1] else None,
                    },
                    "monthly": {
                        "enabled": do_monthly,
                        "target": f"{args.database}.{args.monthly_prices_table}" if do_monthly else None,
                        "start_date": monthly_range[0].isoformat() if monthly_range[0] else None,
                        "end_date": monthly_range[1].isoformat() if monthly_range[1] else None,
                    },
                    "universe": {
                        "enabled": do_universe,
                        "target": f"{args.database}.{args.universe_snapshot_table}" if do_universe else None,
                        "start_date": universe_range[0].isoformat() if universe_range[0] else None,
                        "end_date": universe_range[1].isoformat() if universe_range[1] else None,
                    },
                    "truncate_first": bool(args.truncate_first),
                },
                indent=2,
            )
        )
        return

    if args.command == "validate-splits":
        rows = validate_adjustments(
            session=session,
            clickhouse_url=args.clickhouse_url,
            user=args.user,
            password=args.password,
            timeout=args.timeout,
            database=args.database,
            raw_prices_table=args.raw_prices_table,
            corporate_actions_table=args.corporate_actions_table,
            adjusted_prices_table=args.adjusted_prices_table,
            limit=args.limit,
        )
        print(json.dumps({"status": "ok", "rows": rows}, indent=2))
        return


if __name__ == "__main__":
    main()
