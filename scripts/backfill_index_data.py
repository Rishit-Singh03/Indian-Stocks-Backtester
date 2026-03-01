from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from backfill_bse_prices import (
    clickhouse_query,
    env_or_dotenv,
    load_dotenv,
    make_session,
    parse_yyyy_mm_dd,
    validate_identifier,
    year_shifted,
)

DEFAULT_INDEX_MAP: dict[str, str] = {
    "NIFTY_50": "^NSEI",
    "SENSEX": "^BSESN",
    "NIFTY_BANK": "^NSEBANK",
    "NIFTY_IT": "^CNXIT",
    "NIFTY_AUTO": "^CNXAUTO",
    "NIFTY_PHARMA": "^CNXPHARMA",
    "NIFTY_METAL": "^CNXMETAL",
    "NIFTY_REALTY": "^CNXREALTY",
}


def fetch_prices_table_range(
    session: Any,
    clickhouse_url: str,
    user: str,
    password: str,
    timeout: int,
    database: str,
    prices_table: str,
) -> tuple[date | None, date | None]:
    db = validate_identifier(database)
    tbl = validate_identifier(prices_table)
    query = f"SELECT min(date) AS min_date, max(date) AS max_date FROM {db}.{tbl} FORMAT JSONEachRow"
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
    min_raw = row.get("min_date")
    max_raw = row.get("max_date")
    if not min_raw or str(min_raw) == "0000-00-00":
        return None, None
    if not max_raw or str(max_raw) == "0000-00-00":
        return None, None
    return parse_yyyy_mm_dd(str(min_raw)), parse_yyyy_mm_dd(str(max_raw))


def resolve_date_range(
    args: argparse.Namespace,
    session: Any,
) -> tuple[date, date]:
    if args.start_date:
        start_date = parse_yyyy_mm_dd(args.start_date)
    else:
        start_date = None
    if args.end_date:
        end_date = parse_yyyy_mm_dd(args.end_date)
    else:
        end_date = None

    if start_date is None or end_date is None:
        min_d, max_d = fetch_prices_table_range(
            session=session,
            clickhouse_url=args.clickhouse_url,
            user=args.user,
            password=args.password,
            timeout=args.timeout,
            database=args.database,
            prices_table=args.align_prices_table,
        )
        if start_date is None:
            start_date = min_d
        if end_date is None:
            end_date = max_d

    if end_date is None:
        end_date = datetime.now(timezone.utc).date()
    if start_date is None:
        start_date = year_shifted(end_date, args.years)

    if start_date > end_date:
        raise ValueError("start_date must be <= end_date")
    return start_date, end_date


def parse_index_map(args: argparse.Namespace) -> dict[str, str]:
    if args.index_map_json:
        try:
            value = json.loads(args.index_map_json)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid --index-map-json: {exc}") from exc
        if not isinstance(value, dict):
            raise ValueError("--index-map-json must be a JSON object")
        out: dict[str, str] = {}
        for k, v in value.items():
            name = str(k).strip()
            ticker = str(v).strip()
            if name and ticker:
                out[name] = ticker
        if not out:
            raise ValueError("--index-map-json produced an empty mapping")
        return out
    return DEFAULT_INDEX_MAP.copy()


def ensure_index_table(
    session: Any,
    clickhouse_url: str,
    user: str,
    password: str,
    timeout: int,
    database: str,
    index_table: str,
) -> None:
    db = validate_identifier(database)
    tbl = validate_identifier(index_table)
    ddl = f"""
CREATE TABLE IF NOT EXISTS {db}.{tbl}
(
    index_name String,
    provider_ticker String,
    date Date,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    adj_close Float64,
    volume UInt64,
    source LowCardinality(String),
    fetched_at_utc DateTime
)
ENGINE = ReplacingMergeTree(fetched_at_utc)
PARTITION BY toYYYYMM(date)
ORDER BY (index_name, date)
""".strip()
    clickhouse_query(
        session=session,
        url=clickhouse_url,
        query=ddl,
        user=user,
        password=password,
        timeout=timeout,
    )


def truncate_table(
    session: Any,
    clickhouse_url: str,
    user: str,
    password: str,
    timeout: int,
    database: str,
    table: str,
) -> None:
    db = validate_identifier(database)
    tbl = validate_identifier(table)
    clickhouse_query(
        session=session,
        url=clickhouse_url,
        query=f"TRUNCATE TABLE {db}.{tbl}",
        user=user,
        password=password,
        timeout=timeout,
    )


def normalize_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:
        return None
    return number


def fetch_index_rows(
    index_name: str,
    provider_ticker: str,
    start_date: date,
    end_date: date,
    fetched_at_utc: str,
) -> list[dict[str, Any]]:
    try:
        import yfinance as yf
    except ImportError as exc:
        raise RuntimeError("Missing dependency 'yfinance'. Run `uv sync` and retry.") from exc

    frame = yf.download(
        tickers=provider_ticker,
        start=start_date.isoformat(),
        end=(end_date + timedelta(days=1)).isoformat(),
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
    )
    if frame is None or frame.empty:
        return []

    if hasattr(frame.columns, "nlevels") and frame.columns.nlevels > 1:
        frame.columns = frame.columns.get_level_values(0)

    rows: list[dict[str, Any]] = []
    for idx, row in frame.iterrows():
        open_v = normalize_float(row.get("Open"))
        high_v = normalize_float(row.get("High"))
        low_v = normalize_float(row.get("Low"))
        close_v = normalize_float(row.get("Close"))
        adj_close_v = normalize_float(row.get("Adj Close"))
        vol_v = normalize_float(row.get("Volume"))
        if open_v is None or high_v is None or low_v is None or close_v is None:
            continue
        if adj_close_v is None:
            adj_close_v = close_v
        volume = int(vol_v) if (vol_v is not None and vol_v > 0) else 0
        rows.append(
            {
                "index_name": index_name,
                "provider_ticker": provider_ticker,
                "date": idx.date().isoformat(),
                "open": open_v,
                "high": high_v,
                "low": low_v,
                "close": close_v,
                "adj_close": adj_close_v,
                "volume": volume,
                "source": "YFINANCE_INDEX",
                "fetched_at_utc": fetched_at_utc,
            }
        )
    return rows


def insert_rows(
    session: Any,
    clickhouse_url: str,
    user: str,
    password: str,
    timeout: int,
    database: str,
    index_table: str,
    rows: list[dict[str, Any]],
    batch_size: int,
) -> int:
    if not rows:
        return 0
    db = validate_identifier(database)
    tbl = validate_identifier(index_table)
    query = (
        f"INSERT INTO {db}.{tbl} "
        "(index_name,provider_ticker,date,open,high,low,close,adj_close,volume,source,fetched_at_utc) "
        "FORMAT JSONEachRow"
    )
    monthly_buckets: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        month_key = str(row["date"])[:7]  # YYYY-MM, aligns with toYYYYMM partitioning
        monthly_buckets.setdefault(month_key, []).append(row)

    inserted = 0
    for month_key in sorted(monthly_buckets.keys()):
        month_rows = monthly_buckets[month_key]
        for i in range(0, len(month_rows), batch_size):
            chunk = month_rows[i : i + batch_size]
            payload = "\n".join(json.dumps(row, ensure_ascii=False) for row in chunk).encode("utf-8")
            clickhouse_query(
                session=session,
                url=clickhouse_url,
                query=query,
                user=user,
                password=password,
                timeout=timeout,
                data=payload,
                content_type="application/json",
            )
            inserted += len(chunk)
    return inserted


def parse_args() -> argparse.Namespace:
    dotenv = load_dotenv(Path(".env"))
    parser = argparse.ArgumentParser(
        description="Backfill Indian index daily OHLCV and load into ClickHouse."
    )
    parser.add_argument(
        "--clickhouse-url",
        default=env_or_dotenv(dotenv, "CLICKHOUSE_URL", default="http://localhost:8123"),
        help="ClickHouse HTTP URL.",
    )
    parser.add_argument(
        "--database",
        default=env_or_dotenv(dotenv, "CLICKHOUSE_DATABASE", default="market"),
        help="ClickHouse database name.",
    )
    parser.add_argument(
        "--index-table",
        default=env_or_dotenv(dotenv, "INDEX_TABLE", default="IndexDaily"),
        help="Target index table name.",
    )
    parser.add_argument(
        "--align-prices-table",
        default=env_or_dotenv(dotenv, "OHLCV_TABLE", default="DailyPricesBhavcopy"),
        help="Price table used to infer same date range when start/end not provided.",
    )
    parser.add_argument(
        "--user",
        default=env_or_dotenv(dotenv, "CLICKHOUSE_USER", "CH_USER", default="default"),
        help="ClickHouse username.",
    )
    parser.add_argument(
        "--password",
        default=env_or_dotenv(dotenv, "CLICKHOUSE_PASSWORD", "CH_PASSWORD", default=""),
        help="ClickHouse password.",
    )
    parser.add_argument(
        "--years",
        type=int,
        default=10,
        help="Fallback lookback years when start/end and align table are unavailable.",
    )
    parser.add_argument("--start-date", default="", help="Start date YYYY-MM-DD.")
    parser.add_argument("--end-date", default="", help="End date YYYY-MM-DD.")
    parser.add_argument(
        "--index-map-json",
        default="",
        help='Optional JSON mapping, e.g. {"NIFTY_50":"^NSEI","SENSEX":"^BSESN"}',
    )
    parser.add_argument(
        "--truncate-first",
        action="store_true",
        help="Truncate index table before insert.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5000,
        help="Insert batch size.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=90,
        help="HTTP timeout seconds.",
    )
    parser.add_argument("--verbose", action="store_true", help="Print per-index progress.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    session = make_session()

    try:
        index_map = parse_index_map(args)
        start_date, end_date = resolve_date_range(args=args, session=session)
        ensure_index_table(
            session=session,
            clickhouse_url=args.clickhouse_url,
            user=args.user,
            password=args.password,
            timeout=args.timeout,
            database=args.database,
            index_table=args.index_table,
        )
        if args.truncate_first:
            truncate_table(
                session=session,
                clickhouse_url=args.clickhouse_url,
                user=args.user,
                password=args.password,
                timeout=args.timeout,
                database=args.database,
                table=args.index_table,
            )
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"Failed setup: {exc}") from exc

    fetched_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    inserted_total = 0
    loaded_indices = 0
    failed: list[str] = []
    empty: list[str] = []

    for index_name, provider_ticker in index_map.items():
        try:
            rows = fetch_index_rows(
                index_name=index_name,
                provider_ticker=provider_ticker,
                start_date=start_date,
                end_date=end_date,
                fetched_at_utc=fetched_at_utc,
            )
            if not rows:
                empty.append(f"{index_name} ({provider_ticker})")
                continue
            inserted = insert_rows(
                session=session,
                clickhouse_url=args.clickhouse_url,
                user=args.user,
                password=args.password,
                timeout=args.timeout,
                database=args.database,
                index_table=args.index_table,
                rows=rows,
                batch_size=args.batch_size,
            )
            inserted_total += inserted
            loaded_indices += 1
            if args.verbose:
                print(f"Loaded {index_name} ({provider_ticker}) rows={inserted}")
        except Exception as exc:  # noqa: BLE001
            failed.append(f"{index_name} ({provider_ticker}): {exc}")

    print(
        json.dumps(
            {
                "target": f"{args.database}.{args.index_table}",
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "indices_requested": len(index_map),
                "indices_loaded": loaded_indices,
                "indices_empty": len(empty),
                "indices_failed": len(failed),
                "rows_inserted": inserted_total,
                "empty_examples": empty[:10],
                "failed_examples": failed[:10],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
