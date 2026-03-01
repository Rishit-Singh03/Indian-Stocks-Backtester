from __future__ import annotations

import argparse
import contextlib
import csv
import io
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yfinance as yf

from backfill_bse_prices import (
    clickhouse_query,
    env_or_dotenv,
    load_dotenv,
    make_session,
    parse_yyyy_mm_dd,
    validate_identifier,
)


@dataclass
class UniverseRow:
    symbol: str
    bse_code: str
    yahoo_ticker: str


def fetch_universe(
    *,
    session: Any,
    clickhouse_url: str,
    user: str,
    password: str,
    timeout: int,
    database: str,
    ticker_table: str,
    raw_prices_table: str,
    only_with_prices: bool,
    limit: int,
) -> list[UniverseRow]:
    db = validate_identifier(database)
    tbl = validate_identifier(ticker_table)
    raw_tbl = validate_identifier(raw_prices_table)
    if only_with_prices:
        query = (
            "SELECT tm.symbol, tm.bse_code, tm.yahoo_ticker "
            f"FROM {db}.{tbl} tm "
            "INNER JOIN ("
            f"  SELECT DISTINCT bse_code FROM {db}.{raw_tbl} WHERE bse_code != ''"
            ") px ON px.bse_code = tm.bse_code "
            "WHERE tm.exchange = 'BSE' AND tm.status = 'ACTIVE' AND tm.yahoo_ticker != '' "
            "ORDER BY tm.symbol "
        )
    else:
        query = (
            f"SELECT symbol, bse_code, yahoo_ticker FROM {db}.{tbl} "
            "WHERE exchange = 'BSE' AND status = 'ACTIVE' AND yahoo_ticker != '' "
            "ORDER BY symbol "
        )
    if limit > 0:
        query += f"LIMIT {limit} "
    query += "FORMAT JSONEachRow"

    text = clickhouse_query(
        session=session,
        url=clickhouse_url,
        query=query,
        user=user,
        password=password,
        timeout=timeout,
    )
    out: list[UniverseRow] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        row = json.loads(line)
        out.append(
            UniverseRow(
                symbol=str(row.get("symbol", "")).strip().upper(),
                bse_code=str(row.get("bse_code", "")).strip(),
                yahoo_ticker=str(row.get("yahoo_ticker", "")).strip().upper(),
            )
        )
    return out


def years_ago(day: date, years: int) -> date:
    try:
        return day.replace(year=day.year - years)
    except ValueError:
        return day.replace(month=2, day=28, year=day.year - years)


def split_to_ratio(value: float) -> tuple[float, float]:
    # yfinance split value is the share multiplier for old holdings.
    # 2.0 means 2-for-1 (old:new = 1:2), 0.5 means 1-for-2 reverse split (old:new = 2:1).
    if value <= 0:
        return 1.0, 1.0
    if value >= 1:
        return 1.0, float(value)
    return float(1 / value), 1.0


def candidate_tickers(symbol: str, bse_code: str, yahoo_ticker: str) -> list[str]:
    out: list[str] = []

    def add(value: str) -> None:
        v = value.strip().upper()
        if v and v not in out:
            out.append(v)

    add(yahoo_ticker)
    digits = re.sub(r"\D", "", bse_code)
    if digits:
        add(f"{digits}.BO")
    sym = symbol.strip().upper().replace(" ", "")
    if sym:
        add(f"{sym}.BO")
        add(f"{sym}.NS")
    return out


def rows_from_actions_frame(
    *,
    frame: Any,
    symbol: str,
    bse_code: str,
    ticker_used: str,
    start_date: date,
    end_date: date,
    include_dividends: bool,
    fetched_at_utc: str,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if frame is None or getattr(frame, "empty", True):
        return out
    for idx, row in frame.iterrows():
        ex_date = idx.date() if hasattr(idx, "date") else parse_yyyy_mm_dd(str(idx)[:10])
        if ex_date < start_date or ex_date > end_date:
            continue
        split_val = float(row.get("Stock Splits", 0) or 0)
        div_val = float(row.get("Dividends", 0) or 0)
        if split_val > 0:
            ratio_from, ratio_to = split_to_ratio(split_val)
            out.append(
                {
                    "exchange": "BSE",
                    "symbol": symbol,
                    "bse_code": bse_code,
                    "action_type": "SPLIT",
                    "ratio_from": ratio_from,
                    "ratio_to": ratio_to,
                    "ex_date": ex_date.isoformat(),
                    "announcement_date": ex_date.isoformat(),
                    "source": "YFINANCE_AUTO",
                    "notes": f"ticker={ticker_used}; split_factor={split_val}",
                    "fetched_at_utc": fetched_at_utc,
                }
            )
        if include_dividends and div_val > 0:
            out.append(
                {
                    "exchange": "BSE",
                    "symbol": symbol,
                    "bse_code": bse_code,
                    "action_type": "DIVIDEND",
                    "ratio_from": 1.0,
                    "ratio_to": 1.0,
                    "ex_date": ex_date.isoformat(),
                    "announcement_date": ex_date.isoformat(),
                    "source": "YFINANCE_AUTO",
                    "notes": f"ticker={ticker_used}; cash_dividend={div_val}",
                    "fetched_at_utc": fetched_at_utc,
                }
            )
    return out


def fetch_actions_for_ticker(
    *,
    symbol: str,
    bse_code: str,
    yahoo_ticker: str,
    start_date: date,
    end_date: date,
    include_dividends: bool,
    fetched_at_utc: str,
) -> list[dict[str, Any]]:
    errors: list[str] = []
    for candidate in candidate_tickers(symbol, bse_code, yahoo_ticker):
        ticker = yf.Ticker(candidate)
        had_success = False

        # Method 1: actions frame.
        try:
            actions = ticker.actions
            had_success = True
            rows = rows_from_actions_frame(
                frame=actions,
                symbol=symbol,
                bse_code=bse_code,
                ticker_used=candidate,
                start_date=start_date,
                end_date=end_date,
                include_dividends=include_dividends,
                fetched_at_utc=fetched_at_utc,
            )
            if rows:
                return rows
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{candidate}/actions: {exc}")

        # Method 2: history with actions.
        try:
            hist = ticker.history(
                start=start_date.isoformat(),
                end=(end_date + timedelta(days=1)).isoformat(),
                interval="1d",
                auto_adjust=False,
                actions=True,
            )
            had_success = True
            rows = rows_from_actions_frame(
                frame=hist,
                symbol=symbol,
                bse_code=bse_code,
                ticker_used=candidate,
                start_date=start_date,
                end_date=end_date,
                include_dividends=include_dividends,
                fetched_at_utc=fetched_at_utc,
            )
            if rows:
                return rows
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{candidate}/history: {exc}")

        # If any fetch method worked but yielded no events, treat as non-failure.
        if had_success:
            return []

    if errors:
        raise RuntimeError("; ".join(errors[:3]))
    return []


def ensure_actions_table(
    *,
    session: Any,
    clickhouse_url: str,
    user: str,
    password: str,
    timeout: int,
    database: str,
    corporate_actions_table: str,
) -> None:
    db = validate_identifier(database)
    tbl = validate_identifier(corporate_actions_table)
    ddl = f"""
CREATE TABLE IF NOT EXISTS {db}.{tbl}
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
    clickhouse_query(
        session=session,
        url=clickhouse_url,
        query=ddl,
        user=user,
        password=password,
        timeout=timeout,
    )


def replace_source_rows(
    *,
    session: Any,
    clickhouse_url: str,
    user: str,
    password: str,
    timeout: int,
    database: str,
    corporate_actions_table: str,
    source: str,
) -> None:
    db = validate_identifier(database)
    tbl = validate_identifier(corporate_actions_table)
    clickhouse_query(
        session=session,
        url=clickhouse_url,
        query=f"ALTER TABLE {db}.{tbl} DELETE WHERE source = '{source}'",
        user=user,
        password=password,
        timeout=max(timeout, 120),
    )


def truncate_actions_table(
    *,
    session: Any,
    clickhouse_url: str,
    user: str,
    password: str,
    timeout: int,
    database: str,
    corporate_actions_table: str,
) -> None:
    db = validate_identifier(database)
    tbl = validate_identifier(corporate_actions_table)
    clickhouse_query(
        session=session,
        url=clickhouse_url,
        query=f"TRUNCATE TABLE {db}.{tbl}",
        user=user,
        password=password,
        timeout=max(timeout, 120),
    )


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


def export_csv(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        "symbol",
        "bse_code",
        "action_type",
        "ratio_from",
        "ratio_to",
        "ex_date",
        "announcement_date",
        "source",
        "notes",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in columns})


def parse_args() -> argparse.Namespace:
    dotenv = load_dotenv(Path(".env"))
    parser = argparse.ArgumentParser(
        description="Programmatically fetch corporate actions from yfinance for BSE universe."
    )
    parser.add_argument("--clickhouse-url", default=env_or_dotenv(dotenv, "CLICKHOUSE_URL", default="http://localhost:8123"))
    parser.add_argument("--database", default=env_or_dotenv(dotenv, "CLICKHOUSE_DATABASE", default="market"))
    parser.add_argument("--ticker-table", default=env_or_dotenv(dotenv, "CLICKHOUSE_TABLE", default="TickerMaster"))
    parser.add_argument("--raw-prices-table", default=env_or_dotenv(dotenv, "OHLCV_TABLE", default="DailyPricesBhavcopy"))
    parser.add_argument("--corporate-actions-table", default=env_or_dotenv(dotenv, "CORPORATE_ACTIONS_TABLE", default="CorporateActions"))
    parser.add_argument("--user", default=env_or_dotenv(dotenv, "CLICKHOUSE_USER", "CH_USER", default="default"))
    parser.add_argument("--password", default=env_or_dotenv(dotenv, "CLICKHOUSE_PASSWORD", "CH_PASSWORD", default=""))
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--limit", type=int, default=500, help="Universe size from TickerMaster (0 = all).")
    parser.add_argument("--years", type=int, default=15, help="Lookback years if start-date not provided.")
    parser.add_argument("--start-date", default="", help="Optional start date YYYY-MM-DD.")
    parser.add_argument("--end-date", default="", help="Optional end date YYYY-MM-DD.")
    parser.add_argument("--include-dividends", action="store_true", help="Include dividend events as well.")
    parser.add_argument("--replace-source", action="store_true", help="Delete prior YFINANCE_AUTO rows before insert.")
    parser.add_argument("--truncate-first", action="store_true", help="Truncate CorporateActions before insert.")
    parser.add_argument(
        "--include-all-tickers",
        action="store_true",
        help="By default the script fetches only tickers present in price history. Use this to include all active tickers.",
    )
    parser.add_argument(
        "--show-yf-errors",
        action="store_true",
        help="Show raw yfinance stderr errors for failed tickers.",
    )
    parser.add_argument("--export-csv", default="", help="Optional path to export fetched actions CSV.")
    parser.add_argument("--sleep-seconds", type=float, default=0.0, help="Sleep between ticker fetches.")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    session = make_session()
    fetched_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    end_date = parse_yyyy_mm_dd(args.end_date) if args.end_date else datetime.now(timezone.utc).date()
    start_date = parse_yyyy_mm_dd(args.start_date) if args.start_date else years_ago(end_date, args.years)
    if start_date > end_date:
        raise SystemExit("start_date must be <= end_date")

    ensure_actions_table(
        session=session,
        clickhouse_url=args.clickhouse_url,
        user=args.user,
        password=args.password,
        timeout=args.timeout,
        database=args.database,
        corporate_actions_table=args.corporate_actions_table,
    )
    if args.truncate_first:
        truncate_actions_table(
            session=session,
            clickhouse_url=args.clickhouse_url,
            user=args.user,
            password=args.password,
            timeout=args.timeout,
            database=args.database,
            corporate_actions_table=args.corporate_actions_table,
        )
    elif args.replace_source:
        replace_source_rows(
            session=session,
            clickhouse_url=args.clickhouse_url,
            user=args.user,
            password=args.password,
            timeout=args.timeout,
            database=args.database,
            corporate_actions_table=args.corporate_actions_table,
            source="YFINANCE_AUTO",
        )

    universe = fetch_universe(
        session=session,
        clickhouse_url=args.clickhouse_url,
        user=args.user,
        password=args.password,
        timeout=args.timeout,
        database=args.database,
        ticker_table=args.ticker_table,
        raw_prices_table=args.raw_prices_table,
        only_with_prices=not args.include_all_tickers,
        limit=args.limit,
    )
    if not universe:
        raise SystemExit("No eligible symbols found in ticker table.")

    all_rows: list[dict[str, Any]] = []
    tickers_with_events = 0
    failures = 0
    failed_examples: list[str] = []
    import time

    for idx, item in enumerate(universe, start=1):
        try:
            if args.show_yf_errors:
                rows = fetch_actions_for_ticker(
                    symbol=item.symbol,
                    bse_code=item.bse_code,
                    yahoo_ticker=item.yahoo_ticker,
                    start_date=start_date,
                    end_date=end_date,
                    include_dividends=bool(args.include_dividends),
                    fetched_at_utc=fetched_at_utc,
                )
            else:
                # yfinance prints many symbol-level failures directly to stderr; suppress those by default.
                with contextlib.redirect_stderr(io.StringIO()):
                    rows = fetch_actions_for_ticker(
                        symbol=item.symbol,
                        bse_code=item.bse_code,
                        yahoo_ticker=item.yahoo_ticker,
                        start_date=start_date,
                        end_date=end_date,
                        include_dividends=bool(args.include_dividends),
                        fetched_at_utc=fetched_at_utc,
                    )
            if rows:
                tickers_with_events += 1
                all_rows.extend(rows)
        except Exception as exc:  # noqa: BLE001
            failures += 1
            if len(failed_examples) < 25:
                failed_examples.append(f"{item.yahoo_ticker} ({item.symbol}): {exc}")
        if args.verbose and idx % 50 == 0:
            print(
                f"Progress {idx}/{len(universe)} | events={len(all_rows)} "
                f"tickers_with_events={tickers_with_events} failures={failures}"
            )
        time.sleep(max(args.sleep_seconds, 0.0))

    dedup: dict[tuple[str, str, str, str, float, float, str], dict[str, Any]] = {}
    for row in all_rows:
        key = (
            str(row["symbol"]),
            str(row["bse_code"]),
            str(row["action_type"]),
            str(row["ex_date"]),
            float(row["ratio_from"]),
            float(row["ratio_to"]),
            str(row["source"]),
        )
        dedup[key] = row
    final_rows = list(dedup.values())

    inserted = insert_actions(
        session=session,
        clickhouse_url=args.clickhouse_url,
        user=args.user,
        password=args.password,
        timeout=args.timeout,
        database=args.database,
        corporate_actions_table=args.corporate_actions_table,
        rows=final_rows,
    )

    if args.export_csv:
        export_csv(final_rows, Path(args.export_csv))

    print(
        json.dumps(
            {
                "target": f"{args.database}.{args.corporate_actions_table}",
                "universe_symbols": len(universe),
                "date_range": {"start": start_date.isoformat(), "end": end_date.isoformat()},
                "include_dividends": bool(args.include_dividends),
                "events_fetched": len(all_rows),
                "events_deduped": len(final_rows),
                "tickers_with_events": tickers_with_events,
                "ticker_fetch_failures": failures,
                "failure_examples": failed_examples,
                "only_with_prices": not args.include_all_tickers,
                "inserted_rows": inserted,
                "export_csv": args.export_csv or None,
                "source": "YFINANCE_AUTO",
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
