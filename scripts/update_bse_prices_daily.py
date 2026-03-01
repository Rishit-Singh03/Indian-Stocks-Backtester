from __future__ import annotations

import argparse
import json
import tempfile
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from backfill_bse_prices import (
    clickhouse_query,
    download_bhavcopy_for_day,
    ensure_prices_table,
    env_or_dotenv,
    fetch_universe,
    insert_rows,
    iter_weekdays,
    load_dotenv,
    make_session,
    parse_day_rows,
    parse_yyyy_mm_dd,
    validate_identifier,
)


def fetch_max_loaded_date(
    session: Any,
    clickhouse_url: str,
    user: str,
    password: str,
    timeout: int,
    database: str,
    prices_table: str,
) -> date | None:
    db = validate_identifier(database)
    tbl = validate_identifier(prices_table)
    query = f"SELECT max(date) AS max_date FROM {db}.{tbl} FORMAT JSONEachRow"
    text = clickhouse_query(
        session=session,
        url=clickhouse_url,
        query=query,
        user=user,
        password=password,
        timeout=timeout,
    ).strip()
    if not text:
        return None
    row = json.loads(text.splitlines()[0])
    raw = row.get("max_date")
    if raw in (None, "", "0000-00-00"):
        return None
    return parse_yyyy_mm_dd(str(raw))


def resolve_update_window(
    max_loaded_date: date | None,
    start_date_arg: str,
    end_date_arg: str,
    replay_days: int,
    bootstrap_days: int,
    max_catchup_days: int,
) -> tuple[date, date]:
    end_date = parse_yyyy_mm_dd(end_date_arg) if end_date_arg else datetime.now(timezone.utc).date()
    if start_date_arg:
        start_date = parse_yyyy_mm_dd(start_date_arg)
    elif max_loaded_date is not None:
        if replay_days > 0:
            start_date = max_loaded_date - timedelta(days=replay_days)
        else:
            start_date = max_loaded_date + timedelta(days=1)
    else:
        start_date = end_date - timedelta(days=max(bootstrap_days, 1) - 1)

    if start_date > end_date:
        return end_date, end_date

    if max_catchup_days > 0:
        max_start = end_date - timedelta(days=max_catchup_days - 1)
        if start_date < max_start:
            start_date = max_start
    return start_date, end_date


def parse_args() -> argparse.Namespace:
    dotenv = load_dotenv(Path(".env"))
    parser = argparse.ArgumentParser(
        description=(
            "Incremental daily BSE bhavcopy updater. "
            "It resumes from last loaded date and catches up missed days."
        )
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
        "--master-table",
        default=env_or_dotenv(dotenv, "CLICKHOUSE_TABLE", default="TickerMaster"),
        help="Ticker master table name.",
    )
    parser.add_argument(
        "--prices-table",
        default=env_or_dotenv(dotenv, "OHLCV_TABLE", default="DailyPricesBhavcopy"),
        help="Target daily prices table name.",
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
        "--limit",
        type=int,
        default=0,
        help="Universe size from TickerMaster. 0 means all active BSE symbols.",
    )
    parser.add_argument(
        "--start-date",
        default="",
        help="Optional override start date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end-date",
        default="",
        help="Optional override end date (YYYY-MM-DD). Default is today.",
    )
    parser.add_argument(
        "--replay-days",
        type=int,
        default=0,
        help=(
            "On each run, reprocess this many days before max(date) "
            "to heal transient misses."
        ),
    )
    parser.add_argument(
        "--bootstrap-days",
        type=int,
        default=30,
        help="If price table is empty, backfill this many recent days.",
    )
    parser.add_argument(
        "--max-catchup-days",
        type=int,
        default=730,
        help="Safety cap on run window size in days. Set 0 to disable.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=90,
        help="ClickHouse/HTTP timeout in seconds.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5000,
        help="Insert batch size for ClickHouse.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.0,
        help="Delay between bhavcopy requests.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print periodic progress.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    session = make_session()
    fetched_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    try:
        universe = fetch_universe(
            session=session,
            url=args.clickhouse_url,
            user=args.user,
            password=args.password,
            timeout=args.timeout,
            database=args.database,
            master_table=args.master_table,
            limit=args.limit,
        )
        if not universe:
            raise SystemExit("No eligible BSE symbols found in TickerMaster.")
        by_code = {item.bse_code: item for item in universe}

        ensure_prices_table(
            session=session,
            url=args.clickhouse_url,
            user=args.user,
            password=args.password,
            timeout=args.timeout,
            database=args.database,
            prices_table=args.prices_table,
        )

        max_loaded = fetch_max_loaded_date(
            session=session,
            clickhouse_url=args.clickhouse_url,
            user=args.user,
            password=args.password,
            timeout=args.timeout,
            database=args.database,
            prices_table=args.prices_table,
        )
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"Failed ClickHouse setup/read: {exc}") from exc

    start_date, end_date = resolve_update_window(
        max_loaded_date=max_loaded,
        start_date_arg=args.start_date,
        end_date_arg=args.end_date,
        replay_days=args.replay_days,
        bootstrap_days=args.bootstrap_days,
        max_catchup_days=args.max_catchup_days,
    )
    trading_dates = iter_weekdays(start_date, end_date)

    if not trading_dates:
        print(
            json.dumps(
                {
                    "target": f"{args.database}.{args.prices_table}",
                    "status": "no_work",
                    "max_loaded_before": max_loaded.isoformat() if max_loaded else None,
                },
                indent=2,
            )
        )
        return

    try:
        from bse import BSE
    except ImportError as exc:
        raise SystemExit("Missing dependency 'bse'. Run `uv sync` and retry.") from exc

    inserted_rows = 0
    dates_with_rows = 0
    skipped_empty = 0
    downloaded_files = 0
    missing_reports = 0
    source_counts: dict[str, int] = {}
    symbol_hits: set[str] = set()

    with tempfile.TemporaryDirectory(prefix="bhavcopy_daily_") as tmpdir:
        with BSE(download_folder=tmpdir) as client:
            for idx, trade_date in enumerate(trading_dates, start=1):
                csv_bytes, source = download_bhavcopy_for_day(
                    session=session,
                    bse_client=client,
                    trading_day=trade_date,
                    timeout=args.timeout,
                )
                if not csv_bytes:
                    missing_reports += 1
                    if args.verbose and idx % 30 == 0:
                        print(
                            f"Progress {idx}/{len(trading_dates)} | "
                            f"downloaded={downloaded_files} missing={missing_reports}"
                        )
                    time.sleep(max(args.sleep_seconds, 0.0))
                    continue

                downloaded_files += 1
                source_counts[source] = source_counts.get(source, 0) + 1

                day_rows = parse_day_rows(
                    csv_bytes=csv_bytes,
                    trading_day=trade_date,
                    by_code=by_code,
                    fetched_at_utc=fetched_at_utc,
                )
                if not day_rows:
                    skipped_empty += 1
                else:
                    inserted = insert_rows(
                        session=session,
                        url=args.clickhouse_url,
                        user=args.user,
                        password=args.password,
                        timeout=args.timeout,
                        database=args.database,
                        prices_table=args.prices_table,
                        rows=day_rows,
                        batch_size=args.batch_size,
                    )
                    inserted_rows += inserted
                    dates_with_rows += 1
                    for row in day_rows:
                        symbol_hits.add(str(row["bse_code"]))

                if args.verbose and idx % 30 == 0:
                    print(
                        f"Progress {idx}/{len(trading_dates)} | downloaded={downloaded_files} "
                        f"missing={missing_reports} inserted_rows={inserted_rows}"
                    )
                time.sleep(max(args.sleep_seconds, 0.0))

    try:
        max_loaded_after = fetch_max_loaded_date(
            session=session,
            clickhouse_url=args.clickhouse_url,
            user=args.user,
            password=args.password,
            timeout=args.timeout,
            database=args.database,
            prices_table=args.prices_table,
        )
    except Exception:
        max_loaded_after = None

    print(
        json.dumps(
            {
                "target": f"{args.database}.{args.prices_table}",
                "universe_symbols": len(universe),
                "symbols_with_data_in_run": len(symbol_hits),
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "max_loaded_before": max_loaded.isoformat() if max_loaded else None,
                "max_loaded_after": max_loaded_after.isoformat() if max_loaded_after else None,
                "weekday_dates_considered": len(trading_dates),
                "bhavcopy_files_downloaded": downloaded_files,
                "bhavcopy_files_missing": missing_reports,
                "download_source_breakdown": source_counts,
                "dates_with_inserted_rows": dates_with_rows,
                "files_without_matching_rows": skipped_empty,
                "rows_inserted": inserted_rows,
                "replay_days": args.replay_days,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
