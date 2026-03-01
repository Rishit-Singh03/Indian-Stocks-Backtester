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
)
from backfill_index_data import (
    ensure_index_table,
    fetch_index_rows,
    insert_rows,
    parse_index_map,
)


def fetch_index_max_dates(
    session: Any,
    clickhouse_url: str,
    user: str,
    password: str,
    timeout: int,
    database: str,
    index_table: str,
) -> dict[str, date]:
    db = validate_identifier(database)
    tbl = validate_identifier(index_table)
    query = (
        f"SELECT index_name, max(date) AS max_date "
        f"FROM {db}.{tbl} "
        "GROUP BY index_name FORMAT JSONEachRow"
    )
    text = clickhouse_query(
        session=session,
        url=clickhouse_url,
        query=query,
        user=user,
        password=password,
        timeout=timeout,
    ).strip()
    out: dict[str, date] = {}
    if not text:
        return out
    for line in text.splitlines():
        row = json.loads(line)
        index_name = str(row.get("index_name", "")).strip()
        max_raw = row.get("max_date")
        if not index_name or not max_raw or str(max_raw) == "0000-00-00":
            continue
        out[index_name] = parse_yyyy_mm_dd(str(max_raw))
    return out


def resolve_start_for_index(
    max_loaded: date | None,
    start_date_arg: str,
    end_date: date,
    replay_days: int,
    bootstrap_days: int,
    max_catchup_days: int,
) -> date:
    if start_date_arg:
        start_date = parse_yyyy_mm_dd(start_date_arg)
    elif max_loaded is not None:
        if replay_days > 0:
            start_date = max_loaded - timedelta(days=replay_days)
        else:
            start_date = max_loaded + timedelta(days=1)
    else:
        start_date = end_date - timedelta(days=max(bootstrap_days, 1) - 1)

    if max_catchup_days > 0:
        lower_bound = end_date - timedelta(days=max_catchup_days - 1)
        if start_date < lower_bound:
            start_date = lower_bound
    return start_date


def parse_args() -> argparse.Namespace:
    dotenv = load_dotenv(Path(".env"))
    parser = argparse.ArgumentParser(
        description=(
            "Incremental daily updater for IndexDaily. "
            "Resumes each index from its own last loaded date."
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
        "--index-table",
        default=env_or_dotenv(dotenv, "INDEX_TABLE", default="IndexDaily"),
        help="Target index table name.",
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
        "--index-map-json",
        default="",
        help='Optional JSON mapping, e.g. {"NIFTY_50":"^NSEI","SENSEX":"^BSESN"}',
    )
    parser.add_argument(
        "--start-date",
        default="",
        help="Optional override start date YYYY-MM-DD for all indices.",
    )
    parser.add_argument(
        "--end-date",
        default="",
        help="Optional override end date YYYY-MM-DD. Default is today.",
    )
    parser.add_argument(
        "--replay-days",
        type=int,
        default=0,
        help="Reprocess this many days before each index max(date).",
    )
    parser.add_argument(
        "--bootstrap-days",
        type=int,
        default=30,
        help="If an index has no data yet, backfill this many recent days.",
    )
    parser.add_argument(
        "--max-catchup-days",
        type=int,
        default=730,
        help="Safety cap for per-index window size (0 disables cap).",
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
    end_date = parse_yyyy_mm_dd(args.end_date) if args.end_date else datetime.now(timezone.utc).date()
    fetched_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    try:
        index_map = parse_index_map(args)
        ensure_index_table(
            session=session,
            clickhouse_url=args.clickhouse_url,
            user=args.user,
            password=args.password,
            timeout=args.timeout,
            database=args.database,
            index_table=args.index_table,
        )
        max_dates = fetch_index_max_dates(
            session=session,
            clickhouse_url=args.clickhouse_url,
            user=args.user,
            password=args.password,
            timeout=args.timeout,
            database=args.database,
            index_table=args.index_table,
        )
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"Failed setup: {exc}") from exc

    inserted_total = 0
    loaded_indices = 0
    failed: list[str] = []
    empty: list[str] = []
    index_windows: dict[str, str] = {}

    for index_name, provider_ticker in index_map.items():
        try:
            max_loaded = max_dates.get(index_name)
            start_date = resolve_start_for_index(
                max_loaded=max_loaded,
                start_date_arg=args.start_date,
                end_date=end_date,
                replay_days=max(args.replay_days, 0),
                bootstrap_days=max(args.bootstrap_days, 1),
                max_catchup_days=max(args.max_catchup_days, 0),
            )

            if start_date > end_date:
                index_windows[index_name] = "no_work"
                if args.verbose:
                    print(f"Skip {index_name}: start>{end_date.isoformat()} (no new data)")
                continue

            rows = fetch_index_rows(
                index_name=index_name,
                provider_ticker=provider_ticker,
                start_date=start_date,
                end_date=end_date,
                fetched_at_utc=fetched_at_utc,
            )
            index_windows[index_name] = f"{start_date.isoformat()}..{end_date.isoformat()}"
            if not rows:
                empty.append(f"{index_name} ({provider_ticker})")
                if args.verbose:
                    print(f"No rows for {index_name} ({provider_ticker}) in {index_windows[index_name]}")
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
                print(
                    f"Loaded {index_name} ({provider_ticker}) "
                    f"rows={inserted} window={index_windows[index_name]}"
                )
        except Exception as exc:  # noqa: BLE001
            failed.append(f"{index_name} ({provider_ticker}): {exc}")

    print(
        json.dumps(
            {
                "target": f"{args.database}.{args.index_table}",
                "end_date": end_date.isoformat(),
                "indices_requested": len(index_map),
                "indices_loaded": loaded_indices,
                "indices_empty": len(empty),
                "indices_failed": len(failed),
                "rows_inserted": inserted_total,
                "replay_days": args.replay_days,
                "index_windows": index_windows,
                "empty_examples": empty[:10],
                "failed_examples": failed[:10],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
