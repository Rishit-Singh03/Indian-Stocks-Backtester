from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import tempfile
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zipfile import BadZipFile, ZipFile

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def load_dotenv(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            values[key] = value
    return values


def env_or_dotenv(dotenv: dict[str, str], *keys: str, default: str = "") -> str:
    for key in keys:
        value = os.getenv(key)
        if value:
            return value
    for key in keys:
        value = dotenv.get(key)
        if value:
            return value
    return default


def validate_identifier(identifier: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        raise ValueError(
            f"Invalid ClickHouse identifier: {identifier!r}. Use only letters, numbers, and underscores."
        )
    return identifier


def make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/csv,application/zip,*/*",
            "Referer": "https://www.bseindia.com/markets/MarketInfo/BhavCopy.aspx",
        }
    )
    return session


def clickhouse_query(
    session: requests.Session,
    url: str,
    query: str,
    user: str,
    password: str,
    timeout: int,
    data: bytes | None = None,
    content_type: str | None = None,
) -> str:
    headers: dict[str, str] = {}
    if content_type:
        headers["Content-Type"] = content_type

    response = session.post(
        url,
        params={"query": query},
        data=data,
        auth=(user, password),
        timeout=timeout,
        headers=headers,
    )
    if response.status_code >= 400:
        detail = response.text.strip()
        raise RuntimeError(
            f"ClickHouse error {response.status_code} for query: {query}\n{detail}"
        )
    return response.text


@dataclass
class UniverseRow:
    symbol: str
    bse_code: str
    company_name: str


def fetch_universe(
    session: requests.Session,
    url: str,
    user: str,
    password: str,
    timeout: int,
    database: str,
    master_table: str,
    limit: int,
) -> list[UniverseRow]:
    db = validate_identifier(database)
    tbl = validate_identifier(master_table)
    query = (
        f"SELECT symbol, bse_code, company_name FROM {db}.{tbl} "
        "WHERE exchange = 'BSE' AND status = 'ACTIVE' AND bse_code != '' "
        "ORDER BY toUInt32OrZero(bse_code), symbol "
    )
    if limit > 0:
        query += f"LIMIT {limit} "
    query += "FORMAT JSONEachRow"
    text = clickhouse_query(
        session=session,
        url=url,
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
        symbol = str(row.get("symbol", "")).strip()
        bse_code = str(row.get("bse_code", "")).strip()
        company_name = str(row.get("company_name", "")).strip()
        if symbol and bse_code:
            out.append(UniverseRow(symbol=symbol, bse_code=bse_code, company_name=company_name))
    return out


def ensure_prices_table(
    session: requests.Session,
    url: str,
    user: str,
    password: str,
    timeout: int,
    database: str,
    prices_table: str,
) -> None:
    db = validate_identifier(database)
    tbl = validate_identifier(prices_table)
    ddl = f"""
CREATE TABLE IF NOT EXISTS {db}.{tbl}
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
    volume UInt64,
    source LowCardinality(String),
    fetched_at_utc DateTime
)
ENGINE = ReplacingMergeTree(fetched_at_utc)
PARTITION BY toYYYYMM(date)
ORDER BY (exchange, bse_code, date)
""".strip()
    clickhouse_query(
        session=session,
        url=url,
        query=ddl,
        user=user,
        password=password,
        timeout=timeout,
    )


def truncate_table(
    session: requests.Session,
    url: str,
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
        url=url,
        query=f"TRUNCATE TABLE {db}.{tbl}",
        user=user,
        password=password,
        timeout=timeout,
    )


def normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def pick_value(normalized_row: dict[str, str], candidates: list[str]) -> str:
    for candidate in candidates:
        value = normalized_row.get(normalize_key(candidate), "")
        if value:
            return value
    return ""


def as_float(text: str) -> float | None:
    if not text:
        return None
    stripped = text.replace(",", "").strip()
    try:
        number = float(stripped)
    except ValueError:
        return None
    if number != number:
        return None
    return number


def as_uint(text: str) -> int:
    num = as_float(text)
    if num is None or num < 0:
        return 0
    return int(num)


def clean_code(raw_code: str) -> str:
    digits = re.sub(r"\D", "", raw_code)
    return digits


def iter_weekdays(start_date: date, end_date: date) -> list[date]:
    out: list[date] = []
    current = start_date
    while current <= end_date:
        if current.weekday() < 5:
            out.append(current)
        current += timedelta(days=1)
    return out


def parse_yyyy_mm_dd(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def year_shifted(day: date, years: int) -> date:
    try:
        return day.replace(year=day.year - years)
    except ValueError:
        return day.replace(month=2, day=28, year=day.year - years)


def extract_csv_from_zip(zip_bytes: bytes) -> bytes | None:
    try:
        with ZipFile(io.BytesIO(zip_bytes)) as archive:
            for name in archive.namelist():
                lower = name.lower()
                if lower.endswith(".csv") or lower.endswith(".txt"):
                    return archive.read(name)
            names = archive.namelist()
            if names:
                return archive.read(names[0])
    except BadZipFile:
        return None
    return None


def looks_like_html(payload: bytes) -> bool:
    prefix = payload[:256].strip().lower()
    return prefix.startswith(b"<!doctype html") or prefix.startswith(b"<html")


def legacy_bhavcopy_urls(trading_day: date) -> list[str]:
    ddmmyy = trading_day.strftime("%d%m%y")
    yyyymmdd = trading_day.strftime("%Y%m%d")
    return [
        # Current UDiFF CSV path used by newer bhavcopy releases.
        f"https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_{yyyymmdd}_F_0000.CSV",
        # Legacy zip pattern used for historical bhavcopies.
        f"https://www.bseindia.com/download/BhavCopy/Equity/EQ{ddmmyy}_CSV.ZIP",
        f"https://www.bseindia.com/download/BhavCopy/Equity/EQ{ddmmyy}_CSV.zip",
        f"https://www.bseindia.com/download/BhavCopy/Equity/EQ{ddmmyy}.ZIP",
        f"https://www.bseindia.com/download/BhavCopy/Equity/EQ{ddmmyy}.CSV",
    ]


def download_from_url(session: requests.Session, url: str, timeout: int) -> bytes | None:
    try:
        response = session.get(url, timeout=timeout)
    except requests.RequestException:
        return None
    if response.status_code != 200:
        return None
    content = response.content
    if not content or looks_like_html(content):
        return None
    if url.lower().endswith(".zip") or content[:2] == b"PK":
        return extract_csv_from_zip(content)
    return content


def download_bhavcopy_for_day(
    session: requests.Session,
    bse_client: Any,
    trading_day: date,
    timeout: int,
) -> tuple[bytes | None, str]:
    day_dt = datetime.combine(trading_day, datetime.min.time())
    try:
        downloaded = bse_client.bhavcopyReport(day_dt)
        file_path = Path(downloaded)
        if file_path.exists():
            return file_path.read_bytes(), "BSE_API"
    except Exception:
        pass

    for url in legacy_bhavcopy_urls(trading_day):
        payload = download_from_url(session, url=url, timeout=timeout)
        if payload:
            label = "LEGACY_ZIP" if ".zip" in url.lower() else "DIRECT_CSV"
            return payload, label

    return None, ""


def parse_day_rows(
    csv_bytes: bytes,
    trading_day: date,
    by_code: dict[str, UniverseRow],
    fetched_at_utc: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with io.StringIO(csv_bytes.decode("utf-8-sig", errors="ignore")) as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return rows
        for raw_row in reader:
            normalized = {
                normalize_key(str(key)): str(value).strip()
                for key, value in raw_row.items()
                if key is not None
            }

            raw_code = pick_value(
                normalized,
                [
                    "SC_CODE",
                    "SCRIP_CD",
                    "SCRIPCODE",
                    "SecurityCode",
                    "FinInstrmId",
                    "SctyCd",
                    "SctyId",
                ],
            )
            bse_code = clean_code(raw_code)
            if not bse_code:
                continue
            meta = by_code.get(bse_code)
            if meta is None:
                continue

            open_v = as_float(pick_value(normalized, ["OPEN", "Open", "OpnPric", "OpenPrice"]))
            high_v = as_float(pick_value(normalized, ["HIGH", "High", "HghPric", "HighPrice"]))
            low_v = as_float(pick_value(normalized, ["LOW", "Low", "LwPric", "LowPrice"]))
            close_v = as_float(pick_value(normalized, ["CLOSE", "Close", "ClsPric", "ClosePrice"]))
            volume = as_uint(
                pick_value(
                    normalized,
                    [
                        "NO_OF_SHRS",
                        "NOOFSHRS",
                        "TtlTradgVol",
                        "TotalTradedVolume",
                        "QtyTraded",
                    ],
                )
            )
            if open_v is None or high_v is None or low_v is None or close_v is None:
                continue

            rows.append(
                {
                    "exchange": "BSE",
                    "symbol": meta.symbol,
                    "bse_code": meta.bse_code,
                    "company_name": meta.company_name,
                    "date": trading_day.isoformat(),
                    "open": open_v,
                    "high": high_v,
                    "low": low_v,
                    "close": close_v,
                    "volume": volume,
                    "source": "BSE_BHAVCOPY",
                    "fetched_at_utc": fetched_at_utc,
                }
            )
    return rows


def insert_rows(
    session: requests.Session,
    url: str,
    user: str,
    password: str,
    timeout: int,
    database: str,
    prices_table: str,
    rows: list[dict[str, Any]],
    batch_size: int,
) -> int:
    if not rows:
        return 0
    db = validate_identifier(database)
    tbl = validate_identifier(prices_table)
    query = (
        f"INSERT INTO {db}.{tbl} "
        "(exchange,symbol,bse_code,company_name,date,open,high,low,close,volume,source,fetched_at_utc) "
        "FORMAT JSONEachRow"
    )

    inserted = 0
    for i in range(0, len(rows), batch_size):
        chunk = rows[i : i + batch_size]
        payload = "\n".join(json.dumps(row, ensure_ascii=False) for row in chunk).encode("utf-8")
        clickhouse_query(
            session=session,
            url=url,
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
        description="Direct BSE bhavcopy pipeline: backfill daily OHLCV into ClickHouse."
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
        default=100,
        help="Number of active BSE symbols to include from TickerMaster.",
    )
    parser.add_argument(
        "--years",
        type=int,
        default=10,
        help="Backfill window in years when start/end dates are not provided.",
    )
    parser.add_argument(
        "--start-date",
        default="",
        help="Backfill start date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end-date",
        default="",
        help="Backfill end date (YYYY-MM-DD). Default is today.",
    )
    parser.add_argument(
        "--truncate-first",
        action="store_true",
        help="Truncate target prices table before insert.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=90,
        help="ClickHouse HTTP timeout in seconds.",
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
        help="Print periodic progress during downloads.",
    )
    return parser.parse_args()


def resolve_date_range(args: argparse.Namespace) -> tuple[date, date]:
    end_date = parse_yyyy_mm_dd(args.end_date) if args.end_date else datetime.now(timezone.utc).date()
    start_date = parse_yyyy_mm_dd(args.start_date) if args.start_date else year_shifted(end_date, args.years)
    if start_date > end_date:
        raise ValueError("start_date must be <= end_date")
    return start_date, end_date


def main() -> None:
    args = parse_args()
    start_date, end_date = resolve_date_range(args)
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
        if args.truncate_first:
            truncate_table(
                session=session,
                url=args.clickhouse_url,
                user=args.user,
                password=args.password,
                timeout=args.timeout,
                database=args.database,
                table=args.prices_table,
            )
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"Failed ClickHouse setup/read: {exc}") from exc

    try:
        from bse import BSE
    except ImportError as exc:
        raise SystemExit("Missing dependency 'bse'. Run `uv sync` and retry.") from exc

    trading_dates = iter_weekdays(start_date, end_date)

    inserted_rows = 0
    dates_with_rows = 0
    skipped_empty = 0
    processed_files = 0
    downloaded_files = 0
    missing_reports = 0
    symbol_hits: set[str] = set()
    source_counts: dict[str, int] = {}

    with tempfile.TemporaryDirectory(prefix="bhavcopy_") as tmpdir:
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
                    if args.verbose and idx % 200 == 0:
                        print(
                            f"Progress {idx}/{len(trading_dates)} | "
                            f"downloaded={downloaded_files} missing={missing_reports}"
                        )
                    time.sleep(max(args.sleep_seconds, 0.0))
                    continue

                downloaded_files += 1
                processed_files += 1
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

                if args.verbose and idx % 200 == 0:
                    print(
                        f"Progress {idx}/{len(trading_dates)} | downloaded={downloaded_files} "
                        f"missing={missing_reports} inserted_rows={inserted_rows}"
                    )
                time.sleep(max(args.sleep_seconds, 0.0))

    print(
        json.dumps(
            {
                "target": f"{args.database}.{args.prices_table}",
                "universe_symbols": len(universe),
                "symbols_with_data": len(symbol_hits),
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "weekday_dates_considered": len(trading_dates),
                "bhavcopy_files_downloaded": downloaded_files,
                "bhavcopy_files_missing": missing_reports,
                "bhavcopy_files_processed": processed_files,
                "download_source_breakdown": source_counts,
                "dates_with_inserted_rows": dates_with_rows,
                "files_without_matching_rows": skipped_empty,
                "rows_inserted": inserted_rows,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
