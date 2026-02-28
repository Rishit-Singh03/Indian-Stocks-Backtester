from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BSE_LISTING_URLS = [
    "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w?Group=&Scripcode=&industry=&segment=Equity&status=Active",
    "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w?group=&industry=&segment=Equity&status=Active",
]


@dataclass
class SymbolRow:
    exchange: str
    symbol: str
    isin: str
    company_name: str
    status: str
    yahoo_ticker: str
    bse_code: str
    source: str
    fetched_at_utc: str

    def as_dict(self) -> dict[str, str]:
        return {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "isin": self.isin,
            "company_name": self.company_name,
            "status": self.status,
            "yahoo_ticker": self.yahoo_ticker,
            "bse_code": self.bse_code,
            "source": self.source,
            "fetched_at_utc": self.fetched_at_utc,
        }


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
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json,text/csv,*/*",
        }
    )
    return session


def normalize_text(value: Any, upper: bool = False) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return text.upper() if upper else text


def normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def pick_field(row: dict[str, Any], candidates: list[str]) -> str:
    normalized = {normalize_key(k): v for k, v in row.items()}
    for candidate in candidates:
        raw = normalized.get(normalize_key(candidate))
        if raw is None:
            continue
        text = normalize_text(raw)
        if text:
            return text
    return ""


def find_table(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        if payload and isinstance(payload[0], dict):
            return payload
        return []
    if not isinstance(payload, dict):
        return []

    for key in ("Table", "table", "Data", "data", "Result", "result", "value"):
        value = payload.get(key)
        if isinstance(value, list) and (not value or isinstance(value[0], dict)):
            return value

    for value in payload.values():
        if isinstance(value, list) and (not value or isinstance(value[0], dict)):
            return value
        if isinstance(value, dict):
            maybe = find_table(value)
            if maybe:
                return maybe
    return []


def fetch_bse_rows(session: requests.Session, timeout: int, fetched_at_utc: str) -> list[SymbolRow]:
    headers = {
        "Referer": "https://www.bseindia.com/",
        "Accept": "application/json, text/plain, */*",
    }
    errors: list[str] = []
    for url in BSE_LISTING_URLS:
        try:
            response = session.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            payload = response.json()
            table = find_table(payload)
            if not table:
                errors.append(f"{url} -> empty table")
                continue

            rows: list[SymbolRow] = []
            for row in table:
                symbol = pick_field(row, ["SecurityId", "ScripId", "Scrip ID", "Symbol", "Ticker"])
                bse_code = pick_field(row, ["SecurityCode", "ScripCode", "SCRIP_CD", "SC_CODE", "Code"])
                isin = pick_field(row, ["ISIN", "ISINNo", "ISIN_NUMBER", "ISIN Number"])
                company = pick_field(
                    row, ["SecurityName", "CompanyName", "ScripName", "SC_NAME", "Name of Company"]
                )
                status = pick_field(row, ["Status", "TradingStatus", "ActiveFlag"]) or "ACTIVE"

                symbol = normalize_text(symbol, upper=True).replace(" ", "")
                bse_code = normalize_text(bse_code, upper=True)
                isin = normalize_text(isin, upper=True)
                company = normalize_text(company)
                status = normalize_text(status, upper=True)

                if not symbol:
                    symbol = bse_code
                if not symbol:
                    continue

                yahoo_base = bse_code if bse_code else symbol
                rows.append(
                    SymbolRow(
                        exchange="BSE",
                        symbol=symbol,
                        isin=isin,
                        company_name=company,
                        status=status,
                        yahoo_ticker=f"{yahoo_base}.BO",
                        bse_code=bse_code,
                        source=url,
                        fetched_at_utc=fetched_at_utc,
                    )
                )
            return rows
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{url} -> {exc}")
    raise RuntimeError("Unable to fetch BSE listings.\n" + "\n".join(errors))


def score_row(row: SymbolRow) -> int:
    return sum(
        [
            int(bool(row.symbol)),
            int(bool(row.isin)),
            int(bool(row.company_name)),
            int(bool(row.status)),
            int(bool(row.yahoo_ticker)),
            int(bool(row.bse_code)),
        ]
    )


def deduplicate(rows: list[SymbolRow]) -> list[SymbolRow]:
    deduped: dict[tuple[str, str], SymbolRow] = {}
    for row in rows:
        key = (row.exchange, row.symbol)
        current = deduped.get(key)
        if current is None or score_row(row) > score_row(current):
            deduped[key] = row
    return sorted(deduped.values(), key=lambda item: (item.exchange, item.symbol))


def validate_identifier(identifier: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        raise ValueError(
            f"Invalid ClickHouse identifier: {identifier!r}. Use only letters, numbers, and underscores."
        )
    return identifier


def write_clickhouse(
    session: requests.Session,
    rows: list[SymbolRow],
    clickhouse_url: str,
    database: str,
    table: str,
    user: str,
    password: str,
    timeout: int,
) -> int:
    db = validate_identifier(database)
    tbl = validate_identifier(table)
    target = f"{db}.{tbl}"
    auth = (user, password)

    columns = list(SymbolRow.__annotations__.keys())
    payload = "\n".join(json.dumps(row.as_dict(), ensure_ascii=False) for row in rows)
    insert_response = session.post(
        clickhouse_url,
        params={"query": f"INSERT INTO {target} ({', '.join(columns)}) FORMAT JSONEachRow"},
        data=payload.encode("utf-8"),
        auth=auth,
        timeout=timeout,
        headers={"Content-Type": "application/json"},
    )
    insert_response.raise_for_status()
    return len(rows)


def parse_args() -> argparse.Namespace:
    dotenv = load_dotenv(Path(".env"))
    parser = argparse.ArgumentParser(
        description="Fetch BSE symbol master and load it into ClickHouse."
    )
    parser.add_argument(
        "--clickhouse-url",
        default=env_or_dotenv(dotenv, "CLICKHOUSE_URL", default="http://localhost:8123"),
        help="ClickHouse HTTP URL.",
    )
    parser.add_argument(
        "--database",
        default=env_or_dotenv(dotenv, "CLICKHOUSE_DATABASE", default="market"),
        help="Target ClickHouse database.",
    )
    parser.add_argument(
        "--table",
        default=env_or_dotenv(dotenv, "CLICKHOUSE_TABLE", default="TickerMaster"),
        help="Target ClickHouse table.",
    )
    parser.add_argument(
        "--user",
        default=env_or_dotenv(dotenv, "CLICKHOUSE_USER", "CH_USER", default="default"),
        help="ClickHouse user.",
    )
    parser.add_argument(
        "--password",
        default=env_or_dotenv(dotenv, "CLICKHOUSE_PASSWORD", "CH_PASSWORD", default=""),
        help="ClickHouse password.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="HTTP timeout in seconds.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    session = make_session()
    fetched_at_utc = datetime.now(timezone.utc).isoformat()

    try:
        bse_rows = fetch_bse_rows(session=session, timeout=args.timeout, fetched_at_utc=fetched_at_utc)
        rows = deduplicate(bse_rows)
    except requests.RequestException as exc:
        raise SystemExit(
            "Network request failed while fetching BSE symbol list.\n"
            f"Details: {exc}"
        ) from exc
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc

    try:
        inserted = write_clickhouse(
            session=session,
            rows=rows,
            clickhouse_url=args.clickhouse_url,
            database=args.database,
            table=args.table,
            user=args.user,
            password=args.password,
            timeout=args.timeout,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    except requests.RequestException as exc:
        raise SystemExit(
            "ClickHouse write failed. Check URL/auth and whether ClickHouse is running.\n"
            f"Details: {exc}"
        ) from exc

    print(
        json.dumps(
            {
                "rows_total": len(rows),
                "rows_bse": sum(1 for row in rows if row.exchange == "BSE"),
                "target": f"{args.database}.{args.table}",
                "inserted_rows": inserted,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
