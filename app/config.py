from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


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


@dataclass(frozen=True)
class Settings:
    clickhouse_url: str
    clickhouse_database: str
    clickhouse_user: str
    clickhouse_password: str
    prices_table: str
    weekly_prices_table: str
    monthly_prices_table: str
    universe_snapshot_table: str
    ticker_table: str
    index_table: str
    cors_origins: list[str]


def get_settings() -> Settings:
    dotenv = load_dotenv(Path(".env"))
    cors_raw = env_or_dotenv(dotenv, "CORS_ORIGINS", default="*")
    cors_origins = [item.strip() for item in cors_raw.split(",") if item.strip()]
    if not cors_origins:
        cors_origins = ["*"]
    return Settings(
        clickhouse_url=env_or_dotenv(dotenv, "CLICKHOUSE_URL", default="http://localhost:8123"),
        clickhouse_database=env_or_dotenv(dotenv, "CLICKHOUSE_DATABASE", default="market"),
        clickhouse_user=env_or_dotenv(dotenv, "CLICKHOUSE_USER", "CH_USER", default="default"),
        clickhouse_password=env_or_dotenv(dotenv, "CLICKHOUSE_PASSWORD", "CH_PASSWORD", default=""),
        prices_table=env_or_dotenv(dotenv, "OHLCV_TABLE", default="DailyPricesBhavcopy"),
        weekly_prices_table=env_or_dotenv(dotenv, "WEEKLY_PRICES_TABLE", default="WeeklyPrices"),
        monthly_prices_table=env_or_dotenv(dotenv, "MONTHLY_PRICES_TABLE", default="MonthlyPrices"),
        universe_snapshot_table=env_or_dotenv(dotenv, "UNIVERSE_SNAPSHOT_TABLE", default="UniverseSnapshot"),
        ticker_table=env_or_dotenv(dotenv, "CLICKHOUSE_TABLE", default="TickerMaster"),
        index_table=env_or_dotenv(dotenv, "INDEX_TABLE", default="IndexDaily"),
        cors_origins=cors_origins,
    )
