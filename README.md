# Indian Stocks Backtesting Playground (Data Layer)

This repo currently focuses on data aggregation for Indian stocks:
- BSE symbol master ingestion into ClickHouse
- Direct BSE bhavcopy backfill for daily OHLCV (sample or full universe)
- Incremental daily catch-up updates from last loaded date

## Stack
- Python 3.11+
- `uv` for dependency management
- ClickHouse (Docker)
- BSE bhavcopy downloads via `bse` package

## Prerequisites
- Docker Desktop running
- Python 3.11+
- `uv` installed

## Environment Variables
Create `.env` in project root:

```env
CH_USER=your_clickhouse_user
CH_PASSWORD=your_clickhouse_password
```

Optional overrides used by scripts:

```env
CLICKHOUSE_URL=http://localhost:8123
CLICKHOUSE_DATABASE=market
CLICKHOUSE_TABLE=TickerMaster
OHLCV_TABLE=DailyPricesBhavcopy
INDEX_TABLE=IndexDaily
CLICKHOUSE_USER=your_clickhouse_user
CLICKHOUSE_PASSWORD=your_clickhouse_password
```

## 1) Start ClickHouse
`docker-compose.yml` already includes ClickHouse with env-based auth.

```bash
docker compose up -d
docker compose ps
```

Useful endpoints:
- HTTP: `http://localhost:8123`
- SQL UI: `http://localhost:8123/play`
- Dashboard: `http://localhost:8123/dashboard`
- Native TCP: `localhost:9000`

## 2) Install Python Dependencies
```bash
uv sync
```

## 3) Create `TickerMaster` Table (one-time)
Run this once before symbol ingestion:

```bash
docker exec -it clickhouse clickhouse-client --user <CLICKHOUSE_USER> --password <CLICKHOUSE_PASSWORD> --multiquery --query "
CREATE DATABASE IF NOT EXISTS market;
CREATE TABLE IF NOT EXISTS market.TickerMaster
(
    exchange LowCardinality(String),
    symbol String,
    isin String,
    company_name String,
    status LowCardinality(String),
    yahoo_ticker String,
    bse_code String,
    source String,
    fetched_at_utc String,
    ingested_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (exchange, symbol);
"
```

## 4) Ingest BSE Symbol Master
Script: `scripts/create_symbol_master.py`

```bash
uv run python scripts/create_symbol_master.py
```

Expected output includes:
- `rows_total`
- `rows_bse`
- `target` (`market.TickerMaster`)
- `inserted_rows`

## 5) One-Time Backfill (10 Years)
Script: `scripts/backfill_bse_prices.py`

Sample run (100 symbols):

```bash
uv run python scripts/backfill_bse_prices.py --limit 100 --years 10 --truncate-first
```

Full active BSE universe:

```bash
uv run python scripts/backfill_bse_prices.py --limit 0 --years 10 --truncate-first --batch-size 10000 --verbose
```

What this does:
- Reads BSE symbols from `market.TickerMaster`
- Downloads BSE bhavcopy files date-wise (direct exchange source)
- Creates `market.DailyPricesBhavcopy` if missing
- Filters to your selected symbol universe and inserts into ClickHouse

Notes:
- `--limit 0` means all active BSE symbols in `TickerMaster`
- Historical files are fetched using current + legacy bhavcopy URL fallbacks

## 6) Incremental Daily Updater
Script: `scripts/update_bse_prices_daily.py`

```bash
uv run python scripts/update_bse_prices_daily.py --limit 0 --verbose
```

Behavior:
- Reads `max(date)` from target table
- Default start is `max(date) + 1`
- Automatically catches up missed days if machine was down
- Optional repair mode: `--replay-days 7`

## 7) Lightweight Scheduling (Windows Task Scheduler)
Daily updater (example 7:30 PM):

```powershell
schtasks /Create /TN "BSE-Daily-Update-All" /SC DAILY /ST 19:30 /F /TR "cmd /c cd /d C:\Users\rishs\OneDrive\Desktop\Stock && uv run python scripts\update_bse_prices_daily.py --limit 0"
```

Weekly repair (Sunday 8:00 PM, replay last 7 days):

```powershell
schtasks /Create /TN "BSE-Weekly-Repair" /SC WEEKLY /D SUN /ST 20:00 /F /TR "cmd /c cd /d C:\Users\rishs\OneDrive\Desktop\Stock && uv run python scripts\update_bse_prices_daily.py --limit 0 --replay-days 7"
```

## 8) Backfill Index Data (NIFTY, SENSEX, Others)
Script: `scripts/backfill_index_data.py`

Default run (aligns to your stock table date range):

```bash
uv run python scripts/backfill_index_data.py --truncate-first --verbose
```

Custom range:

```bash
uv run python scripts/backfill_index_data.py --start-date 2016-03-01 --end-date 2026-03-01 --truncate-first
```

Custom index mapping:

```bash
uv run python scripts/backfill_index_data.py --index-map-json "{\"NIFTY_50\":\"^NSEI\",\"SENSEX\":\"^BSESN\",\"NIFTY_BANK\":\"^NSEBANK\"}" --truncate-first
```

Creates table: `market.IndexDaily`

## Validation Queries
Check symbol master:

```sql
SELECT exchange, count()
FROM market.TickerMaster
GROUP BY exchange;
```

Check prices row count:

```sql
SELECT count() FROM market.DailyPricesBhavcopy;
```

Check per-symbol coverage:

```sql
SELECT symbol, min(date) AS min_date, max(date) AS max_date, count() AS rows
FROM market.DailyPricesBhavcopy
GROUP BY symbol
ORDER BY rows DESC
LIMIT 20;
```

Check overall loaded range:

```sql
SELECT min(date) AS min_date, max(date) AS max_date, count() AS rows
FROM market.DailyPricesBhavcopy;
```

Check index table coverage:

```sql
SELECT index_name, min(date), max(date), count() AS rows
FROM market.IndexDaily
GROUP BY index_name
ORDER BY index_name;
```

## Useful Commands
Stop services:

```bash
docker compose down
```

Reset all ClickHouse data (destructive):

```bash
docker compose down -v
docker compose up -d
```
