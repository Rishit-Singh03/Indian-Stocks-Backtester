# Indian Stocks Backtesting Playground (Data Layer)

This repo currently focuses on data aggregation for Indian stocks:
- BSE symbol master ingestion into ClickHouse
- Direct BSE bhavcopy backfill for daily OHLCV (sample size, default 100 symbols)

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

## 5) Backfill 10 Years Daily Data for 100 Stocks
Script: `scripts/backfill_bse_prices.py`

```bash
uv run python scripts/backfill_bse_prices.py --limit 100 --years 10 --truncate-first
```

What this does:
- Reads BSE symbols from `market.TickerMaster`
- Downloads BSE bhavcopy files date-wise (direct exchange source)
- Creates `market.DailyPricesBhavcopy` if missing
- Filters to your selected symbol universe and inserts into ClickHouse

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
