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
CORPORATE_ACTIONS_TABLE=CorporateActions
ADJUSTED_PRICES_TABLE=AdjustedDailyPrices
WEEKLY_PRICES_TABLE=WeeklyPrices
MONTHLY_PRICES_TABLE=MonthlyPrices
UNIVERSE_SNAPSHOT_TABLE=UniverseSnapshot
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

Run unit tests:

```bash
python -m unittest discover -s tests -v
```

## 2.1) Start Backend API (FastAPI)
```bash
uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Open docs:
- `http://localhost:8000/docs`

Implemented endpoints:
- `GET /api/v1/health`
- `GET /api/v1/search?q=...`
- `GET /api/v1/tools`
- `GET /api/v1/tools/registry`
- `POST /api/v1/signals/run`
- `POST /api/v1/exits/run`
- `POST /api/v1/backtest/run-lite`
- `POST /api/v1/backtest/validate-lite`
- `POST /api/v1/backtest/validate`
- `POST /api/v1/backtest/run`
- `GET /api/v1/backtest/{run_id}/status`
- `GET /api/v1/backtest/history`
- `GET /api/v1/backtest/{run_id}`
- `GET /api/v1/backtest/{run_id}/trades?limit=500&offset=0`
- `GET /api/v1/backtest/{run_id}/equity-curve?limit=5000&offset=0`
- `POST /api/v1/backtest/compare`
- `GET /api/v1/indexes/snapshot?on_date=YYYY-MM-DD`
- `GET /api/v1/series?symbols=...&universe=stock|index&interval=1d|1w|1mo`
- `GET /api/v1/ohlcv/{symbol}?universe=stock|index&interval=1d|1w|1mo`
- `GET /api/v1/compare?symbols=...&universe=stock|index&interval=1d|1w|1mo&normalized_base=100`
- `GET /api/v1/correlation?symbols=...&universe=stock|index&interval=1d|1w|1mo&window=52`

When `POST /api/v1/backtest/run` is used (lite or full strategy spec), the API:
- Inserts a `running` row immediately and returns `run_id` quickly.
- Executes backtest in a background task.
- Exposes polling status at `GET /api/v1/backtest/{run_id}/status` (or full result at `GET /api/v1/backtest/{run_id}`).

It also auto-creates ClickHouse tables if missing:
- `BacktestRuns` (run status + summary metrics + spec/result JSON)
- `TradeLog` (per-trade rows)
- `BacktestEquityCurve` (equity curve points)

Phase 2 (first signal tool) example payload for `POST /api/v1/signals/run`:

```json
{
  "tool": "price_change",
  "universe": "stock",
  "symbols": ["RELIANCE", "TCS", "INFY"],
  "interval": "1w",
  "start_date": "2023-01-01",
  "end_date": "2026-03-01",
  "params": {
    "period": "1w",
    "direction": "down",
    "threshold_pct": 10
  },
  "limit": 500
}
```

You can also attach ordered filter steps before signal generation:

```json
{
  "tool": "price_change",
  "universe": "stock",
  "symbols": ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "SBIN"],
  "interval": "1w",
  "start_date": "2023-01-01",
  "end_date": "2026-03-01",
  "filters": [
    {
      "tool": "liquidity_filter",
      "params": {
        "min_avg_volume": 500000,
        "window_bars": 12
      }
    },
    {
      "tool": "price_filter",
      "params": {
        "min_price": 200,
        "max_price": 3000
      }
    },
    {
      "tool": "listing_age_filter",
      "params": {
        "min_weeks": 26
      }
    }
  ],
  "params": {
    "period": "1m",
    "direction": "any",
    "threshold_pct": 5
  },
  "limit": 500
}
```

Additional filter examples:
- `market_cap_filter` (proxy): `{"tool":"market_cap_filter","params":{"rank":"large","window_bars":20,"bucket_pct":33.34}}`
- `index_membership_filter`: `{"tool":"index_membership_filter","params":{"index_name":"NIFTY_50","membership_symbols":["RELIANCE","TCS","INFY"]}}`
- `sector_filter`: `{"tool":"sector_filter","params":{"sectors":["IT","BANKING"],"symbol_sector_map":{"TCS":"IT","INFY":"IT","HDFCBANK":"BANKING"}}}`

`moving_average_crossover` example payload:

```json
{
  "tool": "moving_average_crossover",
  "universe": "stock",
  "symbols": ["RELIANCE", "TCS", "INFY", "HDFCBANK"],
  "interval": "1w",
  "start_date": "2023-01-01",
  "end_date": "2026-03-01",
  "params": {
    "short_window": 4,
    "long_window": 12,
    "cross_direction": "above"
  },
  "limit": 500
}
```

`distance_from_high_low` example payload:

```json
{
  "tool": "distance_from_high_low",
  "universe": "stock",
  "symbols": ["RELIANCE", "TCS", "INFY", "HDFCBANK"],
  "interval": "1w",
  "start_date": "2023-01-01",
  "end_date": "2026-03-01",
  "params": {
    "reference": "high",
    "lookback_weeks": 52,
    "distance_pct": 20
  },
  "limit": 500
}
```

`relative_strength` example payload:

```json
{
  "tool": "relative_strength",
  "universe": "stock",
  "symbols": ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "SBIN"],
  "interval": "1w",
  "start_date": "2023-01-01",
  "end_date": "2026-03-01",
  "params": {
    "period": "1m",
    "rank": "top",
    "count": 3
  },
  "limit": 500
}
```

`volume_spike` example payload:

```json
{
  "tool": "volume_spike",
  "universe": "stock",
  "symbols": ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK"],
  "interval": "1w",
  "start_date": "2023-01-01",
  "end_date": "2026-03-01",
  "params": {
    "multiplier": 1.5,
    "avg_period": 8
  },
  "limit": 500
}
```

`consecutive_moves` example payload:

```json
{
  "tool": "consecutive_moves",
  "universe": "stock",
  "symbols": ["RELIANCE", "TCS", "INFY", "HDFCBANK"],
  "interval": "1w",
  "start_date": "2023-01-01",
  "end_date": "2026-03-01",
  "params": {
    "direction": "down",
    "count": 3
  },
  "limit": 500
}
```

`mean_reversion_zscore` example payload:

```json
{
  "tool": "mean_reversion_zscore",
  "universe": "stock",
  "symbols": ["RELIANCE", "TCS", "INFY", "HDFCBANK"],
  "interval": "1w",
  "start_date": "2023-01-01",
  "end_date": "2026-03-01",
  "params": {
    "lookback": 12,
    "z_threshold": 2.0
  },
  "limit": 500
}
```

`volatility_rank` example payload:

```json
{
  "tool": "volatility_rank",
  "universe": "stock",
  "symbols": ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "SBIN"],
  "interval": "1w",
  "start_date": "2023-01-01",
  "end_date": "2026-03-01",
  "params": {
    "lookback_weeks": 26,
    "rank": "high",
    "count": 3
  },
  "limit": 500
}
```

`index_relative` example payload:

```json
{
  "tool": "index_relative",
  "universe": "stock",
  "symbols": ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "SBIN"],
  "interval": "1w",
  "start_date": "2023-01-01",
  "end_date": "2026-03-01",
  "params": {
    "index_name": "NIFTY_50",
    "period": "1m",
    "threshold_pct": 5,
    "direction": "outperform"
  },
  "limit": 500
}
```

`rsi` example payload:

```json
{
  "tool": "rsi",
  "universe": "stock",
  "symbols": ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "SBIN"],
  "interval": "1w",
  "start_date": "2023-01-01",
  "end_date": "2026-03-01",
  "params": {
    "period": 14,
    "overbought": 70,
    "oversold": 30,
    "mode": "both"
  },
  "limit": 500
}
```

Exit tool example payload for `POST /api/v1/exits/run`:

```json
{
  "tool": "target_profit",
  "universe": "stock",
  "interval": "1w",
  "positions": [
    {"symbol": "RELIANCE", "entry_date": "2025-01-06", "entry_price": 1242.35},
    {"symbol": "TCS", "entry_date": "2025-01-06", "entry_price": 4025.65},
    {"symbol": "INFY", "entry_date": "2025-01-06", "entry_price": 1908.70}
  ],
  "end_date": "2026-03-01",
  "params": {
    "target_profit_pct": 10
  },
  "limit": 500
}
```

`stop_loss` params example:

```json
{
  "stop_loss_pct": 10
}
```

`time_based_exit` params example:

```json
{
  "hold_periods": 8
}
```

Additional exit params examples:

```json
{
  "trailing_stop_pct": 10
}
```

```json
{
  "entry_tool": "price_change",
  "entry_params": {"period": "1m", "direction": "down", "threshold_pct": 8},
  "reversal_tool": "price_change",
  "reversal_params": {"period": "1m", "direction": "up", "threshold_pct": 8}
}
```

```json
{
  "combine": "FIRST_HIT",
  "conditions": [
    {"tool": "stop_loss", "params": {"stop_loss_pct": 10}},
    {"tool": "time_based_exit", "params": {"hold_periods": 8}}
  ]
}
```

Lite backtest payload for `POST /api/v1/backtest/run-lite`:

```json
{
  "universe": "stock",
  "symbols": ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "SBIN"],
  "interval": "1w",
  "start_date": "2023-01-01",
  "end_date": "2026-03-01",
  "filters": [
    {"tool": "liquidity_filter", "params": {"min_avg_volume": 300000, "window_bars": 12}},
    {"tool": "price_filter", "params": {"min_price": 100}}
  ],
  "entry": {
    "tool": "price_change",
    "params": {"period": "1m", "direction": "down", "threshold_pct": 8}
  },
  "exit": {
    "tool": "stop_loss",
    "params": {"stop_loss_pct": 10}
  },
  "initial_capital": 1000000,
  "sizing_method": "fixed_amount",
  "fixed_amount": 50000,
  "max_positions": 10,
  "max_new_positions": 3,
  "slippage_bps": 10,
  "cost_pct": 0.05,
  "benchmark": "NIFTY_50"
}
```

You can explicitly choose sizing tool in `run-lite` using `sizing`:

```json
{
  "sizing": {
    "tool": "inverse_volatility",
    "params": {"lookback_bars": 20}
  }
}
```

`run-lite` response includes:
- `trades[].exit_reason` (tool-triggered exit reason or `forced_last_price_end`)
- `summary.liquidity_flag_count`
- `liquidity_flags[]` when participation exceeds 10% of bar volume
- `returns`, `risk`, `ratios`, `trade_stats`, `monthly_pnl_grid`, `cost_sensitivity` for performance analytics
- `benchmark_comparison` (alpha, beta, tracking error, information ratio, up/down capture) when `benchmark` is provided

Spec validation payload for `POST /api/v1/backtest/validate-lite`:

```json
{
  "universe": "stock",
  "symbols": ["RELIANCE", "TCS", "INFY"],
  "interval": "1w",
  "start_date": "2023-01-01",
  "end_date": "2026-03-01",
  "filters": [
    {"tool": "liquidity_filter", "params": {"min_avg_volume": 300000}}
  ],
  "entry": {
    "tool": "price_change",
    "params": {"period": "1m", "direction": "down", "threshold_pct": 8}
  },
  "exit": {
    "tool": "stop_loss",
    "params": {"stop_loss_pct": 10}
  }
}
```

Full strategy spec payload for `POST /api/v1/backtest/validate` or `POST /api/v1/backtest/run`:

```json
{
  "name": "Weekly Dip Reversion",
  "description": "Buy weekly dips, exit on stop or timeout",
  "universe": {
    "type": "stock",
    "symbols": ["RELIANCE", "TCS", "INFY"],
    "filters": [
      {"tool": "liquidity_filter", "params": {"min_avg_volume": 300000}}
    ]
  },
  "entry": {
    "signals": [
      {"tool": "price_change", "params": {"period": "1m", "direction": "down", "threshold_pct": 8}},
      {"tool": "rsi", "params": {"period": 14, "overbought": 70, "oversold": 30, "mode": "oversold"}}
    ],
    "combine": "AND",
    "rank_by": "price_change",
    "max_signals_per_period": 3
  },
  "exit": {
    "conditions": [
      {"tool": "stop_loss", "params": {"stop_loss_pct": 10}},
      {"tool": "time_based_exit", "params": {"hold_periods": 8}}
    ],
    "combine": "FIRST_HIT"
  },
  "sizing": {"tool": "fixed_amount", "params": {"amount": 50000}},
  "execution": {
    "initial_capital": 1000000,
    "entry_timing": "next_open",
    "rebalance": "weekly",
    "max_positions": 10,
    "costs": {"slippage_bps": 10, "round_trip_pct": 0.05}
  },
  "benchmark": "SENSEX",
  "date_range": {"start": "2023-01-01", "end": "2026-03-01"}
}
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

Daily index updater (example 7:45 PM):

```powershell
schtasks /Create /TN "INDEX-Daily-Update" /SC DAILY /ST 19:45 /F /TR "cmd /c cd /d C:\Users\rishs\OneDrive\Desktop\Stock && uv run python scripts\update_index_data_daily.py"
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

## 9) Incremental Daily Index Updater
Script: `scripts/update_index_data_daily.py`

```bash
uv run python scripts/update_index_data_daily.py --verbose
```

Behavior:
- Resumes each index from its own `max(date)` in `market.IndexDaily`
- Default start is `max(date) + 1`
- Automatically catches up missed days if machine was down
- Optional repair mode: `--replay-days 7`

## 10) Phase 1 Data Hardening (Corporate Actions + Adjusted Prices)
Script: `scripts/phase1_data_hardening.py`

Create Phase 1 tables:

```bash
uv run python scripts/phase1_data_hardening.py init-tables
```

Load corporate actions from CSV:

```bash
uv run python scripts/phase1_data_hardening.py load-actions --csv data/corporate_actions.csv
```

CSV should include at least:
- `symbol` or `bse_code`
- `action_type` (recommended: `SPLIT`, `BONUS`, `DIVIDEND`, `RIGHTS`)
- `ex_date`
- Optional: `ratio_from`, `ratio_to`, `announcement_date`, `source`, `notes`

Rebuild adjusted daily prices (split/bonus adjusted):

```bash
uv run python scripts/phase1_data_hardening.py rebuild-adjusted --truncate-first
```

Optional bounded rebuild:

```bash
uv run python scripts/phase1_data_hardening.py rebuild-adjusted --start-date 2020-01-01 --end-date 2024-12-31 --truncate-first
```

Validate continuity around split/bonus events:

```bash
uv run python scripts/phase1_data_hardening.py validate-splits --limit 20
```

Create aggregate tables (weekly/monthly/universe):

```bash
uv run python scripts/phase1_data_hardening.py init-agg-tables
```

Rebuild all aggregates from `AdjustedDailyPrices`:

```bash
uv run python scripts/phase1_data_hardening.py rebuild-aggregates --truncate-first
```

Optional bounded aggregate rebuild:

```bash
uv run python scripts/phase1_data_hardening.py rebuild-aggregates --start-date 2020-01-01 --end-date 2024-12-31 --truncate-first
```

Rebuild only weekly + universe (skip monthly):

```bash
uv run python scripts/phase1_data_hardening.py rebuild-aggregates --truncate-first --skip-monthly
```

### Programmatic Corporate Actions (No Manual CSV)
Script: `scripts/fetch_corporate_actions_yf.py`

This uses `TickerMaster.yahoo_ticker` and fetches split actions (optional dividends) from yfinance.

```bash
uv run python scripts/fetch_corporate_actions_yf.py --limit 1000 --truncate-first --verbose
```

Include dividends and export a CSV snapshot:

```bash
uv run python scripts/fetch_corporate_actions_yf.py --limit 1000 --include-dividends --truncate-first --export-csv data/corporate_actions_auto.csv
```

Then rebuild adjusted prices:

```bash
uv run python scripts/phase1_data_hardening.py rebuild-adjusted --truncate-first
```

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
