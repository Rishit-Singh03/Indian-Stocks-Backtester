from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
import json
from math import sqrt
import re
from typing import Any, Literal
from uuid import uuid4

from fastapi import BackgroundTasks, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, ValidationError

from app.backtest import (
    lite_payload_to_strategy_spec,
    run_lite_backtest,
    strategy_spec_to_lite_payload,
    validate_lite_spec,
    validate_strategy_spec,
)
from app.backtest.persistence import (
    build_equity_rows,
    build_run_row,
    build_trade_rows,
    ensure_backtest_tables,
    insert_equity_rows,
    insert_run_row,
    insert_trade_rows,
    normalize_run_id,
)
from app.clickhouse import ClickHouseClient, sql_string, validate_identifier
from app.config import get_settings
from app.strategy import TOOL_REGISTRY, ToolValidationError


settings = get_settings()
Interval = Literal["1d", "1w", "1mo"]
app = FastAPI(title="Stock Dashboard API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ch = ClickHouseClient(
    base_url=settings.clickhouse_url,
    user=settings.clickhouse_user,
    password=settings.clickhouse_password,
    timeout=45,
)
_backtest_storage_ready = False


def parse_date_or_default(value: str | None, default_value: date) -> date:
    if not value:
        return default_value
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid date: {value}") from exc


def split_symbols(symbols: str, max_count: int | None = None) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in symbols.split(","):
        value = item.strip().upper()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    if not out:
        raise HTTPException(status_code=400, detail="Provide at least one symbol/index.")
    if max_count is not None and len(out) > max_count:
        raise HTTPException(status_code=400, detail=f"Too many symbols. Max allowed: {max_count}")
    return out


def normalize_symbol_list(symbols: list[str], max_count: int | None = None) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in symbols:
        value = str(raw).strip().upper()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    if not out:
        raise HTTPException(status_code=400, detail="Provide at least one symbol/index.")
    if max_count is not None and len(out) > max_count:
        raise HTTPException(status_code=400, detail=f"Too many symbols. Max allowed: {max_count}")
    return out


def sql_string_list(values: list[str]) -> str:
    return ", ".join(sql_string(value) for value in values)


def ensure_backtest_storage_ready() -> None:
    global _backtest_storage_ready
    if _backtest_storage_ready:
        return
    ensure_backtest_tables(
        ch=ch,
        database=settings.clickhouse_database,
        runs_table=settings.backtest_runs_table,
        trades_table=settings.backtest_trades_table,
        equity_table=settings.backtest_equity_table,
    )
    _backtest_storage_ready = True


def parse_run_id_or_400(run_id: str) -> str:
    try:
        return normalize_run_id(run_id)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"Invalid run_id: {run_id}") from exc


class SignalRunRequest(BaseModel):
    tool: str = Field(..., min_length=1)
    universe: Literal["stock", "index"] = "stock"
    symbols: list[str] = Field(..., min_length=1, max_length=500)
    interval: Interval = "1w"
    start_date: str | None = None
    end_date: str | None = None
    filters: list[dict[str, Any]] = Field(default_factory=list)
    params: dict[str, Any] = Field(default_factory=dict)
    limit: int = Field(default=1000, ge=1, le=50000)


class ExitPosition(BaseModel):
    position_id: str | None = None
    symbol: str = Field(..., min_length=1)
    entry_date: str = Field(..., min_length=1)
    entry_price: float = Field(..., gt=0.0)


class ExitRunRequest(BaseModel):
    tool: str = Field(..., min_length=1)
    universe: Literal["stock", "index"] = "stock"
    interval: Interval = "1w"
    positions: list[ExitPosition] = Field(..., min_length=1, max_length=1000)
    start_date: str | None = None
    end_date: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    limit: int = Field(default=1000, ge=1, le=50000)


class BacktestStep(BaseModel):
    tool: str = Field(..., min_length=1)
    params: dict[str, Any] = Field(default_factory=dict)


class BacktestLiteRequest(BaseModel):
    universe: Literal["stock", "index"] = "stock"
    symbols: list[str] = Field(..., min_length=1, max_length=500)
    interval: Interval = "1w"
    start_date: str | None = None
    end_date: str | None = None
    filters: list[BacktestStep] = Field(default_factory=list)
    entry: BacktestStep
    exit: BacktestStep
    sizing: BacktestStep | None = None
    initial_capital: float = Field(default=1_000_000.0, gt=0.0)
    sizing_method: Literal["fixed_amount", "equal_weight"] = "fixed_amount"
    fixed_amount: float = Field(default=10_000.0, gt=0.0)
    max_positions: int = Field(default=20, ge=1, le=5000)
    max_new_positions: int = Field(default=10, ge=1, le=5000)
    slippage_bps: float = Field(default=0.0, ge=0.0, le=5000.0)
    cost_pct: float = Field(default=0.0, ge=0.0, le=50.0)
    benchmark: str | None = None


class BacktestCompareRequest(BaseModel):
    run_ids: list[str] = Field(..., min_length=2, max_length=10)


def _load_benchmark_rows_for_index_relative(index_name: str, interval: Interval, start_d: date, end_d: date) -> list[dict[str, Any]]:
    benchmark_name = str(index_name).strip().upper()
    if not benchmark_name:
        raise HTTPException(status_code=400, detail="index_relative requires params.index_name")
    try:
        benchmark_rows = load_series_rows([benchmark_name], "index", interval, start_d, end_d)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Failed loading benchmark index data: {exc}") from exc
    if not benchmark_rows:
        raise HTTPException(status_code=400, detail=f"No benchmark data found for index_name={benchmark_name}")
    return benchmark_rows


def _hydrate_signal_step_for_backtest(
    step: dict[str, Any],
    *,
    interval: Interval,
    start_d: date,
    end_d: date,
) -> dict[str, Any]:
    tool_name = str(step.get("tool", "")).strip().lower()
    params = dict(step.get("params", {}))
    if tool_name == "index_relative":
        benchmark_name = str(params.get("index_name", "")).strip().upper()
        params["index_name"] = benchmark_name
        params["benchmark_rows"] = _load_benchmark_rows_for_index_relative(benchmark_name, interval, start_d, end_d)
        return {"tool": tool_name, "params": params}
    if tool_name == "combined_signal":
        raw_signals = params.get("signals", [])
        if not isinstance(raw_signals, list):
            raise HTTPException(status_code=400, detail="combined_signal params.signals must be a list")
        hydrated: list[dict[str, Any]] = []
        for idx, signal_step in enumerate(raw_signals):
            if not isinstance(signal_step, dict):
                raise HTTPException(status_code=400, detail=f"combined_signal signals[{idx}] must be an object")
            hydrated.append(
                _hydrate_signal_step_for_backtest(
                    signal_step,
                    interval=interval,
                    start_d=start_d,
                    end_d=end_d,
                )
            )
        params["signals"] = hydrated
        return {"tool": tool_name, "params": params}
    return {"tool": tool_name, "params": params}


def _resolve_backtest_run_payload(payload: dict[str, Any]) -> tuple[dict[str, Any], BacktestLiteRequest, str]:
    raw_strategy_spec = payload.get("strategy_spec")
    if isinstance(raw_strategy_spec, dict):
        try:
            normalized_full = validate_strategy_spec(registry=TOOL_REGISTRY, strategy_spec=raw_strategy_spec)
            lite_payload = strategy_spec_to_lite_payload(normalized_full)
            lite_request = BacktestLiteRequest.model_validate(lite_payload)
            return normalized_full, lite_request, "full"
        except ToolValidationError as exc:
            raise HTTPException(status_code=400, detail=f"Invalid strategy spec: {exc}") from exc
        except ValidationError as exc:
            raise HTTPException(status_code=400, detail=f"Invalid full->lite mapping: {exc}") from exc

    if isinstance(payload.get("universe"), dict):
        try:
            normalized_full = validate_strategy_spec(registry=TOOL_REGISTRY, strategy_spec=payload)
            lite_payload = strategy_spec_to_lite_payload(normalized_full)
            lite_request = BacktestLiteRequest.model_validate(lite_payload)
            return normalized_full, lite_request, "full"
        except ToolValidationError as exc:
            raise HTTPException(status_code=400, detail=f"Invalid strategy spec: {exc}") from exc
        except ValidationError as exc:
            raise HTTPException(status_code=400, detail=f"Invalid full->lite mapping: {exc}") from exc

    try:
        lite_request = BacktestLiteRequest.model_validate(payload)
    except ValidationError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid backtest payload: {exc.errors()}") from exc

    lite_as_full = lite_payload_to_strategy_spec(lite_request.model_dump())
    try:
        normalized_full = validate_strategy_spec(registry=TOOL_REGISTRY, strategy_spec=lite_as_full)
    except ToolValidationError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid strategy spec: {exc}") from exc
    return normalized_full, lite_request, "lite"


def table_name(which: Literal["stock", "index", "ticker"]) -> str:
    if which == "stock":
        return validate_identifier(settings.prices_table)
    if which == "index":
        return validate_identifier(settings.index_table)
    return validate_identifier(settings.ticker_table)


def load_series_rows(
    symbols: list[str],
    universe: Literal["stock", "index"],
    interval: Interval,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    db = validate_identifier(settings.clickhouse_database)
    universe_snapshot_tbl = validate_identifier(settings.universe_snapshot_table)
    if universe == "stock":
        if interval == "1d":
            target = validate_identifier(settings.prices_table)
            symbol_column = "symbol"
            date_column = "date"
        elif interval == "1w":
            target = validate_identifier(settings.weekly_prices_table)
            symbol_column = "symbol"
            date_column = "week_start"
        else:
            target = validate_identifier(settings.monthly_prices_table)
            symbol_column = "symbol"
            date_column = "month_start"
    else:
        target = validate_identifier(settings.index_table)
        symbol_column = "index_name"
        date_column = "date"
    list_sql = sql_string_list(symbols)
    start_sql = sql_string(start_date.isoformat())
    end_sql = sql_string(end_date.isoformat())

    if universe == "stock" and interval == "1d":
        query = f"""
SELECT
    p.{symbol_column} AS symbol,
    p.{date_column} AS date,
    p.open,
    p.high,
    p.low,
    p.close,
    p.volume
FROM {db}.{target} p
WHERE p.{symbol_column} IN ({list_sql})
  AND p.{date_column} BETWEEN {start_sql} AND {end_sql}
  AND (p.{symbol_column}, toDate(toStartOfWeek(p.{date_column}, 1))) IN
  (
      SELECT symbol, week_start
      FROM {db}.{universe_snapshot_tbl}
      WHERE is_active = 1
        AND symbol IN ({list_sql})
        AND week_start BETWEEN toDate(toStartOfWeek(toDate({start_sql}), 1)) AND toDate(toStartOfWeek(toDate({end_sql}), 1))
  )
ORDER BY symbol, date
FORMAT JSONEachRow
""".strip()
        return ch.query_rows(query)

    if universe == "stock" and interval == "1w":
        query = f"""
SELECT
    p.{symbol_column} AS symbol,
    p.{date_column} AS date,
    p.open,
    p.high,
    p.low,
    p.close,
    p.volume
FROM {db}.{target} p
WHERE p.{symbol_column} IN ({list_sql})
  AND p.{date_column} BETWEEN toDate(toStartOfWeek(toDate({start_sql}), 1)) AND toDate(toStartOfWeek(toDate({end_sql}), 1))
  AND (p.{symbol_column}, p.{date_column}) IN
  (
      SELECT symbol, week_start
      FROM {db}.{universe_snapshot_tbl}
      WHERE is_active = 1
        AND symbol IN ({list_sql})
        AND week_start BETWEEN toDate(toStartOfWeek(toDate({start_sql}), 1)) AND toDate(toStartOfWeek(toDate({end_sql}), 1))
  )
ORDER BY symbol, date
FORMAT JSONEachRow
""".strip()
        return ch.query_rows(query)

    if universe == "stock" and interval == "1mo":
        query = f"""
SELECT
    p.{symbol_column} AS symbol,
    p.{date_column} AS date,
    p.open,
    p.high,
    p.low,
    p.close,
    p.volume
FROM {db}.{target} p
WHERE p.{symbol_column} IN ({list_sql})
  AND p.{date_column} BETWEEN toDate(toStartOfMonth(toDate({start_sql}))) AND toDate(toStartOfMonth(toDate({end_sql})))
  AND (p.{symbol_column}, p.{date_column}) IN
  (
      SELECT
          symbol,
          toDate(toStartOfMonth(week_start)) AS month_start
      FROM {db}.{universe_snapshot_tbl}
      WHERE is_active = 1
        AND symbol IN ({list_sql})
        AND week_start BETWEEN toDate(toStartOfWeek(toDate({start_sql}), 1)) AND toDate(toStartOfWeek(toDate({end_sql}), 1))
      GROUP BY symbol, month_start
  )
ORDER BY symbol, date
FORMAT JSONEachRow
""".strip()
        return ch.query_rows(query)

    if universe == "index" and interval == "1d":
        query = f"""
SELECT
    {symbol_column} AS symbol,
    {date_column} AS date,
    open,
    high,
    low,
    close,
    volume
FROM {db}.{target}
WHERE {symbol_column} IN ({list_sql})
  AND {date_column} BETWEEN {start_sql} AND {end_sql}
ORDER BY symbol, date
FORMAT JSONEachRow
""".strip()
        return ch.query_rows(query)

    # Index weekly/monthly rollup from daily bars.
    bucket_expr = "toDate(toStartOfWeek(date, 1))" if interval == "1w" else "toDate(toStartOfMonth(date))"
    query = f"""
SELECT
    symbol,
    bucket_start AS date,
    argMin(open, date) AS open,
    max(high) AS high,
    min(low) AS low,
    argMax(close, date) AS close,
    sum(volume) AS volume
FROM
(
    SELECT
        {symbol_column} AS symbol,
        date,
        open,
        high,
        low,
        close,
        volume,
        {bucket_expr} AS bucket_start
    FROM {db}.{target}
    WHERE {symbol_column} IN ({list_sql})
      AND date BETWEEN {start_sql} AND {end_sql}
)
GROUP BY symbol, bucket_start
ORDER BY symbol, bucket_start
FORMAT JSONEachRow
""".strip()
    return ch.query_rows(query)


def corr(x: list[float], y: list[float]) -> float | None:
    n = len(x)
    if n < 2 or n != len(y):
        return None
    mx = sum(x) / n
    my = sum(y) / n
    num = sum((a - mx) * (b - my) for a, b in zip(x, y))
    denx = sqrt(sum((a - mx) ** 2 for a in x))
    deny = sqrt(sum((b - my) ** 2 for b in y))
    if denx == 0 or deny == 0:
        return None
    return num / (denx * deny)


@app.get("/api/v1/health")
def health() -> dict[str, Any]:
    try:
        rows = ch.query_rows("SELECT 1 AS ok FORMAT JSONEachRow")
        return {"status": "ok", "clickhouse": bool(rows and rows[0].get("ok") == 1)}
    except Exception as exc:  # noqa: BLE001
        return {"status": "degraded", "clickhouse": False, "error": str(exc)}


@app.get("/api/v1/tools")
def tools() -> dict[str, Any]:
    items = TOOL_REGISTRY.list_tools()
    return {"count": len(items), "tools": items}


@app.get("/api/v1/tools/registry")
def tools_registry() -> dict[str, Any]:
    return tools()


@app.post("/api/v1/signals/run")
def run_signal_tool(request: SignalRunRequest) -> dict[str, Any]:
    if request.universe != "stock":
        raise HTTPException(status_code=400, detail="Phase 2 currently supports stock universe only.")

    names = normalize_symbol_list(request.symbols, max_count=500)
    today = datetime.now(timezone.utc).date()
    end_d = parse_date_or_default(request.end_date, today)
    start_d = parse_date_or_default(request.start_date, end_d - timedelta(days=365 * 3))
    if start_d > end_d:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")

    try:
        rows = load_series_rows(names, request.universe, request.interval, start_d, end_d)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    bars_before_filters = len(rows)
    filter_summaries: list[dict[str, Any]] = []
    if request.filters:
        for idx, step in enumerate(request.filters, start=1):
            if not isinstance(step, dict):
                raise HTTPException(status_code=400, detail=f"filters[{idx - 1}] must be an object")
            filter_name = str(step.get("tool", "")).strip().lower()
            if not filter_name:
                raise HTTPException(status_code=400, detail=f"filters[{idx - 1}].tool is required")
            filter_params_raw = step.get("params", {})
            if filter_params_raw is None:
                filter_params_raw = {}
            if not isinstance(filter_params_raw, dict):
                raise HTTPException(status_code=400, detail=f"filters[{idx - 1}].params must be an object")
            filter_params = dict(filter_params_raw)
            filter_params.setdefault("interval", request.interval)
            before = len(rows)
            try:
                rows = TOOL_REGISTRY.run_filter(filter_name, rows, filter_params)
            except ToolValidationError as exc:
                raise HTTPException(status_code=400, detail=f"Filter {filter_name}: {exc}") from exc
            except Exception as exc:  # noqa: BLE001
                raise HTTPException(status_code=500, detail=f"Filter {filter_name} failed: {exc}") from exc
            after = len(rows)
            filter_summaries.append(
                {
                    "order": idx,
                    "tool": filter_name,
                    "bars_before": before,
                    "bars_after": after,
                    "bars_removed": max(0, before - after),
                }
            )

    tool_name = request.tool.strip().lower()
    params = dict(request.params)
    params.setdefault("interval", request.interval)
    if tool_name == "index_relative":
        index_name = str(params.get("index_name", "")).strip().upper()
        params["index_name"] = index_name
        params["benchmark_rows"] = _load_benchmark_rows_for_index_relative(index_name, request.interval, start_d, end_d)
    if tool_name == "combined_signal":
        hydrated = _hydrate_signal_step_for_backtest(
            {"tool": tool_name, "params": params},
            interval=request.interval,
            start_d=start_d,
            end_d=end_d,
        )
        params = dict(hydrated["params"])
        params["_registry"] = TOOL_REGISTRY
    try:
        signals = TOOL_REGISTRY.run_signal(tool_name, rows, params)
    except ToolValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    limited = signals[: request.limit]
    return {
        "tool": tool_name,
        "universe": request.universe,
        "interval": request.interval,
        "start_date": start_d.isoformat(),
        "end_date": end_d.isoformat(),
        "symbols_requested": len(names),
        "bars_loaded": bars_before_filters,
        "bars_after_filters": len(rows),
        "filters_applied": filter_summaries,
        "signals_total": len(signals),
        "signals_returned": len(limited),
        "signals": limited,
    }


@app.post("/api/v1/exits/run")
def run_exit_tool(request: ExitRunRequest) -> dict[str, Any]:
    if request.universe != "stock":
        raise HTTPException(status_code=400, detail="Phase 2 exits currently support stock universe only.")

    position_rows = [position.model_dump() for position in request.positions]
    symbols = normalize_symbol_list([row["symbol"] for row in position_rows], max_count=500)

    today = datetime.now(timezone.utc).date()
    end_d = parse_date_or_default(request.end_date, today)
    if request.start_date:
        start_d = parse_date_or_default(request.start_date, end_d - timedelta(days=365 * 3))
    else:
        try:
            min_entry = min(datetime.strptime(str(row["entry_date"]), "%Y-%m-%d").date() for row in position_rows)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="positions.entry_date must be YYYY-MM-DD") from exc
        start_d = min_entry
    if start_d > end_d:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")

    try:
        rows = load_series_rows(symbols, request.universe, request.interval, start_d, end_d)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    tool_name = request.tool.strip().lower()
    params = dict(request.params)
    params.setdefault("interval", request.interval)
    params["_registry"] = TOOL_REGISTRY
    try:
        exits = TOOL_REGISTRY.run_exit(tool_name, position_rows, rows, params)
    except ToolValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    limited = exits[: request.limit]
    return {
        "tool": tool_name,
        "universe": request.universe,
        "interval": request.interval,
        "start_date": start_d.isoformat(),
        "end_date": end_d.isoformat(),
        "positions_requested": len(position_rows),
        "symbols_loaded": len(symbols),
        "bars_loaded": len(rows),
        "exits_total": len(exits),
        "exits_returned": len(limited),
        "exits": limited,
    }


def execute_lite_backtest_request(request: BacktestLiteRequest) -> dict[str, Any]:
    if request.universe != "stock":
        raise HTTPException(status_code=400, detail="run-lite currently supports stock universe only.")

    symbols = normalize_symbol_list(request.symbols, max_count=500)
    today = datetime.now(timezone.utc).date()
    end_d = parse_date_or_default(request.end_date, today)
    start_d = parse_date_or_default(request.start_date, end_d - timedelta(days=365 * 3))
    if start_d > end_d:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")

    try:
        rows = load_series_rows(symbols, request.universe, request.interval, start_d, end_d)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    benchmark_rows: list[dict[str, Any]] = []
    benchmark_name: str | None = None
    if request.benchmark:
        benchmark_name = request.benchmark.strip().upper()
        if not benchmark_name:
            raise HTTPException(status_code=400, detail="benchmark must be non-empty when provided")
        try:
            benchmark_rows = load_series_rows([benchmark_name], "index", request.interval, start_d, end_d)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=f"Failed loading benchmark index data: {exc}") from exc
        if not benchmark_rows:
            raise HTTPException(status_code=400, detail=f"No benchmark data found for benchmark={benchmark_name}")

    raw_filter_steps = [
        {"tool": step.tool.strip().lower(), "params": dict(step.params)}
        for step in request.filters
    ]
    raw_entry = {"tool": request.entry.tool.strip().lower(), "params": dict(request.entry.params)}
    raw_exit = {"tool": request.exit.tool.strip().lower(), "params": dict(request.exit.params)}
    if request.sizing is not None:
        raw_sizing = {"tool": request.sizing.tool.strip().lower(), "params": dict(request.sizing.params)}
    elif request.sizing_method == "equal_weight":
        raw_sizing = {"tool": "equal_weight", "params": {}}
    else:
        raw_sizing = {"tool": "fixed_amount", "params": {"amount": request.fixed_amount}}
    try:
        normalized_spec = validate_lite_spec(
            registry=TOOL_REGISTRY,
            interval=request.interval,
            filters=raw_filter_steps,
            entry=raw_entry,
            exit=raw_exit,
            sizing=raw_sizing,
        )
    except ToolValidationError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid strategy spec: {exc}") from exc

    filter_steps = list(normalized_spec["filters"])
    entry_tool = str(normalized_spec["entry"]["tool"])
    entry_params = dict(normalized_spec["entry"]["params"])
    hydrated_entry = _hydrate_signal_step_for_backtest(
        {"tool": entry_tool, "params": entry_params},
        interval=request.interval,
        start_d=start_d,
        end_d=end_d,
    )
    entry_tool = str(hydrated_entry["tool"])
    entry_params = dict(hydrated_entry["params"])
    exit_tool = str(normalized_spec["exit"]["tool"])
    exit_params = dict(normalized_spec["exit"]["params"])
    sizing_tool = str(normalized_spec["sizing"]["tool"])
    sizing_params = dict(normalized_spec["sizing"]["params"])

    try:
        result = run_lite_backtest(
            rows=rows,
            registry=TOOL_REGISTRY,
            filters=filter_steps,
            entry_tool=entry_tool,
            entry_params=entry_params,
            exit_tool=exit_tool,
            exit_params=exit_params,
            sizing_tool=sizing_tool,
            sizing_params=sizing_params,
            interval=request.interval,
            initial_capital=float(request.initial_capital),
            max_positions=int(request.max_positions),
            max_new_positions=int(request.max_new_positions),
            slippage_bps=float(request.slippage_bps),
            cost_pct=float(request.cost_pct),
            benchmark_rows=benchmark_rows,
        )
    except ToolValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return {
        "universe": request.universe,
        "interval": request.interval,
        "start_date": start_d.isoformat(),
        "end_date": end_d.isoformat(),
        "symbols_requested": len(symbols),
        "bars_loaded": len(rows),
        "benchmark": benchmark_name,
        "filters": filter_steps,
        "entry": {"tool": entry_tool, "params": entry_params},
        "exit": {"tool": exit_tool, "params": exit_params},
        "sizing": {"tool": sizing_tool, "params": sizing_params},
        **result,
    }


def _finalize_backtest_run(
    *,
    run_id: str,
    created_at: str,
    strategy_spec: dict[str, Any],
    lite_request: BacktestLiteRequest,
) -> None:
    try:
        result_payload = execute_lite_backtest_request(lite_request)
        trade_rows = build_trade_rows(run_id=run_id, trades=list(result_payload.get("trades", [])))
        equity_rows = build_equity_rows(run_id=run_id, equity_curve=list(result_payload.get("equity_curve", [])))
        if trade_rows:
            insert_trade_rows(
                ch=ch,
                database=settings.clickhouse_database,
                trades_table=settings.backtest_trades_table,
                rows=trade_rows,
            )
        if equity_rows:
            insert_equity_rows(
                ch=ch,
                database=settings.clickhouse_database,
                equity_table=settings.backtest_equity_table,
                rows=equity_rows,
            )
        completed_row = build_run_row(
            run_id=run_id,
            status="completed",
            spec=strategy_spec,
            result=result_payload,
            created_at=created_at,
        )
        insert_run_row(
            ch=ch,
            database=settings.clickhouse_database,
            runs_table=settings.backtest_runs_table,
            row=completed_row,
        )
    except HTTPException as exc:
        failed_row = build_run_row(
            run_id=run_id,
            status="failed",
            spec=strategy_spec,
            error_msg=str(exc.detail),
            created_at=created_at,
        )
        insert_run_row(
            ch=ch,
            database=settings.clickhouse_database,
            runs_table=settings.backtest_runs_table,
            row=failed_row,
        )
    except Exception as exc:  # noqa: BLE001
        failed_row = build_run_row(
            run_id=run_id,
            status="failed",
            spec=strategy_spec,
            error_msg=str(exc),
            created_at=created_at,
        )
        insert_run_row(
            ch=ch,
            database=settings.clickhouse_database,
            runs_table=settings.backtest_runs_table,
            row=failed_row,
        )


@app.post("/api/v1/backtest/run-lite")
def run_lite_backtest_endpoint(request: BacktestLiteRequest) -> dict[str, Any]:
    return execute_lite_backtest_request(request)


@app.post("/api/v1/backtest/validate-lite")
def validate_lite_backtest_endpoint(request: BacktestLiteRequest) -> dict[str, Any]:
    filter_steps = [{"tool": step.tool.strip().lower(), "params": dict(step.params)} for step in request.filters]
    entry = {"tool": request.entry.tool.strip().lower(), "params": dict(request.entry.params)}
    exit_ = {"tool": request.exit.tool.strip().lower(), "params": dict(request.exit.params)}
    if request.sizing is not None:
        sizing = {"tool": request.sizing.tool.strip().lower(), "params": dict(request.sizing.params)}
    elif request.sizing_method == "equal_weight":
        sizing = {"tool": "equal_weight", "params": {}}
    else:
        sizing = {"tool": "fixed_amount", "params": {"amount": request.fixed_amount}}
    try:
        normalized = validate_lite_spec(
            registry=TOOL_REGISTRY,
            interval=request.interval,
            filters=filter_steps,
            entry=entry,
            exit=exit_,
            sizing=sizing,
        )
    except ToolValidationError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid strategy spec: {exc}") from exc
    return {
        "status": "ok",
        "strategy_spec": normalized,
        "benchmark": request.benchmark.strip().upper() if request.benchmark else None,
    }


@app.post("/api/v1/backtest/validate")
def validate_backtest_endpoint(payload: dict[str, Any]) -> dict[str, Any]:
    strategy_spec, lite_request, spec_format = _resolve_backtest_run_payload(payload)
    return {
        "status": "ok",
        "spec_format": spec_format,
        "strategy_spec": strategy_spec,
        "lite_payload": lite_request.model_dump(),
    }


@app.post("/api/v1/backtest/run")
def run_backtest_endpoint(payload: dict[str, Any], background_tasks: BackgroundTasks) -> dict[str, Any]:
    ensure_backtest_storage_ready()
    strategy_spec, lite_request, spec_format = _resolve_backtest_run_payload(payload)
    run_id = str(uuid4())
    running_row = build_run_row(run_id=run_id, status="running", spec=strategy_spec)
    created_at = str(running_row["created_at"])
    insert_run_row(
        ch=ch,
        database=settings.clickhouse_database,
        runs_table=settings.backtest_runs_table,
        row=running_row,
    )
    background_tasks.add_task(
        _finalize_backtest_run,
        run_id=run_id,
        created_at=created_at,
        strategy_spec=strategy_spec,
        lite_request=lite_request,
    )
    return {
        "run_id": run_id,
        "status": "running",
        "spec_format": spec_format,
        "status_url": f"/api/v1/backtest/{run_id}/status",
        "result_url": f"/api/v1/backtest/{run_id}",
    }


@app.get("/api/v1/backtest/history")
def backtest_history(limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)) -> dict[str, Any]:
    ensure_backtest_storage_ready()
    db = validate_identifier(settings.clickhouse_database)
    runs_tbl = validate_identifier(settings.backtest_runs_table)
    query = f"""
SELECT
    run_id,
    argMax(created_at, updated_at) AS created_at,
    max(updated_at) AS latest_updated_at,
    argMax(status, updated_at) AS status,
    argMax(trade_count, updated_at) AS trade_count,
    argMax(total_return, updated_at) AS total_return,
    argMax(sharpe, updated_at) AS sharpe,
    argMax(max_drawdown, updated_at) AS max_drawdown,
    argMax(error_msg, updated_at) AS error_msg
FROM {db}.{runs_tbl}
GROUP BY run_id
ORDER BY created_at DESC
LIMIT {int(limit)}
OFFSET {int(offset)}
FORMAT JSONEachRow
""".strip()
    rows_raw = ch.query_rows(query)
    rows = [{**row, "updated_at": row.get("latest_updated_at")} for row in rows_raw]
    return {
        "limit": int(limit),
        "offset": int(offset),
        "count": len(rows),
        "runs": rows,
    }


@app.get("/api/v1/backtest/{run_id}/status")
def backtest_run_status(run_id: str) -> dict[str, Any]:
    ensure_backtest_storage_ready()
    rid = parse_run_id_or_400(run_id)
    db = validate_identifier(settings.clickhouse_database)
    runs_tbl = validate_identifier(settings.backtest_runs_table)
    query = f"""
SELECT
    run_id,
    argMax(created_at, updated_at) AS created_at,
    max(updated_at) AS latest_updated_at,
    argMax(status, updated_at) AS status,
    argMax(error_msg, updated_at) AS error_msg,
    argMax(trade_count, updated_at) AS trade_count
FROM {db}.{runs_tbl}
WHERE run_id = toUUID({sql_string(rid)})
GROUP BY run_id
FORMAT JSONEachRow
""".strip()
    rows = ch.query_rows(query)
    if not rows:
        raise HTTPException(status_code=404, detail=f"run_id not found: {rid}")
    row = rows[0]
    status = str(row.get("status", "")).strip().lower()
    return {
        "run_id": str(row["run_id"]),
        "status": status,
        "is_terminal": status in {"completed", "failed"},
        "created_at": str(row.get("created_at")),
        "updated_at": str(row.get("latest_updated_at")),
        "error_msg": str(row.get("error_msg", "")),
        "trade_count": int(row.get("trade_count", 0) or 0),
    }


@app.get("/api/v1/backtest/{run_id}")
def backtest_run_details(run_id: str) -> dict[str, Any]:
    ensure_backtest_storage_ready()
    rid = parse_run_id_or_400(run_id)
    db = validate_identifier(settings.clickhouse_database)
    runs_tbl = validate_identifier(settings.backtest_runs_table)
    query = f"""
SELECT
    run_id,
    argMax(created_at, updated_at) AS created_at,
    max(updated_at) AS latest_updated_at,
    argMax(status, updated_at) AS status,
    argMax(spec_json, updated_at) AS spec_json,
    argMax(metrics_json, updated_at) AS metrics_json,
    argMax(result_json, updated_at) AS result_json,
    argMax(error_msg, updated_at) AS error_msg,
    argMax(trade_count, updated_at) AS trade_count,
    argMax(total_return, updated_at) AS total_return,
    argMax(sharpe, updated_at) AS sharpe,
    argMax(max_drawdown, updated_at) AS max_drawdown
FROM {db}.{runs_tbl}
WHERE run_id = toUUID({sql_string(rid)})
GROUP BY run_id
FORMAT JSONEachRow
""".strip()
    rows = ch.query_rows(query)
    if not rows:
        raise HTTPException(status_code=404, detail=f"run_id not found: {rid}")
    row = rows[0]
    try:
        spec_json = json.loads(str(row.get("spec_json", "{}")))
    except json.JSONDecodeError:
        spec_json = {}
    try:
        metrics_json = json.loads(str(row.get("metrics_json", "{}")))
    except json.JSONDecodeError:
        metrics_json = {}
    try:
        result_json = json.loads(str(row.get("result_json", "{}")))
    except json.JSONDecodeError:
        result_json = {}
    return {
        "run_id": str(row["run_id"]),
        "status": row.get("status"),
        "created_at": str(row.get("created_at")),
        "updated_at": str(row.get("latest_updated_at")),
        "error_msg": str(row.get("error_msg", "")),
        "trade_count": int(row.get("trade_count", 0) or 0),
        "total_return": float(row.get("total_return", 0.0) or 0.0),
        "sharpe": float(row.get("sharpe", 0.0) or 0.0),
        "max_drawdown": float(row.get("max_drawdown", 0.0) or 0.0),
        "spec": spec_json,
        "metrics": metrics_json,
        "result": result_json,
    }


@app.get("/api/v1/backtest/{run_id}/trades")
def backtest_run_trades(run_id: str, limit: int = Query(500, ge=1, le=5000), offset: int = Query(0, ge=0)) -> dict[str, Any]:
    ensure_backtest_storage_ready()
    rid = parse_run_id_or_400(run_id)
    db = validate_identifier(settings.clickhouse_database)
    trades_tbl = validate_identifier(settings.backtest_trades_table)
    total_query = f"""
SELECT count() AS c
FROM {db}.{trades_tbl}
WHERE run_id = toUUID({sql_string(rid)})
FORMAT JSONEachRow
""".strip()
    total_rows = ch.query_rows(total_query)
    total = int(total_rows[0]["c"]) if total_rows else 0

    query = f"""
SELECT
    run_id,
    trade_index,
    symbol,
    entry_date,
    exit_date,
    entry_price,
    exit_price,
    shares,
    entry_cost,
    exit_cost,
    pnl,
    pnl_pct,
    exit_reason
FROM {db}.{trades_tbl}
WHERE run_id = toUUID({sql_string(rid)})
ORDER BY trade_index
LIMIT {int(limit)}
OFFSET {int(offset)}
FORMAT JSONEachRow
""".strip()
    rows = ch.query_rows(query)
    return {
        "run_id": rid,
        "total": total,
        "limit": int(limit),
        "offset": int(offset),
        "trades": rows,
    }


@app.get("/api/v1/backtest/{run_id}/equity-curve")
def backtest_run_equity(
    run_id: str,
    limit: int = Query(5000, ge=1, le=50000),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    ensure_backtest_storage_ready()
    rid = parse_run_id_or_400(run_id)
    db = validate_identifier(settings.clickhouse_database)
    equity_tbl = validate_identifier(settings.backtest_equity_table)
    total_query = f"""
SELECT count() AS c
FROM {db}.{equity_tbl}
WHERE run_id = toUUID({sql_string(rid)})
FORMAT JSONEachRow
""".strip()
    total_rows = ch.query_rows(total_query)
    total = int(total_rows[0]["c"]) if total_rows else 0

    query = f"""
SELECT
    run_id,
    point_index,
    date,
    cash,
    market_value,
    equity,
    open_positions
FROM {db}.{equity_tbl}
WHERE run_id = toUUID({sql_string(rid)})
ORDER BY point_index
LIMIT {int(limit)}
OFFSET {int(offset)}
FORMAT JSONEachRow
""".strip()
    rows = ch.query_rows(query)
    return {
        "run_id": rid,
        "total": total,
        "limit": int(limit),
        "offset": int(offset),
        "equity_curve": rows,
    }


@app.post("/api/v1/backtest/compare")
def backtest_compare(request: BacktestCompareRequest) -> dict[str, Any]:
    ensure_backtest_storage_ready()
    ids = [parse_run_id_or_400(item) for item in request.run_ids]
    db = validate_identifier(settings.clickhouse_database)
    runs_tbl = validate_identifier(settings.backtest_runs_table)
    run_id_sql = ", ".join(f"toUUID({sql_string(item)})" for item in ids)
    query = f"""
SELECT
    run_id,
    argMax(created_at, updated_at) AS created_at,
    max(updated_at) AS latest_updated_at,
    argMax(status, updated_at) AS status,
    argMax(metrics_json, updated_at) AS metrics_json,
    argMax(trade_count, updated_at) AS trade_count,
    argMax(total_return, updated_at) AS total_return,
    argMax(sharpe, updated_at) AS sharpe,
    argMax(max_drawdown, updated_at) AS max_drawdown
FROM {db}.{runs_tbl}
WHERE run_id IN ({run_id_sql})
GROUP BY run_id
ORDER BY created_at DESC
FORMAT JSONEachRow
""".strip()
    rows = ch.query_rows(query)
    found_ids = {str(row["run_id"]) for row in rows}
    missing = [rid for rid in ids if rid not in found_ids]
    out_rows: list[dict[str, Any]] = []
    for row in rows:
        try:
            metrics = json.loads(str(row.get("metrics_json", "{}")))
        except json.JSONDecodeError:
            metrics = {}
        out_rows.append(
            {
                "run_id": str(row["run_id"]),
                "status": row.get("status"),
                "created_at": str(row.get("created_at")),
                "trade_count": int(row.get("trade_count", 0) or 0),
                "total_return": float(row.get("total_return", 0.0) or 0.0),
                "sharpe": float(row.get("sharpe", 0.0) or 0.0),
                "max_drawdown": float(row.get("max_drawdown", 0.0) or 0.0),
                "metrics": metrics,
            }
        )
    return {"run_ids": ids, "missing_run_ids": missing, "runs": out_rows}


@app.get("/api/v1/search")
def search(q: str = Query(..., min_length=1), limit: int = Query(20, ge=1, le=100)) -> dict[str, Any]:
    db = validate_identifier(settings.clickhouse_database)
    ticker_tbl = table_name("ticker")
    index_tbl = table_name("index")
    query_raw = q.strip()
    if not query_raw:
        return {"query": q, "count": 0, "results": []}
    q_sql = sql_string(query_raw)
    q_norm = re.sub(r"[^A-Za-z0-9]", "", query_raw).lower()
    q_norm_sql = sql_string(q_norm)
    fetch_cap = max(100, limit * 5)

    stock_query = f"""
SELECT
    'stock' AS type,
    symbol AS code,
    company_name AS name,
    bse_code AS meta
FROM {db}.{ticker_tbl}
WHERE exchange = 'BSE'
  AND status = 'ACTIVE'
  AND (
      positionCaseInsensitiveUTF8(symbol, {q_sql}) > 0
      OR positionCaseInsensitiveUTF8(company_name, {q_sql}) > 0
      OR positionCaseInsensitiveUTF8(bse_code, {q_sql}) > 0
      OR (
          length({q_norm_sql}) > 0
          AND positionCaseInsensitiveUTF8(replaceRegexpAll(symbol, '[^A-Za-z0-9]', ''), {q_norm_sql}) > 0
      )
  )
ORDER BY symbol
LIMIT {fetch_cap}
FORMAT JSONEachRow
""".strip()
    index_query = f"""
SELECT
    'index' AS type,
    index_name AS code,
    index_name AS name,
    any(provider_ticker) AS meta
FROM {db}.{index_tbl}
GROUP BY index_name
HAVING
    positionCaseInsensitiveUTF8(index_name, {q_sql}) > 0
    OR positionCaseInsensitiveUTF8(any(provider_ticker), {q_sql}) > 0
    OR (
        length({q_norm_sql}) > 0
        AND positionCaseInsensitiveUTF8(replaceRegexpAll(index_name, '[^A-Za-z0-9]', ''), {q_norm_sql}) > 0
    )
ORDER BY index_name
LIMIT {fetch_cap}
FORMAT JSONEachRow
""".strip()

    try:
        stock_rows = ch.query_rows(stock_query)
        index_rows = ch.query_rows(index_query)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    query_lower = query_raw.lower()

    def norm(value: str) -> str:
        return re.sub(r"[^a-z0-9]", "", value.lower())

    def score_row(row: dict[str, Any]) -> int:
        code = str(row.get("code", ""))
        name = str(row.get("name", ""))
        code_lower = code.lower()
        name_lower = name.lower()
        code_norm = norm(code)
        name_norm = norm(name)

        score = 0
        if code_lower == query_lower:
            score += 120
        elif code_norm and code_norm == q_norm:
            score += 110
        elif code_lower.startswith(query_lower):
            score += 90
        elif code_norm and q_norm and code_norm.startswith(q_norm):
            score += 80
        elif query_lower in code_lower:
            score += 70
        elif q_norm and q_norm in code_norm:
            score += 65

        if name_lower.startswith(query_lower):
            score += 45
        elif query_lower in name_lower:
            score += 35
        elif q_norm and q_norm in name_norm:
            score += 30

        # Keep indexes competitive for index-like queries (e.g. NIFTY_50 / SENSEX).
        if row.get("type") == "index" and any(k in query_lower for k in ("nifty", "sensex", "index")):
            score += 20
        return score

    dedup: dict[tuple[str, str], dict[str, Any]] = {}
    for row in stock_rows + index_rows:
        key = (str(row.get("type", "")), str(row.get("code", "")))
        if key not in dedup:
            dedup[key] = row

    ranked = sorted(
        dedup.values(),
        key=lambda row: (-score_row(row), str(row.get("type", "")), str(row.get("code", ""))),
    )
    results = ranked[:limit]
    return {"query": q, "count": len(results), "results": results}


@app.get("/api/v1/indexes/snapshot")
def indexes_snapshot(on_date: str | None = None) -> dict[str, Any]:
    db = validate_identifier(settings.clickhouse_database)
    index_tbl = table_name("index")
    target_date = parse_date_or_default(on_date, datetime.now(timezone.utc).date())
    target_sql = sql_string(target_date.isoformat())
    query = f"""
SELECT index_name, date, close
FROM {db}.{index_tbl}
WHERE date <= {target_sql}
ORDER BY index_name, date DESC
LIMIT 2 BY index_name
FORMAT JSONEachRow
""".strip()
    try:
        rows = ch.query_rows(query)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["index_name"])].append(row)

    items: list[dict[str, Any]] = []
    for name in sorted(grouped.keys()):
        values = sorted(grouped[name], key=lambda r: str(r["date"]), reverse=True)
        latest = values[0]
        prev = values[1] if len(values) > 1 else None
        close = float(latest["close"])
        prev_close = float(prev["close"]) if prev is not None else None
        abs_change = close - prev_close if prev_close is not None else None
        pct_change = ((close / prev_close) - 1.0) * 100.0 if prev_close not in (None, 0.0) else None
        items.append(
            {
                "index_name": name,
                "date": str(latest["date"]),
                "close": close,
                "prev_close": prev_close,
                "abs_change": abs_change,
                "pct_change": pct_change,
            }
        )
    return {"as_of_date": target_date.isoformat(), "count": len(items), "items": items}


@app.get("/api/v1/series")
def series(
    symbols: str = Query(..., description="Comma-separated symbols or index names."),
    universe: Literal["stock", "index"] = Query("stock"),
    interval: Interval = Query("1w"),
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, Any]:
    names = split_symbols(symbols)
    today = datetime.now(timezone.utc).date()
    end_d = parse_date_or_default(end_date, today)
    default_start = end_d - timedelta(days=365)
    start_d = parse_date_or_default(start_date, default_start)
    if start_d > end_d:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")

    try:
        rows = load_series_rows(names, universe, interval, start_d, end_d)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["symbol"])].append(
            {
                "date": str(row["date"]),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": int(row["volume"]),
            }
        )
    result = [{"symbol": symbol, "points": grouped.get(symbol, [])} for symbol in names]
    return {
        "universe": universe,
        "interval": interval,
        "start_date": start_d.isoformat(),
        "end_date": end_d.isoformat(),
        "series": result,
    }


@app.get("/api/v1/ohlcv/{symbol}")
def ohlcv(
    symbol: str,
    universe: Literal["stock", "index"] = Query("stock"),
    interval: Interval = Query("1w"),
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, Any]:
    name = symbol.strip().upper()
    if not name:
        raise HTTPException(status_code=400, detail="symbol is required")

    today = datetime.now(timezone.utc).date()
    end_d = parse_date_or_default(end_date, today)
    start_d = parse_date_or_default(start_date, end_d - timedelta(days=365))
    if start_d > end_d:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")

    try:
        rows = load_series_rows([name], universe, interval, start_d, end_d)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    points = [
        {
            "date": str(row["date"]),
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": int(row["volume"]),
        }
        for row in rows
    ]
    return {
        "symbol": name,
        "universe": universe,
        "interval": interval,
        "start_date": start_d.isoformat(),
        "end_date": end_d.isoformat(),
        "points": points,
    }


@app.get("/api/v1/compare")
def compare(
    symbols: str = Query(..., description="Comma-separated symbols or index names."),
    universe: Literal["stock", "index"] = Query("stock"),
    interval: Interval = Query("1w"),
    normalized_base: float = Query(100.0, gt=0.0, le=1000000.0),
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, Any]:
    names = split_symbols(symbols, max_count=6)
    if len(names) < 2:
        raise HTTPException(status_code=400, detail="Provide at least two symbols/index names.")
    today = datetime.now(timezone.utc).date()
    end_d = parse_date_or_default(end_date, today)
    start_d = parse_date_or_default(start_date, end_d - timedelta(days=365))
    if start_d > end_d:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")

    try:
        rows = load_series_rows(names, universe, interval, start_d, end_d)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    grouped: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["symbol"])].append((str(row["date"]), float(row["close"])))

    series_out: list[dict[str, Any]] = []
    for symbol in names:
        points = sorted(grouped.get(symbol, []), key=lambda t: t[0])
        if not points:
            series_out.append({"symbol": symbol, "normalized": []})
            continue
        base = points[0][1]
        if base == 0:
            series_out.append({"symbol": symbol, "normalized": []})
            continue
        normalized = [{"date": d, "value": (v / base) * normalized_base} for d, v in points]
        period_return_pct = ((points[-1][1] / base) - 1.0) * 100.0
        series_out.append(
            {
                "symbol": symbol,
                "base_close": base,
                "normalized_base": normalized_base,
                "period_return_pct": period_return_pct,
                "normalized": normalized,
            }
        )
    return {
        "universe": universe,
        "interval": interval,
        "normalized_base": normalized_base,
        "start_date": start_d.isoformat(),
        "end_date": end_d.isoformat(),
        "series": series_out,
    }


@app.get("/api/v1/correlation")
def correlation(
    symbols: str = Query(..., description="Comma-separated symbols or index names."),
    universe: Literal["stock", "index"] = Query("stock"),
    interval: Interval = Query("1w"),
    window: int = Query(52, ge=10, le=1000),
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, Any]:
    names = split_symbols(symbols, max_count=6)
    if len(names) < 2:
        raise HTTPException(status_code=400, detail="Provide at least two symbols/index names.")

    today = datetime.now(timezone.utc).date()
    end_d = parse_date_or_default(end_date, today)
    days_per_bar = 7 if interval == "1w" else 31 if interval == "1mo" else 2
    default_days = max(365, window * days_per_bar)
    start_d = parse_date_or_default(start_date, end_d - timedelta(days=default_days))
    if start_d > end_d:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")

    try:
        rows = load_series_rows(names, universe, interval, start_d, end_d)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    closes: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for row in rows:
        closes[str(row["symbol"])].append((str(row["date"]), float(row["close"])))
    for symbol in closes:
        closes[symbol].sort(key=lambda t: t[0])

    returns_map: dict[str, dict[str, float]] = {}
    for symbol in names:
        points = closes.get(symbol, [])
        ret: dict[str, float] = {}
        for i in range(1, len(points)):
            prev_date, prev_close = points[i - 1]
            curr_date, curr_close = points[i]
            _ = prev_date
            if prev_close == 0:
                continue
            ret[curr_date] = (curr_close / prev_close) - 1.0
        if window > 0 and len(ret) > window:
            keep_dates = sorted(ret.keys())[-window:]
            ret = {k: ret[k] for k in keep_dates}
        returns_map[symbol] = ret

    matrix: list[dict[str, Any]] = []
    for a in names:
        for b in names:
            da = returns_map.get(a, {})
            db = returns_map.get(b, {})
            common_dates = sorted(set(da.keys()).intersection(db.keys()))
            x = [da[d] for d in common_dates]
            y = [db[d] for d in common_dates]
            value = corr(x, y)
            matrix.append(
                {
                    "symbol_a": a,
                    "symbol_b": b,
                    "correlation": value,
                    "observations": len(common_dates),
                }
            )

    return {
        "universe": universe,
        "interval": interval,
        "window": window,
        "start_date": start_d.isoformat(),
        "end_date": end_d.isoformat(),
        "matrix": matrix,
    }
