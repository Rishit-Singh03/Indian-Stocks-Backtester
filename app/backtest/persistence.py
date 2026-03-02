from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import UUID

from app.clickhouse import ClickHouseClient, validate_identifier


def normalize_run_id(value: str) -> str:
    return str(UUID(str(value)))


def utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def ensure_backtest_tables(
    *,
    ch: ClickHouseClient,
    database: str,
    runs_table: str,
    trades_table: str,
    equity_table: str,
) -> None:
    db = validate_identifier(database)
    runs = validate_identifier(runs_table)
    trades = validate_identifier(trades_table)
    equity = validate_identifier(equity_table)

    ch.query_text(f"CREATE DATABASE IF NOT EXISTS {db}")

    ch.query_text(
        f"""
CREATE TABLE IF NOT EXISTS {db}.{runs}
(
    run_id UUID,
    created_at DateTime,
    updated_at DateTime,
    status LowCardinality(String),
    spec_json String,
    metrics_json String,
    result_json String,
    error_msg String,
    trade_count UInt32,
    total_return Float64,
    sharpe Float64,
    max_drawdown Float64
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (run_id)
""".strip()
    )

    ch.query_text(
        f"""
CREATE TABLE IF NOT EXISTS {db}.{trades}
(
    run_id UUID,
    trade_index UInt32,
    symbol String,
    entry_date Date,
    exit_date Date,
    entry_price Float64,
    exit_price Float64,
    shares Int64,
    entry_cost Float64,
    exit_cost Float64,
    pnl Float64,
    pnl_pct Float64,
    exit_reason String
)
ENGINE = MergeTree
ORDER BY (run_id, trade_index)
""".strip()
    )

    ch.query_text(
        f"""
CREATE TABLE IF NOT EXISTS {db}.{equity}
(
    run_id UUID,
    point_index UInt32,
    date Date,
    cash Float64,
    market_value Float64,
    equity Float64,
    open_positions UInt32
)
ENGINE = MergeTree
ORDER BY (run_id, point_index)
""".strip()
    )


def build_run_row(
    *,
    run_id: str,
    status: str,
    spec: dict,
    result: dict | None = None,
    error_msg: str = "",
    created_at: str | None = None,
) -> dict:
    rid = normalize_run_id(run_id)
    now = utc_now_str()
    created = created_at or now

    summary = (result or {}).get("summary", {})
    metrics_payload = {
        "returns": (result or {}).get("returns"),
        "risk": (result or {}).get("risk"),
        "ratios": (result or {}).get("ratios"),
        "trade_stats": (result or {}).get("trade_stats"),
        "benchmark_comparison": (result or {}).get("benchmark_comparison"),
    }

    return {
        "run_id": rid,
        "created_at": created,
        "updated_at": now,
        "status": status,
        "spec_json": json.dumps(spec, ensure_ascii=False),
        "metrics_json": json.dumps(metrics_payload, ensure_ascii=False),
        "result_json": json.dumps(result or {}, ensure_ascii=False),
        "error_msg": str(error_msg or ""),
        "trade_count": int(summary.get("trades", 0) or 0),
        "total_return": float(summary.get("total_return_pct", 0.0) or 0.0),
        "sharpe": float(summary.get("sharpe", 0.0) or 0.0),
        "max_drawdown": float(summary.get("max_drawdown_pct", 0.0) or 0.0),
    }


def build_trade_rows(*, run_id: str, trades: list[dict]) -> list[dict]:
    rid = normalize_run_id(run_id)
    rows: list[dict] = []
    for idx, trade in enumerate(trades):
        rows.append(
            {
                "run_id": rid,
                "trade_index": int(idx),
                "symbol": str(trade.get("symbol", "")).upper(),
                "entry_date": str(trade.get("entry_date")),
                "exit_date": str(trade.get("exit_date")),
                "entry_price": float(trade.get("entry_price", 0.0) or 0.0),
                "exit_price": float(trade.get("exit_price", 0.0) or 0.0),
                "shares": int(trade.get("shares", 0) or 0),
                "entry_cost": float(trade.get("entry_cost", 0.0) or 0.0),
                "exit_cost": float(trade.get("exit_cost", 0.0) or 0.0),
                "pnl": float(trade.get("pnl", 0.0) or 0.0),
                "pnl_pct": float(trade.get("pnl_pct", 0.0) or 0.0),
                "exit_reason": str(trade.get("exit_reason", "") or ""),
            }
        )
    return rows


def build_equity_rows(*, run_id: str, equity_curve: list[dict]) -> list[dict]:
    rid = normalize_run_id(run_id)
    rows: list[dict] = []
    for idx, point in enumerate(equity_curve):
        rows.append(
            {
                "run_id": rid,
                "point_index": int(idx),
                "date": str(point.get("date")),
                "cash": float(point.get("cash", 0.0) or 0.0),
                "market_value": float(point.get("market_value", 0.0) or 0.0),
                "equity": float(point.get("equity", 0.0) or 0.0),
                "open_positions": int(point.get("open_positions", 0) or 0),
            }
        )
    return rows


def insert_run_row(
    *,
    ch: ClickHouseClient,
    database: str,
    runs_table: str,
    row: dict,
) -> None:
    db = validate_identifier(database)
    runs = validate_identifier(runs_table)
    query = (
        f"INSERT INTO {db}.{runs} "
        "(run_id,created_at,updated_at,status,spec_json,metrics_json,result_json,error_msg,trade_count,total_return,sharpe,max_drawdown) "
        "FORMAT JSONEachRow"
    )
    ch.insert_json_each_row(query, [row])


def insert_trade_rows(
    *,
    ch: ClickHouseClient,
    database: str,
    trades_table: str,
    rows: list[dict],
) -> int:
    db = validate_identifier(database)
    trades = validate_identifier(trades_table)
    query = (
        f"INSERT INTO {db}.{trades} "
        "(run_id,trade_index,symbol,entry_date,exit_date,entry_price,exit_price,shares,entry_cost,exit_cost,pnl,pnl_pct,exit_reason) "
        "FORMAT JSONEachRow"
    )
    return ch.insert_json_each_row(query, rows)


def insert_equity_rows(
    *,
    ch: ClickHouseClient,
    database: str,
    equity_table: str,
    rows: list[dict],
) -> int:
    db = validate_identifier(database)
    equity = validate_identifier(equity_table)
    query = (
        f"INSERT INTO {db}.{equity} "
        "(run_id,point_index,date,cash,market_value,equity,open_positions) FORMAT JSONEachRow"
    )
    return ch.insert_json_each_row(query, rows)
