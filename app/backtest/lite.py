from __future__ import annotations

from datetime import date
from typing import Any

from app.backtest.metrics import compute_backtest_metrics
from app.backtest.portfolio_state import PortfolioState
from app.strategy.tools import ToolRegistry, ToolValidationError


def _to_date(value: Any) -> date:
    return date.fromisoformat(str(value))


def _build_benchmark_equity_curve(
    rows: list[dict[str, Any]],
    *,
    initial_capital: float,
) -> tuple[list[dict[str, Any]], str | None]:
    if not rows:
        return [], None
    points: list[tuple[str, float]] = []
    benchmark_name: str | None = None
    for row in rows:
        try:
            dt = str(row.get("date"))
            _ = _to_date(dt)
            close = float(row.get("close"))
        except Exception:
            continue
        if close <= 0:
            continue
        points.append((dt, close))
        if benchmark_name is None:
            symbol = str(row.get("symbol", "")).strip().upper()
            if symbol:
                benchmark_name = symbol
    points.sort(key=lambda item: item[0])
    if not points:
        return [], benchmark_name
    base = points[0][1]
    if base <= 0:
        return [], benchmark_name
    curve = [{"date": dt, "equity": float(initial_capital) * (close / base)} for dt, close in points]
    return curve, benchmark_name


def run_lite_backtest(
    *,
    rows: list[dict[str, Any]],
    registry: ToolRegistry,
    filters: list[dict[str, Any]],
    entry_tool: str,
    entry_params: dict[str, Any],
    exit_tool: str,
    exit_params: dict[str, Any],
    sizing_tool: str,
    sizing_params: dict[str, Any],
    interval: str,
    initial_capital: float,
    max_positions: int,
    max_new_positions: int,
    slippage_bps: float,
    cost_pct: float,
    benchmark_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if initial_capital <= 0:
        raise ToolValidationError("initial_capital must be > 0")
    if max_positions <= 0:
        raise ToolValidationError("max_positions must be > 0")
    if max_new_positions <= 0:
        raise ToolValidationError("max_new_positions must be > 0")
    if slippage_bps < 0:
        raise ToolValidationError("slippage_bps must be >= 0")
    if cost_pct < 0:
        raise ToolValidationError("cost_pct must be >= 0")

    clean_rows: list[dict[str, Any]] = []
    for row in rows:
        try:
            symbol = str(row.get("symbol", "")).strip().upper()
            dt = str(row.get("date"))
            _ = _to_date(dt)
            clean_rows.append(
                {
                    "symbol": symbol,
                    "date": dt,
                    "open": float(row.get("open")),
                    "high": float(row.get("high")),
                    "low": float(row.get("low")),
                    "close": float(row.get("close")),
                    "volume": float(row.get("volume", 0.0)),
                }
            )
        except Exception:
            continue
    if not clean_rows:
        benchmark_curve, benchmark_name = _build_benchmark_equity_curve(
            benchmark_rows or [],
            initial_capital=float(initial_capital),
        )
        metrics = compute_backtest_metrics(
            equity_curve=[],
            trades=[],
            initial_capital=float(initial_capital),
            interval=interval,
            benchmark_equity_curve=benchmark_curve,
            benchmark_name=benchmark_name,
        )
        return {
            "equity_curve": [],
            "trades": [],
            "summary": {
                "initial_capital": initial_capital,
                "final_equity": initial_capital,
                "total_return_pct": 0.0,
                "bars": 0,
                "trades": 0,
                "win_rate_pct": None,
                "max_drawdown_pct": 0.0,
            },
            **metrics,
            "metrics_status": "empty_input",
            "liquidity_flags": [],
        }

    by_date: dict[str, list[dict[str, Any]]] = {}
    ohlc_map: dict[tuple[str, str], dict[str, Any]] = {}
    dates = sorted({str(row["date"]) for row in clean_rows})
    for row in clean_rows:
        dt = str(row["date"])
        by_date.setdefault(dt, []).append(row)
        ohlc_map[(str(row["symbol"]), dt)] = row
    next_date_map: dict[str, str | None] = {}
    for idx, dt in enumerate(dates):
        next_date_map[dt] = dates[idx + 1] if idx + 1 < len(dates) else None

    state = PortfolioState(initial_capital=float(initial_capital))
    pending_entries: dict[str, list[str]] = {}
    pending_exits: dict[str, dict[str, str]] = {}
    queued_entry_symbols: set[str] = set()
    queued_exit_ids: set[str] = set()
    history_rows: list[dict[str, Any]] = []
    last_seen_close: dict[str, float] = {}
    liquidity_flags: list[dict[str, Any]] = []

    slip = slippage_bps / 10000.0
    fee = cost_pct / 100.0
    bars_before_filters = 0
    bars_after_filters = 0

    def queue_entry(next_dt: str, symbol: str) -> None:
        pending_entries.setdefault(next_dt, []).append(symbol)
        queued_entry_symbols.add(symbol)

    def queue_exit(next_dt: str, position_id: str, reason: str = "strategy_exit") -> None:
        pending_exits.setdefault(next_dt, {})[position_id] = str(reason or "strategy_exit")
        queued_exit_ids.add(position_id)

    for dt in dates:
        today_rows = by_date.get(dt, [])
        history_rows.extend(today_rows)
        for row in today_rows:
            symbol = str(row["symbol"])
            last_seen_close[symbol] = float(row["close"])

        # Execute queued exits at current open.
        today_exits = dict(pending_exits.pop(dt, {}))
        for position_id, reason in today_exits.items():
            queued_exit_ids.discard(position_id)
            pos = state.open_positions.get(position_id)
            if pos is None:
                continue
            row = ohlc_map.get((pos.symbol, dt))
            if row is None:
                next_dt = next_date_map.get(dt)
                if next_dt is not None:
                    queue_exit(next_dt, position_id, reason)
                continue
            sell_price = float(row["open"]) * (1.0 - slip)
            state.close_position(
                position_id=position_id,
                trade_date=dt,
                sell_price=sell_price,
                fee_pct=fee,
                exit_reason=reason,
            )

        # Execute queued entries at current open.
        today_entries = pending_entries.pop(dt, [])
        if today_entries:
            unique_entries: list[str] = []
            seen: set[str] = set()
            for symbol in today_entries:
                if symbol in seen:
                    continue
                seen.add(symbol)
                queued_entry_symbols.discard(symbol)
                unique_entries.append(symbol)

            slots = max_positions - state.open_positions_count
            if slots > 0:
                candidates = [symbol for symbol in unique_entries if not state.has_symbol(symbol)]
                candidates = candidates[:slots]
                sizing_candidates: list[dict[str, Any]] = []
                for symbol in candidates:
                    row = ohlc_map.get((symbol, dt))
                    if row is None:
                        next_dt = next_date_map.get(dt)
                        if next_dt is not None:
                            queue_entry(next_dt, symbol)
                        continue
                    buy_price = float(row["open"]) * (1.0 + slip)
                    if buy_price <= 0:
                        continue
                    sizing_candidates.append({"symbol": symbol, "price": buy_price})

                if sizing_candidates:
                    sizing_tool_params = dict(sizing_params)
                    sizing_tool_params["_registry"] = registry
                    sizing_tool_params["interval"] = interval
                    sizing_tool_params["as_of_date"] = dt
                    sizing_tool_params["history_rows"] = history_rows
                    sized = registry.run_sizing(sizing_tool, sizing_candidates, state.cash, sizing_tool_params)
                else:
                    sized = []

                for allocation_item in sized:
                    symbol = str(allocation_item.get("symbol", "")).strip().upper()
                    buy_price = float(allocation_item.get("price", 0.0))
                    allocation = float(allocation_item.get("allocation", 0.0))
                    if not symbol or buy_price <= 0 or allocation <= 0:
                        continue
                    max_affordable_shares = int((state.cash / (1.0 + fee)) / buy_price)
                    desired_shares = int(allocation / buy_price)
                    shares = min(max_affordable_shares, desired_shares)
                    if shares <= 0:
                        continue
                    volume_row = ohlc_map.get((symbol, dt))
                    bar_volume = float(volume_row.get("volume", 0.0)) if volume_row is not None else 0.0
                    if bar_volume > 0:
                        participation_pct = (shares / bar_volume) * 100.0
                        if participation_pct > 10.0:
                            liquidity_flags.append(
                                {
                                    "date": dt,
                                    "symbol": symbol,
                                    "shares": shares,
                                    "bar_volume": bar_volume,
                                    "participation_pct": participation_pct,
                                    "warning": "position size exceeds 10% of bar volume",
                                }
                            )
                    state.open_position(
                        symbol=symbol,
                        trade_date=dt,
                        buy_price=buy_price,
                        shares=shares,
                        fee_pct=fee,
                    )

        # Mark equity at current close.
        state.record_equity(dt, ohlc_map, last_seen_close=last_seen_close)

        next_dt = next_date_map.get(dt)
        if next_dt is None:
            continue

        # Queue exits for next bar open based on current bar information.
        if state.open_positions_count > 0:
            exit_position_payload = state.build_exit_payload(excluded_ids=queued_exit_ids)
            if exit_position_payload:
                exit_tool_params = dict(exit_params)
                exit_tool_params.setdefault("interval", interval)
                exit_tool_params["_registry"] = registry
                exit_hits = registry.run_exit(exit_tool, exit_position_payload, history_rows, exit_tool_params)
                for hit in exit_hits:
                    position_id = str(hit.get("position_id", "")).strip()
                    exit_date = str(hit.get("exit_date", "")).strip()
                    exit_reason = str(hit.get("exit_reason", exit_tool)).strip() or exit_tool
                    if not position_id or position_id not in state.open_positions:
                        continue
                    if position_id in queued_exit_ids:
                        continue
                    if exit_date and exit_date <= dt:
                        queue_exit(next_dt, position_id, exit_reason)

        # Apply filters + entry signal on current history, queue entries for next bar open.
        filtered_rows = history_rows
        bars_before_filters += len(history_rows)
        for step in filters:
            filter_name = str(step.get("tool", "")).strip().lower()
            step_params = dict(step.get("params", {}))
            step_params.setdefault("interval", interval)
            filtered_rows = registry.run_filter(filter_name, filtered_rows, step_params)
        bars_after_filters += len(filtered_rows)

        entry_tool_params = dict(entry_params)
        entry_tool_params.setdefault("interval", interval)
        if entry_tool == "combined_signal":
            entry_tool_params["_registry"] = registry
        signals = registry.run_signal(entry_tool, filtered_rows, entry_tool_params)
        todays = [sig for sig in signals if str(sig.get("date", "")) == dt]
        todays.sort(key=lambda row: float(row.get("score", 0.0)), reverse=True)

        candidate_symbols: list[str] = []
        for sig in todays:
            symbol = str(sig.get("symbol", "")).strip().upper()
            if not symbol:
                continue
            if symbol in queued_entry_symbols:
                continue
            if state.has_symbol(symbol):
                continue
            candidate_symbols.append(symbol)
            if len(candidate_symbols) >= max_new_positions:
                break
        for symbol in candidate_symbols:
            queue_entry(next_dt, symbol)

    # Force-close remaining positions at the final available close so stale listings
    # do not survive beyond the backtest horizon.
    if dates and state.open_positions_count > 0:
        final_dt = dates[-1]
        for position_id, pos in list(state.open_positions.items()):
            fallback_close = last_seen_close.get(pos.symbol)
            if fallback_close is None or fallback_close <= 0:
                continue
            sell_price = float(fallback_close) * (1.0 - slip)
            state.close_position(
                position_id=position_id,
                trade_date=final_dt,
                sell_price=sell_price,
                fee_pct=fee,
                exit_reason="forced_last_price_end",
            )
        if state.equity_curve and str(state.equity_curve[-1].get("date", "")) == final_dt:
            state.equity_curve.pop()
        state.record_equity(final_dt, ohlc_map, last_seen_close=last_seen_close)

    summary = state.summary(
        bars_before_filters=bars_before_filters,
        bars_after_filters=bars_after_filters,
    )
    summary["liquidity_flag_count"] = len(liquidity_flags)
    benchmark_curve, benchmark_name = _build_benchmark_equity_curve(
        benchmark_rows or [],
        initial_capital=float(initial_capital),
    )
    metrics = compute_backtest_metrics(
        equity_curve=state.equity_curve,
        trades=state.trades,
        initial_capital=float(initial_capital),
        interval=interval,
        benchmark_equity_curve=benchmark_curve,
        benchmark_name=benchmark_name,
    )
    summary.update(
        {
            "cagr_pct": metrics["returns"]["cagr_pct"],
            "annualized_volatility_pct": metrics["risk"]["annualized_volatility_pct"],
            "downside_deviation_pct": metrics["risk"]["downside_deviation_pct"],
            "max_drawdown_duration_bars": metrics["risk"]["max_drawdown_duration_bars"],
            "sharpe": metrics["ratios"]["sharpe"],
            "sortino": metrics["ratios"]["sortino"],
            "calmar": metrics["ratios"]["calmar"],
            "profit_factor": metrics["trade_stats"]["profit_factor"],
            "avg_holding_periods": metrics["trade_stats"]["avg_holding_periods"],
        }
    )

    return {
        "equity_curve": state.equity_curve,
        "trades": state.trades,
        "summary": summary,
        **metrics,
        "liquidity_flags": liquidity_flags,
    }
