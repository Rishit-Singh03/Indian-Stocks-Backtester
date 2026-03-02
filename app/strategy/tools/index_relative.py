from __future__ import annotations

from datetime import date
from typing import Any

from app.strategy.tools.base import ToolSpec, ToolValidationError
from app.strategy.tools.helpers import lookback_bars, to_date


def _normalize_index_relative_direction(value: Any) -> str:
    direction = str(value or "").strip().lower()
    aliases = {
        "up": "outperform",
        "down": "underperform",
        "overperform": "outperform",
        "under_perform": "underperform",
    }
    direction = aliases.get(direction, direction)
    if direction not in {"outperform", "underperform", "any"}:
        raise ToolValidationError("direction must be one of: outperform, underperform, any")
    return direction


def index_relative_signal(universe_rows: list[dict[str, Any]], params: dict[str, Any]) -> list[dict[str, Any]]:
    period = str(params.get("period", "1m")).strip().lower()
    interval = str(params.get("interval", "1w")).strip().lower()
    direction = _normalize_index_relative_direction(params.get("direction", "outperform"))
    try:
        threshold_pct = float(params.get("threshold_pct", 5.0))
    except (TypeError, ValueError) as exc:
        raise ToolValidationError("threshold_pct must be numeric") from exc
    if threshold_pct <= 0:
        raise ToolValidationError("threshold_pct must be > 0")

    benchmark_rows = params.get("benchmark_rows")
    if not isinstance(benchmark_rows, list) or not benchmark_rows:
        raise ToolValidationError("benchmark_rows is required and must be a non-empty list")

    benchmark_name = str(params.get("index_name", "")).strip().upper()
    if not benchmark_name:
        raise ToolValidationError("index_name is required")

    lookback = lookback_bars(period, interval)

    benchmark_points: list[tuple[date, float]] = []
    for row in benchmark_rows:
        try:
            dt = to_date(row.get("date", ""))
            close = float(row.get("close"))
        except Exception:
            continue
        benchmark_points.append((dt, close))
    benchmark_points.sort(key=lambda item: item[0])
    if len(benchmark_points) <= lookback:
        raise ToolValidationError("Not enough benchmark data for requested period")

    benchmark_returns_by_date: dict[str, float] = {}
    for idx in range(lookback, len(benchmark_points)):
        prev_date, prev_close = benchmark_points[idx - lookback]
        curr_date, curr_close = benchmark_points[idx]
        if prev_close <= 0:
            continue
        _ = prev_date
        benchmark_returns_by_date[curr_date.isoformat()] = ((curr_close / prev_close) - 1.0) * 100.0

    grouped: dict[str, list[tuple[date, float]]] = {}
    for row in universe_rows:
        symbol = str(row.get("symbol", "")).strip().upper()
        if not symbol:
            continue
        try:
            dt = to_date(row.get("date", ""))
            close = float(row.get("close"))
        except Exception:
            continue
        grouped.setdefault(symbol, []).append((dt, close))

    signals: list[dict[str, Any]] = []
    for symbol, points in grouped.items():
        points.sort(key=lambda item: item[0])
        if len(points) <= lookback:
            continue
        for idx in range(lookback, len(points)):
            prev_date, prev_close = points[idx - lookback]
            curr_date, curr_close = points[idx]
            if prev_close <= 0:
                continue
            curr_date_iso = curr_date.isoformat()
            benchmark_ret_pct = benchmark_returns_by_date.get(curr_date_iso)
            if benchmark_ret_pct is None:
                continue
            stock_ret_pct = ((curr_close / prev_close) - 1.0) * 100.0
            relative_ret_pct = stock_ret_pct - benchmark_ret_pct

            if direction == "outperform" and relative_ret_pct < threshold_pct:
                continue
            if direction == "underperform" and relative_ret_pct > -threshold_pct:
                continue
            if direction == "any" and abs(relative_ret_pct) < threshold_pct:
                continue

            signals.append(
                {
                    "symbol": symbol,
                    "date": curr_date_iso,
                    "score": abs(relative_ret_pct),
                    "index_name": benchmark_name,
                    "lookback_period": period,
                    "lookback_bars": lookback,
                    "direction": direction,
                    "threshold_pct": threshold_pct,
                    "relative_return_pct": relative_ret_pct,
                    "stock_return_pct": stock_ret_pct,
                    "index_return_pct": benchmark_ret_pct,
                    "reference_date": prev_date.isoformat(),
                    "reference_close": prev_close,
                    "close": curr_close,
                }
            )
    signals.sort(key=lambda row: (str(row["date"]), float(row["score"]), str(row["symbol"])), reverse=True)
    return signals


INDEX_RELATIVE_SPEC = ToolSpec(
    name="index_relative",
    category="signal",
    description="Signals stocks outperforming/underperforming benchmark index by threshold over lookback period.",
    params={
        "index_name": {
            "type": "string",
            "required": True,
        },
        "period": {
            "type": "string",
            "enum": ["1w", "2w", "1m", "3m"],
            "required": True,
        },
        "threshold_pct": {
            "type": "number",
            "min": 0.0001,
            "required": True,
        },
        "direction": {
            "type": "string",
            "enum": ["outperform", "underperform", "any"],
            "required": True,
        },
        "interval": {
            "type": "string",
            "enum": ["1d", "1w", "1mo"],
            "required": False,
            "default": "1w",
        },
    },
)
