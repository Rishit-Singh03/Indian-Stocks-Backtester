from __future__ import annotations

from collections import defaultdict
from typing import Any

from app.strategy.tools.base import ToolSpec, ToolValidationError


def liquidity_filter(universe_rows: list[dict[str, Any]], params: dict[str, Any]) -> list[dict[str, Any]]:
    try:
        min_avg_volume = float(params.get("min_avg_volume", 0.0))
        min_avg_turnover = float(params.get("min_avg_turnover", 0.0))
    except (TypeError, ValueError) as exc:
        raise ToolValidationError("min_avg_volume and min_avg_turnover must be numeric") from exc
    if min_avg_volume < 0 or min_avg_turnover < 0:
        raise ToolValidationError("min_avg_volume and min_avg_turnover must be >= 0")

    try:
        window_bars = int(params.get("window_bars", 20))
    except (TypeError, ValueError) as exc:
        raise ToolValidationError("window_bars must be integer") from exc
    if window_bars <= 0:
        raise ToolValidationError("window_bars must be > 0")

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in universe_rows:
        symbol = str(row.get("symbol", "")).strip().upper()
        if symbol:
            grouped[symbol].append(row)

    eligible: set[str] = set()
    for symbol, rows in grouped.items():
        ordered = sorted(rows, key=lambda item: str(item.get("date", "")))
        window = ordered[-window_bars:]
        volumes: list[float] = []
        turnovers: list[float] = []
        for row in window:
            try:
                vol = float(row.get("volume", 0.0))
                close = float(row.get("close", 0.0))
            except (TypeError, ValueError):
                continue
            volumes.append(vol)
            turnovers.append(vol * close)
        if not volumes:
            continue
        avg_volume = sum(volumes) / len(volumes)
        avg_turnover = sum(turnovers) / len(turnovers) if turnovers else 0.0
        if avg_volume >= min_avg_volume and avg_turnover >= min_avg_turnover:
            eligible.add(symbol)

    return [row for row in universe_rows if str(row.get("symbol", "")).strip().upper() in eligible]


LIQUIDITY_FILTER_SPEC = ToolSpec(
    name="liquidity_filter",
    category="filter",
    description="Filters symbols by rolling average volume/turnover thresholds.",
    params={
        "min_avg_volume": {
            "type": "number",
            "min": 0.0,
            "required": False,
            "default": 0.0,
        },
        "min_avg_turnover": {
            "type": "number",
            "min": 0.0,
            "required": False,
            "default": 0.0,
        },
        "window_bars": {
            "type": "integer",
            "min": 1,
            "required": False,
            "default": 20,
        },
    },
)
