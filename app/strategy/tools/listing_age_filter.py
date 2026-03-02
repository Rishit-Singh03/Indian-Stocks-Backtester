from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Any

from app.strategy.tools.base import ToolSpec, ToolValidationError
from app.strategy.tools.helpers import to_date


def _bars_per_week(interval: str) -> float:
    key = interval.strip().lower()
    if key == "1d":
        return 5.0
    if key == "1w":
        return 1.0
    if key == "1mo":
        return 0.25
    raise ToolValidationError(f"Unsupported interval: {interval}")


def listing_age_filter(universe_rows: list[dict[str, Any]], params: dict[str, Any]) -> list[dict[str, Any]]:
    try:
        min_weeks = float(params.get("min_weeks", 0))
    except (TypeError, ValueError) as exc:
        raise ToolValidationError("min_weeks must be numeric") from exc
    if min_weeks < 0:
        raise ToolValidationError("min_weeks must be >= 0")

    interval = str(params.get("interval", "1w")).strip().lower()
    bars_per_week = _bars_per_week(interval)
    min_bars = int(min_weeks * bars_per_week)
    if min_bars <= 0:
        return universe_rows

    grouped: dict[str, list[tuple[date, dict[str, Any]]]] = defaultdict(list)
    for row in universe_rows:
        symbol = str(row.get("symbol", "")).strip().upper()
        if not symbol:
            continue
        try:
            dt = to_date(row.get("date", ""))
        except Exception:
            continue
        grouped[symbol].append((dt, row))

    out: list[dict[str, Any]] = []
    for _, points in grouped.items():
        points.sort(key=lambda item: item[0])
        for idx, (_, row) in enumerate(points):
            if idx >= min_bars:
                out.append(row)
    return out


LISTING_AGE_FILTER_SPEC = ToolSpec(
    name="listing_age_filter",
    category="filter",
    description="Filters bars until symbol has at least min_weeks of history.",
    params={
        "min_weeks": {
            "type": "number",
            "min": 0.0,
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
