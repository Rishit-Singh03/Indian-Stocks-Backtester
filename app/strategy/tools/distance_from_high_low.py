from __future__ import annotations

from datetime import date
from typing import Any

from app.strategy.tools.base import ToolSpec, ToolValidationError
from app.strategy.tools.helpers import normalize_reference, to_date, weeks_to_bars


def distance_from_high_low_signal(universe_rows: list[dict[str, Any]], params: dict[str, Any]) -> list[dict[str, Any]]:
    reference = normalize_reference(params.get("reference", "high"))
    interval = str(params.get("interval", "1w")).strip().lower()
    try:
        lookback_weeks = int(params.get("lookback_weeks", 52))
        distance_pct = float(params.get("distance_pct", 20.0))
    except (TypeError, ValueError) as exc:
        raise ToolValidationError("lookback_weeks must be integer and distance_pct must be numeric") from exc
    if distance_pct <= 0:
        raise ToolValidationError("distance_pct must be > 0")
    lookback_bars = weeks_to_bars(lookback_weeks, interval)

    grouped: dict[str, list[tuple[date, float, float, float]]] = {}
    for row in universe_rows:
        symbol = str(row.get("symbol", "")).strip().upper()
        if not symbol:
            continue
        try:
            dt = to_date(row.get("date", ""))
            high = float(row.get("high"))
            low = float(row.get("low"))
            close = float(row.get("close"))
        except Exception:
            continue
        grouped.setdefault(symbol, []).append((dt, high, low, close))

    signals: list[dict[str, Any]] = []
    for symbol, points in grouped.items():
        points.sort(key=lambda item: item[0])
        if len(points) < lookback_bars:
            continue
        for idx in range(lookback_bars - 1, len(points)):
            window = points[idx - lookback_bars + 1 : idx + 1]
            dt, _, _, close = points[idx]
            ref_value = max(item[1] for item in window) if reference == "high" else min(item[2] for item in window)
            if ref_value <= 0:
                continue

            if reference == "high":
                dist = ((ref_value - close) / ref_value) * 100.0
                matched = dist >= distance_pct
                score = max(0.0, dist - distance_pct)
            else:
                dist = ((close - ref_value) / ref_value) * 100.0
                matched = dist <= distance_pct
                score = max(0.0, distance_pct - dist)
            if not matched:
                continue

            signals.append(
                {
                    "symbol": symbol,
                    "date": dt.isoformat(),
                    "score": score,
                    "reference": reference,
                    "lookback_weeks": lookback_weeks,
                    "lookback_bars": lookback_bars,
                    "distance_pct_threshold": distance_pct,
                    "distance_from_reference_pct": dist,
                    "reference_value": ref_value,
                    "close": close,
                }
            )

    signals.sort(key=lambda row: (str(row["date"]), float(row["score"]), str(row["symbol"])), reverse=True)
    return signals


DISTANCE_FROM_HIGH_LOW_SPEC = ToolSpec(
    name="distance_from_high_low",
    category="signal",
    description="Signals symbols that are X% below rolling high or within X% above rolling low.",
    params={
        "reference": {
            "type": "string",
            "enum": ["high", "low"],
            "required": True,
        },
        "lookback_weeks": {
            "type": "integer",
            "min": 1,
            "required": True,
        },
        "distance_pct": {
            "type": "number",
            "min": 0.0001,
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
