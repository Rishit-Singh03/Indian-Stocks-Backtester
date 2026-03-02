from __future__ import annotations

from datetime import date
from typing import Any

from app.strategy.tools.base import ToolSpec, ToolValidationError
from app.strategy.tools.helpers import normalize_cross_direction, to_date


def moving_average_crossover_signal(universe_rows: list[dict[str, Any]], params: dict[str, Any]) -> list[dict[str, Any]]:
    try:
        short_window = int(params.get("short_window", 4))
        long_window = int(params.get("long_window", 12))
    except (TypeError, ValueError) as exc:
        raise ToolValidationError("short_window and long_window must be integers") from exc
    if short_window <= 0 or long_window <= 0:
        raise ToolValidationError("short_window and long_window must be > 0")
    if short_window >= long_window:
        raise ToolValidationError("short_window must be < long_window")
    cross_direction = normalize_cross_direction(params.get("cross_direction", "above"))

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
    min_idx = long_window - 1
    prev_idx = min_idx - 1
    for symbol, points in grouped.items():
        points.sort(key=lambda item: item[0])
        closes = [p[1] for p in points]
        dates = [p[0] for p in points]
        if len(closes) < long_window + 1:
            continue

        for idx in range(min_idx, len(closes)):
            if idx - 1 < prev_idx:
                continue
            short_now = sum(closes[idx - short_window + 1 : idx + 1]) / short_window
            long_now = sum(closes[idx - long_window + 1 : idx + 1]) / long_window

            prev_close_idx = idx - 1
            short_prev = sum(closes[prev_close_idx - short_window + 1 : prev_close_idx + 1]) / short_window
            long_prev = sum(closes[prev_close_idx - long_window + 1 : prev_close_idx + 1]) / long_window

            crossed = False
            if cross_direction == "above":
                crossed = short_prev <= long_prev and short_now > long_now
            elif cross_direction == "below":
                crossed = short_prev >= long_prev and short_now < long_now
            if not crossed:
                continue

            distance_pct = ((short_now / long_now) - 1.0) * 100.0 if long_now != 0 else 0.0
            signals.append(
                {
                    "symbol": symbol,
                    "date": dates[idx].isoformat(),
                    "score": abs(distance_pct),
                    "cross_direction": cross_direction,
                    "short_window": short_window,
                    "long_window": long_window,
                    "short_ma": short_now,
                    "long_ma": long_now,
                    "prev_short_ma": short_prev,
                    "prev_long_ma": long_prev,
                    "close": closes[idx],
                    "distance_pct": distance_pct,
                }
            )
    signals.sort(key=lambda row: (str(row["date"]), float(row["score"]), str(row["symbol"])), reverse=True)
    return signals


MOVING_AVERAGE_CROSSOVER_SPEC = ToolSpec(
    name="moving_average_crossover",
    category="signal",
    description="Signals when short moving average crosses above/below long moving average.",
    params={
        "short_window": {
            "type": "integer",
            "min": 1,
            "required": True,
        },
        "long_window": {
            "type": "integer",
            "min": 2,
            "required": True,
        },
        "cross_direction": {
            "type": "string",
            "enum": ["above", "below"],
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
