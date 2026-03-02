from __future__ import annotations

from datetime import date
from typing import Any

from app.strategy.tools.base import ToolSpec, ToolValidationError
from app.strategy.tools.helpers import lookback_bars, normalize_direction, to_date


def price_change_signal(universe_rows: list[dict[str, Any]], params: dict[str, Any]) -> list[dict[str, Any]]:
    period = str(params.get("period", "1w")).strip().lower()
    interval = str(params.get("interval", "1w")).strip().lower()
    direction = normalize_direction(params.get("direction", "any"))
    try:
        threshold_pct = float(params.get("threshold_pct", 10.0))
    except (TypeError, ValueError) as exc:
        raise ToolValidationError("threshold_pct must be a number") from exc
    if threshold_pct <= 0:
        raise ToolValidationError("threshold_pct must be > 0")

    lookback = lookback_bars(period, interval)
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
            pct_change = ((curr_close / prev_close) - 1.0) * 100.0
            if direction == "up" and pct_change < threshold_pct:
                continue
            if direction == "down" and pct_change > -threshold_pct:
                continue
            if direction == "any" and abs(pct_change) < threshold_pct:
                continue
            signals.append(
                {
                    "symbol": symbol,
                    "date": curr_date.isoformat(),
                    "score": abs(pct_change),
                    "pct_change": pct_change,
                    "lookback_bars": lookback,
                    "lookback_period": period,
                    "direction": direction,
                    "reference_date": prev_date.isoformat(),
                    "reference_close": prev_close,
                    "close": curr_close,
                }
            )
    signals.sort(key=lambda row: (str(row["date"]), float(row["score"]), str(row["symbol"])), reverse=True)
    return signals


PRICE_CHANGE_SPEC = ToolSpec(
    name="price_change",
    category="signal",
    description="Signals symbols that moved up/down by threshold over a lookback period.",
    params={
        "period": {
            "type": "string",
            "enum": ["1w", "2w", "1m", "3m"],
            "required": True,
        },
        "direction": {
            "type": "string",
            "enum": ["up", "down", "any"],
            "required": True,
        },
        "threshold_pct": {
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
