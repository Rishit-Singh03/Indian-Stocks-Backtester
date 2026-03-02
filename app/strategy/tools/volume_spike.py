from __future__ import annotations

from datetime import date
from typing import Any

from app.strategy.tools.base import ToolSpec, ToolValidationError
from app.strategy.tools.helpers import to_date


def volume_spike_signal(universe_rows: list[dict[str, Any]], params: dict[str, Any]) -> list[dict[str, Any]]:
    try:
        multiplier = float(params.get("multiplier", 2.0))
    except (TypeError, ValueError) as exc:
        raise ToolValidationError("multiplier must be numeric") from exc
    if multiplier <= 0:
        raise ToolValidationError("multiplier must be > 0")

    try:
        avg_period = int(params.get("avg_period", 20))
    except (TypeError, ValueError) as exc:
        raise ToolValidationError("avg_period must be integer") from exc
    if avg_period <= 0:
        raise ToolValidationError("avg_period must be > 0")

    grouped: dict[str, list[tuple[date, float, float]]] = {}
    for row in universe_rows:
        symbol = str(row.get("symbol", "")).strip().upper()
        if not symbol:
            continue
        try:
            dt = to_date(row.get("date", ""))
            volume = float(row.get("volume"))
            close = float(row.get("close"))
        except Exception:
            continue
        grouped.setdefault(symbol, []).append((dt, volume, close))

    signals: list[dict[str, Any]] = []
    for symbol, points in grouped.items():
        points.sort(key=lambda item: item[0])
        if len(points) <= avg_period:
            continue
        for idx in range(avg_period, len(points)):
            dt, current_volume, close = points[idx]
            window = points[idx - avg_period : idx]
            avg_volume = sum(item[1] for item in window) / avg_period
            if avg_volume <= 0:
                continue
            trigger_level = avg_volume * multiplier
            if current_volume <= trigger_level:
                continue
            ratio = current_volume / avg_volume
            signals.append(
                {
                    "symbol": symbol,
                    "date": dt.isoformat(),
                    "score": ratio / multiplier,
                    "current_volume": current_volume,
                    "avg_volume": avg_volume,
                    "ratio": ratio,
                    "multiplier": multiplier,
                    "avg_period": avg_period,
                    "close": close,
                }
            )

    signals.sort(key=lambda row: (str(row["date"]), float(row["score"]), str(row["symbol"])), reverse=True)
    return signals


VOLUME_SPIKE_SPEC = ToolSpec(
    name="volume_spike",
    category="signal",
    description="Signals when current volume exceeds multiplier times rolling average volume.",
    params={
        "multiplier": {
            "type": "number",
            "min": 0.0001,
            "required": True,
        },
        "avg_period": {
            "type": "integer",
            "min": 1,
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
