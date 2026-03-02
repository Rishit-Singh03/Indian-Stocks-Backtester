from __future__ import annotations

from datetime import date
from typing import Any

from app.strategy.tools.base import ToolSpec, ToolValidationError
from app.strategy.tools.helpers import normalize_direction, to_date


def consecutive_moves_signal(universe_rows: list[dict[str, Any]], params: dict[str, Any]) -> list[dict[str, Any]]:
    direction = normalize_direction(params.get("direction", "up"))
    if direction == "any":
        raise ToolValidationError("direction for consecutive_moves must be up or down")
    try:
        count = int(params.get("count", 3))
    except (TypeError, ValueError) as exc:
        raise ToolValidationError("count must be integer") from exc
    if count <= 0:
        raise ToolValidationError("count must be > 0")

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
        if len(points) <= 1:
            continue

        up_streak = 0
        down_streak = 0
        streak_start_idx = 0
        for idx in range(1, len(points)):
            prev_date, prev_close = points[idx - 1]
            curr_date, curr_close = points[idx]
            if prev_close <= 0:
                up_streak = 0
                down_streak = 0
                streak_start_idx = idx
                continue
            ret_pct = ((curr_close / prev_close) - 1.0) * 100.0
            if ret_pct > 0:
                up_streak += 1
                down_streak = 0
                if up_streak == 1:
                    streak_start_idx = idx - 1
            elif ret_pct < 0:
                down_streak += 1
                up_streak = 0
                if down_streak == 1:
                    streak_start_idx = idx - 1
            else:
                up_streak = 0
                down_streak = 0
                streak_start_idx = idx
                continue

            streak = up_streak if direction == "up" else down_streak
            if streak < count:
                continue
            start_date, start_close = points[streak_start_idx]
            cumulative_pct = ((curr_close / start_close) - 1.0) * 100.0 if start_close > 0 else 0.0
            signals.append(
                {
                    "symbol": symbol,
                    "date": curr_date.isoformat(),
                    "score": float(streak),
                    "direction": direction,
                    "count": count,
                    "streak": streak,
                    "streak_start_date": start_date.isoformat(),
                    "streak_start_close": start_close,
                    "close": curr_close,
                    "last_bar_return_pct": ret_pct,
                    "cumulative_streak_return_pct": cumulative_pct,
                    "reference_date": prev_date.isoformat(),
                    "reference_close": prev_close,
                }
            )

    signals.sort(key=lambda row: (str(row["date"]), float(row["score"]), str(row["symbol"])), reverse=True)
    return signals


CONSECUTIVE_MOVES_SPEC = ToolSpec(
    name="consecutive_moves",
    category="signal",
    description="Signals symbols with N consecutive up or down bars.",
    params={
        "direction": {
            "type": "string",
            "enum": ["up", "down"],
            "required": True,
        },
        "count": {
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
