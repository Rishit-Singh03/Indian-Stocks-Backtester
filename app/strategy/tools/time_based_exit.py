from __future__ import annotations

from typing import Any

from app.strategy.tools.base import ToolSpec, ToolValidationError
from app.strategy.tools.exit_common import find_entry_index, group_rows_by_symbol, normalize_positions


def time_based_exit(
    positions: list[dict[str, Any]],
    universe_rows: list[dict[str, Any]],
    params: dict[str, Any],
) -> list[dict[str, Any]]:
    try:
        hold_periods = int(params.get("hold_periods", 4))
    except (TypeError, ValueError) as exc:
        raise ToolValidationError("hold_periods must be integer") from exc
    if hold_periods <= 0:
        raise ToolValidationError("hold_periods must be > 0")

    normalized_positions = normalize_positions(positions)
    grouped = group_rows_by_symbol(universe_rows)
    exits: list[dict[str, Any]] = []

    for pos in normalized_positions:
        symbol = str(pos["symbol"])
        points = grouped.get(symbol, [])
        if not points:
            continue
        entry_idx = find_entry_index(points, pos["entry_date"])
        if entry_idx is None:
            continue
        target_idx = entry_idx + hold_periods
        if target_idx >= len(points):
            continue
        dt, close = points[target_idx]
        pnl_pct = ((close / pos["entry_price"]) - 1.0) * 100.0
        exits.append(
            {
                "position_id": pos["position_id"],
                "symbol": symbol,
                "entry_date": pos["entry_date"].isoformat(),
                "entry_price": pos["entry_price"],
                "exit_date": dt.isoformat(),
                "exit_price": close,
                "bars_held": hold_periods,
                "pnl_pct": pnl_pct,
                "exit_signal": True,
                "exit_reason": "time_based_exit",
                "hold_periods": hold_periods,
            }
        )
    exits.sort(key=lambda row: (str(row["exit_date"]), str(row["symbol"])), reverse=True)
    return exits


TIME_BASED_EXIT_SPEC = ToolSpec(
    name="time_based_exit",
    category="exit",
    description="Exit after fixed number of bars from entry.",
    params={
        "hold_periods": {
            "type": "integer",
            "min": 1,
            "required": True,
        }
    },
)
