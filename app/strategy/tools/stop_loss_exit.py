from __future__ import annotations

from typing import Any

from app.strategy.tools.base import ToolSpec, ToolValidationError
from app.strategy.tools.exit_common import find_entry_index, group_rows_by_symbol, normalize_positions


def stop_loss_exit(
    positions: list[dict[str, Any]],
    universe_rows: list[dict[str, Any]],
    params: dict[str, Any],
) -> list[dict[str, Any]]:
    try:
        stop_loss_pct = float(params.get("stop_loss_pct", 10.0))
    except (TypeError, ValueError) as exc:
        raise ToolValidationError("stop_loss_pct must be numeric") from exc
    if stop_loss_pct <= 0:
        raise ToolValidationError("stop_loss_pct must be > 0")

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
        for idx in range(entry_idx, len(points)):
            dt, close = points[idx]
            pnl_pct = ((close / pos["entry_price"]) - 1.0) * 100.0
            if pnl_pct <= -stop_loss_pct:
                exits.append(
                    {
                        "position_id": pos["position_id"],
                        "symbol": symbol,
                        "entry_date": pos["entry_date"].isoformat(),
                        "entry_price": pos["entry_price"],
                        "exit_date": dt.isoformat(),
                        "exit_price": close,
                        "bars_held": idx - entry_idx,
                        "pnl_pct": pnl_pct,
                        "exit_signal": True,
                        "exit_reason": "stop_loss",
                        "stop_loss_pct": stop_loss_pct,
                    }
                )
                break
    exits.sort(key=lambda row: (str(row["exit_date"]), str(row["symbol"])), reverse=True)
    return exits


STOP_LOSS_EXIT_SPEC = ToolSpec(
    name="stop_loss",
    category="exit",
    description="Exit when position P&L percentage breaches stop loss threshold.",
    params={
        "stop_loss_pct": {
            "type": "number",
            "min": 0.0001,
            "required": True,
        }
    },
)
