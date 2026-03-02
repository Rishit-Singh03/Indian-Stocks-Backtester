from __future__ import annotations

from typing import Any

from app.strategy.tools.base import ToolSpec, ToolValidationError
from app.strategy.tools.exit_common import find_entry_index, group_rows_by_symbol, normalize_positions


def trailing_stop_exit(
    positions: list[dict[str, Any]],
    universe_rows: list[dict[str, Any]],
    params: dict[str, Any],
) -> list[dict[str, Any]]:
    try:
        trailing_stop_pct = float(params.get("trailing_stop_pct", 10.0))
    except (TypeError, ValueError) as exc:
        raise ToolValidationError("trailing_stop_pct must be numeric") from exc
    if trailing_stop_pct <= 0:
        raise ToolValidationError("trailing_stop_pct must be > 0")

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
        peak = pos["entry_price"]
        for idx in range(entry_idx, len(points)):
            dt, close = points[idx]
            peak = max(peak, close)
            if peak <= 0:
                continue
            drawdown_pct = ((peak - close) / peak) * 100.0
            if drawdown_pct >= trailing_stop_pct:
                pnl_pct = ((close / pos["entry_price"]) - 1.0) * 100.0
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
                        "exit_reason": "trailing_stop",
                        "trailing_stop_pct": trailing_stop_pct,
                        "peak_price": peak,
                        "drawdown_pct": drawdown_pct,
                    }
                )
                break
    exits.sort(key=lambda row: (str(row["exit_date"]), str(row["symbol"])), reverse=True)
    return exits


TRAILING_STOP_EXIT_SPEC = ToolSpec(
    name="trailing_stop",
    category="exit",
    description="Exit when price falls by trailing_stop_pct from post-entry peak.",
    params={
        "trailing_stop_pct": {
            "type": "number",
            "min": 0.0001,
            "required": True,
        }
    },
)
