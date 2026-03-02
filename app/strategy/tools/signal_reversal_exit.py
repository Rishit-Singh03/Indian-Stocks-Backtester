from __future__ import annotations

from typing import Any

from app.strategy.tools.base import ToolRegistry, ToolSpec, ToolValidationError
from app.strategy.tools.exit_common import find_entry_index, group_rows_by_symbol, normalize_positions


def signal_reversal_exit(
    positions: list[dict[str, Any]],
    universe_rows: list[dict[str, Any]],
    params: dict[str, Any],
) -> list[dict[str, Any]]:
    registry = params.get("_registry")
    if not isinstance(registry, ToolRegistry):
        raise ToolValidationError("signal_reversal requires internal registry context")

    entry_tool = str(params.get("entry_tool", "")).strip().lower()
    if not entry_tool:
        raise ToolValidationError("entry_tool is required")
    entry_params_raw = params.get("entry_params", {})
    if entry_params_raw is None:
        entry_params_raw = {}
    if not isinstance(entry_params_raw, dict):
        raise ToolValidationError("entry_params must be object")
    entry_params = dict(entry_params_raw)
    entry_params.setdefault("interval", params.get("interval", "1w"))

    reversal_tool = str(params.get("reversal_tool", "")).strip().lower()
    reversal_params_raw = params.get("reversal_params", {})
    if reversal_params_raw is None:
        reversal_params_raw = {}
    if not isinstance(reversal_params_raw, dict):
        raise ToolValidationError("reversal_params must be object")
    reversal_params = dict(reversal_params_raw)
    reversal_params.setdefault("interval", params.get("interval", "1w"))

    entry_signals = registry.run_signal(entry_tool, universe_rows, entry_params)
    entry_set = {(str(row.get("symbol", "")).strip().upper(), str(row.get("date", "")).strip()) for row in entry_signals}

    reversal_set: set[tuple[str, str]] = set()
    use_reversal_tool = bool(reversal_tool)
    if use_reversal_tool:
        reversal_signals = registry.run_signal(reversal_tool, universe_rows, reversal_params)
        reversal_set = {
            (str(row.get("symbol", "")).strip().upper(), str(row.get("date", "")).strip())
            for row in reversal_signals
        }

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
            dt_iso = dt.isoformat()
            triggered = False
            reason = "signal_reversal"
            if use_reversal_tool:
                triggered = (symbol, dt_iso) in reversal_set
                reason = f"signal_reversal:{reversal_tool}"
            else:
                # Fallback rule: exit when original entry signal no longer true.
                triggered = (symbol, dt_iso) not in entry_set
            if not triggered:
                continue
            pnl_pct = ((close / pos["entry_price"]) - 1.0) * 100.0
            exits.append(
                {
                    "position_id": pos["position_id"],
                    "symbol": symbol,
                    "entry_date": pos["entry_date"].isoformat(),
                    "entry_price": pos["entry_price"],
                    "exit_date": dt_iso,
                    "exit_price": close,
                    "bars_held": idx - entry_idx,
                    "pnl_pct": pnl_pct,
                    "exit_signal": True,
                    "exit_reason": reason,
                    "entry_tool": entry_tool,
                    "reversal_tool": reversal_tool or None,
                }
            )
            break

    exits.sort(key=lambda row: (str(row["exit_date"]), str(row["symbol"])), reverse=True)
    return exits


SIGNAL_REVERSAL_EXIT_SPEC = ToolSpec(
    name="signal_reversal",
    category="exit",
    description="Exit when reversal signal appears, or when entry signal no longer holds.",
    params={
        "entry_tool": {
            "type": "string",
            "required": True,
        },
        "entry_params": {
            "type": "object",
            "required": False,
        },
        "reversal_tool": {
            "type": "string",
            "required": False,
        },
        "reversal_params": {
            "type": "object",
            "required": False,
        },
    },
)
