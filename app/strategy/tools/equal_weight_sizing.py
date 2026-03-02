from __future__ import annotations

from typing import Any

from app.strategy.tools.base import ToolSpec


def equal_weight_sizing(candidates: list[dict[str, Any]], cash: float, params: dict[str, Any]) -> list[dict[str, Any]]:
    _ = params
    if cash <= 0 or not candidates:
        return []
    allocation = cash / max(1, len(candidates))
    out: list[dict[str, Any]] = []
    for item in candidates:
        symbol = str(item.get("symbol", "")).strip().upper()
        if not symbol:
            continue
        out.append({**item, "symbol": symbol, "allocation": allocation})
    return out


EQUAL_WEIGHT_SIZING_SPEC = ToolSpec(
    name="equal_weight",
    category="sizing",
    description="Allocate equal capital across all candidates.",
    params={},
)
