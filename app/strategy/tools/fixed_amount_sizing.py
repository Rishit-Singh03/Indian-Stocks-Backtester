from __future__ import annotations

from typing import Any

from app.strategy.tools.base import ToolSpec, ToolValidationError


def fixed_amount_sizing(candidates: list[dict[str, Any]], cash: float, params: dict[str, Any]) -> list[dict[str, Any]]:
    try:
        amount = float(params.get("amount", 10_000.0))
    except (TypeError, ValueError) as exc:
        raise ToolValidationError("amount must be numeric") from exc
    if amount <= 0:
        raise ToolValidationError("amount must be > 0")
    if cash <= 0:
        return []

    out: list[dict[str, Any]] = []
    for item in candidates:
        symbol = str(item.get("symbol", "")).strip().upper()
        if not symbol:
            continue
        out.append({**item, "symbol": symbol, "allocation": amount})
    return out


FIXED_AMOUNT_SIZING_SPEC = ToolSpec(
    name="fixed_amount",
    category="sizing",
    description="Allocate fixed currency amount per candidate.",
    params={
        "amount": {
            "type": "number",
            "min": 0.0001,
            "required": True,
        }
    },
)
