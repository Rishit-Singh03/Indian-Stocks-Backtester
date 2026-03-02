from __future__ import annotations

from typing import Any

from app.strategy.tools.base import ToolSpec, ToolValidationError


def price_filter(universe_rows: list[dict[str, Any]], params: dict[str, Any]) -> list[dict[str, Any]]:
    min_price_raw = params.get("min_price")
    max_price_raw = params.get("max_price")

    min_price: float | None = None
    max_price: float | None = None
    if min_price_raw is not None:
        try:
            min_price = float(min_price_raw)
        except (TypeError, ValueError) as exc:
            raise ToolValidationError("min_price must be numeric") from exc
    if max_price_raw is not None:
        try:
            max_price = float(max_price_raw)
        except (TypeError, ValueError) as exc:
            raise ToolValidationError("max_price must be numeric") from exc

    if min_price is not None and min_price < 0:
        raise ToolValidationError("min_price must be >= 0")
    if max_price is not None and max_price < 0:
        raise ToolValidationError("max_price must be >= 0")
    if min_price is not None and max_price is not None and min_price > max_price:
        raise ToolValidationError("min_price must be <= max_price")

    out: list[dict[str, Any]] = []
    for row in universe_rows:
        try:
            close = float(row.get("close", 0.0))
        except (TypeError, ValueError):
            continue
        if min_price is not None and close < min_price:
            continue
        if max_price is not None and close > max_price:
            continue
        out.append(row)
    return out


PRICE_FILTER_SPEC = ToolSpec(
    name="price_filter",
    category="filter",
    description="Filters bars by close price range.",
    params={
        "min_price": {
            "type": "number",
            "min": 0.0,
            "required": False,
        },
        "max_price": {
            "type": "number",
            "min": 0.0,
            "required": False,
        },
    },
)
