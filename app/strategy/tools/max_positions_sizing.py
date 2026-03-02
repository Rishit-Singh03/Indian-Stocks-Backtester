from __future__ import annotations

from typing import Any

from app.strategy.tools.base import ToolSpec, ToolValidationError


def max_positions_sizing(candidates: list[dict[str, Any]], cash: float, params: dict[str, Any]) -> list[dict[str, Any]]:
    _ = cash
    try:
        limit = int(params.get("limit", 10))
    except (TypeError, ValueError) as exc:
        raise ToolValidationError("limit must be integer") from exc
    if limit <= 0:
        raise ToolValidationError("limit must be > 0")
    out: list[dict[str, Any]] = []
    for item in candidates:
        symbol = str(item.get("symbol", "")).strip().upper()
        if not symbol:
            continue
        out.append({**item, "symbol": symbol})
        if len(out) >= limit:
            break
    return out


MAX_POSITIONS_SIZING_SPEC = ToolSpec(
    name="max_positions",
    category="sizing",
    description="Limit candidate list to configured max positions.",
    params={
        "limit": {
            "type": "integer",
            "min": 1,
            "required": True,
        }
    },
)
