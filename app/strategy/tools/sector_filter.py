from __future__ import annotations

from typing import Any

from app.strategy.tools.base import ToolSpec, ToolValidationError


def sector_filter(universe_rows: list[dict[str, Any]], params: dict[str, Any]) -> list[dict[str, Any]]:
    sectors_raw = params.get("sectors")
    if not isinstance(sectors_raw, list) or not sectors_raw:
        raise ToolValidationError("sectors must be a non-empty list")
    target_sectors = {str(item).strip().upper() for item in sectors_raw if str(item).strip()}
    if not target_sectors:
        raise ToolValidationError("sectors must contain at least one non-empty value")

    symbol_sector_map = params.get("symbol_sector_map")
    if not isinstance(symbol_sector_map, dict):
        raise ToolValidationError("symbol_sector_map object is required")

    include_unknown = bool(params.get("include_unknown", False))
    normalized_map: dict[str, str] = {}
    for symbol, sector in symbol_sector_map.items():
        sym = str(symbol).strip().upper()
        sec = str(sector).strip().upper()
        if sym and sec:
            normalized_map[sym] = sec

    out: list[dict[str, Any]] = []
    for row in universe_rows:
        symbol = str(row.get("symbol", "")).strip().upper()
        sector = normalized_map.get(symbol)
        if sector is None:
            if include_unknown:
                out.append(row)
            continue
        if sector in target_sectors:
            out.append(row)
    return out


SECTOR_FILTER_SPEC = ToolSpec(
    name="sector_filter",
    category="filter",
    description="Filters symbols by provided sector mapping and target sector list.",
    params={
        "sectors": {
            "type": "array<string>",
            "required": True,
        },
        "symbol_sector_map": {
            "type": "object<string, string>",
            "required": True,
        },
        "include_unknown": {
            "type": "boolean",
            "required": False,
            "default": False,
        },
    },
)
