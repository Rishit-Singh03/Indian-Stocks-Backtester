from __future__ import annotations

from typing import Any

from app.strategy.tools.base import ToolSpec, ToolValidationError


def index_membership_filter(universe_rows: list[dict[str, Any]], params: dict[str, Any]) -> list[dict[str, Any]]:
    index_name = str(params.get("index_name", "")).strip().upper()
    if not index_name:
        raise ToolValidationError("index_name is required")

    membership_symbols_raw = params.get("membership_symbols")
    membership_map_raw = params.get("index_membership")

    allowed: set[str] = set()
    if isinstance(membership_symbols_raw, list):
        for item in membership_symbols_raw:
            symbol = str(item).strip().upper()
            if symbol:
                allowed.add(symbol)
    elif isinstance(membership_map_raw, dict):
        values = membership_map_raw.get(index_name)
        if not isinstance(values, list):
            values = membership_map_raw.get(index_name.lower())
        if not isinstance(values, list):
            raise ToolValidationError(
                "index_membership must contain a list for index_name, "
                "or provide membership_symbols directly"
            )
        for item in values:
            symbol = str(item).strip().upper()
            if symbol:
                allowed.add(symbol)
    else:
        raise ToolValidationError("Provide membership_symbols list or index_membership map in params")

    if not allowed:
        return []
    return [row for row in universe_rows if str(row.get("symbol", "")).strip().upper() in allowed]


INDEX_MEMBERSHIP_FILTER_SPEC = ToolSpec(
    name="index_membership_filter",
    category="filter",
    description="Filters to symbols in provided index membership set/map.",
    params={
        "index_name": {
            "type": "string",
            "required": True,
        },
        "membership_symbols": {
            "type": "array<string>",
            "required": False,
        },
        "index_membership": {
            "type": "object<string, array<string>>",
            "required": False,
        },
    },
)
