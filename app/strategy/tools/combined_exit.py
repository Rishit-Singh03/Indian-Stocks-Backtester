from __future__ import annotations

from datetime import date
from typing import Any

from app.strategy.tools.base import ToolRegistry, ToolSpec, ToolValidationError
from app.strategy.tools.exit_common import normalize_positions


def _to_date(value: str) -> date:
    return date.fromisoformat(value)


def combined_exit(
    positions: list[dict[str, Any]],
    universe_rows: list[dict[str, Any]],
    params: dict[str, Any],
) -> list[dict[str, Any]]:
    registry = params.get("_registry")
    if not isinstance(registry, ToolRegistry):
        raise ToolValidationError("combined_exit requires internal registry context")

    combine = str(params.get("combine", "FIRST_HIT")).strip().upper()
    if combine not in {"FIRST_HIT", "ALL_REQUIRED"}:
        raise ToolValidationError("combine must be FIRST_HIT or ALL_REQUIRED")

    conditions_raw = params.get("conditions")
    if not isinstance(conditions_raw, list) or not conditions_raw:
        raise ToolValidationError("conditions must be a non-empty list")

    condition_hits: list[dict[str, dict[str, Any]]] = []
    for idx, raw in enumerate(conditions_raw):
        if not isinstance(raw, dict):
            raise ToolValidationError(f"conditions[{idx}] must be object")
        tool = str(raw.get("tool", "")).strip().lower()
        if not tool:
            raise ToolValidationError(f"conditions[{idx}].tool is required")
        cond_params_raw = raw.get("params", {})
        if cond_params_raw is None:
            cond_params_raw = {}
        if not isinstance(cond_params_raw, dict):
            raise ToolValidationError(f"conditions[{idx}].params must be object")
        cond_params = dict(cond_params_raw)
        cond_params.setdefault("interval", params.get("interval", "1w"))
        cond_params["_registry"] = registry
        exits = registry.run_exit(tool, positions, universe_rows, cond_params)
        mapping: dict[str, dict[str, Any]] = {}
        for hit in exits:
            pid = str(hit.get("position_id", "")).strip()
            if not pid:
                continue
            mapping[pid] = {**hit, "_condition_tool": tool}
        condition_hits.append(mapping)

    normalized_positions = normalize_positions(positions)
    positions_by_id = {str(pos["position_id"]): pos for pos in normalized_positions}
    out: list[dict[str, Any]] = []
    for position_id, pos in positions_by_id.items():
        hits = [mapping.get(position_id) for mapping in condition_hits]
        if combine == "ALL_REQUIRED":
            if any(hit is None for hit in hits):
                continue
            concrete_hits = [hit for hit in hits if hit is not None]
            chosen = max(concrete_hits, key=lambda item: _to_date(str(item.get("exit_date"))))
            out.append(
                {
                    **chosen,
                    "exit_reason": "combined_exit:ALL_REQUIRED",
                    "conditions_triggered": [str(hit.get("_condition_tool")) for hit in concrete_hits],
                }
            )
        else:
            concrete_hits = [hit for hit in hits if hit is not None]
            if not concrete_hits:
                continue
            chosen = min(concrete_hits, key=lambda item: _to_date(str(item.get("exit_date"))))
            out.append(
                {
                    **chosen,
                    "exit_reason": "combined_exit:FIRST_HIT",
                    "conditions_triggered": [str(hit.get("_condition_tool")) for hit in concrete_hits],
                }
            )

    out.sort(key=lambda row: (str(row.get("exit_date", "")), str(row.get("symbol", ""))), reverse=True)
    return out


COMBINED_EXIT_SPEC = ToolSpec(
    name="combined_exit",
    category="exit",
    description="Combines multiple exit conditions with FIRST_HIT or ALL_REQUIRED logic.",
    params={
        "combine": {
            "type": "string",
            "enum": ["FIRST_HIT", "ALL_REQUIRED"],
            "required": False,
            "default": "FIRST_HIT",
        },
        "conditions": {
            "type": "array<object>",
            "required": True,
        },
    },
)
