from __future__ import annotations

from typing import Any

from app.strategy.tools.base import ToolRegistry, ToolSpec, ToolValidationError


def combined_signal(
    universe_rows: list[dict[str, Any]],
    params: dict[str, Any],
) -> list[dict[str, Any]]:
    registry = params.get("_registry")
    if not isinstance(registry, ToolRegistry):
        raise ToolValidationError("combined_signal requires internal registry context")

    combine = str(params.get("combine", "AND")).strip().upper()
    if combine not in {"AND", "OR"}:
        raise ToolValidationError("combine must be AND or OR")

    signals_raw = params.get("signals")
    if not isinstance(signals_raw, list) or not signals_raw:
        raise ToolValidationError("signals must be a non-empty list")

    rank_by_raw = params.get("rank_by")
    rank_by = str(rank_by_raw).strip().lower() if rank_by_raw is not None else ""
    if rank_by == "":
        rank_by = ""

    interval = str(params.get("interval", "1w")).strip().lower()

    signal_results: list[tuple[str, dict[tuple[str, str], dict[str, Any]]]] = []
    signal_names: list[str] = []
    for idx, raw in enumerate(signals_raw):
        if not isinstance(raw, dict):
            raise ToolValidationError(f"signals[{idx}] must be object")
        tool = str(raw.get("tool", "")).strip().lower()
        if not tool:
            raise ToolValidationError(f"signals[{idx}].tool is required")
        if tool == "combined_signal":
            raise ToolValidationError("combined_signal cannot recursively reference itself")
        raw_params = raw.get("params", {})
        if raw_params is None:
            raw_params = {}
        if not isinstance(raw_params, dict):
            raise ToolValidationError(f"signals[{idx}].params must be object")
        tool_params = dict(raw_params)
        tool_params.setdefault("interval", interval)
        hits = registry.run_signal(tool, universe_rows, tool_params)
        hit_map: dict[tuple[str, str], dict[str, Any]] = {}
        for hit in hits:
            symbol = str(hit.get("symbol", "")).strip().upper()
            dt = str(hit.get("date", "")).strip()
            if not symbol or not dt:
                continue
            try:
                score = float(hit.get("score", 0.0))
            except (TypeError, ValueError):
                continue
            key = (symbol, dt)
            hit_map[key] = {**hit, "symbol": symbol, "date": dt, "score": score}
        signal_names.append(tool)
        signal_results.append((tool, hit_map))

    if rank_by and rank_by not in signal_names:
        raise ToolValidationError(f"rank_by must match one of: {', '.join(signal_names)}")

    if not signal_results:
        return []

    key_sets = [set(mapping.keys()) for _, mapping in signal_results]
    if combine == "AND":
        active_keys = set.intersection(*key_sets) if key_sets else set()
    else:
        active_keys = set.union(*key_sets) if key_sets else set()

    rows: list[dict[str, Any]] = []
    for key in active_keys:
        available: list[tuple[str, dict[str, Any]]] = []
        for tool, mapping in signal_results:
            hit = mapping.get(key)
            if hit is not None:
                available.append((tool, hit))
        if combine == "AND" and len(available) != len(signal_results):
            continue
        if not available:
            continue

        if rank_by:
            selected = next((hit for tool, hit in available if tool == rank_by), available[0][1])
            score = float(selected.get("score", 0.0))
        else:
            score = sum(float(hit.get("score", 0.0)) for _, hit in available) / len(available)
            selected = max(available, key=lambda item: float(item[1].get("score", 0.0)))[1]

        rows.append(
            {
                **selected,
                "score": score,
                "combine": combine,
                "matched_signals": [tool for tool, _ in available],
            }
        )

    rows.sort(key=lambda row: (str(row.get("date", "")), float(row.get("score", 0.0)), str(row.get("symbol", ""))), reverse=True)
    return rows


COMBINED_SIGNAL_SPEC = ToolSpec(
    name="combined_signal",
    category="signal",
    description="Combines multiple signal tools with AND/OR logic for entry decisions.",
    params={
        "combine": {
            "type": "string",
            "enum": ["AND", "OR"],
            "required": False,
            "default": "AND",
        },
        "signals": {
            "type": "array<object>",
            "required": True,
        },
        "rank_by": {
            "type": "string",
            "required": False,
        },
        "interval": {
            "type": "string",
            "enum": ["1d", "1w", "1mo"],
            "required": False,
            "default": "1w",
        },
    },
)
