from __future__ import annotations

from datetime import date
from typing import Any

from app.strategy.tools import ToolRegistry, ToolValidationError


def _type_ok(value: Any, expected: str) -> bool:
    t = expected.strip().lower()
    if t == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if t == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if t == "string":
        return isinstance(value, str)
    if t == "boolean":
        return isinstance(value, bool)
    if t == "object":
        return isinstance(value, dict)
    if t == "array<object>":
        return isinstance(value, list) and all(isinstance(item, dict) for item in value)
    if t == "array<string>":
        return isinstance(value, list) and all(isinstance(item, str) for item in value)
    if t == "object<string, string>":
        return isinstance(value, dict) and all(isinstance(k, str) and isinstance(v, str) for k, v in value.items())
    if t == "object<string, array<string>>":
        return isinstance(value, dict) and all(
            isinstance(k, str) and isinstance(v, list) and all(isinstance(item, str) for item in v)
            for k, v in value.items()
        )
    return True


def _validate_params(category: str, tool: str, params: dict[str, Any], registry: ToolRegistry) -> dict[str, Any]:
    spec = registry.get_tool_spec(category, tool)
    if spec is None:
        raise ToolValidationError(f"Unknown {category} tool: {tool}")
    schema = spec.params or {}
    out = dict(params)

    for key, meta_raw in schema.items():
        if not isinstance(meta_raw, dict):
            continue
        meta = meta_raw
        required = bool(meta.get("required", False))
        if required and key not in out:
            raise ToolValidationError(f"{category}.{tool}: missing required param '{key}'")
        if key not in out:
            default = meta.get("default")
            if default is not None:
                out[key] = default
            continue

        value = out[key]
        expected_type = str(meta.get("type", "")).strip().lower()
        if expected_type and not _type_ok(value, expected_type):
            raise ToolValidationError(
                f"{category}.{tool}: param '{key}' expected type {expected_type}, got {type(value).__name__}"
            )
        enum_values = meta.get("enum")
        if isinstance(enum_values, list) and enum_values:
            if value not in enum_values:
                allowed = ", ".join(str(v) for v in enum_values)
                raise ToolValidationError(f"{category}.{tool}: param '{key}' must be one of: {allowed}")
        min_v = meta.get("min")
        max_v = meta.get("max")
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if min_v is not None and float(value) < float(min_v):
                raise ToolValidationError(f"{category}.{tool}: param '{key}' must be >= {min_v}")
            if max_v is not None and float(value) > float(max_v):
                raise ToolValidationError(f"{category}.{tool}: param '{key}' must be <= {max_v}")

    unknown = [key for key in out.keys() if key not in schema]
    if unknown:
        bad = ", ".join(sorted(unknown))
        raise ToolValidationError(f"{category}.{tool}: unknown params: {bad}")
    return out


def validate_lite_spec(
    *,
    registry: ToolRegistry,
    interval: str,
    filters: list[dict[str, Any]],
    entry: dict[str, Any],
    exit: dict[str, Any],
    sizing: dict[str, Any],
) -> dict[str, Any]:
    normalized_filters: list[dict[str, Any]] = []
    for idx, step in enumerate(filters):
        tool = str(step.get("tool", "")).strip().lower()
        if not tool:
            raise ToolValidationError(f"filters[{idx}].tool is required")
        raw_params = step.get("params", {})
        if raw_params is None:
            raw_params = {}
        if not isinstance(raw_params, dict):
            raise ToolValidationError(f"filters[{idx}].params must be object")
        params = dict(raw_params)
        params = _validate_params("filter", tool, params, registry)
        normalized_filters.append({"tool": tool, "params": params})

    entry_tool = str(entry.get("tool", "")).strip().lower()
    if not entry_tool:
        raise ToolValidationError("entry.tool is required")
    entry_params_raw = entry.get("params", {})
    if entry_params_raw is None:
        entry_params_raw = {}
    if not isinstance(entry_params_raw, dict):
        raise ToolValidationError("entry.params must be object")
    entry_params = dict(entry_params_raw)
    entry_params = _validate_params("signal", entry_tool, entry_params, registry)

    exit_tool = str(exit.get("tool", "")).strip().lower()
    if not exit_tool:
        raise ToolValidationError("exit.tool is required")
    exit_params_raw = exit.get("params", {})
    if exit_params_raw is None:
        exit_params_raw = {}
    if not isinstance(exit_params_raw, dict):
        raise ToolValidationError("exit.params must be object")
    exit_params = dict(exit_params_raw)
    exit_params = _validate_params("exit", exit_tool, exit_params, registry)

    sizing_tool = str(sizing.get("tool", "")).strip().lower()
    if not sizing_tool:
        raise ToolValidationError("sizing.tool is required")
    sizing_params_raw = sizing.get("params", {})
    if sizing_params_raw is None:
        sizing_params_raw = {}
    if not isinstance(sizing_params_raw, dict):
        raise ToolValidationError("sizing.params must be object")
    sizing_params = dict(sizing_params_raw)
    sizing_params = _validate_params("sizing", sizing_tool, sizing_params, registry)

    return {
        "interval": interval,
        "filters": normalized_filters,
        "entry": {"tool": entry_tool, "params": entry_params},
        "exit": {"tool": exit_tool, "params": exit_params},
        "sizing": {"tool": sizing_tool, "params": sizing_params},
    }


def _normalize_step(
    *,
    registry: ToolRegistry,
    category: str,
    raw_step: Any,
    path: str,
) -> dict[str, Any]:
    if not isinstance(raw_step, dict):
        raise ToolValidationError(f"{path} must be object")
    tool = str(raw_step.get("tool", "")).strip().lower()
    if not tool:
        raise ToolValidationError(f"{path}.tool is required")
    params_raw = raw_step.get("params", {})
    if params_raw is None:
        params_raw = {}
    if not isinstance(params_raw, dict):
        raise ToolValidationError(f"{path}.params must be object")
    params = _validate_params(category, tool, dict(params_raw), registry)
    return {"tool": tool, "params": params}


def _normalize_steps(
    *,
    registry: ToolRegistry,
    category: str,
    raw_steps: Any,
    path: str,
    min_items: int = 0,
    max_items: int | None = None,
) -> list[dict[str, Any]]:
    if raw_steps is None:
        raw_steps = []
    if not isinstance(raw_steps, list):
        raise ToolValidationError(f"{path} must be an array")
    if len(raw_steps) < min_items:
        raise ToolValidationError(f"{path} must have at least {min_items} item(s)")
    if max_items is not None and len(raw_steps) > max_items:
        raise ToolValidationError(f"{path} must have at most {max_items} item(s)")
    out: list[dict[str, Any]] = []
    for idx, step in enumerate(raw_steps):
        out.append(
            _normalize_step(
                registry=registry,
                category=category,
                raw_step=step,
                path=f"{path}[{idx}]",
            )
        )
    return out


def _normalize_symbol_list(raw_symbols: Any, path: str) -> list[str]:
    if not isinstance(raw_symbols, list):
        raise ToolValidationError(f"{path} must be an array")
    out: list[str] = []
    seen: set[str] = set()
    for idx, item in enumerate(raw_symbols):
        symbol = str(item).strip().upper()
        if not symbol:
            raise ToolValidationError(f"{path}[{idx}] must be a non-empty string")
        if symbol in seen:
            continue
        seen.add(symbol)
        out.append(symbol)
    if not out:
        raise ToolValidationError(f"{path} must contain at least one symbol")
    return out


def _normalize_iso_date(value: Any, path: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        raise ToolValidationError(f"{path} is required")
    try:
        return date.fromisoformat(raw).isoformat()
    except ValueError as exc:
        raise ToolValidationError(f"{path} must be YYYY-MM-DD") from exc


def _normalize_positive_int(value: Any, path: str, *, minimum: int = 1, maximum: int | None = None) -> int:
    try:
        out = int(value)
    except (TypeError, ValueError) as exc:
        raise ToolValidationError(f"{path} must be an integer") from exc
    if out < minimum:
        raise ToolValidationError(f"{path} must be >= {minimum}")
    if maximum is not None and out > maximum:
        raise ToolValidationError(f"{path} must be <= {maximum}")
    return out


def _normalize_non_negative_float(value: Any, path: str) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError) as exc:
        raise ToolValidationError(f"{path} must be numeric") from exc
    if out < 0:
        raise ToolValidationError(f"{path} must be >= 0")
    return out


def validate_strategy_spec(
    *,
    registry: ToolRegistry,
    strategy_spec: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(strategy_spec, dict):
        raise ToolValidationError("strategy_spec must be an object")

    name = str(strategy_spec.get("name", "Untitled Strategy")).strip() or "Untitled Strategy"
    description = str(strategy_spec.get("description", "")).strip()

    universe_raw = strategy_spec.get("universe", {})
    if not isinstance(universe_raw, dict):
        raise ToolValidationError("universe must be an object")
    universe_type = str(universe_raw.get("type", "stock")).strip().lower()
    if universe_type not in {"stock", "index"}:
        raise ToolValidationError("universe.type must be one of: stock, index")
    symbols_raw = universe_raw.get("symbols", strategy_spec.get("symbols"))
    symbols = _normalize_symbol_list(symbols_raw, "universe.symbols")
    filters = _normalize_steps(
        registry=registry,
        category="filter",
        raw_steps=universe_raw.get("filters", []),
        path="universe.filters",
        min_items=0,
    )

    entry_raw = strategy_spec.get("entry")
    if not isinstance(entry_raw, dict):
        raise ToolValidationError("entry must be an object")
    signals = _normalize_steps(
        registry=registry,
        category="signal",
        raw_steps=entry_raw.get("signals"),
        path="entry.signals",
        min_items=1,
        max_items=5,
    )
    entry_combine = str(entry_raw.get("combine", "AND")).strip().upper()
    if entry_combine not in {"AND", "OR"}:
        raise ToolValidationError("entry.combine must be AND or OR")
    rank_by_raw = entry_raw.get("rank_by")
    rank_by = str(rank_by_raw).strip().lower() if rank_by_raw is not None else ""
    if rank_by == "":
        rank_by = ""
    if rank_by:
        valid_rank_tools = {step["tool"] for step in signals}
        if rank_by not in valid_rank_tools:
            raise ToolValidationError("entry.rank_by must match a tool in entry.signals")
    max_signals_per_period = _normalize_positive_int(
        entry_raw.get("max_signals_per_period", 10),
        "entry.max_signals_per_period",
        minimum=1,
        maximum=5000,
    )

    exit_raw = strategy_spec.get("exit")
    if not isinstance(exit_raw, dict):
        raise ToolValidationError("exit must be an object")
    conditions = _normalize_steps(
        registry=registry,
        category="exit",
        raw_steps=exit_raw.get("conditions"),
        path="exit.conditions",
        min_items=1,
        max_items=5,
    )
    exit_combine = str(exit_raw.get("combine", "FIRST_HIT")).strip().upper()
    if exit_combine not in {"FIRST_HIT", "ALL_REQUIRED"}:
        raise ToolValidationError("exit.combine must be FIRST_HIT or ALL_REQUIRED")

    sizing_raw = strategy_spec.get("sizing", {"tool": "fixed_amount", "params": {"amount": 10000}})
    sizing = _normalize_step(
        registry=registry,
        category="sizing",
        raw_step=sizing_raw,
        path="sizing",
    )

    execution_raw = strategy_spec.get("execution", {})
    if not isinstance(execution_raw, dict):
        raise ToolValidationError("execution must be an object")
    initial_capital = float(execution_raw.get("initial_capital", 1_000_000.0))
    if initial_capital <= 0:
        raise ToolValidationError("execution.initial_capital must be > 0")
    entry_timing = str(execution_raw.get("entry_timing", "next_open")).strip().lower()
    if entry_timing not in {"next_open", "same_close"}:
        raise ToolValidationError("execution.entry_timing must be next_open or same_close")
    rebalance = str(execution_raw.get("rebalance", "weekly")).strip().lower()
    if rebalance not in {"weekly", "monthly"}:
        raise ToolValidationError("execution.rebalance must be weekly or monthly")
    max_positions = _normalize_positive_int(
        execution_raw.get("max_positions", 20),
        "execution.max_positions",
        minimum=1,
        maximum=5000,
    )
    costs_raw = execution_raw.get("costs", {})
    if costs_raw is None:
        costs_raw = {}
    if not isinstance(costs_raw, dict):
        raise ToolValidationError("execution.costs must be an object")
    slippage_bps = _normalize_non_negative_float(costs_raw.get("slippage_bps", 0.0), "execution.costs.slippage_bps")
    round_trip_pct = _normalize_non_negative_float(
        costs_raw.get("round_trip_pct", costs_raw.get("cost_pct", 0.0)),
        "execution.costs.round_trip_pct",
    )

    benchmark_raw = strategy_spec.get("benchmark")
    benchmark = str(benchmark_raw).strip().upper() if benchmark_raw is not None else ""
    if benchmark == "":
        benchmark = None

    date_range_raw = strategy_spec.get("date_range")
    if not isinstance(date_range_raw, dict):
        raise ToolValidationError("date_range must be an object")
    start = _normalize_iso_date(date_range_raw.get("start"), "date_range.start")
    end = _normalize_iso_date(date_range_raw.get("end"), "date_range.end")
    if start > end:
        raise ToolValidationError("date_range.start must be <= date_range.end")

    return {
        "name": name,
        "description": description,
        "universe": {
            "type": universe_type,
            "symbols": symbols,
            "filters": filters,
        },
        "entry": {
            "signals": signals,
            "combine": entry_combine,
            "rank_by": rank_by or None,
            "max_signals_per_period": max_signals_per_period,
        },
        "exit": {
            "conditions": conditions,
            "combine": exit_combine,
        },
        "sizing": sizing,
        "execution": {
            "initial_capital": initial_capital,
            "entry_timing": entry_timing,
            "rebalance": rebalance,
            "max_positions": max_positions,
            "costs": {
                "slippage_bps": slippage_bps,
                "round_trip_pct": round_trip_pct,
            },
        },
        "benchmark": benchmark,
        "date_range": {
            "start": start,
            "end": end,
        },
    }


def strategy_spec_to_lite_payload(strategy_spec: dict[str, Any]) -> dict[str, Any]:
    universe = strategy_spec["universe"]
    entry = strategy_spec["entry"]
    exit_ = strategy_spec["exit"]
    execution = strategy_spec["execution"]
    costs = execution["costs"]
    date_range = strategy_spec["date_range"]

    universe_type = str(universe["type"]).strip().lower()
    if universe_type != "stock":
        raise ToolValidationError("Current backtest engine supports stock universe only")

    entry_timing = str(execution["entry_timing"]).strip().lower()
    if entry_timing != "next_open":
        raise ToolValidationError("Current backtest engine supports execution.entry_timing=next_open only")

    rebalance = str(execution["rebalance"]).strip().lower()
    interval = "1mo" if rebalance == "monthly" else "1w"

    entry_signals = list(entry["signals"])
    if len(entry_signals) == 1:
        lite_entry = dict(entry_signals[0])
    else:
        lite_entry_params: dict[str, Any] = {
            "combine": str(entry["combine"]),
            "signals": entry_signals,
        }
        rank_by = entry.get("rank_by")
        if rank_by:
            lite_entry_params["rank_by"] = str(rank_by)
        lite_entry = {"tool": "combined_signal", "params": lite_entry_params}

    exit_conditions = list(exit_["conditions"])
    if len(exit_conditions) == 1 and str(exit_["combine"]).upper() == "FIRST_HIT":
        lite_exit = dict(exit_conditions[0])
    else:
        lite_exit = {
            "tool": "combined_exit",
            "params": {
                "combine": str(exit_["combine"]),
                "conditions": exit_conditions,
            },
        }

    return {
        "universe": universe_type,
        "symbols": list(universe["symbols"]),
        "interval": interval,
        "start_date": str(date_range["start"]),
        "end_date": str(date_range["end"]),
        "filters": list(universe["filters"]),
        "entry": lite_entry,
        "exit": lite_exit,
        "sizing": dict(strategy_spec["sizing"]),
        "initial_capital": float(execution["initial_capital"]),
        "max_positions": int(execution["max_positions"]),
        "max_new_positions": int(entry["max_signals_per_period"]),
        "slippage_bps": float(costs["slippage_bps"]),
        "cost_pct": float(costs["round_trip_pct"]),
        "benchmark": strategy_spec.get("benchmark"),
    }


def lite_payload_to_strategy_spec(lite_payload: dict[str, Any]) -> dict[str, Any]:
    interval = str(lite_payload.get("interval", "1w")).strip().lower()
    rebalance = "monthly" if interval == "1mo" else "weekly"

    sizing = lite_payload.get("sizing")
    if not isinstance(sizing, dict):
        sizing_method = str(lite_payload.get("sizing_method", "fixed_amount")).strip().lower()
        if sizing_method == "equal_weight":
            sizing = {"tool": "equal_weight", "params": {}}
        else:
            sizing = {
                "tool": "fixed_amount",
                "params": {"amount": float(lite_payload.get("fixed_amount", 10_000.0))},
            }

    return {
        "name": "Lite Strategy",
        "description": "",
        "universe": {
            "type": str(lite_payload.get("universe", "stock")).strip().lower(),
            "symbols": list(lite_payload.get("symbols", [])),
            "filters": list(lite_payload.get("filters", [])),
        },
        "entry": {
            "signals": [dict(lite_payload.get("entry", {}))],
            "combine": "AND",
            "rank_by": None,
            "max_signals_per_period": int(lite_payload.get("max_new_positions", 10)),
        },
        "exit": {
            "conditions": [dict(lite_payload.get("exit", {}))],
            "combine": "FIRST_HIT",
        },
        "sizing": dict(sizing),
        "execution": {
            "initial_capital": float(lite_payload.get("initial_capital", 1_000_000.0)),
            "entry_timing": "next_open",
            "rebalance": rebalance,
            "max_positions": int(lite_payload.get("max_positions", 20)),
            "costs": {
                "slippage_bps": float(lite_payload.get("slippage_bps", 0.0)),
                "round_trip_pct": float(lite_payload.get("cost_pct", 0.0)),
            },
        },
        "benchmark": lite_payload.get("benchmark"),
        "date_range": {
            "start": str(lite_payload.get("start_date")),
            "end": str(lite_payload.get("end_date")),
        },
    }
