from __future__ import annotations

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
