from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


SignalToolFn = Callable[[list[dict[str, Any]], dict[str, Any]], list[dict[str, Any]]]
FilterToolFn = Callable[[list[dict[str, Any]], dict[str, Any]], list[dict[str, Any]]]
ExitToolFn = Callable[
    [list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]],
    list[dict[str, Any]],
]
SizingToolFn = Callable[[list[dict[str, Any]], float, dict[str, Any]], list[dict[str, Any]]]


class ToolValidationError(ValueError):
    pass


@dataclass(frozen=True)
class ToolSpec:
    name: str
    category: str
    description: str
    params: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "category": self.category,
            "description": self.description,
            "params": self.params,
        }


@dataclass(frozen=True)
class _ToolDef:
    spec: ToolSpec
    fn: SignalToolFn


class ToolRegistry:
    def __init__(self) -> None:
        self._signals: dict[str, _ToolDef] = {}
        self._filters: dict[str, _ToolDef] = {}
        self._exits: dict[str, _ToolDef] = {}
        self._sizings: dict[str, _ToolDef] = {}

    def register_signal(self, spec: ToolSpec, fn: SignalToolFn) -> None:
        if spec.name in self._signals:
            raise ValueError(f"Signal tool already registered: {spec.name}")
        self._signals[spec.name] = _ToolDef(spec=spec, fn=fn)

    def register_filter(self, spec: ToolSpec, fn: FilterToolFn) -> None:
        if spec.name in self._filters:
            raise ValueError(f"Filter tool already registered: {spec.name}")
        self._filters[spec.name] = _ToolDef(spec=spec, fn=fn)

    def register_exit(self, spec: ToolSpec, fn: ExitToolFn) -> None:
        if spec.name in self._exits:
            raise ValueError(f"Exit tool already registered: {spec.name}")
        self._exits[spec.name] = _ToolDef(spec=spec, fn=fn)

    def register_sizing(self, spec: ToolSpec, fn: SizingToolFn) -> None:
        if spec.name in self._sizings:
            raise ValueError(f"Sizing tool already registered: {spec.name}")
        self._sizings[spec.name] = _ToolDef(spec=spec, fn=fn)

    def list_tools(self) -> list[dict[str, Any]]:
        out = [item.spec.to_dict() for item in self._signals.values()]
        out.extend(item.spec.to_dict() for item in self._filters.values())
        out.extend(item.spec.to_dict() for item in self._exits.values())
        out.extend(item.spec.to_dict() for item in self._sizings.values())
        return out

    def run_signal(self, name: str, universe_rows: list[dict[str, Any]], params: dict[str, Any]) -> list[dict[str, Any]]:
        tool = self._signals.get(name)
        if tool is None:
            raise ToolValidationError(f"Unknown signal tool: {name}")
        return tool.fn(universe_rows, params)

    def run_filter(self, name: str, universe_rows: list[dict[str, Any]], params: dict[str, Any]) -> list[dict[str, Any]]:
        tool = self._filters.get(name)
        if tool is None:
            raise ToolValidationError(f"Unknown filter tool: {name}")
        return tool.fn(universe_rows, params)

    def run_exit(
        self,
        name: str,
        positions: list[dict[str, Any]],
        universe_rows: list[dict[str, Any]],
        params: dict[str, Any],
    ) -> list[dict[str, Any]]:
        tool = self._exits.get(name)
        if tool is None:
            raise ToolValidationError(f"Unknown exit tool: {name}")
        return tool.fn(positions, universe_rows, params)

    def run_sizing(self, name: str, candidates: list[dict[str, Any]], cash: float, params: dict[str, Any]) -> list[dict[str, Any]]:
        tool = self._sizings.get(name)
        if tool is None:
            raise ToolValidationError(f"Unknown sizing tool: {name}")
        return tool.fn(candidates, cash, params)

    def get_tool_spec(self, category: str, name: str) -> ToolSpec | None:
        key = name.strip().lower()
        category_key = category.strip().lower()
        if category_key == "signal":
            tool = self._signals.get(key)
        elif category_key == "filter":
            tool = self._filters.get(key)
        elif category_key == "exit":
            tool = self._exits.get(key)
        elif category_key == "sizing":
            tool = self._sizings.get(key)
        else:
            return None
        return tool.spec if tool is not None else None
