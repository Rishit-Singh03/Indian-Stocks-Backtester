from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Any

from app.strategy.tools.base import ToolValidationError
from app.strategy.tools.helpers import to_date


def normalize_positions(positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for idx, raw in enumerate(positions):
        symbol = str(raw.get("symbol", "")).strip().upper()
        if not symbol:
            raise ToolValidationError(f"positions[{idx}].symbol is required")
        entry_date_raw = raw.get("entry_date")
        if entry_date_raw is None:
            raise ToolValidationError(f"positions[{idx}].entry_date is required")
        try:
            entry_date = to_date(entry_date_raw)
        except Exception as exc:
            raise ToolValidationError(f"positions[{idx}].entry_date is invalid: {entry_date_raw}") from exc
        try:
            entry_price = float(raw.get("entry_price"))
        except (TypeError, ValueError) as exc:
            raise ToolValidationError(f"positions[{idx}].entry_price must be numeric") from exc
        if entry_price <= 0:
            raise ToolValidationError(f"positions[{idx}].entry_price must be > 0")
        position_id = str(raw.get("position_id") or f"{symbol}:{entry_date.isoformat()}").strip()
        out.append(
            {
                "position_id": position_id,
                "symbol": symbol,
                "entry_date": entry_date,
                "entry_price": entry_price,
            }
        )
    return out


def group_rows_by_symbol(universe_rows: list[dict[str, Any]]) -> dict[str, list[tuple[date, float]]]:
    grouped: dict[str, list[tuple[date, float]]] = defaultdict(list)
    for row in universe_rows:
        symbol = str(row.get("symbol", "")).strip().upper()
        if not symbol:
            continue
        try:
            dt = to_date(row.get("date", ""))
            close = float(row.get("close"))
        except Exception:
            continue
        grouped[symbol].append((dt, close))
    for symbol in grouped:
        grouped[symbol].sort(key=lambda item: item[0])
    return grouped


def find_entry_index(points: list[tuple[date, float]], entry_date: date) -> int | None:
    for idx, (dt, _) in enumerate(points):
        if dt >= entry_date:
            return idx
    return None
