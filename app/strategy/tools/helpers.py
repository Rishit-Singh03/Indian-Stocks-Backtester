from __future__ import annotations

from datetime import date
from typing import Any

from app.strategy.tools.base import ToolValidationError


def to_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def normalize_direction(value: Any) -> str:
    direction = str(value or "").strip().lower()
    if direction not in {"up", "down", "any"}:
        raise ToolValidationError("direction must be one of: up, down, any")
    return direction


def normalize_cross_direction(value: Any) -> str:
    direction = str(value or "").strip().lower()
    if direction not in {"above", "below"}:
        raise ToolValidationError("cross_direction must be one of: above, below")
    return direction


def normalize_reference(value: Any) -> str:
    reference = str(value or "").strip().lower()
    if reference not in {"high", "low"}:
        raise ToolValidationError("reference must be one of: high, low")
    return reference


def normalize_rank(value: Any) -> str:
    rank = str(value or "").strip().lower()
    if rank not in {"top", "bottom"}:
        raise ToolValidationError("rank must be one of: top, bottom")
    return rank


def lookback_bars(period: str, interval: str) -> int:
    period_key = period.strip().lower()
    interval_key = interval.strip().lower()
    mapping: dict[str, dict[str, int]] = {
        "1d": {"1w": 5, "2w": 10, "1m": 21, "3m": 63},
        "1w": {"1w": 1, "2w": 2, "1m": 4, "3m": 13},
        "1mo": {"1m": 1, "3m": 3},
    }
    interval_map = mapping.get(interval_key)
    if interval_map is None:
        raise ToolValidationError(f"Unsupported interval: {interval}")
    bars = interval_map.get(period_key)
    if bars is None:
        valid = ", ".join(sorted(interval_map.keys()))
        raise ToolValidationError(
            f"Unsupported period {period!r} for interval {interval!r}. "
            f"Valid periods: {valid}"
        )
    return bars


def weeks_to_bars(lookback_weeks: int, interval: str) -> int:
    if lookback_weeks <= 0:
        raise ToolValidationError("lookback_weeks must be > 0")
    interval_key = interval.strip().lower()
    if interval_key == "1d":
        return lookback_weeks * 5
    if interval_key == "1w":
        return lookback_weeks
    if interval_key == "1mo":
        return max(1, (lookback_weeks + 3) // 4)
    raise ToolValidationError(f"Unsupported interval: {interval}")
