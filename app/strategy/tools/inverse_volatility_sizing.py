from __future__ import annotations

from collections import defaultdict
from math import sqrt
from typing import Any

from app.strategy.tools.base import ToolSpec, ToolValidationError


def inverse_volatility_sizing(candidates: list[dict[str, Any]], cash: float, params: dict[str, Any]) -> list[dict[str, Any]]:
    if cash <= 0 or not candidates:
        return []
    try:
        lookback_bars = int(params.get("lookback_bars", 20))
    except (TypeError, ValueError) as exc:
        raise ToolValidationError("lookback_bars must be integer") from exc
    if lookback_bars < 2:
        raise ToolValidationError("lookback_bars must be >= 2")

    history_rows = params.get("history_rows")
    as_of_date = str(params.get("as_of_date", "")).strip()
    if not isinstance(history_rows, list) or not as_of_date:
        raise ToolValidationError("inverse_volatility requires history_rows list and as_of_date")

    grouped: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for row in history_rows:
        symbol = str(row.get("symbol", "")).strip().upper()
        dt = str(row.get("date", "")).strip()
        if not symbol or not dt:
            continue
        try:
            close = float(row.get("close"))
        except (TypeError, ValueError):
            continue
        grouped[symbol].append((dt, close))
    for symbol in grouped:
        grouped[symbol].sort(key=lambda item: item[0])

    candidate_symbols = [str(item.get("symbol", "")).strip().upper() for item in candidates if str(item.get("symbol", "")).strip()]
    inverse_weights: dict[str, float] = {}
    for symbol in candidate_symbols:
        points = grouped.get(symbol, [])
        points = [p for p in points if p[0] <= as_of_date]
        if len(points) < lookback_bars + 1:
            continue
        window = points[-(lookback_bars + 1) :]
        returns: list[float] = []
        for idx in range(1, len(window)):
            prev = window[idx - 1][1]
            curr = window[idx][1]
            if prev <= 0:
                continue
            returns.append((curr / prev) - 1.0)
        if len(returns) < 2:
            continue
        mean_ret = sum(returns) / len(returns)
        variance = sum((r - mean_ret) ** 2 for r in returns) / len(returns)
        vol = sqrt(variance)
        if vol <= 0:
            continue
        inverse_weights[symbol] = 1.0 / vol

    if not inverse_weights:
        equal = cash / max(1, len(candidate_symbols))
        return [{**item, "allocation": equal} for item in candidates if str(item.get("symbol", "")).strip()]

    total_inv = sum(inverse_weights.values())
    out: list[dict[str, Any]] = []
    for item in candidates:
        symbol = str(item.get("symbol", "")).strip().upper()
        if not symbol:
            continue
        w = inverse_weights.get(symbol)
        if w is None:
            continue
        allocation = cash * (w / total_inv)
        out.append({**item, "symbol": symbol, "allocation": allocation})
    return out


INVERSE_VOLATILITY_SIZING_SPEC = ToolSpec(
    name="inverse_volatility",
    category="sizing",
    description="Allocate capital proportional to inverse of rolling return volatility.",
    params={
        "lookback_bars": {
            "type": "integer",
            "min": 2,
            "required": True,
        }
    },
)
