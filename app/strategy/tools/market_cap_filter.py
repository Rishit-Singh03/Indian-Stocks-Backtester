from __future__ import annotations

from collections import defaultdict
from math import ceil
from typing import Any

from app.strategy.tools.base import ToolSpec, ToolValidationError


def _normalize_market_cap_rank(value: Any) -> str:
    rank = str(value or "").strip().lower()
    if rank not in {"large", "mid", "small"}:
        raise ToolValidationError("rank must be one of: large, mid, small")
    return rank


def market_cap_filter(universe_rows: list[dict[str, Any]], params: dict[str, Any]) -> list[dict[str, Any]]:
    rank = _normalize_market_cap_rank(params.get("rank", "large"))
    try:
        window_bars = int(params.get("window_bars", 20))
    except (TypeError, ValueError) as exc:
        raise ToolValidationError("window_bars must be integer") from exc
    if window_bars <= 0:
        raise ToolValidationError("window_bars must be > 0")

    try:
        bucket_pct = float(params.get("bucket_pct", 33.34))
    except (TypeError, ValueError) as exc:
        raise ToolValidationError("bucket_pct must be numeric") from exc
    if bucket_pct <= 0 or bucket_pct > 50:
        raise ToolValidationError("bucket_pct must be in (0, 50]")

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in universe_rows:
        symbol = str(row.get("symbol", "")).strip().upper()
        if symbol:
            grouped[symbol].append(row)

    proxies: list[tuple[str, float]] = []
    for symbol, rows in grouped.items():
        ordered = sorted(rows, key=lambda item: str(item.get("date", "")))
        window = ordered[-window_bars:]
        values: list[float] = []
        for row in window:
            try:
                close = float(row.get("close", 0.0))
                volume = float(row.get("volume", 0.0))
            except (TypeError, ValueError):
                continue
            if close > 0 and volume > 0:
                values.append(close * volume)
        if not values:
            continue
        proxy = sum(values) / len(values)
        proxies.append((symbol, proxy))

    if not proxies:
        return []
    proxies.sort(key=lambda item: item[1], reverse=True)
    n = len(proxies)
    k = max(1, ceil(n * bucket_pct / 100.0))

    large_set = {symbol for symbol, _ in proxies[:k]}
    small_set = {symbol for symbol, _ in proxies[-k:]}
    all_set = {symbol for symbol, _ in proxies}
    mid_set = all_set - large_set - small_set
    if not mid_set:
        mid_start = min(k, max(0, n // 3))
        mid_end = max(mid_start + 1, n - k)
        mid_set = {symbol for symbol, _ in proxies[mid_start:mid_end]}

    if rank == "large":
        allowed = large_set
    elif rank == "small":
        allowed = small_set
    else:
        allowed = mid_set

    return [row for row in universe_rows if str(row.get("symbol", "")).strip().upper() in allowed]


MARKET_CAP_FILTER_SPEC = ToolSpec(
    name="market_cap_filter",
    category="filter",
    description="Proxy market-cap filter using rolling average of close*volume and large/mid/small buckets.",
    params={
        "rank": {
            "type": "string",
            "enum": ["large", "mid", "small"],
            "required": True,
        },
        "window_bars": {
            "type": "integer",
            "min": 1,
            "required": False,
            "default": 20,
        },
        "bucket_pct": {
            "type": "number",
            "min": 0.0001,
            "max": 50.0,
            "required": False,
            "default": 33.34,
        },
    },
)
