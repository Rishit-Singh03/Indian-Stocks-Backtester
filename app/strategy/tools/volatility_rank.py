from __future__ import annotations

from collections import defaultdict
from datetime import date
from math import ceil, sqrt
from typing import Any

from app.strategy.tools.base import ToolSpec, ToolValidationError
from app.strategy.tools.helpers import to_date, weeks_to_bars


def _normalize_vol_rank(value: Any) -> str:
    rank = str(value or "").strip().lower()
    if rank == "top":
        return "high"
    if rank == "bottom":
        return "low"
    if rank not in {"high", "low"}:
        raise ToolValidationError("rank must be one of: high, low")
    return rank


def volatility_rank_signal(universe_rows: list[dict[str, Any]], params: dict[str, Any]) -> list[dict[str, Any]]:
    interval = str(params.get("interval", "1w")).strip().lower()
    try:
        lookback_weeks = int(params.get("lookback_weeks", 52))
    except (TypeError, ValueError) as exc:
        raise ToolValidationError("lookback_weeks must be integer") from exc
    lookback_bars = weeks_to_bars(lookback_weeks, interval)
    if lookback_bars < 2:
        raise ToolValidationError("lookback window must span at least 2 bars")

    rank = _normalize_vol_rank(params.get("rank", "high"))

    count_raw = params.get("count")
    percentile_raw = params.get("percentile")
    if count_raw is None and percentile_raw is None:
        count = 20
        percentile = None
    else:
        count = None
        percentile = None
        if count_raw is not None:
            try:
                count = int(count_raw)
            except (TypeError, ValueError) as exc:
                raise ToolValidationError("count must be integer") from exc
            if count <= 0:
                raise ToolValidationError("count must be > 0")
        if percentile_raw is not None:
            try:
                percentile = float(percentile_raw)
            except (TypeError, ValueError) as exc:
                raise ToolValidationError("percentile must be numeric") from exc
            if percentile <= 0 or percentile > 100:
                raise ToolValidationError("percentile must be in (0, 100]")
    if count is not None and percentile is not None:
        raise ToolValidationError("Specify either count or percentile, not both")

    grouped: dict[str, list[tuple[date, float]]] = {}
    for row in universe_rows:
        symbol = str(row.get("symbol", "")).strip().upper()
        if not symbol:
            continue
        try:
            dt = to_date(row.get("date", ""))
            close = float(row.get("close"))
        except Exception:
            continue
        grouped.setdefault(symbol, []).append((dt, close))

    cross_section: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for symbol, points in grouped.items():
        points.sort(key=lambda item: item[0])
        if len(points) < lookback_bars + 1:
            continue
        returns: list[tuple[date, float]] = []
        for idx in range(1, len(points)):
            prev_close = points[idx - 1][1]
            curr_date, curr_close = points[idx]
            if prev_close <= 0:
                continue
            returns.append((curr_date, (curr_close / prev_close) - 1.0))
        if len(returns) < lookback_bars:
            continue

        for idx in range(lookback_bars - 1, len(returns)):
            dt = returns[idx][0]
            window = [item[1] for item in returns[idx - lookback_bars + 1 : idx + 1]]
            mean_ret = sum(window) / lookback_bars
            variance = sum((value - mean_ret) ** 2 for value in window) / lookback_bars
            vol = sqrt(variance)
            cross_section[dt.isoformat()].append(
                {
                    "symbol": symbol,
                    "date": dt.isoformat(),
                    "volatility": vol,
                    "volatility_pct": vol * 100.0,
                    "lookback_weeks": lookback_weeks,
                    "lookback_bars": lookback_bars,
                }
            )

    signals: list[dict[str, Any]] = []
    for dt in sorted(cross_section.keys()):
        items = cross_section[dt]
        if not items:
            continue
        ordered = sorted(items, key=lambda row: float(row["volatility"]), reverse=True)
        take = count if count is not None else max(1, ceil(len(ordered) * float(percentile) / 100.0))
        take = min(take, len(ordered))
        selected = ordered[:take] if rank == "high" else list(reversed(ordered[-take:]))
        for idx, row in enumerate(selected, start=1):
            signals.append(
                {
                    "symbol": row["symbol"],
                    "date": row["date"],
                    "score": float(row["volatility_pct"]),
                    "rank": rank,
                    "rank_position": idx,
                    "volatility_pct": float(row["volatility_pct"]),
                    "lookback_weeks": row["lookback_weeks"],
                    "lookback_bars": row["lookback_bars"],
                }
            )

    signals.sort(
        key=lambda row: (
            str(row["date"]),
            -float(row["rank_position"]),
            float(row["score"]),
            str(row["symbol"]),
        ),
        reverse=True,
    )
    return signals


VOLATILITY_RANK_SPEC = ToolSpec(
    name="volatility_rank",
    category="signal",
    description="Cross-sectional ranking by rolling return volatility over lookback window.",
    params={
        "lookback_weeks": {
            "type": "integer",
            "min": 1,
            "required": True,
        },
        "rank": {
            "type": "string",
            "enum": ["high", "low"],
            "required": True,
        },
        "count": {
            "type": "integer",
            "min": 1,
            "required": False,
        },
        "percentile": {
            "type": "number",
            "min": 0.0001,
            "max": 100.0,
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
