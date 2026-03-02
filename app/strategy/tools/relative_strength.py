from __future__ import annotations

from collections import defaultdict
from datetime import date
from math import ceil
from typing import Any

from app.strategy.tools.base import ToolSpec, ToolValidationError
from app.strategy.tools.helpers import lookback_bars, normalize_rank, to_date


def relative_strength_signal(universe_rows: list[dict[str, Any]], params: dict[str, Any]) -> list[dict[str, Any]]:
    period = str(params.get("period", "1m")).strip().lower()
    interval = str(params.get("interval", "1w")).strip().lower()
    rank = normalize_rank(params.get("rank", "top"))

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

    lookback = lookback_bars(period, interval)
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
        if len(points) <= lookback:
            continue
        for idx in range(lookback, len(points)):
            prev_date, prev_close = points[idx - lookback]
            curr_date, curr_close = points[idx]
            if prev_close <= 0:
                continue
            pct_change = ((curr_close / prev_close) - 1.0) * 100.0
            cross_section[curr_date.isoformat()].append(
                {
                    "symbol": symbol,
                    "date": curr_date.isoformat(),
                    "pct_change": pct_change,
                    "lookback_period": period,
                    "lookback_bars": lookback,
                    "reference_date": prev_date.isoformat(),
                    "reference_close": prev_close,
                    "close": curr_close,
                }
            )

    signals: list[dict[str, Any]] = []
    for dt in sorted(cross_section.keys()):
        items = cross_section[dt]
        if not items:
            continue
        ordered = sorted(items, key=lambda row: float(row["pct_change"]), reverse=True)
        take = count if count is not None else max(1, ceil(len(ordered) * float(percentile) / 100.0))
        take = min(take, len(ordered))
        selected = ordered[:take] if rank == "top" else list(reversed(ordered[-take:]))
        for idx, row in enumerate(selected, start=1):
            pct_change = float(row["pct_change"])
            signals.append(
                {
                    "symbol": row["symbol"],
                    "date": row["date"],
                    "score": abs(pct_change),
                    "pct_change": pct_change,
                    "rank": rank,
                    "rank_position": idx,
                    "lookback_period": row["lookback_period"],
                    "lookback_bars": row["lookback_bars"],
                    "reference_date": row["reference_date"],
                    "reference_close": row["reference_close"],
                    "close": row["close"],
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


RELATIVE_STRENGTH_SPEC = ToolSpec(
    name="relative_strength",
    category="signal",
    description="Cross-sectional ranking by return over lookback period (top/bottom).",
    params={
        "period": {
            "type": "string",
            "enum": ["1w", "2w", "1m", "3m"],
            "required": True,
        },
        "rank": {
            "type": "string",
            "enum": ["top", "bottom"],
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
