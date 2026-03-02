from __future__ import annotations

from datetime import date
from math import sqrt
from typing import Any

from app.strategy.tools.base import ToolSpec, ToolValidationError
from app.strategy.tools.helpers import to_date


def mean_reversion_zscore_signal(universe_rows: list[dict[str, Any]], params: dict[str, Any]) -> list[dict[str, Any]]:
    try:
        lookback = int(params.get("lookback", 20))
    except (TypeError, ValueError) as exc:
        raise ToolValidationError("lookback must be integer") from exc
    if lookback <= 1:
        raise ToolValidationError("lookback must be > 1")

    try:
        z_threshold = float(params.get("z_threshold", 2.0))
    except (TypeError, ValueError) as exc:
        raise ToolValidationError("z_threshold must be numeric") from exc
    if z_threshold <= 0:
        raise ToolValidationError("z_threshold must be > 0")

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

    signals: list[dict[str, Any]] = []
    for symbol, points in grouped.items():
        points.sort(key=lambda item: item[0])
        if len(points) < lookback + 2:
            continue

        returns: list[tuple[date, float, float, float]] = []
        for idx in range(1, len(points)):
            prev_date, prev_close = points[idx - 1]
            curr_date, curr_close = points[idx]
            if prev_close <= 0:
                continue
            ret = (curr_close / prev_close) - 1.0
            returns.append((curr_date, ret, curr_close, prev_close))

        if len(returns) < lookback + 1:
            continue

        for idx in range(lookback, len(returns)):
            curr_date, curr_ret, curr_close, prev_close = returns[idx]
            window = [item[1] for item in returns[idx - lookback : idx]]
            mean_ret = sum(window) / lookback
            variance = sum((value - mean_ret) ** 2 for value in window) / lookback
            std_ret = sqrt(variance)
            if std_ret <= 0:
                continue
            z = (curr_ret - mean_ret) / std_ret
            if abs(z) < z_threshold:
                continue
            signals.append(
                {
                    "symbol": symbol,
                    "date": curr_date.isoformat(),
                    "score": abs(z),
                    "z_score": z,
                    "return_pct": curr_ret * 100.0,
                    "lookback": lookback,
                    "z_threshold": z_threshold,
                    "mean_return_pct": mean_ret * 100.0,
                    "std_return_pct": std_ret * 100.0,
                    "close": curr_close,
                    "reference_close": prev_close,
                }
            )

    signals.sort(key=lambda row: (str(row["date"]), float(row["score"]), str(row["symbol"])), reverse=True)
    return signals


MEAN_REVERSION_ZSCORE_SPEC = ToolSpec(
    name="mean_reversion_zscore",
    category="signal",
    description="Signals bars where return z-score exceeds threshold based on rolling return mean/std.",
    params={
        "lookback": {
            "type": "integer",
            "min": 2,
            "required": True,
        },
        "z_threshold": {
            "type": "number",
            "min": 0.0001,
            "required": True,
        },
        "interval": {
            "type": "string",
            "enum": ["1d", "1w", "1mo"],
            "required": False,
            "default": "1w",
        },
    },
)
