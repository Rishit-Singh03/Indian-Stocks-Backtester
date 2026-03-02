from __future__ import annotations

from datetime import date
from typing import Any

from app.strategy.tools.base import ToolSpec, ToolValidationError
from app.strategy.tools.helpers import to_date


def rsi_signal(universe_rows: list[dict[str, Any]], params: dict[str, Any]) -> list[dict[str, Any]]:
    try:
        period = int(params.get("period", 14))
    except (TypeError, ValueError) as exc:
        raise ToolValidationError("period must be integer") from exc
    if period <= 1:
        raise ToolValidationError("period must be > 1")

    try:
        overbought = float(params.get("overbought", 70.0))
        oversold = float(params.get("oversold", 30.0))
    except (TypeError, ValueError) as exc:
        raise ToolValidationError("overbought and oversold must be numeric") from exc
    if not (0 < oversold < overbought < 100):
        raise ToolValidationError("Require 0 < oversold < overbought < 100")

    mode = str(params.get("mode", "both")).strip().lower()
    if mode not in {"overbought", "oversold", "both"}:
        raise ToolValidationError("mode must be one of: overbought, oversold, both")

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
        if len(points) < period + 1:
            continue

        closes = [p[1] for p in points]
        dates = [p[0] for p in points]
        changes = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
        gains = [max(change, 0.0) for change in changes]
        losses = [max(-change, 0.0) for change in changes]

        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period

        for idx in range(period, len(changes)):
            if idx > period:
                gain = gains[idx]
                loss = losses[idx]
                avg_gain = ((avg_gain * (period - 1)) + gain) / period
                avg_loss = ((avg_loss * (period - 1)) + loss) / period

            if avg_loss == 0:
                rsi = 100.0
            else:
                rs = avg_gain / avg_loss
                rsi = 100.0 - (100.0 / (1.0 + rs))

            is_overbought = rsi >= overbought
            is_oversold = rsi <= oversold
            if mode == "overbought" and not is_overbought:
                continue
            if mode == "oversold" and not is_oversold:
                continue
            if mode == "both" and not (is_overbought or is_oversold):
                continue

            signal_type = "overbought" if is_overbought else "oversold"
            score = (rsi - overbought) if is_overbought else (oversold - rsi)
            date_idx = idx + 1
            signals.append(
                {
                    "symbol": symbol,
                    "date": dates[date_idx].isoformat(),
                    "score": abs(score),
                    "signal_type": signal_type,
                    "rsi": rsi,
                    "period": period,
                    "overbought": overbought,
                    "oversold": oversold,
                    "close": closes[date_idx],
                    "reference_close": closes[date_idx - 1],
                }
            )

    signals.sort(key=lambda row: (str(row["date"]), float(row["score"]), str(row["symbol"])), reverse=True)
    return signals


RSI_SPEC = ToolSpec(
    name="rsi",
    category="signal",
    description="Signals overbought/oversold conditions from RSI.",
    params={
        "period": {
            "type": "integer",
            "min": 2,
            "required": True,
        },
        "overbought": {
            "type": "number",
            "min": 0.0001,
            "max": 100.0,
            "required": True,
        },
        "oversold": {
            "type": "number",
            "min": 0.0001,
            "max": 100.0,
            "required": True,
        },
        "mode": {
            "type": "string",
            "enum": ["overbought", "oversold", "both"],
            "required": False,
            "default": "both",
        },
        "interval": {
            "type": "string",
            "enum": ["1d", "1w", "1mo"],
            "required": False,
            "default": "1w",
        },
    },
)
