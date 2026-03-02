from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class Position:
    position_id: str
    symbol: str
    entry_date: str
    entry_price: float
    shares: int
    entry_gross: float
    entry_cost: float


class PortfolioState:
    def __init__(self, initial_capital: float) -> None:
        self.initial_capital = float(initial_capital)
        self.cash = float(initial_capital)
        self.open_positions: dict[str, Position] = {}
        self.trades: list[dict[str, Any]] = []
        self.equity_curve: list[dict[str, Any]] = []
        self._position_counter = 0

    @property
    def open_positions_count(self) -> int:
        return len(self.open_positions)

    def has_symbol(self, symbol: str) -> bool:
        target = symbol.strip().upper()
        return any(pos.symbol == target for pos in self.open_positions.values())

    def create_position_id(self, symbol: str, trade_date: str) -> str:
        self._position_counter += 1
        return f"{symbol}:{trade_date}:{self._position_counter}"

    def open_position(self, *, symbol: str, trade_date: str, buy_price: float, shares: int, fee_pct: float) -> str | None:
        if shares <= 0 or buy_price <= 0:
            return None
        gross = float(shares) * float(buy_price)
        entry_cost = gross * float(fee_pct)
        total = gross + entry_cost
        if total > self.cash:
            return None
        self.cash -= total
        position_id = self.create_position_id(symbol.strip().upper(), trade_date)
        self.open_positions[position_id] = Position(
            position_id=position_id,
            symbol=symbol.strip().upper(),
            entry_date=trade_date,
            entry_price=float(buy_price),
            shares=int(shares),
            entry_gross=gross,
            entry_cost=entry_cost,
        )
        return position_id

    def close_position(
        self,
        *,
        position_id: str,
        trade_date: str,
        sell_price: float,
        fee_pct: float,
        exit_reason: str = "strategy_exit",
    ) -> dict[str, Any] | None:
        pos = self.open_positions.get(position_id)
        if pos is None:
            return None
        gross = pos.shares * float(sell_price)
        exit_cost = gross * float(fee_pct)
        net = gross - exit_cost
        self.cash += net
        invested = pos.entry_gross + pos.entry_cost
        pnl = net - invested
        pnl_pct = (pnl / invested) * 100.0 if invested > 0 else 0.0
        trade = {
            "position_id": pos.position_id,
            "symbol": pos.symbol,
            "entry_date": pos.entry_date,
            "entry_price": pos.entry_price,
            "exit_date": trade_date,
            "exit_price": float(sell_price),
            "shares": pos.shares,
            "entry_cost": pos.entry_cost,
            "exit_cost": exit_cost,
            "pnl": pnl,
            "pnl_pct": pnl_pct,
            "exit_reason": str(exit_reason),
        }
        self.trades.append(trade)
        del self.open_positions[position_id]
        return trade

    def market_value_at(
        self,
        trade_date: str,
        ohlc_map: dict[tuple[str, str], dict[str, Any]],
        last_seen_close: dict[str, float] | None = None,
    ) -> float:
        value = 0.0
        last_seen = last_seen_close or {}
        for pos in self.open_positions.values():
            row = ohlc_map.get((pos.symbol, trade_date))
            if row is not None:
                value += pos.shares * float(row["close"])
                continue
            fallback = last_seen.get(pos.symbol)
            if fallback is None:
                continue
            value += pos.shares * float(fallback)
        return value

    def record_equity(
        self,
        trade_date: str,
        ohlc_map: dict[tuple[str, str], dict[str, Any]],
        last_seen_close: dict[str, float] | None = None,
    ) -> dict[str, Any]:
        market_value = self.market_value_at(trade_date, ohlc_map, last_seen_close=last_seen_close)
        equity = self.cash + market_value
        point = {
            "date": trade_date,
            "cash": self.cash,
            "market_value": market_value,
            "equity": equity,
            "open_positions": self.open_positions_count,
        }
        self.equity_curve.append(point)
        return point

    def build_exit_payload(self, excluded_ids: set[str]) -> list[dict[str, Any]]:
        return [
            {
                "position_id": pos.position_id,
                "symbol": pos.symbol,
                "entry_date": pos.entry_date,
                "entry_price": pos.entry_price,
            }
            for pos in self.open_positions.values()
            if pos.position_id not in excluded_ids
        ]

    def summary(self, *, bars_before_filters: int, bars_after_filters: int) -> dict[str, Any]:
        final_equity = self.equity_curve[-1]["equity"] if self.equity_curve else self.cash
        total_return_pct = ((final_equity / self.initial_capital) - 1.0) * 100.0 if self.initial_capital > 0 else 0.0
        wins = sum(1 for trade in self.trades if float(trade["pnl"]) > 0)
        losses = sum(1 for trade in self.trades if float(trade["pnl"]) <= 0)
        total_trades = len(self.trades)
        win_rate_pct = (wins / total_trades) * 100.0 if total_trades > 0 else None

        peak = -1.0
        max_drawdown_pct = 0.0
        for point in self.equity_curve:
            eq = float(point["equity"])
            peak = max(peak, eq)
            if peak > 0:
                dd = ((peak - eq) / peak) * 100.0
                max_drawdown_pct = max(max_drawdown_pct, dd)

        return {
            "initial_capital": self.initial_capital,
            "final_equity": final_equity,
            "total_return_pct": total_return_pct,
            "bars": len(self.equity_curve),
            "bars_before_filters_total": bars_before_filters,
            "bars_after_filters_total": bars_after_filters,
            "trades": total_trades,
            "wins": wins,
            "losses": losses,
            "win_rate_pct": win_rate_pct,
            "max_drawdown_pct": max_drawdown_pct,
        }
