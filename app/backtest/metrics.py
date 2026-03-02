from __future__ import annotations

from collections import defaultdict
from datetime import date
from math import sqrt
from statistics import stdev
from typing import Any


def _to_date(value: Any) -> date:
    return date.fromisoformat(str(value))


def _safe_ratio(numerator: float, denominator: float) -> float | None:
    if denominator == 0:
        return None
    return numerator / denominator


def _periods_per_year(interval: str) -> int:
    key = interval.strip().lower()
    if key == "1d":
        return 252
    if key == "1mo":
        return 12
    return 52


def _equity_returns(equity_curve: list[dict[str, Any]]) -> list[float]:
    returns: list[float] = []
    for idx in range(1, len(equity_curve)):
        prev = float(equity_curve[idx - 1].get("equity", 0.0))
        curr = float(equity_curve[idx].get("equity", 0.0))
        if prev <= 0:
            continue
        returns.append((curr / prev) - 1.0)
    return returns


def _equity_returns_by_date(equity_curve: list[dict[str, Any]]) -> dict[str, float]:
    out: dict[str, float] = {}
    for idx in range(1, len(equity_curve)):
        prev = float(equity_curve[idx - 1].get("equity", 0.0))
        curr = float(equity_curve[idx].get("equity", 0.0))
        if prev <= 0:
            continue
        out[str(equity_curve[idx].get("date"))] = (curr / prev) - 1.0
    return out


def _monthly_returns(equity_curve: list[dict[str, Any]]) -> list[dict[str, Any]]:
    month_end: dict[str, float] = {}
    for point in equity_curve:
        dt = _to_date(point["date"])
        month_key = f"{dt.year:04d}-{dt.month:02d}"
        month_end[month_key] = float(point["equity"])
    keys = sorted(month_end.keys())
    out: list[dict[str, Any]] = []
    prev_equity: float | None = None
    for key in keys:
        current = month_end[key]
        if prev_equity is None or prev_equity <= 0:
            ret_pct = 0.0
        else:
            ret_pct = ((current / prev_equity) - 1.0) * 100.0
        out.append({"month": key, "return_pct": ret_pct})
        prev_equity = current
    return out


def _yearly_returns(equity_curve: list[dict[str, Any]]) -> list[dict[str, Any]]:
    year_end: dict[int, float] = {}
    for point in equity_curve:
        dt = _to_date(point["date"])
        year_end[dt.year] = float(point["equity"])
    years = sorted(year_end.keys())
    out: list[dict[str, Any]] = []
    prev_equity: float | None = None
    for year in years:
        current = year_end[year]
        if prev_equity is None or prev_equity <= 0:
            ret_pct = 0.0
        else:
            ret_pct = ((current / prev_equity) - 1.0) * 100.0
        out.append({"year": year, "return_pct": ret_pct})
        prev_equity = current
    return out


def _monthly_pnl_grid(monthly_returns: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_year: dict[int, dict[int, float]] = defaultdict(dict)
    for row in monthly_returns:
        month = str(row.get("month", ""))
        parts = month.split("-")
        if len(parts) != 2:
            continue
        year = int(parts[0])
        m = int(parts[1])
        by_year[year][m] = float(row.get("return_pct", 0.0))
    out: list[dict[str, Any]] = []
    for year in sorted(by_year.keys()):
        row: dict[str, Any] = {"year": year}
        for m in range(1, 13):
            row[f"m{m:02d}"] = by_year[year].get(m)
        out.append(row)
    return out


def _max_drawdown_stats(equity_curve: list[dict[str, Any]]) -> tuple[float, int]:
    peak = -1.0
    max_dd = 0.0
    current_dd_len = 0
    max_dd_len = 0
    for point in equity_curve:
        eq = float(point.get("equity", 0.0))
        if eq >= peak:
            peak = eq
            current_dd_len = 0
            continue
        if peak > 0:
            dd = ((peak - eq) / peak) * 100.0
            max_dd = max(max_dd, dd)
            current_dd_len += 1
            max_dd_len = max(max_dd_len, current_dd_len)
    return max_dd, max_dd_len


def _trade_holding_period(entry_date: str, exit_date: str, interval: str) -> float:
    days = (_to_date(exit_date) - _to_date(entry_date)).days
    if interval == "1d":
        return float(max(days, 0))
    if interval == "1mo":
        return max(days, 0) / 30.4375
    return max(days, 0) / 7.0


def _trade_stats(trades: list[dict[str, Any]], interval: str) -> dict[str, Any]:
    total = len(trades)
    wins = [trade for trade in trades if float(trade.get("pnl", 0.0)) > 0]
    losses = [trade for trade in trades if float(trade.get("pnl", 0.0)) < 0]
    gross_profit = sum(float(trade.get("pnl", 0.0)) for trade in wins)
    gross_loss_abs = abs(sum(float(trade.get("pnl", 0.0)) for trade in losses))
    avg_win_pct = sum(float(trade.get("pnl_pct", 0.0)) for trade in wins) / len(wins) if wins else None
    avg_loss_pct = sum(float(trade.get("pnl_pct", 0.0)) for trade in losses) / len(losses) if losses else None
    holding_periods = [
        _trade_holding_period(str(trade.get("entry_date")), str(trade.get("exit_date")), interval)
        for trade in trades
        if trade.get("entry_date") and trade.get("exit_date")
    ]
    avg_holding = sum(holding_periods) / len(holding_periods) if holding_periods else None
    return {
        "total_trades": total,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": ((len(wins) / total) * 100.0) if total > 0 else None,
        "avg_win_pct": avg_win_pct,
        "avg_loss_pct": avg_loss_pct,
        "profit_factor": _safe_ratio(gross_profit, gross_loss_abs),
        "avg_holding_periods": avg_holding,
    }


def _cost_sensitivity(
    *,
    final_equity: float,
    initial_capital: float,
    total_cost_paid: float,
) -> list[dict[str, Any]]:
    # Estimate only: assumes share quantities would stay unchanged across multipliers.
    pre_cost_equity = final_equity + total_cost_paid
    out: list[dict[str, Any]] = []
    for mult in (0.0, 1.0, 2.0):
        eq = pre_cost_equity - (mult * total_cost_paid)
        ret_pct = ((eq / initial_capital) - 1.0) * 100.0 if initial_capital > 0 else 0.0
        out.append({"cost_multiplier": mult, "estimated_final_equity": eq, "estimated_total_return_pct": ret_pct})
    return out


def _benchmark_comparison(
    *,
    portfolio_curve: list[dict[str, Any]],
    benchmark_curve: list[dict[str, Any]],
    interval: str,
    risk_free_rate_annual: float,
    benchmark_name: str | None,
) -> dict[str, Any] | None:
    if len(portfolio_curve) < 2 or len(benchmark_curve) < 2:
        return None

    p_ret = _equity_returns_by_date(portfolio_curve)
    b_ret = _equity_returns_by_date(benchmark_curve)
    common = sorted(set(p_ret.keys()).intersection(b_ret.keys()))
    if len(common) < 2:
        return None

    x = [b_ret[d] for d in common]
    y = [p_ret[d] for d in common]

    mx = sum(x) / len(x)
    my = sum(y) / len(y)
    var_x = sum((v - mx) ** 2 for v in x)
    cov_xy = sum((a - mx) * (b - my) for a, b in zip(x, y))
    beta = cov_xy / var_x if var_x > 0 else None

    ppy = _periods_per_year(interval)
    rf_per_period = risk_free_rate_annual / ppy
    alpha_annual = None
    if beta is not None:
        alpha_per_period = (my - rf_per_period) - (beta * (mx - rf_per_period))
        alpha_annual = alpha_per_period * ppy

    active = [a - b for a, b in zip(y, x)]
    tracking_error = None
    information_ratio = None
    if len(active) >= 2:
        te = stdev(active) * sqrt(ppy)
        tracking_error = te
        information_ratio = ((sum(active) / len(active)) * ppy) / te if te > 0 else None

    up_pairs = [(pr, br) for pr, br in zip(y, x) if br > 0]
    down_pairs = [(pr, br) for pr, br in zip(y, x) if br < 0]
    up_capture = None
    down_capture = None
    if up_pairs:
        p_up = 1.0
        b_up = 1.0
        for pr, br in up_pairs:
            p_up *= 1.0 + pr
            b_up *= 1.0 + br
        b_up_ret = b_up - 1.0
        up_capture = ((p_up - 1.0) / b_up_ret) if b_up_ret != 0 else None
    if down_pairs:
        p_down = 1.0
        b_down = 1.0
        for pr, br in down_pairs:
            p_down *= 1.0 + pr
            b_down *= 1.0 + br
        b_down_ret = b_down - 1.0
        down_capture = ((p_down - 1.0) / b_down_ret) if b_down_ret != 0 else None

    return {
        "benchmark": benchmark_name or "BENCHMARK",
        "observations": len(common),
        "alpha_annual_pct": (alpha_annual * 100.0) if alpha_annual is not None else None,
        "beta": beta,
        "tracking_error_annual": tracking_error,
        "information_ratio": information_ratio,
        "up_capture": up_capture,
        "down_capture": down_capture,
    }


def compute_backtest_metrics(
    *,
    equity_curve: list[dict[str, Any]],
    trades: list[dict[str, Any]],
    initial_capital: float,
    interval: str,
    risk_free_rate_annual: float = 0.06,
    benchmark_equity_curve: list[dict[str, Any]] | None = None,
    benchmark_name: str | None = None,
) -> dict[str, Any]:
    final_equity = float(equity_curve[-1]["equity"]) if equity_curve else float(initial_capital)
    total_return_pct = ((final_equity / initial_capital) - 1.0) * 100.0 if initial_capital > 0 else 0.0

    cagr_pct = 0.0
    if len(equity_curve) >= 2 and initial_capital > 0:
        start_dt = _to_date(equity_curve[0]["date"])
        end_dt = _to_date(equity_curve[-1]["date"])
        years = max((end_dt - start_dt).days / 365.25, 1e-9)
        cagr_pct = (((final_equity / initial_capital) ** (1.0 / years)) - 1.0) * 100.0

    periodic = _equity_returns(equity_curve)
    ppy = _periods_per_year(interval)
    volatility_pct = 0.0
    downside_deviation_pct = 0.0
    if len(periodic) >= 2:
        volatility_pct = stdev(periodic) * sqrt(ppy) * 100.0
        downside = [ret for ret in periodic if ret < 0]
        if len(downside) >= 2:
            downside_deviation_pct = stdev(downside) * sqrt(ppy) * 100.0

    max_drawdown_pct, max_dd_duration_bars = _max_drawdown_stats(equity_curve)

    cagr_decimal = cagr_pct / 100.0
    volatility_decimal = volatility_pct / 100.0
    downside_decimal = downside_deviation_pct / 100.0
    max_dd_decimal = max_drawdown_pct / 100.0
    sharpe = _safe_ratio(cagr_decimal - risk_free_rate_annual, volatility_decimal)
    sortino = _safe_ratio(cagr_decimal - risk_free_rate_annual, downside_decimal)
    calmar = _safe_ratio(cagr_decimal, max_dd_decimal)

    monthly_returns = _monthly_returns(equity_curve)
    yearly_returns = _yearly_returns(equity_curve)
    monthly_grid = _monthly_pnl_grid(monthly_returns)
    trade_stats = _trade_stats(trades, interval)
    total_cost_paid = sum(float(trade.get("entry_cost", 0.0)) + float(trade.get("exit_cost", 0.0)) for trade in trades)

    benchmark_cmp = None
    if benchmark_equity_curve:
        benchmark_cmp = _benchmark_comparison(
            portfolio_curve=equity_curve,
            benchmark_curve=benchmark_equity_curve,
            interval=interval,
            risk_free_rate_annual=risk_free_rate_annual,
            benchmark_name=benchmark_name,
        )

    information_ratio = benchmark_cmp.get("information_ratio") if benchmark_cmp else None

    return {
        "returns": {
            "initial_capital": float(initial_capital),
            "final_equity": final_equity,
            "total_return_pct": total_return_pct,
            "cagr_pct": cagr_pct,
            "monthly_returns": monthly_returns,
            "yearly_returns": yearly_returns,
        },
        "risk": {
            "max_drawdown_pct": max_drawdown_pct,
            "max_drawdown_duration_bars": max_dd_duration_bars,
            "annualized_volatility_pct": volatility_pct,
            "downside_deviation_pct": downside_deviation_pct,
        },
        "ratios": {
            "sharpe": sharpe,
            "sortino": sortino,
            "calmar": calmar,
            "information_ratio": information_ratio,
        },
        "trade_stats": trade_stats,
        "monthly_pnl_grid": monthly_grid,
        "cost_sensitivity": _cost_sensitivity(
            final_equity=final_equity,
            initial_capital=float(initial_capital),
            total_cost_paid=total_cost_paid,
        ),
        "benchmark_comparison": benchmark_cmp,
    }
