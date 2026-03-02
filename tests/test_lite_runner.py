from __future__ import annotations

import unittest

from app.backtest.lite import run_lite_backtest
from app.strategy import TOOL_REGISTRY
from tests.test_data import make_index_rows, make_stock_rows, rows_with_halted_symbol_after_entry, tiny_rows_for_entry_timing


class LiteRunnerTests(unittest.TestCase):
    def test_lite_runner_sanity(self) -> None:
        rows = make_stock_rows()
        result = run_lite_backtest(
            rows=rows,
            registry=TOOL_REGISTRY,
            filters=[{"tool": "price_filter", "params": {"min_price": 50}}],
            entry_tool="price_change",
            entry_params={"period": "1m", "direction": "down", "threshold_pct": 7},
            exit_tool="time_based_exit",
            exit_params={"hold_periods": 6},
            sizing_tool="fixed_amount",
            sizing_params={"amount": 20_000},
            interval="1w",
            initial_capital=300_000,
            max_positions=5,
            max_new_positions=2,
            slippage_bps=5,
            cost_pct=0.05,
        )
        self.assertIn("equity_curve", result)
        self.assertIn("trades", result)
        self.assertIn("summary", result)
        self.assertGreater(len(result["equity_curve"]), 0)
        self.assertGreaterEqual(result["summary"]["bars"], 1)
        self.assertIn("final_equity", result["summary"])
        self.assertIn("returns", result)
        self.assertIn("risk", result)
        self.assertIn("ratios", result)
        self.assertIn("trade_stats", result)
        self.assertIn("monthly_pnl_grid", result)
        self.assertIn("cost_sensitivity", result)
        self.assertEqual(len(result["cost_sensitivity"]), 3)

    def test_entry_is_next_bar_open(self) -> None:
        rows = tiny_rows_for_entry_timing()
        result = run_lite_backtest(
            rows=rows,
            registry=TOOL_REGISTRY,
            filters=[],
            entry_tool="price_change",
            entry_params={"period": "1w", "direction": "down", "threshold_pct": 5},
            exit_tool="stop_loss",
            exit_params={"stop_loss_pct": 5},
            sizing_tool="fixed_amount",
            sizing_params={"amount": 10_000},
            interval="1w",
            initial_capital=100_000,
            max_positions=1,
            max_new_positions=1,
            slippage_bps=0,
            cost_pct=0,
        )
        self.assertGreaterEqual(len(result["trades"]), 1)
        first_trade = result["trades"][0]
        # Down signal is on 2024-01-08, entry must execute on next bar open (2024-01-15).
        self.assertEqual(first_trade["entry_date"], "2024-01-15")

    def test_force_close_at_end_for_halted_symbol(self) -> None:
        rows = rows_with_halted_symbol_after_entry()
        result = run_lite_backtest(
            rows=rows,
            registry=TOOL_REGISTRY,
            filters=[],
            entry_tool="price_change",
            entry_params={"period": "1w", "direction": "down", "threshold_pct": 5},
            exit_tool="time_based_exit",
            exit_params={"hold_periods": 4},
            sizing_tool="fixed_amount",
            sizing_params={"amount": 10_000},
            interval="1w",
            initial_capital=100_000,
            max_positions=1,
            max_new_positions=1,
            slippage_bps=0,
            cost_pct=0,
        )
        self.assertGreaterEqual(len(result["trades"]), 1)
        forced = [trade for trade in result["trades"] if trade.get("symbol") == "AAA"]
        self.assertEqual(len(forced), 1)
        self.assertEqual(forced[0].get("exit_reason"), "forced_last_price_end")
        self.assertEqual(forced[0].get("exit_date"), "2024-01-29")

    def test_liquidity_flag_when_position_too_large(self) -> None:
        rows = tiny_rows_for_entry_timing()
        result = run_lite_backtest(
            rows=rows,
            registry=TOOL_REGISTRY,
            filters=[],
            entry_tool="price_change",
            entry_params={"period": "1w", "direction": "down", "threshold_pct": 5},
            exit_tool="time_based_exit",
            exit_params={"hold_periods": 2},
            sizing_tool="fixed_amount",
            sizing_params={"amount": 20_000_000},
            interval="1w",
            initial_capital=50_000_000,
            max_positions=1,
            max_new_positions=1,
            slippage_bps=0,
            cost_pct=0,
        )
        self.assertGreater(result["summary"].get("liquidity_flag_count", 0), 0)
        self.assertGreater(len(result.get("liquidity_flags", [])), 0)

    def test_lite_runner_with_benchmark_rows(self) -> None:
        rows = make_stock_rows()
        benchmark_rows = make_index_rows(index_name="NIFTY_50")
        result = run_lite_backtest(
            rows=rows,
            registry=TOOL_REGISTRY,
            filters=[],
            entry_tool="price_change",
            entry_params={"period": "1m", "direction": "down", "threshold_pct": 7},
            exit_tool="time_based_exit",
            exit_params={"hold_periods": 4},
            sizing_tool="fixed_amount",
            sizing_params={"amount": 20_000},
            interval="1w",
            initial_capital=300_000,
            max_positions=5,
            max_new_positions=2,
            slippage_bps=5,
            cost_pct=0.05,
            benchmark_rows=benchmark_rows,
        )
        self.assertIsNotNone(result.get("benchmark_comparison"))
        self.assertIsNotNone(result["ratios"].get("information_ratio"))


if __name__ == "__main__":
    unittest.main()
