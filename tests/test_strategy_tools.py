from __future__ import annotations

import unittest

from app.strategy import TOOL_REGISTRY, ToolValidationError
from tests.test_data import make_index_rows, make_positions, make_stock_rows


class StrategyToolsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.rows = make_stock_rows()
        self.index_rows = make_index_rows()
        self.positions = make_positions()

    def test_tools_group_counts(self) -> None:
        tools = TOOL_REGISTRY.list_tools()
        by = {}
        for tool in tools:
            by.setdefault(tool["category"], []).append(tool["name"])
        self.assertGreaterEqual(len(by.get("signal", [])), 10)
        self.assertGreaterEqual(len(by.get("filter", [])), 6)
        self.assertGreaterEqual(len(by.get("exit", [])), 6)
        self.assertGreaterEqual(len(by.get("sizing", [])), 4)

    def test_signal_tools_execute(self) -> None:
        signal_cases = {
            "price_change": {"period": "1m", "direction": "any", "threshold_pct": 2.0, "interval": "1w"},
            "moving_average_crossover": {
                "short_window": 4,
                "long_window": 12,
                "cross_direction": "above",
                "interval": "1w",
            },
            "distance_from_high_low": {"reference": "high", "lookback_weeks": 26, "distance_pct": 5.0, "interval": "1w"},
            "relative_strength": {"period": "1m", "rank": "top", "count": 2, "interval": "1w"},
            "volume_spike": {"multiplier": 1.1, "avg_period": 8, "interval": "1w"},
            "consecutive_moves": {"direction": "down", "count": 2, "interval": "1w"},
            "mean_reversion_zscore": {"lookback": 10, "z_threshold": 1.0, "interval": "1w"},
            "volatility_rank": {"lookback_weeks": 20, "rank": "high", "count": 2, "interval": "1w"},
            "index_relative": {
                "index_name": "NIFTY_50",
                "period": "1m",
                "threshold_pct": 2.0,
                "direction": "any",
                "benchmark_rows": self.index_rows,
                "interval": "1w",
            },
            "rsi": {"period": 14, "overbought": 70, "oversold": 30, "mode": "both", "interval": "1w"},
            "combined_signal": {
                "_registry": TOOL_REGISTRY,
                "combine": "OR",
                "signals": [
                    {"tool": "price_change", "params": {"period": "1m", "direction": "down", "threshold_pct": 2.0}},
                    {"tool": "rsi", "params": {"period": 14, "overbought": 70, "oversold": 30, "mode": "both"}},
                ],
                "interval": "1w",
            },
        }
        for tool, params in signal_cases.items():
            with self.subTest(tool=tool):
                out = TOOL_REGISTRY.run_signal(tool, self.rows, params)
                self.assertIsInstance(out, list)
                if out:
                    self.assertIn("symbol", out[0])
                    self.assertIn("date", out[0])
                    self.assertIn("score", out[0])

    def test_filter_tools_execute(self) -> None:
        filters = [
            ("liquidity_filter", {"min_avg_volume": 200_000.0, "window_bars": 10}),
            ("price_filter", {"min_price": 50.0, "max_price": 250.0}),
            ("listing_age_filter", {"min_weeks": 10, "interval": "1w"}),
            ("market_cap_filter", {"rank": "large", "window_bars": 12, "bucket_pct": 40.0}),
            (
                "index_membership_filter",
                {"index_name": "NIFTY_50", "membership_symbols": ["RELIANCE", "TCS", "INFY"]},
            ),
            (
                "sector_filter",
                {
                    "sectors": ["IT"],
                    "symbol_sector_map": {
                        "RELIANCE": "ENERGY",
                        "TCS": "IT",
                        "INFY": "IT",
                    },
                },
            ),
        ]
        for tool, params in filters:
            with self.subTest(tool=tool):
                out = TOOL_REGISTRY.run_filter(tool, self.rows, params)
                self.assertIsInstance(out, list)

    def test_exit_tools_execute(self) -> None:
        exits = [
            ("target_profit", {"target_profit_pct": 5.0}),
            ("stop_loss", {"stop_loss_pct": 5.0}),
            ("time_based_exit", {"hold_periods": 4}),
            ("trailing_stop", {"trailing_stop_pct": 5.0}),
            (
                "signal_reversal",
                {
                    "_registry": TOOL_REGISTRY,
                    "interval": "1w",
                    "entry_tool": "price_change",
                    "entry_params": {"period": "1m", "direction": "down", "threshold_pct": 5},
                    "reversal_tool": "price_change",
                    "reversal_params": {"period": "1m", "direction": "up", "threshold_pct": 5},
                },
            ),
            (
                "combined_exit",
                {
                    "_registry": TOOL_REGISTRY,
                    "interval": "1w",
                    "combine": "FIRST_HIT",
                    "conditions": [
                        {"tool": "stop_loss", "params": {"stop_loss_pct": 5}},
                        {"tool": "time_based_exit", "params": {"hold_periods": 4}},
                    ],
                },
            ),
        ]
        for tool, params in exits:
            with self.subTest(tool=tool):
                out = TOOL_REGISTRY.run_exit(tool, self.positions, self.rows, params)
                self.assertIsInstance(out, list)

    def test_sizing_tools_execute(self) -> None:
        candidates = [{"symbol": "RELIANCE", "price": 120.0}, {"symbol": "TCS", "price": 200.0}, {"symbol": "INFY", "price": 150.0}]
        sizing_cases = [
            ("fixed_amount", {"amount": 10_000.0}),
            ("equal_weight", {}),
            ("max_positions", {"limit": 2}),
            (
                "inverse_volatility",
                {"lookback_bars": 10, "history_rows": self.rows, "as_of_date": "2025-12-30"},
            ),
        ]
        for tool, params in sizing_cases:
            with self.subTest(tool=tool):
                out = TOOL_REGISTRY.run_sizing(tool, candidates, 100_000.0, params)
                self.assertIsInstance(out, list)

    def test_invalid_signal_param_fails(self) -> None:
        with self.assertRaises(ToolValidationError):
            TOOL_REGISTRY.run_signal("price_change", self.rows, {"period": "1m", "direction": "invalid", "threshold_pct": 5})


if __name__ == "__main__":
    unittest.main()
