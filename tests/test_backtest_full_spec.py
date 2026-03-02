from __future__ import annotations

import unittest

from app.backtest import lite_payload_to_strategy_spec, strategy_spec_to_lite_payload, validate_strategy_spec
from app.strategy import TOOL_REGISTRY, ToolValidationError


class BacktestFullSpecTests(unittest.TestCase):
    def test_validate_strategy_spec_and_map_to_lite(self) -> None:
        full_spec = {
            "name": "Multi Signal Test",
            "description": "Test full strategy spec",
            "universe": {
                "type": "stock",
                "symbols": ["RELIANCE", "TCS", "INFY"],
                "filters": [{"tool": "liquidity_filter", "params": {"min_avg_volume": 100000}}],
            },
            "entry": {
                "signals": [
                    {"tool": "price_change", "params": {"period": "1m", "direction": "down", "threshold_pct": 5}},
                    {"tool": "rsi", "params": {"period": 14, "overbought": 70, "oversold": 30, "mode": "oversold"}},
                ],
                "combine": "AND",
                "rank_by": "price_change",
                "max_signals_per_period": 3,
            },
            "exit": {
                "conditions": [
                    {"tool": "stop_loss", "params": {"stop_loss_pct": 8}},
                    {"tool": "time_based_exit", "params": {"hold_periods": 4}},
                ],
                "combine": "FIRST_HIT",
            },
            "sizing": {"tool": "fixed_amount", "params": {"amount": 50000}},
            "execution": {
                "initial_capital": 1000000,
                "entry_timing": "next_open",
                "rebalance": "weekly",
                "max_positions": 12,
                "costs": {"slippage_bps": 10, "round_trip_pct": 0.05},
            },
            "benchmark": "SENSEX",
            "date_range": {"start": "2023-01-01", "end": "2025-12-31"},
        }
        normalized = validate_strategy_spec(registry=TOOL_REGISTRY, strategy_spec=full_spec)
        self.assertEqual(normalized["entry"]["combine"], "AND")
        self.assertEqual(len(normalized["entry"]["signals"]), 2)
        self.assertEqual(normalized["execution"]["rebalance"], "weekly")

        lite_payload = strategy_spec_to_lite_payload(normalized)
        self.assertEqual(lite_payload["entry"]["tool"], "combined_signal")
        self.assertEqual(lite_payload["exit"]["tool"], "combined_exit")
        self.assertEqual(lite_payload["interval"], "1w")
        self.assertEqual(lite_payload["max_new_positions"], 3)

    def test_validate_strategy_spec_rank_by_must_match_signal(self) -> None:
        full_spec = {
            "universe": {"type": "stock", "symbols": ["RELIANCE"]},
            "entry": {
                "signals": [{"tool": "price_change", "params": {"period": "1m", "direction": "down", "threshold_pct": 5}}],
                "combine": "AND",
                "rank_by": "rsi",
                "max_signals_per_period": 2,
            },
            "exit": {"conditions": [{"tool": "stop_loss", "params": {"stop_loss_pct": 10}}], "combine": "FIRST_HIT"},
            "sizing": {"tool": "fixed_amount", "params": {"amount": 10000}},
            "execution": {"initial_capital": 1000000, "entry_timing": "next_open", "rebalance": "weekly"},
            "date_range": {"start": "2023-01-01", "end": "2025-01-01"},
        }
        with self.assertRaises(ToolValidationError):
            validate_strategy_spec(registry=TOOL_REGISTRY, strategy_spec=full_spec)

    def test_lite_payload_to_strategy_spec_roundtrip(self) -> None:
        lite_payload = {
            "universe": "stock",
            "symbols": ["RELIANCE", "TCS"],
            "interval": "1w",
            "start_date": "2024-01-01",
            "end_date": "2025-01-01",
            "filters": [],
            "entry": {"tool": "price_change", "params": {"period": "1m", "direction": "down", "threshold_pct": 5}},
            "exit": {"tool": "stop_loss", "params": {"stop_loss_pct": 8}},
            "sizing": {"tool": "fixed_amount", "params": {"amount": 10000}},
            "initial_capital": 1000000,
            "max_positions": 10,
            "max_new_positions": 3,
            "slippage_bps": 10,
            "cost_pct": 0.05,
            "benchmark": "SENSEX",
        }
        full_like = lite_payload_to_strategy_spec(lite_payload)
        normalized = validate_strategy_spec(registry=TOOL_REGISTRY, strategy_spec=full_like)
        self.assertEqual(normalized["universe"]["type"], "stock")
        self.assertEqual(normalized["execution"]["rebalance"], "weekly")
        self.assertEqual(normalized["execution"]["costs"]["round_trip_pct"], 0.05)
        self.assertEqual(normalized["entry"]["max_signals_per_period"], 3)


if __name__ == "__main__":
    unittest.main()
