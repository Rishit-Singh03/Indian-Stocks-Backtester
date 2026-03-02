from __future__ import annotations

import unittest

from app.backtest.metrics import compute_backtest_metrics


class MetricsTests(unittest.TestCase):
    def test_compute_backtest_metrics_core_fields(self) -> None:
        equity_curve = [
            {"date": "2024-01-31", "equity": 100_000.0},
            {"date": "2024-02-29", "equity": 105_000.0},
            {"date": "2024-03-31", "equity": 102_000.0},
            {"date": "2024-04-30", "equity": 110_000.0},
        ]
        trades = [
            {
                "entry_date": "2024-02-01",
                "exit_date": "2024-02-29",
                "entry_cost": 50.0,
                "exit_cost": 45.0,
                "pnl": 5000.0,
                "pnl_pct": 5.0,
            },
            {
                "entry_date": "2024-03-01",
                "exit_date": "2024-03-31",
                "entry_cost": 40.0,
                "exit_cost": 35.0,
                "pnl": -3000.0,
                "pnl_pct": -3.0,
            },
        ]
        out = compute_backtest_metrics(
            equity_curve=equity_curve,
            trades=trades,
            initial_capital=100_000.0,
            interval="1w",
        )
        self.assertIn("returns", out)
        self.assertIn("risk", out)
        self.assertIn("ratios", out)
        self.assertIn("trade_stats", out)
        self.assertIn("monthly_pnl_grid", out)
        self.assertIn("cost_sensitivity", out)
        self.assertEqual(len(out["cost_sensitivity"]), 3)
        self.assertGreaterEqual(out["returns"]["total_return_pct"], 0.0)
        self.assertGreaterEqual(out["risk"]["max_drawdown_pct"], 0.0)

        zero_x = out["cost_sensitivity"][0]["estimated_final_equity"]
        one_x = out["cost_sensitivity"][1]["estimated_final_equity"]
        two_x = out["cost_sensitivity"][2]["estimated_final_equity"]
        self.assertGreaterEqual(zero_x, one_x)
        self.assertGreaterEqual(one_x, two_x)

    def test_compute_backtest_metrics_with_benchmark(self) -> None:
        portfolio_curve = [
            {"date": "2024-01-31", "equity": 100_000.0},
            {"date": "2024-02-29", "equity": 106_000.0},
            {"date": "2024-03-31", "equity": 104_000.0},
            {"date": "2024-04-30", "equity": 112_000.0},
        ]
        benchmark_curve = [
            {"date": "2024-01-31", "equity": 100_000.0},
            {"date": "2024-02-29", "equity": 103_000.0},
            {"date": "2024-03-31", "equity": 102_500.0},
            {"date": "2024-04-30", "equity": 107_000.0},
        ]
        out = compute_backtest_metrics(
            equity_curve=portfolio_curve,
            trades=[],
            initial_capital=100_000.0,
            interval="1w",
            benchmark_equity_curve=benchmark_curve,
            benchmark_name="NIFTY_50",
        )
        cmp = out.get("benchmark_comparison")
        self.assertIsInstance(cmp, dict)
        self.assertEqual(cmp.get("benchmark"), "NIFTY_50")
        self.assertGreaterEqual(int(cmp.get("observations", 0)), 2)
        self.assertIn("beta", cmp)
        self.assertIn("tracking_error_annual", cmp)
        self.assertIsNotNone(out["ratios"].get("information_ratio"))


if __name__ == "__main__":
    unittest.main()
