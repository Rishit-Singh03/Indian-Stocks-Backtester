from __future__ import annotations

import unittest
from uuid import uuid4

from app.backtest.persistence import build_equity_rows, build_run_row, build_trade_rows, normalize_run_id


class PersistenceTests(unittest.TestCase):
    def test_normalize_run_id(self) -> None:
        rid = str(uuid4())
        normalized = normalize_run_id(rid)
        self.assertEqual(normalized, rid.lower())
        with self.assertRaises(Exception):
            normalize_run_id("not-a-uuid")

    def test_build_run_trade_equity_rows(self) -> None:
        rid = str(uuid4())
        result = {
            "summary": {"trades": 2, "total_return_pct": 12.5, "sharpe": 1.2, "max_drawdown_pct": 8.4},
            "returns": {"total_return_pct": 12.5},
            "risk": {"max_drawdown_pct": 8.4},
            "ratios": {"sharpe": 1.2},
            "trade_stats": {"total_trades": 2},
        }
        run_row = build_run_row(run_id=rid, status="completed", spec={"foo": "bar"}, result=result)
        self.assertEqual(run_row["status"], "completed")
        self.assertEqual(run_row["trade_count"], 2)
        self.assertAlmostEqual(run_row["total_return"], 12.5)

        trades = [
            {
                "symbol": "AAA",
                "entry_date": "2024-01-01",
                "exit_date": "2024-01-08",
                "entry_price": 10.0,
                "exit_price": 11.0,
                "shares": 100,
                "entry_cost": 1.0,
                "exit_cost": 1.1,
                "pnl": 98.9,
                "pnl_pct": 9.89,
                "exit_reason": "target_profit",
            }
        ]
        trade_rows = build_trade_rows(run_id=rid, trades=trades)
        self.assertEqual(len(trade_rows), 1)
        self.assertEqual(trade_rows[0]["symbol"], "AAA")
        self.assertEqual(trade_rows[0]["trade_index"], 0)

        curve = [
            {"date": "2024-01-01", "cash": 100000, "market_value": 0, "equity": 100000, "open_positions": 0},
            {"date": "2024-01-08", "cash": 50000, "market_value": 52000, "equity": 102000, "open_positions": 1},
        ]
        eq_rows = build_equity_rows(run_id=rid, equity_curve=curve)
        self.assertEqual(len(eq_rows), 2)
        self.assertEqual(eq_rows[1]["point_index"], 1)
        self.assertAlmostEqual(eq_rows[1]["equity"], 102000.0)


if __name__ == "__main__":
    unittest.main()
