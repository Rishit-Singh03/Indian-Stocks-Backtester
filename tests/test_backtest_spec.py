from __future__ import annotations

import unittest

from app.backtest.spec import validate_lite_spec
from app.strategy import TOOL_REGISTRY, ToolValidationError


class BacktestSpecValidationTests(unittest.TestCase):
    def test_validate_lite_spec_success(self) -> None:
        out = validate_lite_spec(
            registry=TOOL_REGISTRY,
            interval="1w",
            filters=[{"tool": "liquidity_filter", "params": {"min_avg_volume": 100_000}}],
            entry={"tool": "price_change", "params": {"period": "1m", "direction": "down", "threshold_pct": 5}},
            exit={"tool": "stop_loss", "params": {"stop_loss_pct": 10}},
            sizing={"tool": "fixed_amount", "params": {"amount": 10_000}},
        )
        self.assertEqual(out["entry"]["tool"], "price_change")
        self.assertEqual(out["exit"]["tool"], "stop_loss")
        self.assertEqual(out["sizing"]["tool"], "fixed_amount")
        self.assertEqual(out["interval"], "1w")

    def test_validate_lite_spec_missing_required(self) -> None:
        with self.assertRaises(ToolValidationError):
            validate_lite_spec(
                registry=TOOL_REGISTRY,
                interval="1w",
                filters=[],
                entry={"tool": "price_change", "params": {"period": "1m", "direction": "down"}},
                exit={"tool": "stop_loss", "params": {"stop_loss_pct": 10}},
                sizing={"tool": "fixed_amount", "params": {"amount": 10_000}},
            )

    def test_validate_lite_spec_unknown_param(self) -> None:
        with self.assertRaises(ToolValidationError):
            validate_lite_spec(
                registry=TOOL_REGISTRY,
                interval="1w",
                filters=[],
                entry={
                    "tool": "price_change",
                    "params": {"period": "1m", "direction": "down", "threshold_pct": 5, "bad": 1},
                },
                exit={"tool": "stop_loss", "params": {"stop_loss_pct": 10}},
                sizing={"tool": "fixed_amount", "params": {"amount": 10_000}},
            )

    def test_validate_lite_spec_unknown_tool(self) -> None:
        with self.assertRaises(ToolValidationError):
            validate_lite_spec(
                registry=TOOL_REGISTRY,
                interval="1w",
                filters=[],
                entry={"tool": "unknown_signal", "params": {}},
                exit={"tool": "stop_loss", "params": {"stop_loss_pct": 10}},
                sizing={"tool": "fixed_amount", "params": {"amount": 10_000}},
            )


if __name__ == "__main__":
    unittest.main()
