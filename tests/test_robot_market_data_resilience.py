import asyncio
import unittest
from unittest.mock import patch

from backend import main


class RobotMarketDataResilienceTests(unittest.IsolatedAsyncioTestCase):
    async def test_scan_continues_when_one_active_times_out(self) -> None:
        async def fake_analyze_active_signal(
            user_id: str,
            symbol: str,
            timeframe: str = "M1",
            endtime: int | None = None,
            strategy_mode: str = "conservative",
        ):
            if symbol == "EURUSD-OTC":
                await asyncio.sleep(1)
            if symbol == "GBPUSD-OTC":
                return 200, main.build_success(
                    {
                        "symbol": symbol,
                        "signal": "CALL",
                        "direction": "CALL",
                        "confidence": 88,
                        "strategy_score": 88,
                        "payout": 82,
                        "trade_allowed": True,
                    }
                )
            return 200, main.build_success(
                {
                    "symbol": symbol,
                    "signal": "WAIT",
                    "direction": "WAIT",
                    "confidence": 0,
                    "strategy_score": 0,
                    "payout": None,
                    "trade_allowed": False,
                }
            )

        with (
            patch.object(main, "ANALYSIS_ASSETS", ["EURUSD-OTC", "GBPUSD-OTC", "USDJPY-OTC"]),
            patch.object(main, "ACTIVE_DATA_TIMEOUT_SECONDS", 0.05),
            patch.object(main, "analyze_active_signal", side_effect=fake_analyze_active_signal),
            self.assertLogs("backend-gateway", level="INFO") as logs,
        ):
            status_code, payload = await main.scan_local_signals(
                "resilient-user",
                limit=10,
                include_wait=False,
                max_assets=10,
            )

        self.assertEqual(status_code, 200)
        self.assertTrue(payload["ok"])
        self.assertEqual(len(payload["data"]), 1)
        self.assertEqual(payload["data"][0]["symbol"], "GBPUSD-OTC")
        output = "\n".join(logs.output)
        self.assertIn("[ACTIVE_TIMEOUT]", output)
        self.assertIn("[ASSET_SCORE]", output)


if __name__ == "__main__":
    unittest.main()
