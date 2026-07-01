import asyncio
import unittest
from datetime import timedelta
from unittest.mock import patch

from backend import main


class RobotMarketDataResilienceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        main.active_cooldowns.clear()
        main.payout_cooldowns.clear()
        main.session_response_cache.clear()

    def tearDown(self) -> None:
        main.active_cooldowns.clear()
        main.payout_cooldowns.clear()
        main.session_response_cache.clear()

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

    async def test_batch_timeout_does_not_cooldown_every_active(self) -> None:
        async def fake_analyze_active_signal(
            user_id: str,
            symbol: str,
            timeframe: str = "M1",
            endtime: int | None = None,
            strategy_mode: str = "conservative",
        ):
            await asyncio.sleep(1)

        assets = ["EURUSD-OTC", "GBPUSD-OTC", "USDJPY-OTC"]
        with (
            patch.object(main, "ANALYSIS_ASSETS", assets),
            patch.object(main, "ACTIVE_DATA_TIMEOUT_SECONDS", 0.02),
            patch.object(main, "analyze_active_signal", side_effect=fake_analyze_active_signal),
            self.assertLogs("backend-gateway", level="INFO") as logs,
        ):
            status_code, payload = await main.scan_local_signals(
                "batch-timeout-user",
                limit=10,
                include_wait=False,
                max_assets=10,
            )

        self.assertEqual(status_code, 200)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["data"], [])
        self.assertEqual(main.active_cooldowns.get("batch-timeout-user"), None)
        output = "\n".join(logs.output)
        self.assertIn("[ANALYSIS_BATCH_TIMEOUT]", output)

    async def test_active_cooldown_uses_cached_candles_and_payout(self) -> None:
        user_id = "cache-cooldown-user"
        symbol = "EURUSD-OTC"
        now = main.utc_now()
        cache = main.get_session_cache(user_id)
        candle_params = {
            "active": symbol,
            "interval": main.TIMEFRAME_SECONDS["M1"],
            "count": main.ROBOT_CANDLE_COUNT,
            "endtime": 60,
        }
        payout_params = {"active": symbol}
        candles_payload = main.build_success(
            [{"open": 1.0, "close": 1.1}, {"open": 1.1, "close": 1.2}]
        )
        payout_payload = main.build_success([{"symbol": symbol, "payout": 88}])
        cache.last_successful_responses[main.build_cache_key("/candles", candle_params)] = (
            main.BullexResponseCacheEntry(200, candles_payload, now + timedelta(seconds=60))
        )
        cache.last_successful_responses[main.build_cache_key("/payouts", payout_params)] = (
            main.BullexResponseCacheEntry(200, payout_payload, now + timedelta(seconds=60))
        )
        main.set_named_cooldown(
            main.active_cooldowns,
            user_id,
            symbol,
            seconds=15,
            log_label="ACTIVE_TIMEOUT",
            status=main.STATUS_ACTIVE_COOLDOWN,
            reason="ACTIVE_TIMEOUT",
        )
        main.set_named_cooldown(
            main.payout_cooldowns,
            user_id,
            symbol,
            seconds=15,
            log_label="PAYOUT_TIMEOUT",
            status=main.STATUS_PAYOUT_COOLDOWN,
            reason="PAYOUT_TIMEOUT",
        )

        with (
            patch.object(main, "call_bullex_service", side_effect=AssertionError("should use cache")),
            patch.object(
                main,
                "analyze_signal",
                return_value={
                    "symbol": symbol,
                    "signal": "CALL",
                    "direction": "CALL",
                    "confidence": 86,
                    "strategy_score": 86,
                    "payout": 88,
                    "trade_allowed": True,
                },
            ),
            self.assertLogs("backend-gateway", level="INFO") as logs,
        ):
            status_code, payload = await main.analyze_active_signal(
                user_id,
                symbol,
                timeframe="M1",
                endtime=60,
            )

        self.assertEqual(status_code, 200)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["data"]["symbol"], symbol)
        self.assertEqual(payload["data"]["signal"], "CALL")
        self.assertTrue(payload["data"]["trade_allowed"])
        self.assertTrue(payload["data"]["from_cache"])
        output = "\n".join(logs.output)
        self.assertIn("[ACTIVE_CACHE]", output)


if __name__ == "__main__":
    unittest.main()
