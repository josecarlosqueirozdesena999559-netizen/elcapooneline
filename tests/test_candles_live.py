import json
import unittest
from typing import Any
from unittest.mock import AsyncMock, patch

from backend import main


class CandlesLiveTests(unittest.IsolatedAsyncioTestCase):
    async def test_bullex_candles_returns_current_forming_candle(self) -> None:
        calls: list[tuple[str, dict[str, Any] | None]] = []

        async def fake_bullex(method: str, path: str, user_id: str, **kwargs: Any) -> tuple[int, dict[str, Any]]:
            calls.append((path, kwargs.get("params")))
            if path == "/sessions/status":
                return 200, main.build_success({"connected": True, "active_mode": "PRACTICE", "server_time": 125.0})
            if path == "/candles":
                return 200, main.build_success(
                    [
                        {"from": 0, "open": 1.0, "max": 1.2, "min": 0.9, "close": 1.1, "volume": 10},
                        {"from": 60, "open": 1.1, "max": 1.3, "min": 1.0, "close": 1.25, "volume": 12},
                    ]
                )
            raise AssertionError(f"unexpected path {path}")

        with patch.object(main, "call_bullex_service", new=AsyncMock(side_effect=fake_bullex)):
            response = await main.bullex_candles(
                symbol="EURUSD-OTC",
                timeframe="M1",
                limit=60,
                auth={"user_id": "user-live"},
            )

        payload = json.loads(response.body)["data"]
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["symbol"], "EURUSD-OTC")
        self.assertEqual(payload["timeframe"], "M1")
        self.assertEqual(payload["server_time"], 125.0)
        self.assertEqual(payload["candles"][-1]["time"], 120)
        self.assertEqual(payload["candles"][-1]["close"], 1.25)
        self.assertFalse(payload["candles"][-1]["is_closed"])
        self.assertEqual(calls[1][1]["active"], "EURUSD-OTC")
        self.assertEqual(calls[1][1]["interval"], 60)
        self.assertEqual(calls[1][1]["count"], 60)

    async def test_bullex_candles_marks_returned_current_candle_as_open(self) -> None:
        async def fake_bullex(method: str, path: str, user_id: str, **kwargs: Any) -> tuple[int, dict[str, Any]]:
            if path == "/sessions/status":
                return 200, main.build_success({"connected": True, "active_mode": "PRACTICE", "server_time": 130.0})
            if path == "/candles":
                return 200, main.build_success(
                    [{"from": 120, "open": 1.2, "max": 1.4, "min": 1.1, "close": 1.35, "volume": 5}]
                )
            raise AssertionError(f"unexpected path {path}")

        with patch.object(main, "call_bullex_service", new=AsyncMock(side_effect=fake_bullex)):
            response = await main.bullex_candles(
                active="eurusd-otc",
                interval=60,
                count=10,
                auth={"user_id": "user-live"},
            )

        payload = json.loads(response.body)["data"]
        self.assertEqual(payload["symbol"], "EURUSD-OTC")
        self.assertEqual(payload["timeframe"], "M1")
        self.assertEqual(len(payload["candles"]), 1)
        self.assertEqual(payload["candles"][0]["time"], 120)
        self.assertFalse(payload["candles"][0]["is_closed"])

    async def test_debug_candles_live_reports_realtime_status(self) -> None:
        async def fake_bullex(method: str, path: str, user_id: str, **kwargs: Any) -> tuple[int, dict[str, Any]]:
            if path == "/sessions/status":
                return 200, main.build_success({"connected": True, "active_mode": "PRACTICE", "server_time": 125.0})
            if path == "/candles":
                return 200, main.build_success(
                    [{"from": 120, "open": 1.2, "max": 1.4, "min": 1.1, "close": 1.35, "volume": 5}]
                )
            raise AssertionError(f"unexpected path {path}")

        with patch.object(main, "call_bullex_service", new=AsyncMock(side_effect=fake_bullex)):
            response = await main.debug_candles_live(
                symbol="EURUSD-OTC",
                timeframe="M1",
                auth={"user_id": "user-live"},
            )

        payload = json.loads(response.body)["data"]
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["symbol"], "EURUSD-OTC")
        self.assertEqual(payload["last_candle_time"], 120)
        self.assertEqual(payload["last_close"], 1.35)
        self.assertEqual(payload["server_time"], 125.0)
        self.assertEqual(payload["age_seconds"], 5.0)
        self.assertTrue(payload["is_realtime"])


if __name__ == "__main__":
    unittest.main()
