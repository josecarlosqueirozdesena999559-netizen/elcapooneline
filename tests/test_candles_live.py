import json
import asyncio
import unittest
from typing import Any
from unittest.mock import AsyncMock, patch

from backend import main


class CandlesLiveTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        main.chart_candles_cache.clear()

    def tearDown(self) -> None:
        main.chart_candles_cache.clear()

    async def test_bullex_candles_bypasses_gateway_cache_for_chart_realtime(self) -> None:
        service = AsyncMock(
            return_value=(
                200,
                main.build_success(
                    {
                        "server_time": 125.0,
                        "candles": [
                            {"from": 120, "open": 1.2, "max": 1.4, "min": 1.1, "close": 1.35}
                        ],
                    }
                ),
            )
        )

        with patch.object(main, "call_bullex_service", new=service):
            response = await main.bullex_candles(
                symbol="EURUSD-OTC",
                timeframe="M1",
                auth={"user_id": "chart-realtime-user"},
            )

        self.assertEqual(response.status_code, 200)
        service.assert_awaited_once()
        self.assertTrue(service.await_args.kwargs["force_refresh"])

    async def test_bullex_candles_returns_current_forming_candle(self) -> None:
        calls: list[tuple[str, dict[str, Any] | None]] = []

        async def fake_bullex(method: str, path: str, user_id: str, **kwargs: Any) -> tuple[int, dict[str, Any]]:
            calls.append((path, kwargs.get("params")))
            if path == "/sessions/status":
                return 200, main.build_success({"connected": True, "active_mode": "PRACTICE", "server_time": 125.0})
            if path == "/candles":
                return 200, main.build_success(
                    {
                        "server_time": 125.0,
                        "candles": [
                            {"from": 0, "open": 1.0, "max": 1.2, "min": 0.9, "close": 1.1, "volume": 10},
                            {"from": 60, "open": 1.1, "max": 1.3, "min": 1.0, "close": 1.25, "volume": 12},
                        ],
                    }
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
        self.assertEqual(payload["active"], "EURUSD-OTC")
        self.assertEqual(payload["timeframe"], "M1")
        self.assertEqual(payload["interval"], 60)
        self.assertEqual(payload["count"], 3)
        self.assertEqual(payload["limit"], 60)
        self.assertEqual(payload["server_time"], 125.0)
        self.assertEqual(payload["candles"][-1]["time"], 120)
        self.assertEqual(payload["candles"][-1]["close"], 1.25)
        self.assertFalse(payload["candles"][-1]["is_closed"])
        self.assertFalse(payload["from_cache"])
        self.assertEqual(calls[0][1]["active"], "EURUSD-OTC")
        self.assertEqual(calls[0][1]["interval"], 60)
        self.assertEqual(calls[0][1]["count"], 60)

    async def test_bullex_candles_marks_returned_current_candle_as_open(self) -> None:
        async def fake_bullex(method: str, path: str, user_id: str, **kwargs: Any) -> tuple[int, dict[str, Any]]:
            if path == "/sessions/status":
                return 200, main.build_success({"connected": True, "active_mode": "PRACTICE", "server_time": 130.0})
            if path == "/candles":
                return 200, main.build_success(
                    {
                        "server_time": 130.0,
                        "candles": [
                            {"from": 120, "open": 1.2, "max": 1.4, "min": 1.1, "close": 1.35, "volume": 5}
                        ],
                    }
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
        self.assertEqual(payload["active"], "EURUSD-OTC")
        self.assertEqual(payload["timeframe"], "M1")
        self.assertEqual(payload["interval"], 60)
        self.assertEqual(payload["count"], 1)
        self.assertEqual(len(payload["candles"]), 1)
        self.assertEqual(payload["candles"][0]["time"], 120)
        self.assertFalse(payload["candles"][0]["is_closed"])

    async def test_debug_candles_live_reports_realtime_status(self) -> None:
        async def fake_bullex(method: str, path: str, user_id: str, **kwargs: Any) -> tuple[int, dict[str, Any]]:
            if path == "/sessions/status":
                return 200, main.build_success({"connected": True, "active_mode": "PRACTICE", "server_time": 125.0})
            if path == "/candles":
                return 200, main.build_success(
                    {
                        "server_time": 125.0,
                        "candles": [
                            {"from": 120, "open": 1.2, "max": 1.4, "min": 1.1, "close": 1.35, "volume": 5}
                        ],
                    }
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
        self.assertEqual(payload["timeframe"], "M1")
        self.assertEqual(payload["last_candle_time"], 120)
        self.assertEqual(payload["last_close"], 1.35)
        self.assertEqual(payload["server_time"], 125.0)
        self.assertEqual(payload["age_seconds"], 5.0)
        self.assertTrue(payload["is_realtime"])

    async def test_failure_returns_last_valid_cache_with_http_200(self) -> None:
        success = main.build_success(
            {
                "server_time": 125.0,
                "candles": [
                    {"from": 120, "open": 1.2, "max": 1.4, "min": 1.1, "close": 1.35, "volume": 5}
                ],
            }
        )
        service = AsyncMock(
            side_effect=[
                (200, success),
                (503, main.build_error("BULLEX_SERVICE_UNAVAILABLE")),
            ]
        )

        with patch.object(main, "call_bullex_service", new=service):
            first = await main.bullex_candles(
                active="GBPJPY-OTC",
                timeframe="M1",
                count=80,
                auth={"user_id": "chart-cache-user"},
            )
            second = await main.bullex_candles(
                active="GBPJPY-OTC",
                timeframe="M1",
                count=80,
                auth={"user_id": "chart-cache-user"},
            )

        first_payload = json.loads(first.body)
        second_payload = json.loads(second.body)
        self.assertEqual(first.status_code, 200)
        self.assertFalse(first_payload["data"]["from_cache"])
        self.assertEqual(second.status_code, 200)
        self.assertTrue(second_payload["ok"])
        self.assertTrue(second_payload["data"]["from_cache"])
        self.assertEqual(second_payload["data"]["candles"], first_payload["data"]["candles"])
        self.assertIsNone(second_payload["error"])

    async def test_failure_without_cache_returns_controlled_http_200(self) -> None:
        state = main.auto_trader.get("chart-failure-user")
        state.connected = True
        state.active_mode = "PRACTICE"
        session_cache = main.get_session_cache("chart-failure-user")

        with patch.object(
            main,
            "call_bullex_service",
            new=AsyncMock(return_value=(503, main.build_error("BULLEX_SERVICE_UNAVAILABLE"))),
        ):
            response = await main.bullex_candles(
                active="GBPJPY-OTC",
                timeframe="M1",
                count=80,
                auth={"user_id": "chart-failure-user"},
            )

        payload = json.loads(response.body)
        self.assertEqual(response.status_code, 200)
        self.assertFalse(payload["ok"])
        self.assertEqual(
            payload["data"],
            {"candles": [], "from_cache": False},
        )
        self.assertEqual(payload["error"], "CANDLES_TEMPORARY_UNAVAILABLE")
        self.assertTrue(state.connected)
        self.assertEqual(session_cache.failure_count, 0)
        self.assertIsNone(session_cache.next_retry_at)

    async def test_none_payload_is_normalized_without_attribute_error(self) -> None:
        with patch.object(
            main,
            "call_bullex_service",
            new=AsyncMock(return_value=(503, None)),
        ):
            response = await main.bullex_candles(
                active="GBPJPY-OTC",
                timeframe="M1",
                count=80,
                auth={"user_id": "chart-none-payload"},
            )

        payload = json.loads(response.body)
        self.assertEqual(response.status_code, 200)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"], "CANDLES_TEMPORARY_UNAVAILABLE")

    async def test_timeout_is_handled_within_five_seconds(self) -> None:
        async def slow_service(*_args: Any, **_kwargs: Any) -> tuple[int, dict[str, Any]]:
            await asyncio.sleep(1)
            return 200, main.build_success([])

        with (
            patch.object(main, "CANDLES_REQUEST_TIMEOUT_SECONDS", 0.01),
            patch.object(main, "call_bullex_service", new=AsyncMock(side_effect=slow_service)),
            self.assertLogs("backend-gateway", level="WARNING") as logs,
        ):
            response = await main.bullex_candles(
                active="GBPJPY-OTC",
                timeframe="M1",
                count=80,
                auth={"user_id": "chart-timeout-user"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn("[CANDLES_TIMEOUT_HANDLED]", "\n".join(logs.output))

    async def test_chart_rejects_assets_outside_the_ten_allowed_without_upstream(self) -> None:
        with patch.object(main, "call_bullex_service", new=AsyncMock()) as service:
            response = await main.bullex_candles(
                active="NZDUSD-OTC",
                timeframe="M1",
                count=80,
                auth={"user_id": "chart-asset-user"},
            )

        payload = json.loads(response.body)
        self.assertEqual(response.status_code, 200)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"], "CANDLES_TEMPORARY_UNAVAILABLE")
        service.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
