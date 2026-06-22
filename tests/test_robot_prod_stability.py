import unittest
from datetime import timedelta
from unittest.mock import AsyncMock, patch

from backend.auto_trader import AutoTrader, RobotConfigUpdate, utc_now
from bullexapi.ws.client import WebsocketClient


class DummyApi:
    wss_url = "ws://example.invalid/ws"


class RobotProdStabilityTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.main = None

    def get_main(self):
        if self.main is None:
            from backend import main
            self.main = main
        return self.main

    def test_websocket_on_close_accepts_four_arguments(self) -> None:
        client = WebsocketClient(DummyApi())
        client.on_close(None, 1000, "closed", "extra")

    def test_max_entries_per_cycle_and_martingale_steps_are_configurable(self) -> None:
        trader = AutoTrader()
        state = trader.update_config(
            "user-config",
            RobotConfigUpdate(max_entries_per_cycle=3, martingale_steps=2),
        )

        self.assertEqual(state.max_entries_per_cycle, 3)
        self.assertEqual(state.martingale_steps, 2)

    def test_cycle_limit_status_and_reset_after_result_window(self) -> None:
        trader = AutoTrader()
        user_id = "user-cycle-limit"
        state = trader.start(user_id)
        state.connected = True
        state.ws_connected = True
        state.account_detected = True
        state.max_entries_per_cycle = 1
        trader.record_trade(
            user_id,
            {
                "order_id": "ord-1",
                "active": "EURUSD-OTC",
                "direction": "CALL",
                "amount": 5,
                "payout": 90,
            },
        )
        trader.finish_trade(user_id, "ord-1", "WIN", 4.5)

        state.result_display_until = utc_now() - timedelta(seconds=1)
        payload = state.to_dict()
        self.assertEqual(payload["status"], "ANALISANDO")
        self.assertEqual(payload["entries_used_in_cycle"], 0)

        state.entries_used_in_cycle = 1
        allowed, limited_state = trader.prepare_cycle(user_id)
        self.assertFalse(allowed)
        self.assertEqual(limited_state.to_dict()["status"], "LIMITE_CICLO_ATINGIDO")

    async def test_watchdog_reconnects_and_restores_sync(self) -> None:
        main = self.get_main()
        main.auto_trader = AutoTrader()
        user_id = "user-watchdog"
        state = main.auto_trader.start(user_id)
        state.connected = True
        state.ws_connected = False
        state.account_detected = False
        state.sync_started_at = utc_now() - timedelta(seconds=31)

        async def fake_bullex(method, path, request_user_id, json_body=None, params=None):
            self.assertEqual((method, path, request_user_id), ("POST", "/sessions/reconnect", user_id))
            return 200, main.build_success({"connected": True, "active_mode": "PRACTICE"})

        async def fake_fetch(request_user_id):
            self.assertEqual(request_user_id, user_id)
            state.connected = True
            state.ws_connected = True
            state.account_detected = True
            return 200, main.build_success({"connected": True}), state, True, "PRACTICE", "watchdog"

        with (
            patch.object(main, "call_bullex_service", new=AsyncMock(side_effect=fake_bullex)),
            patch.object(main, "fetch_and_sync_robot_connection", new=AsyncMock(side_effect=fake_fetch)),
        ):
            await main.run_robot_watchdog(user_id)

        refreshed = main.auto_trader.get(user_id)
        self.assertTrue(refreshed.ws_connected)
        self.assertTrue(refreshed.account_detected)
        self.assertIsNone(refreshed.sync_started_at)

    async def test_watchdog_throttles_reconnect_attempts_to_once_per_minute(self) -> None:
        main = self.get_main()
        main.auto_trader = AutoTrader()
        user_id = "user-watchdog-throttle"
        state = main.auto_trader.start(user_id)
        state.connected = True
        state.ws_connected = False
        state.account_detected = False
        state.sync_started_at = utc_now() - timedelta(seconds=31)
        state.last_reconnect_attempt_at = utc_now()

        with patch.object(main, "call_bullex_service", new=AsyncMock()) as service_call:
            await main.run_robot_watchdog(user_id)

        service_call.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
