import unittest
from datetime import timedelta
from unittest.mock import AsyncMock, patch

from backend.auto_trader import (
    AutoTrader,
    STATUS_STOPPED,
    STATUS_WAITING_NEXT_CYCLE,
    utc_now,
)
from backend import main


class AutoTraderStateTests(unittest.TestCase):
    def test_disabled_robot_never_opens_cycle(self) -> None:
        trader = AutoTrader()

        can_run, state = trader.prepare_cycle("user-disabled")

        self.assertFalse(can_run)
        self.assertEqual(state.status, STATUS_STOPPED)

    def test_cycle_is_reserved_for_ten_minutes(self) -> None:
        trader = AutoTrader()
        trader.start("user-cycle")

        first_run, state = trader.prepare_cycle("user-cycle")
        second_run, waiting_state = trader.prepare_cycle("user-cycle")

        self.assertTrue(first_run)
        self.assertEqual(state.status, STATUS_WAITING_NEXT_CYCLE)
        self.assertFalse(second_run)
        self.assertEqual(waiting_state.status, STATUS_WAITING_NEXT_CYCLE)
        self.assertGreater(waiting_state.next_cycle_at, utc_now() + timedelta(minutes=9))


class AutoTraderCycleTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        main.auto_trader = AutoTrader()

    async def test_demo_sends_at_most_one_order_per_cycle(self) -> None:
        user_id = "user-demo"
        main.auto_trader.start(user_id)
        calls = []

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
            calls.append((method, path, call_user_id, json_body, params))
            if path == "/sessions/status":
                return 200, main.build_success({"connected": True, "active_mode": "PRACTICE"})
            if path == "/payouts":
                return 200, main.build_success([{"symbol": "EURUSD-OTC", "payout": 90}])
            if path == "/orders/buy-demo":
                return 200, main.build_success({"order_id": "demo-1"})
            raise AssertionError(f"unexpected path: {path}")

        scan_payload = main.build_success(
            [
                {
                    "symbol": "EURUSD-OTC",
                    "signal": "CALL",
                    "confidence": 92,
                    "strength": 80,
                }
            ]
        )

        with (
            patch.object(main, "call_bullex_service", side_effect=fake_bullex),
            patch.object(main, "scan_local_signals", new=AsyncMock(return_value=(200, scan_payload))),
        ):
            first_status, first_payload = await main.execute_robot_cycle(user_id)
            second_status, second_payload = await main.execute_robot_cycle(user_id)

        orders = [call for call in calls if call[1] == "/orders/buy-demo"]
        self.assertEqual(first_status, 200)
        self.assertEqual(first_payload["data"]["status"], "PENDING_RESULT")
        self.assertEqual(second_status, 200)
        self.assertEqual(second_payload["data"]["status"], STATUS_WAITING_NEXT_CYCLE)
        self.assertEqual(len(orders), 1)

    async def test_non_whitelisted_asset_never_operates(self) -> None:
        user_id = "user-apple"
        main.auto_trader.start(user_id)
        calls = []

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
            calls.append(path)
            if path == "/sessions/status":
                return 200, main.build_success({"connected": True, "active_mode": "PRACTICE"})
            if path == "/payouts":
                return 200, main.build_success([{"symbol": "APPLE", "payout": 95}])
            raise AssertionError("an order must not be sent for APPLE")

        scan_payload = main.build_success(
            [{"symbol": "APPLE", "signal": "CALL", "confidence": 99, "strength": 99}]
        )

        with (
            patch.object(main, "call_bullex_service", side_effect=fake_bullex),
            patch.object(main, "scan_local_signals", new=AsyncMock(return_value=(200, scan_payload))),
        ):
            status_code, payload = await main.execute_robot_cycle(user_id)

        self.assertEqual(status_code, 200)
        self.assertEqual(payload["data"]["rejection_reason"], main.ASSET_NOT_ALLOWED)
        self.assertNotIn("/orders/buy-demo", calls)
        self.assertNotIn("/orders/buy-real", calls)

    async def test_disconnected_account_does_not_scan_or_order(self) -> None:
        user_id = "user-disconnected"
        main.auto_trader.start(user_id)

        with (
            patch.object(
                main,
                "call_bullex_service",
                new=AsyncMock(
                    return_value=(
                        409,
                        {"ok": False, "data": {"connected": False}, "error": "SESSION_DISCONNECTED"},
                    )
                ),
            ),
            patch.object(main, "scan_local_signals", new=AsyncMock()) as scan,
        ):
            status_code, payload = await main.execute_robot_cycle(user_id)

        self.assertEqual(status_code, 200)
        self.assertEqual(payload["data"]["rejection_reason"], "ACCOUNT_DISCONNECTED")
        scan.assert_not_awaited()

    async def test_real_is_locked_by_default(self) -> None:
        user_id = "user-real-locked"
        state = main.auto_trader.start(user_id)
        state.account_mode = "REAL"

        with patch.object(main, "call_bullex_service", new=AsyncMock()) as service_call:
            status_code, payload = await main.execute_robot_cycle(
                user_id,
                required_mode="REAL",
                confirm_real_header=True,
            )

        self.assertEqual(status_code, 403)
        self.assertEqual(payload["error"], main.REAL_TRADING_LOCKED)
        service_call.assert_not_awaited()

    async def test_confirmed_real_sends_at_most_one_order_per_cycle(self) -> None:
        user_id = "user-real"
        state = main.auto_trader.start(user_id)
        state.account_mode = "REAL"
        state.allow_real = True
        state.confirm_real = True
        state.entry_value = min(2, main.config.robot_real_max_entry)
        calls = []

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
            calls.append((method, path, json_body))
            if path == "/sessions/status":
                return 200, main.build_success({"connected": True, "active_mode": "REAL"})
            if path == "/payouts":
                return 200, main.build_success([{"symbol": "EURUSD-OTC", "payout": 90}])
            if path == "/orders/buy-real":
                return 200, main.build_success({"order_id": "real-1"})
            raise AssertionError(f"unexpected path: {path}")

        scan_payload = main.build_success(
            [{"symbol": "EURUSD-OTC", "signal": "PUT", "confidence": 93, "strength": 81}]
        )

        with (
            patch.object(main, "call_bullex_service", side_effect=fake_bullex),
            patch.object(main, "scan_local_signals", new=AsyncMock(return_value=(200, scan_payload))),
        ):
            first_status, _ = await main.execute_robot_cycle(
                user_id,
                required_mode="REAL",
                confirm_real_header=True,
            )
            second_status, second_payload = await main.execute_robot_cycle(
                user_id,
                required_mode="REAL",
                confirm_real_header=True,
            )

        orders = [call for call in calls if call[1] == "/orders/buy-real"]
        self.assertEqual(first_status, 200)
        self.assertEqual(second_status, 200)
        self.assertEqual(second_payload["data"]["status"], STATUS_WAITING_NEXT_CYCLE)
        self.assertEqual(len(orders), 1)
        self.assertTrue(orders[0][2]["confirm_real"])
