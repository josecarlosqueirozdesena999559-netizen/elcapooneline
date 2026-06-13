import json
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


SERVER_TIME_M1_OPEN = 56.0


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

    def test_entry_windows_match_each_timeframe(self) -> None:
        cases = {
            "M1": (55, 54),
            "M5": (290, 289),
            "M15": (880, 879),
            "M30": (1770, 1769),
        }
        for timeframe, (open_at, closed_at) in cases.items():
            with self.subTest(timeframe=timeframe):
                self.assertTrue(main.get_entry_window(timeframe, open_at)["entry_window_open"])
                closed = main.get_entry_window(timeframe, closed_at)
                self.assertFalse(closed["entry_window_open"])
                self.assertEqual(closed["seconds_until_entry_window"], 1)

    def test_entry_window_closes_with_less_than_one_second_remaining(self) -> None:
        window = main.get_entry_window("M1", 59.1)

        self.assertFalse(window["entry_window_open"])
        self.assertEqual(window["seconds_until_entry_window"], 56)


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
                return 200, main.build_success(
                    {
                        "connected": True,
                        "active_mode": "PRACTICE",
                        "server_time": SERVER_TIME_M1_OPEN,
                    }
                )
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
            patch.object(main.trade_result_monitor, "start", return_value=True),
        ):
            first_status, first_payload = await main.execute_robot_cycle(user_id)
            second_status, second_payload = await main.execute_robot_cycle(user_id)

        orders = [call for call in calls if call[1] == "/orders/buy-demo"]
        self.assertEqual(first_status, 200)
        self.assertEqual(first_payload["data"]["status"], "PENDING_RESULT")
        self.assertEqual(second_status, 200)
        self.assertEqual(second_payload["data"]["status"], "PENDING_RESULT")
        self.assertEqual(len(orders), 1)

    async def test_non_whitelisted_asset_never_operates(self) -> None:
        user_id = "user-apple"
        main.auto_trader.start(user_id)
        calls = []

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
            calls.append(path)
            if path == "/sessions/status":
                return 200, main.build_success(
                    {
                        "connected": True,
                        "active_mode": "PRACTICE",
                        "server_time": SERVER_TIME_M1_OPEN,
                    }
                )
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

        with patch.object(
            main,
            "call_bullex_service",
            new=AsyncMock(
                return_value=(
                    200,
                    main.build_success({"connected": True, "active_mode": "REAL"}),
                )
            ),
        ) as service_call:
            status_code, payload = await main.execute_robot_cycle(
                user_id,
                required_mode="REAL",
            )

        self.assertEqual(status_code, 403)
        self.assertEqual(payload["error"], "ALLOW_REAL_REQUIRED")
        service_call.assert_awaited_once()

    async def test_real_without_confirm_real_is_blocked(self) -> None:
        user_id = "user-real-no-confirm"
        state = main.auto_trader.get(user_id)
        state.account_mode = "REAL"
        state.allow_real = True
        state.confirm_real = False

        with (
            patch.object(
                main,
                "call_bullex_service",
                new=AsyncMock(
                    return_value=(
                        200,
                        main.build_success(
                            {
                                "connected": True,
                                "active_mode": "REAL",
                                "server_time": SERVER_TIME_M1_OPEN,
                            }
                        ),
                    )
                ),
            ),
            patch.object(main, "persist_robot"),
            patch.object(main, "ensure_robot_worker") as ensure_worker,
        ):
            response = await main.robot_start({"user_id": user_id})

        payload = json.loads(response.body)
        self.assertEqual(response.status_code, 403)
        self.assertEqual(payload["error"], "CONFIRM_REAL_REQUIRED")
        self.assertFalse(state.enabled)
        ensure_worker.assert_not_called()

    async def test_real_with_practice_active_mode_is_blocked(self) -> None:
        user_id = "user-real-practice"
        state = main.auto_trader.get(user_id)
        state.account_mode = "REAL"
        state.allow_real = True
        state.confirm_real = True

        with (
            patch.object(
                main,
                "call_bullex_service",
                new=AsyncMock(
                    return_value=(
                        200,
                        main.build_success({"connected": True, "active_mode": "PRACTICE"}),
                    )
                ),
            ),
            patch.object(main, "persist_robot"),
            patch.object(main, "ensure_robot_worker") as ensure_worker,
        ):
            response = await main.robot_start({"user_id": user_id})

        payload = json.loads(response.body)
        self.assertEqual(response.status_code, 403)
        self.assertEqual(payload["error"], "BULLEX_ACTIVE_MODE_NOT_REAL")
        self.assertFalse(state.enabled)
        ensure_worker.assert_not_called()

    async def test_confirmed_real_start_is_allowed(self) -> None:
        user_id = "user-real-start"
        state = main.auto_trader.get(user_id)
        state.account_mode = "REAL"
        state.allow_real = True
        state.confirm_real = True
        state.entry_value = min(2, main.config.robot_real_max_entry)

        with (
            patch.object(
                main,
                "call_bullex_service",
                new=AsyncMock(
                    return_value=(
                        200,
                        main.build_success(
                            {
                                "connected": True,
                                "active_mode": "REAL",
                                "server_time": SERVER_TIME_M1_OPEN,
                            }
                        ),
                    )
                ),
            ),
            patch.object(main, "persist_robot"),
            patch.object(main, "ensure_robot_worker") as ensure_worker,
        ):
            response = await main.robot_start({"user_id": user_id})

        self.assertEqual(response.status_code, 200)
        self.assertTrue(state.enabled)
        ensure_worker.assert_called_once_with(user_id)

    async def test_real_entry_above_limit_is_blocked(self) -> None:
        user_id = "user-real-over-limit"
        state = main.auto_trader.get(user_id)
        state.account_mode = "REAL"
        state.allow_real = True
        state.confirm_real = True
        state.entry_value = main.config.robot_real_max_entry + 0.01

        with (
            patch.object(
                main,
                "call_bullex_service",
                new=AsyncMock(
                    return_value=(
                        200,
                        main.build_success({"connected": True, "active_mode": "REAL"}),
                    )
                ),
            ),
            patch.object(main, "persist_robot"),
            patch.object(main, "ensure_robot_worker") as ensure_worker,
        ):
            response = await main.robot_start({"user_id": user_id})

        payload = json.loads(response.body)
        self.assertEqual(response.status_code, 403)
        self.assertEqual(payload["error"], "REAL_ENTRY_VALUE_EXCEEDS_MAX")
        self.assertFalse(state.enabled)
        ensure_worker.assert_not_called()

    async def test_robot_state_reports_real_readiness(self) -> None:
        user_id = "user-real-ready"
        state = main.auto_trader.get(user_id)
        state.account_mode = "REAL"
        state.allow_real = True
        state.confirm_real = True

        with (
            patch.object(
                main,
                "call_bullex_service",
                new=AsyncMock(
                    return_value=(
                        200,
                        main.build_success({"connected": True, "active_mode": "REAL"}),
                    )
                ),
            ),
            patch.object(main, "sync_user_store_from_payload"),
        ):
            response = await main.robot_state({"user_id": user_id})

        data = json.loads(response.body)["data"]
        self.assertTrue(data["allow_real"])
        self.assertTrue(data["confirm_real"])
        self.assertEqual(data["account_mode"], "REAL")
        self.assertEqual(data["active_mode"], "REAL")
        self.assertTrue(data["real_ready"])
        self.assertIsNone(data["real_block_reason"])

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
                return 200, main.build_success(
                    {
                        "connected": True,
                        "active_mode": "REAL",
                        "server_time": SERVER_TIME_M1_OPEN,
                    }
                )
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
            )
            second_status, second_payload = await main.execute_robot_cycle(
                user_id,
                required_mode="REAL",
            )

        orders = [call for call in calls if call[1] == "/orders/buy-real"]
        self.assertEqual(first_status, 200)
        self.assertEqual(second_status, 200)
        self.assertEqual(second_payload["data"]["status"], "PENDING_RESULT")
        self.assertEqual(len(orders), 1)
        self.assertTrue(orders[0][2]["confirm_real"])

    async def test_outside_entry_window_does_not_send_order(self) -> None:
        user_id = "user-window-wait"
        main.auto_trader.start(user_id)
        calls = []

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
            calls.append(path)
            if path == "/sessions/status":
                return 200, main.build_success(
                    {"connected": True, "active_mode": "PRACTICE", "server_time": 20.0}
                )
            raise AssertionError(f"unexpected path: {path}")

        with (
            patch.object(main, "call_bullex_service", side_effect=fake_bullex),
            patch.object(main, "scan_local_signals", new=AsyncMock()) as scan,
        ):
            status_code, payload = await main.execute_robot_cycle(user_id)

        self.assertEqual(status_code, 200)
        self.assertEqual(payload["data"]["status"], "WAITING_ENTRY_WINDOW")
        self.assertEqual(payload["data"]["rejection_reason"], "WAITING_ENTRY_WINDOW")
        self.assertEqual(payload["data"]["seconds_until_entry_window"], 35)
        self.assertNotIn("/orders/buy-demo", calls)
        scan.assert_not_awaited()

    async def test_m5_sends_five_minute_expiration(self) -> None:
        user_id = "user-m5"
        state = main.auto_trader.start(user_id)
        state.timeframe = "M5"
        calls = []

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
            calls.append((path, json_body))
            if path == "/sessions/status":
                return 200, main.build_success(
                    {"connected": True, "active_mode": "PRACTICE", "server_time": 295.0}
                )
            if path == "/payouts":
                return 200, main.build_success([{"symbol": "EURUSD-OTC", "payout": 90}])
            if path == "/orders/buy-demo":
                return 200, main.build_success({"order_id": "demo-m5"})
            raise AssertionError(f"unexpected path: {path}")

        scan_payload = main.build_success(
            [{"symbol": "EURUSD-OTC", "signal": "CALL", "confidence": 92, "strength": 80}]
        )
        with (
            patch.object(main, "call_bullex_service", side_effect=fake_bullex),
            patch.object(
                main,
                "scan_local_signals",
                new=AsyncMock(return_value=(200, scan_payload)),
            ) as scan,
            patch.object(main.trade_result_monitor, "start", return_value=True),
        ):
            status_code, payload = await main.execute_robot_cycle(user_id)

        orders = [body for path, body in calls if path == "/orders/buy-demo"]
        self.assertEqual(status_code, 200)
        self.assertEqual(payload["data"]["last_trade"]["expiration"], "M5")
        self.assertEqual(orders[0]["expiration"], 5)
        self.assertEqual(scan.await_args.kwargs["timeframe"], "M5")
        self.assertEqual(scan.await_args.kwargs["endtime"], 295)

    async def test_window_closing_during_analysis_blocks_late_order(self) -> None:
        user_id = "user-window-closing"
        main.auto_trader.start(user_id)
        status_times = iter((56.0, 59.5))
        calls = []

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
            calls.append(path)
            if path == "/sessions/status":
                return 200, main.build_success(
                    {
                        "connected": True,
                        "active_mode": "PRACTICE",
                        "server_time": next(status_times),
                    }
                )
            if path == "/payouts":
                return 200, main.build_success([{"symbol": "EURUSD-OTC", "payout": 90}])
            raise AssertionError(f"late order reached unexpected path: {path}")

        scan_payload = main.build_success(
            [{"symbol": "EURUSD-OTC", "signal": "CALL", "confidence": 92, "strength": 80}]
        )
        with (
            patch.object(main, "call_bullex_service", side_effect=fake_bullex),
            patch.object(main, "scan_local_signals", new=AsyncMock(return_value=(200, scan_payload))),
        ):
            status_code, payload = await main.execute_robot_cycle(user_id)

        self.assertEqual(status_code, 200)
        self.assertEqual(payload["data"]["status"], "WAITING_ENTRY_WINDOW")
        self.assertFalse(payload["data"]["entry_window_open"])
        self.assertNotIn("/orders/buy-demo", calls)
