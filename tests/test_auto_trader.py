import asyncio
import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

from backend.auto_trader import (
    AutoTrader,
    STATUS_ACCOUNT_DISCONNECTED,
    STATUS_ORDER_REJECTED,
    STATUS_PENDING_RESULT,
    STATUS_SENDING_ORDER,
    STATUS_STOPPED,
    STATUS_WAITING_NEXT_CYCLE,
    utc_now,
)
from backend import main


SERVER_TIME_M1_OPEN = 56.0


def make_cycle_due(user_id: str) -> None:
    main.auto_trader.get(user_id).next_cycle_at = utc_now() - timedelta(seconds=1)


class AutoTraderStateTests(unittest.TestCase):
    def test_disabled_robot_never_opens_cycle(self) -> None:
        trader = AutoTrader()

        can_run, state = trader.prepare_cycle("user-disabled")

        self.assertFalse(can_run)
        self.assertEqual(state.status, STATUS_STOPPED)

    def test_cycle_is_reserved_for_ten_minutes(self) -> None:
        trader = AutoTrader()
        trader.start("user-cycle")
        trader.get("user-cycle").next_cycle_at = utc_now() - timedelta(seconds=1)

        first_run, state = trader.prepare_cycle("user-cycle")
        self.assertTrue(first_run)
        self.assertEqual(state.status, "ANALYZING")
        second_run, waiting_state = trader.prepare_cycle("user-cycle")

        self.assertFalse(second_run)
        self.assertEqual(waiting_state.status, STATUS_WAITING_NEXT_CYCLE)
        self.assertGreater(waiting_state.next_cycle_at, utc_now() + timedelta(minutes=9))

    def test_start_delays_first_cycle_for_full_cycle_minutes(self) -> None:
        trader = AutoTrader()
        state = trader.start("user-delayed-start")

        payload = state.to_dict()

        self.assertTrue(state.enabled)
        self.assertEqual(state.status, STATUS_WAITING_NEXT_CYCLE)
        self.assertIsNone(state.pending_signal)
        self.assertIsNone(state.last_signal)
        self.assertIsNone(state.rejection_reason)
        self.assertGreaterEqual(payload["seconds_until_next_cycle"], 599)
        self.assertLessEqual(payload["seconds_until_next_cycle"], 600)

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

    def test_waiting_entry_window_keeps_complete_time_contract(self) -> None:
        trader = AutoTrader()
        state = trader.start("user-waiting-contract")
        state.timeframe = "M5"
        trader.set_pending_signal(
            "user-waiting-contract",
            {
                "symbol": "EURUSD-OTC",
                "signal": "CALL",
                "confidence": 92,
                "payout": 88,
            },
        )
        trader.update_entry_window(
            "user-waiting-contract",
            main.get_entry_window("M5", 120.0),
        )

        payload = state.to_dict()

        self.assertEqual(payload["status"], "WAITING_ENTRY_WINDOW")
        self.assertIn("seconds_until_next_cycle", payload)
        self.assertEqual(payload["seconds_until_entry_window"], 170)
        self.assertEqual(payload["expiration_seconds"], 300)
        self.assertFalse(payload["entry_window_open"])
        self.assertFalse(payload["operation_in_progress"])
        self.assertEqual(payload["pending_signal"]["symbol"], "EURUSD-OTC")
        self.assertEqual(payload["pending_signal"]["signal"], "CALL")
        self.assertEqual(payload["pending_signal"]["direction"], "CALL")
        self.assertEqual(payload["pending_signal"]["confidence"], 92)
        self.assertEqual(payload["pending_signal"]["payout"], 88)
        self.assertIsNone(payload["last_trade"])

    def test_open_operation_returns_real_remaining_expiration(self) -> None:
        trader = AutoTrader()
        state = trader.start("user-open-expiration")
        state.timeframe = "M5"
        sent_at = utc_now() - timedelta(seconds=42)
        trader.record_trade(
            "user-open-expiration",
            {
                "order_id": "open-1",
                "sent_at": sent_at.isoformat(),
            },
        )

        payload = state.to_dict()

        self.assertTrue(payload["operation_in_progress"])
        self.assertEqual(payload["last_trade"]["result"], "PENDING_RESULT")
        self.assertGreaterEqual(payload["expiration_seconds"], 257)
        self.assertLessEqual(payload["expiration_seconds"], 258)
        self.assertFalse(payload["result_waiting"])

    def test_expired_open_operation_waits_for_final_result(self) -> None:
        trader = AutoTrader()
        state = trader.start("user-result-waiting")
        expired_at = utc_now() - timedelta(seconds=2)
        trader.record_trade(
            "user-result-waiting",
            {
                "order_id": "waiting-result-1",
                "expected_expire_at": expired_at.isoformat(),
                "result": STATUS_PENDING_RESULT,
            },
        )

        payload = state.to_dict()
        can_run, waiting_state = trader.prepare_cycle("user-result-waiting")

        self.assertEqual(payload["status"], STATUS_PENDING_RESULT)
        self.assertEqual(payload["expiration_seconds"], 0)
        self.assertTrue(payload["result_waiting"])
        self.assertTrue(payload["operation_in_progress"])
        self.assertFalse(can_run)
        self.assertEqual(waiting_state.status, STATUS_PENDING_RESULT)

    def test_restore_keeps_pending_signal_waiting_for_entry_window(self) -> None:
        trader = AutoTrader()
        state = trader.start("user-pending-restore")
        trader.set_pending_signal(
            "user-pending-restore",
            {
                "symbol": "EURUSD-OTC",
                "signal": "CALL",
                "confidence": 93,
                "payout": 90,
            },
        )

        restored = AutoTrader().restore("user-pending-restore", state.to_dict())

        self.assertEqual(restored.status, "WAITING_ENTRY_WINDOW")
        self.assertEqual(restored.pending_signal["symbol"], "EURUSD-OTC")
        self.assertEqual(restored.pending_signal["signal"], "CALL")
        self.assertEqual(restored.last_signal, restored.pending_signal)

    def test_restore_rebuilds_next_cycle_from_finished_at(self) -> None:
        finished_at = utc_now() - timedelta(minutes=2)
        trader = AutoTrader()

        restored = trader.restore(
            "user-finished-restore",
            {
                "enabled": True,
                "cycle_minutes": 10,
                "last_entry_at": (finished_at - timedelta(minutes=1)).isoformat(),
                "last_trade": {
                    "order_id": "finished-1",
                    "result": "WIN",
                    "finished_at": finished_at.isoformat(),
                },
                "status": "WAITING_NEXT_CYCLE",
                "entry_window_open": True,
            },
        )

        self.assertEqual(restored.status, "WAITING_NEXT_CYCLE")
        self.assertFalse(restored.entry_window_open)
        self.assertEqual(
            restored.next_cycle_at,
            finished_at + timedelta(minutes=10),
        )
        self.assertGreaterEqual(restored.to_dict()["seconds_until_next_cycle"], 479)
        self.assertLessEqual(restored.to_dict()["seconds_until_next_cycle"], 480)

    def test_restore_uses_last_entry_when_finished_at_is_missing(self) -> None:
        last_entry_at = utc_now() - timedelta(minutes=3)
        trader = AutoTrader()

        restored = trader.restore(
            "user-entry-restore",
            {
                "enabled": True,
                "cycle_minutes": 10,
                "last_entry_at": last_entry_at.isoformat(),
                "last_trade": {"order_id": "open-without-finished", "result": "LOSS"},
            },
        )

        self.assertEqual(
            restored.next_cycle_at,
            last_entry_at + timedelta(minutes=10),
        )


class AutoTraderCycleTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        main.auto_trader = AutoTrader()

    async def test_robot_state_after_result_returns_next_cycle_contract(self) -> None:
        user_id = "user-result-state"
        state = main.auto_trader.start(user_id)
        state.cycle_minutes = 10
        main.auto_trader.record_trade(
            user_id,
            {
                "order_id": "result-state-1",
                "active": "EURUSD-OTC",
                "direction": "CALL",
                "amount": 2,
                "result": "PENDING_RESULT",
            },
        )
        main.auto_trader.finish_trade(user_id, "result-state-1", "WIN", 1.76)

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
                                "active_mode": "PRACTICE",
                                "server_time": SERVER_TIME_M1_OPEN,
                            }
                        ),
                    )
                ),
            ),
            patch.object(main, "sync_user_store_from_payload"),
        ):
            response = await main.robot_state({"user_id": user_id})

        payload = json.loads(response.body)["data"]
        self.assertEqual(payload["status"], "WAITING_NEXT_CYCLE")
        self.assertEqual(payload["last_trade"]["result"], "WIN")
        self.assertIsNotNone(payload["last_trade"]["finished_at"])
        self.assertFalse(payload["operation_in_progress"])
        self.assertFalse(payload["entry_window_open"])
        self.assertGreaterEqual(payload["seconds_until_next_cycle"], 599)
        self.assertLessEqual(payload["seconds_until_next_cycle"], 600)

    async def test_demo_sends_at_most_one_order_per_cycle(self) -> None:
        user_id = "user-demo"
        main.auto_trader.start(user_id)
        make_cycle_due(user_id)
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

    async def test_start_does_not_operate_immediately(self) -> None:
        user_id = "user-start-waits"

        with (
            patch.object(main, "persist_robot"),
            patch.object(main, "ensure_robot_worker"),
            self.assertLogs("backend-gateway", level="INFO") as logs,
        ):
            response = await main.robot_start({"user_id": user_id})

        payload = json.loads(response.body)["data"]
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["status"], STATUS_WAITING_NEXT_CYCLE)
        self.assertGreaterEqual(payload["seconds_until_next_cycle"], 599)
        self.assertLessEqual(payload["seconds_until_next_cycle"], 600)
        self.assertIn("[ROBOT_START_DELAYED]", "\n".join(logs.output))

        with (
            patch.object(main, "call_bullex_service", new=AsyncMock()) as service_call,
            patch.object(main, "scan_local_signals", new=AsyncMock()) as scan,
        ):
            status_code, tick_payload = await main.execute_robot_cycle(user_id)

        self.assertEqual(status_code, 200)
        self.assertEqual(tick_payload["data"]["status"], STATUS_WAITING_NEXT_CYCLE)
        service_call.assert_not_awaited()
        scan.assert_not_awaited()

    async def test_order_status_is_sending_while_bullex_order_is_in_flight(self) -> None:
        user_id = "user-sending"
        main.auto_trader.start(user_id)
        make_cycle_due(user_id)
        order_started = asyncio.Event()
        release_order = asyncio.Event()

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
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
                order_started.set()
                await release_order.wait()
                return 200, main.build_success({"order_id": "sending-1"})
            raise AssertionError(f"unexpected path: {path}")

        scan_payload = main.build_success(
            [{"symbol": "EURUSD-OTC", "signal": "CALL", "confidence": 92, "strength": 80}]
        )

        with (
            patch.object(main, "call_bullex_service", side_effect=fake_bullex),
            patch.object(main, "scan_local_signals", new=AsyncMock(return_value=(200, scan_payload))),
            patch.object(main.trade_result_monitor, "start", return_value=True),
        ):
            task = asyncio.create_task(main.execute_robot_cycle(user_id))
            await asyncio.wait_for(order_started.wait(), timeout=1)
            sending_payload = main.auto_trader.get(user_id).to_dict()
            release_order.set()
            status_code, payload = await task

        self.assertEqual(sending_payload["status"], STATUS_SENDING_ORDER)
        self.assertFalse(sending_payload["operation_in_progress"])
        self.assertEqual(status_code, 200)
        self.assertEqual(payload["data"]["status"], STATUS_PENDING_RESULT)
        self.assertTrue(payload["data"]["operation_in_progress"])

    async def test_successful_order_goes_to_pending_result_with_last_trade_contract(self) -> None:
        user_id = "user-order-success"
        state = main.auto_trader.start(user_id)
        state.entry_value = 3
        make_cycle_due(user_id)

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
            if path == "/sessions/status":
                return 200, main.build_success(
                    {
                        "connected": True,
                        "active_mode": "PRACTICE",
                        "server_time": SERVER_TIME_M1_OPEN,
                    }
                )
            if path == "/payouts":
                return 200, main.build_success([{"symbol": "EURUSD-OTC", "payout": 91}])
            if path == "/orders/buy-demo":
                return 200, main.build_success({"order_id": "success-1"})
            raise AssertionError(f"unexpected path: {path}")

        scan_payload = main.build_success(
            [{"symbol": "EURUSD-OTC", "signal": "PUT", "confidence": 94, "strength": 80}]
        )

        with (
            patch.object(main, "call_bullex_service", side_effect=fake_bullex),
            patch.object(main, "scan_local_signals", new=AsyncMock(return_value=(200, scan_payload))),
            patch.object(main.trade_result_monitor, "start", return_value=True),
            self.assertLogs("backend-gateway", level="INFO") as logs,
        ):
            status_code, payload = await main.execute_robot_cycle(user_id)

        trade = payload["data"]["last_trade"]
        self.assertEqual(status_code, 200)
        self.assertEqual(payload["data"]["status"], STATUS_PENDING_RESULT)
        self.assertTrue(payload["data"]["operation_in_progress"])
        self.assertEqual(trade["order_id"], "success-1")
        self.assertEqual(trade["active"], "EURUSD-OTC")
        self.assertEqual(trade["direction"], "PUT")
        self.assertEqual(trade["amount"], 3)
        self.assertIsNotNone(trade["sent_at"])
        self.assertEqual(trade["expiration"], "M1")
        self.assertEqual(trade["result"], STATUS_PENDING_RESULT)
        self.assertEqual(trade["server_time_at_send"], "1970-01-01T00:00:56+00:00")
        self.assertEqual(trade["server_timestamp_at_send"], SERVER_TIME_M1_OPEN)
        self.assertEqual(trade["expiration_source"], "server_time_aligned")
        self.assertEqual(
            trade["expected_expire_at"],
            datetime.fromtimestamp(61, timezone.utc).isoformat(),
        )
        output = "\n".join(logs.output)
        self.assertIn("[ORDER_SEND_START]", output)
        self.assertIn("[EXPIRATION_SET]", output)
        self.assertIn("[ORDER_SEND_SUCCESS]", output)

    async def test_bullex_returned_expiration_is_used_as_primary_source(self) -> None:
        user_id = "user-order-expiration-source"
        main.auto_trader.start(user_id)
        make_cycle_due(user_id)
        returned_expiration = datetime.fromtimestamp(123, timezone.utc).isoformat()

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
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
                return 200, main.build_success(
                    {
                        "order_id": "source-1",
                        "close_time": returned_expiration,
                    }
                )
            raise AssertionError(f"unexpected path: {path}")

        scan_payload = main.build_success(
            [{"symbol": "EURUSD-OTC", "signal": "CALL", "confidence": 92, "strength": 80}]
        )

        with (
            patch.object(main, "call_bullex_service", side_effect=fake_bullex),
            patch.object(main, "scan_local_signals", new=AsyncMock(return_value=(200, scan_payload))),
            patch.object(main.trade_result_monitor, "start", return_value=True),
        ):
            status_code, payload = await main.execute_robot_cycle(user_id)

        trade = payload["data"]["last_trade"]
        self.assertEqual(status_code, 200)
        self.assertEqual(trade["expected_expire_at"], returned_expiration)
        self.assertEqual(trade["expiration_source"], "close_time")

    async def test_rejected_order_stays_order_rejected_and_clears_pending_signal(self) -> None:
        user_id = "user-order-rejected"
        main.auto_trader.start(user_id)
        make_cycle_due(user_id)

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
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
                return 409, main.build_error("MARKET_CLOSED")
            raise AssertionError(f"unexpected path: {path}")

        scan_payload = main.build_success(
            [{"symbol": "EURUSD-OTC", "signal": "CALL", "confidence": 92, "strength": 80}]
        )

        with (
            patch.object(main, "call_bullex_service", side_effect=fake_bullex),
            patch.object(main, "scan_local_signals", new=AsyncMock(return_value=(200, scan_payload))),
            patch.object(main.trade_result_monitor, "start", return_value=True) as monitor,
            self.assertLogs("backend-gateway", level="ERROR") as logs,
        ):
            status_code, payload = await main.execute_robot_cycle(user_id)

        self.assertEqual(status_code, 409)
        self.assertEqual(payload["data"]["status"], STATUS_ORDER_REJECTED)
        self.assertEqual(payload["data"]["rejection_reason"], "MARKET_CLOSED")
        self.assertIsNone(payload["data"]["pending_signal"])
        self.assertFalse(payload["data"]["operation_in_progress"])
        self.assertNotEqual(payload["data"]["status"], STATUS_WAITING_NEXT_CYCLE)
        monitor.assert_not_called()
        self.assertIn("[ORDER_SEND_FAILED]", "\n".join(logs.output))

    async def test_non_whitelisted_asset_never_operates(self) -> None:
        user_id = "user-apple"
        main.auto_trader.start(user_id)
        make_cycle_due(user_id)
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
        state = main.auto_trader.start(user_id)
        make_cycle_due(user_id)
        main.auto_trader.set_pending_signal(
            user_id,
            {
                "symbol": "EURUSD-OTC",
                "signal": "CALL",
                "confidence": 92,
                "payout": 90,
            },
        )
        state.blocked_filters = ["OLD_FILTER"]
        state.quality_score = 88

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
        self.assertEqual(payload["data"]["status"], STATUS_ACCOUNT_DISCONNECTED)
        self.assertEqual(payload["data"]["rejection_reason"], "ACCOUNT_DISCONNECTED")
        self.assertIsNone(payload["data"]["pending_signal"])
        self.assertIsNone(payload["data"]["last_signal"])
        self.assertFalse(payload["data"]["operation_in_progress"])
        self.assertFalse(payload["data"]["entry_window_open"])
        self.assertEqual(payload["data"]["blocked_filters"], [])
        self.assertEqual(payload["data"]["quality_score"], 0)
        scan.assert_not_awaited()

    async def test_robot_state_disconnected_clears_pending_signal(self) -> None:
        user_id = "user-state-disconnected"
        state = main.auto_trader.start(user_id)
        main.auto_trader.set_pending_signal(
            user_id,
            {
                "symbol": "EURUSD-OTC",
                "signal": "PUT",
                "confidence": 92,
                "payout": 90,
            },
        )
        state.entry_window_open = True
        state.quality_score = 91

        with (
            patch.object(
                main,
                "call_bullex_service",
                new=AsyncMock(
                    return_value=(
                        200,
                        main.build_success({"connected": False, "active_mode": "PRACTICE"}),
                    )
                ),
            ),
            patch.object(main, "sync_user_store_from_payload"),
        ):
            response = await main.robot_state({"user_id": user_id})

        payload = json.loads(response.body)["data"]
        self.assertEqual(response.status_code, 200)
        self.assertFalse(payload["connected"])
        self.assertEqual(payload["status"], STATUS_ACCOUNT_DISCONNECTED)
        self.assertEqual(payload["rejection_reason"], "ACCOUNT_DISCONNECTED")
        self.assertEqual(payload["seconds_until_next_cycle"], 0)
        self.assertIsNone(payload["pending_signal"])
        self.assertIsNone(payload["last_signal"])

    async def test_real_is_locked_by_default(self) -> None:
        user_id = "user-real-locked"
        state = main.auto_trader.start(user_id)
        state.account_mode = "REAL"
        make_cycle_due(user_id)

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
        make_cycle_due(user_id)
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
            patch.object(main.trade_result_monitor, "start", return_value=True) as monitor,
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
        monitor.assert_called_once_with(user_id, "real-1")

    async def test_pending_signal_waits_then_sends_without_reanalysis(self) -> None:
        user_id = "user-window-wait"
        main.auto_trader.start(user_id)
        make_cycle_due(user_id)
        calls = []
        status_times = iter((20.0, 20.0, 56.0))

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
            if path == "/orders/buy-demo":
                return 200, main.build_success({"order_id": "pending-window-1"})
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
            self.assertLogs("backend-gateway", level="INFO") as logs,
        ):
            first_status, first_payload = await main.execute_robot_cycle(user_id)
            second_status, second_payload = await main.execute_robot_cycle(user_id)

        pending = first_payload["data"]["pending_signal"]
        self.assertEqual(first_status, 200)
        self.assertEqual(first_payload["data"]["status"], "WAITING_ENTRY_WINDOW")
        self.assertEqual(pending["symbol"], "EURUSD-OTC")
        self.assertEqual(pending["signal"], "CALL")
        self.assertEqual(pending["direction"], "CALL")
        self.assertEqual(pending["confidence"], 92)
        self.assertEqual(pending["payout"], 90.0)
        self.assertEqual(pending["timeframe"], "M1")
        self.assertIsNotNone(pending["created_at"])
        self.assertEqual(first_payload["data"]["seconds_until_entry_window"], 35)
        self.assertFalse(first_payload["data"]["entry_window_open"])
        self.assertEqual(first_payload["data"]["expiration_seconds"], 60)
        self.assertEqual(second_status, 200)
        self.assertEqual(second_payload["data"]["status"], "PENDING_RESULT")
        self.assertIsNone(second_payload["data"]["pending_signal"])
        self.assertEqual(second_payload["data"]["last_trade"]["order_id"], "pending-window-1")
        self.assertEqual(scan.await_count, 1)
        self.assertEqual(calls.count("/orders/buy-demo"), 1)
        output = "\n".join(logs.output)
        self.assertIn("[PENDING_SIGNAL_SET]", output)
        self.assertIn("[ENTRY_WINDOW_WAIT]", output)
        self.assertIn("[ENTRY_WINDOW_OPEN]", output)
        self.assertIn("[TRADE_SENT_AT]", output)
        self.assertIn("[PENDING_SIGNAL_CLEARED]", output)

    async def test_missed_entry_window_clears_pending_signal_and_reanalyzes(self) -> None:
        user_id = "user-window-missed"
        state = main.auto_trader.start(user_id)
        main.auto_trader.set_pending_signal(
            user_id,
            {
                "symbol": "EURUSD-OTC",
                "signal": "PUT",
                "confidence": 91,
                "payout": 89,
            },
        )
        state.seconds_until_entry_window = 1

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
            if path == "/sessions/status":
                return 200, main.build_success(
                    {"connected": True, "active_mode": "PRACTICE", "server_time": 1.0}
                )
            raise AssertionError(f"unexpected path: {path}")

        with (
            patch.object(main, "call_bullex_service", side_effect=fake_bullex),
            patch.object(main, "scan_local_signals", new=AsyncMock()) as scan,
            self.assertLogs("backend-gateway", level="INFO") as logs,
        ):
            status_code, payload = await main.execute_robot_cycle(user_id)

        self.assertEqual(status_code, 200)
        self.assertEqual(payload["data"]["status"], "ANALYZING")
        self.assertIsNone(payload["data"]["pending_signal"])
        self.assertIsNone(payload["data"]["last_signal"])
        self.assertEqual(payload["data"]["seconds_until_next_cycle"], 0)
        scan.assert_not_awaited()
        self.assertIn("[PENDING_SIGNAL_CLEARED]", "\n".join(logs.output))

    async def test_m5_sends_five_minute_expiration(self) -> None:
        user_id = "user-m5"
        state = main.auto_trader.start(user_id)
        state.timeframe = "M5"
        make_cycle_due(user_id)
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
        make_cycle_due(user_id)
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
