import asyncio
import json
import unittest
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, patch

from backend.auto_trader import (
    AutoTrader,
    STATUS_ACCOUNT_DISCONNECTED,
    STATUS_ORDER_REJECTED,
    STATUS_PENDING_RESULT,
    STATUS_RESULT_RECEIVED,
    STATUS_SENDING_ORDER,
    STATUS_STOPPED,
    STATUS_WAITING_NEXT_CYCLE,
    utc_now,
)
from backend import main


SERVER_TIME_M1_OPEN = 25.0


def make_cycle_due(user_id: str) -> None:
    main.auto_trader.get(user_id).next_cycle_at = utc_now() - timedelta(seconds=1)


class AutoTraderStateTests(unittest.TestCase):
    def test_new_users_receive_independent_default_states(self) -> None:
        trader = AutoTrader()

        user_a = trader.update_config(
            "user-a",
            main.RobotConfigUpdate(entry_value=15),
        )
        user_b = trader.get("user-b")

        self.assertIsNot(user_a, user_b)
        self.assertEqual(user_a.entry_value, 15)
        self.assertEqual(user_b.entry_value, 2)
        self.assertEqual(user_b.cycle_minutes, 5)
        self.assertEqual(user_b.min_confidence, 94)
        self.assertEqual(user_b.min_payout, 88)
        self.assertEqual(user_b.stop_win, 50)
        self.assertEqual(user_b.stop_loss, 30)
        self.assertEqual(user_b.strategy_mode, "conservative")
        self.assertEqual(user_b.account_mode, "DEMO")
        self.assertFalse(user_b.enabled)
        self.assertEqual(trader.source("user-a"), "memory")
        self.assertEqual(trader.source("user-b"), "default")

    def test_updating_user_b_does_not_change_user_a(self) -> None:
        trader = AutoTrader()
        trader.update_config(
            "user-a",
            main.RobotConfigUpdate(entry_value=15, stop_loss=40),
        )

        trader.update_config("user-b", main.RobotConfigUpdate(stop_loss=12))

        self.assertEqual(trader.get("user-a").entry_value, 15)
        self.assertEqual(trader.get("user-a").stop_loss, 40)
        self.assertEqual(trader.get("user-b").entry_value, 2)
        self.assertEqual(trader.get("user-b").stop_loss, 12)

    def test_sending_order_never_falls_back_to_analyzing(self) -> None:
        trader = AutoTrader()
        trader.start("user-order-transition")
        trader.set_pending_signal(
            "user-order-transition",
            {
                "symbol": "EURUSD-OTC",
                "signal": "CALL",
                "confidence": 92,
                "payout": 90,
            },
        )

        sending = trader.start_sending_order("user-order-transition")
        sending_status = sending.status
        rejected = trader.reject_order("user-order-transition", "active suspended")

        self.assertEqual(sending_status, STATUS_SENDING_ORDER)
        self.assertEqual(rejected.status, STATUS_ORDER_REJECTED)
        self.assertNotEqual(rejected.status, "ANALYZING")
        self.assertFalse(rejected.operation_in_progress)

    def test_disabled_robot_never_opens_cycle(self) -> None:
        trader = AutoTrader()

        can_run, state = trader.prepare_cycle("user-disabled")

        self.assertFalse(can_run)
        self.assertEqual(state.status, STATUS_STOPPED)

    def test_cycle_is_reserved_for_five_minutes(self) -> None:
        trader = AutoTrader()
        trader.start("user-cycle")
        trader.get("user-cycle").next_cycle_at = utc_now() - timedelta(seconds=1)

        first_run, state = trader.prepare_cycle("user-cycle")
        self.assertTrue(first_run)
        self.assertEqual(state.status, "WAITING_ANALYSIS_WINDOW")
        second_run, waiting_state = trader.prepare_cycle("user-cycle")

        self.assertFalse(second_run)
        self.assertEqual(waiting_state.status, STATUS_WAITING_NEXT_CYCLE)
        self.assertGreater(waiting_state.next_cycle_at, utc_now() + timedelta(minutes=4))

    def test_second_2_recovers_analyzing_to_waiting_analysis_window(self) -> None:
        trader = AutoTrader()
        state = trader.start("user-second-2")
        state.connected = True
        state.status = "ANALYZING"
        state.analysis_result = "RUNNING"
        state.last_analysis_result = "RUNNING"

        trader.update_entry_window("user-second-2", main.get_entry_window("M1", 2.0))
        payload = state.to_dict()

        self.assertEqual(payload["status"], "WAITING_ANALYSIS_WINDOW")
        self.assertEqual(payload["analysis_result"], "WAITING_NEXT_ANALYSIS_WINDOW")
        self.assertEqual(payload["display_countdown_label"], "Análise em")
        self.assertEqual(payload["display_countdown_seconds"], 3)

    def test_second_7_allows_analyzing(self) -> None:
        trader = AutoTrader()
        state = trader.start("user-second-7")
        state.connected = True
        state.status = "WAITING_ANALYSIS_WINDOW"

        trader.update_entry_window("user-second-7", main.get_entry_window("M1", 7.0))
        trader.start_analysis("user-second-7")
        payload = state.to_dict()

        self.assertEqual(payload["status"], "ANALYZING")
        self.assertTrue(payload["analysis_window_open"])
        self.assertEqual(payload["display_countdown_seconds"], 0)

    def test_second_21_without_pending_waits_next_analysis_window(self) -> None:
        trader = AutoTrader()
        state = trader.start("user-second-21")
        state.connected = True
        state.status = "ANALYZING"
        state.analysis_result = "RUNNING"
        state.last_analysis_result = "RUNNING"

        trader.update_entry_window("user-second-21", main.get_entry_window("M1", 21.0))
        payload = state.to_dict()

        self.assertEqual(payload["status"], "WAITING_ANALYSIS_WINDOW")
        self.assertEqual(payload["analysis_result"], "WAITING_NEXT_ANALYSIS_WINDOW")
        self.assertEqual(payload["display_countdown_label"], "Análise em")
        self.assertEqual(payload["display_countdown_seconds"], 44)

    def test_display_countdown_seconds_is_always_an_integer(self) -> None:
        trader = AutoTrader()
        state = trader.start("user-countdown-integer")

        for status in ("WAITING_ANALYSIS_WINDOW", "ANALYZING", "STOPPED"):
            state.status = status
            payload = state.to_dict()
            self.assertIsInstance(payload["display_countdown_seconds"], int)
            self.assertGreaterEqual(payload["display_countdown_seconds"], 0)

    def test_start_delays_first_cycle_for_full_cycle_minutes(self) -> None:
        trader = AutoTrader()
        state = trader.start("user-delayed-start")

        payload = state.to_dict()

        self.assertTrue(state.enabled)
        self.assertEqual(state.status, STATUS_WAITING_NEXT_CYCLE)
        self.assertIsNone(state.pending_signal)
        self.assertIsNone(state.last_signal)
        self.assertIsNone(state.rejection_reason)
        self.assertIsNotNone(state.cycle_id)
        self.assertIsNotNone(payload["current_cycle_started_at"])
        self.assertGreaterEqual(payload["seconds_until_next_cycle"], 299)
        self.assertLessEqual(payload["seconds_until_next_cycle"], 300)
        self.assertEqual(payload["display_countdown_label"], "Próxima entrada em")
        self.assertGreaterEqual(payload["display_countdown_seconds"], 299)
        self.assertLessEqual(payload["display_countdown_seconds"], 300)

    def test_waiting_analysis_window_uses_analysis_countdown_for_display(self) -> None:
        trader = AutoTrader()
        state = trader.start("user-analysis-countdown")
        window = main.get_entry_window("M1", 30.0)

        trader.wait_analysis_window(
            "user-analysis-countdown",
            window,
            analysis_result="WAITING_NEXT_ANALYSIS_WINDOW",
            rejection_reason="WAITING_NEXT_ANALYSIS_WINDOW",
        )

        payload = state.to_dict()
        self.assertEqual(payload["status"], "WAITING_ANALYSIS_WINDOW")
        self.assertEqual(payload["seconds_until_analysis_window"], 35)
        self.assertEqual(payload["seconds_until_next_cycle"], 35)
        self.assertEqual(payload["display_countdown_label"], "Análise em")
        self.assertEqual(payload["display_countdown_seconds"], 35)

    def test_config_keeps_selected_five_minute_cycle(self) -> None:
        trader = AutoTrader()

        state = trader.update_config(
            "user-five-minute-config",
            main.RobotConfigUpdate(cycle_minutes=5),
        )

        self.assertEqual(state.cycle_minutes, 5)

    def test_start_never_reuses_previous_cycle(self) -> None:
        trader = AutoTrader()
        state = trader.start("user-new-cycle")
        first_cycle_id = state.cycle_id
        state.pending_signal = {"symbol": "EURUSD-OTC"}
        state.last_signal = {"symbol": "EURUSD-OTC"}
        state.rejection_reason = "OLD_REASON"
        state.current_cycle_started_at = utc_now() - timedelta(minutes=30)
        state.next_cycle_at = utc_now() - timedelta(minutes=20)

        restarted = trader.start("user-new-cycle")

        self.assertIsNotNone(restarted.cycle_id)
        self.assertNotEqual(restarted.cycle_id, first_cycle_id)
        self.assertIsNone(restarted.pending_signal)
        self.assertIsNone(restarted.last_signal)
        self.assertIsNone(restarted.rejection_reason)
        self.assertGreater(restarted.next_cycle_at, utc_now() + timedelta(minutes=4))

    def test_entry_windows_match_each_timeframe(self) -> None:
        cases = {
            "M1": (25, 24, 29, 30),
            "M5": (265, 264, 269, 270),
            "M15": (865, 864, 869, 870),
            "M30": (1765, 1764, 1769, 1770),
        }
        for timeframe, (open_at, before_at, end_at, missed_at) in cases.items():
            with self.subTest(timeframe=timeframe):
                self.assertTrue(main.get_entry_window(timeframe, open_at)["entry_window_open"])
                self.assertTrue(main.get_entry_window(timeframe, end_at)["entry_window_open"])
                before = main.get_entry_window(timeframe, before_at)
                self.assertFalse(before["entry_window_open"])
                self.assertEqual(before["seconds_until_entry_window"], 1)
                missed = main.get_entry_window(timeframe, missed_at)
                self.assertFalse(missed["entry_window_open"])
                self.assertTrue(missed["missed_entry_window"])
                self.assertEqual(missed["seconds_until_entry_window"], 0)

    def test_entry_window_contract_exposes_target_seconds(self) -> None:
        window = main.get_entry_window("M1", 24)

        self.assertFalse(window["entry_window_open"])
        self.assertEqual(window["seconds_until_entry_window"], 1)
        self.assertFalse(window["analysis_window_open"])
        self.assertEqual(window["seconds_until_analysis_window"], 41)
        self.assertEqual(window["analysis_window_start_second"], 5)
        self.assertEqual(window["analysis_window_end_second"], 20)
        self.assertEqual(window["entry_window_start_second"], 25)
        self.assertEqual(window["entry_window_end_second"], 29)
        self.assertEqual(window["buy_target_second"], 25)

    def test_analysis_window_contract_for_m1(self) -> None:
        window_at_10 = main.get_entry_window("M1", 10)
        window_at_20 = main.get_entry_window("M1", 20)
        window_at_30 = main.get_entry_window("M1", 30)

        self.assertTrue(window_at_10["analysis_window_open"])
        self.assertEqual(window_at_10["seconds_until_analysis_window"], 0)
        self.assertTrue(window_at_20["analysis_window_open"])
        self.assertFalse(window_at_30["analysis_window_open"])
        self.assertEqual(window_at_30["seconds_until_analysis_window"], 35)

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
        self.assertEqual(payload["seconds_until_entry_window"], 145)
        self.assertEqual(payload["display_countdown_label"], "Entrada em")
        self.assertEqual(payload["display_countdown_seconds"], 145)
        self.assertEqual(payload["entry_window_start_second"], 265)
        self.assertEqual(payload["entry_window_end_second"], 269)
        self.assertEqual(payload["buy_target_second"], 265)
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
        self.assertTrue(payload["result_waiting"])
        self.assertTrue(payload["show_expiration_countdown"])
        self.assertTrue(payload["operation_message"].startswith("Expira em "))
        self.assertNotEqual(payload["operation_message"], "Expira em 00:00")

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
        self.assertEqual(payload["operation_message"], "Aguardando resultado...")
        self.assertEqual(payload["expiration_display"], "Aguardando resultado...")
        self.assertFalse(payload["show_expiration_countdown"])
        self.assertNotEqual(payload["operation_message"], "Expira em 00:00")
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

    async def test_robot_config_and_state_are_isolated_by_user_id(self) -> None:
        with (
            patch.object(main, "persist_robot") as persist,
            patch.object(main, "ensure_robot_worker"),
            patch.object(main, "stop_robot_worker", new=AsyncMock()),
        ):
            response_a = await main.robot_config(
                main.RobotConfigUpdate(entry_value=15, stop_loss=40),
                {"user_id": "user-a"},
            )
            response_b = await main.robot_config(
                main.RobotConfigUpdate(stop_loss=12),
                {"user_id": "user-b"},
            )

        data_a = json.loads(response_a.body)["data"]
        data_b = json.loads(response_b.body)["data"]
        self.assertEqual(data_a["entry_value"], 15)
        self.assertEqual(data_a["stop_loss"], 40)
        self.assertEqual(data_b["entry_value"], 2)
        self.assertEqual(data_b["stop_loss"], 12)
        self.assertEqual(
            [call.args[0] for call in persist.call_args_list],
            ["user-a", "user-b"],
        )

        session_payload = main.build_success(
            {
                "connected": True,
                "active_mode": "PRACTICE",
                "server_time": SERVER_TIME_M1_OPEN,
            }
        )
        with (
            patch.object(
                main,
                "call_bullex_service",
                new=AsyncMock(return_value=(200, session_payload)),
            ),
            patch.object(main, "sync_user_store_from_payload"),
        ):
            state_a_response = await main.robot_state({"user_id": "user-a"})
            state_b_response = await main.robot_state({"user_id": "user-b"})

        state_a = json.loads(state_a_response.body)["data"]
        state_b = json.loads(state_b_response.body)["data"]
        self.assertNotEqual(state_a["entry_value"], state_b["entry_value"])
        self.assertNotEqual(state_a["stop_loss"], state_b["stop_loss"])

    async def test_user_isolation_debug_reports_current_user_only(self) -> None:
        main.auto_trader.update_config(
            "user-a",
            main.RobotConfigUpdate(entry_value=15, stop_win=90),
        )

    async def test_robot_settings_debug_returns_current_user_settings(self) -> None:
        main.auto_trader.update_config(
            "user-settings-a",
            main.RobotConfigUpdate(entry_value=15, min_confidence=97),
        )

        response = await main.debug_robot_settings(
            {"user_id": "user-settings-a"}
        )
        payload = json.loads(response.body)

        self.assertEqual(payload["user_id"], "user-settings-a")
        self.assertEqual(payload["source"], "memory")
        self.assertEqual(payload["settings"]["entry_value"], 15)
        self.assertEqual(payload["settings"]["min_confidence"], 97)
        self.assertNotIn("enabled", payload["settings"])

        response = await main.debug_user_isolation({"user_id": "user-b"})

        payload = json.loads(response.body)
        self.assertEqual(
            payload,
            {
                "user_id": "user-b",
                "has_state": True,
                "entry_value": 2.0,
                "stop_win": 50.0,
                "stop_loss": 30.0,
                "source": "default",
            },
        )

    async def test_robot_state_after_result_keeps_result_visible_for_five_seconds(self) -> None:
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
        self.assertEqual(payload["status"], STATUS_RESULT_RECEIVED)
        self.assertEqual(payload["last_trade"]["result"], "WIN")
        self.assertIsNotNone(payload["last_trade"]["finished_at"])
        self.assertIsNotNone(payload["result_received_at"])
        self.assertIsNotNone(payload["result_display_until"])
        self.assertFalse(payload["result_waiting"])
        self.assertFalse(payload["operation_in_progress"])
        self.assertFalse(payload["entry_window_open"])
        self.assertEqual(payload["seconds_until_next_cycle"], 0)

        state.result_display_until = utc_now() - timedelta(seconds=1)
        waiting_payload = state.to_dict()
        self.assertEqual(waiting_payload["status"], STATUS_WAITING_NEXT_CYCLE)
        self.assertGreaterEqual(waiting_payload["seconds_until_next_cycle"], 599)
        self.assertLessEqual(waiting_payload["seconds_until_next_cycle"], 600)

    async def test_demo_sends_at_most_one_order_per_cycle(self) -> None:
        user_id = "user-demo"
        main.auto_trader.start(user_id)
        main.auto_trader.set_pending_signal(
            user_id,
            {
                "symbol": "EURUSD-OTC",
                "signal": "CALL",
                "confidence": 94,
                "payout": 90,
            },
        )
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
            if path == "/orders/buy-demo":
                return 200, main.build_success({"order_id": "demo-1"})
            raise AssertionError(f"unexpected path: {path}")

        scan_payload = main.build_success(
            [
                {
                    "symbol": "EURUSD-OTC",
                    "signal": "CALL",
                    "confidence": 94,
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
        self.assertGreaterEqual(payload["seconds_until_next_cycle"], 299)
        self.assertLessEqual(payload["seconds_until_next_cycle"], 300)
        self.assertIn("[ROBOT_START_DELAYED]", "\n".join(logs.output))
        self.assertIn("[ROBOT_START_NEW_CYCLE]", "\n".join(logs.output))
        self.assertIsNotNone(payload["cycle_id"])

        with (
            patch.object(main, "call_bullex_service", new=AsyncMock()) as service_call,
            patch.object(main, "scan_local_signals", new=AsyncMock()) as scan,
        ):
            status_code, tick_payload = await main.execute_robot_cycle(user_id)

        self.assertEqual(status_code, 200)
        self.assertEqual(tick_payload["data"]["status"], STATUS_WAITING_NEXT_CYCLE)
        service_call.assert_not_awaited()
        scan.assert_not_awaited()

    async def test_due_cycle_runs_analysis_and_rejects_when_no_signal(self) -> None:
        user_id = "user-cycle-due-no-signal"
        state = main.auto_trader.start(user_id)
        state.cycle_minutes = 1
        make_cycle_due(user_id)

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
            if path == "/sessions/status":
                return 200, main.build_success(
                    {
                        "connected": True,
                        "active_mode": "PRACTICE",
                        "server_time": 20.0,
                    }
                )
            raise AssertionError(f"unexpected path: {path}")

        with (
            patch.object(main, "call_bullex_service", side_effect=fake_bullex),
            patch.object(main, "scan_local_signals", new=AsyncMock(return_value=(200, main.build_success([])))) as scan,
            self.assertLogs("backend-gateway", level="INFO") as logs,
        ):
            status_code, payload = await main.execute_robot_cycle(user_id)

        data = payload["data"]
        output = "\n".join(logs.output)
        self.assertEqual(status_code, 200)
        self.assertEqual(data["status"], "WAITING_ANALYSIS_WINDOW")
        self.assertEqual(data["rejection_reason"], "WAITING_NEXT_ANALYSIS_WINDOW")
        self.assertEqual(
            data["last_rejection_reason"],
            "CANDLES_UNAVAILABLE",
        )
        self.assertEqual(data["last_analysis_result"], "NO_CANDIDATE_THIS_CANDLE")
        self.assertEqual(data["analysis_result"], "NO_CANDIDATE_THIS_CANDLE")
        self.assertIsNotNone(data["last_analysis_at"])
        self.assertIsNone(data["pending_signal"])
        self.assertGreaterEqual(data["seconds_until_analysis_window"], 44)
        self.assertIn("[CYCLE_DUE]", output)
        self.assertIn("[ANALYSIS_STARTED]", output)
        self.assertIn("[ANALYSIS_FINISHED]", output)
        self.assertIn("[NO_CANDIDATE_THIS_CANDLE]", output)
        scan.assert_awaited_once()

    async def test_due_cycle_with_valid_signal_creates_pending_signal(self) -> None:
        user_id = "user-cycle-due-valid-signal"
        state = main.auto_trader.start(user_id)
        state.cycle_minutes = 1
        make_cycle_due(user_id)
        status_times = iter((20.0, 20.0))

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
            if path == "/sessions/status":
                return 200, main.build_success(
                    {
                        "connected": True,
                        "active_mode": "PRACTICE",
                        "server_time": next(status_times),
                    }
                )
            raise AssertionError(f"unexpected path: {path}")

        scan_payload = main.build_success(
            [
                {
                    "symbol": "EURUSD-OTC",
                    "signal": "CALL",
                    "confidence": 94,
                    "strength": 80,
                    "payout": 90,
                }
            ]
        )

        with (
            patch.object(main, "call_bullex_service", side_effect=fake_bullex),
            patch.object(main, "scan_local_signals", new=AsyncMock(return_value=(200, scan_payload))) as scan,
            self.assertLogs("backend-gateway", level="INFO") as logs,
        ):
            status_code, payload = await main.execute_robot_cycle(user_id)

        data = payload["data"]
        output = "\n".join(logs.output)
        self.assertEqual(status_code, 200)
        self.assertEqual(data["status"], "WAITING_ENTRY_WINDOW")
        self.assertEqual(data["analysis_window_start_second"], 5)
        self.assertEqual(data["analysis_window_end_second"], 20)
        self.assertEqual(data["entry_window_start_second"], 25)
        self.assertEqual(data["entry_window_end_second"], 29)
        self.assertEqual(data["current_candle_seconds"], 20.0)
        self.assertEqual(data["last_analysis_result"], "BEST_CANDIDATE_SELECTED")
        self.assertEqual(data["analysis_result"], "BEST_CANDIDATE_SELECTED")
        self.assertIsNotNone(data["analysis_started_at"])
        self.assertIsNotNone(data["last_analysis_at"])
        self.assertEqual(data["pending_signal"]["symbol"], "EURUSD-OTC")
        self.assertEqual(data["pending_signal"]["signal"], "CALL")
        self.assertTrue(data["pending_signal"]["strategy_name"].startswith("Confluência "))
        self.assertIsNotNone(data["pending_signal"]["strategy_reason"])
        self.assertIn("Payout", data["pending_signal"]["used_strategies"])
        self.assertEqual(data["candidates_count"], 1)
        self.assertEqual(data["best_candidate"]["symbol"], "EURUSD-OTC")
        self.assertEqual(data["strategy_score"], data["pending_signal"]["strategy_score"])
        self.assertIn("[CYCLE_DUE]", output)
        self.assertIn("[ANALYSIS_STARTED]", output)
        self.assertIn("[ANALYSIS_WINDOW_OPEN]", output)
        self.assertIn("[ANALYSIS_FINISHED]", output)
        self.assertIn("[ANALYSIS_CANDIDATES]", output)
        self.assertIn("[BEST_CANDIDATE_SELECTED]", output)
        self.assertIn("[PENDING_SIGNAL_SET]", output)
        scan.assert_awaited_once()

    async def test_due_cycle_analyzes_at_second_10_and_prepares_pending_signal(self) -> None:
        user_id = "user-analysis-window-open"
        state = main.auto_trader.start(user_id)
        state.cycle_minutes = 1
        make_cycle_due(user_id)
        status_times = iter((10.0, 10.0))

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
            if path == "/sessions/status":
                return 200, main.build_success(
                    {
                        "connected": True,
                        "active_mode": "PRACTICE",
                        "server_time": next(status_times),
                    }
                )
            raise AssertionError(f"unexpected path: {path}")

        scan_payload = main.build_success(
            [
                {
                    "symbol": "EURUSD-OTC",
                    "signal": "PUT",
                    "confidence": 95,
                    "strength": 81,
                    "payout": 90,
                }
            ]
        )

        with (
            patch.object(main, "call_bullex_service", side_effect=fake_bullex),
            patch.object(main, "scan_local_signals", new=AsyncMock(return_value=(200, scan_payload))) as scan,
        ):
            status_code, payload = await main.execute_robot_cycle(user_id)

        data = payload["data"]
        self.assertEqual(status_code, 200)
        self.assertEqual(data["status"], "WAITING_ENTRY_WINDOW")
        self.assertTrue(data["analysis_window_open"])
        self.assertEqual(data["seconds_until_analysis_window"], 0)
        self.assertEqual(data["seconds_until_entry_window"], 15)
        self.assertEqual(data["pending_signal"]["symbol"], "EURUSD-OTC")
        self.assertEqual(data["pending_signal"]["signal"], "PUT")
        scan.assert_awaited_once()

    async def test_missing_server_time_uses_vps_fallback_and_still_selects_candidate(self) -> None:
        user_id = "user-server-time-fallback"
        state = main.auto_trader.start(user_id)
        state.cycle_minutes = 1
        make_cycle_due(user_id)
        fallback_now = datetime.fromtimestamp(10, timezone.utc)

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
            if path == "/sessions/status":
                return 200, main.build_success(
                    {
                        "connected": True,
                        "active_mode": "PRACTICE",
                    }
                )
            raise AssertionError(f"unexpected path: {path}")

        scan_payload = main.build_success(
            [
                {
                    "symbol": "EURUSD-OTC",
                    "signal": "CALL",
                    "confidence": 95,
                    "strength": 82,
                    "payout": 90,
                }
            ]
        )

        with (
            patch.object(main, "utc_now", return_value=fallback_now),
            patch.object(main, "call_bullex_service", side_effect=fake_bullex),
            patch.object(main, "scan_local_signals", new=AsyncMock(return_value=(200, scan_payload))) as scan,
            self.assertLogs("backend-gateway", level="INFO") as logs,
        ):
            status_code, payload = await main.execute_robot_cycle(user_id)

        data = payload["data"]
        output = "\n".join(logs.output)
        self.assertEqual(status_code, 200)
        self.assertEqual(data["status"], "WAITING_ENTRY_WINDOW")
        self.assertEqual(data["server_time_source"], "vps_fallback")
        self.assertEqual(data["current_candle_seconds"], 10.0)
        self.assertEqual(data["pending_signal"]["symbol"], "EURUSD-OTC")
        self.assertEqual(data["best_candidate"]["symbol"], "EURUSD-OTC")
        self.assertIn("[SERVER_TIME_FALLBACK]", output)
        scan.assert_awaited_once()

    async def test_due_cycle_after_second_20_waits_next_analysis_window_without_scan(self) -> None:
        user_id = "user-analysis-window-missed"
        main.auto_trader.start(user_id)
        make_cycle_due(user_id)

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
            if path == "/sessions/status":
                return 200, main.build_success(
                    {
                        "connected": True,
                        "active_mode": "PRACTICE",
                        "server_time": 30.0,
                    }
                )
            raise AssertionError(f"unexpected path: {path}")

        with (
            patch.object(main, "call_bullex_service", side_effect=fake_bullex),
            patch.object(main, "scan_local_signals", new=AsyncMock()) as scan,
            self.assertLogs("backend-gateway", level="INFO") as logs,
        ):
            status_code, payload = await main.execute_robot_cycle(user_id)

        data = payload["data"]
        output = "\n".join(logs.output)
        self.assertEqual(status_code, 200)
        self.assertEqual(data["status"], "WAITING_ANALYSIS_WINDOW")
        self.assertEqual(data["rejection_reason"], "WAITING_NEXT_ANALYSIS_WINDOW")
        self.assertNotEqual(data["rejection_reason"], "MISSED_ENTRY_WINDOW")
        self.assertIsNone(data["pending_signal"])
        self.assertFalse(data["analysis_window_open"])
        self.assertEqual(data["seconds_until_analysis_window"], 35)
        self.assertEqual(data["current_candle_seconds"], 30.0)
        self.assertIn("[WAITING_ANALYSIS_WINDOW]", output)
        self.assertNotIn("[MISSED_ENTRY_WINDOW]", output)
        scan.assert_not_awaited()

    async def test_analysis_state_is_visible_while_scan_is_running(self) -> None:
        user_id = "user-analysis-running"
        main.auto_trader.start(user_id)
        make_cycle_due(user_id)
        scan_started = asyncio.Event()
        release_scan = asyncio.Event()

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
            if path == "/sessions/status":
                return 200, main.build_success(
                    {
                        "connected": True,
                        "active_mode": "PRACTICE",
                        "server_time": 20.0,
                    }
                )
            raise AssertionError(f"unexpected path: {path}")

        async def slow_scan(*args, **kwargs):
            scan_started.set()
            await release_scan.wait()
            return 200, main.build_success([])

        with (
            patch.object(main, "call_bullex_service", side_effect=fake_bullex),
            patch.object(main, "scan_local_signals", side_effect=slow_scan),
        ):
            task = asyncio.create_task(main.execute_robot_cycle(user_id))
            await asyncio.wait_for(scan_started.wait(), timeout=1)
            running = main.auto_trader.get(user_id).to_dict()
            release_scan.set()
            _, finished_payload = await task

        self.assertEqual(running["status"], "ANALYZING")
        self.assertEqual(running["last_analysis_result"], "RUNNING")
        self.assertEqual(
            running["analysis_message"],
            "Analisando mercado...",
        )
        self.assertEqual(finished_payload["data"]["status"], "WAITING_ANALYSIS_WINDOW")
        self.assertEqual(finished_payload["data"]["analysis_result"], "NO_CANDIDATE_THIS_CANDLE")
        self.assertEqual(
            finished_payload["data"]["last_rejection_reason"],
            "CANDLES_UNAVAILABLE",
        )
        self.assertGreaterEqual(
            finished_payload["data"]["seconds_until_analysis_window"],
            44,
        )

    async def test_running_analysis_over_10_seconds_recovers_with_timeout(self) -> None:
        user_id = "user-analysis-timeout"
        state = main.auto_trader.start(user_id)
        state.status = STATUS_WAITING_NEXT_CYCLE
        state.next_cycle_at = utc_now() - timedelta(seconds=1)
        state.analysis_result = "RUNNING"
        state.last_analysis_result = "RUNNING"
        state.analysis_started_at = utc_now() - timedelta(seconds=11)

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
                                "server_time": 15.0,
                            }
                        ),
                    )
                ),
            ),
            patch.object(main, "sync_user_store_from_payload"),
            patch.object(main, "persist_robot"),
            self.assertLogs("backend-gateway", level="INFO") as logs,
        ):
            response = await main.robot_state({"user_id": user_id})

        data = json.loads(response.body)["data"]
        output = "\n".join(logs.output)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data["status"], "WAITING_ANALYSIS_WINDOW")
        self.assertEqual(data["rejection_reason"], "ANALYSIS_TIMEOUT")
        self.assertEqual(data["analysis_result"], "ANALYSIS_TIMEOUT")
        self.assertIn("aguardando pr", data["last_rejection_reason"])
        self.assertGreaterEqual(data["seconds_until_analysis_window"], 49)
        self.assertIn("[ANALYSIS_TIMEOUT]", output)
        self.assertIn("[ANALYSIS_STATE_RECOVERED]", output)

    async def test_analysis_exception_schedules_next_cycle(self) -> None:
        user_id = "user-analysis-error"
        main.auto_trader.start(user_id)
        make_cycle_due(user_id)

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
            if path == "/sessions/status":
                return 200, main.build_success(
                    {"connected": True, "active_mode": "PRACTICE", "server_time": 20.0}
                )
            raise AssertionError(f"unexpected path: {path}")

        async def broken_scan(*args, **kwargs):
            raise RuntimeError("scan exploded")

        with (
            patch.object(main, "call_bullex_service", side_effect=fake_bullex),
            patch.object(main, "scan_local_signals", side_effect=broken_scan),
            self.assertLogs("backend-gateway", level="INFO") as logs,
        ):
            status_code, payload = await main.execute_robot_cycle(user_id)

        data = payload["data"]
        output = "\n".join(logs.output)
        self.assertEqual(status_code, 500)
        self.assertEqual(data["status"], "WAITING_ANALYSIS_WINDOW")
        self.assertEqual(data["rejection_reason"], "ANALYSIS_ERROR")
        self.assertEqual(data["analysis_result"], "ANALYSIS_ERROR")
        self.assertEqual(data["last_order_error"], "scan exploded")
        self.assertGreaterEqual(data["seconds_until_analysis_window"], 44)
        self.assertIn("[ANALYSIS_ERROR]", output)
        self.assertIn("[ANALYSIS_ERROR_RECOVERED]", output)

    async def test_waiting_next_cycle_running_result_never_returns_invalid_zero_state(self) -> None:
        user_id = "user-invalid-running-state"
        state = main.auto_trader.start(user_id)
        state.status = STATUS_WAITING_NEXT_CYCLE
        state.next_cycle_at = utc_now() - timedelta(seconds=1)
        state.analysis_result = "RUNNING"
        state.last_analysis_result = "RUNNING"
        state.analysis_started_at = utc_now()

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
                                "server_time": 30.0,
                            }
                        ),
                    )
                ),
            ),
            patch.object(main, "sync_user_store_from_payload"),
        ):
            response = await main.robot_state({"user_id": user_id})

        data = json.loads(response.body)["data"]
        self.assertEqual(response.status_code, 200)
        self.assertFalse(
            data["status"] == STATUS_WAITING_NEXT_CYCLE
            and data["seconds_until_next_cycle"] == 0
            and data["analysis_result"] == "RUNNING"
        )
        self.assertEqual(data["status"], "WAITING_ANALYSIS_WINDOW")
        self.assertEqual(data["analysis_result"], "WAITING_NEXT_ANALYSIS_WINDOW")
        self.assertIsNone(data["pending_signal"])
        self.assertIsNone(data["best_candidate"])

    async def test_analyzing_outside_window_recovers_to_waiting_analysis_window(self) -> None:
        user_id = "user-analyzing-outside-window"
        state = main.auto_trader.start(user_id)
        state.status = "ANALYZING"
        state.analysis_result = "RUNNING"
        state.last_analysis_result = "RUNNING"
        state.analysis_started_at = utc_now()
        state.best_candidate = None
        state.pending_signal = None

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
                                "server_time": 30.0,
                            }
                        ),
                    )
                ),
            ),
            patch.object(main, "sync_user_store_from_payload"),
            patch.object(main, "persist_robot"),
            self.assertLogs("backend-gateway", level="INFO") as logs,
        ):
            response = await main.robot_state({"user_id": user_id})

        data = json.loads(response.body)["data"]
        output = "\n".join(logs.output)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data["status"], "WAITING_ANALYSIS_WINDOW")
        self.assertEqual(data["analysis_result"], "WAITING_NEXT_ANALYSIS_WINDOW")
        self.assertEqual(data["seconds_until_analysis_window"], 35)
        self.assertIsNone(data["pending_signal"])
        self.assertIsNone(data["best_candidate"])
        self.assertIn("[WAITING_ANALYSIS_WINDOW]", output)
        self.assertIn("[ANALYSIS_STATE_RECOVERED]", output)

    async def test_pending_signal_blocks_new_analysis(self) -> None:
        user_id = "user-pending-blocks-analysis"
        main.auto_trader.start(user_id)
        main.auto_trader.set_pending_signal(
            user_id,
            {
                "symbol": "EURUSD-OTC",
                "signal": "CALL",
                "confidence": 92,
                "payout": 90,
            },
        )

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
            if path == "/sessions/status":
                return 200, main.build_success(
                    {
                        "connected": True,
                        "active_mode": "PRACTICE",
                        "server_time": 20.0,
                    }
                )
            raise AssertionError(f"unexpected path: {path}")

        with (
            patch.object(main, "call_bullex_service", side_effect=fake_bullex),
            patch.object(main, "scan_local_signals", new=AsyncMock()) as scan,
        ):
            status_code, payload = await main.execute_robot_cycle(user_id)

        self.assertEqual(status_code, 200)
        self.assertEqual(payload["data"]["status"], "WAITING_ENTRY_WINDOW")
        self.assertIsNotNone(payload["data"]["pending_signal"])
        scan.assert_not_awaited()

    async def test_pending_result_blocks_new_analysis(self) -> None:
        user_id = "user-result-blocks-analysis"
        state = main.auto_trader.start(user_id)
        main.auto_trader.record_trade(
            user_id,
            {
                "order_id": "pending-result-1",
                "active": "EURUSD-OTC",
                "direction": "CALL",
                "amount": 2,
                "result": "PENDING_RESULT",
            },
        )
        state.next_cycle_at = utc_now() - timedelta(seconds=1)

        with (
            patch.object(main, "call_bullex_service", new=AsyncMock()) as bullex,
            patch.object(main, "scan_local_signals", new=AsyncMock()) as scan,
        ):
            status_code, payload = await main.execute_robot_cycle(user_id)

        self.assertEqual(status_code, 200)
        self.assertEqual(payload["data"]["status"], "PENDING_RESULT")
        self.assertTrue(payload["data"]["operation_in_progress"])
        bullex.assert_not_awaited()
        scan.assert_not_awaited()

    async def test_configured_five_minute_cycle_is_used_on_start(self) -> None:
        user_id = "user-config-five"
        with (
            patch.object(main, "persist_robot") as persist,
            patch.object(main, "ensure_robot_worker"),
            patch.object(main, "stop_robot_worker", new=AsyncMock()),
            self.assertLogs("backend-gateway", level="INFO") as logs,
        ):
            config_response = await main.robot_config(
                main.RobotConfigUpdate(cycle_minutes=5),
                {"user_id": user_id},
            )
            start_response = await main.robot_start({"user_id": user_id})

        configured = json.loads(config_response.body)["data"]
        started = json.loads(start_response.body)["data"]
        self.assertEqual(configured["cycle_minutes"], 5)
        self.assertEqual(started["cycle_minutes"], 5)
        self.assertGreaterEqual(started["seconds_until_next_cycle"], 299)
        self.assertLessEqual(started["seconds_until_next_cycle"], 300)
        self.assertEqual([call.args[0] for call in persist.call_args_list], [user_id, user_id])
        output = "\n".join(logs.output)
        self.assertIn("[CYCLE_CONFIG]", output)
        self.assertIn("cycle_minutes=5", output)

    async def test_highest_strategy_score_candidate_is_selected(self) -> None:
        user_id = "user-ranking"
        state = main.auto_trader.start(user_id)
        make_cycle_due(user_id)
        status_times = iter((20.0, 20.0))

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
            if path == "/sessions/status":
                return 200, main.build_success(
                    {
                        "connected": True,
                        "active_mode": "PRACTICE",
                        "server_time": next(status_times),
                    }
                )
            raise AssertionError(f"unexpected path: {path}")

        scan_payload = main.build_success(
            [
                {
                    "symbol": "EURUSD-OTC",
                    "signal": "CALL",
                    "confidence": 94,
                    "strength": 5,
                    "trend": "SIDEWAYS",
                    "payout": 90,
                    "reason": "Candidato penalizado por tendencia lateral.",
                },
                {
                    "symbol": "GBPUSD-OTC",
                    "signal": "PUT",
                    "confidence": 95,
                    "strength": 30,
                    "trend": "DOWN",
                    "payout": 91,
                    "reason": "Maior confluencia entre estrategias.",
                },
            ]
        )

        with (
            patch.object(main, "call_bullex_service", side_effect=fake_bullex),
            patch.object(
                main,
                "scan_local_signals",
                new=AsyncMock(return_value=(200, scan_payload)),
            ),
        ):
            status_code, payload = await main.execute_robot_cycle(user_id)

        data = payload["data"]
        self.assertEqual(status_code, 200)
        self.assertEqual(data["status"], "WAITING_ENTRY_WINDOW")
        self.assertEqual(data["candidates_count"], 2)
        self.assertEqual(len(data["candidates"]), 2)
        self.assertEqual(data["best_candidate"]["symbol"], "GBPUSD-OTC")
        self.assertEqual(data["pending_signal"]["symbol"], "GBPUSD-OTC")
        self.assertGreater(
            data["best_candidate"]["strategy_score"],
            0,
        )

    async def test_order_status_is_sending_while_bullex_order_is_in_flight(self) -> None:
        user_id = "user-sending"
        main.auto_trader.start(user_id)
        main.auto_trader.set_pending_signal(
            user_id,
            {
                "symbol": "EURUSD-OTC",
                "signal": "CALL",
                "confidence": 94,
                "payout": 90,
            },
        )
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
            if path == "/orders/buy-demo":
                order_started.set()
                await release_order.wait()
                return 200, main.build_success({"order_id": "sending-1"})
            raise AssertionError(f"unexpected path: {path}")

        scan_payload = main.build_success(
            [{"symbol": "EURUSD-OTC", "signal": "CALL", "confidence": 94, "strength": 80}]
        )
        sending_from_statuses = []
        original_start_sending = main.auto_trader.start_sending_order

        def track_start_sending(call_user_id):
            sending_from_statuses.append(main.auto_trader.get(call_user_id).status)
            return original_start_sending(call_user_id)

        with (
            patch.object(main, "call_bullex_service", side_effect=fake_bullex),
            patch.object(main, "scan_local_signals", new=AsyncMock(return_value=(200, scan_payload))),
            patch.object(
                main.auto_trader,
                "start_sending_order",
                side_effect=track_start_sending,
            ),
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
        self.assertEqual(sending_from_statuses, ["WAITING_ENTRY_WINDOW"])

    async def test_successful_order_goes_to_pending_result_with_last_trade_contract(self) -> None:
        user_id = "user-order-success"
        state = main.auto_trader.start(user_id)
        state.entry_value = 3
        main.auto_trader.set_pending_signal(
            user_id,
            {
                "symbol": "EURUSD-OTC",
                "signal": "PUT",
                "confidence": 94,
                "payout": 91,
            },
        )

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
            if path == "/sessions/status":
                return 200, main.build_success(
                    {
                        "connected": True,
                        "active_mode": "PRACTICE",
                        "server_time": SERVER_TIME_M1_OPEN,
                    }
                )
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
        self.assertIsNotNone(payload["data"]["last_signal"])
        self.assertEqual(trade["order_id"], "success-1")
        self.assertEqual(trade["active"], "EURUSD-OTC")
        self.assertEqual(trade["direction"], "PUT")
        self.assertEqual(trade["amount"], 3)
        self.assertIsNotNone(trade["sent_at"])
        self.assertEqual(trade["expiration"], "M1")
        self.assertEqual(trade["result"], STATUS_PENDING_RESULT)
        self.assertEqual(trade["server_time_at_send"], "1970-01-01T00:00:25+00:00")
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
        main.auto_trader.set_pending_signal(
            user_id,
            {
                "symbol": "EURUSD-OTC",
                "signal": "CALL",
                "confidence": 94,
                "payout": 90,
            },
        )
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
            if path == "/orders/buy-demo":
                return 200, main.build_success(
                    {
                        "order_id": "source-1",
                        "close_time": returned_expiration,
                    }
                )
            raise AssertionError(f"unexpected path: {path}")

        scan_payload = main.build_success(
            [{"symbol": "EURUSD-OTC", "signal": "CALL", "confidence": 94, "strength": 80}]
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

    async def test_order_falls_back_to_next_candidate_when_asset_unavailable(self) -> None:
        user_id = "user-order-fallback"
        state = main.auto_trader.start(user_id)
        state.entry_value = 2
        main.auto_trader.set_pending_signal(
            user_id,
            {
                "symbol": "EURUSD-OTC",
                "signal": "CALL",
                "confidence": 94,
                "payout": 90,
                "strategy_score": 94,
            },
        )
        state.candidates = [
            {
                "symbol": "EURUSD-OTC",
                "signal": "CALL",
                "direction": "CALL",
                "confidence": 94,
                "payout": 90,
                "strategy_score": 94,
                "trade_allowed": True,
            },
            {
                "symbol": "GBPUSD-OTC",
                "signal": "PUT",
                "direction": "PUT",
                "confidence": 93,
                "payout": 91,
                "strategy_score": 93,
                "trade_allowed": True,
            },
        ]
        state.candidates_count = len(state.candidates)
        order_actives: list[str] = []

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
            if path == "/sessions/status":
                return 200, main.build_success(
                    {
                        "connected": True,
                        "active_mode": "PRACTICE",
                        "server_time": SERVER_TIME_M1_OPEN,
                    }
                )
            if path == "/orders/buy-demo":
                order_actives.append(json_body["active"])
                if json_body["active"] == "EURUSD-OTC":
                    return 409, main.build_error("Cannot purchase an option (the asset is not available at the moment).")
                return 200, main.build_success({"order_id": "fallback-1"})
            raise AssertionError(f"unexpected path: {path}")

        with (
            patch.object(main, "call_bullex_service", side_effect=fake_bullex),
            patch.object(main.trade_result_monitor, "start", return_value=True) as monitor,
            self.assertLogs("backend-gateway", level="INFO") as logs,
        ):
            status_code, payload = await main.execute_robot_cycle(user_id)

        data = payload["data"]
        trade = data["last_trade"]
        output = "\n".join(logs.output)
        self.assertEqual(status_code, 200)
        self.assertEqual(order_actives, ["EURUSD-OTC", "GBPUSD-OTC"])
        self.assertEqual(data["status"], STATUS_PENDING_RESULT)
        self.assertEqual(data["order_attempts"], 2)
        self.assertTrue(data["fallback_candidate_used"])
        self.assertEqual(data["best_candidate"]["symbol"], "GBPUSD-OTC")
        self.assertIsNone(data["pending_signal"])
        self.assertEqual(trade["active"], "GBPUSD-OTC")
        self.assertEqual(trade["direction"], "PUT")
        self.assertEqual(trade["order_attempts"], 2)
        self.assertTrue(trade["fallback_candidate_used"])
        monitor.assert_called_once_with(user_id, "fallback-1", trade["expires_at"])
        self.assertIn("[ORDER_SEND_FAILED]", output)
        self.assertIn("[ORDER_FALLBACK_NEXT_CANDIDATE]", output)
        self.assertIn("[ORDER_SEND_SUCCESS]", output)

    async def test_order_rejects_after_three_unavailable_candidates(self) -> None:
        user_id = "user-order-fallback-exhausted"
        state = main.auto_trader.start(user_id)
        main.auto_trader.set_pending_signal(
            user_id,
            {
                "symbol": "EURUSD-OTC",
                "signal": "CALL",
                "confidence": 94,
                "payout": 90,
                "strategy_score": 94,
            },
        )
        state.candidates = [
            {
                "symbol": symbol,
                "signal": "CALL",
                "direction": "CALL",
                "confidence": confidence,
                "payout": payout,
                "strategy_score": confidence,
                "trade_allowed": True,
            }
            for symbol, confidence, payout in (
                ("EURUSD-OTC", 94, 90),
                ("GBPUSD-OTC", 93, 91),
                ("USDJPY-OTC", 92, 89),
                ("EURJPY-OTC", 91, 88),
            )
        ]
        state.candidates_count = len(state.candidates)
        order_actives: list[str] = []

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
            if path == "/sessions/status":
                return 200, main.build_success(
                    {
                        "connected": True,
                        "active_mode": "PRACTICE",
                        "server_time": SERVER_TIME_M1_OPEN,
                    }
                )
            if path == "/orders/buy-demo":
                order_actives.append(json_body["active"])
                return 409, main.build_error("active suspended")
            raise AssertionError(f"unexpected path: {path}")

        with (
            patch.object(main, "call_bullex_service", side_effect=fake_bullex),
            patch.object(main.trade_result_monitor, "start", return_value=True) as monitor,
            self.assertLogs("backend-gateway", level="INFO") as logs,
        ):
            status_code, payload = await main.execute_robot_cycle(user_id)

        data = payload["data"]
        output = "\n".join(logs.output)
        self.assertEqual(status_code, 409)
        self.assertEqual(order_actives, ["EURUSD-OTC", "GBPUSD-OTC", "USDJPY-OTC"])
        self.assertEqual(data["status"], STATUS_ORDER_REJECTED)
        self.assertEqual(data["order_attempts"], 3)
        self.assertTrue(data["fallback_candidate_used"])
        self.assertEqual(data["last_order_error"], "Nenhum ativo disponível no momento da compra.")
        self.assertIsNone(data["pending_signal"])
        self.assertGreaterEqual(data["seconds_until_next_cycle"], 299)
        self.assertLessEqual(data["seconds_until_next_cycle"], 300)
        monitor.assert_not_called()
        self.assertIn("[ORDER_SEND_FAILED]", output)
        self.assertIn("[ORDER_FALLBACK_NEXT_CANDIDATE]", output)
        self.assertIn("[ORDER_REJECTED]", output)

    async def test_rejected_order_stays_order_rejected_and_clears_pending_signal(self) -> None:
        user_id = "user-order-rejected"
        main.auto_trader.start(user_id)
        main.auto_trader.set_pending_signal(
            user_id,
            {
                "symbol": "EURUSD-OTC",
                "signal": "CALL",
                "confidence": 94,
                "payout": 90,
            },
        )

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
            if path == "/sessions/status":
                return 200, main.build_success(
                    {
                        "connected": True,
                        "active_mode": "PRACTICE",
                        "server_time": SERVER_TIME_M1_OPEN,
                    }
                )
            if path == "/orders/buy-demo":
                return 409, main.build_error("MARKET_CLOSED")
            raise AssertionError(f"unexpected path: {path}")

        scan_payload = main.build_success(
            [{"symbol": "EURUSD-OTC", "signal": "CALL", "confidence": 94, "strength": 80}]
        )

        with (
            patch.object(main, "call_bullex_service", side_effect=fake_bullex),
            patch.object(main, "scan_local_signals", new=AsyncMock(return_value=(200, scan_payload))),
            patch.object(main.trade_result_monitor, "start", return_value=True) as monitor,
            self.assertLogs("backend-gateway", level="INFO") as logs,
        ):
            status_code, payload = await main.execute_robot_cycle(user_id)

        self.assertEqual(status_code, 409)
        self.assertEqual(payload["data"]["status"], STATUS_ORDER_REJECTED)
        self.assertEqual(payload["data"]["rejection_reason"], "MARKET_CLOSED")
        self.assertEqual(payload["data"]["last_order_error"], "MARKET_CLOSED")
        self.assertIsNotNone(payload["data"]["rejected_at"])
        self.assertIsNotNone(payload["data"]["last_signal"])
        self.assertIsNone(payload["data"]["pending_signal"])
        self.assertFalse(payload["data"]["operation_in_progress"])
        self.assertGreaterEqual(payload["data"]["seconds_until_next_cycle"], 299)
        self.assertLessEqual(payload["data"]["seconds_until_next_cycle"], 300)
        self.assertNotEqual(payload["data"]["status"], STATUS_WAITING_NEXT_CYCLE)
        monitor.assert_not_called()
        output = "\n".join(logs.output)
        self.assertIn("[ORDER_SEND_FAILED]", output)
        self.assertIn("[ORDER_REJECTED]", output)
        self.assertIn("[NEXT_CYCLE_SCHEDULED]", output)

    async def test_order_rejected_is_visible_for_five_seconds_then_waits(self) -> None:
        user_id = "user-order-rejected-visible"
        state = main.auto_trader.start(user_id)
        state.cycle_minutes = 5
        state.last_signal = {"symbol": "EURUSD-OTC", "signal": "CALL"}

        rejected = main.auto_trader.reject_order(
            user_id,
            "active suspended",
            last_order_error=main.readable_order_error("active suspended"),
        )
        visible_payload = rejected.to_dict()
        rejected.rejected_at = utc_now() - timedelta(seconds=5)
        waiting_payload = rejected.to_dict()

        self.assertEqual(visible_payload["status"], STATUS_ORDER_REJECTED)
        self.assertEqual(visible_payload["last_order_error"], "Ativo suspenso pela BullEx")
        self.assertEqual(waiting_payload["status"], STATUS_WAITING_NEXT_CYCLE)
        self.assertIsNone(waiting_payload["rejection_reason"])
        self.assertEqual(waiting_payload["last_rejection_reason"], "active suspended")
        self.assertEqual(waiting_payload["last_signal"]["symbol"], "EURUSD-OTC")

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
                        "server_time": 10.0,
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
        self.assertEqual(payload["data"]["status"], "WAITING_ANALYSIS_WINDOW")
        self.assertEqual(payload["data"]["rejection_reason"], "WAITING_NEXT_ANALYSIS_WINDOW")
        self.assertEqual(payload["data"]["analysis_result"], "NO_CANDIDATE_THIS_CANDLE")
        self.assertEqual(
            payload["data"]["last_rejection_reason"],
            "ACTIVE_CLOSED",
        )
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
            first_status, first_payload = await main.execute_robot_cycle(user_id)
            second_status, second_payload = await main.execute_robot_cycle(user_id)
            status_code, payload = await main.execute_robot_cycle(user_id)

        self.assertEqual(first_status, 200)
        self.assertNotEqual(first_payload["data"]["status"], STATUS_ACCOUNT_DISCONNECTED)
        self.assertEqual(first_payload["data"]["connection_failure_count"], 1)
        self.assertEqual(second_status, 200)
        self.assertNotEqual(second_payload["data"]["status"], STATUS_ACCOUNT_DISCONNECTED)
        self.assertEqual(second_payload["data"]["connection_failure_count"], 2)
        self.assertEqual(status_code, 200)
        self.assertEqual(payload["data"]["status"], STATUS_ACCOUNT_DISCONNECTED)
        self.assertEqual(payload["data"]["rejection_reason"], "ACCOUNT_DISCONNECTED")
        self.assertIsNone(payload["data"]["pending_signal"])
        self.assertIsNone(payload["data"]["last_signal"])
        self.assertFalse(payload["data"]["operation_in_progress"])
        self.assertFalse(payload["data"]["entry_window_open"])
        self.assertEqual(payload["data"]["blocked_filters"], [])
        self.assertEqual(payload["data"]["quality_score"], 0)
        self.assertEqual(payload["data"]["connection_failure_count"], 3)
        self.assertEqual(payload["data"]["connection_status_source"], "disconnected")
        scan.assert_not_awaited()

    async def test_robot_state_disconnects_after_three_failed_checks_and_account_disconnected(self) -> None:
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
            first_response = await main.robot_state({"user_id": user_id})
            second_response = await main.robot_state({"user_id": user_id})
            response = await main.robot_state({"user_id": user_id})

        first_payload = json.loads(first_response.body)["data"]
        second_payload = json.loads(second_response.body)["data"]
        payload = json.loads(response.body)["data"]
        self.assertEqual(first_response.status_code, 200)
        self.assertFalse(first_payload["connected"])
        self.assertEqual(first_payload["connection_failure_count"], 1)
        self.assertEqual(first_payload["connection_status_source"], "disconnected")
        self.assertNotEqual(first_payload["status"], STATUS_ACCOUNT_DISCONNECTED)
        self.assertIsNotNone(first_payload["pending_signal"])
        self.assertEqual(second_response.status_code, 200)
        self.assertFalse(second_payload["connected"])
        self.assertEqual(second_payload["connection_failure_count"], 2)
        self.assertNotEqual(second_payload["status"], STATUS_ACCOUNT_DISCONNECTED)
        self.assertEqual(response.status_code, 200)
        self.assertFalse(payload["connected"])
        self.assertEqual(payload["status"], STATUS_ACCOUNT_DISCONNECTED)
        self.assertEqual(payload["rejection_reason"], "ACCOUNT_DISCONNECTED")
        self.assertEqual(payload["connection_failure_count"], 3)
        self.assertEqual(payload["connection_status_source"], "disconnected")
        self.assertEqual(payload["seconds_until_next_cycle"], 0)
        self.assertIsNone(payload["pending_signal"])
        self.assertIsNone(payload["last_signal"])

    async def test_robot_state_ignores_session_false_negative_when_account_connected(self) -> None:
        user_id = "user-state-account-connected"
        state = main.auto_trader.start(user_id)
        state.connected = True
        state.active_mode = "PRACTICE"
        calls: list[str] = []

        async def fake_bullex(method: str, path: str, user_id_arg: str, **kwargs: Any) -> tuple[int, dict[str, Any]]:
            calls.append(path)
            if path == "/sessions/status":
                return 409, {"ok": False, "data": {"connected": False}, "error": "SESSION_DISCONNECTED"}
            if path == "/account":
                return 200, main.build_success({"connected": True, "active_mode": "PRACTICE"})
            raise AssertionError(f"unexpected path {path}")

        with (
            patch.object(main, "call_bullex_service", side_effect=fake_bullex),
            patch.object(main, "sync_user_store_from_payload"),
            self.assertLogs("backend-gateway", level="WARNING") as logs,
        ):
            response = await main.robot_state({"user_id": user_id})

        payload = json.loads(response.body)["data"]
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["connected"])
        self.assertEqual(payload["active_mode"], "PRACTICE")
        self.assertNotEqual(payload["status"], STATUS_ACCOUNT_DISCONNECTED)
        self.assertEqual(payload["connection_failure_count"], 0)
        self.assertEqual(payload["connection_status_source"], "bullex_service")
        self.assertEqual(calls[:2], ["/sessions/status", "/account"])
        self.assertIn("[CONNECTION_FALSE_NEGATIVE_IGNORED]", "\n".join(logs.output))

    async def test_bullex_connect_syncs_robot_connection_immediately(self) -> None:
        user_id = "user-connect-sync"
        state = main.auto_trader.start(user_id)
        state.status = STATUS_ACCOUNT_DISCONNECTED
        state.rejection_reason = "ACCOUNT_DISCONNECTED"
        state.last_rejection_reason = "ACCOUNT_DISCONNECTED"
        state.connection_failure_count = 2

        payload = main.build_success(
            {"connected": True, "requires_2fa": False, "active_mode": "PRACTICE"}
        )

        with (
            patch.object(main, "call_bullex_service", new=AsyncMock(return_value=(200, payload))),
            patch.object(main, "sync_user_store_from_payload"),
            patch.object(main, "persist_robot") as persist,
            patch.object(main, "ensure_robot_worker") as worker,
            self.assertLogs("backend-gateway", level="INFO") as logs,
        ):
            response = await main.bullex_connect(
                {"email": "user@example.com", "password": "secret"},
                {"user_id": user_id},
            )

        synced = main.auto_trader.get(user_id)
        self.assertEqual(response.status_code, 200)
        self.assertTrue(synced.connected)
        self.assertEqual(synced.active_mode, "PRACTICE")
        self.assertIsNone(synced.rejection_reason)
        self.assertIsNone(synced.last_rejection_reason)
        self.assertEqual(synced.status, STATUS_WAITING_NEXT_CYCLE)
        self.assertEqual(synced.connection_failure_count, 0)
        self.assertEqual(synced.connection_status_source, "bullex_service")
        self.assertIsNotNone(synced.connection_checked_at)
        persist.assert_called_once_with(user_id)
        worker.assert_called_once_with(user_id)
        output = "\n".join(logs.output)
        self.assertIn("[BULLEX_CONNECTED]", output)
        self.assertIn("[ROBOT_CONNECTION_SYNCED]", output)

    async def test_robot_sync_connection_endpoint_returns_connection_fields(self) -> None:
        user_id = "user-sync-connection"
        main.auto_trader.start(user_id)
        payload = main.build_success(
            {
                "connected": True,
                "active_mode": "REAL",
                "server_time": SERVER_TIME_M1_OPEN,
            }
        )

        with (
            patch.object(main, "call_bullex_service", new=AsyncMock(return_value=(200, payload))),
            patch.object(main, "sync_user_store_from_payload"),
            patch.object(main, "persist_robot"),
        ):
            response = await main.robot_sync_connection({"user_id": user_id})

        data = json.loads(response.body)["data"]
        self.assertEqual(response.status_code, 200)
        self.assertTrue(data["connected"])
        self.assertEqual(data["active_mode"], "REAL")
        self.assertIsNotNone(data["connection_checked_at"])
        self.assertEqual(data["connection_status_source"], "bullex_service")
        self.assertEqual(data["connection_failure_count"], 0)

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
        main.auto_trader.set_pending_signal(
            user_id,
            {
                "symbol": "EURUSD-OTC",
                "signal": "PUT",
                "confidence": 95,
                "payout": 90,
            },
        )
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
            if path == "/orders/buy-real":
                return 200, main.build_success({"order_id": "real-1"})
            raise AssertionError(f"unexpected path: {path}")

        scan_payload = main.build_success(
            [{"symbol": "EURUSD-OTC", "signal": "PUT", "confidence": 95, "strength": 81}]
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
        monitor.assert_called_once_with(
            user_id,
            "real-1",
            second_payload["data"]["last_trade"]["expires_at"],
        )

    async def test_pending_signal_waits_then_sends_without_reanalysis(self) -> None:
        user_id = "user-window-wait"
        main.auto_trader.start(user_id)
        make_cycle_due(user_id)
        calls = []
        status_times = iter((20.0, 20.0, 25.0))

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
            [{"symbol": "EURUSD-OTC", "signal": "CALL", "confidence": 94, "strength": 80}]
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
        self.assertEqual(pending["confidence"], 94)
        self.assertEqual(pending["payout"], 90.0)
        self.assertEqual(pending["timeframe"], "M1")
        self.assertIsNotNone(pending["created_at"])
        self.assertEqual(first_payload["data"]["seconds_until_entry_window"], 5)
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
        self.assertIn("[WAITING_ENTRY_WINDOW]", output)
        self.assertIn("[ENTRY_WINDOW_WAIT]", output)
        self.assertIn("[ENTRY_WINDOW_OPEN]", output)
        self.assertIn("[SENDING_ORDER]", output)
        self.assertIn("[PENDING_RESULT]", output)
        self.assertIn("[TRADE_SENT_AT]", output)
        self.assertIn("[PENDING_SIGNAL_CLEARED]", output)

    async def test_m1_only_buys_between_seconds_25_and_29(self) -> None:
        cases = {
            24.0: "WAITING_ENTRY_WINDOW",
            25.0: "PENDING_RESULT",
            29.0: "PENDING_RESULT",
            30.0: "SIGNAL_REJECTED",
            55.0: "SIGNAL_REJECTED",
            56.0: "SIGNAL_REJECTED",
            57.0: "SIGNAL_REJECTED",
            58.0: "SIGNAL_REJECTED",
            59.0: "SIGNAL_REJECTED",
        }

        for second, expected_status in cases.items():
            with self.subTest(second=second):
                user_id = f"user-window-{int(second)}"
                main.auto_trader.start(user_id)
                main.auto_trader.set_pending_signal(
                    user_id,
                    {
                        "symbol": "EURUSD-OTC",
                        "signal": "CALL",
                        "confidence": 92,
                        "payout": 90,
                    },
                )
                calls = []

                async def fake_bullex(
                    method,
                    path,
                    call_user_id,
                    json_body=None,
                    params=None,
                ):
                    calls.append(path)
                    if path == "/sessions/status":
                        return 200, main.build_success(
                            {
                                "connected": True,
                                "active_mode": "PRACTICE",
                                "server_time": second,
                            }
                        )
                    if path == "/orders/buy-demo":
                        return 200, main.build_success(
                            {"order_id": f"window-{int(second)}"}
                        )
                    raise AssertionError(f"unexpected path: {path}")

                with (
                    patch.object(
                        main,
                        "call_bullex_service",
                        side_effect=fake_bullex,
                    ),
                    patch.object(
                        main.trade_result_monitor,
                        "start",
                        return_value=True,
                    ),
                ):
                    status_code, payload = await main.execute_robot_cycle(user_id)

                data = payload["data"]
                self.assertEqual(status_code, 200)
                self.assertEqual(data["status"], expected_status)
                self.assertEqual(data["current_candle_seconds"], second)
                self.assertEqual(data["entry_window_start_second"], 25)
                self.assertEqual(data["entry_window_end_second"], 29)
                self.assertEqual(data["buy_target_second"], 25)
                if second in {25.0, 29.0}:
                    self.assertTrue(
                        main.get_entry_window("M1", second)["entry_window_open"]
                    )
                    self.assertEqual(calls.count("/orders/buy-demo"), 1)
                else:
                    self.assertFalse(data["entry_window_open"])
                    self.assertNotIn("/orders/buy-demo", calls)
                if second == 24.0:
                    self.assertEqual(data["seconds_until_entry_window"], 1)
                if second >= 30.0:
                    self.assertEqual(
                        data["rejection_reason"],
                        "MISSED_ENTRY_WINDOW",
                    )

    async def test_missed_entry_window_finishes_with_clear_rejection(self) -> None:
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
                    {"connected": True, "active_mode": "PRACTICE", "server_time": 30.0}
                )
            raise AssertionError(f"unexpected path: {path}")

        with (
            patch.object(main, "call_bullex_service", side_effect=fake_bullex),
            patch.object(main, "scan_local_signals", new=AsyncMock()) as scan,
            self.assertLogs("backend-gateway", level="INFO") as logs,
        ):
            status_code, payload = await main.execute_robot_cycle(user_id)

        self.assertEqual(status_code, 200)
        self.assertEqual(payload["data"]["status"], "SIGNAL_REJECTED")
        self.assertEqual(payload["data"]["rejection_reason"], "MISSED_ENTRY_WINDOW")
        self.assertEqual(payload["data"]["last_rejection_reason"], "MISSED_ENTRY_WINDOW")
        self.assertIsNone(payload["data"]["pending_signal"])
        self.assertGreaterEqual(payload["data"]["seconds_until_next_cycle"], 299)
        scan.assert_not_awaited()
        output = "\n".join(logs.output)
        self.assertIn("[PENDING_SIGNAL_CLEARED]", output)
        self.assertIn("[MISSED_ENTRY_WINDOW]", output)

    async def test_m5_sends_five_minute_expiration(self) -> None:
        user_id = "user-m5"
        state = main.auto_trader.start(user_id)
        state.timeframe = "M5"
        main.auto_trader.set_pending_signal(
            user_id,
            {
                "symbol": "EURUSD-OTC",
                "signal": "CALL",
                "confidence": 94,
                "payout": 90,
            },
        )
        calls = []

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
            calls.append((path, json_body))
            if path == "/sessions/status":
                return 200, main.build_success(
                    {"connected": True, "active_mode": "PRACTICE", "server_time": 265.0}
                )
            if path == "/orders/buy-demo":
                return 200, main.build_success({"order_id": "demo-m5"})
            raise AssertionError(f"unexpected path: {path}")

        scan_payload = main.build_success(
            [{"symbol": "EURUSD-OTC", "signal": "CALL", "confidence": 94, "strength": 80}]
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
        scan.assert_not_awaited()

    async def test_window_closing_during_analysis_blocks_late_order(self) -> None:
        user_id = "user-window-closing"
        main.auto_trader.start(user_id)
        make_cycle_due(user_id)
        status_times = iter((10.0, 30.0))
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
            [{"symbol": "EURUSD-OTC", "signal": "CALL", "confidence": 94, "strength": 80}]
        )
        with (
            patch.object(main, "call_bullex_service", side_effect=fake_bullex),
            patch.object(main, "scan_local_signals", new=AsyncMock(return_value=(200, scan_payload))),
        ):
            status_code, payload = await main.execute_robot_cycle(user_id)

        self.assertEqual(status_code, 200)
        self.assertEqual(payload["data"]["status"], "WAITING_ANALYSIS_WINDOW")
        self.assertEqual(payload["data"]["rejection_reason"], "WAITING_NEXT_ANALYSIS_WINDOW")
        self.assertIsNone(payload["data"]["pending_signal"])
        self.assertEqual(payload["data"]["seconds_until_analysis_window"], 35)
        self.assertFalse(payload["data"]["entry_window_open"])
        self.assertNotIn("/orders/buy-demo", calls)
