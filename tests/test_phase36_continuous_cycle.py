import json
import unittest
from datetime import timedelta
from unittest.mock import AsyncMock, patch

from backend import main
from backend.auto_trader import (
    STATUS_PENDING_RESULT,
    STATUS_RESULT_RECEIVED,
    STATUS_SENDING_ORDER,
    STATUS_WAITING_NEXT_CYCLE,
    utc_now,
)


def make_signal(symbol: str = "EURUSD-OTC", direction: str = "CALL", score: int = 95) -> dict:
    return {
        "symbol": symbol,
        "signal": direction,
        "direction": direction,
        "confidence": score,
        "payout": 90,
        "trend": "UP" if direction == "CALL" else "DOWN",
        "strength": 80,
        "strategy_score": score,
        "trade_allowed": True,
    }


class Phase36ContinuousCycleTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        main.auto_trader = main.AutoTrader()

    async def test_robot_start_clears_old_entry_and_schedules_initial_analysis(self) -> None:
        user_id = "phase36-start-clean"
        state = main.auto_trader.get(user_id)
        state.enabled = True
        state.status = STATUS_SENDING_ORDER
        state.pending_signal = make_signal("GBPUSD-OTC", "PUT", 90)
        state.best_candidate = dict(state.pending_signal)

        with (
            patch.object(main, "persist_robot"),
            patch.object(main, "ensure_robot_worker") as worker,
            patch.object(main, "schedule_robot_tick") as tick,
        ):
            response = await main.robot_start({"user_id": user_id})

        data = json.loads(response.body)["data"]
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data["status"], STATUS_WAITING_NEXT_CYCLE)
        self.assertIsNone(data["pending_signal"])
        self.assertIsNone(data["best_candidate"])
        self.assertIsNone(data["voice_message"])
        self.assertIn("silêncio", data["analysis_message"])
        worker.assert_called_once_with(user_id)
        tick.assert_called_once_with(user_id)

    async def test_waiting_cycle_updates_best_candidate_without_pending_signal(self) -> None:
        user_id = "phase36-analysis"
        state = main.auto_trader.start(user_id)
        state.next_cycle_at = utc_now() + timedelta(minutes=5)

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
            if path == "/sessions/status":
                return 200, main.build_success(
                    {"connected": True, "active_mode": "PRACTICE", "server_time": 10.0}
                )
            if path == "/payouts":
                return 200, main.build_success({"active": params["active"], "payout": 90})
            raise AssertionError(f"unexpected path: {path}")

        with (
            patch.object(main, "call_bullex_service", new=AsyncMock(side_effect=fake_bullex)),
            patch.object(main, "scan_local_signals", new=AsyncMock(return_value=(200, main.build_success([make_signal()])))),
            patch.object(main, "persist_robot"),
        ):
            status_code, payload = await main.execute_robot_cycle(user_id)

        data = payload["data"]
        self.assertEqual(status_code, 200)
        self.assertEqual(data["status"], STATUS_WAITING_NEXT_CYCLE)
        self.assertIsNone(data["pending_signal"])
        self.assertEqual(main.auto_trader.get(user_id).best_candidate["symbol"], "EURUSD-OTC")
        self.assertIsNone(data["best_candidate"])
        self.assertIsNone(data["voice_message"])
        self.assertIn("silêncio", data["analysis_message"])
        self.assertEqual(data["display_countdown_label"], "Entrada em")
        self.assertNotIn("analysis_window_open", data)
        self.assertNotEqual(data["status"], "WAITING_ANALYSIS_WINDOW")

    async def test_cycle_due_sends_current_best_candidate_immediately(self) -> None:
        user_id = "phase36-entry"
        state = main.auto_trader.start(user_id)
        state.next_cycle_at = utc_now() - timedelta(seconds=1)
        main.auto_trader.set_analysis_candidates(user_id, [make_signal()], make_signal())
        state.status = STATUS_WAITING_NEXT_CYCLE
        state.pending_signal = None

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
            if path == "/sessions/status":
                return 200, main.build_success(
                    {"connected": True, "active_mode": "PRACTICE", "server_time": 300.0}
                )
            if path == "/orders/buy-demo":
                return 200, main.build_success({"order_id": "phase36-1"})
            raise AssertionError(f"unexpected path: {path}")

        with (
            patch.object(main, "call_bullex_service", new=AsyncMock(side_effect=fake_bullex)),
            patch.object(main.trade_result_monitor, "start"),
            patch.object(main, "persist_robot"),
        ):
            status_code, payload = await main.execute_robot_cycle(user_id)

        data = payload["data"]
        self.assertEqual(status_code, 200)
        self.assertEqual(data["status"], STATUS_PENDING_RESULT)
        self.assertTrue(data["operation_in_progress"])
        self.assertTrue(data["result_waiting"])
        self.assertIsNone(data["pending_signal"])
        self.assertEqual(data["last_trade"]["active"], "EURUSD-OTC")

    async def test_expiration_uses_fresh_bullex_time_after_order(self) -> None:
        user_id = "phase36-fresh-expiration"
        state = main.auto_trader.start(user_id)
        state.next_cycle_at = utc_now() - timedelta(seconds=1)
        main.auto_trader.set_analysis_candidates(user_id, [make_signal()], make_signal())
        state.status = STATUS_WAITING_NEXT_CYCLE
        state.pending_signal = None
        session_calls = 0

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
            nonlocal session_calls
            if path == "/sessions/status":
                session_calls += 1
                server_time = 359.0 if session_calls == 1 else 314.0
                return 200, main.build_success(
                    {"connected": True, "active_mode": "PRACTICE", "server_time": server_time}
                )
            if path == "/orders/buy-demo":
                return 200, main.build_success({"order_id": "phase36-expiration-1"})
            raise AssertionError(f"unexpected path: {path}")

        with (
            patch.object(main, "call_bullex_service", new=AsyncMock(side_effect=fake_bullex)),
            patch.object(main.trade_result_monitor, "start"),
            patch.object(main, "persist_robot"),
        ):
            status_code, payload = await main.execute_robot_cycle(user_id)

        trade = payload["data"]["last_trade"]
        self.assertEqual(status_code, 200)
        self.assertEqual(trade["server_timestamp_at_send"], 314.0)
        self.assertEqual(trade["expiration_source"], "server_time_aligned")
        self.assertEqual(trade["expected_expire_at"], "1970-01-01T00:06:01+00:00")

    async def test_cycle_due_analyzes_once_when_best_candidate_is_missing(self) -> None:
        user_id = "phase36-force-analysis"
        state = main.auto_trader.start(user_id)
        state.next_cycle_at = utc_now() - timedelta(seconds=1)

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
            if path == "/sessions/status":
                return 200, main.build_success(
                    {"connected": True, "active_mode": "PRACTICE", "server_time": 300.0}
                )
            if path == "/payouts":
                return 200, main.build_success({"active": params["active"], "payout": 90})
            if path == "/orders/buy-demo":
                return 200, main.build_success({"order_id": "phase36-2"})
            raise AssertionError(f"unexpected path: {path}")

        with (
            patch.object(main, "call_bullex_service", new=AsyncMock(side_effect=fake_bullex)),
            patch.object(main, "scan_local_signals", new=AsyncMock(return_value=(200, main.build_success([make_signal("GBPUSD-OTC", "PUT")])))),
            patch.object(main.trade_result_monitor, "start"),
            patch.object(main, "persist_robot"),
        ):
            status_code, payload = await main.execute_robot_cycle(user_id)

        data = payload["data"]
        self.assertEqual(status_code, 200)
        self.assertEqual(data["status"], STATUS_PENDING_RESULT)
        self.assertEqual(data["best_candidate"]["symbol"], "GBPUSD-OTC")
        self.assertEqual(data["last_trade"]["active"], "GBPUSD-OTC")

    async def test_result_display_then_new_cycle_starts_clean(self) -> None:
        user_id = "phase36-result"
        state = main.auto_trader.start(user_id)
        state.operation_in_progress = True
        state.last_trade = {"order_id": "phase36-result-1", "amount": 2, "result": STATUS_PENDING_RESULT}
        finalized, state = main.auto_trader.finish_trade(user_id, "phase36-result-1", "WIN", 1.8)
        self.assertTrue(finalized)
        self.assertEqual(state.status, STATUS_RESULT_RECEIVED)

        state.result_display_until = utc_now() - timedelta(seconds=1)
        payload = state.to_dict()

        self.assertEqual(payload["status"], STATUS_WAITING_NEXT_CYCLE)
        self.assertIsNone(payload["best_candidate"])
        self.assertIsNone(payload["pending_signal"])
        self.assertGreater(payload["seconds_until_next_cycle"], 0)

    async def test_robot_state_sends_order_when_state_is_stuck_sending(self) -> None:
        user_id = "phase36-state-recovers-sending"
        state = main.auto_trader.start(user_id)
        state.connected = True
        state.next_cycle_at = utc_now() - timedelta(seconds=1)
        main.auto_trader.set_pending_signal(user_id, make_signal("GBPUSD-OTC", "PUT", 90))
        self.assertEqual(main.auto_trader.get(user_id).status, STATUS_SENDING_ORDER)

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
            if path == "/sessions/status":
                return 200, main.build_success(
                    {"connected": True, "active_mode": "PRACTICE", "server_time": 300.0}
                )
            if path == "/orders/buy-demo":
                return 200, main.build_success({"order_id": "phase36-state-1"})
            raise AssertionError(f"unexpected path: {path}")

        with (
            patch.object(main, "call_bullex_service", new=AsyncMock(side_effect=fake_bullex)),
            patch.object(main.trade_result_monitor, "start"),
            patch.object(main, "persist_robot"),
        ):
            response = await main.robot_state({"user_id": user_id})

        data = json.loads(response.body)["data"]
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data["status"], STATUS_PENDING_RESULT)
        self.assertEqual(data["last_trade"]["active"], "GBPUSD-OTC")
        self.assertTrue(data["operation_in_progress"])

    def test_sending_order_payload_includes_voice_message(self) -> None:
        user_id = "phase36-voice"
        state = main.auto_trader.start(user_id)
        main.auto_trader.set_pending_signal(user_id, make_signal("GBPUSD-OTC", "PUT", 90))

        payload = state.to_dict()

        self.assertEqual(payload["status"], STATUS_SENDING_ORDER)
        self.assertIn("Entrada liberada agora", payload["voice_message"])
        self.assertIn("GBPUSD-OTC", payload["voice_message"])
        self.assertIn("PUT", payload["voice_message"])
        self.assertIsNotNone(payload["voice_event_id"])


if __name__ == "__main__":
    unittest.main()
