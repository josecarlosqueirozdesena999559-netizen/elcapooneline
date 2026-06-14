import json
import unittest
from datetime import timedelta
from unittest.mock import AsyncMock, patch

from backend import main
from backend.auto_trader import (
    STATUS_PENDING_RESULT,
    STATUS_RESULT_RECEIVED,
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
        self.assertEqual(data["best_candidate"]["symbol"], "EURUSD-OTC")
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


if __name__ == "__main__":
    unittest.main()
