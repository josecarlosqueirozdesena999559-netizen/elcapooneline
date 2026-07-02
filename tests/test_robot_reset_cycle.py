import asyncio
from contextlib import suppress
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend import main
from backend.auto_trader import AutoTrader, utc_now
from backend.robot_persistence import SQLiteRobotPersistence


class RobotResetCycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.old_persistence = main.robot_persistence
        self.old_trader = main.auto_trader
        main.robot_persistence = SQLiteRobotPersistence(
            str(Path(self.directory.name) / "robot.db")
        )
        main.auto_trader = AutoTrader()

    async def asyncTearDown(self) -> None:
        main.robot_persistence = self.old_persistence
        main.auto_trader = self.old_trader
        self.directory.cleanup()

    async def test_reset_cycle_clears_stop_block_and_stops_worker(self) -> None:
        user_id = "user-reset"
        state = main.auto_trader.start(user_id)
        state.wins = 3
        state.losses = 2
        state.profit = 11.5
        state.status = "SIGNAL_REJECTED"
        state.rejection_reason = "STOP_WIN_HIT"
        state.last_rejection_reason = "STOP_WIN_HIT"
        state.cycle_result = "WIN"
        state.rejected_at = utc_now()
        previous_cycle_id = state.cycle_id

        with (
            patch.object(main, "stop_robot_worker") as stop_worker,
            patch.object(main, "persist_robot"),
        ):
            response = await main.robot_reset_cycle(
                {"reset_daily_profit": True},
                {"user_id": user_id},
            )

        payload = json.loads(response.body)["data"]
        refreshed = main.auto_trader.get(user_id)

        self.assertEqual(response.status_code, 200)
        self.assertFalse(refreshed.enabled)
        self.assertEqual(refreshed.status, "STOPPED")
        self.assertIsNone(refreshed.rejection_reason)
        self.assertIsNone(refreshed.last_rejection_reason)
        self.assertIsNone(refreshed.cycle_result)
        self.assertIsNone(refreshed.rejected_at)
        self.assertEqual(refreshed.wins, 0)
        self.assertEqual(refreshed.losses, 0)
        self.assertEqual(refreshed.profit, 0.0)
        self.assertIsNone(refreshed.last_trade)
        self.assertIsNone(refreshed.pending_signal)
        self.assertFalse(refreshed.gale_pending)
        self.assertFalse(refreshed.gale_active)
        self.assertFalse(refreshed.operation_in_progress)
        self.assertNotEqual(refreshed.cycle_id, previous_cycle_id)
        self.assertIsNone(refreshed.current_cycle_started_at)
        self.assertIsNone(refreshed.next_cycle_at)
        self.assertEqual(payload["status"], "STOPPED")
        self.assertFalse(payload["worker_running"])
        self.assertFalse(payload["operation_in_progress"])
        self.assertFalse(payload["result_waiting"])
        stop_worker.assert_awaited_once_with(user_id)

    async def test_reset_cycle_with_reset_score_clears_score_and_history(self) -> None:
        user_id = "user-reset-score"
        main.auto_trader.start(user_id)
        main.auto_trader.record_trade(
            user_id,
            {
                "order_id": "reset-score-1",
                "active": "EURUSD-OTC",
                "direction": "CALL",
                "amount": 2,
                "confidence": 90,
                "payout": 88,
                "result": "PENDING_RESULT",
                "sent_at": "2026-06-18T12:00:00+00:00",
            },
        )
        main.auto_trader.finish_trade(user_id, "reset-score-1", "LOSS", -2)
        trade = main.auto_trader.get(user_id).last_trade
        main.robot_persistence.save_trade(user_id, trade)
        main.robot_persistence.save_trade_history(user_id, trade)

        with (
            patch.object(main, "stop_robot_worker"),
            patch.object(main, "persist_robot"),
        ):
            await main.robot_reset_cycle(
                {"reset_score": True},
                {"user_id": user_id},
            )

        refreshed = main.auto_trader.get(user_id)
        self.assertEqual(refreshed.wins, 0)
        self.assertEqual(refreshed.losses, 0)
        self.assertEqual(refreshed.profit, 0.0)
        self.assertEqual(main.auto_trader.history(user_id)["trades"], [])
        self.assertEqual(main.robot_persistence.load_trades(user_id), [])
        self.assertEqual(main.robot_persistence.load_trade_history(user_id, 30), [])

    async def test_reset_score_clears_score_history_and_keeps_robot_running(self) -> None:
        user_id = "user-reset-score-only"
        state = main.auto_trader.start(user_id)
        state.status = "WAITING_NEXT_CYCLE"
        state.worker_running = True
        main.auto_trader.record_trade(
            user_id,
            {
                "order_id": "score-only-1",
                "active": "EURUSD-OTC",
                "direction": "CALL",
                "amount": 2,
                "confidence": 90,
                "payout": 88,
                "result": "PENDING_RESULT",
                "sent_at": "2026-06-18T12:00:00+00:00",
            },
        )
        main.auto_trader.finish_trade(user_id, "score-only-1", "WIN", 1.8)
        main.robot_persistence.save_trade(user_id, main.auto_trader.get(user_id).last_trade)
        main.robot_persistence.save_trade_history(user_id, main.auto_trader.get(user_id).last_trade)

        with patch.object(main, "persist_robot") as persist:
            response = await main.robot_reset_score({"user_id": user_id})

        payload = json.loads(response.body)["data"]
        refreshed = main.auto_trader.get(user_id)

        self.assertEqual(response.status_code, 200)
        self.assertTrue(refreshed.enabled)
        self.assertEqual(refreshed.wins, 0)
        self.assertEqual(refreshed.losses, 0)
        self.assertEqual(refreshed.profit, 0.0)
        self.assertIsNone(refreshed.last_trade)
        self.assertEqual(main.auto_trader.history(user_id)["trades"], [])
        self.assertEqual(main.robot_persistence.load_trades(user_id), [])
        self.assertEqual(main.robot_persistence.load_trade_history(user_id, 30), [])
        self.assertEqual(payload["wins"], 0)
        self.assertEqual(payload["losses"], 0)
        self.assertEqual(payload["profit"], 0.0)
        persist.assert_called_once_with(user_id)

    async def test_robot_stop_clears_state_and_worker_immediately(self) -> None:
        user_id = "user-stop"
        state = main.auto_trader.start(user_id)
        state.operation_in_progress = True
        state.last_trade = {
            "order_id": "stop-open-1",
            "amount": 2,
            "result": "PENDING_RESULT",
        }
        state.pending_signal = {"symbol": "EURUSD-OTC"}
        state.gale_pending = True
        state.gale_active = True

        async def idle_worker() -> None:
            await asyncio.sleep(60)

        task = asyncio.create_task(idle_worker())
        main.robot_tasks[user_id] = task

        try:
            with patch.object(main, "persist_robot"):
                response = await main.robot_stop({"user_id": user_id})
        finally:
            if not task.done():
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task

        payload = json.loads(response.body)["data"]
        refreshed = main.auto_trader.get(user_id)

        self.assertEqual(response.status_code, 200)
        self.assertFalse(refreshed.enabled)
        self.assertEqual(refreshed.status, "STOPPED")
        self.assertTrue(refreshed.operation_in_progress)
        self.assertFalse(refreshed.gale_pending)
        self.assertTrue(refreshed.gale_active)
        self.assertFalse(payload["enabled"])
        self.assertEqual(payload["status"], "STOPPED")
        self.assertFalse(payload["worker_running"])
        self.assertTrue(payload["operation_in_progress"])
        self.assertTrue(payload["result_waiting"])

        finalized, finished_state = main.auto_trader.finish_trade(user_id, "stop-open-1", "LOSS", 0)
        self.assertTrue(finalized)
        self.assertEqual(finished_state.status, "STOPPED")
        self.assertEqual(finished_state.losses, 1)

    async def test_robot_config_rejects_changes_while_robot_is_running(self) -> None:
        user_id = "user-config-locked"
        main.auto_trader.start(user_id)

        response = await main.robot_config({"entry_value": 5}, {"user_id": user_id})
        payload = json.loads(response.body)

        self.assertEqual(response.status_code, 409)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"], "ROBOT_RUNNING_CONFIG_LOCKED")
        self.assertEqual(payload["message"], "Pare o robô antes de alterar configurações.")

    async def test_robot_config_allows_changes_after_robot_is_fully_stopped(self) -> None:
        user_id = "user-config-unlocked"
        main.auto_trader.get(user_id)

        with patch.object(main, "persist_robot"):
            response = await main.robot_config({"entry_value": 5}, {"user_id": user_id})
        payload = json.loads(response.body)["data"]

        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["entry_value"], 5)

    async def test_robot_start_requires_reset_cycle_after_stop_hit(self) -> None:
        user_id = "user-stop-blocked-start"
        state = main.auto_trader.get(user_id)
        state.status = "STOP_WIN_HIT"

        response = await main.robot_start({"user_id": user_id})
        payload = json.loads(response.body)

        self.assertEqual(response.status_code, 409)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"], "RESET_CYCLE_REQUIRED")
