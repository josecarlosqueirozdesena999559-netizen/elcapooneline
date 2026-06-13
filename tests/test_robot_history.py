import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from backend import main
from backend.auto_trader import AutoTrader
from backend.robot_persistence import SQLiteRobotPersistence


def final_trade(
    order_id: str,
    result: str,
    profit: float,
    *,
    finished_at: datetime,
    mode: str = "DEMO",
) -> dict:
    return {
        "order_id": order_id,
        "mode": mode,
        "active": "EURUSD-OTC",
        "direction": "CALL",
        "amount": 2,
        "confidence": 90,
        "payout": 88,
        "result": result,
        "profit": profit,
        "sent_at": (finished_at - timedelta(minutes=1)).isoformat(),
        "finished_at": finished_at.isoformat(),
        "expiration": "M1",
    }


class RobotHistoryPersistenceTests(unittest.TestCase):
    def test_history_is_filtered_by_user_days_and_most_recent_first(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            persistence = SQLiteRobotPersistence(str(Path(directory) / "robot.db"))
            now = datetime.now(timezone.utc)
            persistence.save_trade_history(
                "user-a",
                final_trade("recent", "WIN", 1.76, finished_at=now - timedelta(hours=1)),
            )
            persistence.save_trade_history(
                "user-a",
                final_trade("week", "LOSS", -2, finished_at=now - timedelta(days=5)),
            )
            persistence.save_trade_history(
                "user-a",
                final_trade("old", "WIN", 1.5, finished_at=now - timedelta(days=20)),
            )
            persistence.save_trade_history(
                "user-b",
                final_trade("other-user", "WIN", 2, finished_at=now),
            )

            today = persistence.load_trade_history("user-a", 1)
            week = persistence.load_trade_history("user-a", 7)
            month = persistence.load_trade_history("user-a", 30)

            self.assertEqual([item["order_id"] for item in today], ["recent"])
            self.assertEqual([item["order_id"] for item in week], ["recent", "week"])
            self.assertEqual(
                [item["order_id"] for item in month],
                ["recent", "week", "old"],
            )
            self.assertNotIn("other-user", {item["order_id"] for item in month})

    def test_history_upsert_keeps_one_row_per_user_and_order(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            persistence = SQLiteRobotPersistence(str(Path(directory) / "robot.db"))
            now = datetime.now(timezone.utc)
            trade = final_trade("same-order", "LOSS", -2, finished_at=now)

            persistence.save_trade_history("user-a", trade)
            persistence.save_trade_history("user-a", trade)

            items = persistence.load_trade_history("user-a", 1)
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["result"], "LOSS")


class RobotHistoryEndpointTests(unittest.IsolatedAsyncioTestCase):
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

    async def test_finished_trade_is_saved_and_returned_by_history(self) -> None:
        user_id = "user-finished"
        state = main.auto_trader.start(user_id)
        state.cycle_minutes = 10
        main.auto_trader.record_trade(
            user_id,
            {
                "order_id": "finished-1",
                "mode": "DEMO",
                "active": "EURUSD-OTC",
                "direction": "CALL",
                "amount": 2,
                "confidence": 92,
                "payout": 90,
                "result": "PENDING_RESULT",
                "expiration": "M1",
            },
        )

        self.assertEqual(main.robot_persistence.load_trade_history(user_id, 30), [])
        await main.finish_monitored_trade(user_id, "finished-1", "WIN", 1.8)
        response = await main.robot_history(30, {"user_id": user_id})
        payload = json.loads(response.body)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(payload["data"]["items"]), 1)
        item = payload["data"]["items"][0]
        self.assertEqual(item["order_id"], "finished-1")
        self.assertEqual(item["result"], "WIN")
        self.assertEqual(item["profit"], 1.8)
        self.assertIsNotNone(item["finished_at"])

    async def test_real_finished_trade_is_saved_with_real_account_mode(self) -> None:
        user_id = "user-real-finished"
        state = main.auto_trader.start(user_id)
        state.account_mode = "REAL"
        main.auto_trader.record_trade(
            user_id,
            {
                "order_id": "real-finished-1",
                "mode": "REAL",
                "active": "EURUSD-OTC",
                "direction": "PUT",
                "amount": 5,
                "confidence": 94,
                "payout": 87,
                "result": "PENDING_RESULT",
                "expiration": "M5",
            },
        )

        await main.finish_monitored_trade(user_id, "real-finished-1", "LOSS", -5)
        items = main.robot_persistence.load_trade_history(user_id, 30)

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["account_mode"], "REAL")
        self.assertEqual(items[0]["result"], "LOSS")
        self.assertEqual(items[0]["profit"], -5)

    async def test_stats_include_profit_factor_and_streaks(self) -> None:
        user_id = "user-stats"
        now = datetime.now(timezone.utc)
        trades = [
            final_trade("1", "WIN", 2, finished_at=now - timedelta(minutes=5)),
            final_trade("2", "WIN", 3, finished_at=now - timedelta(minutes=4)),
            final_trade("3", "LOSS", -2, finished_at=now - timedelta(minutes=3)),
            final_trade("4", "LOSS", -1, finished_at=now - timedelta(minutes=2)),
            final_trade("5", "WIN", 1, finished_at=now - timedelta(minutes=1)),
        ]
        for trade in trades:
            main.robot_persistence.save_trade_history(user_id, trade)

        response = await main.robot_stats(7, {"user_id": user_id})
        stats = json.loads(response.body)["data"]

        self.assertEqual(stats["wins"], 3)
        self.assertEqual(stats["losses"], 2)
        self.assertEqual(stats["total_trades"], 5)
        self.assertEqual(stats["win_rate"], 60.0)
        self.assertEqual(stats["profit"], 3.0)
        self.assertEqual(stats["profit_factor"], 2.0)
        self.assertEqual(stats["current_win_streak"], 1)
        self.assertEqual(stats["current_loss_streak"], 0)
        self.assertEqual(stats["best_win_streak"], 2)
        self.assertEqual(stats["best_loss_streak"], 2)

    async def test_endpoints_never_mix_users(self) -> None:
        now = datetime.now(timezone.utc)
        main.robot_persistence.save_trade_history(
            "user-a",
            final_trade("a-1", "WIN", 1, finished_at=now),
        )
        main.robot_persistence.save_trade_history(
            "user-b",
            final_trade("b-1", "LOSS", -2, finished_at=now),
        )

        history_response = await main.robot_history(30, {"user_id": "user-a"})
        stats_response = await main.robot_stats(30, {"user_id": "user-a"})
        history = json.loads(history_response.body)["data"]["items"]
        stats = json.loads(stats_response.body)["data"]

        self.assertEqual([item["order_id"] for item in history], ["a-1"])
        self.assertEqual(stats["wins"], 1)
        self.assertEqual(stats["losses"], 0)
