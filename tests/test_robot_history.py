import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi.testclient import TestClient

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
    **extra,
) -> dict:
    trade = {
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
    trade.update(extra)
    return trade


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

    def test_history_persists_gale_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            persistence = SQLiteRobotPersistence(str(Path(directory) / "robot.db"))
            now = datetime.now(timezone.utc)
            persistence.save_trade_history(
                "user-gale",
                final_trade(
                    "gale-order",
                    "WIN",
                    3.52,
                    finished_at=now,
                    is_gale=True,
                    gale_step=1,
                    parent_order_id="base-order",
                    cycle_result="WIN",
                    final_result="WIN",
                    original_amount=2.0,
                    gale_amount=4.0,
                ),
            )

            items = persistence.load_trade_history("user-gale", 1)

            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["parent_order_id"], "base-order")
            self.assertEqual(items[0]["cycle_result"], "WIN")
            self.assertEqual(items[0]["final_result"], "WIN")
            self.assertEqual(items[0]["original_amount"], 2.0)
            self.assertEqual(items[0]["gale_amount"], 4.0)


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
        with self.assertLogs("backend-gateway", level="INFO") as logs:
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
        self.assertFalse(state.operation_in_progress)
        self.assertEqual(state.status, "RESULT_RECEIVED")
        self.assertIsNotNone(state.result_received_at)
        self.assertIsNotNone(state.result_display_until)
        self.assertEqual(state.to_dict()["seconds_until_next_cycle"], 0)
        output = "\n".join(logs.output)
        self.assertIn("[RESULT_RECEIVED]", output)
        self.assertIn("[RESULT_DISPLAY_UNTIL]", output)

    async def test_history_returns_in_memory_finished_trade_when_persistence_is_empty(self) -> None:
        user_id = "user-memory-history"
        main.auto_trader.start(user_id)
        main.auto_trader.record_trade(
            user_id,
            {
                "order_id": "memory-1",
                "mode": "DEMO",
                "active": "EURUSD-OTC",
                "direction": "CALL",
                "amount": 2,
                "confidence": 91,
                "payout": 87,
                "result": "PENDING_RESULT",
                "sent_at": "2026-06-18T12:00:00+00:00",
                "expiration": "M1",
            },
        )
        main.auto_trader.finish_trade(user_id, "memory-1", "WIN", 1.7)

        self.assertEqual(main.robot_persistence.load_trade_history(user_id, 30), [])
        response = await main.robot_history(30, {"user_id": user_id})
        payload = json.loads(response.body)

        self.assertEqual(response.status_code, 200)
        self.assertEqual([item["order_id"] for item in payload["data"]["items"]], ["memory-1"])
        self.assertEqual(payload["data"]["trades"][0]["result"], "WIN")

    async def test_history_without_items_returns_empty_success_payload(self) -> None:
        response = await main.robot_history(1, {"user_id": "user-empty"})
        payload = json.loads(response.body)

        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["data"], {"items": [], "trades": []})
        self.assertIsNone(payload["error"])

    async def test_stats_without_items_returns_empty_success_payload(self) -> None:
        response = await main.robot_stats(1, {"user_id": "user-empty"})
        payload = json.loads(response.body)

        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["ok"])
        self.assertEqual(
            payload["data"],
            {
                "wins": 0,
                "losses": 0,
                "total_trades": 0,
                "win_rate": 0.0,
                "profit": 0.0,
                "profit_factor": 0.0,
                "current_win_streak": 0,
                "current_loss_streak": 0,
                "best_win_streak": 0,
                "best_loss_streak": 0,
            },
        )
        self.assertIsNone(payload["error"])

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

    async def test_stats_count_only_final_cycle_result_when_history_has_entry_and_gale(self) -> None:
        user_id = "user-gale-stats"
        now = datetime.now(timezone.utc)
        main.robot_persistence.save_trade_history(
            user_id,
            final_trade(
                "base-1",
                "LOSS",
                -2,
                finished_at=now - timedelta(minutes=2),
                final_result=None,
                cycle_result=None,
            ),
        )
        main.robot_persistence.save_trade_history(
            user_id,
            final_trade(
                "gale-1",
                "LOSS",
                -4,
                finished_at=now - timedelta(minutes=1),
                is_gale=True,
                gale_step=1,
                parent_order_id="base-1",
                final_result="LOSS",
                cycle_result="LOSS",
                original_amount=2.0,
                gale_amount=4.0,
            ),
        )

        response = await main.robot_stats(7, {"user_id": user_id})
        stats = json.loads(response.body)["data"]

        self.assertEqual(stats["wins"], 0)
        self.assertEqual(stats["losses"], 1)
        self.assertEqual(stats["total_trades"], 1)
        self.assertEqual(stats["profit"], -6.0)

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


class RobotHistoryCorsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(main.app)

    def test_cors_configuration_matches_expected_contract(self) -> None:
        middleware = next(
            middleware
            for middleware in main.app.user_middleware
            if middleware.cls.__name__ == "CORSMiddleware"
        )

        self.assertEqual(
            middleware.kwargs["allow_origins"],
            [
                "https://www.elcapobot.online",
                "https://elcapobot.online",
                "http://localhost:3000",
                "http://localhost:5173",
                "http://localhost:5174",
            ],
        )
        self.assertEqual(middleware.kwargs["allow_methods"], ["GET", "POST", "OPTIONS"])
        self.assertEqual(middleware.kwargs["allow_headers"], main.CORS_ALLOWED_HEADERS)

    def test_options_robot_stats_returns_cors_headers_for_www_origin(self) -> None:
        response = self.client.options(
            "/robot/stats?days=1",
            headers={
                "Origin": "https://www.elcapobot.online",
                "Access-Control-Request-Method": "GET",
                "Access-Control-Request-Headers": "x-api-key,x-user-id",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.headers.get("access-control-allow-origin"),
            "https://www.elcapobot.online",
        )
        self.assertIn("GET", response.headers.get("access-control-allow-methods", ""))
        allow_headers = response.headers.get("access-control-allow-headers", "").lower()
        self.assertIn("x-api-key", allow_headers)
        self.assertIn("x-user-id", allow_headers)

    def test_options_robot_history_returns_cors_headers_for_www_origin(self) -> None:
        response = self.client.options(
            "/robot/history?days=1",
            headers={
                "Origin": "https://www.elcapobot.online",
                "Access-Control-Request-Method": "GET",
                "Access-Control-Request-Headers": "x-api-key,x-user-id",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.headers.get("access-control-allow-origin"),
            "https://www.elcapobot.online",
        )
        self.assertIn("GET", response.headers.get("access-control-allow-methods", ""))
        allow_headers = response.headers.get("access-control-allow-headers", "").lower()
        self.assertIn("x-api-key", allow_headers)
        self.assertIn("x-user-id", allow_headers)
