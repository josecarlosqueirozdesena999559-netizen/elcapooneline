import asyncio
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from backend.auto_trader import AutoTrader, STATUS_WAITING_NEXT_CYCLE
from backend.robot_persistence import SQLiteRobotPersistence
from bullex_service import main as bullex_main
from bullex_service.session_store import SessionStore


class SessionPersistenceTests(unittest.TestCase):
    def test_session_token_is_encrypted_and_password_is_never_stored(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = str(Path(directory) / "sessions.db")
            store = SessionStore(database_path, "test-secret")
            store.save_connected(
                "user-session",
                "user@example.com",
                "PRACTICE",
                "sensitive-ssid-token",
            )

            with closing(sqlite3.connect(database_path)) as connection:
                columns = {
                    row[1] for row in connection.execute("pragma table_info(bullex_sessions)").fetchall()
                }
                encrypted_token = connection.execute(
                    "select encrypted_session_token from bullex_sessions where user_id = ?",
                    ("user-session",),
                ).fetchone()[0]

            self.assertNotIn("password", columns)
            self.assertNotEqual(encrypted_token, "sensitive-ssid-token")
            restored = store.load_connected()
            self.assertEqual(restored[0].session_token, "sensitive-ssid-token")

    def test_session_manager_restores_with_persisted_ssid(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = SessionStore(str(Path(directory) / "sessions.db"), "test-secret")
            store.save_connected(
                "user-session",
                "user@example.com",
                "PRACTICE",
                "persisted-ssid",
            )

            fake_client = SimpleNamespace(
                connect=lambda: (True, None),
                get_balance_mode=lambda: "PRACTICE",
            )
            manager = bullex_main.SessionManager(store)
            with (
                patch.object(bullex_main, "Bullex", return_value=fake_client),
                patch.object(manager, "_session_context", side_effect=lambda session: _FakeContext(session)),
            ):
                manager.restore_sessions()

            restored = manager.get("user-session")
            self.assertIsNotNone(restored)
            self.assertEqual(restored.password, None)
            self.assertEqual(restored.state.SSID, "persisted-ssid")


class _FakeContext:
    def __init__(self, session) -> None:
        self.session = session

    def __enter__(self):
        return None

    def __exit__(self, *_args):
        return False


class RobotPersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_robot_state_and_history_survive_restart(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            persistence = SQLiteRobotPersistence(str(Path(directory) / "robot.db"))
            trader = AutoTrader()
            state = trader.start("user-robot")
            state.entry_value = 5
            state.cycle_minutes = 3
            state.min_confidence = 90
            state.min_payout = 82
            state.stop_win = 100
            state.stop_loss = 40
            trader.record_trade(
                "user-robot",
                {
                    "order_id": "order-1",
                    "active": "EURUSD-OTC",
                    "direction": "CALL",
                    "amount": 5,
                    "payout": 88,
                    "result": "PENDING_RESULT",
                    "sent_at": "2026-06-12T12:00:00+00:00",
                },
            )
            trader.finish_trade("user-robot", "order-1", "WIN", 4.4)
            persistence.save_state("user-robot", state.to_dict())
            persistence.save_trade("user-robot", state.last_trade)

            restored_trader = AutoTrader()
            user_id, payload = persistence.load_states()[0]
            restored = restored_trader.restore(user_id, payload, persistence.load_trades(user_id))

            self.assertTrue(restored.enabled)
            self.assertEqual(restored.status, STATUS_WAITING_NEXT_CYCLE)
            self.assertEqual(restored.entry_value, 5)
            self.assertEqual(restored.wins, 1)
            self.assertEqual(restored.profit, 4.4)
            self.assertEqual(restored_trader.history(user_id)["trades"][0]["order_id"], "order-1")

    async def test_startup_reactivates_enabled_robot_and_records_diagnostic(self) -> None:
        from backend import main

        persistence = SimpleNamespace(
            load_states=lambda: [("user-restore", {"enabled": True, "status": "STOPPED"})],
            load_trades=lambda _user_id: [],
            save_restore_status=unittest.mock.Mock(),
        )
        old_trader = main.auto_trader
        old_persistence = main.robot_persistence
        old_tasks = main.robot_tasks
        main.auto_trader = AutoTrader()
        main.robot_persistence = persistence
        main.robot_tasks = {}
        try:
            with (
                patch.object(main, "read_restored_session_status", new=AsyncMock(return_value=True)),
                patch.object(main, "ensure_robot_worker") as ensure_worker,
            ):
                await main.restore_robot_states()

            state = main.auto_trader.get("user-restore")
            self.assertTrue(state.enabled)
            self.assertNotEqual(state.status, "STOPPED")
            ensure_worker.assert_called_once_with("user-restore")
            persistence.save_restore_status.assert_called_once_with(
                "user-restore",
                session_restored=True,
                robot_restored=True,
            )
        finally:
            main.auto_trader = old_trader
            main.robot_persistence = old_persistence
            main.robot_tasks = old_tasks

    async def test_robot_state_reports_restored_connection(self) -> None:
        from backend import main

        old_trader = main.auto_trader
        main.auto_trader = AutoTrader()
        main.auto_trader.start("user-state")
        try:
            with (
                patch.object(
                    main,
                    "call_bullex_service",
                    new=AsyncMock(
                        return_value=(
                            200,
                            main.build_success(
                                {"connected": True, "active_mode": "PRACTICE"}
                            ),
                        )
                    ),
                ),
                patch.object(main, "sync_user_store_from_payload"),
            ):
                response = await main.robot_state({"user_id": "user-state"})

            payload = json.loads(response.body)
            self.assertTrue(payload["data"]["enabled"])
            self.assertNotEqual(payload["data"]["status"], "STOPPED")
            self.assertTrue(payload["data"]["connected"])
        finally:
            main.auto_trader = old_trader
