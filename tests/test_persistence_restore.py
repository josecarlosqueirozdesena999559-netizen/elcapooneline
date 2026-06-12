import asyncio
import hashlib
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

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
            self.assertNotIn("cookies", columns)
            self.assertNotIn("session_data", columns)
            self.assertNotEqual(encrypted_token, "sensitive-ssid-token")
            with self.assertLogs("bullex-service", level="INFO") as logs:
                restored = store.load_connected()
            self.assertEqual(restored[0].session_token, "sensitive-ssid-token")
            self.assertIn("ssid_length=20", "\n".join(logs.output))

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
                connect=Mock(side_effect=AssertionError("restore must not login with password")),
                restore_with_ssid=Mock(return_value=(True, None)),
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
            fake_client.restore_with_ssid.assert_called_once_with("persisted-ssid")
            fake_client.connect.assert_not_called()

    def test_session_manager_marks_restore_unsupported_without_login(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = SessionStore(str(Path(directory) / "sessions.db"), "test-secret")
            store.save_connected(
                "user-session",
                "user@example.com",
                "PRACTICE",
                "persisted-ssid",
            )

            fake_client = SimpleNamespace(
                connect=Mock(side_effect=AssertionError("restore must not login with password")),
                get_balance_mode=lambda: "PRACTICE",
            )
            manager = bullex_main.SessionManager(store)
            with (
                patch.object(bullex_main, "Bullex", return_value=fake_client),
                self.assertLogs("bullex-service", level="WARNING") as logs,
            ):
                manager.restore_sessions()

            self.assertIsNone(manager.get("user-session"))
            fake_client.connect.assert_not_called()
            self.assertIn(
                "[SESSION_RESTORE] status=unsupported reason=no_ssid_restore_method",
                "\n".join(logs.output),
            )

    def test_session_manager_marks_invalid_ssid_as_broker_unsupported(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = SessionStore(str(Path(directory) / "sessions.db"), "test-secret")
            store.save_connected("user-session", "user@example.com", "PRACTICE", "persisted-ssid")
            fake_client = SimpleNamespace(
                connect=Mock(side_effect=AssertionError("restore must not login with password")),
                restore_with_ssid=Mock(return_value=(False, "invalid_ssid")),
                get_balance_mode=lambda: "PRACTICE",
            )
            manager = bullex_main.SessionManager(store)
            with (
                patch.object(bullex_main, "Bullex", return_value=fake_client),
                patch.object(manager, "_session_context", side_effect=lambda session: _FakeContext(session)),
                self.assertLogs("bullex-service", level="WARNING") as logs,
            ):
                manager.restore_sessions()

            self.assertIsNone(manager.get("user-session"))
            fake_client.connect.assert_not_called()
            self.assertIn(
                "status=unsupported reason=broker_invalidates_ssid",
                "\n".join(logs.output),
            )

    def test_session_persistence_debug_does_not_expose_secrets(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = SessionStore(str(Path(directory) / "sessions.db"), "test-secret")
            store.save_connected(
                "user-session",
                "user@example.com",
                "PRACTICE",
                "persisted-ssid",
            )

            debug = store.persistence_debug()

            self.assertEqual(debug["stored_sessions"], 1)
            self.assertEqual(
                debug["users"][0],
                {
                    "user_id": "user-session",
                    "ssid_present": True,
                    "session_file_exists": True,
                    "last_connected_at": debug["users"][0]["last_connected_at"],
                },
            )
            self.assertNotIn("persisted-ssid", json.dumps(debug))

    def test_session_persistence_debug_endpoint_reports_missing_key(self) -> None:
        old_manager = bullex_main.session_manager
        bullex_main.session_manager = bullex_main.SessionManager(None)
        try:
            debug = bullex_main.sessions_persistence_debug()
        finally:
            bullex_main.session_manager = old_manager

        self.assertEqual(
            debug,
            {
                "stored_sessions": 0,
                "users": [],
            },
        )

    def test_persistence_debug_route_is_registered(self) -> None:
        routes = {
            route.path: route.methods
            for route in bullex_main.app.routes
            if hasattr(route, "methods")
        }

        self.assertIn("/sessions/persistence-debug", routes)
        self.assertIn("GET", routes["/sessions/persistence-debug"])

    def test_save_and_load_logs_have_matching_ssid_fingerprint(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = SessionStore(str(Path(directory) / "sessions.db"), "test-secret")
            with self.assertLogs("bullex-service", level="INFO") as logs:
                store.save_connected("user-session", "user@example.com", "PRACTICE", "persisted-ssid")
                store.load_connected()

            messages = "\n".join(logs.output)
            fingerprint = hashlib.sha256(b"persisted-ssid").hexdigest()[:12]
            self.assertIn(f"[SESSION_SAVE] user_id=user-session ssid_present=True ssid_length=14", messages)
            self.assertIn(f"[SESSION_LOAD] user_id=user-session ssid_present=True ssid_length=14", messages)
            self.assertEqual(messages.count("persisted_fields=ssid"), 2)
            self.assertEqual(messages.count("token_present=False"), 2)
            self.assertEqual(messages.count("cookies_present=False"), 2)
            self.assertEqual(messages.count("session_data_present=False"), 2)
            self.assertEqual(messages.count(f"ssid_fingerprint={fingerprint}"), 2)

    def test_gateway_persistence_debug_route_is_registered_and_forwards_raw_data(self) -> None:
        from backend import main

        routes = {
            route.path: route.methods
            for route in main.app.routes
            if hasattr(route, "methods")
        }
        expected = {
            "stored_sessions": 1,
            "users": [
                {
                    "user_id": "user-session",
                    "ssid_present": True,
                    "session_file_exists": True,
                    "last_connected_at": "2026-06-12T12:00:00+00:00",
                }
            ],
        }

        self.assertIn("/sessions/persistence-debug", routes)
        self.assertIn("GET", routes["/sessions/persistence-debug"])

        with patch.object(
            main,
            "call_bullex_service",
            new=AsyncMock(return_value=(200, main.build_success(expected))),
        ) as service_call:
            response = asyncio.run(main.sessions_persistence_debug())

        self.assertEqual(json.loads(response.body), expected)
        service_call.assert_awaited_once_with(
            "GET",
            "/sessions/persistence-debug",
            "persistence-debug",
        )


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
