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

import httpx

from backend.auto_trader import AutoTrader, STATUS_RESULT_RECEIVED
from backend.robot_persistence import (
    SQLiteRobotPersistence,
    SupabaseRobotPersistence,
    extract_robot_settings,
)
from bullex_service import main as bullex_main
from bullex_service.session_store import SessionStore


class SessionPersistenceTests(unittest.TestCase):
    def test_build_account_payload_keeps_real_connected_with_zero_balance(self) -> None:
        session = bullex_main.ManagedSession(
            user_id="user-real-zero",
            client=SimpleNamespace(
                check_connect=lambda: True,
                get_balance=lambda: 0,
                get_currency=lambda: "BRL",
                get_balance_mode=lambda: "REAL",
            ),
            email="real@example.com",
        )

        payload = bullex_main.build_account_payload(session)

        self.assertTrue(payload["connected"])
        self.assertEqual(payload["mode"], "REAL")
        self.assertEqual(payload["currency"], "BRL")
        self.assertEqual(payload["balance"], 0.0)
        self.assertEqual(payload["real_balance_warning"], "BALANCE_ZERO")

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

    def test_session_manager_reuses_existing_valid_session_without_new_login(self) -> None:
        manager = bullex_main.SessionManager(None)
        existing_client = SimpleNamespace(
            check_connect=lambda: True,
            websocket_alive=lambda: True,
            get_balance_mode=lambda: "PRACTICE",
            get_balance=lambda: 100.0,
            get_currency=lambda: "USD",
        )
        existing = bullex_main.ManagedSession(
            user_id="user-reuse",
            client=existing_client,
            email="user@example.com",
            password="secret",
            desired_mode="PRACTICE",
        )
        manager.upsert(existing)

        with patch.object(bullex_main, "Bullex", side_effect=AssertionError("should not create new session")):
            session = manager.connect(
                "user-reuse",
                bullex_main.ConnectRequest(email="user@example.com", password="secret", account_mode="PRACTICE"),
            )

        self.assertIs(session, existing)
        self.assertEqual(manager.login_progress_payload("user-reuse")["state"], "READY")

    def test_session_manager_retries_login_after_timeout(self) -> None:
        first_client = SimpleNamespace(api=SimpleNamespace(close=Mock()))
        second_client = SimpleNamespace(
            get_balance_mode=lambda: "PRACTICE",
            get_balance=lambda: 100.0,
            get_currency=lambda: "USD",
            check_connect=lambda: True,
            websocket_alive=lambda: True,
            connect=Mock(return_value=(True, None)),
        )
        manager = bullex_main.SessionManager(None)
        attempts = {"count": 0}

        def fake_run(operation, *, timeout_seconds=60):
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise TimeoutError("slow login")
            return operation()

        with (
            patch.object(bullex_main, "Bullex", side_effect=[first_client, second_client]),
            patch.object(manager, "_session_context", side_effect=lambda session: _FakeContext(session)),
            patch.object(manager, "_run_with_timeout", side_effect=fake_run),
            patch.object(bullex_main.time, "sleep"),
        ):
            session = manager.connect(
                "user-timeout",
                bullex_main.ConnectRequest(email="user@example.com", password="secret", account_mode="PRACTICE"),
            )

        self.assertIs(session.client, second_client)
        self.assertEqual(manager.login_progress_payload("user-timeout")["state"], "READY")
        second_client.connect.assert_called_once()

    def test_session_manager_reconnects_with_ssid_without_password(self) -> None:
        manager = bullex_main.SessionManager(None)
        old_client = SimpleNamespace(api=SimpleNamespace(close=Mock()))
        session = bullex_main.ManagedSession(
            user_id="user-ssid",
            client=old_client,
            email="user@example.com",
            password=None,
            desired_mode="PRACTICE",
            state=bullex_main.SessionState(SSID="persisted-ssid"),
        )
        fake_client = SimpleNamespace(
            restore_with_ssid=Mock(return_value=(True, None)),
            get_balance_mode=lambda: "PRACTICE",
            get_balance=lambda: 50.0,
            get_currency=lambda: "USD",
            check_connect=lambda: True,
            websocket_alive=lambda: True,
        )

        with (
            patch.object(bullex_main, "Bullex", return_value=fake_client),
            patch.object(manager, "_session_context", side_effect=lambda current: _FakeContext(current)),
        ):
            restored = manager._attempt_reconnect(session, "SESSION_EXPIRED")

        self.assertIs(restored.client, fake_client)
        fake_client.restore_with_ssid.assert_called_once_with("persisted-ssid")

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
    async def test_extract_robot_settings_keeps_only_supabase_settings_columns(self) -> None:
        settings = extract_robot_settings(
            {
                "user_id": "user-a",
                "enabled": True,
                "entry_value": "10.5",
                "stop_win": "50",
                "stop_loss": 30,
                "cycle_minutes": "5",
                "min_confidence": "94",
                "min_payout": "88.5",
                "strategy_mode": "balanced",
                "account_mode": "DEMO",
                "allow_real": "false",
                "confirm_real": "true",
                "max_entries_per_cycle": "2",
                "martingale_enabled": True,
                "martingale_steps": 2,
                "martingale_multiplier": 2.5,
                "wins": 9,
                "losses": 1,
                "profit": 20,
                "status": "RUNNING",
                "connected": True,
                "active_mode": "PRACTICE",
                "timeframe": "M1",
            }
        )

        self.assertEqual(
            settings,
            {
                "entry_value": 10.5,
                "stop_win": 50.0,
                "stop_loss": 30.0,
                "cycle_minutes": 5,
                "min_confidence": 94,
                "min_payout": 88.5,
                "strategy_mode": "balanced",
                "account_mode": "DEMO",
                "allow_real": False,
                "confirm_real": True,
                "max_entries_per_cycle": 2,
            },
        )

    async def test_supabase_settings_400_is_not_retried_until_settings_change(self) -> None:
        persistence = SupabaseRobotPersistence("https://example.supabase.co", "service-key")
        request = httpx.Request(
            "POST",
            "https://example.supabase.co/rest/v1/robot_user_settings?on_conflict=user_id",
        )
        response = httpx.Response(400, request=request, text='{"message":"bad column"}')
        error = httpx.HTTPStatusError("bad request", request=request, response=response)
        persistence._ensure_user = Mock()
        persistence._request = Mock(side_effect=error)
        first_settings = {"entry_value": 10, "allow_real": False}

        persistence.save_settings("user-a", first_settings)
        persistence.save_settings("user-a", dict(first_settings))
        persistence.save_settings("user-a", {"entry_value": 11, "allow_real": False})

        self.assertEqual(persistence._ensure_user.call_count, 2)
        self.assertEqual(persistence._request.call_count, 2)
        self.assertEqual(
            persistence._request.call_args_list[0].kwargs["json"],
            {"user_id": "user-a", "entry_value": 10.0, "allow_real": False},
        )
        self.assertEqual(
            persistence._request.call_args_list[1].kwargs["json"],
            {"user_id": "user-a", "entry_value": 11.0, "allow_real": False},
        )

    async def test_extract_robot_settings_normalizes_practice_account_mode(self) -> None:
        settings = extract_robot_settings(
            {
                "entry_value": 2,
                "account_mode": "PRACTICE",
                "strategy_mode": "conservative",
            }
        )

        self.assertEqual(settings["account_mode"], "DEMO")

    async def test_robot_user_settings_survive_restart_without_cross_user_leak(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = str(Path(directory) / "robot-settings.db")
            persistence = SQLiteRobotPersistence(database_path)
            persistence.save_settings(
                "user-a",
                {
                    "entry_value": 15,
                    "stop_win": 80,
                    "stop_loss": 25,
                    "cycle_minutes": 5,
                    "min_confidence": 96,
                    "min_payout": 90,
                    "strategy_mode": "balanced",
                    "account_mode": "DEMO",
                    "allow_real": False,
                    "confirm_real": False,
                    "max_entries_per_cycle": 1,
                    "martingale_enabled": True,
                    "martingale_steps": 1,
                    "martingale_multiplier": 2.5,
                },
            )
            persistence.save_settings(
                "user-b",
                {
                    "entry_value": 2,
                    "stop_win": 50,
                    "stop_loss": 12,
                    "cycle_minutes": 5,
                    "min_confidence": 94,
                    "min_payout": 88,
                    "strategy_mode": "conservative",
                    "account_mode": "DEMO",
                    "allow_real": False,
                    "confirm_real": False,
                    "max_entries_per_cycle": 1,
                    "martingale_enabled": False,
                    "martingale_steps": 1,
                    "martingale_multiplier": 2,
                },
            )

            restarted = SQLiteRobotPersistence(database_path)
            settings_a = restarted.load_settings("user-a")
            settings_b = restarted.load_settings("user-b")

            self.assertEqual(settings_a["entry_value"], 15)
            self.assertEqual(settings_a["stop_loss"], 25)
            self.assertTrue(settings_a["martingale_enabled"])
            self.assertEqual(settings_a["martingale_multiplier"], 2.5)
            self.assertEqual(settings_b["entry_value"], 2)
            self.assertEqual(settings_b["stop_loss"], 12)
            self.assertFalse(settings_b["martingale_enabled"])
            self.assertIsNone(restarted.load_settings("user-new"))

    async def test_robot_state_loads_dedicated_settings_after_memory_reset(self) -> None:
        from backend import main

        with tempfile.TemporaryDirectory() as directory:
            persistence = SQLiteRobotPersistence(
                str(Path(directory) / "robot-settings-state.db")
            )
            old_trader = main.auto_trader
            old_persistence = main.robot_persistence
            main.auto_trader = AutoTrader()
            main.robot_persistence = persistence
            try:
                state_a = main.get_user_robot_state("user-a")
                state_a.entry_value = 15
                state_a.stop_loss = 22
                main.persist_robot("user-a")

                state_b = main.get_user_robot_state("user-b")
                state_b.stop_loss = 11
                main.persist_robot("user-b")

                main.auto_trader = AutoTrader()
                refreshed_a = main.get_user_robot_state("user-a")
                refreshed_b = main.get_user_robot_state("user-b")
                new_user = main.get_user_robot_state("user-new")

                self.assertEqual(refreshed_a.entry_value, 15)
                self.assertEqual(refreshed_a.stop_loss, 22)
                self.assertEqual(refreshed_b.entry_value, 2)
                self.assertEqual(refreshed_b.stop_loss, 11)
                self.assertEqual(new_user.entry_value, 2)
                self.assertEqual(new_user.cycle_minutes, 5)
                self.assertEqual(new_user.min_confidence, 80)
                self.assertEqual(new_user.min_payout, 80)
            finally:
                main.auto_trader = old_trader
                main.robot_persistence = old_persistence

    async def test_persist_robot_continues_when_save_settings_fails(self) -> None:
        from backend import main

        class _PersistenceStub:
            def __init__(self) -> None:
                self.saved_state = False
                self.saved_trade = False
                self.saved_settings_payload = None

            def save_state(self, user_id: str, state: dict[str, object]) -> None:
                self.saved_state = True

            def save_settings(self, user_id: str, settings: dict[str, object]) -> None:
                self.saved_settings_payload = settings
                raise RuntimeError("settings boom")

            def save_trade(self, user_id: str, trade: dict[str, object]) -> None:
                self.saved_trade = True

        old_trader = main.auto_trader
        old_persistence = main.robot_persistence
        main.auto_trader = AutoTrader()
        stub = _PersistenceStub()
        main.robot_persistence = stub
        try:
            state = main.auto_trader.start("user-save-settings-error")
            state.last_trade = {"order_id": "order-1"}
            with self.assertLogs("backend-gateway", level="WARNING") as logs:
                main.persist_robot("user-save-settings-error")

            self.assertTrue(stub.saved_state)
            self.assertTrue(stub.saved_trade)
            self.assertEqual(stub.saved_settings_payload["account_mode"], "DEMO")
            self.assertIn("step=save_settings", "\n".join(logs.output))
        finally:
            main.auto_trader = old_trader
            main.robot_persistence = old_persistence

    async def test_robot_settings_requires_user_id(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            persistence = SQLiteRobotPersistence(str(Path(directory) / "robot.db"))

            with self.assertRaisesRegex(ValueError, "USER_ID_REQUIRED"):
                persistence.save_settings("", {})
            with self.assertRaisesRegex(ValueError, "USER_ID_REQUIRED"):
                persistence.load_settings("")

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

            self.assertEqual(persistence.load_state("user-robot")["entry_value"], 5)
            self.assertIsNone(persistence.load_state("other-user"))
            restored_trader = AutoTrader()
            user_id, payload = persistence.load_states()[0]
            restored = restored_trader.restore(user_id, payload, persistence.load_trades(user_id))

            self.assertTrue(restored.enabled)
            self.assertEqual(restored.status, STATUS_RESULT_RECEIVED)
            self.assertEqual(restored.entry_value, 5)
            self.assertEqual(restored.wins, 1)
            self.assertEqual(restored.profit, 4.4)
            self.assertEqual(restored_trader.history(user_id)["trades"][0]["order_id"], "order-1")
            self.assertEqual(restored_trader.source(user_id), "memory")

    async def test_startup_reactivates_enabled_robot_and_records_diagnostic(self) -> None:
        from backend import main

        persistence = SimpleNamespace(
            load_states=lambda: [("user-restore", {"enabled": True, "status": "STOPPED"})],
            load_state=lambda _user_id: None,
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
            self.assertTrue(state.connected)
            self.assertEqual(state.connection_status_source, "bullex_service")
            self.assertEqual(state.entry_value, 2)
            self.assertEqual(state.min_confidence, 80)
            self.assertEqual(state.min_payout, 80)
            self.assertEqual(main.auto_trader.source("user-restore"), "memory")
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

    async def test_restore_pending_signal_resumes_sending_order_worker(self) -> None:
        from backend import main
        from backend.auto_trader import STATUS_SENDING_ORDER

        persistence = SimpleNamespace(
            load_states=lambda: [
                (
                    "user-pending-restore",
                    {
                        "enabled": True,
                        "status": "SENDING_ORDER",
                        "pending_signal": {
                            "symbol": "EURUSD-OTC",
                            "signal": "CALL",
                            "direction": "CALL",
                            "confidence": 94,
                            "payout": 88,
                            "strategy_score": 94,
                        },
                    },
                )
            ],
            load_state=lambda _user_id: None,
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

            state = main.auto_trader.get("user-pending-restore")
            self.assertTrue(state.enabled)
            self.assertTrue(state.connected)
            self.assertEqual(state.status, STATUS_SENDING_ORDER)
            self.assertIsNotNone(state.pending_signal)
            ensure_worker.assert_called_once_with("user-pending-restore")
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
