import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from backend import main
from backend.auto_trader import AutoTrader, RobotConfigUpdate
from backend.robot_persistence import SQLiteRobotPersistence
from bullex_service import main as bullex_main
from bullex_service.session_store import PersistedSession


class FakeWebSocket:
    def __init__(self) -> None:
        self.accepted = False
        self.closed = False

    async def accept(self) -> None:
        self.accepted = True

    async def close(self, code: int) -> None:
        self.closed = True


class WorkerLifecycleTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.old_trader = main.auto_trader
        self.old_tasks = main.robot_tasks
        self.old_persistence = main.robot_persistence
        self.old_restorable = dict(main.restorable_robot_states)
        self.old_hydrated = set(main.robot_state_hydrated_users)
        self.old_active_users = dict(main.active_users)
        main.auto_trader = AutoTrader()
        main.robot_tasks = {}
        main.restorable_robot_states.clear()
        main.robot_state_hydrated_users.clear()
        main.active_users.clear()

    async def asyncTearDown(self) -> None:
        for user_id in list(main.robot_tasks):
            await main.stop_robot_worker(user_id)
        main.auto_trader = self.old_trader
        main.robot_tasks = self.old_tasks
        main.robot_persistence = self.old_persistence
        main.restorable_robot_states.clear()
        main.restorable_robot_states.update(self.old_restorable)
        main.robot_state_hydrated_users.clear()
        main.robot_state_hydrated_users.update(self.old_hydrated)
        main.active_users.clear()
        main.active_users.update(self.old_active_users)

    async def test_each_user_has_at_most_one_worker(self) -> None:
        user_id = "single-worker"
        state = main.auto_trader.start(user_id)
        state.connected = True
        state.active_mode = "PRACTICE"
        main.mark_user_active(user_id)
        blocker = asyncio.Event()

        async def idle_worker(_user_id: str) -> None:
            await blocker.wait()

        with (
            patch.object(main, "robot_worker", side_effect=idle_worker) as worker,
            self.assertLogs("backend-gateway", level="INFO") as logs,
        ):
            main.ensure_robot_worker(user_id)
            first_task = main.robot_tasks[user_id]
            main.ensure_robot_worker(user_id)
            self.assertIs(main.robot_tasks[user_id], first_task)
            await main.stop_robot_worker(user_id)

        worker.assert_called_once_with(user_id)
        output = "\n".join(logs.output)
        self.assertIn("[WORKER_CREATED]", output)
        self.assertIn("[WORKER_ALREADY_RUNNING]", output)

    async def test_disconnected_user_never_gets_worker(self) -> None:
        user_id = "offline-no-worker"
        state = main.auto_trader.start(user_id)
        state.connected = False
        state.active_mode = None

        with patch.object(main, "robot_worker", new=AsyncMock()) as worker:
            main.ensure_robot_worker(user_id)

        self.assertNotIn(user_id, main.robot_tasks)
        worker.assert_not_called()

    async def test_market_websocket_is_unique_per_user(self) -> None:
        manager = main.ConnectionManager()
        first = FakeWebSocket()
        second = FakeWebSocket()

        await manager.connect("user-ws", "EURUSD-OTC", first)
        await manager.connect("user-ws", "GBPUSD-OTC", second)

        self.assertEqual(len(manager._connections), 1)
        self.assertTrue(first.closed)
        self.assertTrue(second.accepted)
        self.assertIs(manager._connections["user-ws"][1], second)

    async def test_robot_start_revalidates_persisted_connection_on_demand(self) -> None:
        user_id = "start-restorable-session"
        main.restorable_robot_states[user_id] = {
            "enabled": True,
            "connected": True,
            "active_mode": "PRACTICE",
            "connection_checked_at": main.utc_now().isoformat(),
            "account_mode": "DEMO",
        }
        main.robot_persistence = SimpleNamespace(
            load_settings=lambda _user_id: None,
            load_trades=lambda _user_id: [],
        )
        upstream_payload = main.build_success(
            {"connected": True, "active_mode": "PRACTICE"}
        )

        with (
            patch.object(
                main,
                "call_bullex_service",
                new=AsyncMock(return_value=(200, upstream_payload)),
            ) as upstream,
            patch.object(main, "persist_robot"),
            patch.object(main, "ensure_robot_worker"),
        ):
            response = await main.robot_start({"user_id": user_id})

        self.assertEqual(response.status_code, 200)
        upstream.assert_awaited()
        self.assertEqual(upstream.await_args.args[1], "/sessions/status")
        self.assertTrue(upstream.await_args.kwargs["allow_session_restore"])


class SessionRestoreLifecycleTests(unittest.TestCase):
    @staticmethod
    def persisted() -> PersistedSession:
        return PersistedSession(
            user_id="restorable-user",
            email="user@example.com",
            account_mode="PRACTICE",
            session_token="persisted-ssid",
            last_connected_at=None,
        )

    def test_manager_starts_empty_without_loading_any_user(self) -> None:
        store = Mock()

        with patch.object(bullex_main, "Bullex") as bullex:
            manager = bullex_main.SessionManager(store)

        self.assertEqual(
            bullex_main.app.router.on_startup,
            [bullex_main.startup_without_session_restore],
        )
        with self.assertLogs("bullex-service", level="INFO") as logs:
            bullex_main.startup_without_session_restore()
        self.assertIn("[STARTUP_READY] restore disabled", "\n".join(logs.output))
        self.assertNotIn("restore_persisted_sessions", vars(bullex_main))
        bullex.assert_not_called()
        store.load_connected_user.assert_not_called()
        self.assertEqual(manager.sessions, {})
        self.assertEqual(manager.websockets, {})
        self.assertEqual(manager.workers, {})

    def test_on_demand_restore_reuses_single_session_and_websocket(self) -> None:
        store = Mock()
        store.load_connected_user.return_value = self.persisted()
        fake_client = SimpleNamespace(
            restore_with_ssid=Mock(return_value=(True, None)),
            check_connect=lambda: True,
            websocket_alive=lambda: True,
            get_balance_mode=lambda: "REAL",
            get_balance=lambda: 100.0,
            get_currency=lambda: "USD",
            api=SimpleNamespace(close=Mock()),
        )
        manager = bullex_main.SessionManager(store)

        with (
            patch.object(bullex_main, "Bullex", return_value=fake_client) as bullex,
            patch.object(manager, "_persist_connected"),
        ):
            first = manager.restore_on_demand("restorable-user")
            second = manager.restore_on_demand("restorable-user")

        self.assertIs(first, second)
        bullex.assert_called_once()
        self.assertEqual(len(manager.sessions), 1)
        self.assertEqual(len(manager.websockets), 1)

    def test_disconnect_does_not_load_persisted_session(self) -> None:
        store = Mock()
        manager = bullex_main.SessionManager(store)

        with patch.object(bullex_main, "Bullex") as bullex:
            manager.disconnect("restorable-user")

        bullex.assert_not_called()
        store.load_connected_user.assert_not_called()
        self.assertNotIn("restorable-user", manager.sessions)
        self.assertNotIn("restorable-user", manager.websockets)
        store.mark_disconnected.assert_called_with(
            "restorable-user",
            revoke_token=True,
        )

    def test_status_polling_does_not_restore_persisted_session(self) -> None:
        store = Mock()
        manager = bullex_main.SessionManager(store)
        old_manager = bullex_main.session_manager
        bullex_main.session_manager = manager
        try:
            response = bullex_main.session_status(
                x_user_id="polling-user",
                x_allow_session_restore=None,
            )
        finally:
            bullex_main.session_manager = old_manager

        self.assertEqual(response.status_code, 404)
        store.load_connected_user.assert_not_called()
        self.assertEqual(manager.sessions, {})
        self.assertEqual(manager.websockets, {})

    def test_status_restore_returns_controlled_404_when_not_persisted(self) -> None:
        store = Mock()
        store.load_connected_user.return_value = None
        manager = bullex_main.SessionManager(store)
        old_manager = bullex_main.session_manager
        bullex_main.session_manager = manager
        try:
            response = bullex_main.session_status(
                x_user_id="missing-restored-user",
                x_allow_session_restore="true",
            )
        finally:
            bullex_main.session_manager = old_manager

        self.assertEqual(response.status_code, 404)
        store.load_connected_user.assert_called_once_with("missing-restored-user")
        self.assertEqual(manager.sessions, {})
        self.assertEqual(manager.websockets, {})

    def test_fresh_connect_clears_only_current_users_old_session(self) -> None:
        store = Mock()
        old_api = SimpleNamespace(close=Mock())
        old_client = SimpleNamespace(api=old_api)
        other_client = SimpleNamespace(api=SimpleNamespace(close=Mock()))
        new_client = SimpleNamespace(
            api=SimpleNamespace(close=Mock()),
            connect=Mock(return_value=(True, None)),
            check_connect=lambda: True,
            websocket_alive=lambda: True,
            get_balance_mode=lambda: "REAL",
            get_balance=lambda: 100.0,
            get_currency=lambda: "USD",
        )
        manager = bullex_main.SessionManager(store)
        manager.upsert(
            bullex_main.ManagedSession(
                user_id="current-user",
                client=old_client,
                email="old@example.com",
            )
        )
        other_session = manager.upsert(
            bullex_main.ManagedSession(
                user_id="other-user",
                client=other_client,
                email="other@example.com",
            )
        )
        probe = manager.get_probe_state("current-user")
        probe.failure_count = 3
        probe.next_retry_at = 9999999999

        with (
            patch.object(bullex_main, "Bullex", return_value=new_client) as bullex,
            self.assertLogs("bullex-service", level="INFO") as logs,
        ):
            connected = manager.connect(
                "current-user",
                bullex_main.ConnectRequest(
                    email="new@example.com",
                    password="secret",
                    account_mode="PRACTICE",
                ),
            )

        self.assertIs(manager.get("current-user"), connected)
        self.assertIs(manager.get("other-user"), other_session)
        self.assertEqual(len(manager.sessions), 2)
        self.assertEqual(len(manager.websockets), 2)
        self.assertEqual(manager.get_probe_state("current-user").failure_count, 0)
        self.assertEqual(manager.get_probe_state("current-user").next_retry_at, 0.0)
        old_api.close.assert_called_once()
        other_client.api.close.assert_not_called()
        bullex.assert_called_once_with("new@example.com", "secret")
        output = "\n".join(logs.output)
        for marker in (
            "[CONNECT_REQUEST]",
            "[CONNECT_CLEAR_OLD_SESSION]",
            "[CONNECT_OLD_SESSION_CLOSED]",
            "[CONNECT_BACKOFF_CLEARED]",
            "[CONNECT_ATTEMPT]",
            "[CONNECT_CREATE_SESSION]",
            "[CONNECT_WS_START]",
            "[CONNECT_SUCCESS]",
        ):
            self.assertIn(marker, output)

    def test_connect_endpoint_handles_unexpected_error_and_cleans_session(self) -> None:
        store = Mock()
        api = SimpleNamespace(close=Mock())
        manager = bullex_main.SessionManager(store)
        manager.upsert(
            bullex_main.ManagedSession(
                user_id="failing-user",
                client=SimpleNamespace(api=api),
                email="old@example.com",
            )
        )
        old_manager = bullex_main.session_manager
        bullex_main.session_manager = manager
        try:
            with (
                patch.object(manager, "connect", side_effect=RuntimeError("login failed")),
                self.assertLogs("bullex-service", level="WARNING") as logs,
            ):
                response = bullex_main.connect_session(
                    bullex_main.ConnectRequest(
                        email="new@example.com",
                        password="secret",
                    ),
                    x_user_id="failing-user",
                )
        finally:
            bullex_main.session_manager = old_manager

        self.assertEqual(response.status_code, 503)
        self.assertNotIn("failing-user", manager.sessions)
        self.assertNotIn("failing-user", manager.websockets)
        api.close.assert_called_once_with()
        store.mark_disconnected.assert_called_once_with(
            "failing-user",
            revoke_token=True,
        )
        self.assertIn("[CONNECT_FAILED_HANDLED]", "\n".join(logs.output))

    def test_connected_session_check_is_reused_for_market_calls(self) -> None:
        check_connect = Mock(return_value=True)
        client = SimpleNamespace(
            check_connect=check_connect,
            websocket_alive=Mock(return_value=True),
            get_balance_mode=Mock(return_value="PRACTICE"),
            api=SimpleNamespace(close=Mock()),
        )
        manager = bullex_main.SessionManager(Mock())
        manager.upsert(
            bullex_main.ManagedSession(
                user_id="cached-session-user",
                client=client,
            )
        )

        with self.assertLogs("bullex-service", level="INFO") as logs:
            for _ in range(3):
                result = manager.run(
                    "cached-session-user",
                    lambda _session: "ok",
                    disconnect_on_error=False,
                )
                self.assertEqual(result, "ok")

        self.assertEqual(check_connect.call_count, 1)
        self.assertEqual(
            "\n".join(logs.output).count("[SESSION-CHECK]"),
            1,
        )

    def test_candles_cache_ignores_count_and_endtime_for_same_asset_timeframe(self) -> None:
        get_candles = Mock(return_value=[])
        client = SimpleNamespace(
            check_connect=Mock(return_value=True),
            websocket_alive=Mock(return_value=True),
            get_balance_mode=Mock(return_value="PRACTICE"),
            get_candles=get_candles,
            api=SimpleNamespace(close=Mock()),
        )
        manager = bullex_main.SessionManager(Mock())
        manager.upsert(
            bullex_main.ManagedSession(
                user_id="candles-cache-user",
                client=client,
            )
        )
        old_manager = bullex_main.session_manager
        bullex_main.session_manager = manager
        try:
            first = bullex_main.get_candles(
                active="EURUSD-OTC",
                interval=60,
                count=10,
                endtime=100,
                x_user_id="candles-cache-user",
            )
            second = bullex_main.get_candles(
                active="EURUSD-OTC",
                interval=60,
                count=80,
                endtime=200,
                x_user_id="candles-cache-user",
            )
        finally:
            bullex_main.session_manager = old_manager

        self.assertTrue(first["ok"])
        self.assertEqual(second, first)
        get_candles.assert_called_once_with("EURUSD-OTC", 60, 10, 100)


class MaxEntriesPersistenceTests(unittest.TestCase):
    def test_partial_settings_save_preserves_saved_max_entries(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            persistence = SQLiteRobotPersistence(
                str(Path(directory) / "max-entries.db")
            )
            persistence.save_settings(
                "max-entries-user",
                {"max_entries_per_cycle": 4, "stop_loss": 30},
            )
            persistence.save_settings(
                "max-entries-user",
                {"stop_loss": 25},
            )

            settings = persistence.load_settings("max-entries-user")

        self.assertEqual(settings["max_entries_per_cycle"], 4)
        self.assertEqual(settings["stop_loss"], 25)

    def test_config_model_does_not_hardcode_max_entries_to_one(self) -> None:
        trader = AutoTrader()
        update = RobotConfigUpdate(maxEntriesPerCycle=4)

        state = trader.update_config("max-entries-user", update)
        trader.update_config(
            "max-entries-user",
            RobotConfigUpdate(stop_loss=20),
        )

        self.assertEqual(state.max_entries_per_cycle, 4)


if __name__ == "__main__":
    unittest.main()
