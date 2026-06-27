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
        main.auto_trader = AutoTrader()
        main.robot_tasks = {}
        main.restorable_robot_states.clear()
        main.robot_state_hydrated_users.clear()

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

    async def test_each_user_has_at_most_one_worker(self) -> None:
        user_id = "single-worker"
        state = main.auto_trader.start(user_id)
        state.connected = True
        state.active_mode = "PRACTICE"
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

    def test_startup_loads_metadata_without_constructing_bullex(self) -> None:
        store = Mock()
        store.load_connected.return_value = [self.persisted()]
        manager = bullex_main.SessionManager(store)

        with patch.object(bullex_main, "Bullex") as bullex:
            manager.restore_sessions()

        bullex.assert_not_called()
        self.assertEqual(manager.sessions, {})
        self.assertIn("restorable-user", manager.restorable_sessions)

    def test_on_demand_restore_reuses_single_session_and_websocket(self) -> None:
        store = Mock()
        store.load_connected.return_value = [self.persisted()]
        fake_client = SimpleNamespace(
            restore_with_ssid=Mock(return_value=(True, None)),
            check_connect=lambda: True,
            websocket_alive=lambda: True,
            get_balance_mode=lambda: "PRACTICE",
            get_balance=lambda: 100.0,
            get_currency=lambda: "USD",
            api=SimpleNamespace(close=Mock()),
        )
        manager = bullex_main.SessionManager(store)

        with (
            patch.object(bullex_main, "Bullex", return_value=fake_client) as bullex,
            patch.object(manager, "_persist_connected"),
        ):
            manager.restore_sessions()
            first = manager.restore_on_demand("restorable-user")
            second = manager.restore_on_demand("restorable-user")

        self.assertIs(first, second)
        bullex.assert_called_once()
        self.assertEqual(len(manager.sessions), 1)
        self.assertEqual(len(manager.websockets), 1)

    def test_disconnect_removes_restorable_metadata_without_opening_session(self) -> None:
        store = Mock()
        store.load_connected.return_value = [self.persisted()]
        manager = bullex_main.SessionManager(store)

        with patch.object(bullex_main, "Bullex") as bullex:
            manager.restore_sessions()
            manager.disconnect("restorable-user")

        bullex.assert_not_called()
        self.assertNotIn("restorable-user", manager.restorable_sessions)
        self.assertNotIn("restorable-user", manager.sessions)
        self.assertNotIn("restorable-user", manager.websockets)
        store.mark_disconnected.assert_called_with(
            "restorable-user",
            revoke_token=True,
        )


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
