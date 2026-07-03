import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from bullex_service import main
from bullexapi.stable_api import Bullex


class FakeAssetsClient:
    def __init__(self) -> None:
        self.calls = 0

    def update_ACTIVES_OPCODE(self, timeout=8) -> None:
        self.calls += 1

    def get_all_ACTIVES_OPCODE(self) -> dict[str, int]:
        return {"EURUSD-OTC": 1, "GBPUSD-OTC": 2}


class BullexInstrumentsCacheTests(unittest.TestCase):
    def setUp(self) -> None:
        self.old_session_manager = main.session_manager
        main.session_manager = main.SessionManager(None)

    def tearDown(self) -> None:
        main.session_manager = self.old_session_manager

    def test_read_assets_uses_valid_cache_without_second_instruments_call(self) -> None:
        client = FakeAssetsClient()

        first = main.read_assets(client, user_id="cache-user")
        second = main.read_assets(client, user_id="cache-user")

        self.assertEqual(client.calls, 1)
        self.assertEqual(first, second)
        self.assertEqual([asset["symbol"] for asset in second], ["EURUSD-OTC", "GBPUSD-OTC"])

    def test_read_assets_returns_stale_cache_during_backoff(self) -> None:
        user_id = "backoff-user"
        state = main.session_manager.get_instruments_cache_state(user_id)
        state.assets = [{"symbol": "EURUSD-OTC", "name": "EURUSD-OTC", "enabled": True}]
        state.expires_at = time.monotonic() - 1
        state.next_retry_at = time.monotonic() + 30
        client = FakeAssetsClient()

        assets = main.read_assets(client, user_id=user_id)

        self.assertEqual(client.calls, 0)
        self.assertEqual(assets, state.assets)

    def test_read_assets_reuses_stale_cache_when_same_user_lock_is_busy(self) -> None:
        user_id = "lock-user"
        state = main.session_manager.get_instruments_cache_state(user_id)
        state.assets = [{"symbol": "GBPUSD-OTC", "name": "GBPUSD-OTC", "enabled": True}]
        state.expires_at = time.monotonic() - 1
        self.assertTrue(state.lock.acquire(blocking=False))
        try:
            assets = main.read_assets(FakeAssetsClient(), user_id=user_id)
        finally:
            state.lock.release()

        self.assertEqual(assets, state.assets)

    def test_read_assets_failure_sets_backoff_without_disconnect_error(self) -> None:
        user_id = "error-user"
        client = FakeAssetsClient()

        with patch.object(main, "read_assets_uncached", side_effect=TimeoutError("stuck")):
            with self.assertRaises(main.ServiceError) as raised:
                main.read_assets(client, user_id=user_id)

        state = main.session_manager.get_instruments_cache_state(user_id)
        self.assertEqual(raised.exception.message, "INSTRUMENTS_TIMEOUT")
        self.assertGreater(state.next_retry_at, time.monotonic())


class StableApiInstrumentLoopTests(unittest.TestCase):
    def test_get_instruments_times_out_instead_of_spinning_forever(self) -> None:
        client = object.__new__(Bullex)
        client.suspend = 0
        client.api = SimpleNamespace(
            instruments=None,
            get_instruments=lambda _type: None,
        )
        client.connect = lambda: None

        started_at = time.monotonic()
        with self.assertRaises(TimeoutError):
            client.get_instruments("forex", timeout=0.15)

        self.assertLess(time.monotonic() - started_at, 1.0)


if __name__ == "__main__":
    unittest.main()
