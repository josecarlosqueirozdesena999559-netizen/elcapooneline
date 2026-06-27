import json
import unittest
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import httpx

from backend import main
from backend.auto_trader import AutoTrader
from backend.user_store import SupabaseUserStore


class AsyncResponse:
    def __init__(self, status_code: int, payload: dict) -> None:
        self.status_code = status_code
        self._payload = payload
        self.is_success = 200 <= status_code < 400

    def json(self) -> dict:
        return self._payload


class AsyncClientContext:
    def __init__(self, response_factory) -> None:
        self._response_factory = response_factory

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def request(self, **kwargs):
        return self._response_factory(**kwargs)


class FailingUserStore:
    def save_connection(self, *_args, **_kwargs):
        raise httpx.HTTPStatusError(
            "bad request",
            request=httpx.Request("POST", "https://example.supabase.co/rest/v1/bullex_connections"),
            response=httpx.Response(400, text='{"message":"schema mismatch"}'),
        )

    update_connection = save_connection
    disconnect = save_connection

    def get_market_assets_snapshot(self, *_args, **_kwargs):
        return []

    def connection_upsert_diagnostic(self, user_id, payload=None):
        return {
            "table": "bullex_connections",
            "fields": ["user_id", "connected"],
            "payload": {"user_id": user_id, **(payload or {})},
        }


class SupabaseUserStoreTests(unittest.TestCase):
    def test_logs_url_payload_and_response_body_on_http_400(self) -> None:
        store = SupabaseUserStore("https://example.supabase.co", "service-key")
        request = httpx.Request(
            "POST",
            "https://example.supabase.co/rest/v1/bullex_connections?on_conflict=user_id",
        )
        response = httpx.Response(
            400,
            request=request,
            text='{"code":"PGRST204","message":"Could not find last_connected_at"}',
        )
        client = Mock()
        client.request.return_value = response
        context = Mock()
        context.__enter__ = Mock(return_value=client)
        context.__exit__ = Mock(return_value=False)

        with (
            patch("backend.user_store.httpx.Client", return_value=context),
            self.assertLogs("backend-gateway", level="WARNING") as logs,
            self.assertRaises(httpx.HTTPStatusError),
        ):
            store._request(
                "POST",
                "/bullex_connections?on_conflict=user_id",
                json={"user_id": "user-1", "connected": True},
            )

        output = "\n".join(logs.output)
        self.assertIn(str(request.url), output)
        self.assertIn("'user_id': 'user-1'", output)
        self.assertIn("Could not find last_connected_at", output)

    def test_retries_without_last_connected_at_for_old_schema(self) -> None:
        store = SupabaseUserStore("https://example.supabase.co", "service-key")
        request = httpx.Request(
            "POST",
            "https://example.supabase.co/rest/v1/bullex_connections?on_conflict=user_id",
        )
        response = httpx.Response(
            400,
            request=request,
            text='{"code":"PGRST204","message":"Could not find last_connected_at"}',
        )
        error = httpx.HTTPStatusError("bad request", request=request, response=response)
        store._request = Mock(
            side_effect=[
                error,
                [{"user_id": "user-1", "connected": True}],
            ]
        )

        record = store._upsert_connection(
            "user-1",
            {"connected": True, "last_connected_at": "2026-06-12T12:00:00+00:00"},
        )

        self.assertTrue(record.connected)
        retry_payload = store._request.call_args_list[1].kwargs["json"]
        self.assertNotIn("last_connected_at", retry_payload)


class EndpointResilienceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.old_store = main.user_store
        self.old_trader = main.auto_trader
        main.user_store = FailingUserStore()
        main.auto_trader = AutoTrader()
        main.session_response_cache.clear()
        main.robot_tasks.clear()
        main.recent_connect_users.clear()
        main.robot_start_requested_users.clear()

    def tearDown(self) -> None:
        main.user_store = self.old_store
        main.auto_trader = self.old_trader
        main.session_response_cache.clear()
        main.robot_tasks.clear()
        main.recent_connect_users.clear()
        main.robot_start_requested_users.clear()

    async def test_robot_state_stays_200_when_supabase_fails(self) -> None:
        state = main.auto_trader.start("user-robot")
        state.connected = True
        state.active_mode = "PRACTICE"

        with patch.object(main, "call_bullex_service", new=AsyncMock()) as service_call:
            response = await main.robot_state({"user_id": "user-robot"})

        body = json.loads(response.body)
        self.assertEqual(response.status_code, 200)
        self.assertTrue(body["data"]["connected"])
        service_call.assert_not_awaited()

    async def test_bullex_account_stays_200_when_supabase_fails(self) -> None:
        payload = main.build_success(
            {"connected": True, "email": "user@example.com", "mode": "PRACTICE"}
        )

        with patch.object(main, "call_bullex_service", new=AsyncMock(return_value=(200, payload))):
            response = await main.bullex_account({"user_id": "user-account"})

        self.assertEqual(response.status_code, 200)
        self.assertTrue(json.loads(response.body)["data"]["connected"])

    async def test_bullex_connect_stays_200_when_supabase_fails(self) -> None:
        payload = main.build_success(
            {"connected": True, "requires_2fa": False, "active_mode": "PRACTICE"}
        )

        with patch.object(main, "call_bullex_service", new=AsyncMock(return_value=(200, payload))):
            response = await main.bullex_connect(
                {"email": "user@example.com", "password": "not-persisted"},
                {"user_id": "user-connect"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(json.loads(response.body)["data"]["connected"])

    async def test_robot_state_cache_prevents_repeated_bullex_calls(self) -> None:
        user_id = "user-cache"
        state = main.auto_trader.start(user_id)
        state.connected = True
        state.active_mode = "PRACTICE"

        with patch.object(main, "call_bullex_service", new=AsyncMock()) as service_call:
            for _ in range(100):
                response = await main.robot_state({"user_id": user_id})
                self.assertEqual(response.status_code, 200)

        service_call.assert_not_awaited()

    async def test_bullex_account_returns_connected_false_instead_of_404(self) -> None:
        with patch.object(
            main,
            "call_bullex_service",
            new=AsyncMock(return_value=(404, {"ok": False, "data": {"connected": False}, "error": "SESSION_NOT_FOUND"})),
        ):
            response = await main.bullex_account({"user_id": "user-account-missing"})

        body = json.loads(response.body)
        self.assertEqual(response.status_code, 200)
        self.assertTrue(body["ok"])
        self.assertFalse(body["data"]["connected"])
        self.assertIsNone(body["error"])

    async def test_worker_is_not_started_while_user_is_offline(self) -> None:
        user_id = "user-offline-worker"
        state = main.auto_trader.start(user_id)
        state.enabled = True
        main.mark_session_failure(user_id, offline=True)

        main.ensure_robot_worker(user_id)

        self.assertNotIn(user_id, main.robot_tasks)

    async def test_sessions_status_is_throttled_for_15_seconds(self) -> None:
        user_id = "user-throttle"
        main.mark_connect_activity(user_id)
        requests: list[str] = []

        def response_factory(**kwargs):
            requests.append(kwargs["url"])
            return AsyncResponse(
                200,
                main.build_success({"connected": True, "active_mode": "PRACTICE", "server_time": 125.0}),
            )

        with patch("backend.main.httpx.AsyncClient", return_value=AsyncClientContext(response_factory)):
            first_status, first_payload = await main.call_bullex_service("GET", "/sessions/status", user_id)
            second_status, second_payload = await main.call_bullex_service("GET", "/sessions/status", user_id)

        self.assertEqual(first_status, 200)
        self.assertEqual(second_status, 200)
        self.assertEqual(first_payload, second_payload)
        self.assertEqual(len(requests), 1)

    async def test_sessions_status_returns_last_known_state_when_repeated_within_5_seconds(self) -> None:
        user_id = "user-throttle-stale"
        main.mark_connect_activity(user_id)
        requests: list[str] = []

        def response_factory(**kwargs):
            requests.append(kwargs["url"])
            return AsyncResponse(
                200,
                main.build_success({"connected": True, "active_mode": "PRACTICE", "server_time": 250.0}),
            )

        with patch("backend.main.httpx.AsyncClient", return_value=AsyncClientContext(response_factory)):
            first_status, first_payload = await main.call_bullex_service("GET", "/sessions/status", user_id)
            cached = main.get_session_cache(user_id).responses["/sessions/status"]
            cached.expires_at = main.utc_now() - main.timedelta(seconds=1)
            second_status, second_payload = await main.call_bullex_service("GET", "/sessions/status", user_id)

        self.assertEqual(first_status, 200)
        self.assertEqual(second_status, 200)
        self.assertEqual(first_payload, second_payload)
        self.assertEqual(len(requests), 1)

    async def test_refresh_entry_window_uses_robot_connection_cache_for_15_seconds(self) -> None:
        user_id = "user-entry-window-cache"
        state = main.auto_trader.start(user_id)
        state.timeframe = "M1"
        main.auto_trader.sync_connection(
            user_id,
            connected=True,
            active_mode="PRACTICE",
            source="bullex_service",
        )
        state = main.auto_trader.get(user_id)
        state.server_time = "2026-06-22T12:00:00+00:00"

        with patch.object(main, "call_bullex_service", new=AsyncMock()) as service_call:
            status_code, payload, window = await main.refresh_entry_window(user_id, state)

        self.assertEqual(status_code, 200)
        self.assertTrue(payload["ok"])
        self.assertTrue(payload["data"]["connected"])
        self.assertIsNotNone(window)
        self.assertIn(window["server_time_source"], {"bullex", "vps_fallback"})
        service_call.assert_not_awaited()

    def test_temporary_recovery_state_returns_to_waiting_next_cycle(self) -> None:
        user_id = "user-recovery"
        state = main.auto_trader.start(user_id)
        main.auto_trader.defer_cycle(
            user_id,
            "WAITING_RECOVERY",
            wait_seconds=1,
            rejection_reason="WAITING_RECOVERY",
        )

        state.next_cycle_at = main.utc_now()
        payload = state.to_dict()

        self.assertEqual(payload["status"], "WAITING_NEXT_CYCLE")
        self.assertTrue(state.enabled)

    async def test_debug_endpoint_returns_upsert_contract(self) -> None:
        response = await main.debug_bullex_connection_schema({"user_id": "user-debug"})
        body = json.loads(response.body)["data"]

        self.assertEqual(body["table"], "bullex_connections")
        self.assertEqual(body["payload"]["user_id"], "user-debug")
        self.assertIn("connected", body["fields"])


class MarketDataResilienceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.old_store = main.user_store
        self.old_trader = main.auto_trader
        main.user_store = main.create_user_store()
        main.auto_trader = AutoTrader()
        main.session_response_cache.clear()
        main.robot_tasks.clear()

    def tearDown(self) -> None:
        main.user_store = self.old_store
        main.auto_trader = self.old_trader
        main.session_response_cache.clear()
        main.robot_tasks.clear()

    async def test_bullex_assets_returns_stale_cache_on_failure(self) -> None:
        user_id = "user-assets-cache"
        state = main.auto_trader.start(user_id)
        state.connected = True
        state.active_mode = "PRACTICE"
        cached_assets = [{"symbol": "EURUSD-OTC", "name": "EURUSD-OTC", "enabled": True, "payout": 91}]
        main.get_session_cache(user_id).responses["/assets"] = main.BullexResponseCacheEntry(
            status_code=200,
            payload=main.build_assets_payload(cached_assets, source="bullex_service", stale=False),
            expires_at=main.utc_now() - timedelta(seconds=1),
        )

        with patch.object(
            main,
            "call_bullex_service",
            new=AsyncMock(return_value=(502, main.build_error("BULLEX_SERVICE_UNAVAILABLE"))),
        ):
            response = await main.bullex_assets({"user_id": user_id})

        body = json.loads(response.body)
        self.assertEqual(response.status_code, 200)
        self.assertTrue(body["ok"])
        self.assertEqual(body["data"], cached_assets)
        self.assertEqual(body["meta"]["source"], "cache")
        self.assertTrue(body["meta"]["syncing"])
        self.assertEqual(main.get_session_cache(user_id).assets_failure_count, 1)
        self.assertTrue(main.auto_trader.get(user_id).connected)

    async def test_bullex_assets_honors_backoff_and_uses_cache_without_refetch(self) -> None:
        user_id = "user-assets-backoff"
        cached_assets = [{"symbol": "GBPUSD-OTC", "name": "GBPUSD-OTC", "enabled": True}]
        cache = main.get_session_cache(user_id)
        cache.responses["/assets"] = main.BullexResponseCacheEntry(
            status_code=200,
            payload=main.build_assets_payload(cached_assets, source="bullex_service", stale=False),
            expires_at=main.utc_now() - timedelta(seconds=1),
        )
        cache.assets_next_retry_at = main.utc_now() + timedelta(seconds=10)

        with patch.object(main, "call_bullex_service", new=AsyncMock()) as service_call:
            response = await main.bullex_assets({"user_id": user_id})

        body = json.loads(response.body)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(body["data"], cached_assets)
        self.assertEqual(body["meta"]["source"], "cache")
        service_call.assert_not_awaited()

    async def test_bullex_payouts_does_not_mark_connected_user_as_disconnected(self) -> None:
        user_id = "user-payout-stable"
        main.user_store.save_connection(user_id, {"connected": True, "account_mode": "PRACTICE"})

        with patch.object(
            main,
            "call_bullex_service",
            new=AsyncMock(return_value=(409, {"ok": False, "data": {"connected": False}, "error": "SESSION_DISCONNECTED"})),
        ):
            response = await main.bullex_payouts(active="EURUSD-OTC", auth={"user_id": user_id})

        body = json.loads(response.body)
        self.assertEqual(response.status_code, 409)
        self.assertFalse(body["ok"])
        self.assertTrue(main.user_store.get_user(user_id).connected)

    async def test_bullex_account_keeps_connected_snapshot_on_false_negative(self) -> None:
        user_id = "user-account-stable"
        main.user_store.save_connection(
            user_id,
            {
                "connected": True,
                "account_mode": "PRACTICE",
                "currency": "USD",
                "last_balance": 42.5,
                "bullex_email": "user@example.com",
            },
        )
        state = main.auto_trader.start(user_id)
        state.connected = True
        state.active_mode = "PRACTICE"

        with patch.object(
            main,
            "call_bullex_service",
            new=AsyncMock(return_value=(409, {"ok": False, "data": {"connected": False}, "error": "SESSION_DISCONNECTED"})),
        ):
            response = await main.bullex_account({"user_id": user_id})

        body = json.loads(response.body)
        self.assertEqual(response.status_code, 200)
        self.assertTrue(body["ok"])
        self.assertTrue(body["data"]["connected"])
        self.assertEqual(body["data"]["active_mode"], "PRACTICE")
        self.assertEqual(body["data"]["balance"], 42.5)
