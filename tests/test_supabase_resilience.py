import json
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import httpx

from backend import main
from backend.auto_trader import AutoTrader
from backend.user_store import SupabaseUserStore


class FailingUserStore:
    def save_connection(self, *_args, **_kwargs):
        raise httpx.HTTPStatusError(
            "bad request",
            request=httpx.Request("POST", "https://example.supabase.co/rest/v1/bullex_connections"),
            response=httpx.Response(400, text='{"message":"schema mismatch"}'),
        )

    update_connection = save_connection
    disconnect = save_connection

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

    def tearDown(self) -> None:
        main.user_store = self.old_store
        main.auto_trader = self.old_trader

    async def test_robot_state_stays_200_when_supabase_fails(self) -> None:
        main.auto_trader.start("user-robot")
        payload = main.build_success({"connected": True, "active_mode": "PRACTICE"})

        with patch.object(main, "call_bullex_service", new=AsyncMock(return_value=(200, payload))):
            response = await main.robot_state({"user_id": "user-robot"})

        body = json.loads(response.body)
        self.assertEqual(response.status_code, 200)
        self.assertTrue(body["data"]["connected"])

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

    async def test_debug_endpoint_returns_upsert_contract(self) -> None:
        response = await main.debug_bullex_connection_schema({"user_id": "user-debug"})
        body = json.loads(response.body)["data"]

        self.assertEqual(body["table"], "bullex_connections")
        self.assertEqual(body["payload"]["user_id"], "user-debug")
        self.assertIn("connected", body["fields"])
