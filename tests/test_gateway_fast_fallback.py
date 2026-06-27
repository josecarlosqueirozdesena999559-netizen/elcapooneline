import asyncio
import json
import time
import unittest
from datetime import timedelta
from unittest.mock import AsyncMock, patch

import httpx
from fastapi.testclient import TestClient

from backend import main
from backend.auto_trader import AutoTrader


class SlowClientContext:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def request(self, **_kwargs):
        await asyncio.sleep(1)
        raise AssertionError("wait_for should cancel the slow upstream request")


class RecordingClientContext:
    def __init__(self) -> None:
        self.requests = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def request(self, **kwargs):
        self.requests.append(kwargs)
        return httpx.Response(
            200,
            json=main.build_success({"connected": True, "active_mode": "PRACTICE"}),
        )


class GatewayFastFallbackTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.old_trader = main.auto_trader
        main.auto_trader = AutoTrader()
        main.session_response_cache.clear()
        main.recent_connect_users.clear()
        main.robot_start_requested_users.clear()

    def tearDown(self) -> None:
        main.auto_trader = self.old_trader
        main.session_response_cache.clear()
        main.recent_connect_users.clear()
        main.robot_start_requested_users.clear()

    @staticmethod
    def cache_account(user_id: str, *, balance: float = 42.5) -> None:
        payload = main.build_success(
            {
                "connected": True,
                "active_mode": "PRACTICE",
                "mode": "PRACTICE",
                "balance": balance,
                "currency": "USD",
                "email": "cached@example.com",
            }
        )
        entry = main.BullexResponseCacheEntry(
            status_code=200,
            payload=payload,
            expires_at=main.utc_now() - timedelta(seconds=1),
        )
        cache = main.get_session_cache(user_id)
        cache.responses["/account"] = entry
        cache.last_successful_responses["/account"] = entry

    async def test_account_timeout_returns_last_valid_cache_quickly(self) -> None:
        user_id = "fast-timeout-account"
        self.cache_account(user_id)
        main.mark_connect_activity(user_id)

        started = time.monotonic()
        with (
            patch.object(main, "BULLEX_UPSTREAM_TIMEOUT_SECONDS", 0.01),
            patch("backend.main.httpx.AsyncClient", return_value=SlowClientContext()),
            self.assertLogs("backend-gateway", level="WARNING") as logs,
        ):
            status_code, payload = await main.call_bullex_service(
                "GET",
                "/account",
                user_id,
            )
        elapsed = time.monotonic() - started

        self.assertEqual(status_code, 200)
        self.assertLess(elapsed, 0.5)
        self.assertTrue(payload["ok"])
        self.assertTrue(payload["data"]["connected"])
        self.assertTrue(payload["data"]["from_cache"])
        self.assertEqual(payload["warning"], main.BULLEX_TEMPORARY_UNAVAILABLE)
        output = "\n".join(logs.output)
        self.assertIn("[ACCOUNT_FETCH_TIMEOUT]", output)
        self.assertIn("[ACCOUNT_FETCH_FALLBACK]", output)
        self.assertIn("[UPSTREAM_ERROR_HANDLED]", output)

    async def test_offline_account_and_status_skip_upstream_and_backoff(self) -> None:
        user_id = "offline-polling-user"

        with (
            patch("backend.main.httpx.AsyncClient") as upstream_client,
            self.assertLogs("backend-gateway", level="INFO") as logs,
        ):
            account = await main.bullex_account({"user_id": user_id})
            status = await main.bullex_status({"user_id": user_id})

        for response in (account, status):
            payload = json.loads(response.body)
            self.assertEqual(response.status_code, 200)
            self.assertTrue(payload["ok"])
            self.assertFalse(payload["data"]["connected"])
        upstream_client.assert_not_called()
        cache = main.get_session_cache(user_id)
        self.assertEqual(cache.failure_count, 0)
        self.assertIsNone(cache.next_retry_at)
        output = "\n".join(logs.output)
        self.assertIn("[OFFLINE_USER_SKIPPED]", output)
        self.assertIn("[BACKOFF_SKIPPED_OFFLINE_USER]", output)

    async def test_active_user_in_backoff_gets_controlled_200_payload(self) -> None:
        user_id = "active-backoff-user"
        main.mark_connect_activity(user_id)
        cache = main.get_session_cache(user_id)
        cache.next_retry_at = main.utc_now() + timedelta(seconds=42)

        with patch("backend.main.httpx.AsyncClient") as upstream_client:
            response = await main.bullex_status({"user_id": user_id})

        payload = json.loads(response.body)
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["data"]["status"], "backoff")
        self.assertGreater(payload["data"]["retry_in"], 0)
        upstream_client.assert_not_called()

    def test_offline_failure_does_not_create_user_backoff(self) -> None:
        user_id = "offline-backoff-user"

        with self.assertLogs("backend-gateway", level="INFO") as logs:
            main.mark_session_failure(user_id)

        cache = main.get_session_cache(user_id)
        self.assertEqual(cache.failure_count, 0)
        self.assertIsNone(cache.next_retry_at)
        output = "\n".join(logs.output)
        self.assertIn("[BACKOFF_SKIPPED_OFFLINE_USER]", output)
        self.assertNotIn("[USER_BACKOFF_ACTIVE]", output)

    def test_connect_activity_expires_after_five_minutes(self) -> None:
        user_id = "expired-active-user"
        main.recent_connect_users[user_id] = main.utc_now() - timedelta(seconds=301)

        self.assertFalse(main.is_user_active(user_id))
        self.assertNotIn(user_id, main.recent_connect_users)

    async def test_account_uses_ten_second_cache_without_upstream_call(self) -> None:
        user_id = "fast-account-cache-hit"
        self.cache_account(user_id)
        cached = main.get_session_cache(user_id).responses["/account"]
        cached.expires_at = main.utc_now() + timedelta(
            seconds=main.ACCOUNT_CACHE_TTL_SECONDS
        )

        with (
            patch("backend.main.httpx.AsyncClient") as upstream_client,
            self.assertLogs("backend-gateway", level="INFO") as logs,
        ):
            status_code, payload = await main.call_bullex_service(
                "GET",
                "/account",
                user_id,
            )

        self.assertEqual(status_code, 200)
        self.assertEqual(payload["data"]["balance"], 42.5)
        upstream_client.assert_not_called()
        self.assertIn("[ACCOUNT_CACHE_HIT]", "\n".join(logs.output))

    async def test_session_restore_bypasses_stale_status_cache(self) -> None:
        user_id = "restore-bypasses-cache"
        stale_payload = main.build_error("SESSION_NOT_FOUND")
        stale_payload["data"] = {"connected": False}
        cache = main.get_session_cache(user_id)
        cache.responses["/sessions/status"] = main.BullexResponseCacheEntry(
            status_code=404,
            payload=stale_payload,
            expires_at=main.utc_now() + timedelta(seconds=60),
        )
        client = RecordingClientContext()

        with patch("backend.main.httpx.AsyncClient", return_value=client):
            status_code, payload = await main.call_bullex_service(
                "GET",
                "/sessions/status",
                user_id,
                allow_session_restore=True,
            )

        self.assertEqual(status_code, 200)
        self.assertTrue(payload["data"]["connected"])
        self.assertEqual(len(client.requests), 1)
        self.assertEqual(client.requests[0]["headers"]["x-allow-session-restore"], "true")

    async def test_robot_state_is_memory_only_and_balance_zero_stays_connected(self) -> None:
        user_id = "fast-robot-state"
        state = main.auto_trader.start(user_id)
        state.connected = True
        state.active_mode = "PRACTICE"
        self.cache_account(user_id, balance=0)

        with (
            patch.object(main, "call_bullex_service", new=AsyncMock()) as upstream,
            patch.object(main, "get_user_account_snapshot") as persistent_snapshot,
            self.assertLogs("backend-gateway", level="INFO") as logs,
        ):
            response = await main.robot_state({"user_id": user_id})

        payload = json.loads(response.body)
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["data"]["connected"])
        self.assertEqual(payload["data"]["balance"], 0)
        upstream.assert_not_awaited()
        persistent_snapshot.assert_not_called()
        self.assertIn("[ROBOT_STATE_FAST_RETURN]", "\n".join(logs.output))

    async def test_status_exception_returns_memory_fallback(self) -> None:
        user_id = "fast-status-fallback"
        state = main.auto_trader.start(user_id)
        state.connected = True
        state.active_mode = "PRACTICE"

        with patch.object(
            main,
            "call_bullex_service",
            new=AsyncMock(side_effect=RuntimeError("upstream down")),
        ):
            response = await main.bullex_status({"user_id": user_id})

        payload = json.loads(response.body)
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["data"]["connected"])
        self.assertEqual(payload["warning"], main.BULLEX_TEMPORARY_UNAVAILABLE)

    async def test_connect_uses_sixty_second_policy_and_maps_timeout(self) -> None:
        with (
            patch.object(main, "BULLEX_CONNECT_TIMEOUT_SECONDS", 0.01),
            patch("backend.main.httpx.AsyncClient", return_value=SlowClientContext()),
            self.assertLogs("backend-gateway", level="WARNING") as logs,
        ):
            status_code, payload = await main.call_bullex_service(
                "POST",
                "/sessions/connect",
                "connect-timeout",
                json_body={"email": "user@example.com", "password": "secret"},
            )

        self.assertEqual(status_code, 504)
        self.assertEqual(payload["error"], "LOGIN_TIMEOUT")
        output = "\n".join(logs.output)
        self.assertIn("[CONNECT_TIMEOUT_HANDLED]", output)
        self.assertIn("[BAD_GATEWAY_PREVENTED]", output)


class GatewayControlledErrorCorsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.old_api_key = main.config.panel_api_key
        self.old_trader = main.auto_trader
        main.config.panel_api_key = "test-key"
        main.auto_trader = AutoTrader()
        main.session_response_cache.clear()
        main.recent_connect_users.clear()
        main.robot_start_requested_users.clear()
        self.client = TestClient(main.app)

    def tearDown(self) -> None:
        main.config.panel_api_key = self.old_api_key
        main.auto_trader = self.old_trader
        main.session_response_cache.clear()
        main.recent_connect_users.clear()
        main.robot_start_requested_users.clear()

    def test_account_exception_is_controlled_json_with_cors_headers(self) -> None:
        with patch.object(
            main,
            "call_bullex_service",
            new=AsyncMock(side_effect=RuntimeError("upstream down")),
        ):
            response = self.client.get(
                "/bullex/account",
                headers={
                    "Origin": "https://elcapobot.online",
                    "x-api-key": "test-key",
                    "x-user-id": "cors-controlled-error",
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.json()["ok"])
        self.assertIsNone(response.json()["data"])
        self.assertEqual(
            response.json()["error"],
            main.BULLEX_TEMPORARY_UNAVAILABLE,
        )
        self.assertEqual(
            response.headers.get("access-control-allow-origin"),
            "https://elcapobot.online",
        )

    def test_status_exception_is_controlled_json_with_cors_headers(self) -> None:
        with patch.object(
            main,
            "call_bullex_service",
            new=AsyncMock(side_effect=RuntimeError("status upstream down")),
        ):
            response = self.client.get(
                "/bullex/status",
                headers={
                    "Origin": "https://elcapobot.online",
                    "x-api-key": "test-key",
                    "x-user-id": "cors-status-error",
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.json()["ok"])
        self.assertEqual(
            response.json()["error"],
            main.BULLEX_TEMPORARY_UNAVAILABLE,
        )
        self.assertEqual(
            response.headers.get("access-control-allow-origin"),
            "https://elcapobot.online",
        )

    def test_unexpected_robot_state_exception_keeps_cors_headers(self) -> None:
        with patch.object(
            main,
            "recover_sync_timeout_if_needed",
            side_effect=RuntimeError("unexpected state failure"),
        ):
            response = self.client.get(
                "/robot/state",
                headers={
                    "Origin": "https://elcapobot.online",
                    "x-api-key": "test-key",
                    "x-user-id": "cors-robot-error",
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json()["error"],
            main.BULLEX_TEMPORARY_UNAVAILABLE,
        )
        self.assertEqual(
            response.headers.get("access-control-allow-origin"),
            "https://elcapobot.online",
        )

    def test_connect_converts_upstream_502_to_login_failed_with_cors(self) -> None:
        with patch.object(
            main,
            "call_bullex_service",
            new=AsyncMock(
                return_value=(
                    502,
                    {"ok": False, "error": main.BULLEX_TEMPORARY_UNAVAILABLE},
                )
            ),
        ):
            response = self.client.post(
                "/bullex/connect",
                headers={
                    "Origin": "https://elcapobot.online",
                    "x-api-key": "test-key",
                    "x-user-id": "connect-upstream-502",
                },
                json={"email": "user@example.com", "password": "secret"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["error"], main.BULLEX_TEMPORARY_UNAVAILABLE)
        self.assertEqual(
            response.json()["detail"],
            main.BULLEX_TEMPORARY_UNAVAILABLE,
        )
        self.assertEqual(
            response.headers.get("access-control-allow-origin"),
            "https://elcapobot.online",
        )

    def test_connect_start_and_stop_never_leak_unhandled_exception(self) -> None:
        headers = {
            "Origin": "https://elcapobot.online",
            "x-api-key": "test-key",
            "x-user-id": "protected-endpoint-error",
        }
        cases = (
            (
                "/bullex/connect",
                {"email": "user@example.com", "password": "secret"},
                patch.object(
                    main,
                    "call_bullex_service",
                    new=AsyncMock(side_effect=RuntimeError("connect failure")),
                ),
            ),
            (
                "/robot/start",
                None,
                patch.object(
                    main,
                    "get_user_robot_state",
                    side_effect=RuntimeError("start failure"),
                ),
            ),
            (
                "/robot/stop",
                None,
                patch.object(
                    main.auto_trader,
                    "stop",
                    side_effect=RuntimeError("stop failure"),
                ),
            ),
        )

        for path, body, failure in cases:
            with self.subTest(path=path), failure:
                response = self.client.post(path, headers=headers, json=body)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(
                    response.json()["error"],
                    main.BULLEX_TEMPORARY_UNAVAILABLE,
                )
                if path == "/bullex/connect":
                    self.assertIn("connect failure", response.json()["detail"])
                self.assertEqual(
                    response.headers.get("access-control-allow-origin"),
                    "https://elcapobot.online",
                )


if __name__ == "__main__":
    unittest.main()
