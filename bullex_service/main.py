import asyncio
import logging
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from threading import BoundedSemaphore, RLock
from typing import Any

from fastapi import FastAPI, Header, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

import bullexapi.global_value as global_value
from bullexapi.stable_api import Bullex
from bullex_service.session_store import SessionStore, create_session_store
from websocket._exceptions import WebSocketConnectionClosedException


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bullex-service")

CORS_ALLOWED_ORIGINS_DEFAULT = (
    "https://elcapobot.online,"
    "https://www.elcapobot.online,"
    "http://localhost:5173,"
    "http://localhost:3000"
)
CORS_ALLOWED_METHODS = ["GET", "POST", "OPTIONS"]
CORS_ALLOWED_HEADERS = [
    "x-api-key",
    "x-user-id",
    "content-type",
    "authorization",
]

ALLOWED_BALANCE_MODES = {"PRACTICE", "REAL", "TOURNAMENT"}
ALLOWED_ACTIONS = {"call", "put"}
SESSION_NOT_FOUND = "SESSION_NOT_FOUND"
SESSION_DISCONNECTED = "SESSION_DISCONNECTED"
BULLEX_ACTIVE_MODE_NOT_REAL = "BULLEX_ACTIVE_MODE_NOT_REAL"
ASSET_NOT_ALLOWED = "ASSET_NOT_ALLOWED"
BINARY_ALLOWED_ASSETS = [
    "EURUSD-OTC",
    "EURGBP-OTC",
    "USDCHF-OTC",
    "EURJPY-OTC",
    "NZDUSD-OTC",
    "GBPUSD-OTC",
    "GBPJPY-OTC",
    "USDJPY-OTC",
    "AUDCAD-OTC",
    "AUDUSD-OTC",
    "USDCAD-OTC",
    "AUDJPY-OTC",
    "GBPCAD-OTC",
    "GBPCHF-OTC",
    "GBPAUD-OTC",
    "EURCAD-OTC",
    "CHFJPY-OTC",
    "CADCHF-OTC",
    "EURAUD-OTC",
    "EURNZD-OTC",
    "AUDCHF-OTC",
]
BINARY_ALLOWED_ASSET_SET = set(BINARY_ALLOWED_ASSETS)
SESSION_EXCEPTION_TYPES = (WebSocketConnectionClosedException, ConnectionError, TimeoutError)
SESSION_STATUS_TTL_SECONDS = 10
ACCOUNT_TTL_SECONDS = 10
ASSETS_TTL_SECONDS = 300
PAYOUT_TTL_SECONDS = 60
CANDLES_TTL_SECONDS = 2
SESSION_STATUS_THROTTLE_SECONDS = 10
SESSION_OFFLINE_TTL_SECONDS = 60
SESSION_FAILURE_BACKOFF_SECONDS = (10, 30, 60, 300)
LOGIN_TIMEOUT_SECONDS = 60
LOGIN_RETRY_DELAY_SECONDS = 5
LOGIN_MAX_ATTEMPTS = 3
LOGIN_PROGRESS_STATES = (
    "CONNECTING",
    "AUTHENTICATING",
    "OPENING_WEBSOCKET",
    "LOADING_PROFILE",
    "LOADING_BALANCE",
    "READY",
)


class ServiceError(Exception):
    def __init__(
        self,
        message: str,
        status_code: int = 400,
        data: dict[str, Any] | None = None,
    ):
        self.message = message
        self.status_code = status_code
        self.data = data
        super().__init__(message)


class ConnectRequest(BaseModel):
    email: str | None = None
    password: str | None = None
    sms_code: str | None = None
    account_mode: str = Field(default="REAL")


class ChangeModeRequest(BaseModel):
    mode: str
    confirm_real: bool = True


class BuyOrderRequest(BaseModel):
    amount: float
    active: str
    action: str
    expiration: int
    confirm_real: bool = True


@dataclass
class SessionState:
    check_websocket_if_connect: Any = None
    ssl_Mutual_exclusion: bool = False
    ssl_Mutual_exclusion_write: bool = False
    SSID: Any = None
    check_websocket_if_error: bool = False
    websocket_error_reason: Any = None
    balance_id: Any = None


@dataclass
class ManagedSession:
    user_id: str
    client: Bullex
    email: str | None = None
    password: str | None = None
    sms_code: str | None = None
    desired_mode: str = "REAL"
    requires_2fa: bool = False
    active_mode: str | None = None
    real_mode_confirmed: bool = False
    state: SessionState = field(default_factory=SessionState)


@dataclass
class CachedProbe:
    status_code: int
    payload: dict[str, Any]
    expires_at: float


@dataclass
class SessionProbeState:
    responses: dict[str, CachedProbe] = field(default_factory=dict)
    failure_count: int = 0
    next_retry_at: float = 0.0
    offline_until: float = 0.0
    last_request_at: dict[str, float] = field(default_factory=dict)


@dataclass
class LoginProgress:
    state: str = "IDLE"
    attempt: int = 0
    max_attempts: int = LOGIN_MAX_ATTEMPTS
    updated_at: float = 0.0
    error: str | None = None
    active: bool = False


class SessionManager:
    def __init__(self, store: SessionStore | None = None) -> None:
        self.sessions: dict[str, ManagedSession] = {}
        self.websockets: dict[str, Any] = {}
        self.workers: dict[str, Any] = {}
        self.locks: dict[str, RLock] = {}
        self.async_locks: dict[str, asyncio.Lock] = {}
        self.last_account_cache: dict[str, dict[str, Any]] = {}
        self.last_status_cache: dict[str, dict[str, Any]] = {}
        self._probe_cache: dict[str, SessionProbeState] = {}
        self._login_progress: dict[str, LoginProgress] = {}
        self.store = store
        self._runtime_lock = RLock()
        self._max_concurrent_api_calls = read_max_concurrent_api_calls()
        self._call_gate = BoundedSemaphore(self._max_concurrent_api_calls)

        logger.warning("MVP_SAFE_MODE: bullexapi global-state protected by process lock")
        logger.info(
            "bullex-service configured with BULLEX_MAX_CONCURRENT_API_CALLS=%s",
            self._max_concurrent_api_calls,
        )

    def get(self, user_id: str) -> ManagedSession | None:
        return self.sessions.get(user_id)

    def user_lock(self, user_id: str) -> RLock:
        return self.locks.setdefault(user_id, RLock())

    def async_user_lock(self, user_id: str) -> asyncio.Lock:
        return self.async_locks.setdefault(user_id, asyncio.Lock())

    def get_probe_state(self, user_id: str) -> SessionProbeState:
        return self._probe_cache.setdefault(user_id, SessionProbeState())

    def get_login_progress(self, user_id: str) -> LoginProgress:
        return self._login_progress.setdefault(user_id, LoginProgress())

    def upsert(self, session: ManagedSession) -> ManagedSession:
        self.sessions[session.user_id] = session
        self.websockets[session.user_id] = getattr(session.client, "api", session.client)
        return session

    def remove(self, user_id: str) -> None:
        self.sessions.pop(user_id, None)
        self.websockets.pop(user_id, None)
        self.workers.pop(user_id, None)

    def clear_probe_cache(self, user_id: str) -> None:
        probe = self.get_probe_state(user_id)
        probe.responses.clear()
        probe.last_request_at.clear()

    def clear_user_runtime_cache(self, user_id: str) -> None:
        probe = self.get_probe_state(user_id)
        probe.responses.clear()
        probe.last_request_at.clear()
        probe.failure_count = 0
        probe.next_retry_at = 0.0
        probe.offline_until = 0.0
        self.last_account_cache.pop(user_id, None)
        self.last_status_cache.pop(user_id, None)

    def login_progress_payload(self, user_id: str) -> dict[str, Any]:
        progress = self.get_login_progress(user_id)
        return {
            "state": progress.state,
            "attempt": progress.attempt,
            "max_attempts": progress.max_attempts,
            "updated_at": progress.updated_at,
            "error": progress.error,
            "active": progress.active,
        }

    def _set_login_progress(
        self,
        user_id: str,
        state: str,
        *,
        attempt: int,
        active: bool,
        error: str | None = None,
    ) -> None:
        progress = self.get_login_progress(user_id)
        progress.state = state
        progress.attempt = attempt
        progress.max_attempts = LOGIN_MAX_ATTEMPTS
        progress.updated_at = time.time()
        progress.error = error
        progress.active = active

    def _clear_login_progress(self, user_id: str) -> None:
        progress = self.get_login_progress(user_id)
        progress.state = "READY"
        progress.attempt = 0
        progress.max_attempts = LOGIN_MAX_ATTEMPTS
        progress.updated_at = time.time()
        progress.error = None
        progress.active = False

    def _cache_probe(
        self,
        user_id: str,
        cache_key: str,
        status_code: int,
        payload: dict[str, Any],
        *,
        ttl_seconds: int,
    ) -> None:
        self.get_probe_state(user_id).responses[cache_key] = CachedProbe(
            status_code=status_code,
            payload=payload,
            expires_at=time.time() + ttl_seconds,
        )

    def _throttled_probe(self, user_id: str, cache_key: str, *, path: str) -> tuple[int, dict[str, Any]] | None:
        if path != "/sessions/status":
            return None
        probe = self.get_probe_state(user_id)
        now = time.time()
        last_request_at = probe.last_request_at.get(cache_key, 0.0)
        probe.last_request_at[cache_key] = now
        if last_request_at <= 0 or (now - last_request_at) >= SESSION_STATUS_THROTTLE_SECONDS:
            return None
        cached = probe.responses.get(cache_key)
        if cached is None:
            return None
        logger.debug("[SESSION_STATUS_THROTTLED] %s %s", user_id, cache_key)
        return cached.status_code, cached.payload

    def _probe_backoff_seconds(self, failure_count: int) -> int:
        index = min(max(failure_count, 1), len(SESSION_FAILURE_BACKOFF_SECONDS)) - 1
        return SESSION_FAILURE_BACKOFF_SECONDS[index]

    def _mark_probe_success(
        self,
        user_id: str,
        cache_key: str,
        status_code: int,
        payload: dict[str, Any],
        *,
        ttl_seconds: int,
    ) -> None:
        probe = self.get_probe_state(user_id)
        probe.failure_count = 0
        probe.next_retry_at = 0.0
        probe.offline_until = 0.0
        self._cache_probe(user_id, cache_key, status_code, payload, ttl_seconds=ttl_seconds)
        if cache_key == "/account":
            self.last_account_cache[user_id] = payload
        elif cache_key == "/sessions/status":
            self.last_status_cache[user_id] = payload

    def _mark_probe_failure(self, user_id: str, *, offline: bool = False) -> None:
        probe = self.get_probe_state(user_id)
        probe.failure_count += 1
        now = time.time()
        disconnected_payload = build_success({"connected": False})
        if offline:
            probe.offline_until = now + SESSION_OFFLINE_TTL_SECONDS
            probe.next_retry_at = probe.offline_until
            self.last_account_cache.pop(user_id, None)
            self.last_status_cache.pop(user_id, None)
            self._cache_probe(
                user_id,
                "/sessions/status",
                404,
                {"ok": False, "data": {"connected": False}, "error": SESSION_NOT_FOUND},
                ttl_seconds=SESSION_STATUS_TTL_SECONDS,
            )
            self._cache_probe(
                user_id,
                "/account",
                200,
                disconnected_payload,
                ttl_seconds=ACCOUNT_TTL_SECONDS,
            )
            return
        probe.next_retry_at = now + self._probe_backoff_seconds(probe.failure_count)
        self._cache_probe(
            user_id,
            "/sessions/status",
            200,
            disconnected_payload,
            ttl_seconds=SESSION_STATUS_TTL_SECONDS,
        )
        self._cache_probe(
            user_id,
            "/account",
            200,
            disconnected_payload,
            ttl_seconds=ACCOUNT_TTL_SECONDS,
        )

    def get_cached_probe(self, user_id: str, cache_key: str, *, path: str) -> tuple[int, dict[str, Any]] | None:
        throttled = self._throttled_probe(user_id, cache_key, path=path)
        if throttled is not None:
            return throttled
        probe = self.get_probe_state(user_id)
        now = time.time()
        cached = probe.responses.get(cache_key)
        if cached is not None and now < cached.expires_at:
            logger.debug("[CACHE_HIT] user_id=%s path=%s", user_id, path)
            if path == "/sessions/status":
                logger.debug("[SESSION_STATUS_CACHE_HIT] %s %s", user_id, path)
            return cached.status_code, cached.payload
        logger.debug("[CACHE_MISS] user_id=%s path=%s", user_id, path)
        if path == "/sessions/status":
            logger.debug("[SESSION_STATUS_CACHE_MISS] %s %s", user_id, path)
        if probe.offline_until > now:
            logger.warning("[SESSION_CHECK_SKIPPED] %s %s reason=offline", user_id, path)
            logger.warning("[USER_OFFLINE_SKIPPED] %s %s", user_id, path)
            logger.warning("[CPU_LOOP_PROTECTION] %s %s reason=offline", user_id, path)
            if cached is not None:
                return cached.status_code, cached.payload
            if path == "/account":
                return 200, build_success({"connected": False})
            return 404, {"ok": False, "data": {"connected": False}, "error": SESSION_NOT_FOUND}
        if probe.next_retry_at > now:
            logger.warning("[SESSION_CHECK_SKIPPED] %s %s reason=backoff", user_id, path)
            logger.warning("[BACKOFF_ACTIVE] %s %s", user_id, path)
            logger.warning("[CPU_LOOP_PROTECTION] %s %s reason=backoff", user_id, path)
            if cached is not None:
                return cached.status_code, cached.payload
            return 200, build_success({"connected": False})
        return None

    def get_last_probe(self, user_id: str, cache_key: str) -> tuple[int, dict[str, Any]] | None:
        cached = self.get_probe_state(user_id).responses.get(cache_key)
        if cached is None:
            return None
        return cached.status_code, cached.payload

    def require(self, user_id: str) -> ManagedSession:
        session = self.get(user_id)
        if session is None:
            raise ServiceError(SESSION_NOT_FOUND, 404)
        return session

    def ensure_session_alive(self, user_id: str) -> ManagedSession:
        cached = self.get_cached_probe(user_id, "/sessions/status", path="/sessions/status")
        if cached is not None:
            session = self.get(user_id)
            if session is not None:
                return session
        logger.info("[SESSION-CHECK] %s", user_id)
        session = self.require(user_id)
        if session.requires_2fa:
            logger.info("[SESSION-ALIVE] %s", user_id)
            return session

        alive = False
        dead_reason = "UNKNOWN"
        try:
            with self._session_context(session):
                alive = self._is_session_alive(session)
        except SESSION_EXCEPTION_TYPES as exc:
            dead_reason = type(exc).__name__
        except Exception as exc:
            dead_reason = type(exc).__name__

        if alive:
            logger.info("[SESSION-ALIVE] %s", user_id)
            self._mark_probe_success(
                user_id,
                "/sessions/status",
                200,
                build_success(
                    {
                        "user_id": user_id,
                        "connected": True,
                        "requires_2fa": session.requires_2fa,
                        "active_mode": session.client.get_balance_mode(),
                    }
                ),
                ttl_seconds=SESSION_STATUS_TTL_SECONDS,
            )
            return session

        logger.warning("[SESSION-DEAD] %s %s", user_id, dead_reason)
        self._mark_probe_failure(user_id)
        return self._attempt_reconnect(session, dead_reason)

    def _run_with_timeout(self, operation, *, timeout_seconds: int = LOGIN_TIMEOUT_SECONDS):
        executor = ThreadPoolExecutor(max_workers=1)
        future = executor.submit(operation)
        try:
            return future.result(timeout=timeout_seconds)
        except FutureTimeoutError as exc:
            future.cancel()
            executor.shutdown(wait=False, cancel_futures=True)
            raise TimeoutError(f"operation exceeded {timeout_seconds}s") from exc
        finally:
            if future.done():
                executor.shutdown(wait=False, cancel_futures=True)

    def _wait_until_session_ready(self, session: ManagedSession, *, timeout_seconds: int = LOGIN_TIMEOUT_SECONDS) -> None:
        deadline = time.time() + timeout_seconds
        last_error: Exception | None = None
        while time.time() < deadline:
            try:
                if self._is_session_alive(session):
                    return
            except Exception as exc:
                last_error = exc
            time.sleep(0.5)
        if last_error is not None:
            raise TimeoutError(type(last_error).__name__)
        raise TimeoutError("websocket_not_ready")

    def _populate_ready_state(self, session: ManagedSession, *, user_id: str, attempt: int) -> None:
        self._set_login_progress(user_id, "OPENING_WEBSOCKET", attempt=attempt, active=True)
        logger.info("[LOGIN_WS] user_id=%s attempt=%s", user_id, attempt)
        with self._session_context(session):
            self._wait_until_session_ready(session)
            self._set_login_progress(user_id, "LOADING_PROFILE", attempt=attempt, active=True)
            session.desired_mode = "REAL"
            logger.info("[REAL_MODE_FORCED] user_id=%s attempt=%s", user_id, attempt)
            real_balance_id, practice_balance_id, balances_discovered = discover_balance_ids(
                session.client
            )
            if real_balance_id is not None:
                logger.info(
                    "[REAL_BALANCE_ID_FOUND] user_id=%s real_balance_id=%s practice_balance_id=%s",
                    user_id,
                    real_balance_id,
                    practice_balance_id,
                )
            current_mode = normalize_mode(session.client.get_balance_mode())
            logger.info("[LOGIN_AUTH] user_id=%s attempt=%s mode=%s", user_id, attempt, current_mode)
            if balances_discovered and real_balance_id is None:
                logger.warning(
                    "[CHANGE_BALANCE_REAL_FAILED] user_id=%s active_mode=%s reason=real_balance_id_missing",
                    user_id,
                    current_mode,
                )
                raise real_mode_service_error(current_mode)
            try:
                current_mode = force_real_mode(session, user_id=user_id)
                session.active_mode = current_mode
                session.real_mode_confirmed = current_mode == "REAL"
                logger.info("[REAL_MODE_CONFIRMED] user_id=%s active_mode=%s", user_id, current_mode)
            except ServiceError as exc:
                current_mode = str((exc.data or {}).get("active_mode") or current_mode).strip().upper()
                session.active_mode = current_mode
                session.real_mode_confirmed = False
                logger.warning(
                    "[REAL_MODE_FAILED] user_id=%s active_mode=%s",
                    user_id,
                    current_mode,
                )
                return
            self._set_login_progress(user_id, "LOADING_BALANCE", attempt=attempt, active=True)
            session.client.get_balance()
            session.client.get_currency()

    def _connect_with_retries(self, user_id: str, action) -> ManagedSession:
        last_error: Exception | None = None
        for attempt in range(1, LOGIN_MAX_ATTEMPTS + 1):
            self._set_login_progress(user_id, "CONNECTING", attempt=attempt, active=True)
            if attempt == 1:
                logger.info("[LOGIN_STARTED] user_id=%s", user_id)
            else:
                logger.warning("[LOGIN_RETRY] user_id=%s attempt=%s", user_id, attempt)
            try:
                session = action(attempt)
                self._set_login_progress(user_id, "READY", attempt=attempt, active=False)
                logger.info("[LOGIN_READY] user_id=%s attempt=%s", user_id, attempt)
                logger.info("[LOGIN_SUCCESS] user_id=%s attempt=%s", user_id, attempt)
                return session
            except TimeoutError as exc:
                last_error = exc
                self._set_login_progress(user_id, "CONNECTING", attempt=attempt, active=True, error="LOGIN_TIMEOUT")
                logger.warning("[LOGIN_TIMEOUT] user_id=%s attempt=%s error=%s", user_id, attempt, exc)
                if attempt >= LOGIN_MAX_ATTEMPTS:
                    break
                time.sleep(LOGIN_RETRY_DELAY_SECONDS)
            except ServiceError as exc:
                last_error = exc
                if "timeout" in str(exc.message).lower() and attempt < LOGIN_MAX_ATTEMPTS:
                    logger.warning("[LOGIN_TIMEOUT] user_id=%s attempt=%s error=%s", user_id, attempt, exc.message)
                    logger.warning("[LOGIN_RETRY] user_id=%s attempt=%s", user_id, attempt + 1)
                    time.sleep(LOGIN_RETRY_DELAY_SECONDS)
                    continue
                self._set_login_progress(user_id, "CONNECTING", attempt=attempt, active=False, error=exc.message)
                logger.warning("[LOGIN_FAILED] user_id=%s attempt=%s error=%s", user_id, attempt, exc.message)
                raise
            except Exception as exc:
                last_error = exc
                self._set_login_progress(user_id, "CONNECTING", attempt=attempt, active=False, error=type(exc).__name__)
                logger.warning("[LOGIN_FAILED] user_id=%s attempt=%s error=%s", user_id, attempt, type(exc).__name__)
                raise ServiceError(type(exc).__name__, 401) from exc

        error_message = "LOGIN_TIMEOUT" if isinstance(last_error, TimeoutError) else type(last_error).__name__ if last_error else "LOGIN_FAILED"
        self._set_login_progress(user_id, "CONNECTING", attempt=LOGIN_MAX_ATTEMPTS, active=False, error=error_message)
        logger.warning("[LOGIN_FAILED] user_id=%s attempt=%s error=%s", user_id, LOGIN_MAX_ATTEMPTS, error_message)
        raise ServiceError(error_message, 504 if error_message == "LOGIN_TIMEOUT" else 401)

    def run(self, user_id: str, operation, *, disconnect_on_error: bool = True):
        with self.user_lock(user_id):
            session = self.ensure_session_alive(user_id)
            try:
                with self._session_context(session):
                    return operation(session)
            except ServiceError as exc:
                if exc.message == SESSION_DISCONNECTED and disconnect_on_error:
                    logger.warning("[SESSION-DISCONNECTED] %s", user_id)
                    self.remove(user_id)
                raise
            except SESSION_EXCEPTION_TYPES as exc:
                if disconnect_on_error:
                    self._mark_disconnected(user_id, type(exc).__name__)
                raise ServiceError(SESSION_DISCONNECTED, 409) from exc
            except Exception as exc:
                if disconnect_on_error:
                    self._mark_disconnected(user_id, type(exc).__name__)
                raise ServiceError(type(exc).__name__, 503) from exc

    def connect(self, user_id: str, payload: ConnectRequest) -> ManagedSession:
        with self.user_lock(user_id):
            payload.account_mode = "REAL"
            logger.info("[REAL_MODE_FORCED] user_id=%s requested_mode=REAL", user_id)
            logger.info("[REAL_MODE_REQUESTED] user_id=%s", user_id)
            logger.info("[CONNECT_REQUEST] user_id=%s", user_id)
            self.clear_user_runtime_cache(user_id)
            logger.info("[CONNECT_BACKOFF_CLEARED] user_id=%s", user_id)

            is_2fa_continuation = bool(
                payload.sms_code
                and self.get(user_id) is not None
                and not payload.email
                and not payload.password
            )
            if not is_2fa_continuation:
                existing = self.get(user_id)
                logger.info(
                    "[CONNECT_CLEAR_OLD_SESSION] user_id=%s had_active_session=%s",
                    user_id,
                    existing is not None,
                )
                if existing is not None:
                    self._close_session(existing)
                self.remove(user_id)
                self.clear_user_runtime_cache(user_id)
                if self.store is not None:
                    self.store.mark_disconnected(user_id, revoke_token=True)
                logger.info(
                    "[CONNECT_OLD_SESSION_CLOSED] user_id=%s had_active_session=%s",
                    user_id,
                    existing is not None,
                )

            try:
                logger.info("[CONNECT_ATTEMPT] user_id=%s", user_id)
                session = self._connect_unlocked(user_id, payload)
                connected = (not session.requires_2fa) and session.real_mode_confirmed
                self._mark_probe_success(
                    user_id,
                    "/sessions/status",
                    200,
                    build_success(
                        {
                            "user_id": user_id,
                            "connected": connected,
                            "requires_2fa": session.requires_2fa,
                            "active_mode": (
                                session.active_mode or session.client.get_balance_mode()
                                if not session.requires_2fa
                                else None
                            ),
                        }
                    ),
                    ttl_seconds=SESSION_STATUS_TTL_SECONDS,
                )
                logger.info("[CONNECT_SUCCESS] user_id=%s", user_id)
                return session
            except Exception as exc:
                logger.warning(
                    "[CONNECT_FAILED_HANDLED] user_id=%s detail=%s",
                    user_id,
                    getattr(exc, "message", None) or type(exc).__name__,
                )
                raise

    def _connect_unlocked(self, user_id: str, payload: ConnectRequest) -> ManagedSession:
        probe = self.get_probe_state(user_id)
        probe.failure_count = 0
        probe.next_retry_at = 0.0
        probe.offline_until = 0.0
        existing = self.get(user_id)

        if existing is not None and not existing.requires_2fa:
            try:
                with self._session_context(existing):
                    if self._is_session_alive(existing):
                        target_mode = normalize_mode(payload.account_mode)
                        if existing.desired_mode != target_mode:
                            existing.desired_mode = target_mode
                            self._populate_ready_state(existing, user_id=user_id, attempt=1)
                            self._persist_connected(existing)
                        self._set_login_progress(user_id, "READY", attempt=1, active=False)
                        logger.info("[LOGIN_SUCCESS] user_id=%s attempt=1 reused_session=true", user_id)
                        return existing
            except Exception:
                logger.warning("[SESSION-REUSE-FAILED] %s", user_id, exc_info=True)

        if payload.sms_code and existing and not payload.email and not payload.password:
            def connect_existing_2fa(attempt: int) -> ManagedSession:
                session = existing
                session.sms_code = payload.sms_code
                session.desired_mode = normalize_mode(payload.account_mode)
                self._set_login_progress(user_id, "AUTHENTICATING", attempt=attempt, active=True)
                logger.info("[LOGIN_AUTH] user_id=%s attempt=%s mode=2FA", user_id, attempt)
                with self._session_context(session):
                    ok, reason = self._run_with_timeout(
                        lambda: session.client.connect_2fa(payload.sms_code),
                        timeout_seconds=LOGIN_TIMEOUT_SECONDS,
                    )
                self._finalize_connect(session, ok, reason, user_id=user_id, attempt=attempt)
                self._persist_connected(session)
                return session

            return self._connect_with_retries(user_id, connect_existing_2fa)

        if not payload.email or not payload.password:
            raise ServiceError("email e password sao obrigatorios para conectar")

        desired_mode = normalize_mode(payload.account_mode)

        def connect_new_session(attempt: int) -> ManagedSession:
            logger.info("[CONNECT_CREATE_SESSION] user_id=%s attempt=%s", user_id, attempt)
            new_session = ManagedSession(
                user_id=user_id,
                client=Bullex(payload.email, payload.password),
                email=payload.email,
                password=payload.password,
                sms_code=payload.sms_code,
                desired_mode=desired_mode,
            )
            if existing is not None and attempt == 1:
                self._close_session(existing)
            self.upsert(new_session)
            self._set_login_progress(user_id, "AUTHENTICATING", attempt=attempt, active=True)
            logger.info("[LOGIN_AUTH] user_id=%s attempt=%s mode=password", user_id, attempt)
            logger.info("[CONNECT_WS_START] user_id=%s attempt=%s", user_id, attempt)
            try:
                with self._session_context(new_session):
                    ok, reason = self._run_with_timeout(
                        lambda: new_session.client.connect(payload.sms_code),
                        timeout_seconds=LOGIN_TIMEOUT_SECONDS,
                    )
                self._finalize_connect(new_session, ok, reason, user_id=user_id, attempt=attempt)
                self._persist_connected(new_session)
                return new_session
            except Exception:
                self.remove(user_id)
                raise

        return self._connect_with_retries(user_id, connect_new_session)

    def disconnect(self, user_id: str) -> str:
        with self.user_lock(user_id):
            session = self.get(user_id)
            if session is None:
                self.remove(user_id)
                if self.store is not None:
                    self.store.mark_disconnected(user_id, revoke_token=True)
                self._mark_probe_failure(user_id, offline=True)
                logger.info("[WORKER_DESTROYED] user_id=%s", user_id)
                logger.info("[SESSION-DISCONNECTED] %s", user_id)
                return user_id
            with self._session_context(session):
                try:
                    session.client.logout()
                except Exception:
                    logger.exception("falha ao executar logout da sessao %s", session.user_id)
                try:
                    session.client.api.close()
                except Exception:
                    logger.exception("falha ao fechar websocket da sessao %s", session.user_id)
            self.remove(user_id)
            if self.store is not None:
                self.store.mark_disconnected(user_id, revoke_token=True)
            self._mark_probe_failure(user_id, offline=True)
            logger.info("[WORKER_DESTROYED] user_id=%s", user_id)
            logger.info("[SESSION-DISCONNECTED] %s", user_id)
            return user_id

    def reconnect(self, user_id: str) -> ManagedSession:
        with self.user_lock(user_id):
            if self.get(user_id) is None:
                raise ServiceError(SESSION_NOT_FOUND, 404)
            session = self.require(user_id)
            return self._attempt_reconnect(session, "MANUAL")

    def _is_session_alive(self, session: ManagedSession) -> bool:
        check_connect = getattr(session.client, "check_connect", None)
        if callable(check_connect) and not bool(check_connect()):
            return False

        websocket_alive = getattr(session.client, "websocket_alive", None)
        if callable(websocket_alive):
            return bool(websocket_alive())
        if websocket_alive is not None:
            return bool(websocket_alive)

        api = getattr(session.client, "api", None)
        api_websocket_alive = getattr(api, "websocket_alive", None)
        if callable(api_websocket_alive):
            return bool(api_websocket_alive())
        if api_websocket_alive is not None:
            return bool(api_websocket_alive)

        return True

    def _attempt_reconnect(self, session: ManagedSession, reason: str) -> ManagedSession:
        user_id = session.user_id
        logger.info("[SESSION-RECONNECT-START] %s", user_id)
        old_client = session.client

        def reconnect_attempt(attempt: int) -> ManagedSession:
            new_session = ManagedSession(
                user_id=user_id,
                client=Bullex(session.email, session.password or ""),
                email=session.email,
                password=session.password,
                sms_code=session.sms_code,
                desired_mode="REAL",
                requires_2fa=session.requires_2fa,
                state=SessionState(SSID=session.state.SSID),
            )
            try:
                old_client.api.close()
            except Exception:
                pass

            restore_with_ssid = getattr(new_session.client, "restore_with_ssid", None)
            if new_session.state.SSID and callable(restore_with_ssid):
                self._set_login_progress(user_id, "AUTHENTICATING", attempt=attempt, active=True)
                logger.info("[LOGIN_AUTH] user_id=%s attempt=%s mode=ssid_restore", user_id, attempt)
                with self._session_context(new_session):
                    ok, connect_reason = self._run_with_timeout(
                        lambda: restore_with_ssid(str(new_session.state.SSID)),
                        timeout_seconds=LOGIN_TIMEOUT_SECONDS,
                    )
                self._finalize_connect(new_session, ok, connect_reason, user_id=user_id, attempt=attempt)
                self.upsert(new_session)
                self._persist_connected(new_session)
                logger.info("[SESSION-RECONNECT-OK] %s restored_ssid=true", user_id)
                return new_session

            if not session.email or not session.password:
                logger.warning("[SESSION-RECONNECT-FAILED] %s missing_credentials", user_id)
                self._mark_disconnected(user_id, reason)

            self._set_login_progress(user_id, "AUTHENTICATING", attempt=attempt, active=True)
            logger.info("[LOGIN_AUTH] user_id=%s attempt=%s mode=password", user_id, attempt)
            with self._session_context(new_session):
                ok, connect_reason = self._run_with_timeout(
                    lambda: new_session.client.connect(new_session.sms_code),
                    timeout_seconds=LOGIN_TIMEOUT_SECONDS,
                )
            self._finalize_connect(new_session, ok, connect_reason, user_id=user_id, attempt=attempt)
            self.upsert(new_session)
            self._persist_connected(new_session)
            logger.info("[SESSION-RECONNECT-OK] %s restored_ssid=false", user_id)
            return new_session

        try:
            return self._connect_with_retries(user_id, reconnect_attempt)
        except ServiceError as exc:
            logger.warning("[SESSION-RECONNECT-FAILED] %s %s", user_id, exc.message)
            self._mark_disconnected(user_id, exc.message)

    def restore_on_demand(self, user_id: str) -> ManagedSession:
        existing = self.get(user_id)
        if existing is not None:
            logger.info(
                "[SESSION_RESTORE_SKIPPED] user_id=%s reason=already_active",
                user_id,
            )
            return existing

        with self.user_lock(user_id):
            existing = self.get(user_id)
            if existing is not None:
                logger.info(
                    "[SESSION_RESTORE_SKIPPED] user_id=%s reason=already_active",
                    user_id,
                )
                return existing

            logger.info("[SESSION_RESTORE_ON_DEMAND] user_id=%s", user_id)
            if self.store is None:
                raise ServiceError(SESSION_NOT_FOUND, 404)
            persisted = self.store.load_connected_user(user_id)
            if persisted is None:
                raise ServiceError(SESSION_NOT_FOUND, 404)
            session = ManagedSession(
                user_id=persisted.user_id,
                client=Bullex(persisted.email, ""),
                email=persisted.email,
                desired_mode="REAL",
                state=SessionState(SSID=persisted.session_token),
            )
            restore_with_ssid = getattr(session.client, "restore_with_ssid", None)
            if not callable(restore_with_ssid):
                self.store.mark_disconnected(session.user_id)
                logger.warning(
                    "[SESSION_RESTORE] status=unsupported reason=no_ssid_restore_method user_id=%s",
                    session.user_id,
                )
                raise ServiceError(SESSION_DISCONNECTED, 409)

            restore_failure_reason = "unknown"
            try:
                with self._session_context(session):
                    ok, reason = restore_with_ssid(persisted.session_token)
                    restore_failure_reason = str(reason or "restore_rejected")
                self._finalize_connect(session, ok, reason, user_id=session.user_id, attempt=1)
                self.upsert(session)
                self._persist_connected(session)
                self._clear_login_progress(session.user_id)
                logger.info("[SESSION_RESTORE] user_id=%s status=success", session.user_id)
                return session
            except Exception as exc:
                self._close_session(session)
                self.remove(session.user_id)
                self.store.mark_disconnected(session.user_id)
                if restore_failure_reason == "invalid_ssid":
                    logger.warning(
                        "[SESSION_RESTORE] user_id=%s status=unsupported reason=broker_invalidates_ssid",
                        session.user_id,
                    )
                else:
                    logger.warning(
                        "[SESSION_RESTORE] user_id=%s status=failed reason=%s",
                        session.user_id,
                        restore_failure_reason if restore_failure_reason != "unknown" else type(exc).__name__,
                    )
                raise ServiceError(SESSION_DISCONNECTED, 409) from exc

    def persistence_debug(self) -> dict[str, Any]:
        if self.store is None:
            return {
                "stored_sessions": 0,
                "users": [],
            }
        return self.store.persistence_debug()

    def _mark_disconnected(self, user_id: str, reason: str) -> None:
        logger.warning("[SESSION-DISCONNECTED] %s", user_id)
        session = self.get(user_id)
        if session is not None:
            try:
                session.client.api.close()
            except Exception:
                logger.warning(
                    "[SESSION_CLOSE_FAILED] user_id=%s reason=%s",
                    user_id,
                    reason,
                )
        self.remove(user_id)
        logger.info("[WORKER_DESTROYED] user_id=%s", user_id)
        if self.store is not None:
            self.store.mark_disconnected(user_id)
        self._mark_probe_failure(user_id, offline=True)
        raise ServiceError(SESSION_DISCONNECTED, 409) from None

    def _persist_connected(self, session: ManagedSession) -> None:
        if self.store is None or session.requires_2fa:
            return
        if not session.real_mode_confirmed:
            logger.warning(
                "[REAL_MODE_FAILED] user_id=%s active_mode=%s reason=persist_blocked",
                session.user_id,
                session.active_mode or "UNKNOWN",
            )
            return
        token = session.state.SSID
        if not session.email or not token:
            return
        self.store.save_connected(
            session.user_id,
            session.email,
            session.desired_mode,
            str(token),
        )

    def _finalize_connect(
        self,
        session: ManagedSession,
        ok: bool,
        reason: Any,
        *,
        user_id: str,
        attempt: int,
    ) -> None:
        if ok:
            session.requires_2fa = False
            self._populate_ready_state(session, user_id=user_id, attempt=attempt)
            return

        session.requires_2fa = reason == "2FA"
        if session.requires_2fa:
            return
        raise ServiceError(f"falha ao conectar: {reason}", 401)

    def _close_session(self, session: ManagedSession) -> None:
        with self._session_context(session):
            try:
                session.client.api.close()
            except Exception:
                logger.exception("falha ao fechar sessao anterior de %s", session.user_id)

    @contextmanager
    def _session_context(self, session: ManagedSession):
        with self._call_gate:
            with self._runtime_lock:
                self._activate(session.state)
                try:
                    yield
                finally:
                    session.state = self._capture()

    def _activate(self, state: SessionState) -> None:
        global_value.check_websocket_if_connect = state.check_websocket_if_connect
        global_value.ssl_Mutual_exclusion = state.ssl_Mutual_exclusion
        global_value.ssl_Mutual_exclusion_write = state.ssl_Mutual_exclusion_write
        global_value.SSID = state.SSID
        global_value.check_websocket_if_error = state.check_websocket_if_error
        global_value.websocket_error_reason = state.websocket_error_reason
        global_value.balance_id = state.balance_id

    def _capture(self) -> SessionState:
        return SessionState(
            check_websocket_if_connect=global_value.check_websocket_if_connect,
            ssl_Mutual_exclusion=global_value.ssl_Mutual_exclusion,
            ssl_Mutual_exclusion_write=global_value.ssl_Mutual_exclusion_write,
            SSID=global_value.SSID,
            check_websocket_if_error=global_value.check_websocket_if_error,
            websocket_error_reason=global_value.websocket_error_reason,
            balance_id=global_value.balance_id,
        )


def normalize_mode(mode: str) -> str:
    normalized = (mode or "").strip().upper()
    if normalized not in ALLOWED_BALANCE_MODES:
        raise ServiceError("mode invalido. Use PRACTICE, REAL ou TOURNAMENT")
    return normalized


def read_max_concurrent_api_calls() -> int:
    raw_value = os.getenv("BULLEX_MAX_CONCURRENT_API_CALLS", "1").strip()
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise RuntimeError("BULLEX_MAX_CONCURRENT_API_CALLS must be an integer") from exc

    return max(1, value)


def normalize_action(action: str) -> str:
    normalized = (action or "").strip().lower()
    if normalized not in ALLOWED_ACTIONS:
        raise ServiceError("action invalida. Use call ou put")
    return normalized


def normalize_binary_active(active: str) -> str:
    return (active or "").strip().upper()


def ensure_binary_asset_allowed(active: str) -> str:
    normalized = normalize_binary_active(active)
    if normalized not in BINARY_ALLOWED_ASSET_SET:
        raise ServiceError(ASSET_NOT_ALLOWED, 400)
    return normalized


def build_success(data: Any) -> dict[str, Any]:
    return {"ok": True, "data": data, "error": None}


def build_error(
    message: str,
    data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {"ok": False, "data": data, "error": message}


def with_login_progress(payload: dict[str, Any], user_id: str) -> dict[str, Any]:
    data = payload.get("data")
    if isinstance(data, dict):
        data["login_progress"] = session_manager.login_progress_payload(user_id)
    return payload


def require_user_id(x_user_id: str | None) -> str:
    user_id = (x_user_id or "").strip()
    if not user_id:
        raise ServiceError("header x-user-id e obrigatorio", 400)
    return user_id


def ensure_mode_matches(client: Bullex, expected_mode: str) -> str:
    current_mode = normalize_mode(client.get_balance_mode())
    if current_mode != expected_mode:
        raise ServiceError(
            f"conta ativa em {current_mode}; esperado {expected_mode} para esta operacao",
            409,
        )
    return current_mode


def ensure_session_ready(session: ManagedSession) -> None:
    if session.requires_2fa:
        raise ServiceError("sessao aguardando 2FA", 409)
    if not session.client.check_connect():
        raise ServiceError(SESSION_DISCONNECTED, 409)


def parse_order_id(order_id: str) -> int | str:
    return int(order_id) if order_id.isdigit() else order_id


def build_cache_key(path: str, params: dict[str, Any] | None = None) -> str:
    if not params:
        return path
    parts = [f"{key}={params[key]}" for key in sorted(params)]
    return f"{path}?{'&'.join(parts)}"


def normalize_balance_value(balance: Any) -> float | None:
    if balance is None:
        return None
    return float(balance)


def real_mode_service_error(active_mode: Any) -> ServiceError:
    try:
        mode = normalize_mode(active_mode)
    except ServiceError:
        mode = str(active_mode or "UNKNOWN").strip().upper() or "UNKNOWN"
    return ServiceError(
        BULLEX_ACTIVE_MODE_NOT_REAL,
        409,
        {
            "connected": True,
            "active_mode": mode,
            "mode": mode,
            "balance": None,
        },
    )


def force_real_mode(session: ManagedSession, *, user_id: str) -> str:
    session.desired_mode = "REAL"
    session.real_mode_confirmed = False
    logger.info("[REAL_MODE_FORCED] user_id=%s", user_id)
    active_mode = "UNKNOWN"
    for attempt in range(1, 4):
        try:
            before_mode = normalize_mode(session.client.get_balance_mode())
        except ServiceError:
            before_mode = "UNKNOWN"
        logger.info(
            "[CHANGE_BALANCE_REAL_ATTEMPT] user_id=%s attempt=%s active_mode_before=%s",
            user_id,
            attempt,
            before_mode,
        )
        logger.info("[CHANGE_BALANCE_REAL_CALL] user_id=%s active_mode=%s", user_id, before_mode)
        try:
            session.client.change_balance("REAL")
            logger.info(
                "[CHANGE_BALANCE_REAL_RESULT] user_id=%s attempt=%s result=called",
                user_id,
                attempt,
            )
        except Exception:
            logger.warning(
                "[CHANGE_BALANCE_REAL_RESULT] user_id=%s attempt=%s result=exception",
                user_id,
                attempt,
                exc_info=True,
            )
            logger.warning(
                "[CHANGE_BALANCE_REAL_FAILED] user_id=%s active_mode=%s reason=exception",
                user_id,
                before_mode,
                exc_info=True,
            )
        time.sleep(1)
        try:
            active_mode = normalize_mode(session.client.get_balance_mode())
        except ServiceError:
            active_mode = "UNKNOWN"
        session.active_mode = active_mode
        logger.info(
            "[ACTIVE_MODE_AFTER_CHANGE] user_id=%s attempt=%s active_mode=%s",
            user_id,
            attempt,
            active_mode,
        )
        if active_mode == "REAL":
            session.real_mode_confirmed = True
            logger.info("[CHANGE_BALANCE_REAL_RESULT] user_id=%s attempt=%s result=confirmed", user_id, attempt)
            logger.info("[CHANGE_BALANCE_REAL_OK] user_id=%s active_mode=%s", user_id, active_mode)
            logger.info("[REAL_MODE_CONFIRMED] user_id=%s active_mode=%s", user_id, active_mode)
            return active_mode
        if attempt < 3:
            logger.warning(
                "[CHANGE_BALANCE_REAL_RESULT] user_id=%s attempt=%s result=still_%s",
                user_id,
                attempt,
                active_mode,
            )

    session.real_mode_confirmed = False
    logger.warning("[REAL_MODE_FAILED] user_id=%s active_mode=%s", user_id, active_mode)
    logger.warning("[CHANGE_BALANCE_REAL_FAILED] user_id=%s active_mode=%s", user_id, active_mode)
    raise real_mode_service_error(active_mode)


def discover_balance_ids(client: Bullex) -> tuple[Any, Any, bool]:
    balances: Any = None
    balances_discovered = False
    get_profile = getattr(client, "get_profile_ansyc", None)
    if callable(get_profile):
        profile = get_profile()
        balances = profile.get("balances") if isinstance(profile, dict) else None
        balances_discovered = isinstance(balances, list)
    if not isinstance(balances, list):
        get_balances = getattr(client, "get_balances", None)
        if callable(get_balances):
            raw = get_balances()
            balances = raw.get("msg") if isinstance(raw, dict) else None
            balances_discovered = isinstance(balances, list)

    real_balance_id = None
    practice_balance_id = None
    for balance in balances if isinstance(balances, list) else []:
        if not isinstance(balance, dict):
            continue
        balance_type = balance.get("type")
        if balance_type == 1:
            real_balance_id = balance.get("id")
        elif balance_type == 4:
            practice_balance_id = balance.get("id")
    return real_balance_id, practice_balance_id, balances_discovered


def read_separated_balances(client: Bullex) -> tuple[float | None, float | None]:
    real_balance = None
    practice_balance = None
    get_balances = getattr(client, "get_balances", None)
    if callable(get_balances):
        raw = get_balances()
        items = raw.get("msg") if isinstance(raw, dict) else None
        if isinstance(items, list):
            for item in items:
                if not isinstance(item, dict):
                    continue
                amount = normalize_balance_value(item.get("amount"))
                if item.get("type") == 1:
                    real_balance = amount
                elif item.get("type") == 4:
                    practice_balance = amount
    return real_balance, practice_balance


def build_real_only_account_response(account: dict[str, Any]) -> dict[str, Any]:
    active_mode = str(
        account.get("active_mode")
        or account.get("active_mode_from_bullex")
        or account.get("mode")
        or ""
    ).strip().upper() or None
    if bool(account.get("connected")) and active_mode != "REAL":
        blocked = {
            **account,
            "active_mode": active_mode,
            "active_mode_from_bullex": active_mode,
            "mode": active_mode,
            "balance": None,
        }
        logger.warning(
            "[PRACTICE_BALANCE_BLOCKED] user_id=%s active_mode=%s",
            account.get("user_id") or "unknown",
            active_mode,
        )
        return {
            "ok": False,
            "data": blocked,
            "error": BULLEX_ACTIVE_MODE_NOT_REAL,
        }
    return build_success(account)


def normalize_real_only_account_payload(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data") if isinstance(payload, dict) else None
    if isinstance(data, dict):
        return build_real_only_account_response(data)
    return payload


def build_account_payload(session: ManagedSession) -> dict[str, Any]:
    connected = bool(session.client.check_connect())
    account = {
        "connected": connected,
        "balance": None,
        "currency": None,
        "mode": None,
        "user_id": session.user_id,
        "email": session.email,
        "requires_2fa": session.requires_2fa,
        "active_mode_real_detected": False,
        "active_mode": None,
        "active_mode_from_bullex": None,
        "balance_real": None,
        "balance_practice": None,
    }

    if connected and not session.requires_2fa:
        mode = normalize_mode(session.client.get_balance_mode())
        balance_real, balance_practice = read_separated_balances(session.client)
        if balance_real is None and balance_practice is None:
            current_balance = normalize_balance_value(session.client.get_balance())
            if mode == "REAL":
                balance_real = current_balance
            elif mode == "PRACTICE":
                balance_practice = current_balance
        balance = balance_real if mode == "REAL" else None
        currency = (session.client.get_currency() or "BRL") if mode == "REAL" else None
        account["balance"] = balance
        account["currency"] = currency
        account["mode"] = mode
        account["active_mode_real_detected"] = mode == "REAL"
        account["active_mode"] = mode
        account["active_mode_from_bullex"] = mode
        account["balance_real"] = balance_real
        account["balance_practice"] = balance_practice
        if mode == "REAL":
            logger.info("[ACCOUNT_REAL_CONNECTED] user_id=%s email=%s", session.user_id, session.email)
            logger.info("[REAL_BALANCE_DETECTED] user_id=%s balance=%s", session.user_id, balance_real)
            if balance == 0:
                account["real_balance_warning"] = "BALANCE_ZERO"
                logger.info("[BALANCE_ZERO_NOT_DISCONNECTED] user_id=%s mode=REAL", session.user_id)
        elif balance_practice is not None:
            logger.warning(
                "[PRACTICE_BALANCE_IGNORED] user_id=%s balance=%s",
                session.user_id,
                balance_practice,
            )

    return account


def normalize_active(symbol: Any, active_id: Any) -> dict[str, Any]:
    return {
        "active_id": active_id,
        "symbol": str(symbol),
        "name": str(symbol),
        "enabled": True,
    }


def normalize_assets(raw_assets: Any) -> list[dict[str, Any]]:
    if not isinstance(raw_assets, dict):
        return []
    available_assets = {
        normalize_binary_active(symbol): normalize_active(normalize_binary_active(symbol), active_id)
        for symbol, active_id in raw_assets.items()
    }
    filtered_assets = []
    for symbol in BINARY_ALLOWED_ASSETS:
        asset = available_assets.get(symbol)
        if asset is None:
            logger.warning("[BINARY ASSET MISSING] %s", symbol)
            continue
        filtered_assets.append(asset)
    return filtered_assets


def normalize_candle(candle: dict[str, Any]) -> dict[str, Any]:
    return {
        "from": candle.get("from") or candle.get("at") or candle.get("id"),
        "open": candle.get("open"),
        "close": candle.get("close"),
        "min": candle.get("min") if "min" in candle else candle.get("low"),
        "max": candle.get("max") if "max" in candle else candle.get("high"),
        "volume": candle.get("volume", 0),
    }


def normalize_candles(raw_candles: Any) -> list[dict[str, Any]]:
    if not isinstance(raw_candles, list):
        return []
    return [normalize_candle(candle) for candle in raw_candles if isinstance(candle, dict)]


def read_assets(client: Bullex) -> list[dict[str, Any]]:
    client.update_ACTIVES_OPCODE()
    return normalize_assets(client.get_all_ACTIVES_OPCODE())


def read_digital_payout(client: Bullex, active: str) -> int | float | None:
    getter = getattr(client, "get_digital_payout", None)
    if not callable(getter):
        return None
    try:
        payout = getter(active, seconds=3)
    except SESSION_EXCEPTION_TYPES:
        raise
    except Exception:
        logger.exception("falha ao consultar payout digital de %s", active)
        raise ServiceError(SESSION_DISCONNECTED, 409)
    return payout if payout else None


cors_origins = [
    origin.strip()
    for origin in os.getenv("CORS_ORIGINS", CORS_ALLOWED_ORIGINS_DEFAULT).split(",")
    if origin.strip()
]

app = FastAPI(title="bullex-service", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=CORS_ALLOWED_METHODS,
    allow_headers=CORS_ALLOWED_HEADERS,
)
session_manager = SessionManager(create_session_store())


@app.middleware("http")
async def serialize_user_market_requests(request: Request, call_next):
    if request.url.path not in {
        "/sessions/status",
        "/account",
        "/candles",
        "/payouts",
    }:
        return await call_next(request)
    user_id = str(request.headers.get("x-user-id") or "").strip()
    if not user_id:
        return await call_next(request)
    async with session_manager.async_user_lock(user_id):
        return await call_next(request)


@app.on_event("startup")
def startup_without_session_restore() -> None:
    logger.info("[STARTUP_READY] restore disabled")


def ensure_session_alive(user_id: str) -> ManagedSession:
    return session_manager.ensure_session_alive(user_id)


@app.exception_handler(ServiceError)
def service_error_handler(_: Request, exc: ServiceError) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content=build_error(exc.message, exc.data))


@app.exception_handler(RequestValidationError)
def validation_error_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
    message = "; ".join(error["msg"] for error in exc.errors())
    return JSONResponse(status_code=422, content=build_error(message))


@app.exception_handler(Exception)
def unhandled_error_handler(_: Request, exc: Exception) -> JSONResponse:
    logger.exception("erro nao tratado", exc_info=exc)
    return JSONResponse(status_code=500, content=build_error("erro interno"))


@app.get("/health")
def health() -> dict[str, Any]:
    return build_success({"status": "healthy", "service": "bullex-service"})


@app.post("/sessions/connect")
def connect_session(
    payload: ConnectRequest,
    x_user_id: str | None = Header(default=None),
) -> Any:
    user_id = str(x_user_id or "").strip()
    try:
        user_id = require_user_id(x_user_id)
        payload.account_mode = "REAL"
        session = session_manager.connect(user_id, payload)
        session.desired_mode = "REAL"

        connected = False
        active_mode = session.active_mode or "REAL"
        if not session.requires_2fa:
            if not session.real_mode_confirmed:
                with session_manager._session_context(session):
                    try:
                        active_mode = force_real_mode(session, user_id=user_id)
                    except ServiceError as exc:
                        active_mode = str((exc.data or {}).get("active_mode") or "UNKNOWN").strip().upper()
                        session.active_mode = active_mode
                        session.real_mode_confirmed = False
                        session_manager.clear_user_runtime_cache(user_id)
                        return JSONResponse(
                            status_code=exc.status_code,
                            content=build_error(exc.message, exc.data),
                        )
            connected = active_mode == "REAL"
            if not connected:
                logger.warning("[REAL_MODE_FAILED] user_id=%s active_mode=%s", user_id, active_mode)
                return JSONResponse(
                    status_code=409,
                    content=build_error(
                        BULLEX_ACTIVE_MODE_NOT_REAL,
                        {
                            "connected": True,
                            "active_mode": active_mode,
                            "mode": active_mode,
                            "balance": None,
                        },
                    ),
                )
            logger.info("[REAL_MODE_CONFIRMED] user_id=%s", user_id)

        return with_login_progress(build_success(
            {
                "user_id": user_id,
                "connected": connected,
                "requires_2fa": session.requires_2fa,
                "active_mode": active_mode,
                "active_mode_from_bullex": active_mode,
                "active_mode_real_detected": active_mode == "REAL",
            }
        ), user_id)
    except ServiceError as exc:
        logger.warning(
            "[CONNECT_FAILED_HANDLED] user_id=%s detail=%s",
            user_id or "missing",
            exc.message,
        )
        return JSONResponse(
            status_code=exc.status_code,
            content=build_error(exc.message, exc.data),
        )
    except Exception as exc:
        logger.warning(
            "[CONNECT_FAILED_HANDLED] user_id=%s detail=%s",
            user_id or "missing",
            type(exc).__name__,
            exc_info=True,
        )
        try:
            failed_session = session_manager.get(user_id)
            if failed_session is not None:
                session_manager._close_session(failed_session)
            session_manager.remove(user_id)
            session_manager.clear_probe_cache(user_id)
            if session_manager.store is not None:
                session_manager.store.mark_disconnected(user_id, revoke_token=True)
        except Exception:
            logger.warning(
                "[CONNECT_CLEANUP_FAILED] user_id=%s",
                user_id or "missing",
                exc_info=True,
            )
        return JSONResponse(
            status_code=503,
            content=build_error("LOGIN_FAILED"),
        )


@app.get("/sessions/status")
def session_status(
    x_user_id: str | None = Header(default=None),
    x_allow_session_restore: str | None = Header(default=None),
) -> JSONResponse:
    user_id = require_user_id(x_user_id)
    allow_session_restore = str(x_allow_session_restore or "").strip().lower() == "true"
    with session_manager.user_lock(user_id):
        try:
            if session_manager.get(user_id) is None and allow_session_restore:
                session_manager.restore_on_demand(user_id)
        except ServiceError as exc:
            if exc.message in {SESSION_NOT_FOUND, SESSION_DISCONNECTED}:
                status_code = 404 if exc.message == SESSION_NOT_FOUND else 409
                payload = {"ok": False, "data": {"connected": False}, "error": exc.message}
                session_manager._mark_probe_failure(user_id, offline=True)
                return JSONResponse(status_code=status_code, content=with_login_progress(payload, user_id))
            raise

        cached = session_manager.get_cached_probe(user_id, "/sessions/status", path="/sessions/status")
        if cached is not None:
            status_code, payload = cached
            return JSONResponse(status_code=status_code, content=with_login_progress(payload, user_id))

        def operation(current: ManagedSession) -> dict[str, Any]:
            connected = bool(current.client.check_connect())
            active_mode = current.client.get_balance_mode() if connected and not current.requires_2fa else None
            return {
                "user_id": user_id,
                "connected": connected,
                "requires_2fa": current.requires_2fa,
                "email": current.email,
                "active_mode": active_mode,
                "server_time": current.client.get_server_timestamp() if connected else None,
            }

        try:
            payload = with_login_progress(build_success(session_manager.run(user_id, operation)), user_id)
            session_manager._mark_probe_success(
                user_id,
                "/sessions/status",
                200,
                payload,
                ttl_seconds=SESSION_STATUS_TTL_SECONDS,
            )
            return JSONResponse(status_code=200, content=payload)
        except ServiceError as exc:
            if exc.message in {SESSION_NOT_FOUND, SESSION_DISCONNECTED}:
                status_code = 404 if exc.message == SESSION_NOT_FOUND else 409
                payload = {"ok": False, "data": {"connected": False}, "error": exc.message}
                session_manager._mark_probe_failure(user_id, offline=True)
                return JSONResponse(status_code=status_code, content=with_login_progress(payload, user_id))
            raise


@app.get("/sessions/persistence-debug")
def sessions_persistence_debug() -> dict[str, Any]:
    return session_manager.persistence_debug()


@app.post("/sessions/disconnect")
def disconnect_session(x_user_id: str | None = Header(default=None)) -> dict[str, Any]:
    user_id = require_user_id(x_user_id)
    session_manager.disconnect(user_id)
    session_manager.clear_probe_cache(user_id)
    return build_success({"user_id": user_id, "connected": False})


@app.post("/sessions/reconnect")
def reconnect_session(x_user_id: str | None = Header(default=None)) -> dict[str, Any]:
    user_id = require_user_id(x_user_id)
    session_manager.reconnect(user_id)
    session_manager.clear_probe_cache(user_id)
    return with_login_progress(build_success(session_manager.run(user_id, build_account_payload)), user_id)


@app.get("/account/balance")
def account_balance(x_user_id: str | None = Header(default=None)) -> dict[str, Any]:
    user_id = require_user_id(x_user_id)
    return build_real_only_account_response(
        session_manager.run(user_id, build_account_payload)
    )


@app.get("/account")
def account_overview(x_user_id: str | None = Header(default=None)) -> dict[str, Any]:
    user_id = require_user_id(x_user_id)
    cached = session_manager.get_cached_probe(user_id, "/account", path="/account")
    if cached is not None:
        _, payload = cached
        return with_login_progress(
            normalize_real_only_account_payload(payload),
            user_id,
        )
    if session_manager.get(user_id) is None:
        previous = session_manager.get_last_probe(user_id, "/account")
        if previous is not None:
            return with_login_progress(
                normalize_real_only_account_payload(previous[1]),
                user_id,
            )
        payload = with_login_progress(build_success({"connected": False}), user_id)
        return payload

    def operation(session: ManagedSession) -> dict[str, Any]:
        return build_account_payload(session)

    try:
        account = session_manager.run(
            user_id,
            operation,
            disconnect_on_error=False,
        )
        payload = with_login_progress(
            build_real_only_account_response(account),
            user_id,
        )
        if bool((payload.get("data") or {}).get("connected")):
            session_manager._mark_probe_success(
                user_id,
                "/account",
                200,
                payload,
                ttl_seconds=ACCOUNT_TTL_SECONDS,
            )
        else:
            session_manager._mark_probe_failure(user_id, offline=True)
        return payload
    except ServiceError as exc:
        if exc.message in {SESSION_NOT_FOUND, SESSION_DISCONNECTED}:
            cached = session_manager.get_last_probe(user_id, "/account")
            if cached is not None:
                return with_login_progress(
                    normalize_real_only_account_payload(cached[1]),
                    user_id,
                )
            return with_login_progress(build_success({"connected": False}), user_id)
        cached = session_manager.get_last_probe(user_id, "/account")
        if cached is not None:
            return with_login_progress(
                normalize_real_only_account_payload(cached[1]),
                user_id,
            )
        return with_login_progress(build_error("ACCOUNT_TEMPORARY_UNAVAILABLE"), user_id)


@app.post("/account/change-mode")
def account_change_mode(payload: ChangeModeRequest, x_user_id: str | None = Header(default=None)) -> dict[str, Any]:
    user_id = require_user_id(x_user_id)
    target_mode = "REAL"

    def operation(session: ManagedSession) -> dict[str, Any]:
        ensure_session_ready(session)
        active_mode = force_real_mode(session, user_id=user_id)
        session.desired_mode = active_mode
        session_manager._persist_connected(session)
        session_manager.clear_probe_cache(user_id)
        return {"mode": active_mode}

    return build_success(session_manager.run(user_id, operation))


@app.get("/assets")
def list_assets(x_user_id: str | None = Header(default=None)) -> dict[str, Any]:
    user_id = require_user_id(x_user_id)
    cache_key = build_cache_key("/assets")
    cached = session_manager.get_cached_probe(user_id, cache_key, path="/assets")
    if cached is not None:
        _, payload = cached
        return payload

    def operation(session: ManagedSession) -> list[dict[str, Any]]:
        ensure_session_ready(session)
        try:
            return read_assets(session.client)
        except SESSION_EXCEPTION_TYPES as exc:
            logger.warning("[SESSION-DEAD] %s %s", user_id, type(exc).__name__)
            raise ServiceError(SESSION_DISCONNECTED, 409) from exc
        except Exception as exc:
            logger.exception("falha ao listar ativos")
            raise ServiceError(SESSION_DISCONNECTED, 409) from exc

    payload = build_success(session_manager.run(user_id, operation))
    session_manager._mark_probe_success(
        user_id,
        cache_key,
        200,
        payload,
        ttl_seconds=ASSETS_TTL_SECONDS,
    )
    return payload


@app.get("/candles")
def get_candles(
    active: str,
    interval: int,
    count: int,
    endtime: int | None = None,
    x_user_id: str | None = Header(default=None),
) -> dict[str, Any]:
    user_id = require_user_id(x_user_id)
    active = ensure_binary_asset_allowed(active)
    resolved_endtime = endtime or int(time.time())
    cache_key = build_cache_key(
        "/candles",
        {"active": active, "interval": interval},
    )
    cached = session_manager.get_cached_probe(user_id, cache_key, path="/candles")
    if cached is not None:
        _, payload = cached
        return payload

    def operation(session: ManagedSession) -> list[dict[str, Any]]:
        ensure_session_ready(session)
        try:
            candles = session.client.get_candles(active, interval, count, resolved_endtime)
        except SESSION_EXCEPTION_TYPES as exc:
            logger.warning("[SESSION-DEAD] %s %s", user_id, type(exc).__name__)
            raise ServiceError(SESSION_DISCONNECTED, 409) from exc
        except Exception as exc:
            logger.exception("falha ao obter candles de %s", active)
            raise ServiceError(SESSION_DISCONNECTED, 409) from exc
        if candles is None:
            raise ServiceError(SESSION_DISCONNECTED, 409)
        return normalize_candles(candles)

    try:
        payload = build_success(
            session_manager.run(
                user_id,
                operation,
                disconnect_on_error=False,
            )
        )
        session_manager._mark_probe_success(
            user_id,
            cache_key,
            200,
            payload,
            ttl_seconds=CANDLES_TTL_SECONDS,
        )
        return payload
    except ServiceError:
        cached = session_manager.get_last_probe(user_id, cache_key)
        if cached is not None:
            return cached[1]
        return build_error("CANDLES_TEMPORARY_UNAVAILABLE")


@app.get("/payouts")
def get_payouts(active: str | None = None, x_user_id: str | None = Header(default=None)) -> dict[str, Any]:
    user_id = require_user_id(x_user_id)
    active = ensure_binary_asset_allowed(active) if active else None
    cache_key = build_cache_key("/payouts", {"active": active} if active else None)
    cached = session_manager.get_cached_probe(user_id, cache_key, path="/payouts")
    if cached is not None:
        _, payload = cached
        return payload

    def operation(session: ManagedSession) -> list[dict[str, Any]]:
        ensure_session_ready(session)
        if active:
            symbols = [active]
        else:
            try:
                symbols = [asset["symbol"] for asset in read_assets(session.client)]
            except SESSION_EXCEPTION_TYPES as exc:
                logger.warning("[SESSION-DEAD] %s %s", user_id, type(exc).__name__)
                raise ServiceError(SESSION_DISCONNECTED, 409) from exc
            except Exception as exc:
                logger.exception("falha ao listar ativos para payouts")
                raise ServiceError(SESSION_DISCONNECTED, 409) from exc

        return [
            {
                "symbol": symbol,
                "payout": read_digital_payout(session.client, symbol) if active else None,
                "type": "digital",
            }
            for symbol in symbols
        ]

    try:
        payload = build_success(
            session_manager.run(
                user_id,
                operation,
                disconnect_on_error=False,
            )
        )
        session_manager._mark_probe_success(
            user_id,
            cache_key,
            200,
            payload,
            ttl_seconds=PAYOUT_TTL_SECONDS,
        )
        return payload
    except ServiceError:
        cached = session_manager.get_last_probe(user_id, cache_key)
        if cached is not None:
            return cached[1]
        return build_error("PAYOUTS_TEMPORARY_UNAVAILABLE")


@app.post("/orders/buy-demo")
def buy_demo(payload: BuyOrderRequest, x_user_id: str | None = Header(default=None)) -> dict[str, Any]:
    require_user_id(x_user_id)
    raise ServiceError("REAL_MODE_ONLY", 409)


@app.post("/orders/buy-real")
def buy_real(payload: BuyOrderRequest, x_user_id: str | None = Header(default=None)) -> dict[str, Any]:
    user_id = require_user_id(x_user_id)
    payload.action = normalize_action(payload.action)
    logger.info(
        "[REAL MODE DETECTED] user_id=%s confirm_real=%s",
        user_id,
        payload.confirm_real,
    )
    logger.info(
        "[REAL BUY ATTEMPT] user_id=%s active=%s action=%s amount=%s expiration=%s",
        user_id,
        payload.active,
        payload.action,
        payload.amount,
        payload.expiration,
    )
    if not payload.confirm_real:
        reason = "CONFIRM_REAL_REQUIRED"
        logger.warning("[REAL BUY BLOCKED reason=%s] user_id=%s", reason, user_id)
        raise ServiceError(reason, 403)
    if payload.amount <= 0:
        reason = "AMOUNT_MUST_BE_POSITIVE"
        logger.warning("[REAL BUY BLOCKED reason=%s] user_id=%s", reason, user_id)
        raise ServiceError(reason, 403)

    def operation(session: ManagedSession) -> dict[str, Any]:
        ensure_session_ready(session)
        try:
            ensure_mode_matches(session.client, "REAL")
        except ServiceError as exc:
            reason = "ACCOUNT_MODE_NOT_REAL"
            logger.warning(
                "[REAL BUY BLOCKED reason=%s] user_id=%s detail=%s",
                reason,
                user_id,
                exc.message,
            )
            raise ServiceError(reason, 403) from exc
        ok, order_id = session.client.buy(payload.amount, payload.active, payload.action, payload.expiration)
        if not ok:
            reason = f"falha ao criar ordem real: {order_id}"
            logger.warning("[REAL BUY BLOCKED reason=%s] user_id=%s", reason, user_id)
            raise ServiceError(reason, 409)
        logger.info("[REAL BUY SUCCESS order_id=%s] user_id=%s", order_id, user_id)
        return {
            "mode": "REAL",
            "order_id": order_id,
            "active": payload.active,
            "amount": payload.amount,
            "action": payload.action,
            "expiration": payload.expiration,
        }

    return build_success(session_manager.run(user_id, operation))


@app.get("/orders/{order_id}/result")
def order_result(order_id: str, x_user_id: str | None = Header(default=None)) -> dict[str, Any]:
    user_id = require_user_id(x_user_id)
    parsed_order_id = parse_order_id(order_id)

    def operation(session: ManagedSession) -> dict[str, Any]:
        ensure_session_ready(session)
        socket_option_closed = getattr(session.client.api, "socket_option_closed", {})
        order_binary = getattr(session.client.api, "order_binary", {})
        closed_order = socket_option_closed.get(parsed_order_id)
        if closed_order is None:
            closed_order = socket_option_closed.get(str(parsed_order_id))
        if closed_order is None:
            closed_order = order_binary.get(parsed_order_id)
        if closed_order is None:
            closed_order = order_binary.get(str(parsed_order_id))
        if not isinstance(closed_order, dict):
            return {"order_id": parsed_order_id, "result": "PENDING_RESULT", "profit": None}
        message = closed_order.get("msg") if isinstance(closed_order.get("msg"), dict) else closed_order
        if not isinstance(message, dict):
            return {"order_id": parsed_order_id, "result": "PENDING_RESULT", "profit": None}

        result = str(message.get("win") or "").strip().lower()
        if not result:
            return {"order_id": parsed_order_id, "result": "PENDING_RESULT", "profit": None}
        amount = float(message.get("sum") or 0)
        if result == "equal":
            profit = 0.0
        elif result == "loose":
            profit = -amount
        else:
            profit = float(message.get("win_amount") or 0) - amount
        return {"order_id": parsed_order_id, "result": result, "profit": profit}

    return build_success(session_manager.run(user_id, operation))
