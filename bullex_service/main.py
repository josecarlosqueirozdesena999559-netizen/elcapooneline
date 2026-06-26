import logging
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from threading import BoundedSemaphore, RLock
from typing import Any

from fastapi import FastAPI, Header, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

import bullexapi.global_value as global_value
from bullexapi.stable_api import Bullex
from bullex_service.session_store import SessionStore, create_session_store
from websocket._exceptions import WebSocketConnectionClosedException


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bullex-service")

ALLOWED_BALANCE_MODES = {"PRACTICE", "REAL", "TOURNAMENT"}
ALLOWED_ACTIONS = {"call", "put"}
SESSION_NOT_FOUND = "SESSION_NOT_FOUND"
SESSION_DISCONNECTED = "SESSION_DISCONNECTED"
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
SESSION_STATUS_TTL_SECONDS = 15
ACCOUNT_TTL_SECONDS = 10
ASSETS_TTL_SECONDS = 300
PAYOUT_TTL_SECONDS = 60
CANDLES_TTL_SECONDS = 2
SESSION_STATUS_THROTTLE_SECONDS = 5
SESSION_OFFLINE_TTL_SECONDS = 60
SESSION_FAILURE_BACKOFF_SECONDS = (10, 30, 60, 300)


class ServiceError(Exception):
    def __init__(self, message: str, status_code: int = 400):
        self.message = message
        self.status_code = status_code
        super().__init__(message)


class ConnectRequest(BaseModel):
    email: str | None = None
    password: str | None = None
    sms_code: str | None = None
    account_mode: str = Field(default="PRACTICE")


class ChangeModeRequest(BaseModel):
    mode: str
    confirm_real: bool = False


class BuyOrderRequest(BaseModel):
    amount: float
    active: str
    action: str
    expiration: int
    confirm_real: bool = False


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
    desired_mode: str = "PRACTICE"
    requires_2fa: bool = False
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


class SessionManager:
    def __init__(self, store: SessionStore | None = None) -> None:
        self.sessions: dict[str, ManagedSession] = {}
        self._probe_cache: dict[str, SessionProbeState] = {}
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

    def get_probe_state(self, user_id: str) -> SessionProbeState:
        return self._probe_cache.setdefault(user_id, SessionProbeState())

    def upsert(self, session: ManagedSession) -> ManagedSession:
        self.sessions[session.user_id] = session
        return session

    def remove(self, user_id: str) -> None:
        self.sessions.pop(user_id, None)

    def clear_probe_cache(self, user_id: str) -> None:
        probe = self.get_probe_state(user_id)
        probe.responses.clear()
        probe.last_request_at.clear()

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
        logger.info("[SESSION_STATUS_THROTTLED] %s %s", user_id, cache_key)
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

    def _mark_probe_failure(self, user_id: str, *, offline: bool = False) -> None:
        probe = self.get_probe_state(user_id)
        probe.failure_count += 1
        now = time.time()
        disconnected_payload = build_success({"connected": False})
        if offline:
            probe.offline_until = now + SESSION_OFFLINE_TTL_SECONDS
            probe.next_retry_at = probe.offline_until
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
            logger.info("[CACHE_HIT] user_id=%s path=%s", user_id, path)
            if path == "/sessions/status":
                logger.info("[SESSION_STATUS_CACHE_HIT] %s %s", user_id, path)
            return cached.status_code, cached.payload
        logger.info("[CACHE_MISS] user_id=%s path=%s", user_id, path)
        if path == "/sessions/status":
            logger.info("[SESSION_STATUS_CACHE_MISS] %s %s", user_id, path)
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
            return session

        logger.warning("[SESSION-DEAD] %s %s", user_id, dead_reason)
        self._mark_probe_failure(user_id)
        return self._attempt_reconnect(session, dead_reason)

    def run(self, user_id: str, operation):
        session = self.ensure_session_alive(user_id)
        try:
            with self._session_context(session):
                return operation(session)
        except ServiceError as exc:
            if exc.message == SESSION_DISCONNECTED:
                logger.warning("[SESSION-DISCONNECTED] %s", user_id)
                self.remove(user_id)
            raise
        except SESSION_EXCEPTION_TYPES as exc:
            self._mark_disconnected(user_id, type(exc).__name__)
        except Exception as exc:
            self._mark_disconnected(user_id, type(exc).__name__)

    def connect(self, user_id: str, payload: ConnectRequest) -> ManagedSession:
        probe = self.get_probe_state(user_id)
        probe.failure_count = 0
        probe.next_retry_at = 0.0
        probe.offline_until = 0.0
        existing = self.get(user_id)

        if payload.sms_code and existing and not payload.email and not payload.password:
            session = existing
            session.sms_code = payload.sms_code
            session.desired_mode = normalize_mode(payload.account_mode)
            with self._session_context(session):
                ok, reason = session.client.connect_2fa(payload.sms_code)
                self._finalize_connect(session, ok, reason)
            self._persist_connected(session)
            return session

        if not payload.email or not payload.password:
            raise ServiceError("email e password sao obrigatorios para conectar")

        new_session = ManagedSession(
            user_id=user_id,
            client=Bullex(payload.email, payload.password),
            email=payload.email,
            password=payload.password,
            sms_code=payload.sms_code,
            desired_mode=normalize_mode(payload.account_mode),
        )

        if existing is not None:
            self._close_session(existing)

        self.upsert(new_session)
        with self._session_context(new_session):
            ok, reason = new_session.client.connect(payload.sms_code)
            self._finalize_connect(new_session, ok, reason)
        self._persist_connected(new_session)
        return new_session

    def disconnect(self, user_id: str) -> str:
        session = self.require(user_id)
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
        logger.info("[SESSION-DISCONNECTED] %s", user_id)
        return user_id

    def reconnect(self, user_id: str) -> ManagedSession:
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
        if not session.email or not session.password:
            logger.warning("[SESSION-RECONNECT-FAILED] %s missing_credentials", user_id)
            self._mark_disconnected(user_id, reason)

        old_client = session.client
        new_session = ManagedSession(
            user_id=user_id,
            client=Bullex(session.email, session.password),
            email=session.email,
            password=session.password,
            sms_code=session.sms_code,
            desired_mode=session.desired_mode,
            requires_2fa=session.requires_2fa,
        )

        try:
            try:
                old_client.api.close()
            except Exception:
                pass
            with self._session_context(new_session):
                ok, connect_reason = new_session.client.connect(new_session.sms_code)
                self._finalize_connect(new_session, ok, connect_reason)
        except SESSION_EXCEPTION_TYPES as exc:
            logger.warning("[SESSION-RECONNECT-FAILED] %s %s", user_id, type(exc).__name__)
            self._mark_disconnected(user_id, type(exc).__name__)
        except Exception as exc:
            logger.warning("[SESSION-RECONNECT-FAILED] %s %s", user_id, exc)
            self._mark_disconnected(user_id, type(exc).__name__)

        self.upsert(new_session)
        self._persist_connected(new_session)
        logger.info("[SESSION-RECONNECT-OK] %s", user_id)
        return new_session

    def restore_sessions(self) -> None:
        if self.store is None:
            logger.warning("[SESSION_RESTORE] status=disabled reason=missing_encryption_key")
            return

        for persisted in self.store.load_connected():
            session = ManagedSession(
                user_id=persisted.user_id,
                client=Bullex(persisted.email, ""),
                email=persisted.email,
                desired_mode=normalize_mode(persisted.account_mode),
                state=SessionState(SSID=persisted.session_token),
            )
            restore_with_ssid = getattr(session.client, "restore_with_ssid", None)
            if not callable(restore_with_ssid):
                self.store.mark_disconnected(session.user_id)
                logger.warning(
                    "[SESSION_RESTORE] status=unsupported reason=no_ssid_restore_method user_id=%s",
                    session.user_id,
                )
                continue

            self.upsert(session)
            restore_failure_reason = "unknown"
            try:
                with self._session_context(session):
                    ok, reason = restore_with_ssid(persisted.session_token)
                    restore_failure_reason = str(reason or "restore_rejected")
                    self._finalize_connect(session, ok, reason)
                self._persist_connected(session)
                logger.info("[SESSION_RESTORE] user_id=%s status=success", session.user_id)
            except Exception as exc:
                self.remove(session.user_id)
                self.store.mark_disconnected(session.user_id)
                if restore_failure_reason == "invalid_ssid":
                    logger.warning(
                        "[SESSION_RESTORE] user_id=%s status=unsupported reason=broker_invalidates_ssid",
                        session.user_id,
                    )
                    continue
                logger.warning(
                    "[SESSION_RESTORE] user_id=%s status=failed reason=%s",
                    session.user_id,
                    restore_failure_reason if restore_failure_reason != "unknown" else type(exc).__name__,
                )

    def persistence_debug(self) -> dict[str, Any]:
        if self.store is None:
            return {
                "stored_sessions": 0,
                "users": [],
            }
        return self.store.persistence_debug()

    def _mark_disconnected(self, user_id: str, reason: str) -> None:
        logger.warning("[SESSION-DISCONNECTED] %s", user_id)
        self.remove(user_id)
        if self.store is not None:
            self.store.mark_disconnected(user_id)
        self._mark_probe_failure(user_id, offline=True)
        raise ServiceError(SESSION_DISCONNECTED, 409) from None

    def _persist_connected(self, session: ManagedSession) -> None:
        if self.store is None or session.requires_2fa:
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

    def _finalize_connect(self, session: ManagedSession, ok: bool, reason: Any) -> None:
        if ok:
            session.requires_2fa = False
            current_mode = session.client.get_balance_mode()
            if session.desired_mode != current_mode:
                session.client.change_balance(session.desired_mode)
                current_mode = session.client.get_balance_mode()
            if current_mode != session.desired_mode:
                raise ServiceError("nao foi possivel ativar o modo solicitado", 409)
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


def build_error(message: str) -> dict[str, Any]:
    return {"ok": False, "data": None, "error": message}


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


def build_account_payload(session: ManagedSession) -> dict[str, Any]:
    connected = bool(session.client.check_connect())
    account = {
        "connected": connected,
        "balance": None,
        "currency": None,
        "mode": None,
        "email": session.email,
        "requires_2fa": session.requires_2fa,
    }

    if connected and not session.requires_2fa:
        balance = normalize_balance_value(session.client.get_balance())
        mode = normalize_mode(session.client.get_balance_mode())
        currency = session.client.get_currency() or ("BRL" if mode == "REAL" else None)
        account["balance"] = balance
        account["currency"] = currency
        account["mode"] = mode
        if mode == "REAL":
            logger.info("[ACCOUNT_REAL_CONNECTED] user_id=%s email=%s", session.user_id, session.email)
            if balance == 0:
                account["real_balance_warning"] = "BALANCE_ZERO"
                logger.info("[BALANCE_ZERO_NOT_DISCONNECTED] user_id=%s mode=REAL", session.user_id)

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


app = FastAPI(title="bullex-service", version="0.1.0")
session_manager = SessionManager(create_session_store())


@app.on_event("startup")
def restore_persisted_sessions() -> None:
    persistence_debug_registered = any(
        getattr(route, "path", None) == "/sessions/persistence-debug"
        for route in app.routes
    )
    logger.info(
        "[SESSION_PERSISTENCE_ROUTE] path=/sessions/persistence-debug registered=%s",
        persistence_debug_registered,
    )
    session_manager.restore_sessions()


def ensure_session_alive(user_id: str) -> ManagedSession:
    return session_manager.ensure_session_alive(user_id)


@app.exception_handler(ServiceError)
def service_error_handler(_: Request, exc: ServiceError) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content=build_error(exc.message))


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
def connect_session(payload: ConnectRequest, x_user_id: str | None = Header(default=None)) -> dict[str, Any]:
    user_id = require_user_id(x_user_id)
    payload.account_mode = normalize_mode(payload.account_mode)
    session = session_manager.connect(user_id, payload)
    session_manager.clear_probe_cache(user_id)

    connected = False
    active_mode = None
    if not session.requires_2fa:
        with session_manager._session_context(session):
            active_mode = session.client.get_balance_mode()
        connected = True

    return build_success(
        {
            "user_id": user_id,
            "connected": connected,
            "requires_2fa": session.requires_2fa,
            "active_mode": active_mode,
        }
    )


@app.get("/sessions/status")
def session_status(x_user_id: str | None = Header(default=None)) -> JSONResponse:
    user_id = require_user_id(x_user_id)
    cached = session_manager.get_cached_probe(user_id, "/sessions/status", path="/sessions/status")
    if cached is not None:
        status_code, payload = cached
        return JSONResponse(status_code=status_code, content=payload)

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
        payload = build_success(session_manager.run(user_id, operation))
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
            return JSONResponse(status_code=status_code, content=payload)
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
    return build_success(session_manager.run(user_id, build_account_payload))


@app.get("/account/balance")
def account_balance(x_user_id: str | None = Header(default=None)) -> dict[str, Any]:
    user_id = require_user_id(x_user_id)

    def operation(session: ManagedSession) -> dict[str, Any]:
        ensure_session_ready(session)
        return {
            "balance": session.client.get_balance(),
            "currency": session.client.get_currency(),
            "mode": session.client.get_balance_mode(),
        }

    return build_success(session_manager.run(user_id, operation))


@app.get("/account")
def account_overview(x_user_id: str | None = Header(default=None)) -> dict[str, Any]:
    user_id = require_user_id(x_user_id)
    cached = session_manager.get_cached_probe(user_id, "/account", path="/account")
    if cached is not None:
        _, payload = cached
        return payload
    if session_manager.get(user_id) is None:
        payload = build_success({"connected": False})
        session_manager._mark_probe_failure(user_id, offline=True)
        return payload

    def operation(session: ManagedSession) -> dict[str, Any]:
        return build_account_payload(session)

    try:
        payload = build_success(session_manager.run(user_id, operation))
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
            session_manager._mark_probe_failure(user_id, offline=True)
            return build_success({"connected": False})
        raise


@app.post("/account/change-mode")
def account_change_mode(payload: ChangeModeRequest, x_user_id: str | None = Header(default=None)) -> dict[str, Any]:
    user_id = require_user_id(x_user_id)
    target_mode = normalize_mode(payload.mode)
    if target_mode == "REAL" and not payload.confirm_real:
        raise ServiceError("operacao REAL bloqueada sem confirm_real=true", 403)

    def operation(session: ManagedSession) -> dict[str, Any]:
        ensure_session_ready(session)
        session.client.change_balance(target_mode)
        active_mode = session.client.get_balance_mode()
        if active_mode != target_mode:
            raise ServiceError("falha ao trocar o modo da conta", 409)
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
        {"active": active, "interval": interval, "count": count, "endtime": resolved_endtime},
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

    payload = build_success(session_manager.run(user_id, operation))
    session_manager._mark_probe_success(
        user_id,
        cache_key,
        200,
        payload,
        ttl_seconds=CANDLES_TTL_SECONDS,
    )
    return payload


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

    payload = build_success(session_manager.run(user_id, operation))
    session_manager._mark_probe_success(
        user_id,
        cache_key,
        200,
        payload,
        ttl_seconds=PAYOUT_TTL_SECONDS,
    )
    return payload


@app.post("/orders/buy-demo")
def buy_demo(payload: BuyOrderRequest, x_user_id: str | None = Header(default=None)) -> dict[str, Any]:
    user_id = require_user_id(x_user_id)
    payload.action = normalize_action(payload.action)

    def operation(session: ManagedSession) -> dict[str, Any]:
        ensure_session_ready(session)
        ensure_mode_matches(session.client, "PRACTICE")
        ok, order_id = session.client.buy(payload.amount, payload.active, payload.action, payload.expiration)
        if not ok:
            raise ServiceError(f"falha ao criar ordem demo: {order_id}", 409)
        return {
            "mode": "PRACTICE",
            "order_id": order_id,
            "active": payload.active,
            "amount": payload.amount,
            "action": payload.action,
            "expiration": payload.expiration,
        }

    return build_success(session_manager.run(user_id, operation))


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
