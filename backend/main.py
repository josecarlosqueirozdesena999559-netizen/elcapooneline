import logging
import math
import os
import asyncio
import json
from copy import deepcopy
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from time import monotonic
from typing import Any

import httpx
from fastapi import Body, Depends, FastAPI, Header, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict

from backend.auto_trader import (
    AutoTrader,
    RobotConfigUpdate,
    parse_datetime,
    strip_ai_fields,
    utc_now,
)
from backend.status import (
    STATUS_ACCOUNT_DISCONNECTED,
    STATUS_ACTIVE_COOLDOWN,
    STATUS_ANALYZING,
    STATUS_BULLEX_ACTIVE_MODE_NOT_REAL,
    STATUS_BUYING,
    STATUS_CONNECTION_BACKOFF,
    STATUS_INSUFFICIENT_BALANCE,
    STATUS_ORDER_REJECTED,
    STATUS_PAYOUT_COOLDOWN,
    STATUS_PENDING_GALE_RESULT,
    STATUS_PENDING_RESULT,
    STATUS_SENDING_GALE_ORDER,
    STATUS_SENDING_ORDER,
    STATUS_SIGNAL_FOUND,
    STATUS_SIGNAL_EXPIRED,
    STATUS_STOP_LOSS_HIT,
    STATUS_STOP_WIN_HIT,
    STATUS_STOPPED,
    STATUS_WAITING_ANALYSIS_WINDOW,
    STATUS_WAITING_ENTRY,
    STATUS_WAITING_ENTRY_WINDOW,
    STATUS_WAITING_GALE_ENTRY,
    STATUS_WAITING_RESULT,
    STATUS_WAITING_NEXT_CYCLE,
    STATUS_WAITING_RECOVERY,
    TEMPORARY_WAIT_STATUSES,
    normalize_robot_status,
)
from backend.robot_persistence import (
    RobotPersistence,
    create_robot_persistence,
    extract_robot_settings,
)
from backend.signal_engine import analyze_signal
from backend.trade_result_monitor import TradeResultMonitor
from backend.user_store import InMemoryUserStore, UserStore, create_user_store


logger = logging.getLogger("backend-gateway")

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
ROBOT_CONFIG_DEFAULTS = {
    "account_mode": "REAL",
    "timeframe": "M1",
    "strategy_mode": "conservative",
    "entry_value": 2.0,
    "cycle_minutes": 5,
    "min_confidence": 80,
    "min_payout": 80.0,
    "stop_win": 50.0,
    "stop_loss": 30.0,
    "max_entries_per_cycle": 1,
    "allow_real": True,
    "confirm_real": True,
    "martingale_enabled": False,
    "martingale_steps": 1,
    "martingale_multiplier": 2.0,
}
ROBOT_CONFIG_ALLOWED_FIELDS = set(ROBOT_CONFIG_DEFAULTS)
MIN_REAL_ENTRY = 2.0
MAX_REAL_ENTRY = 1000.0

ASSET_NOT_ALLOWED = "ASSET_NOT_ALLOWED"
SESSION_NOT_FOUND = "SESSION_NOT_FOUND"
SESSION_DISCONNECTED = "SESSION_DISCONNECTED"
LOW_QUALITY_SIGNAL = "Sinal bloqueado por baixa qualidade"
MAX_ORDER_ATTEMPTS_PER_CYCLE = 3
NO_AVAILABLE_ASSET_ERROR = "Nenhum ativo disponível no momento da compra."
CRITICAL_TRADE_BLOCKS = {
    STATUS_ACCOUNT_DISCONNECTED,
    "STOP_WIN_HIT",
    "STOP_LOSS_HIT",
    "ACTIVE_CLOSED",
    "OPERATION_IN_PROGRESS",
    "CANDLES_UNAVAILABLE",
}
ANALYSIS_DETAIL_FIELDS = (
    "ema9",
    "ema21",
    "rsi",
    "rsi14",
    "atr",
    "atr_pct",
    "body_ratio",
    "candle_body",
    "upper_wick",
    "lower_wick",
    "upper_wick_ratio",
    "lower_wick_ratio",
    "directional_candles_5",
    "alternating_last_3",
    "candle_reading",
    "entry_reason",
    "block_reasons",
    "metrics",
)
ORDER_AVAILABILITY_ERROR_TERMS = (
    "asset is not available",
    "active suspended",
    "cannot purchase",
    "active not found",
)
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
ANALYSIS_ASSETS = [
    "EURUSD-OTC",
    "GBPUSD-OTC",
    "USDJPY-OTC",
    "EURJPY-OTC",
    "AUDUSD-OTC",
    "EURGBP-OTC",
    "USDCHF-OTC",
    "USDCAD-OTC",
    "GBPJPY-OTC",
    "AUDJPY-OTC",
]
ROBOT_MAX_ASSETS_PER_CYCLE = len(ANALYSIS_ASSETS)
CHART_ALLOWED_ASSET_SET = set(ANALYSIS_ASSETS)
SESSION_CACHE_TTL_SECONDS = 10
SESSION_STATUS_THROTTLE_SECONDS = 10
ROBOT_SESSION_REFRESH_SECONDS = 15
SESSION_OFFLINE_TTL_SECONDS = 60
SESSION_FAILURE_BACKOFF_SECONDS = (10, 30, 60, 300)
SESSION_CACHEABLE_PATHS = {"/sessions/status", "/account"}
ACTIVE_USER_TTL_SECONDS = 300
ACCOUNT_CACHE_TTL_SECONDS = 15
ORDER_RESULT_CACHE_TTL_SECONDS = 1
BULLEX_UPSTREAM_TIMEOUT_SECONDS = 5.0
BULLEX_MARKET_DATA_TIMEOUT_SECONDS = 2.0
BULLEX_CONNECT_TIMEOUT_SECONDS = 60.0
BULLEX_TEMPORARY_UNAVAILABLE = "BULLEX_TEMPORARY_UNAVAILABLE"
BAD_GATEWAY_PROTECTED_PATHS = {
    "/bullex/account",
    "/bullex/status",
    "/bullex/connect",
    "/robot/state",
    "/robot/start",
    "/robot/stop",
}
ASSETS_CACHE_TTL_SECONDS = 300
ASSETS_RETRY_BACKOFF_SECONDS = (10, 30, 60)
PAYOUT_CACHE_TTL_SECONDS = 60
CANDLES_CACHE_TTL_SECONDS = 60
CANDLES_REQUEST_TIMEOUT_SECONDS = 5.0
ACTIVE_COOLDOWN_SECONDS = 15
PAYOUT_COOLDOWN_SECONDS = 15
STALE_MARKET_DATA_SECONDS = 120
ROBOT_ASSET_QUEUE_SLEEP_SECONDS = 0.0
ROBOT_CANDLE_COUNT = 100
ACTIVE_DATA_TIMEOUT_SECONDS = 2.0


def build_success(data: Any) -> dict[str, Any]:
    return {"ok": True, "data": data, "error": None}


def build_error(message: str) -> dict[str, Any]:
    return {"ok": False, "data": None, "error": message}


INSUFFICIENT_BALANCE_START_MESSAGE = "Você está sem saldo para iniciar. Faça um depósito na BullEx."
ENTRY_VALUE_EXCEEDS_BALANCE_MESSAGE = "Seu saldo é menor que o valor da entrada."


def normalize_service_payload(
    payload: Any,
    *,
    error: str = BULLEX_TEMPORARY_UNAVAILABLE,
) -> dict[str, Any]:
    if isinstance(payload, dict):
        return payload
    logger.warning(
        "[INVALID_UPSTREAM_PAYLOAD_HANDLED] payload_type=%s",
        type(payload).__name__,
    )
    return build_error(error)


def build_controlled_upstream_error(detail: Any) -> dict[str, Any]:
    return {
        "ok": False,
        "data": None,
        "error": BULLEX_TEMPORARY_UNAVAILABLE,
        "detail": str(detail or BULLEX_TEMPORARY_UNAVAILABLE)[:240],
    }


CONFIG_LOCK_ERROR = {
    "ok": False,
    "error": "ROBOT_RUNNING_CONFIG_LOCKED",
    "message": "Pare o robô antes de alterar configurações.",
}


ROBOT_BASIC_CONFIG_FIELDS = {
    "stop_win",
    "stop_loss",
    "entry_value",
    "account_mode",
    "allow_real",
    "confirm_real",
    "timeframe",
    "min_confidence",
    "min_payout",
    "martingale_enabled",
    "martingale_steps",
    "martingale_multiplier",
}
ROBOT_BASIC_CONFIG_ALIASES = {
    "stopWin",
    "stopLoss",
    "entryValue",
    "accountMode",
    "allowReal",
    "confirmReal",
    "timeframe",
    "minConfidence",
    "minPayout",
    "martingaleEnabled",
    "martingaleSteps",
    "martingaleMultiplier",
}


def ignored_ai_config_fields(payload: dict[str, Any]) -> list[str]:
    ignored: list[str] = []
    for key in payload:
        normalized = str(key or "").strip().lower()
        if normalized.startswith(("ai", "use_ai", "openai", "gemini")):
            ignored.append(str(key))
    return ignored


def strip_ai_config_fields(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in payload.items()
        if not str(key or "").strip().lower().startswith(("ai", "use_ai", "openai", "gemini"))
    }


def filter_robot_config_payload(payload: dict[str, Any]) -> dict[str, Any]:
    filtered = strip_ai_config_fields(payload)
    return {
        key: value
        for key, value in filtered.items()
        if key in ROBOT_BASIC_CONFIG_FIELDS or key in ROBOT_BASIC_CONFIG_ALIASES
    }


def current_robot_config_payload(state: Any) -> dict[str, Any]:
    return {
        "account_mode": str(getattr(state, "account_mode", ROBOT_CONFIG_DEFAULTS["account_mode"]) or ROBOT_CONFIG_DEFAULTS["account_mode"]),
        "timeframe": str(getattr(state, "timeframe", ROBOT_CONFIG_DEFAULTS["timeframe"]) or ROBOT_CONFIG_DEFAULTS["timeframe"]),
        "strategy_mode": str(getattr(state, "strategy_mode", ROBOT_CONFIG_DEFAULTS["strategy_mode"]) or ROBOT_CONFIG_DEFAULTS["strategy_mode"]),
        "entry_value": float(getattr(state, "entry_value", ROBOT_CONFIG_DEFAULTS["entry_value"]) or ROBOT_CONFIG_DEFAULTS["entry_value"]),
        "cycle_minutes": int(getattr(state, "cycle_minutes", ROBOT_CONFIG_DEFAULTS["cycle_minutes"]) or ROBOT_CONFIG_DEFAULTS["cycle_minutes"]),
        "min_confidence": int(getattr(state, "min_confidence", ROBOT_CONFIG_DEFAULTS["min_confidence"]) or ROBOT_CONFIG_DEFAULTS["min_confidence"]),
        "min_payout": float(getattr(state, "min_payout", ROBOT_CONFIG_DEFAULTS["min_payout"]) or ROBOT_CONFIG_DEFAULTS["min_payout"]),
        "stop_win": float(getattr(state, "stop_win", ROBOT_CONFIG_DEFAULTS["stop_win"]) or ROBOT_CONFIG_DEFAULTS["stop_win"]),
        "stop_loss": float(getattr(state, "stop_loss", ROBOT_CONFIG_DEFAULTS["stop_loss"]) or ROBOT_CONFIG_DEFAULTS["stop_loss"]),
        "max_entries_per_cycle": int(getattr(state, "max_entries_per_cycle", ROBOT_CONFIG_DEFAULTS["max_entries_per_cycle"]) or ROBOT_CONFIG_DEFAULTS["max_entries_per_cycle"]),
        "allow_real": bool(getattr(state, "allow_real", ROBOT_CONFIG_DEFAULTS["allow_real"])),
        "confirm_real": bool(getattr(state, "confirm_real", ROBOT_CONFIG_DEFAULTS["confirm_real"])),
        "martingale_enabled": bool(getattr(state, "martingale_enabled", ROBOT_CONFIG_DEFAULTS["martingale_enabled"])),
        "martingale_steps": int(getattr(state, "martingale_steps", ROBOT_CONFIG_DEFAULTS["martingale_steps"]) or ROBOT_CONFIG_DEFAULTS["martingale_steps"]),
        "martingale_multiplier": float(getattr(state, "martingale_multiplier", ROBOT_CONFIG_DEFAULTS["martingale_multiplier"]) or ROBOT_CONFIG_DEFAULTS["martingale_multiplier"]),
    }


def build_local_signal_review(signal: dict[str, Any]) -> dict[str, Any]:
    blocked_filters = [str(item) for item in (signal.get("blocked_filters") or [])]
    confidence = int(signal.get("confidence") or 0)
    payout = float(signal.get("payout") or 0)
    approved = bool(signal.get("trade_allowed")) and str(signal.get("signal") or "").upper() in {"CALL", "PUT"}
    if not approved:
        risk = "HIGH"
    elif confidence >= 90 and payout >= 85:
        risk = "LOW"
    elif confidence >= 75 and payout >= 80:
        risk = "MEDIUM"
    else:
        risk = "HIGH"
    recommendation = "VALID_SIGNAL" if approved else "REJECT_SIGNAL"
    summary = str(signal.get("reason") or signal.get("entry_reason") or "Revisao local concluida.").strip()
    return {
        "approved": approved,
        "risk": risk,
        "quality": max(0, min(100, confidence)),
        "summary": summary,
        "warnings": blocked_filters,
        "recommendation": recommendation,
        "source": "local",
    }


def normalize_binary_active(active: str) -> str:
    return (active or "").strip().upper()


def is_binary_asset_allowed(active: str) -> bool:
    return normalize_binary_active(active) in BINARY_ALLOWED_ASSET_SET


class ConnectionManager:
    def __init__(self) -> None:
        self._connections: dict[str, tuple[str, WebSocket]] = {}
        self._lock = asyncio.Lock()

    async def connect(self, user_id: str, active: str, websocket: WebSocket) -> None:
        await websocket.accept()
        previous: WebSocket | None = None
        async with self._lock:
            existing = self._connections.get(user_id)
            if existing is not None and existing[1] is not websocket:
                previous = existing[1]
            self._connections[user_id] = (active, websocket)
        if previous is not None:
            with suppress(Exception):
                await previous.close(code=1000)

    async def disconnect(self, user_id: str, active: str, websocket: WebSocket) -> None:
        async with self._lock:
            existing = self._connections.get(user_id)
            if existing is not None and existing == (active, websocket):
                self._connections.pop(user_id, None)

    async def disconnect_user(self, user_id: str) -> None:
        async with self._lock:
            existing = self._connections.pop(user_id, None)
        if existing is not None:
            with suppress(Exception):
                await existing[1].close(code=1000)

    async def broadcast_to_user_active(self, user_id: str, active: str, payload: dict[str, Any]) -> None:
        async with self._lock:
            existing = self._connections.get(user_id)
            targets = [existing[1]] if existing is not None and existing[0] == active else []
        for websocket in targets:
            try:
                await websocket.send_json(payload)
            except Exception:
                logger.exception("falha ao enviar payload WS para %s %s", user_id, active)
                await self.disconnect(user_id, active, websocket)


manager = ConnectionManager()


@dataclass
class BullexResponseCacheEntry:
    status_code: int
    payload: dict[str, Any]
    expires_at: datetime


@dataclass
class BullexUserSessionCache:
    responses: dict[str, BullexResponseCacheEntry] = field(default_factory=dict)
    last_successful_responses: dict[str, BullexResponseCacheEntry] = field(default_factory=dict)
    failure_count: int = 0
    next_retry_at: datetime | None = None
    offline_until: datetime | None = None
    last_request_at: dict[str, datetime] = field(default_factory=dict)
    assets_failure_count: int = 0
    assets_next_retry_at: datetime | None = None


session_response_cache: dict[str, BullexUserSessionCache] = {}
active_cooldowns: dict[str, dict[str, datetime]] = {}
payout_cooldowns: dict[str, dict[str, datetime]] = {}
active_users: dict[str, datetime] = {}
analysis_asset_queue_offsets: dict[str, int] = {}
background_refresh_tasks: set[tuple[str, str, str]] = set()


def get_session_cache(user_id: str) -> BullexUserSessionCache:
    return session_response_cache.setdefault(user_id, BullexUserSessionCache())


def schedule_background_refresh(
    user_id: str,
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
) -> None:
    if method != "GET" or path not in {"/candles", "/payouts"}:
        return
    cache_key = build_cache_key(path, params)
    task_key = (user_id, path, cache_key)
    if task_key in background_refresh_tasks:
        return
    background_refresh_tasks.add(task_key)

    async def refresh() -> None:
        try:
            await call_bullex_service(
                method,
                path,
                user_id,
                params=params,
                allow_failure_backoff=False,
                force_refresh=True,
            )
        except Exception as exc:
            logger.info(
                "[BACKGROUND_REFRESH_SKIPPED] user_id=%s path=%s reason=%s",
                user_id,
                path,
                exc.__class__.__name__,
            )
        finally:
            background_refresh_tasks.discard(task_key)

    try:
        asyncio.create_task(refresh())
    except RuntimeError:
        background_refresh_tasks.discard(task_key)


def session_backoff_seconds(failure_count: int) -> int:
    if failure_count <= 0:
        return 0
    return SESSION_OFFLINE_TTL_SECONDS


def payload_connected_state(payload: dict[str, Any]) -> bool | None:
    payload = normalize_service_payload(payload)
    data = payload.get("data")
    if not isinstance(data, dict) or "connected" not in data:
        return None
    return bool(data.get("connected"))


def disconnected_cache_payload(*, source: str) -> dict[str, Any]:
    return build_success({"connected": False, "connection_status_source": source})


def cache_bullex_response(user_id: str, path: str, status_code: int, payload: dict[str, Any]) -> None:
    cache = get_session_cache(user_id)
    ttl_seconds = request_cache_ttl_seconds(path) or SESSION_CACHE_TTL_SECONDS
    entry = BullexResponseCacheEntry(
        status_code=status_code,
        payload=deepcopy(payload),
        expires_at=utc_now() + timedelta(seconds=ttl_seconds),
    )
    cache.responses[path] = entry
    if payload.get("ok") and payload_connected_state(payload) is not False:
        cache.last_successful_responses[path] = deepcopy(entry)


def cached_successful_response(
    user_id: str,
    cache_key: str,
) -> BullexResponseCacheEntry | None:
    cache = get_session_cache(user_id)
    cached = cache.last_successful_responses.get(cache_key)
    if cached is not None:
        return cached
    current = cache.responses.get(cache_key)
    if (
        current is not None
        and current.payload.get("ok")
        and (
            cache_key not in SESSION_CACHEABLE_PATHS
            or payload_connected_state(current.payload) is not False
        )
    ):
        return current
    return None


def add_stale_warning(payload: dict[str, Any]) -> dict[str, Any]:
    fallback = deepcopy(payload)
    fallback["warning"] = BULLEX_TEMPORARY_UNAVAILABLE
    data = fallback.get("data")
    if isinstance(data, dict):
        data["from_cache"] = True
    meta = fallback.get("meta")
    fallback["meta"] = {
        **(meta if isinstance(meta, dict) else {}),
        "source": "cache",
        "stale": True,
    }
    return fallback


def temporary_upstream_response(
    user_id: str,
    path: str,
    cache_key: str,
    *,
    reason: str,
    allow_failure_backoff: bool = True,
) -> tuple[int, dict[str, Any]]:
    recent_account_payload = recent_real_account_connection_payload(user_id) if path == "/sessions/status" else None
    if recent_account_payload is not None:
        logger.warning(
            "[SESSION_STATUS_GRACE_FROM_ACCOUNT] user_id=%s reason=%s",
            user_id,
            reason,
        )
        return 200, recent_account_payload
    if not allow_failure_backoff:
        logger.info(
            "[BACKOFF_SKIPPED_RESTORE] user_id=%s path=%s",
            user_id,
            path,
        )
        cached = cached_successful_response(user_id, cache_key)
        if cached is not None:
            return 200, add_stale_warning(cached.payload)
        return 503, build_error(BULLEX_TEMPORARY_UNAVAILABLE)
    if not is_user_active(user_id):
        logger.info(
            "[BACKOFF_SKIPPED_OFFLINE_USER] user_id=%s path=%s reason=%s",
            user_id,
            path,
            reason,
        )
        cached = cached_successful_response(user_id, cache_key)
        if cached is not None:
            return 200, add_stale_warning(cached.payload)
        return 200, disconnected_cache_payload(source="offline_user")
    cache = get_session_cache(user_id)
    cache.failure_count += 1
    cache.next_retry_at = utc_now() + timedelta(seconds=SESSION_STATUS_THROTTLE_SECONDS)
    logger.warning(
        "[UPSTREAM_ERROR_HANDLED] user_id=%s path=%s reason=%s",
        user_id,
        path,
        reason,
    )
    cached = cached_successful_response(user_id, cache_key)
    if cached is not None:
        if path == "/account":
            logger.warning(
                "[ACCOUNT_FETCH_FALLBACK] user_id=%s source=last_valid_cache reason=%s",
                user_id,
                reason,
            )
            logger.warning(
                "[ACCOUNT_CACHE_RETURNED] user_id=%s source=last_valid_cache",
                user_id,
            )
        return 200, add_stale_warning(cached.payload)
    return 503, build_error(BULLEX_TEMPORARY_UNAVAILABLE)


def clear_session_backoff(user_id: str) -> None:
    cache = get_session_cache(user_id)
    cache.failure_count = 0
    cache.next_retry_at = None
    cache.offline_until = None
    state = auto_trader.get(user_id)
    if state.status in {STATUS_CONNECTION_BACKOFF, STATUS_WAITING_RECOVERY} and state.enabled:
        logger.info("[RECOVERY_SUCCESS] user_id=%s", user_id)
        auto_trader.defer_cycle(user_id, STATUS_WAITING_NEXT_CYCLE, wait_seconds=1, rejection_reason=None)


def mark_session_failure(user_id: str, *, offline: bool = False) -> None:
    if not is_user_active(user_id):
        logger.info(
            "[BACKOFF_SKIPPED_OFFLINE_USER] user_id=%s offline=%s",
            user_id,
            offline,
        )
        return
    cache = get_session_cache(user_id)
    cache.failure_count += 1
    now = utc_now()
    auto_trader.defer_cycle(
        user_id,
        STATUS_WAITING_RECOVERY if offline else STATUS_CONNECTION_BACKOFF,
        wait_seconds=SESSION_OFFLINE_TTL_SECONDS,
        rejection_reason="WAITING_RECOVERY" if offline else "CONNECTION_BACKOFF",
        last_rejection_reason="WAITING_RECOVERY" if offline else "CONNECTION_BACKOFF",
    )
    logger.warning(
        "[%s] user_id=%s retry_at=%s failures=%s",
        "WAITING_RECOVERY" if offline else "USER_BACKOFF_ACTIVE",
        user_id,
        (now + timedelta(seconds=SESSION_OFFLINE_TTL_SECONDS)).isoformat(),
        cache.failure_count,
    )
    if offline:
        cache.offline_until = now + timedelta(seconds=SESSION_OFFLINE_TTL_SECONDS)
        cache.next_retry_at = cache.offline_until
        for path in SESSION_CACHEABLE_PATHS:
            cache.last_successful_responses.pop(path, None)
            cache_bullex_response(
                user_id,
                path,
                200 if path == "/account" else 404,
                disconnected_cache_payload(source="offline_cache"),
            )
        return

    cache.next_retry_at = now + timedelta(seconds=session_backoff_seconds(cache.failure_count))


def connection_guard_reason(user_id: str) -> tuple[str, float] | None:
    cache = get_session_cache(user_id)
    now = utc_now()
    if cache.offline_until is not None and now < cache.offline_until:
        return "offline", (cache.offline_until - now).total_seconds()
    if cache.next_retry_at is not None and now < cache.next_retry_at:
        return "backoff", (cache.next_retry_at - now).total_seconds()
    return None


def request_cache_ttl_seconds(path: str, params: dict[str, Any] | None = None) -> int | None:
    if is_order_result_path(path):
        return ORDER_RESULT_CACHE_TTL_SECONDS
    if path == "/sessions/status":
        return SESSION_CACHE_TTL_SECONDS
    if path == "/account":
        return ACCOUNT_CACHE_TTL_SECONDS
    if path == "/assets":
        return ASSETS_CACHE_TTL_SECONDS
    if path == "/payouts":
        return PAYOUT_CACHE_TTL_SECONDS
    if path == "/candles":
        return CANDLES_CACHE_TTL_SECONDS
    return None


def metric_label_for_path(path: str) -> str | None:
    if path == "/candles":
        return "CANDLES_FETCH_MS"
    if path == "/payouts":
        return "PAYOUT_FETCH_MS"
    if path == "/account":
        return "ACCOUNT_FETCH_MS"
    return None


def log_fetch_metric(path: str, started_at: float, *, user_id: str, source: str, status_code: int | None = None) -> None:
    label = metric_label_for_path(path)
    if label is None:
        return
    elapsed_ms = int((monotonic() - started_at) * 1000)
    logger.info(
        "[%s] user_id=%s path=%s source=%s status_code=%s ms=%s",
        label,
        user_id,
        path,
        source,
        status_code,
        elapsed_ms,
    )


def stale_successful_response(
    user_id: str,
    cache_key: str,
    *,
    max_age_seconds: int = STALE_MARKET_DATA_SECONDS,
) -> BullexResponseCacheEntry | None:
    cached = cached_successful_response(user_id, cache_key)
    if cached is None:
        return None
    stale_until = cached.expires_at + timedelta(seconds=max_age_seconds)
    return cached if utc_now() <= stale_until else None


def cached_market_response(
    user_id: str,
    path: str,
    params: dict[str, Any],
    *,
    max_age_seconds: int = STALE_MARKET_DATA_SECONDS,
) -> BullexResponseCacheEntry | None:
    exact = stale_successful_response(
        user_id,
        build_cache_key(path, params),
        max_age_seconds=max_age_seconds,
    )
    if exact is not None:
        return exact

    symbol = normalize_binary_active(str(params.get("active") or ""))
    if not symbol:
        return None
    cache = get_session_cache(user_id)
    candidates: list[BullexResponseCacheEntry] = []
    for cache_key, entry in cache.last_successful_responses.items():
        if not cache_key.startswith(f"{path}?") or utc_now() > entry.expires_at + timedelta(seconds=max_age_seconds):
            continue
        if f"active={symbol}" not in cache_key:
            continue
        if path == "/candles":
            interval = str(params.get("interval") or "")
            count = str(params.get("count") or "")
            if interval and f"interval={interval}" not in cache_key:
                continue
            if count and f"count={count}" not in cache_key:
                continue
        candidates.append(entry)
    if not candidates:
        return None
    return max(candidates, key=lambda item: item.expires_at)


def cached_candles_for_active(
    user_id: str,
    symbol: str,
    timeframe: str,
    *,
    endtime: int | None = None,
) -> list[dict[str, Any]]:
    params: dict[str, Any] = {
        "active": normalize_binary_active(symbol),
        "interval": TIMEFRAME_SECONDS[timeframe],
        "count": ROBOT_CANDLE_COUNT,
    }
    if endtime is not None:
        params["endtime"] = endtime
    cached = cached_market_response(user_id, "/candles", params)
    return extract_candles(cached.payload) if cached is not None else []


def cached_payout_for_active(user_id: str, symbol: str) -> float | None:
    normalized_symbol = normalize_binary_active(symbol)
    cached = cached_market_response(
        user_id,
        "/payouts",
        {"active": normalized_symbol},
    )
    return extract_payout(cached.payload, normalized_symbol) if cached is not None else None


def select_analysis_assets_for_cycle(
    user_id: str,
    *,
    max_assets: int | None,
) -> list[str]:
    assets = [symbol for symbol in BINARY_ALLOWED_ASSETS if symbol in ANALYSIS_ASSETS]
    if max_assets is None or max_assets <= 0 or max_assets >= len(assets):
        return assets
    offset = analysis_asset_queue_offsets.get(user_id, 0) % len(assets)
    selected = [assets[(offset + index) % len(assets)] for index in range(max_assets)]
    analysis_asset_queue_offsets[user_id] = (offset + max_assets) % len(assets)
    logger.info(
        "[ANALYSIS_ASSET_QUEUE] user_id=%s offset=%s limit=%s assets=%s",
        user_id,
        offset,
        max_assets,
        ",".join(selected),
    )
    return selected


def invalidate_account_cache(user_id: str) -> None:
    cache = get_session_cache(user_id)
    cache.responses.pop("/account", None)
    cache.last_successful_responses.pop("/account", None)
    logger.info("[ACCOUNT_CACHE_INVALIDATED] user_id=%s reason=ORDER_SENT", user_id)


def seconds_until_next_candle_after_trade(state: Any) -> float | None:
    last_trade = getattr(state, "last_trade", None)
    if not isinstance(last_trade, dict):
        return None
    if str(last_trade.get("result") or "").strip().upper() not in {"WIN", "LOSS", "TIMEOUT"}:
        return None
    finished_at = parse_datetime(last_trade.get("finished_at"))
    if finished_at is None:
        return None
    timeframe = str(getattr(state, "timeframe", "M1") or "M1").strip().upper()
    interval = TIMEFRAME_SECONDS.get(timeframe, 60)
    now = utc_now()
    seconds_since_finish = (now - finished_at).total_seconds()
    if seconds_since_finish < 0:
        return 1.0
    seconds_into_candle = now.timestamp() % interval
    remaining = interval - seconds_into_candle
    if remaining <= 0:
        remaining = interval
    if seconds_since_finish >= remaining + 0.5:
        return None
    return max(0.5, remaining)


def normalize_allowed_assets_list(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, list):
        return []
    return [
        asset
        for asset in payload
        if isinstance(asset, dict) and is_binary_asset_allowed(str(asset.get("symbol") or ""))
    ]


def build_assets_payload(
    assets: list[dict[str, Any]],
    *,
    source: str,
    stale: bool,
) -> dict[str, Any]:
    return {
        "ok": True,
        "data": assets,
        "error": None,
        "meta": {
            "source": source,
            "syncing": stale,
            "stale": stale,
        },
    }


def get_cached_response_entry(user_id: str, cache_key: str) -> BullexResponseCacheEntry | None:
    return get_session_cache(user_id).responses.get(cache_key)


def get_assets_cache_entry(user_id: str) -> BullexResponseCacheEntry | None:
    return get_cached_response_entry(user_id, "/assets")


def get_cached_assets_payload(user_id: str) -> dict[str, Any] | None:
    cached = get_assets_cache_entry(user_id)
    if cached is None:
        return None
    assets = normalize_allowed_assets_list(cached.payload.get("data"))
    if not assets:
        return None
    return build_assets_payload(assets, source="cache", stale=True)


def get_snapshot_assets_payload(user_id: str) -> dict[str, Any] | None:
    try:
        assets = normalize_allowed_assets_list(user_store.get_market_assets_snapshot(user_id))
    except Exception:
        logger.exception("falha ao carregar snapshot de market_assets para %s", user_id)
        return None
    if not assets:
        return None
    return build_assets_payload(assets, source="snapshot", stale=True)


def clear_assets_backoff(user_id: str) -> None:
    cache = get_session_cache(user_id)
    cache.assets_failure_count = 0
    cache.assets_next_retry_at = None


def schedule_assets_retry(user_id: str) -> int:
    cache = get_session_cache(user_id)
    cache.assets_failure_count += 1
    index = min(cache.assets_failure_count - 1, len(ASSETS_RETRY_BACKOFF_SECONDS) - 1)
    retry_seconds = ASSETS_RETRY_BACKOFF_SECONDS[index]
    cache.assets_next_retry_at = utc_now() + timedelta(seconds=retry_seconds)
    return retry_seconds


def assets_retry_remaining(user_id: str) -> float | None:
    retry_at = get_session_cache(user_id).assets_next_retry_at
    if retry_at is None:
        return None
    remaining = (retry_at - utc_now()).total_seconds()
    return remaining if remaining > 0 else None


def account_still_connected(user_id: str) -> bool:
    if bool(getattr(auto_trader.get(user_id), "connected", False)):
        return True
    return get_user_account_snapshot(user_id).get("connected") is True


def log_ignored_disconnect(user_id: str, path: str, payload: dict[str, Any]) -> None:
    payload = normalize_service_payload(payload)
    if not is_session_disconnected(payload):
        return
    logger.warning(
        "[DISCONNECT_IGNORED_NON_SESSION_ERROR] user_id=%s path=%s error=%s",
        user_id,
        path,
        payload.get("error"),
    )


def build_cache_key(path: str, params: dict[str, Any] | None = None) -> str:
    if not params:
        return path
    normalized_params = dict(params)
    if path == "/candles":
        try:
            interval = int(normalized_params.get("interval") or TIMEFRAME_SECONDS["M1"])
        except (TypeError, ValueError):
            interval = TIMEFRAME_SECONDS["M1"]
        try:
            endtime = int(normalized_params.get("endtime") or utc_now().timestamp())
        except (TypeError, ValueError):
            endtime = int(utc_now().timestamp())
        normalized_params["endtime"] = int(endtime // interval) * interval
    parts = [f"{key}={normalized_params[key]}" for key in sorted(normalized_params)]
    return f"{path}?{'&'.join(parts)}"


def should_throttle_session_status(user_id: str, cache_key: str) -> bool:
    cache = get_session_cache(user_id)
    now = utc_now()
    last_request_at = cache.last_request_at.get(cache_key)
    cache.last_request_at[cache_key] = now
    if last_request_at is None:
        return False
    return 0 <= (now - last_request_at).total_seconds() < SESSION_STATUS_THROTTLE_SECONDS


def cached_session_status_response(user_id: str, cache_key: str) -> tuple[int, dict[str, Any]] | None:
    cached = get_session_cache(user_id).responses.get(cache_key)
    if cached is None:
        return None
    logger.info("[SESSION_STATUS_THROTTLED] user_id=%s path=/sessions/status", user_id)
    return cached.status_code, cached.payload


def get_named_cooldown(
    store: dict[str, dict[str, datetime]],
    user_id: str,
    symbol: str,
) -> float | None:
    user_cooldowns = store.get(user_id, {})
    expires_at = user_cooldowns.get(symbol)
    if expires_at is None:
        return None
    remaining = (expires_at - utc_now()).total_seconds()
    if remaining <= 0:
        user_cooldowns.pop(symbol, None)
        if not user_cooldowns:
            store.pop(user_id, None)
        return None
    return remaining


def set_named_cooldown(
    store: dict[str, dict[str, datetime]],
    user_id: str,
    symbol: str,
    *,
    seconds: int,
    log_label: str,
    status: str,
    reason: str,
) -> None:
    normalized = normalize_binary_active(symbol)
    if not normalized:
        return
    expires_at = utc_now() + timedelta(seconds=seconds)
    store.setdefault(user_id, {})[normalized] = expires_at
    logger.warning("[%s] user_id=%s symbol=%s until=%s", log_label, user_id, normalized, expires_at.isoformat())


def active_cooldown_remaining(user_id: str, symbol: str) -> float | None:
    return get_named_cooldown(active_cooldowns, user_id, normalize_binary_active(symbol))


def payout_cooldown_remaining(user_id: str, symbol: str) -> float | None:
    return get_named_cooldown(payout_cooldowns, user_id, normalize_binary_active(symbol))


class GatewayConfig:
    def __init__(self) -> None:
        self.bullex_service_url = os.getenv("BULLEX_SERVICE_URL", "http://bullex-service:8000").rstrip("/")
        self.panel_api_key = os.getenv("PANEL_API_KEY", "")
        self.supabase_url = os.getenv("SUPABASE_URL", "").strip()
        self.supabase_service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
        self.robot_real_max_entry = float(os.getenv("ROBOT_REAL_MAX_ENTRY", str(MAX_REAL_ENTRY)))
        self.cors_origins = [
            origin.strip()
            for origin in os.getenv(
                "CORS_ORIGINS",
                CORS_ALLOWED_ORIGINS_DEFAULT,
            ).split(",")
            if origin.strip()
        ]


config = GatewayConfig()
app = FastAPI(title="backend-gateway", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.cors_origins,
    allow_credentials=True,
    allow_methods=CORS_ALLOWED_METHODS,
    allow_headers=CORS_ALLOWED_HEADERS,
)


@app.middleware("http")
async def log_cors_allowed_origin(request: Request, call_next):
    origin = str(request.headers.get("origin") or "").strip()
    if origin and origin in config.cors_origins:
        logger.info(
            "[CORS_ALLOWED_ORIGIN] origin=%s method=%s path=%s",
            origin,
            request.method,
            request.url.path,
        )
    try:
        return await call_next(request)
    except Exception as exc:
        if request.url.path not in BAD_GATEWAY_PROTECTED_PATHS:
            raise
        logger.warning(
            "[UPSTREAM_ERROR_HANDLED] path=%s reason=%s",
            request.url.path,
            exc.__class__.__name__,
            exc_info=True,
        )
        response = JSONResponse(
            status_code=200,
            content=build_controlled_upstream_error(exc),
        )
        if origin in config.cors_origins:
            response.headers["Access-Control-Allow-Origin"] = origin
            response.headers["Access-Control-Allow-Credentials"] = "true"
            response.headers["Vary"] = "Origin"
        return response


user_store: UserStore = create_user_store()
auto_trader = AutoTrader()
robot_persistence: RobotPersistence = create_robot_persistence()
robot_tasks: dict[str, asyncio.Task[None]] = {}
robot_worker_last_tick_at: dict[str, datetime] = {}
robot_worker_restart_attempted: set[str] = set()
restorable_robot_states: dict[str, dict[str, Any]] = {}
robot_state_hydrated_users: set[str] = set()
chart_candles_cache: dict[tuple[str, str, str], dict[str, Any]] = {}
CONNECTION_GRACE_SECONDS = 30
ROBOT_VALID_CACHE_SECONDS = 300


def mark_user_active(user_id: str) -> None:
    active_users[user_id] = utc_now()


def is_user_active(user_id: str) -> bool:
    last_seen = active_users.get(user_id)
    if last_seen is None:
        state = auto_trader.get(user_id)
        return bool(state.enabled and user_id in robot_tasks)
    if (utc_now() - last_seen).total_seconds() < ACTIVE_USER_TTL_SECONDS:
        return True
    active_users.pop(user_id, None)
    return False


def inactive_user_payload(user_id: str, path: str) -> dict[str, Any]:
    cached = cached_successful_response(user_id, path)
    if cached is not None:
        return add_stale_warning(cached.payload)
    return disconnected_cache_payload(source="offline_user")


def backoff_payload(user_id: str, remaining: float) -> dict[str, Any]:
    return build_success(
        {
            "connected": False,
            "status": "backoff",
            "retry_in": max(0, int(math.ceil(remaining))),
        }
    )


def normalize_ws_value(value: Any) -> str:
    return str(value or "").strip()


def build_market_ws_payload(user_id: str, active: str, candle: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "candle",
        "user_id": user_id,
        "active": active,
        "time": candle.get("from") or candle.get("time"),
        "open": candle.get("open"),
        "high": candle.get("max") if "max" in candle else candle.get("high"),
        "low": candle.get("min") if "min" in candle else candle.get("low"),
        "close": candle.get("close"),
        "volume": candle.get("volume", 0),
    }


def extract_latest_candle(payload: dict[str, Any]) -> dict[str, Any] | None:
    payload = normalize_service_payload(payload)
    data = payload.get("data")
    candles: list[dict[str, Any]] = []
    if isinstance(data, list):
        candles = [item for item in data if isinstance(item, dict)]
    elif isinstance(data, dict) and isinstance(data.get("candles"), list):
        candles = [item for item in data["candles"] if isinstance(item, dict)]

    if not candles:
        return None
    return candles[-1]


def extract_candles(payload: dict[str, Any]) -> list[dict[str, Any]]:
    payload = normalize_service_payload(payload)
    data = payload.get("data")
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict) and isinstance(data.get("candles"), list):
        return [item for item in data["candles"] if isinstance(item, dict)]
    return []


def normalize_timeframe_seconds(timeframe: str | None, interval: int | None = None) -> tuple[str, int]:
    if timeframe is not None:
        normalized = str(timeframe).strip().upper()
        if normalized not in TIMEFRAME_SECONDS:
            raise HTTPException(status_code=422, detail="INVALID_TIMEFRAME")
        return normalized, TIMEFRAME_SECONDS[normalized]
    if interval is None:
        return "M1", TIMEFRAME_SECONDS["M1"]
    for label, seconds in TIMEFRAME_SECONDS.items():
        if int(interval) == seconds:
            return label, seconds
    raise HTTPException(status_code=422, detail="INVALID_TIMEFRAME")


def numeric_candle_time(candle: dict[str, Any]) -> float | None:
    value = candle.get("time")
    if value is None:
        for key in ("from", "at", "id"):
            candidate = candle.get(key)
            if candidate is not None:
                value = candidate
                break
    parsed = parse_datetime(value)
    if parsed is not None:
        return parsed.timestamp()
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def number_or_none(value: Any) -> float | int | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return int(parsed) if parsed.is_integer() else parsed


def normalize_live_candle(candle: dict[str, Any], *, interval: int, server_time: float) -> dict[str, Any] | None:
    candle_time = numeric_candle_time(candle)
    if candle_time is None:
        return None
    high = candle.get("high") if "high" in candle else candle.get("max")
    low = candle.get("low") if "low" in candle else candle.get("min")
    normalized = {
        "time": int(candle_time),
        "open": number_or_none(candle.get("open")),
        "high": number_or_none(high),
        "low": number_or_none(low),
        "close": number_or_none(candle.get("close")),
        "volume": number_or_none(candle.get("volume")) or 0,
        "is_closed": server_time >= candle_time + interval,
    }
    if normalized["high"] is None:
        normalized["high"] = normalized["close"] if normalized["close"] is not None else normalized["open"]
    if normalized["low"] is None:
        normalized["low"] = normalized["close"] if normalized["close"] is not None else normalized["open"]
    return normalized


def build_live_candles_payload(
    symbol: str,
    timeframe: str,
    interval: int,
    limit: int,
    server_time: float,
    payload: dict[str, Any],
) -> dict[str, Any]:
    current_candle_time = int(server_time // interval) * interval
    candles = [
        normalized
        for candle in extract_candles(payload)
        if (normalized := normalize_live_candle(candle, interval=interval, server_time=server_time)) is not None
    ]
    candles.sort(key=lambda item: int(item["time"]))

    latest_close = next(
        (candle.get("close") for candle in reversed(candles) if candle.get("close") is not None),
        None,
    )
    if candles and int(candles[-1]["time"]) < current_candle_time and latest_close is not None:
        candles.append(
            {
                "time": current_candle_time,
                "open": latest_close,
                "high": latest_close,
                "low": latest_close,
                "close": latest_close,
                "volume": 0,
                "is_closed": False,
            }
        )
    if candles:
        candles[-1]["is_closed"] = False
    return {
        "symbol": symbol,
        "active": symbol,
        "timeframe": timeframe,
        "interval": interval,
        "count": min(limit, len(candles)),
        "limit": limit,
        "server_time": server_time,
        "candles": candles[-limit:],
    }


def build_chart_candles_success(
    data: dict[str, Any],
    *,
    from_cache: bool,
    limit: int,
) -> dict[str, Any]:
    normalized = deepcopy(data)
    candles = normalized.get("candles")
    if isinstance(candles, list):
        normalized["candles"] = candles[-limit:]
        normalized["count"] = len(normalized["candles"])
    normalized["limit"] = limit
    normalized["from_cache"] = from_cache
    normalized["updating"] = from_cache
    payload = build_success(normalized)
    if from_cache:
        payload["warning"] = "Atualizando candles..."
    return payload


def build_chart_candles_unavailable() -> dict[str, Any]:
    return {
        "ok": False,
        "data": {
            "candles": [],
            "from_cache": False,
        },
        "error": "CANDLES_TEMPORARY_UNAVAILABLE",
        "warning": "Atualizando candles...",
    }


def is_session_disconnected(payload: dict[str, Any]) -> bool:
    payload = normalize_service_payload(payload)
    error = str(payload.get("error") or "").strip().upper()
    return error in {"SESSION_NOT_FOUND", "SESSION_DISCONNECTED"}


def payload_indicates_offline(status_code: int, payload: dict[str, Any]) -> bool:
    connected = payload_connected_state(payload)
    return status_code == 404 or is_session_disconnected(payload) or connected is False


async def close_market_websocket(websocket: WebSocket, payload: dict[str, Any]) -> None:
    try:
        await websocket.send_json(payload)
    except Exception:
        logger.exception("falha ao enviar mensagem final do websocket de mercado")
    try:
        await websocket.close(code=1008)
    except Exception:
        logger.exception("falha ao fechar websocket de mercado")


async def stream_market_updates(websocket: WebSocket, user_id: str, active: str) -> None:
    previous_signature: tuple[Any, Any] | None = None
    while True:
        try:
            status_code, payload = await call_bullex_service(
                "GET",
                "/candles",
                user_id,
                params={"active": active, "interval": 60, "count": 2},
            )
            if not payload.get("ok"):
                if is_session_disconnected(payload):
                    mark_disconnected_from_payload(user_id, payload)
                    await close_market_websocket(
                        websocket,
                        {
                            "type": "error",
                            "error": "SESSION_DISCONNECTED",
                        },
                    )
                    return
                logger.warning("[MARKET WS ERROR] user_id=%s active=%s status=%s error=%s", user_id, active, status_code, payload.get("error"))
                await websocket.send_json({"type": "warning", "error": "MARKET_STREAM_TEMPORARY_ERROR"})
                await asyncio.sleep(1)
                continue

            latest_candle = extract_latest_candle(payload)
            if latest_candle is None:
                logger.warning("[MARKET WS ERROR] user_id=%s active=%s error=UNEXPECTED_CANDLES_PAYLOAD", user_id, active)
                await websocket.send_json({"type": "warning", "error": "MARKET_STREAM_TEMPORARY_ERROR"})
                await asyncio.sleep(1)
                continue

            current_signature = (
                latest_candle.get("from") or latest_candle.get("time"),
                latest_candle.get("close"),
            )
            if current_signature != previous_signature:
                message = build_market_ws_payload(user_id, active, latest_candle)
                logger.info("[MARKET WS MESSAGE] user_id=%s active=%s payload=%s", user_id, active, message)
                await manager.broadcast_to_user_active(user_id, active, message)
                previous_signature = current_signature
        except WebSocketDisconnect:
            raise
        except Exception:
            logger.exception("[MARKET WS ERROR] user_id=%s active=%s error=UNHANDLED_STREAM_EXCEPTION", user_id, active)
            try:
                await websocket.send_json({"type": "warning", "error": "MARKET_STREAM_TEMPORARY_ERROR"})
            except Exception:
                raise
        await asyncio.sleep(1)


async def require_headers(
    x_api_key: str | None = Header(default=None),
    x_user_id: str | None = Header(default=None),
) -> dict[str, str]:
    require_api_key_value(x_api_key)

    user_id = (x_user_id or "").strip()
    if not user_id:
        raise HTTPException(status_code=400, detail="MISSING_USER_ID")

    return {"user_id": user_id}


def require_api_key_value(x_api_key: str | None) -> None:
    if not config.panel_api_key:
        raise HTTPException(status_code=500, detail="PANEL_API_KEY_NOT_CONFIGURED")
    if x_api_key != config.panel_api_key:
        raise HTTPException(status_code=401, detail="INVALID_API_KEY")


async def require_api_key(x_api_key: str | None = Header(default=None)) -> None:
    require_api_key_value(x_api_key)


@app.exception_handler(HTTPException)
async def http_exception_handler(_: Request, exc: HTTPException) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content=build_error(str(exc.detail)))


@app.exception_handler(RequestValidationError)
async def validation_error_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
    message = "; ".join(error["msg"] for error in exc.errors())
    return JSONResponse(status_code=422, content=build_error(message))


@app.exception_handler(Exception)
async def unhandled_error_handler(request: Request, exc: Exception) -> JSONResponse:
    if request.url.path in BAD_GATEWAY_PROTECTED_PATHS:
        logger.warning(
            "[UPSTREAM_ERROR_HANDLED] path=%s reason=%s",
            request.url.path,
            exc.__class__.__name__,
            exc_info=True,
        )
        return JSONResponse(
            status_code=200,
            content=build_controlled_upstream_error(exc),
        )
    return JSONResponse(status_code=500, content=build_error("INTERNAL_ERROR"))


async def call_bullex_service(
    method: str,
    path: str,
    user_id: str,
    json_body: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
    *,
    allow_session_restore: bool = False,
    allow_failure_backoff: bool = True,
    force_refresh: bool = False,
) -> tuple[int, dict[str, Any]]:
    cache_key = build_cache_key(path, params)
    ttl_seconds = request_cache_ttl_seconds(path, params)
    if method == "GET" and ttl_seconds is not None and not allow_session_restore and not force_refresh:
        cache = get_session_cache(user_id)
        cached = cache.responses.get(cache_key)
        now = utc_now()
        if cached is not None and now < cached.expires_at:
            log_fetch_metric(path, monotonic(), user_id=user_id, source="fresh_cache", status_code=cached.status_code)
            if path in {"/candles", "/payouts"}:
                symbol = normalize_binary_active(str((params or {}).get("active") or ""))
                logger.info("[ACTIVE_CACHE] user_id=%s symbol=%s path=%s", user_id, symbol, path)
                schedule_background_refresh(user_id, method, path, params=params)
            elif path == "/account":
                schedule_background_refresh(user_id, method, path, params=params)
            if path == "/account":
                logger.info(
                    "[ACCOUNT_CACHE_HIT] user_id=%s ttl_remaining=%.2f",
                    user_id,
                    (cached.expires_at - now).total_seconds(),
                )
                logger.info("[ACCOUNT_CACHE_RETURNED] user_id=%s source=fresh_cache", user_id)
            elif path == "/sessions/status":
                logger.info(
                    "[SESSION_STATUS_CACHE_HIT] user_id=%s path=%s ttl_remaining=%.2f",
                    user_id,
                    path,
                    (cached.expires_at - now).total_seconds(),
                )
            elif is_order_result_path(path):
                logger.info(
                    "[ORDER_RESULT_POLL_THROTTLED] user_id=%s path=%s ttl_remaining=%.2f",
                    user_id,
                    path,
                    (cached.expires_at - now).total_seconds(),
                )
            else:
                logger.info(
                    "[CACHE_HIT] user_id=%s path=%s ttl_remaining=%.2f",
                    user_id,
                    path,
                    (cached.expires_at - now).total_seconds(),
                )
            return cached.status_code, deepcopy(cached.payload)
        if path in SESSION_CACHEABLE_PATHS and not is_user_active(user_id):
            logger.info("[OFFLINE_USER_SKIPPED] user_id=%s path=%s", user_id, path)
            logger.info("[BACKOFF_SKIPPED_OFFLINE_USER] user_id=%s path=%s", user_id, path)
            return 200, inactive_user_payload(user_id, cache_key)
        if path == "/sessions/status" and should_throttle_session_status(user_id, cache_key):
            throttled = cached_session_status_response(user_id, cache_key)
            if throttled is not None:
                return throttled
        if path == "/sessions/status":
            logger.info("[SESSION_STATUS_CACHE_MISS] user_id=%s path=%s", user_id, path)
        else:
            logger.info("[CACHE_MISS] user_id=%s path=%s", user_id, path)
        if path in SESSION_CACHEABLE_PATHS:
            guard = connection_guard_reason(user_id)
        else:
            guard = None
        if guard is not None:
            reason, remaining = guard
            logger.warning("[SESSION_CHECK_SKIPPED] user_id=%s path=%s reason=%s retry_in=%.2f", user_id, path, reason, remaining)
            if reason == "offline":
                logger.warning("[USER_OFFLINE_SKIPPED] user_id=%s path=%s retry_in=%.2f", user_id, path, remaining)
            else:
                logger.warning("[BACKOFF_ACTIVE] user_id=%s path=%s retry_in=%.2f", user_id, path, remaining)
            logger.warning("[CPU_LOOP_PROTECTION] user_id=%s path=%s reason=%s", user_id, path, reason)
            successful = cached_successful_response(user_id, cache_key)
            if successful is not None:
                if path == "/account":
                    logger.warning(
                        "[ACCOUNT_FETCH_FALLBACK] user_id=%s source=last_valid_cache reason=%s",
                        user_id,
                        reason,
                    )
                    logger.warning(
                        "[ACCOUNT_CACHE_RETURNED] user_id=%s source=last_valid_cache",
                        user_id,
                    )
                return 200, add_stale_warning(successful.payload)
            if cached is not None:
                return cached.status_code, deepcopy(cached.payload)
            return 200, backoff_payload(user_id, remaining)
        # Market data has per-active isolation; session backoff must not stop the robot cycle.

    headers = {"x-user-id": user_id}
    if allow_session_restore:
        headers["x-allow-session-restore"] = "true"
    url = f"{config.bullex_service_url}{path}"
    timeout_seconds = (
        BULLEX_CONNECT_TIMEOUT_SECONDS
        if method == "POST" and path == "/sessions/connect"
        else BULLEX_MARKET_DATA_TIMEOUT_SECONDS
        if method == "GET" and path in {"/candles", "/payouts"}
        else BULLEX_UPSTREAM_TIMEOUT_SECONDS
    )

    request_started_at = monotonic()
    try:
        async with httpx.AsyncClient(timeout=timeout_seconds) as client:
            response = await asyncio.wait_for(
                client.request(
                    method=method,
                    url=url,
                    headers=headers,
                    json=json_body,
                    params=params,
                ),
                timeout=timeout_seconds,
            )
    except (asyncio.TimeoutError, httpx.TimeoutException):
        log_fetch_metric(path, request_started_at, user_id=user_id, source="timeout")
        if path == "/account":
            logger.warning(
                "[ACCOUNT_FETCH_TIMEOUT] user_id=%s timeout_seconds=%s",
                user_id,
                timeout_seconds,
            )
            logger.warning(
                "[ACCOUNT_TIMEOUT_HANDLED] user_id=%s timeout_seconds=%s",
                user_id,
                timeout_seconds,
            )
        if method == "POST" and path == "/sessions/connect":
            logger.warning(
                "[CONNECT_TIMEOUT_HANDLED] user_id=%s timeout_seconds=%s",
                user_id,
                timeout_seconds,
            )
            return 504, build_error("LOGIN_TIMEOUT")
        if method == "GET" and path in SESSION_CACHEABLE_PATHS:
            return temporary_upstream_response(
                user_id,
                path,
                cache_key,
                reason="timeout",
                allow_failure_backoff=allow_failure_backoff,
            )
        logger.warning(
            "[UPSTREAM_ERROR_HANDLED] user_id=%s path=%s reason=timeout",
            user_id,
            path,
        )
        if method == "GET" and path in {"/candles", "/payouts"}:
            stale = stale_successful_response(user_id, cache_key)
            if stale is not None:
                logger.warning(
                    "[MARKET_DATA_STALE_FALLBACK] user_id=%s path=%s cache_key=%s reason=timeout",
                    user_id,
                    path,
                    cache_key,
                )
                logger.info(
                    "[ACTIVE_CACHE] user_id=%s symbol=%s path=%s stale=true",
                    user_id,
                    normalize_binary_active(str((params or {}).get("active") or "")),
                    path,
                )
                return 200, add_stale_warning(stale.payload)
            symbol = normalize_binary_active(str((params or {}).get("active") or ""))
            if symbol:
                set_named_cooldown(
                    active_cooldowns if path == "/candles" else payout_cooldowns,
                    user_id,
                    symbol,
                    seconds=ACTIVE_COOLDOWN_SECONDS if path == "/candles" else PAYOUT_COOLDOWN_SECONDS,
                    log_label="ACTIVE_TIMEOUT" if path == "/candles" else "PAYOUT_TIMEOUT",
                    status=STATUS_ACTIVE_COOLDOWN if path == "/candles" else STATUS_PAYOUT_COOLDOWN,
                    reason="ACTIVE_TIMEOUT" if path == "/candles" else "PAYOUT_TIMEOUT",
                )
        return 503, build_error(BULLEX_TEMPORARY_UNAVAILABLE)
    except httpx.HTTPError as exc:
        log_fetch_metric(path, request_started_at, user_id=user_id, source=exc.__class__.__name__)
        if method == "GET" and path == "/payouts":
            symbol = normalize_binary_active(str((params or {}).get("active") or ""))
            if symbol:
                set_named_cooldown(
                    payout_cooldowns,
                    user_id,
                    symbol,
                    seconds=PAYOUT_COOLDOWN_SECONDS,
                    log_label="PAYOUT_COOLDOWN",
                    status=STATUS_PAYOUT_COOLDOWN,
                    reason="PAYOUT_COOLDOWN",
                )
        if method == "GET" and path in SESSION_CACHEABLE_PATHS:
            return temporary_upstream_response(
                user_id,
                path,
                cache_key,
                reason=exc.__class__.__name__,
                allow_failure_backoff=allow_failure_backoff,
            )
        logger.warning(
            "[UPSTREAM_ERROR_HANDLED] user_id=%s path=%s reason=%s",
            user_id,
            path,
            exc.__class__.__name__,
        )
        if method == "GET" and path in {"/candles", "/payouts"}:
            stale = stale_successful_response(user_id, cache_key)
            if stale is not None:
                logger.warning(
                    "[MARKET_DATA_STALE_FALLBACK] user_id=%s path=%s cache_key=%s reason=%s",
                    user_id,
                    path,
                    cache_key,
                    exc.__class__.__name__,
                )
                logger.info(
                    "[ACTIVE_CACHE] user_id=%s symbol=%s path=%s stale=true",
                    user_id,
                    normalize_binary_active(str((params or {}).get("active") or "")),
                    path,
                )
                return 200, add_stale_warning(stale.payload)
            symbol = normalize_binary_active(str((params or {}).get("active") or ""))
            if symbol:
                set_named_cooldown(
                    active_cooldowns if path == "/candles" else payout_cooldowns,
                    user_id,
                    symbol,
                    seconds=ACTIVE_COOLDOWN_SECONDS if path == "/candles" else PAYOUT_COOLDOWN_SECONDS,
                    log_label="ACTIVE_SKIPPED",
                    status=STATUS_ACTIVE_COOLDOWN if path == "/candles" else STATUS_PAYOUT_COOLDOWN,
                    reason=exc.__class__.__name__,
                )
        return 503, build_error(BULLEX_TEMPORARY_UNAVAILABLE)
    except Exception as exc:
        log_fetch_metric(path, request_started_at, user_id=user_id, source=exc.__class__.__name__)
        logger.warning(
            "[UPSTREAM_ERROR_HANDLED] user_id=%s path=%s reason=%s",
            user_id,
            path,
            exc.__class__.__name__,
            exc_info=True,
        )
        if method == "GET" and path in SESSION_CACHEABLE_PATHS:
            return temporary_upstream_response(
                user_id,
                path,
                cache_key,
                reason=exc.__class__.__name__,
                allow_failure_backoff=allow_failure_backoff,
            )
        if method == "GET" and path in {"/candles", "/payouts"}:
            stale = stale_successful_response(user_id, cache_key)
            if stale is not None:
                logger.warning(
                    "[MARKET_DATA_STALE_FALLBACK] user_id=%s path=%s cache_key=%s reason=%s",
                    user_id,
                    path,
                    cache_key,
                    exc.__class__.__name__,
                )
                logger.info(
                    "[ACTIVE_CACHE] user_id=%s symbol=%s path=%s stale=true",
                    user_id,
                    normalize_binary_active(str((params or {}).get("active") or "")),
                    path,
                )
                return 200, add_stale_warning(stale.payload)
        return 503, build_error(BULLEX_TEMPORARY_UNAVAILABLE)

    log_fetch_metric(path, request_started_at, user_id=user_id, source="upstream", status_code=response.status_code)

    try:
        payload = response.json()
    except ValueError:
        payload = build_error("INVALID_BULLEX_RESPONSE")

    response_contract_valid = (
        isinstance(payload, dict)
        and "ok" in payload
        and "data" in payload
        and "error" in payload
    )
    if not response_contract_valid:
        payload = build_success(payload) if response.is_success else build_error("INVALID_BULLEX_RESPONSE")

    if (
        method == "GET"
        and path in SESSION_CACHEABLE_PATHS
        and (response.status_code >= 500 or not response_contract_valid)
    ):
        return temporary_upstream_response(
            user_id,
            path,
            cache_key,
            reason=f"status_{response.status_code}",
            allow_failure_backoff=allow_failure_backoff,
        )

    if response.status_code >= 500:
        error = str(payload.get("error") or "").strip().upper()
        logger.warning(
            "[UPSTREAM_ERROR_HANDLED] user_id=%s path=%s reason=status_%s",
            user_id,
            path,
            response.status_code,
        )
        if method == "POST" and path == "/sessions/connect" and error == "LOGIN_TIMEOUT":
            logger.warning("[CONNECT_TIMEOUT_HANDLED] user_id=%s source=upstream", user_id)
            return 504, build_error("LOGIN_TIMEOUT")
        if method == "GET" and path in {"/candles", "/payouts"}:
            stale = stale_successful_response(user_id, cache_key)
            if stale is not None:
                logger.warning(
                    "[MARKET_DATA_STALE_FALLBACK] user_id=%s path=%s cache_key=%s reason=status_%s",
                    user_id,
                    path,
                    cache_key,
                    response.status_code,
                )
                logger.info(
                    "[ACTIVE_CACHE] user_id=%s symbol=%s path=%s stale=true",
                    user_id,
                    normalize_binary_active(str((params or {}).get("active") or "")),
                    path,
                )
                return 200, add_stale_warning(stale.payload)
        return 503, build_error(BULLEX_TEMPORARY_UNAVAILABLE)

    cacheable_success = (
        method == "GET"
        and ttl_seconds is not None
        and response.is_success
        and payload.get("ok")
        and (
            path not in SESSION_CACHEABLE_PATHS
            or payload_connected_state(payload) is not False
        )
    )
    if cacheable_success:
        entry = BullexResponseCacheEntry(
            status_code=response.status_code,
            payload=deepcopy(payload),
            expires_at=utc_now() + timedelta(seconds=ttl_seconds),
        )
        cache = get_session_cache(user_id)
        cache.responses[cache_key] = entry
        cache.last_successful_responses[cache_key] = deepcopy(entry)

    if method == "GET" and path in SESSION_CACHEABLE_PATHS:
        if payload.get("ok") and payload_connected_state(payload) is True:
            clear_session_backoff(user_id)
        elif payload_indicates_offline(response.status_code, payload):
            if allow_failure_backoff:
                mark_session_failure(user_id, offline=True)
            else:
                logger.info(
                    "[BACKOFF_SKIPPED_RESTORE] user_id=%s path=%s",
                    user_id,
                    path,
                )
        else:
            if allow_failure_backoff:
                mark_session_failure(user_id)
            else:
                logger.info(
                    "[BACKOFF_SKIPPED_RESTORE] user_id=%s path=%s",
                    user_id,
                    path,
                )

    if method == "GET" and path in {"/candles", "/payouts"}:
        symbol = normalize_binary_active(str((params or {}).get("active") or ""))
        error_text = str(payload.get("error") or "").strip().lower()
        if symbol and (
            response.status_code == 404
            or "asset unavailable" in error_text
            or "active suspended" in error_text
            or "active not found" in error_text
        ):
            set_named_cooldown(
                active_cooldowns,
                user_id,
                symbol,
                seconds=ACTIVE_COOLDOWN_SECONDS,
                log_label="ACTIVE_COOLDOWN",
                status=STATUS_ACTIVE_COOLDOWN,
                reason="ACTIVE_COOLDOWN",
            )
        elif path == "/payouts" and symbol and (response.status_code >= 500 or not payload.get("ok")):
            set_named_cooldown(
                payout_cooldowns,
                user_id,
                symbol,
                seconds=PAYOUT_COOLDOWN_SECONDS,
                log_label="PAYOUT_COOLDOWN",
                status=STATUS_PAYOUT_COOLDOWN,
                reason="PAYOUT_COOLDOWN",
            )

    return response.status_code, payload


def json_response(status_code: int, payload: dict[str, Any]) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content=normalize_service_payload(payload),
    )


def build_connection_payload(data: dict[str, Any], fallback_email: str | None = None) -> dict[str, Any]:
    updates: dict[str, Any] = {}
    if fallback_email is not None:
        updates["bullex_email"] = fallback_email

    field_map = {
        "email": "bullex_email",
        "connected": "connected",
        "balance": "last_balance",
        "currency": "currency",
        "mode": "account_mode",
        "active_mode": "account_mode",
        "active_mode_from_bullex": "account_mode",
        "requires_2fa": "requires_2fa",
    }
    for source_field, target_field in field_map.items():
        if source_field in data:
            updates[target_field] = data[source_field]
    if updates.get("connected") is True:
        updates["last_connected_at"] = datetime.now(timezone.utc).isoformat()
    return updates


def build_real_account_contract(payload: dict[str, Any]) -> dict[str, Any]:
    payload = normalize_service_payload(
        payload,
        error="REAL_BALANCE_NOT_DETECTED",
    )
    raw_data = payload.get("data")
    data = deepcopy(raw_data) if isinstance(raw_data, dict) else {}
    connected = bool(data.get("connected"))
    active_mode = str(
        data.get("active_mode_from_bullex")
        or data.get("active_mode")
        or data.get("mode")
        or ""
    ).strip().upper() or None
    balance_real = number_or_none(data.get("balance_real"))
    balance_practice = number_or_none(data.get("balance_practice"))
    current_balance = number_or_none(data.get("balance"))
    if active_mode == "REAL" and balance_real is None:
        balance_real = current_balance
    elif active_mode == "PRACTICE" and balance_practice is None:
        balance_practice = current_balance

    data.update(
        {
            "connected": connected,
            "active_mode_real_detected": active_mode == "REAL",
            "active_mode": active_mode,
            "active_mode_from_bullex": active_mode,
            "balance_real": balance_real,
            "balance_practice": balance_practice,
            "balance": balance_real if active_mode == "REAL" else None,
            "mode": active_mode,
        }
    )
    if active_mode == "REAL" and balance_real is not None:
        logger.info("[REAL_BALANCE_DETECTED] balance=%s", balance_real)
        return build_success(data)
    if balance_practice is not None:
        logger.warning("[PRACTICE_BALANCE_IGNORED] balance=%s", balance_practice)
    logger.warning("[REAL_MODE_NOT_CONFIRMED] active_mode=%s", active_mode)
    return {
        "ok": False,
        "data": data,
        "error": (
            "BULLEX_ACTIVE_MODE_NOT_REAL"
            if active_mode is not None and active_mode != "REAL"
            else "REAL_BALANCE_NOT_DETECTED"
        ),
    }


def build_insufficient_balance_start_response(state: Any, *, message: str) -> dict[str, Any]:
    data = build_robot_payload(state)["data"]
    data.update(
        {
            "enabled": False,
            "worker_running": False,
            "operation_in_progress": False,
            "status": STATUS_INSUFFICIENT_BALANCE,
            "operation_message": message,
            "status_message": message,
        }
    )
    return {
        "ok": False,
        "error": STATUS_INSUFFICIENT_BALANCE,
        "message": message,
        "data": data,
    }


def sync_user_store_from_payload(
    user_id: str,
    payload: dict[str, Any],
    fallback_email: str | None = None,
    *,
    is_new_connection: bool = False,
) -> None:
    payload = normalize_service_payload(payload)
    try:
        if not payload.get("ok"):
            if payload.get("error") in {SESSION_DISCONNECTED, SESSION_NOT_FOUND}:
                user_store.disconnect(user_id)
            return

        data = payload.get("data")
        if not isinstance(data, dict):
            return

        updates = build_connection_payload(data, fallback_email)
        if updates:
            if is_new_connection:
                user_store.save_connection(user_id, updates)
            else:
                user_store.update_connection(user_id, updates)
    except Exception as exc:
        logger.warning(
            "[SUPABASE PERSISTENCE WARNING] user_id=%s operation=bullex_connection error=%s",
            user_id,
            exc,
        )


def mark_disconnected_from_payload(user_id: str, payload: dict[str, Any]) -> None:
    payload = normalize_service_payload(payload)
    if payload.get("error") not in {SESSION_DISCONNECTED, SESSION_NOT_FOUND}:
        return
    try:
        user_store.disconnect(user_id)
    except Exception:
        logger.exception("falha ao marcar sessao desconectada para %s", user_id)


def extract_account_status(payload: dict[str, Any]) -> tuple[bool, str | None]:
    payload = normalize_service_payload(payload)
    data = payload.get("data")
    if not payload.get("ok") or not isinstance(data, dict):
        return False, None
    connected = bool(data.get("connected"))
    mode = data.get("active_mode") or data.get("mode")
    return connected, str(mode).strip().upper() if mode else None


def connection_source_from_payload(payload: dict[str, Any], *, default: str = "bullex_service") -> str:
    payload = normalize_service_payload(payload)
    data = payload.get("data")
    if payload.get("ok") and isinstance(data, dict):
        raw_source = str(data.get("connection_status_source") or data.get("source") or "").strip()
        return raw_source if raw_source in {"memory", "bullex_service", "cached", "cached_grace"} else default
    return "disconnected" if is_session_disconnected(payload) else "cached"


def connection_grace_until(state: Any) -> datetime | None:
    grace_until = parse_datetime(getattr(state, "connection_grace_until", None))
    if grace_until is not None:
        return grace_until
    last_connected_at = parse_datetime(getattr(state, "last_connected_at", None))
    if last_connected_at is None:
        return None
    return last_connected_at + timedelta(seconds=CONNECTION_GRACE_SECONDS)


def connection_grace_active(state: Any) -> bool:
    grace_until = connection_grace_until(state)
    return grace_until is not None and utc_now() <= grace_until


def keep_connection_in_grace(user_id: str, state: Any, active_mode: str | None, checked_at: datetime) -> Any:
    state = auto_trader.sync_connection(
        user_id,
        connected=False,
        active_mode=state.active_mode or active_mode,
        source="cached_grace",
        checked_at=checked_at,
    )
    state.connected = True
    state.connection_grace_until = connection_grace_until(state)
    logger.warning(
        "[CONNECTION_GRACE_ACTIVE] user_id=%s failures=%s grace_until=%s",
        user_id,
        state.connection_failure_count,
        state.connection_grace_until,
    )
    return state


def sync_robot_connection_from_payload(
    user_id: str,
    payload: dict[str, Any],
    *,
    source: str | None = None,
) -> tuple[Any, bool, str | None, str]:
    state = auto_trader.get(user_id)
    connected, active_mode = extract_account_status(payload)
    checked_at = utc_now()
    resolved_source = source or connection_source_from_payload(payload)
    if connected:
        state = auto_trader.sync_connection(
            user_id,
            connected=True,
            active_mode=active_mode,
            source=resolved_source,
            checked_at=checked_at,
        )
        logger.info(
            "[ROBOT_CONNECTION_SYNCED] user_id=%s connected=true active_mode=%s source=%s",
            user_id,
            active_mode,
            resolved_source,
        )
        return state, True, active_mode, resolved_source

    if state.connected and state.connection_failure_count < 3 and connection_grace_active(state):
        state = keep_connection_in_grace(user_id, state, active_mode, checked_at)
        return state, True, state.active_mode, "cached_grace"

    state = auto_trader.sync_connection(
        user_id,
        connected=False,
        active_mode=active_mode,
        source="disconnected",
        checked_at=checked_at,
    )
    logger.warning(
        "[ROBOT_CONNECTION_CHECK_FAILED] user_id=%s failures=%s source=disconnected",
        user_id,
        state.connection_failure_count,
    )
    return state, False, active_mode, "disconnected"


async def reconcile_robot_connection_from_payload(
    user_id: str,
    payload: dict[str, Any],
    *,
    source: str | None = None,
) -> tuple[Any, bool, str | None, str]:
    state, connected, active_mode, resolved_source = sync_robot_connection_from_payload(
        user_id,
        payload,
        source=source,
    )
    payload_connected, _ = extract_account_status(payload)
    if payload_connected:
        sync_user_store_from_payload(user_id, payload)
        return state, connected, active_mode, resolved_source

    account_status, account_payload = await call_bullex_service("GET", "/account", user_id)
    account_connected, account_active_mode = extract_account_status(account_payload)
    if account_connected:
        sync_user_store_from_payload(user_id, account_payload)
        state = auto_trader.sync_connection(
            user_id,
            connected=True,
            active_mode=account_active_mode or active_mode,
            source="bullex_service",
            align_status=True,
        )
        logger.warning(
            "[CONNECTION_FALSE_NEGATIVE_IGNORED] user_id=%s failures=%s session_status=%s account_status=%s",
            user_id,
            state.connection_failure_count,
            payload.get("error") or payload.get("status"),
            account_status,
        )
        logger.info(
            "[ROBOT_CONNECTION_SYNCED] user_id=%s connected=true active_mode=%s source=bullex_service",
            user_id,
            state.active_mode,
        )
        return state, True, state.active_mode, "bullex_service"

    if state.connection_failure_count >= 3 and not connection_grace_active(state):
        state = auto_trader.defer_cycle(
            user_id,
            STATUS_WAITING_RECOVERY,
            wait_seconds=SESSION_OFFLINE_TTL_SECONDS,
            rejection_reason="WAITING_RECOVERY",
            last_rejection_reason="WAITING_RECOVERY",
            last_order_error="WAITING_RECOVERY",
        )
        state.connected = False
        state.active_mode = account_active_mode or active_mode
        logger.warning(
            "[WAITING_RECOVERY] user_id=%s failures=%s account_status=%s",
            user_id,
            state.connection_failure_count,
            account_status,
        )
        return state, False, state.active_mode, "backoff_active"

    logger.warning(
        "[CONNECTION_GRACE_ACTIVE] user_id=%s failures=%s account_connected=false grace_until=%s",
        user_id,
        state.connection_failure_count,
        state.connection_grace_until,
    )
    if not connected and connection_grace_active(state):
        state.connected = True
        state.connection_status_source = "cached_grace"
        return state, True, state.active_mode or active_mode, "cached_grace"
    return state, connected, active_mode, resolved_source


async def fetch_and_sync_robot_connection(
    user_id: str,
    *,
    allow_session_restore: bool = False,
) -> tuple[int, dict[str, Any], Any, bool, str | None, str]:
    status_code, payload = await call_bullex_service(
        "GET",
        "/sessions/status",
        user_id,
        allow_session_restore=allow_session_restore,
    )
    state, connected, active_mode, source = await reconcile_robot_connection_from_payload(user_id, payload)
    return status_code, payload, state, connected, active_mode, source


async def refresh_account_snapshot_if_needed(
    user_id: str,
    *,
    connected: bool,
    active_mode: str | None,
) -> dict[str, Any]:
    snapshot = get_user_account_snapshot(user_id)
    needs_refresh = (
        connected
        and (
            snapshot.get("connected") is not True
            or snapshot.get("balance") is None
            or snapshot.get("currency") is None
            or (active_mode is not None and snapshot.get("mode") != active_mode)
        )
    )
    if not needs_refresh:
        return snapshot
    _, payload = await call_bullex_service("GET", "/account", user_id)
    payload = normalize_service_payload(payload)
    if payload.get("ok"):
        sync_user_store_from_payload(user_id, payload)
        data = payload.get("data")
        if isinstance(data, dict):
            snapshot = get_user_account_snapshot(user_id)
            if active_mode == "REAL" and snapshot.get("balance") == 0:
                logger.info("[ACCOUNT_REAL_CONNECTED] user_id=%s balance=0 mode=REAL", user_id)
            return snapshot
    return snapshot


def fresh_robot_connection(state: Any, *, max_age_seconds: int = ROBOT_SESSION_REFRESH_SECONDS) -> bool:
    checked_at = getattr(state, "connection_checked_at", None)
    if checked_at is None:
        return False
    age = (utc_now() - checked_at).total_seconds()
    return 0 <= age <= max_age_seconds


def cached_robot_connection_payload(state: Any) -> dict[str, Any]:
    return build_success(
        {
            "connected": bool(getattr(state, "connected", False)),
            "active_mode": getattr(state, "active_mode", None),
            "server_time": None,
            "connection_status_source": getattr(state, "connection_status_source", "cached"),
        }
    )


def estimate_state_server_timestamp(state: Any) -> float | None:
    checked_at = getattr(state, "connection_checked_at", None)
    server_time = getattr(state, "server_time", None)
    if checked_at is None or not server_time:
        return None
    parsed_server_time = parse_datetime(server_time)
    if parsed_server_time is None:
        return None
    elapsed = (utc_now() - checked_at).total_seconds()
    if elapsed < 0:
        return None
    return parsed_server_time.timestamp() + elapsed


def build_guarded_connection_payload(reason: str) -> dict[str, Any]:
    return disconnected_cache_payload(source="offline_cache" if reason == "offline" else "backoff_active")


TIMEFRAME_SECONDS = {"M1": 60, "M5": 300, "M15": 900, "M30": 1800}
ENTRY_WINDOWS = {
    "M1": (0, 3),
    "M5": (0, 3),
    "M15": (0, 3),
    "M30": (0, 3),
}
ANALYSIS_WINDOWS = {
    "M1": (5, 20),
    "M5": (5, 20),
    "M15": (5, 20),
    "M30": (5, 20),
}
EXPIRATION_SAFETY_SECONDS = 1
ORDER_EXPIRATION_FIELDS = (
    "expected_expire_at",
    "expires_at",
    "expire_at",
    "close_time",
    "closed_at",
    "expiration_time",
    "expiration_at",
    "expiration_timestamp",
    "expire_timestamp",
    "close_timestamp",
)


def extract_server_timestamp(payload: dict[str, Any]) -> float | None:
    payload = normalize_service_payload(payload)
    data = payload.get("data")
    if not payload.get("ok") or not isinstance(data, dict):
        return None
    try:
        timestamp = float(data["server_time"])
    except (KeyError, TypeError, ValueError):
        return None
    return timestamp if timestamp > 0 else None


def get_entry_window(
    timeframe: str,
    server_timestamp: float | None = None,
    *,
    server_time_source: str = "bullex",
) -> dict[str, Any]:
    normalized = str(timeframe).strip().upper()
    if normalized not in TIMEFRAME_SECONDS:
        raise ValueError("INVALID_TIMEFRAME")
    if server_timestamp is None:
        server_timestamp = utc_now().timestamp()
        server_time_source = "vps_fallback"

    expiration_seconds = TIMEFRAME_SECONDS[normalized]
    window_start, window_end = ENTRY_WINDOWS[normalized]
    analysis_window_start, analysis_window_end = ANALYSIS_WINDOWS[normalized]
    seconds_in_candle = float(server_timestamp) % expiration_seconds
    seconds_until_close = expiration_seconds - seconds_in_candle
    analysis_window_open = analysis_window_start <= seconds_in_candle <= analysis_window_end
    entry_window_open = window_start <= seconds_in_candle <= window_end
    if analysis_window_open:
        seconds_until_analysis_window = 0
    elif seconds_in_candle < analysis_window_start:
        seconds_until_analysis_window = math.ceil(analysis_window_start - seconds_in_candle)
    else:
        seconds_until_analysis_window = math.ceil(
            expiration_seconds - seconds_in_candle + analysis_window_start
        )
    missed_entry_window = seconds_in_candle > window_end
    if entry_window_open:
        seconds_until_entry_window = 0
    else:
        seconds_until_entry_window = math.ceil(expiration_seconds - seconds_in_candle + window_start)

    return {
        "server_timestamp": float(server_timestamp),
        "server_time": datetime.fromtimestamp(server_timestamp, timezone.utc).isoformat(),
        "server_time_source": server_time_source,
        "timeframe": normalized,
        "analysis_window_open": analysis_window_open,
        "seconds_until_analysis_window": seconds_until_analysis_window,
        "analysis_window_start_second": analysis_window_start,
        "analysis_window_end_second": analysis_window_end,
        "entry_window_open": entry_window_open,
        "missed_entry_window": missed_entry_window,
        "seconds_until_entry_window": seconds_until_entry_window,
        "current_candle_seconds": round(seconds_in_candle, 3),
        "entry_window_start_second": window_start,
        "entry_window_end_second": window_end,
        "buy_target_second": window_start,
        "seconds_until_close": round(seconds_until_close, 3),
        "expiration_seconds": expiration_seconds,
        "expiration": normalized,
        "expiration_minutes": expiration_seconds // 60,
    }


def parse_order_expiration_value(value: Any) -> datetime | None:
    parsed = parse_datetime(value)
    if parsed is not None:
        return parsed

    try:
        timestamp = float(value)
    except (TypeError, ValueError):
        return None
    if timestamp <= 0:
        return None
    if timestamp > 1_000_000_000_000:
        timestamp = timestamp / 1000
    return datetime.fromtimestamp(timestamp, timezone.utc)


def extract_order_expiration(order_data: dict[str, Any]) -> tuple[datetime | None, str | None]:
    for field in ORDER_EXPIRATION_FIELDS:
        if field not in order_data:
            continue
        parsed = parse_order_expiration_value(order_data.get(field))
        if parsed is not None:
            return parsed, field
    return None, None


def calculate_expected_expire_at(
    timeframe: str,
    order_data: dict[str, Any],
    entry_window: dict[str, Any],
    sent_at: datetime,
) -> tuple[datetime, str]:
    returned_expiration, source = extract_order_expiration(order_data)
    if returned_expiration is not None and source is not None:
        return returned_expiration, source

    interval = TIMEFRAME_SECONDS[str(timeframe).strip().upper()]
    try:
        server_timestamp = float(entry_window["server_timestamp"])
    except (KeyError, TypeError, ValueError):
        server_timestamp = sent_at.timestamp()
    aligned_timestamp = math.ceil(server_timestamp / interval) * interval
    if aligned_timestamp <= server_timestamp:
        aligned_timestamp += interval
    aligned_timestamp += EXPIRATION_SAFETY_SECONDS
    return datetime.fromtimestamp(aligned_timestamp, timezone.utc), "server_time_aligned"


async def refresh_entry_window(user_id: str, state: Any) -> tuple[int, dict[str, Any], dict[str, Any] | None]:
    if fresh_robot_connection(state) or robot_has_recent_real_cache(user_id, state):
        estimated_timestamp = estimate_state_server_timestamp(state)
        window = get_entry_window(
            state.timeframe,
            estimated_timestamp,
            server_time_source="bullex" if estimated_timestamp is not None else "vps_fallback",
        )
        auto_trader.update_entry_window(user_id, window)
        logger.info(
            "[ENTRY_WINDOW_CALCULATED] user_id=%s timeframe=%s current_candle_seconds=%s "
            "server_time_source=%s analysis_window_start=%s analysis_window_end=%s analysis_open=%s "
            "window_start=%s window_end=%s buy_target_second=%s open=%s",
            user_id,
            state.timeframe,
            window["current_candle_seconds"],
            window["server_time_source"],
            window["analysis_window_start_second"],
            window["analysis_window_end_second"],
            window["analysis_window_open"],
            window["entry_window_start_second"],
            window["entry_window_end_second"],
            window["buy_target_second"],
            window["entry_window_open"],
        )
        return 200, cached_robot_connection_payload(state), window

    status_code, payload = await call_bullex_service("GET", "/sessions/status", user_id)
    timestamp = extract_server_timestamp(payload)
    if timestamp is None:
        timestamp = utc_now().timestamp()
        window = get_entry_window(
            state.timeframe,
            timestamp,
            server_time_source="vps_fallback",
        )
        logger.warning(
            "[SERVER_TIME_FALLBACK] user_id=%s status_code=%s current_candle_seconds=%s",
            user_id,
            status_code,
            window["current_candle_seconds"],
        )
    else:
        previous_source = getattr(state, "server_time_source", None)
        window = get_entry_window(
            state.timeframe,
            timestamp,
            server_time_source="bullex",
        )
        if previous_source == "vps_fallback" and getattr(state, "server_time", None):
            logger.info("[SERVER_TIME_BULLEX_RESTORED] user_id=%s", user_id)
    auto_trader.update_entry_window(user_id, window)
    logger.info(
        "[ENTRY_WINDOW_CALCULATED] user_id=%s timeframe=%s current_candle_seconds=%s "
        "server_time_source=%s analysis_window_start=%s analysis_window_end=%s analysis_open=%s "
        "window_start=%s window_end=%s buy_target_second=%s open=%s",
        user_id,
        state.timeframe,
        window["current_candle_seconds"],
        window["server_time_source"],
        window["analysis_window_start_second"],
        window["analysis_window_end_second"],
        window["analysis_window_open"],
        window["entry_window_start_second"],
        window["entry_window_end_second"],
        window["buy_target_second"],
        window["entry_window_open"],
    )
    return status_code, payload, window


def real_block_reason(
    state: Any,
    *,
    connected: bool,
    active_mode: str | None,
    user_id: str | None = None,
) -> str | None:
    reason: str | None = None
    if state.account_mode != "REAL":
        reason = "ACCOUNT_MODE_NOT_REAL"
    elif state.operation_in_progress:
        reason = "OPERATION_IN_PROGRESS"
    elif robot_connection_unavailable(connected, active_mode):
        reason = "BULLEX_NOT_CONNECTED"
    elif active_mode != "REAL":
        reason = "BULLEX_ACTIVE_MODE_NOT_REAL"
    elif state.entry_value <= 0:
        reason = "AMOUNT_MUST_BE_POSITIVE"
    else:
        stop_reason = (daily_stop_reason(user_id, state) if user_id is not None else None) or robot_stop_reason(state)
        if stop_reason is not None:
            reason = stop_reason
        elif state.entry_value > MAX_REAL_ENTRY:
            reason = "REAL_ENTRY_VALUE_EXCEEDS_MAX"
    logger.info(
        "[REAL_READY_CHECK] user_id=%s account_mode=%s active_mode=%s connected=%s allow_real=%s confirm_real=%s reason=%s",
        user_id,
        getattr(state, "account_mode", None),
        active_mode,
        connected,
        getattr(state, "allow_real", None),
        getattr(state, "confirm_real", None),
        reason,
    )
    return reason


def validate_real_buy_gateway_payload(body: dict[str, Any]) -> str | None:
    if body.get("confirm_real") is not True:
        return "CONFIRM_REAL_REQUIRED"
    try:
        amount = float(body.get("amount"))
    except (TypeError, ValueError):
        return "AMOUNT_MUST_BE_POSITIVE"
    if amount <= 0:
        return "AMOUNT_MUST_BE_POSITIVE"
    return None


def real_buy_gateway_block_reason(user_id: str, state: Any, body: dict[str, Any]) -> str | None:
    if state.account_mode != "REAL":
        return "ACCOUNT_MODE_NOT_REAL"
    payload_reason = validate_real_buy_gateway_payload(body)
    if payload_reason is not None:
        return payload_reason
    if state.operation_in_progress:
        return "OPERATION_IN_PROGRESS"
    return daily_stop_reason(user_id, state) or robot_stop_reason(state)


def extract_payout(payload: dict[str, Any], symbol: str) -> float | None:
    payload = normalize_service_payload(payload)
    data = payload.get("data")
    if not isinstance(data, list):
        return None
    for item in data:
        if not isinstance(item, dict) or normalize_binary_active(str(item.get("symbol") or "")) != symbol:
            continue
        try:
            return float(item["payout"])
        except (KeyError, TypeError, ValueError):
            return None
    return None


def build_management_summary(user_id: str, state: Any) -> dict[str, Any]:
    today = datetime.now(timezone.utc).date()
    reset_at = parse_datetime(getattr(state, "stop_reset_at", None))
    gross_profit = 0.0
    gross_loss = 0.0
    net_profit = 0.0
    trades_count = 0
    try:
        trades = load_robot_history_items(user_id, 1)
    except Exception:
        logger.warning("[MANAGEMENT_HISTORY_FALLBACK] user_id=%s", user_id, exc_info=True)
        trades = auto_trader.history(user_id).get("trades", [])

    for trade in trades:
        result = str(trade.get("result") or trade.get("final_result") or "").strip().upper()
        if result not in {"WIN", "LOSS"}:
            continue
        finished_at = parse_datetime(trade.get("finished_at"))
        if finished_at is None or finished_at.date() != today:
            continue
        if reset_at is not None and finished_at < reset_at:
            continue
        trade_profit = float(trade.get("profit") or 0)
        trades_count += 1
        net_profit += trade_profit
        if trade_profit > 0:
            gross_profit += trade_profit
        elif trade_profit < 0:
            gross_loss += abs(trade_profit)

    stop_win = float(getattr(state, "stop_win", 0) or 0)
    stop_loss = float(getattr(state, "stop_loss", 0) or 0)
    stop_reason = None
    if stop_loss > 0 and gross_loss >= stop_loss:
        stop_reason = STATUS_STOP_LOSS_HIT
    elif stop_win > 0 and gross_profit >= stop_win:
        stop_reason = STATUS_STOP_WIN_HIT

    return {
        "gross_profit": round(gross_profit, 2),
        "gross_loss": round(gross_loss, 2),
        "net_profit": round(net_profit, 2),
        "trades_count": trades_count,
        "stop_win": stop_win,
        "stop_loss": stop_loss,
        "stop_reason": stop_reason,
        "reset_at": reset_at.isoformat() if reset_at is not None else None,
    }


def daily_stop_reason(user_id: str, state: Any) -> str | None:
    summary = build_management_summary(user_id, state)
    if summary["stop_reason"] == STATUS_STOP_LOSS_HIT:
        return "STOP_LOSS_HIT"
    if summary["stop_reason"] == STATUS_STOP_WIN_HIT:
        return "STOP_WIN_HIT"
    return None


def asset_cooldown_reason(user_id: str, symbol: str) -> str | None:
    remaining = active_cooldown_remaining(user_id, symbol)
    if remaining is not None:
        return "ACTIVE_COOLDOWN"
    losses = []
    for trade in auto_trader.history(user_id).get("trades", []):
        if normalize_binary_active(str(trade.get("active") or "")) != symbol:
            continue
        if trade.get("result") != "LOSS":
            break
        finished_at = parse_datetime(trade.get("finished_at"))
        if finished_at is None:
            break
        losses.append(finished_at)
        if len(losses) == 2:
            break
    if len(losses) < 2:
        return None
    if datetime.now(timezone.utc) < losses[0] + timedelta(minutes=30):
        return "ASSET_COOLDOWN"
    return None


def apply_strategy_guard(
    user_id: str,
    state: Any,
    signal: dict[str, Any],
    *,
    payout: float | None,
) -> tuple[bool, dict[str, Any], str | None]:
    selected = {
        **signal,
        "strategy_mode": signal.get("strategy_mode") or state.strategy_mode,
        "payout": payout,
    }
    blocked_filters = list(dict.fromkeys(selected.get("blocked_filters") or []))
    approved_filters = list(dict.fromkeys(selected.get("approved_filters") or []))

    def set_filter(name: str, passed: bool) -> None:
        if passed:
            if name in blocked_filters:
                blocked_filters.remove(name)
            if name not in approved_filters:
                approved_filters.append(name)
        else:
            if name in approved_filters:
                approved_filters.remove(name)
            if name not in blocked_filters:
                blocked_filters.append(name)

    confidence = int(selected.get("confidence") or 0)
    direction = str(selected.get("signal") or selected.get("direction") or "WAIT").upper()
    if direction not in {"CALL", "PUT"} and not any(
        reason in blocked_filters for reason in ("CANDLES_UNAVAILABLE", "INSUFFICIENT_CANDLES")
    ):
        trend = str(selected.get("trend") or "").upper()
        ema9 = float(selected.get("ema9") or 0)
        ema21 = float(selected.get("ema21") or 0)
        direction = "PUT" if trend in {"DOWN", "BEARISH"} or (ema9 and ema21 and ema9 < ema21) else "CALL"
    if payout is None:
        set_filter("PAYOUT_UNAVAILABLE", False)
    else:
        set_filter("PAYOUT_UNAVAILABLE", True)
        set_filter("MIN_PAYOUT", float(payout) >= state.min_payout)
    set_filter("MIN_CONFIDENCE", confidence >= state.min_confidence)
    if direction in {"CALL", "PUT"}:
        if "SIGNAL_WAIT" in blocked_filters:
            blocked_filters.remove("SIGNAL_WAIT")
        if "SIGNAL_WAIT" in approved_filters:
            approved_filters.remove("SIGNAL_WAIT")
        if "DIRECTION_VALID" not in approved_filters:
            approved_filters.append("DIRECTION_VALID")
    else:
        if "CANDLES_UNAVAILABLE" not in blocked_filters:
            blocked_filters.append("CANDLES_UNAVAILABLE")
    if "INSUFFICIENT_CANDLES" in blocked_filters:
        blocked_filters.remove("INSUFFICIENT_CANDLES")
        if "CANDLES_UNAVAILABLE" not in blocked_filters:
            blocked_filters.append("CANDLES_UNAVAILABLE")
    set_filter(
        "TREND_CLEAR",
        selected.get("trend") != "SIDEWAYS" and int(selected.get("strength") or 0) >= 20,
    )
    set_filter("SIDEWAYS_FILTER", selected.get("trend") != "SIDEWAYS")

    if {"ema9", "ema21"}.issubset(selected):
        ema9 = float(selected.get("ema9") or 0)
        ema21 = float(selected.get("ema21") or 0)
        ema_ok = (direction == "CALL" and ema9 > ema21) or (direction == "PUT" and ema9 < ema21)
        set_filter("EMA_TREND", ema_ok)
    if "rsi" in selected:
        rsi = float(selected.get("rsi") or 50)
        rsi_ok = (direction == "CALL" and 55 <= rsi <= 75) or (direction == "PUT" and 25 <= rsi <= 45)
        set_filter("RSI_RANGE", rsi_ok)
    if "body_ratio" in selected and float(selected.get("body_ratio") or 0) < 0.55:
        set_filter("CANDLE_STRENGTH", False)
    if direction == "CALL" and "upper_wick_ratio" in selected and float(selected.get("upper_wick_ratio") or 0) > 0.45:
        set_filter("WICK_REJECTION", False)
    if direction == "PUT" and "lower_wick_ratio" in selected and float(selected.get("lower_wick_ratio") or 0) > 0.45:
        set_filter("WICK_REJECTION", False)
    if "atr_pct" in selected and float(selected.get("atr_pct") or 0) < 0.0001:
        set_filter("VOLATILITY", False)
    if "directional_candles_5" in selected and int(selected.get("directional_candles_5") or 0) < 3:
        set_filter("LAST_5_CONFIRMATION", False)
    if selected.get("alternating_last_3") and "NO_ALTERNATING_LAST_3" not in blocked_filters:
        set_filter("NO_ALTERNATING_LAST_3", False)

    symbol = normalize_binary_active(str(selected.get("symbol") or ""))
    cooldown = asset_cooldown_reason(user_id, symbol)
    if cooldown is not None:
        if cooldown not in blocked_filters:
            blocked_filters.append(cooldown)
        logger.warning("[ASSET_COOLDOWN] user_id=%s symbol=%s", user_id, symbol)

    penalties = {
        "TREND_CLEAR": 10,
        "SIDEWAYS_FILTER": 10,
        "EMA_TREND": 8,
        "RSI_RANGE": 8,
        "WICK_REJECTION": 8,
        "CANDLE_STRENGTH": 8,
        "DOJI_FILTER": 5,
        "VOLATILITY": 5,
        "LAST_5_CONFIRMATION": 5,
        "NO_ALTERNATING_LAST_3": 5,
        "ASSET_COOLDOWN": 10,
    }
    strategy_score = max(
        0,
        confidence - sum(penalties.get(name, 0) for name in set(blocked_filters)),
    )
    hard_blocks = [
        name
        for name in blocked_filters
        if name in CRITICAL_TRADE_BLOCKS
    ]
    trade_allowed = not hard_blocks
    reason = str(selected.get("reason") or selected.get("signal_explanation") or "").strip()
    if blocked_filters:
        reason = f"{reason} Penalizacoes/bloqueios: {', '.join(blocked_filters)}.".strip()
    selected["blocked_filters"] = blocked_filters
    selected["approved_filters"] = approved_filters
    selected["trade_allowed"] = trade_allowed
    selected["direction"] = direction
    selected["signal"] = direction
    selected["strategy_score"] = strategy_score
    selected["quality_score"] = strategy_score
    selected["score"] = strategy_score
    selected["block_reasons"] = list(blocked_filters)
    selected["reason"] = reason
    selected["entry_reason"] = selected.get("entry_reason") or reason
    selected["quality_reason"] = "OK" if trade_allowed else ",".join(hard_blocks)
    if trade_allowed:
        logger.info(
            "[STRATEGY_FILTER_PASS] user_id=%s symbol=%s mode=%s strategy_score=%s",
            user_id,
            symbol,
            state.strategy_mode,
            strategy_score,
        )
        return True, selected, None

    logger.info(
        "[STRATEGY_FILTER_BLOCK] user_id=%s symbol=%s mode=%s blocked_filters=%s",
        user_id,
        symbol,
        state.strategy_mode,
        blocked_filters,
    )
    return False, selected, hard_blocks[0] if hard_blocks else LOW_QUALITY_SIGNAL


def robot_stop_reason(state: Any) -> str | None:
    if state.operation_in_progress:
        return "OPERATION_IN_PROGRESS"
    if state.stop_win > 0 and state.profit >= state.stop_win:
        return STATUS_STOP_WIN_HIT
    if state.stop_loss > 0 and state.profit <= -state.stop_loss:
        return STATUS_STOP_LOSS_HIT
    return None


async def pause_robot_by_stop(user_id: str, reason: str) -> Any:
    state = auto_trader.pause_by_stop(user_id, reason)
    if reason == STATUS_STOP_WIN_HIT:
        logger.warning("[STOP_WIN_HIT] user_id=%s profit=%s", user_id, state.profit)
    else:
        logger.warning("[STOP_LOSS_HIT] user_id=%s profit=%s", user_id, state.profit)
    logger.warning("[ROBOT_PAUSED_BY_STOP] user_id=%s reason=%s", user_id, reason)
    persist_robot(user_id)
    await stop_robot_worker(user_id)
    return state


def robot_connection_unavailable(connected: bool, active_mode: str | None) -> bool:
    return not connected or active_mode is None


def is_stop_status(status: Any) -> bool:
    return str(status or "").strip().upper() in {
        STATUS_STOP_WIN_HIT,
        STATUS_STOP_LOSS_HIT,
    }


def get_user_account_snapshot(user_id: str | None) -> dict[str, Any]:
    if not user_id:
        return {"email": None, "balance": None, "currency": None, "mode": None, "connected": None}
    try:
        record = user_store.get_user(str(user_id))
    except Exception:
        logger.warning("[ACCOUNT_SNAPSHOT_LOOKUP_FAILED] user_id=%s", user_id, exc_info=True)
        return {"email": None, "balance": None, "currency": None, "mode": None, "connected": None}
    if record is None:
        return {"email": None, "balance": None, "currency": None, "mode": None, "connected": None}
    balance = None
    if record.last_balance is not None:
        try:
            balance = float(record.last_balance)
        except (TypeError, ValueError):
            balance = None
    mode = str(record.account_mode).strip().upper() if record.account_mode else None
    return {
        "email": record.bullex_email,
        "balance": balance,
        "currency": record.currency,
        "mode": mode,
        "connected": record.connected,
    }


def get_cached_account_snapshot(user_id: str) -> dict[str, Any]:
    cached = cached_successful_response(user_id, "/account")
    data = cached.payload.get("data") if cached is not None else None
    if not isinstance(data, dict):
        if isinstance(user_store, InMemoryUserStore):
            return get_user_account_snapshot(user_id)
        return {
            "email": None,
            "balance": None,
            "currency": None,
            "mode": None,
            "connected": None,
        }
    mode = data.get("active_mode_from_bullex") or data.get("active_mode") or data.get("mode")
    return {
        "email": data.get("email"),
        "balance": data.get("balance"),
        "currency": data.get("currency"),
        "mode": str(mode).strip().upper() if mode else None,
        "connected": data.get("connected"),
    }


def robot_has_recent_real_cache(user_id: str, state: Any, *, max_age_seconds: int = ROBOT_VALID_CACHE_SECONDS) -> bool:
    if not bool(getattr(state, "connected", False)):
        return False
    if str(getattr(state, "active_mode", "") or "").strip().upper() != "REAL":
        return False
    checked_at = getattr(state, "connection_checked_at", None)
    if checked_at is None:
        return False
    if not (0 <= (utc_now() - checked_at).total_seconds() <= max_age_seconds):
        return False
    snapshot = get_cached_account_snapshot(user_id)
    try:
        balance = float(snapshot.get("balance"))
    except (TypeError, ValueError):
        return False
    return balance > 0


def recent_real_account_connection_payload(user_id: str) -> dict[str, Any] | None:
    cached = get_session_cache(user_id).responses.get("/account")
    if cached is None or utc_now() >= cached.expires_at:
        return None
    data = cached.payload.get("data")
    if not isinstance(data, dict):
        return None
    active_mode = str(
        data.get("active_mode_from_bullex")
        or data.get("active_mode")
        or data.get("mode")
        or ""
    ).strip().upper()
    if data.get("connected") is not True or active_mode != "REAL" or data.get("balance") is None:
        return None
    clear_session_backoff(user_id)
    state = auto_trader.sync_connection(
        user_id,
        connected=True,
        active_mode="REAL",
        source="account_cache_grace",
        align_status=True,
    )
    return build_success(
        {
            "connected": True,
            "active_mode": "REAL",
            "server_time": None,
            "connection_status_source": state.connection_status_source,
            "from_cache": True,
        }
    )


def memory_account_fallback(user_id: str) -> dict[str, Any] | None:
    state = auto_trader.get(user_id)
    snapshot = get_cached_account_snapshot(user_id)
    connected = bool(state.connected or snapshot.get("connected") is True)
    active_mode = state.active_mode or snapshot.get("mode")
    if not connected and active_mode is None:
        return None
    return add_stale_warning(
        build_success(
            {
                "connected": connected,
                "active_mode": active_mode,
                "mode": active_mode,
                "balance": snapshot.get("balance"),
                "currency": snapshot.get("currency"),
                "email": snapshot.get("email"),
            }
        )
    )


def memory_status_fallback(user_id: str) -> dict[str, Any] | None:
    payload = memory_account_fallback(user_id)
    if payload is None:
        return None
    data = payload.get("data")
    if not isinstance(data, dict):
        return None
    state = auto_trader.get(user_id)
    data["connection_status_source"] = state.connection_status_source or "memory"
    data["robot"] = build_robot_payload(
        state,
        connected=bool(data.get("connected")),
        active_mode=data.get("active_mode"),
        connection_checked_at=state.connection_checked_at.isoformat()
        if state.connection_checked_at is not None
        else None,
        connection_status_source=data["connection_status_source"],
    )["data"]
    return payload


def build_robot_payload(state: Any, **extra: Any) -> dict[str, Any]:
    data = strip_ai_fields(state.to_dict())
    data["status"] = normalize_robot_status(data.get("status"))
    data["account_mode"] = "REAL"
    data["allow_real"] = True
    data["confirm_real"] = True
    if str(data.get("active_mode") or "").strip().upper() == "DEMO":
        data["active_mode"] = "REAL"
    user_id = extra.pop("user_id", None)
    if user_id is not None:
        data["management"] = build_management_summary(str(user_id), state)
        data["management_stop_reason"] = data["management"]["stop_reason"]
        data["management_gross_profit"] = data["management"]["gross_profit"]
        data["management_gross_loss"] = data["management"]["gross_loss"]
        worker_task = robot_tasks.get(str(user_id))
        last_tick_at = robot_worker_last_tick_at.get(str(user_id))
        if (
            worker_task is not None
            and not worker_task.done()
            and last_tick_at is not None
            and (utc_now() - last_tick_at).total_seconds() > 30
            and data.get("enabled")
        ):
            logger.warning("[WORKER_STALE_RESTART] user_id=%s last_tick_at=%s", user_id, last_tick_at)
            worker_task.cancel()
            try:
                asyncio.get_running_loop()
                robot_tasks[str(user_id)] = asyncio.create_task(robot_worker(str(user_id)))
            except RuntimeError:
                robot_tasks.pop(str(user_id), None)
            worker_task = robot_tasks.get(str(user_id))
            last_tick_at = robot_worker_last_tick_at.get(str(user_id))
        data["worker_running"] = bool(worker_task is not None and not worker_task.done())
        data["worker_last_tick_at"] = last_tick_at.isoformat() if last_tick_at is not None else None
        has_locked_signal = bool(data.get("pending_signal") or data.get("best_candidate"))
        has_open_operation = bool(data.get("operation_in_progress") or data.get("result_waiting"))
        has_result_display = data.get("status") in {"WIN", "LOSS"} or (
            str(data.get("cycle_result") or "").upper() in {"WIN", "LOSS"}
            and data.get("result_display_until")
        )
        if data.get("enabled") and data["worker_running"] and data.get("status") in {
            STATUS_STOPPED,
        } and not (has_locked_signal or has_open_operation or has_result_display):
            data["status"] = STATUS_ANALYZING
    data.update(strip_ai_fields(extra))
    data["status"] = normalize_robot_status(data.get("status"))
    visible_result_status = str(data.get("status") or "").upper()
    visible_cycle_result = str(data.get("cycle_result") or "").upper()
    if data.get("operation_in_progress") or data.get("result_waiting"):
        data["status"] = STATUS_WAITING_RESULT
        data["analysis_message"] = None
        data["operation_message"] = data.get("operation_message") or "Operação aberta"
    elif data.get("status") in {STATUS_SENDING_ORDER, STATUS_SENDING_GALE_ORDER, STATUS_BUYING}:
        data["status"] = STATUS_BUYING
        data["analysis_message"] = None
        data["operation_message"] = data.get("operation_message") or "Executando ordem"
    elif visible_result_status in {"WIN", "LOSS"} or (
        visible_cycle_result in {"WIN", "LOSS"} and data.get("result_display_until")
    ):
        data["status"] = visible_result_status if visible_result_status in {"WIN", "LOSS"} else visible_cycle_result
        data["analysis_message"] = None
        data["operation_message"] = data.get("operation_message") or data["status"]
    elif data.get("status") == STATUS_WAITING_NEXT_CYCLE:
        data["pending_signal"] = None
        data["last_signal"] = None
        data["best_candidate"] = None
        data["cycle_best_candidate"] = None
        data["cycle_best_trade_candidate"] = None
        data["best_candidate_summary"] = None
        data["analysis_message"] = "Analisando mercado..."
        data["status_message"] = "Analisando diversos ativos"
    elif data.get("pending_signal"):
        data["status"] = STATUS_WAITING_ENTRY
        data["analysis_message"] = None
        data["status_message"] = data.get("status_message") or "Melhor ativo encontrado"
    elif data.get("best_candidate") and data.get("status") in {STATUS_ANALYZING, STATUS_SIGNAL_FOUND}:
        data["status"] = STATUS_SIGNAL_FOUND
        data["analysis_message"] = None
        data["status_message"] = data.get("status_message") or "Melhor ativo encontrado"
    data["account_mode"] = "REAL"
    data["allow_real"] = True
    data["confirm_real"] = True
    if str(data.get("active_mode") or "").strip().upper() == "DEMO":
        data["active_mode"] = "REAL"
    connected = bool(data.get("connected"))
    active_mode = str(data.get("active_mode") or "").strip().upper() or None
    if connected and active_mode == "REAL":
        data["connected"] = True
        data["active_mode"] = "REAL"
        data["session_status"] = "connected"
        data["session"] = "connected"
        if data.get("status") == STATUS_ACCOUNT_DISCONNECTED:
            data["status"] = STATUS_WAITING_NEXT_CYCLE if data.get("enabled") else STATUS_STOPPED
            data["rejection_reason"] = None
            data["last_rejection_reason"] = None
            data["operation_message"] = None
    else:
        data["session_status"] = "disconnected"
        data["session"] = "disconnected"
    if data.get("worker_running") and data.get("status") == STATUS_STOPPED:
        data["status"] = "RUNNING"
    if data.get("status") == STATUS_ACCOUNT_DISCONNECTED:
        data["connected"] = False
        data["enabled"] = False
        data["active_mode"] = None
        data["session_status"] = "disconnected"
        data["session"] = "disconnected"
        data["worker_running"] = False
        data["operation_in_progress"] = False
        data["result_waiting"] = False
        data["operation_message"] = "Conta BullEx desconectada"
        data["analysis_message"] = None
        data["display_countdown_label"] = None
        data["display_countdown_seconds"] = 0
        data["best_candidate_summary"] = None
        data["pending_signal"] = None
        data["last_signal"] = None
        data["last_trade"] = None
    if data.get("status") == STATUS_INSUFFICIENT_BALANCE:
        data["enabled"] = False
        data["worker_running"] = False
        data["operation_in_progress"] = False
        data["result_waiting"] = False
        data["analysis_message"] = None
        data["pending_signal"] = None
        data["operation_message"] = INSUFFICIENT_BALANCE_START_MESSAGE
        data["status_message"] = INSUFFICIENT_BALANCE_START_MESSAGE
        data["real_ready"] = False
    if data.get("status") == STATUS_BULLEX_ACTIVE_MODE_NOT_REAL:
        data["enabled"] = False
        data["worker_running"] = False
        data["operation_in_progress"] = False
        data["result_waiting"] = False
        data["analysis_message"] = None
        data["pending_signal"] = None
        data["operation_message"] = "Entre na conta REAL da BullEx para iniciar o robô."
        data["status_message"] = "Entre na conta REAL da BullEx para iniciar o robô."
    if data.get("status") == STATUS_BULLEX_ACTIVE_MODE_NOT_REAL:
        data["real_ready"] = False
    if is_stop_status(data.get("status")):
        data["enabled"] = False
        data["worker_running"] = False
        data["operation_in_progress"] = False
        data["result_waiting"] = False
    if data.get("operation_in_progress"):
        logger.info(
            "[EXPIRATION_COUNTDOWN] status=%s order_id=%s expiration_seconds=%s result_waiting=%s",
            data.get("status"),
            (data.get("last_trade") or {}).get("order_id"),
            data.get("expiration_seconds"),
            data.get("result_waiting"),
        )
        if data.get("result_waiting"):
            logger.info(
                "[RESULT_WAITING] order_id=%s",
                (data.get("last_trade") or {}).get("order_id"),
            )
    return build_success(data)


def build_robot_state_fallback_payload(user_id: str | None, exc: Exception) -> dict[str, Any]:
    state = auto_trader.get(str(user_id)) if user_id else None
    if state is not None:
        try:
            state.status = normalize_robot_status(getattr(state, "status", None))
            return build_robot_payload(
                state,
                user_id=user_id,
                real_ready=False,
                real_block_reason=exc.__class__.__name__,
            )
        except Exception:
            logger.info(
                "[ROBOT_STATE_FALLBACK_PAYLOAD] user_id=%s reason=minimal_payload",
                user_id,
                exc_info=True,
            )
    return build_success(
        {
            "status": STATUS_STOPPED,
            "enabled": False,
            "connected": False,
            "active_mode": None,
            "worker_running": False,
            "operation_in_progress": False,
            "result_waiting": False,
            "account_mode": "REAL",
            "allow_real": True,
            "confirm_real": True,
            "real_ready": False,
            "real_block_reason": exc.__class__.__name__,
        }
    )


def robot_config_locked(user_id: str, state: Any) -> bool:
    worker_task = robot_tasks.get(user_id)
    worker_running = bool(worker_task is not None and not worker_task.done())
    result_waiting = bool(
        getattr(state, "operation_in_progress", False)
        and str(((getattr(state, "last_trade", None) or {}).get("result") or "")).upper()
        not in {"WIN", "LOSS", "TIMEOUT"}
    )
    return bool(
        getattr(state, "enabled", False)
        or worker_running
        or getattr(state, "operation_in_progress", False)
        or result_waiting
    )


def recover_sync_timeout_if_needed(user_id: str) -> Any:
    recovered, state = auto_trader.recover_sync_timeout(user_id)
    if recovered:
        logger.warning(
            "[SYNC_TIMEOUT_RECOVERED] user_id=%s status=%s connected=%s enabled=%s",
            user_id,
            state.status,
            state.connected,
            state.enabled,
        )
        persist_robot(user_id)
    return state


def get_real_balance_warning(
    user_id: str | None,
    state: Any,
    active_mode: str | None,
    *,
    snapshot: dict[str, Any] | None = None,
) -> str | None:
    if user_id is None or getattr(state, "account_mode", None) != "REAL" or active_mode != "REAL":
        return None
    snapshot = snapshot if snapshot is not None else get_user_account_snapshot(user_id)
    if snapshot.get("balance") is None:
        return None
    balance = float(snapshot["balance"])
    if balance <= 0:
        logger.warning("[INSUFFICIENT_BALANCE_REAL] user_id=%s balance=%s", user_id, balance)
    return "BALANCE_ZERO" if balance <= 0 else None


async def stop_real_robot_for_insufficient_balance(
    user_id: str,
    *,
    balance: float | None,
    entry_value: float | None = None,
) -> Any:
    state = auto_trader.insufficient_balance(user_id)
    persist_robot(user_id)
    logger.warning(
        "[ROBOT_STOPPED_BALANCE_ZERO] user_id=%s balance=%s entry_value=%s",
        user_id,
        balance,
        entry_value if entry_value is not None else state.entry_value,
    )
    logger.warning(
        "[INSUFFICIENT_BALANCE_REAL] user_id=%s balance=%s entry_value=%s",
        user_id,
        balance,
        entry_value if entry_value is not None else state.entry_value,
    )
    await stop_robot_worker(user_id)
    return state


def recover_timed_out_analysis_if_needed(user_id: str) -> Any:
    recovered, state = auto_trader.recover_timed_out_analysis(user_id)
    if recovered:
        logger.warning("[ANALYSIS_TIMEOUT] user_id=%s", user_id)
        logger.info("[ANALYSIS_RECOVERED] user_id=%s reason=%s", user_id, state.rejection_reason)
        logger.info("[NEXT_CYCLE_SCHEDULED] user_id=%s next_cycle_at=%s", user_id, state.next_cycle_at)
        persist_robot(user_id)
    return state


def recover_running_analysis_if_needed(user_id: str, window: dict[str, Any]) -> tuple[str | None, Any]:
    reason, state = auto_trader.recover_running_analysis(user_id, window)
    if reason is None:
        return None, state
    if reason == "ANALYSIS_TIMEOUT":
        logger.warning("[ANALYSIS_TIMEOUT] user_id=%s", user_id)
    logger.info(
        "[WAITING_ANALYSIS_WINDOW] user_id=%s timeframe=%s current_candle_seconds=%s "
        "analysis_result=%s seconds_until_analysis_window=%s",
        user_id,
        state.timeframe,
        window["current_candle_seconds"],
        state.analysis_result,
        state.seconds_until_analysis_window,
    )
    logger.info("[ANALYSIS_STATE_RECOVERED] user_id=%s reason=%s", user_id, reason)
    persist_robot(user_id)
    return reason, state


def recover_analysis_error_to_window(
    user_id: str,
    error: Any,
    window: dict[str, Any] | None = None,
) -> Any:
    state = auto_trader.get(user_id)
    if window is None:
        window = get_entry_window(
            state.timeframe,
            utc_now().timestamp(),
            server_time_source="vps_fallback",
        )
        logger.warning(
            "[SERVER_TIME_FALLBACK] user_id=%s reason=analysis_recovery current_candle_seconds=%s",
            user_id,
            window["current_candle_seconds"],
        )
    friendly_error = readable_order_error(error)
    state = auto_trader.wait_analysis_window(
        user_id,
        window,
        clear_pending=True,
        analysis_result="ANALYSIS_ERROR",
        rejection_reason="ANALYSIS_ERROR",
        last_rejection_reason=friendly_error,
        force_next=True,
    )
    state.last_order_error = friendly_error
    logger.info(
        "[ANALYSIS_RECOVERED] user_id=%s error=%s next_cycle_at=%s",
        user_id,
        friendly_error,
        state.next_cycle_at,
    )
    return state


def readable_order_error(error: Any) -> str:
    raw_error = str(error or "ORDER_FAILED").strip() or "ORDER_FAILED"
    normalized = raw_error.lower().replace("_", " ").replace("-", " ")
    if "asset is not available" in normalized or "cannot purchase" in normalized:
        return "Ativo indisponivel no momento da compra"
    if "active suspended" in normalized or "ativo suspenso" in normalized:
        return "Ativo suspenso pela BullEx"
    if "payout" in normalized and any(term in normalized for term in ("low", "baixo", "minimum", "minimo")):
        return "Payout abaixo do minimo permitido"
    if "active not found" in normalized or "ativo nao encontrado" in normalized:
        return "Ativo nao encontrado na BullEx"
    if "account mismatch" in normalized or "mode mismatch" in normalized or "modo da conta" in normalized:
        return "Conta BullEx incompativel com o modo selecionado"
    return raw_error


def is_order_availability_error(error: Any) -> bool:
    normalized = str(error or "").strip().lower().replace("_", " ").replace("-", " ")
    return any(term in normalized for term in ORDER_AVAILABILITY_ERROR_TERMS)


def candidate_pre_order_block_reason(candidate: dict[str, Any]) -> str | None:
    symbol = normalize_binary_active(str(candidate.get("symbol") or ""))
    if not symbol or not is_binary_asset_allowed(symbol):
        return "ACTIVE_CLOSED"
    active_status = str(candidate.get("active_status") or candidate.get("status") or "").upper()
    blocked_filters = set(str(item) for item in (candidate.get("blocked_filters") or []))
    if candidate.get("suspended") or "SUSPEND" in active_status or "ACTIVE_SUSPENDED" in blocked_filters:
        return "ACTIVE_SUSPENDED"
    if candidate.get("is_open") is False or active_status in {"CLOSED", "INACTIVE"} or "ACTIVE_CLOSED" in blocked_filters:
        return "ACTIVE_CLOSED"
    return None


def order_attempt_candidates(state: Any, selected: dict[str, Any]) -> list[dict[str, Any]]:
    if state.gale_pending or bool(selected.get("is_gale")):
        return [dict(selected)]
    candidates = [dict(selected)]
    ranked_candidates = sorted(
        [candidate for candidate in state.candidates if isinstance(candidate, dict)],
        key=lambda item: (
            int(item.get("strategy_score") or item.get("score") or 0),
            int(item.get("confidence") or 0),
            float(item.get("payout") or 0),
        ),
        reverse=True,
    )
    candidates.extend(ranked_candidates)
    unique: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for candidate in candidates:
        symbol = normalize_binary_active(str(candidate.get("symbol") or ""))
        direction = str(candidate.get("signal") or candidate.get("direction") or "").upper()
        key = (symbol, direction)
        if key in seen:
            continue
        seen.add(key)
        normalized = {**candidate, "symbol": symbol, "signal": direction, "direction": direction}
        unique.append(normalized)
    return unique


def build_strategy_narration(candidate: dict[str, Any]) -> tuple[str, str, list[str]]:
    approved = set(candidate.get("approved_filters") or [])
    used: list[str] = []
    if "EMA_TREND" in approved or {"ema9", "ema21"}.issubset(candidate):
        used.append("EMA9/EMA21")
    if "RSI_RANGE" in approved or "rsi" in candidate:
        used.append("RSI")
    if "CANDLE_STRENGTH" in approved or "body_ratio" in candidate:
        used.append("Candle Force")
    if "WICK_REJECTION" in approved or "upper_wick_ratio" in candidate or "lower_wick_ratio" in candidate:
        used.append("Pavios")
    if "LAST_5_CONFIRMATION" in approved or "directional_candles_5" in candidate:
        used.append("Últimos Candles")
    if "VOLATILITY" in approved or "atr_pct" in candidate:
        used.append("Volatilidade")
    if candidate.get("payout") is not None:
        used.append("Payout")
    if not used:
        used = ["Score de Estratégias", "Payout"]

    strategy_name = "Confluência " + " + ".join(used)
    strategy_reason = str(candidate.get("reason") or candidate.get("signal_explanation") or "").strip()
    if not strategy_reason:
        symbol = normalize_binary_active(str(candidate.get("symbol") or ""))
        direction = str(candidate.get("direction") or candidate.get("signal") or "").strip().upper()
        confidence = int(candidate.get("confidence") or candidate.get("strategy_score") or candidate.get("score") or 0)
        payout = candidate.get("payout")
        direction_text = "CALL" if direction == "CALL" else "PUT" if direction == "PUT" else "direção definida pela estratégia"
        payout_text = f" payout de {float(payout):.0f}%" if payout is not None else " payout confirmado"
        strategy_reason = (
            f"{symbol or 'Ativo selecionado'} com entrada {direction_text}, "
            f"confiança {confidence} e{payout_text}. "
            f"Leitura baseada em {', '.join(used)}."
        )
    return strategy_name, strategy_reason, used


async def submit_bullex_order(
    user_id: str,
    endpoint: str,
    body: dict[str, Any],
) -> tuple[int, dict[str, Any]]:
    service_paths = {
        "/bullex/buy-demo": "/orders/buy-demo",
        "/bullex/buy-real": "/orders/buy-real",
    }
    return await call_bullex_service(
        "POST",
        service_paths[endpoint],
        user_id,
        json_body=body,
    )


def validate_buy_real_order_payload(body: dict[str, Any]) -> str | None:
    required = ("active", "action", "amount", "expiration", "confirm_real")
    missing = [field for field in required if field not in body or body.get(field) in {None, ""}]
    if missing:
        return "BUY_REAL_PAYLOAD_MISSING_" + "_".join(missing).upper()
    if str(body.get("action") or "").strip().lower() not in {"call", "put"}:
        return "BUY_REAL_PAYLOAD_INVALID_ACTION"
    try:
        amount = float(body.get("amount"))
    except (TypeError, ValueError):
        return "BUY_REAL_PAYLOAD_INVALID_AMOUNT"
    if amount <= 0:
        return "BUY_REAL_PAYLOAD_INVALID_AMOUNT"
    try:
        expiration = int(body.get("expiration"))
    except (TypeError, ValueError):
        return "BUY_REAL_PAYLOAD_INVALID_EXPIRATION"
    if expiration <= 0:
        return "BUY_REAL_PAYLOAD_INVALID_EXPIRATION"
    if body.get("confirm_real") is not True:
        return "BUY_REAL_PAYLOAD_CONFIRM_REAL_REQUIRED"
    return None


def is_order_result_path(path: str) -> bool:
    return path.startswith("/orders/") and path.endswith("/result")


def persist_robot(user_id: str) -> None:
    state_payload: dict[str, Any] | None = None
    last_trade: dict[str, Any] | None = None
    try:
        state = auto_trader.get(user_id)
        state.account_mode = "REAL"
        state.allow_real = True
        state.confirm_real = True
        state_payload = strip_ai_fields(state.to_dict())
        state_payload.update(
            {
                "account_mode": "REAL",
                "allow_real": True,
                "confirm_real": True,
            }
        )
        last_trade = state.last_trade
        robot_state_hydrated_users.add(user_id)
        restorable_robot_states[user_id] = deepcopy(state_payload)
    except Exception:
        logger.warning("[ROBOT_PERSISTENCE_WARNING] user_id=%s step=read_state", user_id, exc_info=True)
        return

    try:
        robot_persistence.save_state(user_id, state_payload)
    except Exception:
        logger.warning("[ROBOT_PERSISTENCE_WARNING] user_id=%s step=save_state", user_id, exc_info=True)

    try:
        save_settings = getattr(robot_persistence, "save_settings", None)
        if callable(save_settings):
            save_settings(user_id, state_payload)
    except Exception:
        logger.warning("[ROBOT_PERSISTENCE_WARNING] user_id=%s step=save_settings", user_id, exc_info=True)

    if last_trade:
        try:
            robot_persistence.save_trade(user_id, last_trade)
        except Exception:
            logger.warning("[ROBOT_PERSISTENCE_WARNING] user_id=%s step=save_trade", user_id, exc_info=True)


def robot_persistence_source() -> str:
    if robot_persistence.__class__.__name__ == "SupabaseRobotPersistence":
        return "supabase"
    return "memory"


def get_user_robot_state(user_id: str) -> Any:
    if user_id in robot_state_hydrated_users and auto_trader.has_state(user_id):
        state = auto_trader.get(user_id)
        state.account_mode = "REAL"
        state.allow_real = True
        state.confirm_real = True
        if state.active_mode is None or str(state.active_mode).strip().upper() == "DEMO":
            state.active_mode = "REAL"
        return state
    if auto_trader.has_state(user_id) and user_id not in restorable_robot_states:
        state = auto_trader.get(user_id)
        state.account_mode = "REAL"
        state.allow_real = True
        state.confirm_real = True
        if state.active_mode is None or str(state.active_mode).strip().upper() == "DEMO":
            state.active_mode = "REAL"
        robot_state_hydrated_users.add(user_id)
        persist_robot(user_id)
        return state
    robot_state_hydrated_users.discard(user_id)
    try:
        load_settings = getattr(robot_persistence, "load_settings", None)
        settings = load_settings(user_id) if callable(load_settings) else None
        payload = restorable_robot_states.get(user_id)
        if payload is None:
            payload = robot_persistence.load_state(user_id)
        if payload is not None:
            payload = {
                **payload,
                "enabled": False,
                "connected": False,
                "active_mode": None,
                "connection_checked_at": None,
                "connection_status_source": "restorable",
            }
            if settings is not None:
                payload = {**payload, **settings}
            payload["account_mode"] = "REAL"
            payload["allow_real"] = True
            payload["confirm_real"] = True
            if payload.get("active_mode") is None or str(payload.get("active_mode")).strip().upper() == "DEMO":
                payload["active_mode"] = "REAL"
            trades = robot_persistence.load_trades(user_id)
            state = auto_trader.restore(
                user_id,
                payload,
                trades,
                source=robot_persistence_source(),
            )
            robot_state_hydrated_users.add(user_id)
            logger.info("[USER_STATE_LOADED_NO_WORKER] user_id=%s source=%s", user_id, robot_persistence_source())
            return state
        if settings is not None:
            state = auto_trader.get(user_id)
            for field, value in settings.items():
                if field != "account_mode" and hasattr(state, field):
                    setattr(state, field, value)
            state.account_mode = "REAL"
            state.allow_real = True
            state.confirm_real = True
            if state.active_mode is None or str(state.active_mode).strip().upper() == "DEMO":
                state.active_mode = "REAL"
            state.enabled = False
            auto_trader.mark_source(user_id, robot_persistence_source())
            robot_state_hydrated_users.add(user_id)
            logger.info("[USER_STATE_LOADED_NO_WORKER] user_id=%s source=%s", user_id, robot_persistence_source())
            return state
    except Exception:
        logger.exception("[ROBOT USER LOAD ERROR] user_id=%s", user_id)
    robot_state_hydrated_users.add(user_id)
    state = auto_trader.get(user_id)
    state.account_mode = "REAL"
    state.allow_real = True
    state.confirm_real = True
    if state.active_mode is None or str(state.active_mode).strip().upper() == "DEMO":
        state.active_mode = "REAL"
    return state


async def analyze_active_signal(
    user_id: str,
    symbol: str,
    timeframe: str = "M1",
    endtime: int | None = None,
    strategy_mode: str = "conservative",
) -> tuple[int, dict[str, Any]]:
    cached_candles = cached_candles_for_active(user_id, symbol, timeframe, endtime=endtime)
    cached_payout = cached_payout_for_active(user_id, symbol)
    used_cache = bool(cached_candles or cached_payout is not None)
    if active_cooldown_remaining(user_id, symbol) is not None:
        if cached_candles:
            logger.info("[ACTIVE_CACHE] user_id=%s symbol=%s path=/candles reason=ACTIVE_COOLDOWN", user_id, symbol)
        else:
            logger.warning("[ACTIVE_SKIPPED] user_id=%s symbol=%s reason=ACTIVE_COOLDOWN", user_id, symbol)
            return 200, build_success(
                {
                    "symbol": symbol,
                    "signal": "WAIT",
                    "confidence": 0,
                    "trade_allowed": False,
                    "quality_reason": "ACTIVE_COOLDOWN",
                    "blocked_filters": ["ACTIVE_COOLDOWN"],
                    "approved_filters": [],
                }
            )
    if payout_cooldown_remaining(user_id, symbol) is not None:
        if cached_payout is not None:
            logger.info("[ACTIVE_CACHE] user_id=%s symbol=%s path=/payouts reason=PAYOUT_COOLDOWN", user_id, symbol)
        else:
            logger.warning("[ACTIVE_SKIPPED] user_id=%s symbol=%s reason=PAYOUT_COOLDOWN", user_id, symbol)
            return 200, build_success(
                {
                    "symbol": symbol,
                    "signal": "WAIT",
                    "confidence": 0,
                    "trade_allowed": False,
                    "quality_reason": "PAYOUT_COOLDOWN",
                    "blocked_filters": ["PAYOUT_COOLDOWN"],
                    "approved_filters": [],
                }
            )
    interval = TIMEFRAME_SECONDS[timeframe]
    candle_params: dict[str, Any] = {
        "active": symbol,
        "interval": interval,
        "count": ROBOT_CANDLE_COUNT,
    }
    if endtime is not None:
        candle_params["endtime"] = endtime
    payload = build_success(cached_candles) if cached_candles else None
    if payload is None:
        try:
            status_code, payload = await asyncio.wait_for(
                call_bullex_service(
                    "GET",
                    "/candles",
                    user_id,
                    params=candle_params,
                ),
                timeout=ACTIVE_DATA_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            cached_candles = cached_candles_for_active(user_id, symbol, timeframe, endtime=endtime)
            if cached_candles:
                logger.info("[ACTIVE_CACHE] user_id=%s symbol=%s path=/candles reason=CANDLES_TIMEOUT", user_id, symbol)
                used_cache = True
                payload = build_success(cached_candles)
            else:
                set_named_cooldown(
                    active_cooldowns,
                    user_id,
                    symbol,
                    seconds=ACTIVE_COOLDOWN_SECONDS,
                    log_label="ACTIVE_TIMEOUT",
                    status=STATUS_ACTIVE_COOLDOWN,
                    reason="CANDLES_TIMEOUT",
                )
                return 200, build_success(
                    {
                        "symbol": symbol,
                        "signal": "WAIT",
                        "confidence": 0,
                        "trade_allowed": False,
                        "quality_reason": "CANDLES_TIMEOUT",
                        "blocked_filters": ["CANDLES_TIMEOUT"],
                        "approved_filters": [],
                    }
                )
        log_ignored_disconnect(user_id, "/candles", payload)
        if not payload.get("ok"):
            if is_session_disconnected(payload):
                return 409, build_error(SESSION_DISCONNECTED)
            cached_candles = cached_candles_for_active(user_id, symbol, timeframe, endtime=endtime)
            if cached_candles:
                logger.info("[ACTIVE_CACHE] user_id=%s symbol=%s path=/candles reason=CANDLES_UNAVAILABLE", user_id, symbol)
                used_cache = True
                payload = build_success(cached_candles)
            else:
                set_named_cooldown(
                    active_cooldowns,
                    user_id,
                    symbol,
                    seconds=ACTIVE_COOLDOWN_SECONDS,
                    log_label="ACTIVE_SKIPPED",
                    status=STATUS_ACTIVE_COOLDOWN,
                    reason=str(payload.get("error") or "CANDLES_UNAVAILABLE"),
                )
                return status_code, payload

    payout_status = 200
    payout = cached_payout
    payout_payload = build_success([{"symbol": symbol, "payout": payout}]) if payout is not None else None
    if payout_payload is None:
        try:
            payout_status, payout_payload = await asyncio.wait_for(
                call_bullex_service(
                    "GET",
                    "/payouts",
                    user_id,
                    params={"active": symbol},
                ),
                timeout=ACTIVE_DATA_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            cached_payout = cached_payout_for_active(user_id, symbol)
            if cached_payout is not None:
                logger.info("[ACTIVE_CACHE] user_id=%s symbol=%s path=/payouts reason=PAYOUT_TIMEOUT", user_id, symbol)
                used_cache = True
                payout_status = 200
                payout_payload = build_success([{"symbol": symbol, "payout": cached_payout}])
            else:
                set_named_cooldown(
                    payout_cooldowns,
                    user_id,
                    symbol,
                    seconds=PAYOUT_COOLDOWN_SECONDS,
                    log_label="PAYOUT_TIMEOUT",
                    status=STATUS_PAYOUT_COOLDOWN,
                    reason="PAYOUT_TIMEOUT",
                )
                payout_status = 200
                payout_payload = build_success([])
        log_ignored_disconnect(user_id, "/payouts", payout_payload)
    payout = extract_payout(payout_payload, symbol) if payout_payload.get("ok") else None
    if payout is None and payout_status >= 400:
        set_named_cooldown(
            payout_cooldowns,
            user_id,
            symbol,
            seconds=PAYOUT_COOLDOWN_SECONDS,
            log_label="PAYOUT_COOLDOWN",
            status=STATUS_PAYOUT_COOLDOWN,
            reason="PAYOUT_COOLDOWN",
        )
    signal = analyze_signal(
        symbol,
        extract_candles(payload),
        timeframe=timeframe,
        strategy_mode=strategy_mode,
        payout=payout,
    )
    if used_cache:
        signal["from_cache"] = True
    if payout_status >= 400 and payout is None:
        signal["blocked_filters"] = list(signal.get("blocked_filters") or []) + ["PAYOUT_UNAVAILABLE"]
        signal["trade_allowed"] = False
        signal["quality_reason"] = LOW_QUALITY_SIGNAL
    logger.info("[ACTIVE_OK] user_id=%s symbol=%s payout=%s confidence=%s", user_id, symbol, payout, signal.get("confidence"))
    logger.info("[SIGNAL ANALYZE] %s %s %s", symbol, signal["signal"], signal["confidence"])
    return 200, build_success(signal)


async def scan_local_signals(
    user_id: str,
    limit: int = 5,
    include_wait: bool = False,
    timeframe: str = "M1",
    endtime: int | None = None,
    strategy_mode: str = "conservative",
    max_assets: int | None = None,
    asset_sleep_seconds: float = 0.0,
) -> tuple[int, dict[str, Any]]:
    logger.info("[SIGNAL SCAN START]")
    signals = []
    analysis_assets = select_analysis_assets_for_cycle(user_id, max_assets=max_assets)
    logger.info(
        "[ANALYSIS_FILTER] total_assets=%s filtered_assets=%s",
        len(BINARY_ALLOWED_ASSETS),
        len(analysis_assets),
    )
    logger.info("[ANALYSIS_FILTER_10_ASSETS] assets=%s", ",".join(analysis_assets))

    async def analyze_one(symbol: str) -> tuple[str, int, dict[str, Any]]:
        try:
            logger.info("[ANALYZING_ASSET] user_id=%s asset=%s", user_id, symbol)
            logger.info("[ANALYZING_ASSET] symbol=%s", symbol)
            status_code, payload = await asyncio.wait_for(
                analyze_active_signal(
                    user_id,
                    symbol,
                    timeframe=timeframe,
                    endtime=endtime,
                    strategy_mode=strategy_mode,
                ),
                timeout=(ACTIVE_DATA_TIMEOUT_SECONDS * 2) + 0.5,
            )
            return symbol, status_code, payload
        except asyncio.TimeoutError:
            cached_candles = cached_candles_for_active(user_id, symbol, timeframe, endtime=endtime)
            if cached_candles:
                cached_payout = cached_payout_for_active(user_id, symbol)
                cached_signal = analyze_signal(
                    symbol,
                    cached_candles,
                    timeframe=timeframe,
                    strategy_mode=strategy_mode,
                    payout=cached_payout,
                )
                cached_signal["from_cache"] = True
                logger.info("[ACTIVE_CACHE] user_id=%s symbol=%s path=/candles reason=ANALYSIS_TIMEOUT", user_id, symbol)
                return symbol, 200, build_success(cached_signal)
            logger.warning("[ACTIVE_TIMEOUT] user_id=%s symbol=%s phase=analysis", user_id, symbol)
            return symbol, 200, build_success(
                {
                    "symbol": symbol,
                    "signal": "WAIT",
                    "confidence": 0,
                    "trade_allowed": False,
                    "quality_reason": "ACTIVE_TIMEOUT",
                    "blocked_filters": ["ACTIVE_TIMEOUT"],
                    "approved_filters": [],
                    "_analysis_timeout": True,
                }
            )
        except Exception as exc:
            logger.warning("[ACTIVE_SKIPPED] user_id=%s symbol=%s reason=%s", user_id, symbol, exc.__class__.__name__)
            return symbol, 200, build_success(
                {
                    "symbol": symbol,
                    "signal": "WAIT",
                    "confidence": 0,
                    "trade_allowed": False,
                    "quality_reason": exc.__class__.__name__,
                    "blocked_filters": [exc.__class__.__name__],
                    "approved_filters": [],
                }
            )

    results = await asyncio.gather(*(analyze_one(symbol) for symbol in analysis_assets), return_exceptions=False)
    timed_out_symbols = [
        symbol
        for symbol, _status_code, payload in results
        if isinstance(payload, dict)
        and payload.get("ok")
        and isinstance(payload.get("data"), dict)
        and payload["data"].get("_analysis_timeout")
    ]
    all_assets_timed_out = bool(analysis_assets) and len(timed_out_symbols) == len(analysis_assets)
    if all_assets_timed_out:
        logger.warning(
            "[ANALYSIS_BATCH_TIMEOUT] user_id=%s assets=%s action=retry_next_tick",
            user_id,
            ",".join(timed_out_symbols),
        )
    else:
        for symbol in timed_out_symbols:
            set_named_cooldown(
                active_cooldowns,
                user_id,
                symbol,
                seconds=ACTIVE_COOLDOWN_SECONDS,
                log_label="ACTIVE_TIMEOUT",
                status=STATUS_ACTIVE_COOLDOWN,
                reason="ACTIVE_TIMEOUT",
            )

    for symbol, status_code, payload in results:
        try:
            if not payload.get("ok"):
                if is_session_disconnected(payload):
                    logger.warning("[SIGNAL ERROR] %s %s", symbol, payload.get("error"))
                    logger.warning("[ACTIVE_SKIPPED] user_id=%s symbol=%s reason=%s", user_id, symbol, payload.get("error"))
                    continue
                logger.warning("[SIGNAL ERROR] %s %s", symbol, payload.get("error"))
                logger.warning("[ACTIVE_SKIPPED] user_id=%s symbol=%s reason=%s", user_id, symbol, payload.get("error"))
                continue

            signal = payload["data"]
            logger.info(
                "[ASSET_SCORE] user_id=%s symbol=%s signal=%s confidence=%s score=%s payout=%s allowed=%s",
                user_id,
                symbol,
                signal.get("signal"),
                signal.get("confidence"),
                signal.get("strategy_score") or signal.get("score") or 0,
                signal.get("payout"),
                signal.get("trade_allowed"),
            )
            if signal["confidence"] < 70 and not include_wait:
                continue
            if (signal["signal"] == "WAIT" or not signal.get("trade_allowed", True)) and not include_wait:
                continue
            signals.append(signal)
        except Exception as exc:
            logger.warning("[ACTIVE_SKIPPED] user_id=%s symbol=%s reason=%s", user_id, symbol, exc.__class__.__name__)
            continue

    signals.sort(key=lambda item: item["confidence"], reverse=True)
    limited_signals = signals[:limit]
    logger.info("[SIGNAL SCAN RESULT] count=%s", len(limited_signals))
    return 200, build_success(limited_signals)


def simple_candle_direction(candles: list[dict[str, Any]]) -> str:
    prices: list[float] = []
    for candle in candles:
        value = candle.get("close", candle.get("close_price", candle.get("c")))
        try:
            prices.append(float(value))
        except (TypeError, ValueError):
            continue
    if len(prices) < 2:
        return "CALL"
    return "CALL" if prices[-1] >= prices[0] else "PUT"


def candidate_rank(candidate: dict[str, Any] | None) -> tuple[int, int, float]:
    if not isinstance(candidate, dict):
        return (-1, -1, -1.0)
    return (
        int(candidate.get("strategy_score") or candidate.get("score") or 0),
        int(candidate.get("confidence") or 0),
        float(candidate.get("payout") or 0),
    )


def candidate_meets_cycle_threshold(
    candidate: dict[str, Any] | None,
    state: Any,
    *,
    minimum_confidence: int,
) -> bool:
    if not isinstance(candidate, dict):
        return False
    if str(candidate.get("direction") or candidate.get("signal") or "").upper() not in {"CALL", "PUT"}:
        return False
    if candidate_pre_order_block_reason(candidate) is not None:
        return False
    try:
        payout = float(candidate.get("payout") or 0)
        confidence = int(candidate.get("confidence") or 0)
    except (TypeError, ValueError):
        return False
    return payout >= float(state.min_payout) and confidence >= minimum_confidence


def choose_better_candidate(
    current: dict[str, Any] | None,
    incoming: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if incoming is None:
        return current
    if current is None:
        return dict(incoming)
    return dict(incoming) if candidate_rank(incoming) > candidate_rank(current) else current


def resolve_cycle_entry_candidate(state: Any) -> dict[str, Any] | None:
    strict = state.cycle_best_trade_candidate
    if candidate_meets_cycle_threshold(strict, state, minimum_confidence=int(state.min_confidence)):
        return dict(strict)
    fallback = state.cycle_best_candidate
    if candidate_meets_cycle_threshold(fallback, state, minimum_confidence=70):
        candidate = dict(fallback)
        candidate["fallback_candidate_used"] = True
        return candidate
    return None


async def select_fallback_candidate(
    user_id: str,
    state: Any,
    *,
    endtime: int | None = None,
    max_assets: int | None = ROBOT_MAX_ASSETS_PER_CYCLE,
) -> dict[str, Any] | None:
    assets = select_analysis_assets_for_cycle(user_id, max_assets=max_assets)
    for index, symbol in enumerate(assets):
        if index > 0:
            await asyncio.sleep(ROBOT_ASSET_QUEUE_SLEEP_SECONDS)
        cached_candles = cached_candles_for_active(user_id, symbol, state.timeframe, endtime=endtime)
        cached_payout = cached_payout_for_active(user_id, symbol)
        if active_cooldown_remaining(user_id, symbol) is not None:
            if cached_candles:
                logger.info("[ACTIVE_CACHE] user_id=%s symbol=%s path=/candles reason=FALLBACK_ACTIVE_COOLDOWN", user_id, symbol)
            else:
                logger.warning("[ACTIVE_COOLDOWN] user_id=%s symbol=%s", user_id, symbol)
                continue
        if payout_cooldown_remaining(user_id, symbol) is not None:
            if cached_payout is not None:
                logger.info("[ACTIVE_CACHE] user_id=%s symbol=%s path=/payouts reason=FALLBACK_PAYOUT_COOLDOWN", user_id, symbol)
            else:
                logger.warning("[PAYOUT_COOLDOWN] user_id=%s symbol=%s", user_id, symbol)
                continue
        try:
            payout = cached_payout
            if payout is None:
                try:
                    payout_status, payout_payload = await asyncio.wait_for(
                        call_bullex_service(
                            "GET",
                            "/payouts",
                            user_id,
                            params={"active": symbol},
                        ),
                        timeout=ACTIVE_DATA_TIMEOUT_SECONDS,
                    )
                except asyncio.TimeoutError:
                    logger.warning("[ACTIVE_SKIPPED] user_id=%s symbol=%s reason=FALLBACK_PAYOUT_TIMEOUT", user_id, symbol)
                    continue
                log_ignored_disconnect(user_id, "/payouts", payout_payload)
                payout = (
                    extract_payout(payout_payload, symbol)
                    if payout_status < 400 and payout_payload.get("ok")
                    else None
                )
            if payout is None:
                set_named_cooldown(
                    payout_cooldowns,
                    user_id,
                    symbol,
                    seconds=PAYOUT_COOLDOWN_SECONDS,
                    log_label="PAYOUT_COOLDOWN",
                    status=STATUS_PAYOUT_COOLDOWN,
                    reason="PAYOUT_COOLDOWN",
                )
                continue

            candles = cached_candles
            if not candles:
                candle_params: dict[str, Any] = {
                    "active": symbol,
                    "interval": TIMEFRAME_SECONDS[state.timeframe],
                    "count": ROBOT_CANDLE_COUNT,
                }
                if endtime is not None:
                    candle_params["endtime"] = endtime
                try:
                    candle_status, candle_payload = await asyncio.wait_for(
                        call_bullex_service(
                            "GET",
                            "/candles",
                            user_id,
                            params=candle_params,
                        ),
                        timeout=ACTIVE_DATA_TIMEOUT_SECONDS,
                    )
                except asyncio.TimeoutError:
                    logger.warning("[ACTIVE_SKIPPED] user_id=%s symbol=%s reason=FALLBACK_CANDLES_TIMEOUT", user_id, symbol)
                    continue
                log_ignored_disconnect(user_id, "/candles", candle_payload)
                candles = extract_candles(candle_payload) if candle_status < 400 else []
            if not candles:
                continue
        except Exception as exc:
            logger.debug(
                "[FALLBACK_CANDIDATE_SKIPPED] user_id=%s symbol=%s error=%s",
                user_id,
                symbol,
                exc,
            )
            continue

        direction = simple_candle_direction(candles)
        candidate = {
            "symbol": symbol,
            "direction": direction,
            "signal": direction,
            "strategy_score": 70,
            "score": 70,
            "quality_score": 70,
            "confidence": 70,
            "payout": payout,
            "reason": "Fallback operacional pelo movimento simples das últimas velas.",
            "entry_reason": "Fallback operacional pelo movimento simples das últimas velas.",
            "candle_reading": "Leitura simplificada por fallback operacional.",
            "block_reasons": [],
            "metrics": {
                "symbol": symbol,
                "timeframe": state.timeframe,
                "candles_count": len(candles),
                "fallback": True,
            },
            "approved_filters": ["FALLBACK_OPEN_ASSET"],
            "blocked_filters": [],
            "trade_allowed": True,
            "strategy_mode": state.strategy_mode,
            "target_entry_second": state.buy_target_second,
            "entry_window_start_second": state.entry_window_start_second,
            "entry_window_end_second": state.entry_window_end_second,
        }
        strategy_name, strategy_reason, used_strategies = build_strategy_narration(candidate)
        candidate.update(
            {
                "strategy_name": strategy_name,
                "strategy_reason": strategy_reason,
                "used_strategies": used_strategies,
            }
        )
        logger.info(
            "[FALLBACK_CANDIDATE_SELECTED] user_id=%s symbol=%s direction=%s payout=%s",
            user_id,
            symbol,
            direction,
            payout,
        )
        return candidate
    return None


async def update_cycle_analysis(
    user_id: str,
    state: Any,
    entry_window: dict[str, Any],
    *,
    force: bool = False,
) -> Any:
    now = utc_now()
    if force:
        state.best_candidate = None
        state.cycle_best_candidate = None
        state.cycle_best_trade_candidate = None
        state.candidates = []
        state.candidates_count = 0
    if not force and state.last_analysis_at is not None:
        elapsed = (now - state.last_analysis_at).total_seconds()
        if elapsed < 3:
            return state

    logger.info(
        "[CYCLE_ANALYSIS] user_id=%s cycle_id=%s seconds_until_next_cycle=%s",
        user_id,
        state.cycle_id,
        state.to_dict()["seconds_until_next_cycle"],
    )
    logger.info("[WORKER_ANALYSIS_STARTED] user_id=%s cycle_id=%s", user_id, state.cycle_id)
    cycle_started_at = monotonic()
    scan_status, scan_payload = await scan_local_signals(
        user_id,
        limit=ROBOT_MAX_ASSETS_PER_CYCLE,
        include_wait=True,
        timeframe=state.timeframe,
        endtime=int(entry_window["server_timestamp"]),
        strategy_mode=state.strategy_mode,
        max_assets=ROBOT_MAX_ASSETS_PER_CYCLE,
        asset_sleep_seconds=ROBOT_ASSET_QUEUE_SLEEP_SECONDS,
    )
    if scan_payload.get("ok"):
        signals = [item for item in scan_payload.get("data", []) if isinstance(item, dict)]
    else:
        logger.warning(
            "[ANALYSIS_RECOVERED] user_id=%s error=%s action=FALLBACK_CANDIDATE",
            user_id,
            scan_payload.get("error") or "SIGNAL_SCAN_FAILED",
        )
        signals = []

    if not signals:
        fallback = await select_fallback_candidate(
            user_id,
            state,
            endtime=int(entry_window["server_timestamp"]),
        )
        signals = [fallback] if fallback is not None else []

    candidates: list[dict[str, Any]] = []
    for raw_signal in signals:
        symbol = normalize_binary_active(str(raw_signal.get("symbol") or ""))
        if not symbol:
            continue
        if active_cooldown_remaining(user_id, symbol) is not None and not raw_signal.get("from_cache"):
            logger.warning("[ACTIVE_COOLDOWN] user_id=%s symbol=%s", user_id, symbol)
            continue
        payout = raw_signal.get("payout")
        if payout is None and payout_cooldown_remaining(user_id, symbol) is not None:
            payout = cached_payout_for_active(user_id, symbol)
            if payout is not None:
                logger.info("[ACTIVE_CACHE] user_id=%s symbol=%s path=/payouts reason=CANDIDATE_PAYOUT_COOLDOWN", user_id, symbol)
            elif not raw_signal.get("from_cache"):
                logger.warning("[PAYOUT_COOLDOWN] user_id=%s symbol=%s", user_id, symbol)
                continue
        if payout is None and is_binary_asset_allowed(symbol):
            payout_status, payout_payload = await call_bullex_service(
                "GET",
                "/payouts",
                user_id,
                params={"active": symbol},
            )
            log_ignored_disconnect(user_id, "/payouts", payout_payload)
            payout = (
                extract_payout(payout_payload, symbol)
                if payout_status < 400 and payout_payload.get("ok")
                else None
            )
            if payout is None and payout_status >= 400:
                set_named_cooldown(
                    payout_cooldowns,
                    user_id,
                    symbol,
                    seconds=PAYOUT_COOLDOWN_SECONDS,
                    log_label="PAYOUT_COOLDOWN",
                    status=STATUS_PAYOUT_COOLDOWN,
                    reason="PAYOUT_COOLDOWN",
                )

        allowed, ranked, _ = apply_strategy_guard(
            user_id,
            state,
            {**raw_signal, "symbol": symbol},
            payout=payout,
        )
        active_status = str(raw_signal.get("active_status") or raw_signal.get("status") or "").upper()
        real_block = None
        if not is_binary_asset_allowed(symbol):
            real_block = "ACTIVE_CLOSED"
        elif raw_signal.get("suspended") or "SUSPEND" in active_status:
            real_block = "ACTIVE_SUSPENDED"
        elif raw_signal.get("is_open") is False or active_status in {"CLOSED", "INACTIVE"}:
            real_block = "ACTIVE_CLOSED"

        blocked_filters = list(ranked.get("blocked_filters") or [])
        if real_block and real_block not in blocked_filters:
            blocked_filters.append(real_block)
        direction = ranked.get("direction") or ranked.get("signal") or "WAIT"
        candidate = {
            **{
                key: ranked[key]
                for key in ANALYSIS_DETAIL_FIELDS
                if key in ranked
            },
            "symbol": symbol,
            "direction": direction,
            "signal": direction,
            "strategy_score": int(ranked.get("strategy_score") or 0),
            "score": int(ranked.get("strategy_score") or 0),
            "quality_score": int(ranked.get("quality_score") or 0),
            "confidence": int(ranked.get("confidence") or 0),
            "payout": payout,
            "reason": ranked.get("reason") or ranked.get("signal_explanation"),
            "entry_reason": ranked.get("entry_reason") or ranked.get("reason") or ranked.get("signal_explanation"),
            "candle_reading": ranked.get("candle_reading"),
            "block_reasons": list(ranked.get("block_reasons") or ranked.get("blocked_filters") or []),
            "metrics": dict(ranked.get("metrics") or {}),
            "approved_filters": list(ranked.get("approved_filters") or []),
            "blocked_filters": blocked_filters,
            "trade_allowed": bool(allowed and real_block is None and direction in {"CALL", "PUT"}),
            "strategy_mode": ranked.get("strategy_mode") or state.strategy_mode,
            "target_entry_second": state.buy_target_second,
            "entry_window_start_second": state.entry_window_start_second,
            "entry_window_end_second": state.entry_window_end_second,
        }
        strategy_name, strategy_reason, used_strategies = build_strategy_narration(candidate)
        candidate.update(
            {
                "strategy_name": strategy_name,
                "strategy_reason": strategy_reason,
                "used_strategies": used_strategies,
            }
        )
        candidates.append(candidate)
        logger.info(
            "[ASSET_SCORE] user_id=%s asset=%s score=%s payout=%s confidence=%s",
            user_id,
            symbol,
            candidate.get("strategy_score") or candidate.get("score") or 0,
            candidate.get("payout"),
            candidate.get("confidence"),
        )

    strict_candidates = [
        candidate
        for candidate in candidates
        if candidate_meets_cycle_threshold(
            candidate,
            state,
            minimum_confidence=int(state.min_confidence),
        )
    ]
    visible_candidates = [
        candidate
        for candidate in candidates
        if candidate_meets_cycle_threshold(candidate, state, minimum_confidence=70)
    ] or candidates
    current_best_candidate = max(visible_candidates, key=candidate_rank) if visible_candidates else None
    current_best_trade_candidate = max(strict_candidates, key=candidate_rank) if strict_candidates else None
    previous_symbol = (state.cycle_best_candidate or {}).get("symbol") if state.cycle_best_candidate else None
    state = auto_trader.set_analysis_candidates(
        user_id,
        candidates,
        current_best_candidate,
    )
    state.cycle_best_candidate = choose_better_candidate(state.cycle_best_candidate, current_best_candidate)
    state.cycle_best_trade_candidate = choose_better_candidate(
        state.cycle_best_trade_candidate,
        current_best_trade_candidate,
    )
    state.best_candidate = dict(state.cycle_best_candidate) if state.cycle_best_candidate is not None else None
    if state.cycle_best_candidate is not None:
        state.strategy_score = int((state.cycle_best_candidate or {}).get("strategy_score") or 0)
        state.strategy_name = (state.cycle_best_candidate or {}).get("strategy_name")
        state.strategy_reason = (state.cycle_best_candidate or {}).get("strategy_reason")
        state.used_strategies = list((state.cycle_best_candidate or {}).get("used_strategies") or [])
        state.candle_reading = (state.cycle_best_candidate or {}).get("candle_reading")
        state.entry_reason = (state.cycle_best_candidate or {}).get("entry_reason")
        state.block_reasons = list(
            (state.cycle_best_candidate or {}).get("block_reasons")
            or (state.cycle_best_candidate or {}).get("blocked_filters")
            or []
        )
        state.metrics = dict((state.cycle_best_candidate or {}).get("metrics") or {})
    if state.cycle_best_candidate is not None and state.cycle_best_candidate.get("symbol") != previous_symbol:
        logger.info(
            "[BEST_CANDIDATE] user_id=%s cycle_id=%s symbol=%s direction=%s confidence=%s payout=%s fallback=%s",
            user_id,
            state.cycle_id,
            state.cycle_best_candidate.get("symbol"),
            state.cycle_best_candidate.get("direction"),
            state.cycle_best_candidate.get("confidence"),
            state.cycle_best_candidate.get("payout"),
            not bool(state.cycle_best_trade_candidate),
        )
        logger.info(
            "[BEST_CANDIDATE] user_id=%s asset=%s direction=%s confidence=%s payout=%s",
            user_id,
            state.cycle_best_candidate.get("symbol"),
            state.cycle_best_candidate.get("direction"),
            state.cycle_best_candidate.get("confidence"),
            state.cycle_best_candidate.get("payout"),
        )
    if state.cycle_best_candidate is None:
        logger.info("[NO_SIGNAL_FOUND] user_id=%s reason=NO_CANDIDATES", user_id)
    logger.info(
        "[WORKER_ANALYSIS_FINISHED] user_id=%s cycle_id=%s candidates=%s",
        user_id,
        state.cycle_id,
        len(candidates),
    )
    logger.info(
        "[CYCLE_DURATION_MS] user_id=%s cycle_id=%s phase=analysis ms=%s",
        user_id,
        state.cycle_id,
        int((monotonic() - cycle_started_at) * 1000),
    )
    return state


async def fetch_trade_result(user_id: str, order_id: str) -> tuple[int, dict[str, Any]]:
    status_code, payload = await call_bullex_service("GET", f"/orders/{order_id}/result", user_id)
    mark_disconnected_from_payload(user_id, payload)
    return status_code, payload


def reset_cycle_after_finish(user_id: str) -> Any:
    logger.info("[CYCLE_RESET_STARTED] user_id=%s", user_id)
    state = auto_trader.reset_cycle_after_result(user_id)
    logger.info(
        "[CYCLE_RESET_DONE] user_id=%s cycle_id=%s next_cycle_at=%s",
        user_id,
        state.cycle_id,
        state.next_cycle_at,
    )
    logger.info("[NEXT_CYCLE_SCHEDULED] user_id=%s next_cycle_at=%s", user_id, state.next_cycle_at)
    logger.info(
        "[WAITING_NEXT_CYCLE] user_id=%s cycle_id=%s next_cycle_at=%s",
        user_id,
        state.cycle_id,
        state.next_cycle_at,
    )
    logger.info("[READY_NEXT_CYCLE] user_id=%s cycle_id=%s", user_id, state.cycle_id)
    persist_robot(user_id)
    return state


def reset_cycle_after_result(user_id: str) -> Any:
    return reset_cycle_after_finish(user_id)


def result_display_expired(state: Any) -> bool:
    if getattr(state, "result_display_until", None) is None:
        return False
    if str(getattr(state, "status", "") or "").upper() not in {"WIN", "LOSS", "RESULT_RECEIVED", "GALE_RESULT_RECEIVED"}:
        return False
    return utc_now() >= state.result_display_until


def waiting_result_stale(state: Any) -> bool:
    if str(getattr(state, "status", "") or "").upper() != STATUS_WAITING_RESULT:
        return False
    trade_result = str(((getattr(state, "last_trade", None) or {}).get("result")) or "").upper()
    if getattr(state, "operation_in_progress", False) or trade_result not in {"", "WIN", "LOSS", "TIMEOUT"}:
        return False
    base = getattr(state, "last_entry_at", None) or getattr(state, "current_cycle_started_at", None)
    return base is not None and (utc_now() - base).total_seconds() > 90


async def finish_monitored_trade(user_id: str, order_id: str, result: str, profit: float) -> None:
    should_pause_worker = False
    async with auto_trader.result_lock(user_id):
        async with auto_trader.cycle_lock(user_id):
            finalized, state = auto_trader.finish_trade(user_id, order_id, result, profit)
        if not finalized and state.gale_pending:
            logger.info(
                "[TRADE_RESULT] user_id=%s order_id=%s result=LOSS profit=%s",
                user_id,
                order_id,
                (state.last_trade or {}).get("profit"),
            )
            logger.info(
                "[GALE_TRIGGERED] user_id=%s order_id=%s gale_amount=%s multiplier=%s",
                user_id,
                order_id,
                state.gale_amount,
                state.martingale_multiplier,
            )
            logger.info(
                "[TRADE_LOSS_GALE_TRIGGERED] user_id=%s order_id=%s gale_amount=%s multiplier=%s",
                user_id,
                order_id,
                state.gale_amount,
                state.martingale_multiplier,
            )
            logger.info(
                "[WAITING_GALE_ENTRY] user_id=%s order_id=%s active=%s direction=%s",
                user_id,
                order_id,
                (state.pending_signal or {}).get("symbol"),
                state.gale_direction,
            )
            if state.last_trade:
                try:
                    robot_persistence.save_trade_history(user_id, state.last_trade)
                    logger.info(
                        "[HISTORY_SAVED] user_id=%s order_id=%s result=%s final_result=%s",
                        user_id,
                        order_id,
                        state.last_trade.get("result"),
                        state.last_trade.get("final_result"),
                    )
                except Exception:
                    logger.exception(
                        "[ROBOT HISTORY ERROR] user_id=%s order_id=%s",
                        user_id,
                        order_id,
                    )
        if finalized and state.last_trade:
            cycle_profit = float(state.last_trade.get("profit") or 0)
            if state.last_trade.get("is_gale"):
                cycle_profit = round(
                    float((state.gale_parent_trade or {}).get("profit") or 0) + cycle_profit,
                    2,
                )
            logger.info(
                "[RESULT_RECEIVED] user_id=%s order_id=%s result=%s profit=%s",
                user_id,
                order_id,
                result,
                state.last_trade.get("profit"),
            )
            logger.info(
                "[CYCLE_RESULT_%s] user_id=%s order_id=%s profit=%s",
                str(state.cycle_result or result).upper(),
                user_id,
                order_id,
                state.profit,
            )
            logger.info(
                "[STATE] RESULT_%s user_id=%s order_id=%s",
                str(result).upper(),
                user_id,
                order_id,
            )
            logger.info(
                "[STATE] SHOW_RESULT user_id=%s order_id=%s result=%s result_display_until=%s",
                user_id,
                order_id,
                result,
                state.result_display_until,
            )
            logger.info(
                "[TRADE_RESULT] user_id=%s order_id=%s result=%s profit=%s",
                user_id,
                order_id,
                result,
                state.last_trade.get("profit"),
            )
            logger.info(
                "[ORDER_RESULT] user_id=%s order_id=%s result=%s profit=%s",
                user_id,
                order_id,
                result,
                state.last_trade.get("profit"),
            )
            if state.last_trade.get("is_gale"):
                logger.info(
                    "[GALE_RESULT] user_id=%s order_id=%s parent_order_id=%s result=%s profit=%s",
                    user_id,
                    order_id,
                    state.last_trade.get("parent_order_id"),
                    result,
                    state.last_trade.get("profit"),
                )
            if state.cycle_result == "LOSS":
                logger.info(
                    "[LOSS_COUNTED] user_id=%s order_id=%s losses=%s profit=%s",
                    user_id,
                    order_id,
                    state.losses,
                    state.profit,
                )
            try:
                robot_persistence.save_trade_history(user_id, state.last_trade)
                logger.info(
                    "[HISTORY_SAVED] user_id=%s order_id=%s result=%s final_result=%s",
                    user_id,
                    order_id,
                    state.last_trade.get("result"),
                    state.last_trade.get("final_result"),
                )
            except Exception:
                logger.exception(
                    "[ROBOT HISTORY ERROR] user_id=%s order_id=%s",
                    user_id,
                    order_id,
                )
            logger.info(
                "[CYCLE_FINAL_RESULT] user_id=%s order_id=%s cycle_result=%s final_result=%s cycle_profit=%s",
                user_id,
                order_id,
                state.cycle_result,
                state.last_trade.get("final_result"),
                cycle_profit,
            )
            logger.info(
                "[SCORE_UPDATED] user_id=%s wins=%s losses=%s profit=%s cycle_result=%s",
                user_id,
                state.wins,
                state.losses,
                state.profit,
                state.cycle_result,
            )
            logger.info(
                "[RESULT_DISPLAY_UNTIL] user_id=%s result_display_until=%s",
                user_id,
                state.result_display_until,
            )
            if state.last_trade.get("is_gale"):
                logger.info(
                    "[%s] user_id=%s order_id=%s parent_order_id=%s",
                    state.cycle_result,
                    user_id,
                    order_id,
                    state.last_trade.get("parent_order_id"),
                )
            elif result == "LOSS" and not state.martingale_enabled:
                logger.info("[GALE_DISABLED_LOSS_FINAL] user_id=%s order_id=%s", user_id, order_id)
            if state.status in {STATUS_STOP_WIN_HIT, STATUS_STOP_LOSS_HIT}:
                should_pause_worker = True
                if state.status == STATUS_STOP_WIN_HIT:
                    logger.warning("[STOP_WIN_HIT] user_id=%s profit=%s", user_id, state.profit)
                else:
                    logger.warning("[STOP_LOSS_HIT] user_id=%s profit=%s", user_id, state.profit)
                logger.warning("[ROBOT_PAUSED_BY_STOP] user_id=%s reason=%s", user_id, state.status)
        persist_robot(user_id)
    if should_pause_worker:
        await stop_robot_worker(user_id)


async def timeout_monitored_trade(user_id: str, order_id: str) -> None:
    async with auto_trader.lock(user_id):
        auto_trader.timeout_trade(user_id, order_id)
        persist_robot(user_id)


trade_result_monitor = TradeResultMonitor(
    fetch_result=fetch_trade_result,
    finish_trade=finish_monitored_trade,
    timeout_trade=timeout_monitored_trade,
)


async def execute_robot_cycle(
    user_id: str,
    *,
    required_mode: str | None = None,
) -> tuple[int, dict[str, Any]]:
    async with auto_trader.cycle_lock(user_id):
        state = recover_sync_timeout_if_needed(user_id)
        if result_display_expired(state) or waiting_result_stale(state):
            state = reset_cycle_after_result(user_id)
        initial_stop_reason = daily_stop_reason(user_id, state) or robot_stop_reason(state)
        if initial_stop_reason in {STATUS_STOP_WIN_HIT, STATUS_STOP_LOSS_HIT}:
            state = await pause_robot_by_stop(user_id, initial_stop_reason)
            return 200, build_robot_payload(state, user_id=user_id)
        had_pending_signal = state.pending_signal is not None
        running_analysis = state.analysis_result == "RUNNING" or state.last_analysis_result == "RUNNING"
        if not had_pending_signal and not running_analysis:
            can_run, state = auto_trader.prepare_cycle(user_id)
            if not can_run:
                if state.status == STATUS_WAITING_NEXT_CYCLE and state.enabled:
                    logger.info(
                        "[WAITING_NEXT_CYCLE] user_id=%s cycle_id=%s next_cycle_at=%s seconds=%s",
                        user_id,
                        state.cycle_id,
                        state.next_cycle_at,
                        state.to_dict()["seconds_until_next_cycle"],
                    )
                    logger.info(
                        "[ANALYSIS_SKIPPED_NEXT_CYCLE] user_id=%s cycle_id=%s",
                        user_id,
                        state.cycle_id,
                    )
                    return 200, build_robot_payload(state, user_id=user_id)
                if state.status != STATUS_WAITING_NEXT_CYCLE or not state.enabled:
                    return 200, build_robot_payload(state)
            elif state.to_dict()["seconds_until_next_cycle"] <= 0:
                logger.info(
                    "[CYCLE_START] user_id=%s cycle_id=%s current_cycle_started_at=%s",
                    user_id,
                    state.cycle_id,
                    state.current_cycle_started_at,
                )

        logger.info("[ROBOT TICK] user_id=%s", user_id)
        try:
            result_waiting = bool(
                state.operation_in_progress
                and str((state.last_trade or {}).get("result") or "").upper() not in {"WIN", "LOSS", "TIMEOUT"}
            )
            if state.operation_in_progress or result_waiting:
                state.status = STATUS_PENDING_GALE_RESULT if state.gale_active else STATUS_WAITING_RESULT
                logger.info("[STATE] WAITING_RESULT user_id=%s order_id=%s", user_id, (state.last_trade or {}).get("order_id"))
                logger.info("[RESULT_WAIT_ONLY] user_id=%s status=%s", user_id, state.status)
                return 200, build_robot_payload(state)

            active_stop_reason = daily_stop_reason(user_id, state) or robot_stop_reason(state)
            if active_stop_reason is not None:
                if active_stop_reason in {STATUS_STOP_WIN_HIT, STATUS_STOP_LOSS_HIT}:
                    state = await pause_robot_by_stop(user_id, active_stop_reason)
                else:
                    if active_stop_reason.startswith("DAILY_STOP"):
                        state.enabled = False
                        logger.warning("[DAILY_STOP_HIT] user_id=%s reason=%s", user_id, active_stop_reason)
                    state = auto_trader.reject(user_id, active_stop_reason)
                    logger.info("[ROBOT SIGNAL REJECTED] user_id=%s reason=%s", user_id, active_stop_reason)
                return 200, build_robot_payload(state, user_id=user_id)

            if required_mode is not None and state.account_mode != required_mode:
                return 409, build_error(f"ACCOUNT_MODE_NOT_{required_mode}")

            if state.account_mode == "REAL":
                account_snapshot = get_cached_account_snapshot(user_id)
                snapshot_mode = account_snapshot.get("mode") or state.active_mode
                raw_balance = account_snapshot.get("balance")
                real_balance = float(raw_balance) if snapshot_mode == "REAL" and raw_balance is not None else None
                if real_balance is not None and real_balance <= 0:
                    state = await stop_real_robot_for_insufficient_balance(
                        user_id,
                        balance=real_balance,
                    )
                    return 200, build_robot_payload(
                        state,
                        user_id=user_id,
                        balance=real_balance,
                        balance_real=real_balance,
                    )
                if real_balance is not None and float(state.entry_value) > real_balance:
                    state = await stop_real_robot_for_insufficient_balance(
                        user_id,
                        balance=real_balance,
                        entry_value=float(state.entry_value),
                    )
                    return 200, build_robot_payload(
                        state,
                        user_id=user_id,
                        balance=real_balance,
                        balance_real=real_balance,
                        operation_message=ENTRY_VALUE_EXCEEDS_BALANCE_MESSAGE,
                        status_message=ENTRY_VALUE_EXCEEDS_BALANCE_MESSAGE,
                    )

            status_code, account_payload, entry_window = await refresh_entry_window(user_id, state)
            state, connected, active_mode, connection_source = await reconcile_robot_connection_from_payload(
                user_id,
                account_payload,
            )
            if robot_connection_unavailable(connected, active_mode):
                state = auto_trader.disconnect_account(user_id)
                logger.warning("[ROBOT_BLOCKED_ACCOUNT_DISCONNECTED] user_id=%s", user_id)
                return 200, build_robot_payload(
                    state,
                    connected=False,
                    active_mode=None,
                    connection_checked_at=state.connection_checked_at.isoformat()
                    if state.connection_checked_at is not None
                    else None,
                    connection_status_source="disconnected",
                )
            if state.account_mode == "REAL":
                account_snapshot = get_cached_account_snapshot(user_id)
                snapshot_mode = account_snapshot.get("mode") or active_mode
                raw_balance = account_snapshot.get("balance")
                real_balance = float(raw_balance) if snapshot_mode == "REAL" and raw_balance is not None else None
                if active_mode == "REAL" and real_balance is not None and real_balance <= 0:
                    state = await stop_real_robot_for_insufficient_balance(
                        user_id,
                        balance=real_balance,
                    )
                    return 200, build_robot_payload(
                        state,
                        user_id=user_id,
                        balance=real_balance,
                        balance_real=real_balance,
                    )
                if active_mode == "REAL" and real_balance is not None and float(state.entry_value) > real_balance:
                    state = await stop_real_robot_for_insufficient_balance(
                        user_id,
                        balance=real_balance,
                        entry_value=float(state.entry_value),
                    )
                    return 200, build_robot_payload(
                        state,
                        user_id=user_id,
                        balance=real_balance,
                        balance_real=real_balance,
                        operation_message=ENTRY_VALUE_EXCEEDS_BALANCE_MESSAGE,
                        status_message=ENTRY_VALUE_EXCEEDS_BALANCE_MESSAGE,
                    )
                logger.info(
                    "[REAL MODE DETECTED] user_id=%s active_mode=%s connected=%s confirm_real=%s",
                    user_id,
                    active_mode,
                    connected,
                    state.confirm_real,
                )
                logger.info(
                    "[REAL BUY ATTEMPT] user_id=%s entry_value=%s",
                    user_id,
                    state.entry_value,
                )
                block_reason = real_block_reason(
                    state,
                    connected=connected,
                    active_mode=active_mode,
                    user_id=user_id,
                )
                if block_reason is not None:
                    auto_trader.lock_real(user_id, block_reason)
                    persist_robot(user_id)
                    logger.warning(
                        "[REAL BUY BLOCKED reason=%s] user_id=%s",
                        block_reason,
                        user_id,
                    )
                    return 403, build_error(block_reason)

            expected_bullex_mode = "REAL"
            if active_mode != expected_bullex_mode:
                state = auto_trader.reject(user_id, f"ACCOUNT_MODE_MUST_BE_{expected_bullex_mode}")
                logger.info(
                    "[ROBOT SIGNAL REJECTED] user_id=%s reason=ACCOUNT_MODE_MUST_BE_%s",
                    user_id,
                    expected_bullex_mode,
                )
                return 200, build_robot_payload(state)
            if entry_window is None:
                state = recover_analysis_error_to_window(user_id, "SERVER_TIME_UNAVAILABLE")
                return status_code, build_robot_payload(state)
            recovered_reason, state = recover_running_analysis_if_needed(user_id, entry_window)
            if recovered_reason is not None:
                return 200, build_robot_payload(state)
            selected = dict(state.pending_signal) if state.pending_signal else None
            if selected is not None and not state.operation_in_progress:
                state.status = STATUS_WAITING_GALE_ENTRY if state.gale_pending else STATUS_WAITING_ENTRY
                state.rejection_reason = None
            seconds_until_next_cycle = state.to_dict()["seconds_until_next_cycle"]
            if (
                selected is None
                and state.enabled
                and state.status == STATUS_WAITING_NEXT_CYCLE
                and not state.operation_in_progress
            ):
                if seconds_until_next_cycle > 0:
                    logger.info(
                        "[ENTRY_WINDOW] user_id=%s cycle_id=%s next_cycle_at=%s phase=waiting_next_cycle",
                        user_id,
                        state.cycle_id,
                        state.next_cycle_at,
                    )
                    return 200, build_robot_payload(state)

                logger.info(
                    "[CYCLE_END] user_id=%s cycle_id=%s phase=selection",
                    user_id,
                    state.cycle_id,
                )
                state = await update_cycle_analysis(user_id, state, entry_window, force=True)
                selected = resolve_cycle_entry_candidate(state)
                if selected is None:
                    logger.info(
                        "[NO_TRADE] user_id=%s cycle_id=%s best_candidate=%s confidence=%s payout=%s",
                        user_id,
                        state.cycle_id,
                        (state.cycle_best_candidate or {}).get("symbol"),
                        (state.cycle_best_candidate or {}).get("confidence"),
                        (state.cycle_best_candidate or {}).get("payout"),
                    )
                    logger.info("[NO_SIGNAL_FOUND] user_id=%s reason=NO_TRADE", user_id)
                    state = auto_trader.wait_analysis_window(
                        user_id,
                        entry_window,
                        clear_pending=True,
                        analysis_result="NO_CANDIDATE_THIS_CANDLE",
                        last_rejection_reason="NO_TRADE",
                        force_next=True,
                    )
                    state.analysis_message = "Analisando mercado..."
                    logger.info(
                        "[NEXT_ANALYSIS_RETRY_SCHEDULED] user_id=%s cycle_id=%s reason=NO_TRADE next_cycle_at=%s",
                        user_id,
                        state.cycle_id,
                        state.next_cycle_at,
                    )
                    return 200, build_robot_payload(state)
                logger.info(
                    "[BEST_CANDIDATE] user_id=%s cycle_id=%s symbol=%s direction=%s confidence=%s payout=%s fallback=%s",
                    user_id,
                    state.cycle_id,
                    selected.get("symbol"),
                    selected.get("direction") or selected.get("signal"),
                    selected.get("confidence"),
                    selected.get("payout"),
                    bool(selected.get("fallback_candidate_used")),
                )
                logger.info(
                    "[BEST_CANDIDATE_FOUND] user_id=%s cycle_id=%s symbol=%s direction=%s confidence=%s payout=%s",
                    user_id,
                    state.cycle_id,
                    selected.get("symbol"),
                    selected.get("direction") or selected.get("signal"),
                    selected.get("confidence"),
                    selected.get("payout"),
                )
                state = auto_trader.set_pending_signal(user_id, selected)
                if selected.get("fallback_candidate_used"):
                    state.fallback_candidate_used = True
                selected = dict(state.pending_signal or {})
                logger.info(
                    "[SIGNAL_FOUND] user_id=%s cycle_id=%s symbol=%s direction=%s confidence=%s payout=%s",
                    user_id,
                    state.cycle_id,
                    selected.get("symbol"),
                    selected.get("signal") or selected.get("direction"),
                    selected.get("confidence"),
                    selected.get("payout"),
                )
                logger.info("[STATE] SIGNAL_FOUND user_id=%s cycle_id=%s", user_id, state.cycle_id)
                logger.info(
                    "[SIGNAL_PREPARED] user_id=%s cycle_id=%s symbol=%s direction=%s seconds_until_entry=%s",
                    user_id,
                    state.cycle_id,
                    selected.get("symbol"),
                    selected.get("signal") or selected.get("direction"),
                    state.seconds_until_entry_window,
                )
            if selected is None and False:
                if not entry_window["analysis_window_open"]:
                    state = auto_trader.wait_analysis_window(user_id, entry_window)
                    logger.info(
                        "[WAITING_ANALYSIS_WINDOW] user_id=%s timeframe=%s "
                        "current_candle_seconds=%s analysis_window_start=%s "
                        "analysis_window_end=%s seconds_until_analysis_window=%s",
                        user_id,
                        state.timeframe,
                        entry_window["current_candle_seconds"],
                        entry_window["analysis_window_start_second"],
                        entry_window["analysis_window_end_second"],
                        entry_window["seconds_until_analysis_window"],
                    )
                    logger.info(
                        "[NEXT_CYCLE_SCHEDULED] user_id=%s next_cycle_at=%s",
                        user_id,
                        state.next_cycle_at,
                    )
                    return 200, build_robot_payload(state)
                logger.info(
                    "[ANALYSIS_WINDOW_OPEN] user_id=%s timeframe=%s "
                    "current_candle_seconds=%s analysis_window_start=%s analysis_window_end=%s",
                    user_id,
                    state.timeframe,
                    entry_window["current_candle_seconds"],
                    entry_window["analysis_window_start_second"],
                    entry_window["analysis_window_end_second"],
                )
                state = auto_trader.start_analysis(user_id)
                if state.status != STATUS_ANALYZING:
                    state = auto_trader.wait_analysis_window(user_id, entry_window)
                    return 200, build_robot_payload(state)
                logger.info(
                    "[ANALYSIS_STARTED] user_id=%s cycle_id=%s",
                    user_id,
                    state.cycle_id,
                )
                logger.info("[STATE] ANALYZING user_id=%s cycle_id=%s", user_id, state.cycle_id)
                logger.info(
                    "[WORKER_ANALYSIS_STARTED] user_id=%s cycle_id=%s",
                    user_id,
                    state.cycle_id,
                )
                scan_status, scan_payload = await scan_local_signals(
                    user_id,
                    limit=len(ANALYSIS_ASSETS),
                    include_wait=True,
                    timeframe=state.timeframe,
                    endtime=int(entry_window["server_timestamp"]),
                    strategy_mode=state.strategy_mode,
                )
                scan_error = None
                if not scan_payload.get("ok"):
                    scan_error = str(scan_payload.get("error") or "SIGNAL_SCAN_FAILED")
                    logger.warning(
                        "[ANALYSIS_RECOVERED] user_id=%s error=%s action=FALLBACK_CANDIDATE",
                        user_id,
                        scan_error,
                    )
                    signals = []
                else:
                    signals = [item for item in scan_payload.get("data", []) if isinstance(item, dict)]
                logger.info(
                    "[ANALYSIS_FINISHED] user_id=%s cycle_id=%s candidates=%s",
                    user_id,
                    state.cycle_id,
                    len(signals),
                )
                if not signals:
                    fallback = await select_fallback_candidate(
                        user_id,
                        state,
                        endtime=int(entry_window["server_timestamp"]),
                    )
                    if fallback is not None:
                        signals = [fallback]
                    else:
                        state = auto_trader.wait_analysis_window(
                            user_id,
                            entry_window,
                            clear_pending=True,
                            analysis_result="NO_CANDIDATE_THIS_CANDLE",
                            last_rejection_reason="CANDLES_UNAVAILABLE",
                            force_next=True,
                        )
                        if scan_error is not None:
                            state.last_order_error = readable_order_error(scan_error)
                        auto_trader.set_analysis_candidates(user_id, [], None)
                        logger.info(
                            "[NO_CANDIDATE_THIS_CANDLE] user_id=%s reason=%s",
                            user_id,
                            state.last_rejection_reason,
                        )
                        logger.info(
                            "[NO_SIGNAL_FOUND] user_id=%s cycle_id=%s reason=%s",
                            user_id,
                            state.cycle_id,
                            state.last_rejection_reason,
                        )
                        return 200, build_robot_payload(state)

                candidates: list[dict[str, Any]] = []
                for raw_signal in signals:
                    symbol = normalize_binary_active(str(raw_signal.get("symbol") or ""))
                    payout = raw_signal.get("payout")
                    if payout is None and is_binary_asset_allowed(symbol):
                        payout_status, payout_payload = await call_bullex_service(
                            "GET",
                            "/payouts",
                            user_id,
                            params={"active": symbol},
                        )
                        log_ignored_disconnect(user_id, "/payouts", payout_payload)
                        payout = (
                            extract_payout(payout_payload, symbol)
                            if payout_status < 400 and payout_payload.get("ok")
                            else None
                        )

                    allowed, ranked, _ = apply_strategy_guard(
                        user_id,
                        state,
                        {**raw_signal, "symbol": symbol},
                        payout=payout,
                    )
                    if not is_binary_asset_allowed(symbol):
                        allowed = False
                        ranked["trade_allowed"] = False
                        ranked["blocked_filters"] = list(
                            dict.fromkeys(
                                [*ranked.get("blocked_filters", []), "ACTIVE_CLOSED"]
                            )
                        )
                        ranked["quality_reason"] = "ACTIVE_CLOSED"
                    active_status = str(
                        raw_signal.get("active_status")
                        or raw_signal.get("status")
                        or ""
                    ).upper()
                    if raw_signal.get("suspended") or "SUSPEND" in active_status:
                        allowed = False
                        ranked["trade_allowed"] = False
                        ranked["blocked_filters"] = list(
                            dict.fromkeys(
                                [*ranked.get("blocked_filters", []), "ACTIVE_CLOSED"]
                            )
                        )
                        ranked["quality_reason"] = "ACTIVE_CLOSED"
                    elif raw_signal.get("is_open") is False or active_status in {
                        "CLOSED",
                        "INACTIVE",
                    }:
                        allowed = False
                        ranked["trade_allowed"] = False
                        ranked["blocked_filters"] = list(
                            dict.fromkeys(
                                [*ranked.get("blocked_filters", []), "ACTIVE_CLOSED"]
                            )
                        )
                        ranked["quality_reason"] = "ACTIVE_CLOSED"
                    candidate = {
                        **{
                            key: ranked[key]
                            for key in ANALYSIS_DETAIL_FIELDS
                            if key in ranked
                        },
                        "symbol": symbol,
                        "direction": ranked.get("direction") or ranked.get("signal") or "WAIT",
                        "signal": ranked.get("direction") or ranked.get("signal") or "WAIT",
                        "strategy_score": int(ranked.get("strategy_score") or 0),
                        "score": int(ranked.get("strategy_score") or 0),
                        "quality_score": int(ranked.get("quality_score") or 0),
                        "confidence": int(ranked.get("confidence") or 0),
                        "payout": payout,
                        "reason": ranked.get("reason") or ranked.get("signal_explanation"),
                        "entry_reason": ranked.get("entry_reason")
                        or ranked.get("reason")
                        or ranked.get("signal_explanation"),
                        "candle_reading": ranked.get("candle_reading"),
                        "block_reasons": list(ranked.get("block_reasons") or ranked.get("blocked_filters") or []),
                        "metrics": dict(ranked.get("metrics") or {}),
                        "approved_filters": list(ranked.get("approved_filters") or []),
                        "blocked_filters": list(ranked.get("blocked_filters") or []),
                        "trade_allowed": bool(allowed and ranked.get("trade_allowed")),
                        "strategy_mode": ranked.get("strategy_mode") or state.strategy_mode,
                        "target_entry_second": state.buy_target_second,
                        "entry_window_start_second": state.entry_window_start_second,
                        "entry_window_end_second": state.entry_window_end_second,
                    }
                    strategy_name, strategy_reason, used_strategies = (
                        build_strategy_narration(candidate)
                    )
                    candidate.update(
                        {
                            "strategy_name": strategy_name,
                            "strategy_reason": strategy_reason,
                            "used_strategies": used_strategies,
                        }
                    )
                    candidates.append(candidate)

                approved_candidates = [
                    candidate
                    for candidate in candidates
                    if candidate["trade_allowed"]
                    and candidate.get("payout") is not None
                    and candidate.get("direction") in {"CALL", "PUT"}
                ]
                selected = (
                    max(
                        approved_candidates,
                        key=lambda item: (
                            int(item["strategy_score"]),
                            int(item["confidence"]),
                            float(item.get("payout") or 0),
                        ),
                    )
                    if approved_candidates
                    else None
                )
                auto_trader.set_analysis_candidates(user_id, candidates, selected)
                logger.info(
                    "[ANALYSIS_CANDIDATES] user_id=%s cycle_id=%s count=%s candidates=%s",
                    user_id,
                    state.cycle_id,
                    len(candidates),
                    candidates,
                )

                if selected is None:
                    fallback = await select_fallback_candidate(
                        user_id,
                        state,
                        endtime=int(entry_window["server_timestamp"]),
                    )
                    if fallback is not None:
                        candidates.append(fallback)
                        selected = fallback
                        auto_trader.set_analysis_candidates(user_id, candidates, selected)

                if selected is None:
                    highest_rejected = max(
                        candidates,
                        key=lambda item: (
                            int(item["strategy_score"]),
                            int(item["confidence"]),
                        ),
                        default=None,
                    )
                    blocked_reasons = list((highest_rejected or {}).get("blocked_filters", []))
                    critical_reasons = [
                        reason
                        for reason in (
                            "ACTIVE_CLOSED",
                            "CANDLES_UNAVAILABLE",
                            STATUS_ACCOUNT_DISCONNECTED,
                            "STOP_WIN_HIT",
                            "STOP_LOSS_HIT",
                            "OPERATION_IN_PROGRESS",
                        )
                        if reason in blocked_reasons
                    ]
                    if "PAYOUT_UNAVAILABLE" in blocked_reasons:
                        critical_reasons.append("ACTIVE_CLOSED")
                    last_rejection_reason = (
                        critical_reasons[0]
                        if critical_reasons
                        else "CANDLES_UNAVAILABLE"
                    )
                    state = auto_trader.wait_analysis_window(
                        user_id,
                        entry_window,
                        clear_pending=True,
                        analysis_result="NO_CANDIDATE_THIS_CANDLE",
                        last_rejection_reason=last_rejection_reason,
                        force_next=True,
                    )
                    state.blocked_filters = list((highest_rejected or {}).get("blocked_filters") or [])
                    state.approved_filters = list((highest_rejected or {}).get("approved_filters") or [])
                    state.quality_score = int((highest_rejected or {}).get("quality_score") or 0)
                    auto_trader.set_analysis_candidates(user_id, candidates, None)
                    logger.info(
                        "[NO_CANDIDATE_THIS_CANDLE] user_id=%s reason=%s candidates=%s",
                        user_id,
                        last_rejection_reason,
                        len(candidates),
                    )
                    logger.info(
                        "[NO_SIGNAL_FOUND] user_id=%s cycle_id=%s reason=%s candidates=%s",
                        user_id,
                        state.cycle_id,
                        last_rejection_reason,
                        len(candidates),
                    )
                    return 200, build_robot_payload(state)

                logger.info(
                    "[BEST_CANDIDATE] user_id=%s cycle_id=%s symbol=%s direction=%s confidence=%s payout=%s score=%s",
                    user_id,
                    state.cycle_id,
                    selected["symbol"],
                    selected["direction"],
                    selected["confidence"],
                    selected["payout"],
                    selected["strategy_score"],
                )
                logger.info(
                    "[BEST_CANDIDATE_FOUND] user_id=%s cycle_id=%s symbol=%s direction=%s confidence=%s payout=%s",
                    user_id,
                    state.cycle_id,
                    selected["symbol"],
                    selected["direction"],
                    selected["confidence"],
                    selected["payout"],
                )
                logger.info(
                    "[BEST_CANDIDATE_SELECTED] user_id=%s symbol=%s direction=%s strategy_score=%s confidence=%s payout=%s strategy_name=%s strategy_reason=%s used_strategies=%s",
                    user_id,
                    selected["symbol"],
                    selected["direction"],
                    selected["strategy_score"],
                    selected["confidence"],
                    selected["payout"],
                    selected["strategy_name"],
                    selected["strategy_reason"],
                    selected["used_strategies"],
                )

                state = auto_trader.set_pending_signal(user_id, selected)
                selected = dict(state.pending_signal or {})
                logger.info(
                    "[SIGNAL_FOUND] user_id=%s cycle_id=%s symbol=%s direction=%s confidence=%s payout=%s",
                    user_id,
                    state.cycle_id,
                    selected.get("symbol"),
                    selected.get("signal") or selected.get("direction"),
                    selected.get("confidence"),
                    selected.get("payout"),
                )
                logger.info("[STATE] SIGNAL_FOUND user_id=%s cycle_id=%s", user_id, state.cycle_id)
                logger.info(
                    "[SIGNAL_PREPARED] user_id=%s cycle_id=%s symbol=%s direction=%s seconds_until_entry=%s",
                    user_id,
                    state.cycle_id,
                    selected.get("symbol"),
                    selected.get("signal") or selected.get("direction"),
                    state.seconds_until_entry_window,
                )
                logger.info(
                    "[CYCLE_FINISHED_SIGNAL_LOCKED] user_id=%s symbol=%s signal=%s confidence=%s "
                    "payout=%s timeframe=%s",
                    user_id,
                    selected.get("symbol"),
                    selected.get("signal"),
                    selected.get("confidence"),
                    selected.get("payout"),
                    selected.get("timeframe"),
                )

            if selected is not None and not entry_window["entry_window_open"]:
                if entry_window["missed_entry_window"]:
                    target_timestamp = selected.get("target_entry_timestamp")
                    if target_timestamp is None:
                        target_timestamp = (
                            float(entry_window["server_timestamp"])
                            + float(entry_window["seconds_until_entry_window"])
                        )
                        selected["target_entry_timestamp"] = target_timestamp
                        selected["entry_target"] = "NEXT_CANDLE_OPEN"
                        selected["target_entry_second"] = int(entry_window["buy_target_second"])
                        state.pending_signal = dict(selected)
                        state.last_signal = dict(selected)
                        state.best_candidate = dict(selected)
                        state.cycle_best_candidate = dict(selected)
                        state.cycle_best_trade_candidate = dict(selected)
                        state.status = STATUS_WAITING_ENTRY
                        state.rejection_reason = None
                        state.seconds_until_entry_window = int(entry_window["seconds_until_entry_window"])
                        logger.info(
                            "[ENTRY_SCHEDULED_NEXT_CANDLE] user_id=%s cycle_id=%s symbol=%s seconds_until_entry=%s target_entry_timestamp=%s",
                            user_id,
                            state.cycle_id,
                            selected.get("symbol"),
                            state.seconds_until_entry_window,
                            target_timestamp,
                        )
                        logger.info(
                            "[ENTRY_SCHEDULED] user_id=%s cycle_id=%s symbol=%s seconds_until_entry=%s target=NEXT_CANDLE_OPEN",
                            user_id,
                            state.cycle_id,
                            selected.get("symbol"),
                            state.seconds_until_entry_window,
                        )
                        logger.info("[STATE] WAITING_ENTRY user_id=%s cycle_id=%s", user_id, state.cycle_id)
                    elif float(entry_window["server_timestamp"]) > (
                        float(target_timestamp) + float(entry_window["entry_window_end_second"])
                    ):
                        logger.warning(
                            "[ENTRY_WINDOW_MISSED] user_id=%s symbol=%s server_time=%s "
                            "timeframe=%s current_candle_seconds=%s window_end=%s",
                            user_id,
                            selected.get("symbol"),
                            entry_window["server_time"],
                            state.timeframe,
                            entry_window["current_candle_seconds"],
                            entry_window["entry_window_end_second"],
                        )
                        state = auto_trader.expire_pending_signal(
                            user_id,
                            reason="ENTRY_WINDOW_MISSED",
                            wait_seconds=max(1, int(entry_window["seconds_until_entry_window"])),
                        )
                        logger.warning(
                            "[SIGNAL_EXPIRED] user_id=%s cycle_id=%s reason=ENTRY_WINDOW_MISSED",
                            user_id,
                            state.cycle_id,
                        )
                        return 200, build_robot_payload(state, user_id=user_id)
                    else:
                        state.status = STATUS_WAITING_ENTRY
                        state.rejection_reason = None
                        state.seconds_until_entry_window = int(entry_window["seconds_until_entry_window"])
                waiting_log = "[WAITING_GALE_ENTRY]" if state.gale_pending else "[WAITING_NEXT_CANDLE_ENTRY]"
                logger.info("[STATE] WAITING_ENTRY user_id=%s cycle_id=%s", user_id, state.cycle_id)
                logger.info(
                    "%s user_id=%s symbol=%s server_time=%s timeframe=%s current_candle_seconds=%s "
                    "seconds_until_entry=%s window_start=%s window_end=%s",
                    waiting_log,
                    user_id,
                    selected.get("symbol"),
                    entry_window["server_time"],
                    state.timeframe,
                    entry_window["current_candle_seconds"],
                    entry_window["seconds_until_entry_window"],
                    entry_window["entry_window_start_second"],
                    entry_window["entry_window_end_second"],
                )
                logger.info(
                    "[ENTRY_WINDOW] user_id=%s cycle_id=%s symbol=%s seconds_until_entry=%s window_start=%s window_end=%s",
                    user_id,
                    state.cycle_id,
                    selected.get("symbol"),
                    entry_window["seconds_until_entry_window"],
                    entry_window["entry_window_start_second"],
                    entry_window["entry_window_end_second"],
                )
                return 200, build_robot_payload(state)

            logger.info(
                "[ENTRY_COUNTDOWN_ZERO] user_id=%s cycle_id=%s symbol=%s current_candle_seconds=%s",
                user_id,
                state.cycle_id,
                selected.get("symbol") if isinstance(selected, dict) else None,
                entry_window["current_candle_seconds"],
            )
            logger.info(
                "[NEXT_CANDLE_ENTRY_WINDOW_OPEN] user_id=%s server_time=%s timeframe=%s "
                "seconds_in_candle=%s window_start=%s window_end=%s buy_target_second=%s",
                user_id,
                entry_window["server_time"],
                state.timeframe,
                entry_window["current_candle_seconds"],
                entry_window["entry_window_start_second"],
                entry_window["entry_window_end_second"],
                entry_window["buy_target_second"],
            )
            logger.info(
                "[ENTRY_WINDOW] user_id=%s cycle_id=%s symbol=%s state=OPEN current_candle_seconds=%s window_start=%s window_end=%s",
                user_id,
                state.cycle_id,
                selected.get("symbol") if isinstance(selected, dict) else None,
                entry_window["current_candle_seconds"],
                entry_window["entry_window_start_second"],
                entry_window["entry_window_end_second"],
            )
            if not state.enabled:
                state.status = STATUS_STOPPED
                state.pending_signal = None
                state.gale_pending = False
                state.gale_active = False
                return 200, build_robot_payload(state)
            order_path = "/bullex/buy-real"

            skipped_candidates = 0
            last_order_status = 409
            last_order_reason = "NO_AVAILABLE_ASSET"
            last_friendly_error = NO_AVAILABLE_ASSET_ERROR
            attempted_unavailable = False
            for candidate in order_attempt_candidates(state, selected):
                is_gale_order = bool(state.gale_pending or candidate.get("is_gale"))
                if not state.enabled:
                    state.status = STATUS_STOPPED
                    state.pending_signal = None
                    state.gale_pending = False
                    state.gale_active = False
                    return 200, build_robot_payload(state)
                if state.order_attempts >= MAX_ORDER_ATTEMPTS_PER_CYCLE:
                    break
                validation_reason = candidate_pre_order_block_reason(candidate)
                if validation_reason is None:
                    payout = candidate.get("payout")
                    try:
                        payout_value = float(payout) if payout is not None else None
                    except (TypeError, ValueError):
                        payout_value = None
                    if payout_value is not None and payout_value < float(state.min_payout):
                        validation_reason = "PAYOUT_TOO_LOW"
                stop_reason = daily_stop_reason(user_id, state) or robot_stop_reason(state)
                if stop_reason in {STATUS_STOP_WIN_HIT, STATUS_STOP_LOSS_HIT}:
                    state = await pause_robot_by_stop(user_id, stop_reason)
                    return 200, build_robot_payload(state)
                if validation_reason is not None:
                    logger.info(
                        "[ENTRY_BLOCKED] user_id=%s reason=%s asset=%s",
                        user_id,
                        validation_reason,
                        candidate.get("symbol"),
                    )
                    if validation_reason == "PAYOUT_TOO_LOW":
                        state = auto_trader.reject_strategy(
                            user_id,
                            "SIGNAL_REJECTED",
                            last_rejection_reason="PAYOUT_TOO_LOW",
                            blocked_filters=["PAYOUT_TOO_LOW"],
                        )
                        state.last_order_error = "PAYOUT_TOO_LOW"
                        state.pending_signal = None
                        logger.warning(
                            "[SIGNAL_REJECTED] user_id=%s symbol=%s reason=PAYOUT_TOO_LOW",
                            user_id,
                            candidate.get("symbol"),
                        )
                        return 200, build_robot_payload(state, user_id=user_id)
                    skipped_candidates += 1
                    logger.warning(
                        "[ORDER_FALLBACK_NEXT_CANDIDATE] user_id=%s skipped_active=%s reason=%s attempts=%s",
                        user_id,
                        candidate.get("symbol"),
                        validation_reason,
                        state.order_attempts,
                    )
                    continue

                next_attempt = state.order_attempts + 1
                state = auto_trader.set_order_attempt(user_id, candidate, next_attempt)
                selected = dict(state.pending_signal or candidate)
                symbol = str(selected["symbol"])
                direction = str(selected.get("signal") or selected.get("direction"))
                payout = selected.get("payout")
                order_amount = state.gale_amount if is_gale_order else state.entry_value
                logger.info(
                    "[ENTRY_ALLOWED] user_id=%s cycle_id=%s symbol=%s direction=%s amount=%s payout=%s",
                    user_id,
                    state.cycle_id,
                    symbol,
                    direction,
                    order_amount,
                    payout,
                )
                order_body = {
                    "active": symbol,
                    "action": direction.lower(),
                    "amount": order_amount,
                    "expiration": entry_window["expiration_minutes"],
                }
                if state.account_mode == "REAL":
                    order_body["confirm_real"] = True
                    payload_reason = validate_buy_real_order_payload(order_body)
                    if payload_reason is not None:
                        logger.error(
                            "[BUY_REAL_PAYLOAD_INVALID] user_id=%s reason=%s payload=%s",
                            user_id,
                            payload_reason,
                            strip_ai_fields(order_body),
                        )
                        state.last_order_error = payload_reason
                        state = reset_cycle_after_finish(user_id)
                        logger.info(
                            "[NEXT_CYCLE_SCHEDULED] user_id=%s next_cycle_at=%s",
                            user_id,
                            state.next_cycle_at,
                        )
                        return 200, build_robot_payload(state, user_id=user_id)
                    logger.info("[BUY_REAL_PAYLOAD] user_id=%s payload=%s", user_id, strip_ai_fields(order_body))
                    logger.info(
                        "[REAL BUY ATTEMPT] user_id=%s active=%s direction=%s amount=%s expiration=%s",
                        user_id,
                        symbol,
                        direction,
                        order_amount,
                        entry_window["expiration_minutes"],
                    )

                logger.info(
                    "[ENTRY_ALLOWED] user_id=%s asset=%s direction=%s amount=%s payout=%s confidence=%s",
                    user_id,
                    symbol,
                    direction,
                    order_amount,
                    payout,
                    selected.get("confidence"),
                )
                state = auto_trader.start_sending_order(user_id)
                logger.info("[STATE] BUYING user_id=%s cycle_id=%s symbol=%s direction=%s", user_id, state.cycle_id, symbol, direction)
                logger.info(
                    "[%s] user_id=%s symbol=%s direction=%s",
                    "SENDING_GALE_ORDER" if is_gale_order else "BUYING",
                    user_id,
                    symbol,
                    direction,
                )
                logger.info(
                    "[ORDER_ATTEMPT] user_id=%s cycle_id=%s path=%s active=%s direction=%s amount=%s expiration=%s attempt=%s gale=%s",
                    user_id,
                    state.cycle_id,
                    order_path,
                    symbol,
                    direction,
                    order_amount,
                    entry_window["expiration_minutes"],
                    state.order_attempts,
                    is_gale_order,
                )
                try:
                    logger.info(
                        "[ORDER_SENT] user_id=%s asset=%s direction=%s amount=%s",
                        user_id,
                        symbol,
                        direction,
                        order_amount,
                    )
                    order_status, order_payload = await submit_bullex_order(
                        user_id,
                        order_path,
                        order_body,
                    )
                except Exception as exc:
                    reason = str(exc).strip() or type(exc).__name__
                    friendly_error = readable_order_error(reason)
                    last_order_status = 502
                    last_order_reason = reason
                    last_friendly_error = friendly_error
                    logger.exception("[ORDER_SEND_FAILED] user_id=%s active=%s error=%s", user_id, symbol, reason)
                    if is_order_availability_error(reason):
                        attempted_unavailable = True
                        set_named_cooldown(
                            active_cooldowns,
                            user_id,
                            symbol,
                            seconds=ACTIVE_COOLDOWN_SECONDS,
                            log_label="ACTIVE_COOLDOWN",
                            status=STATUS_ACTIVE_COOLDOWN,
                            reason="ACTIVE_COOLDOWN",
                        )
                        logger.info(
                            "[ORDER_FALLBACK_NEXT_CANDIDATE] user_id=%s failed_active=%s attempts=%s",
                            user_id,
                            symbol,
                            state.order_attempts,
                        )
                        continue
                    state = auto_trader.reject_order(
                        user_id,
                        reason,
                        last_order_error=friendly_error,
                    )
                    logger.error(
                        "[ORDER_REJECTED] user_id=%s reason=%s last_order_error=%s",
                        user_id,
                        reason,
                        friendly_error,
                    )
                    logger.info(
                        "[NEXT_CYCLE_SCHEDULED] user_id=%s next_cycle_at=%s",
                        user_id,
                        state.next_cycle_at,
                    )
                    if state.account_mode == "REAL":
                        persist_robot(user_id)
                        logger.warning("[REAL BUY BLOCKED reason=%s] user_id=%s", reason, user_id)
                        return 502, build_error(reason)
                    return 502, build_robot_payload(state)
                mark_disconnected_from_payload(user_id, order_payload)
                if not order_payload.get("ok"):
                    reason = str(order_payload.get("error") or "ORDER_FAILED")
                    friendly_error = readable_order_error(reason)
                    last_order_status = order_status
                    last_order_reason = reason
                    last_friendly_error = friendly_error
                    logger.error("[ORDER_SEND_FAILED] user_id=%s active=%s error=%s", user_id, symbol, reason)
                    if is_order_availability_error(reason):
                        attempted_unavailable = True
                        set_named_cooldown(
                            active_cooldowns,
                            user_id,
                            symbol,
                            seconds=ACTIVE_COOLDOWN_SECONDS,
                            log_label="ACTIVE_COOLDOWN",
                            status=STATUS_ACTIVE_COOLDOWN,
                            reason="ACTIVE_COOLDOWN",
                        )
                        logger.info(
                            "[ORDER_FALLBACK_NEXT_CANDIDATE] user_id=%s failed_active=%s attempts=%s",
                            user_id,
                            symbol,
                            state.order_attempts,
                        )
                        continue
                    state = auto_trader.reject_order(
                        user_id,
                        reason,
                        last_order_error=friendly_error,
                    )
                    logger.error(
                        "[ORDER_REJECTED] user_id=%s reason=%s last_order_error=%s",
                        user_id,
                        reason,
                        friendly_error,
                    )
                    logger.info(
                        "[NEXT_CYCLE_SCHEDULED] user_id=%s next_cycle_at=%s",
                        user_id,
                        state.next_cycle_at,
                    )
                    if state.account_mode == "REAL":
                        persist_robot(user_id)
                        logger.warning("[REAL BUY BLOCKED reason=%s] user_id=%s", reason, user_id)
                        return order_status, build_error(reason)
                    return order_status, build_robot_payload(state)

                order_data = order_payload.get("data") if isinstance(order_payload.get("data"), dict) else {}
                order_id = order_data.get("order_id")
                if order_id is None or not str(order_id).strip():
                    state = auto_trader.reject_order(
                        user_id,
                        "ORDER_ID_MISSING",
                        last_order_error="BullEx nao retornou o identificador da ordem",
                    )
                    logger.error("[ORDER_SEND_FAILED] user_id=%s active=%s error=ORDER_ID_MISSING", user_id, symbol)
                    logger.error(
                        "[ORDER_REJECTED] user_id=%s reason=ORDER_ID_MISSING last_order_error=%s",
                        user_id,
                        state.last_order_error,
                    )
                    logger.info(
                        "[NEXT_CYCLE_SCHEDULED] user_id=%s next_cycle_at=%s",
                        user_id,
                        state.next_cycle_at,
                    )
                    if state.account_mode == "REAL":
                        persist_robot(user_id)
                        logger.warning("[REAL BUY BLOCKED reason=ORDER_ID_MISSING] user_id=%s", user_id)
                        return 502, build_error("ORDER_ID_MISSING")
                    return 502, build_robot_payload(state)
                if state.account_mode == "REAL":
                    logger.info("[REAL BUY SUCCESS order_id=%s] user_id=%s", order_id, user_id)
                logger.info(
                    "[ORDER_ACCEPTED] user_id=%s cycle_id=%s order_id=%s symbol=%s direction=%s",
                    user_id,
                    state.cycle_id,
                    order_id,
                    symbol,
                    direction,
                )
                logger.info(
                    "[ENTRY_SENT] user_id=%s cycle_id=%s order_id=%s symbol=%s direction=%s amount=%s payout=%s",
                    user_id,
                    state.cycle_id,
                    order_id,
                    symbol,
                    direction,
                    order_amount,
                    payout,
                )

                sent_at = datetime.now(timezone.utc)
                expiration_window = entry_window
                try:
                    fresh_timestamp = estimate_state_server_timestamp(state)
                    if fresh_timestamp is None:
                        fresh_status, fresh_payload = await call_bullex_service("GET", "/sessions/status", user_id)
                        fresh_timestamp = extract_server_timestamp(fresh_payload)
                    else:
                        fresh_status = 200
                    if fresh_status < 500 and fresh_timestamp is not None:
                        expiration_window = get_entry_window(state.timeframe, fresh_timestamp, server_time_source="bullex")
                except Exception as exc:
                    logger.warning(
                        "[EXPIRATION_SERVER_TIME_REFRESH_FAILED] user_id=%s error=%s",
                        user_id,
                        str(exc).strip() or type(exc).__name__,
                    )
                expected_expire_at, expiration_source = calculate_expected_expire_at(
                    state.timeframe,
                    order_data,
                    expiration_window,
                    sent_at,
                )
                trade = {
                    **order_data,
                    "mode": state.account_mode,
                    "active": symbol,
                    "direction": direction,
                    "amount": order_amount,
                    "confidence": selected["confidence"],
                    "payout": payout,
                    "expiration": state.timeframe,
                    "timeframe": state.timeframe,
                    "result": STATUS_PENDING_RESULT,
                    "sent_at": sent_at.isoformat(),
                    "expected_expire_at": expected_expire_at.isoformat(),
                    "expires_at": expected_expire_at.isoformat(),
                    "expiration_source": expiration_source,
                    "server_time_at_send": expiration_window.get("server_time"),
                    "server_timestamp_at_send": expiration_window.get("server_timestamp"),
                    "cycle_id": state.cycle_id,
                    "order_attempts": state.order_attempts,
                    "fallback_candidate_used": state.fallback_candidate_used,
                    "strategy_name": selected.get("strategy_name"),
                    "strategy_reason": selected.get("strategy_reason"),
                    "entry_reason": selected.get("entry_reason"),
                    "used_strategies": list(selected.get("used_strategies") or []),
                    "candle_reading": selected.get("candle_reading"),
                    "strategy_score": int(selected.get("strategy_score") or selected.get("score") or 0),
                    "quality_score": int(selected.get("quality_score") or 0),
                    "block_reasons": list(selected.get("block_reasons") or selected.get("blocked_filters") or []),
                    "metrics": dict(selected.get("metrics") or {}),
                    "is_gale": is_gale_order,
                    "gale_step": int(selected.get("gale_step") or (1 if is_gale_order else 0)),
                    "parent_order_id": selected.get("parent_order_id") or state.gale_original_order_id,
                    "cycle_result": None,
                    "final_result": None,
                    "original_amount": float(selected.get("original_amount") or state.entry_value),
                    "gale_amount": float(selected.get("gale_amount") or order_amount),
                }
                trade["timestamp"] = trade["sent_at"]
                state = auto_trader.record_trade(user_id, trade)
                logger.info("[STATE] ORDER_OPEN user_id=%s cycle_id=%s order_id=%s", user_id, state.cycle_id, order_id)
                logger.info("[STATE] WAITING_RESULT user_id=%s cycle_id=%s order_id=%s", user_id, state.cycle_id, order_id)
                logger.info("[WAITING_RESULT] user_id=%s cycle_id=%s order_id=%s", user_id, state.cycle_id, order_id)
                invalidate_account_cache(user_id)
                state.entry_window_open = False
                logger.info(
                    "[EXPIRATION_SET] user_id=%s order_id=%s timeframe=%s expected_expire_at=%s source=%s server_time_at_send=%s",
                    user_id,
                    order_id,
                    state.timeframe,
                    trade["expected_expire_at"],
                    expiration_source,
                    trade["server_time_at_send"],
                )
                logger.info(
                    "[%s] user_id=%s order_id=%s status=%s",
                    "GALE_ORDER_SEND_SUCCESS" if is_gale_order else "ORDER_SEND_SUCCESS",
                    user_id,
                    order_id,
                    state.status,
                )
                logger.info(
                    "[ORDER_SENT] user_id=%s cycle_id=%s order_id=%s symbol=%s direction=%s fallback=%s",
                    user_id,
                    state.cycle_id,
                    order_id,
                    symbol,
                    direction,
                    state.fallback_candidate_used,
                )
                if is_gale_order:
                    logger.info(
                        "[GALE_ORDER_SENT] user_id=%s cycle_id=%s order_id=%s parent_order_id=%s amount=%s",
                        user_id,
                        state.cycle_id,
                        order_id,
                        trade.get("parent_order_id"),
                        order_amount,
                    )
                logger.info(
                    "[TRADE_EXECUTED] user_id=%s order_id=%s symbol=%s direction=%s amount=%s",
                    user_id,
                    order_id,
                    symbol,
                    direction,
                    order_amount,
                )
                logger.info(
                    "[%s] user_id=%s order_id=%s",
                    "PENDING_GALE_RESULT" if is_gale_order else "PENDING_RESULT",
                    user_id,
                    order_id,
                )
                logger.info(
                    "[TRADE_SENT_AT] user_id=%s server_time=%s timeframe=%s "
                    "seconds_in_candle=%s seconds_until_close=%s expiration=%s",
                    user_id,
                    entry_window["server_time"],
                    state.timeframe,
                    entry_window["current_candle_seconds"],
                    entry_window["seconds_until_close"],
                    entry_window["expiration"],
                )
                logger.info(
                    "[REAL_TRADE_SENT] user_id=%s order_id=%s",
                    user_id,
                    trade.get("order_id"),
                )
                logger.info(
                    "[CYCLE_END] user_id=%s cycle_id=%s result=ORDER_SENT order_id=%s",
                    user_id,
                    state.cycle_id,
                    order_id,
                )
                trade_result_monitor.start(user_id, order_id, trade.get("expires_at"))
                return 200, build_robot_payload(state)

            final_error = NO_AVAILABLE_ASSET_ERROR if attempted_unavailable or skipped_candidates else last_friendly_error
            state = auto_trader.reject_order(
                user_id,
                last_order_reason,
                last_order_error=final_error,
            )
            logger.error(
                "[ORDER_REJECTED] user_id=%s reason=%s last_order_error=%s",
                user_id,
                last_order_reason,
                final_error,
            )
            logger.info(
                "[NEXT_CYCLE_SCHEDULED] user_id=%s next_cycle_at=%s",
                user_id,
                state.next_cycle_at,
            )
            return last_order_status, build_robot_payload(state)
        except Exception as exc:
            error = str(exc).strip() or type(exc).__name__
            logger.exception("[ROBOT_CYCLE_RECOVERED] user_id=%s error=%s", user_id, exc)
            state = recover_analysis_error_to_window(
                user_id,
                error,
                locals().get("entry_window") if isinstance(locals().get("entry_window"), dict) else None,
            )
            return 200, build_robot_payload(state)
        finally:
            persist_robot(user_id)


async def run_analysis_now(user_id: str) -> tuple[int, dict[str, Any]]:
    state = auto_trader.get(user_id)
    if (
        not state.enabled
        or not state.connected
        or state.active_mode is None
        or state.operation_in_progress
        or state.pending_signal is not None
    ):
        return 200, build_robot_payload(state)
    logger.info(
        "[ANALYSIS_FORCED_START] user_id=%s current_candle_seconds=%s",
        user_id,
        state.current_candle_seconds,
    )
    state.next_cycle_at = utc_now()
    return await execute_robot_cycle(user_id)


async def robot_worker(user_id: str) -> None:
    try:
        logger.info("[WORKER_RUNNING_TRUE] user_id=%s", user_id)
        while auto_trader.get(user_id).enabled:
            recover_sync_timeout_if_needed(user_id)
            robot_worker_last_tick_at[user_id] = utc_now()
            logger.info("[WORKER_HEARTBEAT] user_id=%s", user_id)
            logger.info("[ROBOT_RUNNING] user_id=%s", user_id)
            state = auto_trader.get(user_id)
            if result_display_expired(state) or waiting_result_stale(state):
                state = reset_cycle_after_result(user_id)
            result_waiting = bool(
                state.operation_in_progress
                and str((state.last_trade or {}).get("result") or "").strip().upper() not in {"WIN", "LOSS", "TIMEOUT"}
            )
            guard = connection_guard_reason(user_id)
            if not state.connected or state.active_mode is None:
                if robot_has_recent_real_cache(user_id, state):
                    logger.warning("[ROBOT_WORKER_USING_RECENT_REAL_CACHE] user_id=%s", user_id)
                else:
                    logger.warning("[ROBOT_WORKER_BLOCKED_DISCONNECTED] user_id=%s", user_id)
                    logger.info("[CPU_GUARD_SLEEP] user_id=%s seconds=3.00", user_id)
                    await asyncio.sleep(3)
                    continue
            if guard is not None:
                reason, remaining = guard
                if reason == "offline":
                    logger.warning("[USER_OFFLINE_SKIPPED] user_id=%s retry_in=%.2f", user_id, remaining)
                else:
                    logger.warning("[BACKOFF_ACTIVE] user_id=%s retry_in=%.2f", user_id, remaining)
                logger.warning("[CPU_LOOP_PROTECTION] user_id=%s reason=%s", user_id, reason)
                logger.warning("[BACKOFF_IGNORED_FOR_ROBOT_WORKER] user_id=%s reason=%s", user_id, reason)
            if state.operation_in_progress or result_waiting:
                logger.info(
                    "[ANALYSIS_SKIPPED_WAITING_RESULT] user_id=%s order_id=%s",
                    user_id,
                    (state.last_trade or {}).get("order_id"),
                )
                logger.info(
                    "[SKIP_ANALYSIS_WAITING_RESULT] user_id=%s order_id=%s",
                    user_id,
                    (state.last_trade or {}).get("order_id"),
                )
                logger.info("[CPU_GUARD_SLEEP] user_id=%s seconds=0.50", user_id)
                await asyncio.sleep(0.5)
                continue
            post_trade_wait = seconds_until_next_candle_after_trade(state)
            if post_trade_wait is not None:
                sleep_seconds = max(0.5, min(post_trade_wait, 5.0))
                logger.info(
                    "[SKIP_HEAVY_ANALYSIS_UNTIL_NEXT_CANDLE] user_id=%s seconds=%.2f",
                    user_id,
                    post_trade_wait,
                )
                logger.info("[CPU_GUARD_SLEEP] user_id=%s seconds=%.2f", user_id, sleep_seconds)
                await asyncio.sleep(sleep_seconds)
                continue
            logger.info("[ROBOT_WORKER_TICK] user_id=%s", user_id)
            cycle_started_at = monotonic()
            await execute_robot_cycle(user_id)
            logger.info(
                "[CYCLE_DURATION_MS] user_id=%s source=worker ms=%s",
                user_id,
                int((monotonic() - cycle_started_at) * 1000),
            )
            state = auto_trader.get(user_id)
            if not state.enabled:
                break
            if state.status in {STATUS_WAITING_ENTRY_WINDOW, STATUS_WAITING_ENTRY}:
                delay = max(1, state.seconds_until_entry_window)
            elif state.status in TEMPORARY_WAIT_STATUSES:
                delay = max(1, state.to_dict()["seconds_until_next_cycle"])
            elif state.status == STATUS_WAITING_ANALYSIS_WINDOW:
                delay = 3
            elif state.status == STATUS_ORDER_REJECTED and state.rejected_at is not None:
                delay = max(1, 5 - int((utc_now() - state.rejected_at).total_seconds()))
            elif state.status == STATUS_WAITING_NEXT_CYCLE and state.enabled:
                delay = max(1, min(5, float(state.to_dict()["seconds_until_next_cycle"])))
            else:
                delay = max(1, state.to_dict()["seconds_until_next_cycle"])
            delay = max(0.5, float(delay))
            logger.info("[CPU_GUARD_SLEEP] user_id=%s seconds=%.2f", user_id, delay)
            await asyncio.sleep(delay)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.exception("[WORKER_CRASHED] user_id=%s error=%s", user_id, str(exc).strip() or type(exc).__name__)
        persist_robot(user_id)
    finally:
        current = asyncio.current_task()
        if robot_tasks.get(user_id) is current:
            robot_tasks.pop(user_id, None)
        logger.info("[WORKER_DESTROYED] user_id=%s", user_id)
        logger.info("[WORKER_STOPPED] user_id=%s", user_id)
        state = auto_trader.get(user_id)
        if (
            getattr(state, "enabled", False)
            and not is_stop_status(getattr(state, "status", None))
            and user_id not in robot_worker_restart_attempted
        ):
            robot_worker_restart_attempted.add(user_id)
            robot_tasks[user_id] = asyncio.create_task(robot_worker(user_id))
            logger.info("[WORKER_RESTARTED] user_id=%s", user_id)


def ensure_robot_worker(user_id: str) -> None:
    if not is_user_active(user_id):
        logger.info("[OFFLINE_USER_SKIPPED] user_id=%s operation=worker_start", user_id)
        return
    state = auto_trader.get(user_id)
    if not state.enabled:
        logger.info("[SESSION_RESTORE_SKIPPED] user_id=%s reason=robot_disabled", user_id)
        return
    account_snapshot = get_cached_account_snapshot(user_id)
    if (
        state.account_mode == "REAL"
        and (state.active_mode or account_snapshot.get("mode")) == "REAL"
        and account_snapshot.get("balance") is not None
        and float(account_snapshot["balance"]) <= 0
    ):
        auto_trader.insufficient_balance(user_id)
        persist_robot(user_id)
        logger.warning(
            "[INSUFFICIENT_BALANCE_REAL] user_id=%s balance=%s",
            user_id,
            account_snapshot.get("balance"),
        )
        logger.warning(
            "[ROBOT_STOPPED_BALANCE_ZERO] user_id=%s balance=%s",
            user_id,
            account_snapshot.get("balance"),
        )
        return
    if robot_connection_unavailable(bool(state.connected), state.active_mode):
        logger.warning("[ROBOT_WORKER_BLOCKED_DISCONNECTED] user_id=%s", user_id)
        return
    guard = connection_guard_reason(user_id)
    if guard is not None and not robot_has_recent_real_cache(user_id, state):
        reason, remaining = guard
        logger.warning("[SESSION_CHECK_SKIPPED] user_id=%s worker_start=true reason=%s retry_in=%.2f", user_id, reason, remaining)
        return
    task = robot_tasks.get(user_id)
    last_tick_at = robot_worker_last_tick_at.get(user_id)
    stale_worker = (
        task is not None
        and not task.done()
        and last_tick_at is not None
        and (utc_now() - last_tick_at).total_seconds() > 30
    )
    if stale_worker:
        logger.warning("[WORKER_STALE_RESTART] user_id=%s last_tick_at=%s", user_id, last_tick_at)
        task.cancel()
        robot_tasks.pop(user_id, None)
        task = None
    if task is None or task.done():
        robot_tasks[user_id] = asyncio.create_task(robot_worker(user_id))
        logger.info("[WORKER_CREATED] user_id=%s", user_id)
        logger.info("[WORKER_RUNNING_TRUE] user_id=%s", user_id)
        logger.info("[ROBOT_WORKER_STARTED] user_id=%s", user_id)
    else:
        logger.info("[WORKER_ALREADY_RUNNING] user_id=%s", user_id)
    logger.info("[WORKER_RUNNING_TRUE] user_id=%s", user_id)


def schedule_robot_tick(user_id: str) -> None:
    if not auto_trader.get(user_id).enabled:
        return

    async def run_tick() -> None:
        try:
            await execute_robot_cycle(user_id)
        except Exception:
            logger.exception("[ROBOT_INITIAL_TICK_RECOVERED] user_id=%s", user_id)

    asyncio.create_task(run_tick())


async def stop_robot_worker(user_id: str) -> None:
    task = robot_tasks.pop(user_id, None)
    if task is None or task.done():
        logger.info("[WORKER_ALREADY_STOPPED] user_id=%s", user_id)
        return
    if task is asyncio.current_task():
        logger.info("[WORKER_STOPPED] user_id=%s", user_id)
        return
    logger.info("[WORKER_STOPPING] user_id=%s", user_id)
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task


async def read_restored_session_status(user_id: str) -> bool:
    for attempt in range(5):
        _, payload = await call_bullex_service(
            "GET",
            "/sessions/status",
            user_id,
            allow_failure_backoff=False,
        )
        connected, _ = extract_account_status(payload)
        if connected:
            sync_user_store_from_payload(user_id, payload)
            return True
        if attempt < 4:
            await asyncio.sleep(2)
    return False


@app.on_event("startup")
async def restore_robot_states() -> None:
    logger.info("[STARTUP_RESTORE_DISABLED] no session restore on startup")
    restored_count = 0
    try:
        for user_id, payload in robot_persistence.load_states():
            session_restored = False
            payload = {
                **payload,
                "account_mode": "REAL",
                "allow_real": True,
                "confirm_real": True,
                "connected": session_restored,
                "active_mode": None,
                "connection_checked_at": None,
                "connection_status_source": "startup_no_session_restore",
            }
            restorable_robot_states[user_id] = deepcopy(payload)
            trades = robot_persistence.load_trades(user_id)
            auto_trader.restore(
                user_id,
                payload,
                trades,
                source=robot_persistence_source(),
            )
            robot_state_hydrated_users.add(user_id)
            restored_count += 1
            logger.info(
                "[USER_STATE_LOADED_NO_WORKER] user_id=%s source=%s",
                user_id,
                robot_persistence_source(),
            )
    except Exception:
        logger.exception("[ROBOT STARTUP RESTORE ERROR]")
    logger.info("[ON_DEMAND_RESTORE_ONLY] robot restore requires user action")
    logger.info("[STARTUP_READY] restored_users=%s worker_start=false", restored_count)


@app.on_event("shutdown")
async def shutdown_robot_workers() -> None:
    for user_id in list(robot_tasks):
        persist_robot(user_id)
        await stop_robot_worker(user_id)
    await trade_result_monitor.shutdown()


@app.get("/health")
async def health() -> dict[str, Any]:
    return build_success({"status": "healthy", "service": "backend-gateway"})


@app.get("/cors-test")
async def cors_test() -> dict[str, bool]:
    return {"ok": True}


@app.get("/signals/analyze")
async def signals_analyze(
    active: str,
    strategy_mode: str = Query(default="conservative"),
    auth: dict[str, str] = Depends(require_headers),
) -> JSONResponse:
    if not is_binary_asset_allowed(active):
        return json_response(400, build_error(ASSET_NOT_ALLOWED))

    symbol = normalize_binary_active(active)
    status_code, payload = await analyze_active_signal(
        auth["user_id"],
        symbol,
        strategy_mode=strategy_mode,
    )
    return json_response(status_code, payload)


@app.get("/signals/review")
async def signals_review(
    active: str,
    auth: dict[str, str] = Depends(require_headers),
) -> JSONResponse:
    if not is_binary_asset_allowed(active):
        return json_response(400, build_error(ASSET_NOT_ALLOWED))

    symbol = normalize_binary_active(active)
    status_code, payload = await analyze_active_signal(auth["user_id"], symbol)
    if not payload.get("ok"):
        return json_response(status_code, payload)

    signal = payload["data"]
    review = build_local_signal_review(signal)
    return json_response(200, build_success({"signal": signal, "review": review}))


@app.get("/signals/scan")
async def signals_scan(
    limit: int = Query(default=5, ge=1, le=len(BINARY_ALLOWED_ASSETS)),
    include_wait: bool = False,
    strategy_mode: str = Query(default="conservative"),
    auth: dict[str, str] = Depends(require_headers),
) -> JSONResponse:
    status_code, payload = await scan_local_signals(
        auth["user_id"],
        limit=limit,
        include_wait=include_wait,
        strategy_mode=strategy_mode,
    )
    return json_response(status_code, payload)


@app.get("/signals/top-reviewed")
async def signals_top_reviewed(auth: dict[str, str] = Depends(require_headers)) -> JSONResponse:
    status_code, payload = await scan_local_signals(auth["user_id"], limit=5, include_wait=False)
    if not payload.get("ok"):
        return json_response(status_code, payload)

    reviewed = []
    for signal in payload["data"][:5]:
        review = build_local_signal_review(signal)
        reviewed.append({"signal": signal, "review": review})

    reviewed.sort(
        key=lambda item: (
            int(item["review"].get("quality") or 0),
            int(item["signal"].get("confidence") or 0),
        ),
        reverse=True,
    )
    return json_response(200, build_success(reviewed))


async def _robot_state_impl(auth: dict[str, str]) -> JSONResponse:
    user_id = auth["user_id"]
    if not is_user_active(user_id):
        logger.info("[OFFLINE_USER_SKIPPED] user_id=%s path=/robot/state", user_id)
    # This polling endpoint must remain independent from BullEx, WebSocket and
    # persistence latency. Startup restoration populates auto_trader; a missing
    # entry is represented by its in-memory default state.
    auto_trader.get(user_id)
    state = recover_sync_timeout_if_needed(user_id)
    repaired_legacy_real_flags = (
        state.account_mode != "REAL"
        or not bool(state.allow_real)
        or not bool(state.confirm_real)
        or str(state.active_mode or "").strip().upper() == "DEMO"
    )
    state.account_mode = "REAL"
    state.allow_real = True
    state.confirm_real = True
    if str(state.active_mode or "").strip().upper() == "DEMO":
        state.active_mode = "REAL"
    if repaired_legacy_real_flags:
        persist_robot(user_id)
    account_snapshot = get_cached_account_snapshot(user_id)
    connected = bool(state.connected or account_snapshot.get("connected") is True)
    active_mode = state.active_mode or account_snapshot.get("mode")
    active_mode = str(active_mode).strip().upper() if active_mode else None
    source = state.connection_status_source or (
        "cached" if account_snapshot.get("connected") is not None else "memory"
    )
    if connected and active_mode == "REAL":
        if source in {"disconnected", "offline_cache", "backoff_active"}:
            source = "cached"
        clear_session_backoff(user_id)
        state = auto_trader.sync_connection(
            user_id,
            connected=True,
            active_mode="REAL",
            source=source,
            align_status=True,
        )
    window = get_entry_window(
        state.timeframe,
        utc_now().timestamp(),
        server_time_source="vps_fallback",
    )
    auto_trader.update_entry_window(
        user_id,
        window,
    )
    state = auto_trader.get(user_id)
    block_reason = real_block_reason(state, connected=connected, active_mode=active_mode, user_id=user_id)
    real_balance_warning = get_real_balance_warning(
        user_id,
        state,
        active_mode,
        snapshot=account_snapshot,
    )
    if (
        state.account_mode == "REAL"
        and active_mode == "REAL"
        and account_snapshot.get("balance") is not None
        and float(account_snapshot["balance"]) <= 0
    ):
        state = await stop_real_robot_for_insufficient_balance(
            user_id,
            balance=float(account_snapshot["balance"]),
        )
        real_balance_warning = "BALANCE_ZERO"
        block_reason = STATUS_INSUFFICIENT_BALANCE
    worker_task = robot_tasks.get(user_id)
    worker_running = bool(worker_task is not None and not worker_task.done())
    if state.enabled and block_reason is None and not worker_running:
        if user_id in robot_worker_restart_attempted:
            block_reason = "WORKER_NOT_RUNNING"
            logger.warning("[WORKER_NOT_RUNNING] user_id=%s action=restart_limit_reached", user_id)
        else:
            logger.warning("[WORKER_NOT_RUNNING] user_id=%s action=auto_create", user_id)
            ensure_robot_worker(user_id)
            worker_task = robot_tasks.get(user_id)
            worker_running = bool(worker_task is not None and not worker_task.done())
            if not worker_running:
                block_reason = "WORKER_NOT_RUNNING"
    if state.enabled and worker_running and state.status == STATUS_STOPPED:
        state.status = STATUS_ANALYZING
        persist_robot(user_id)
    balance_value = number_or_none(account_snapshot.get("balance"))
    real_ready = bool(
        connected
        and active_mode == "REAL"
        and block_reason is None
        and (balance_value is None or float(balance_value) > 0)
    )
    logger.info(
        "[ROBOT_STATE_FAST_RETURN] user_id=%s connected=%s source=%s",
        user_id,
        connected,
        source,
    )
    return json_response(
        200,
        build_robot_payload(
            state,
            user_id=user_id,
            connected=connected,
            active_mode=active_mode,
            balance=account_snapshot.get("balance"),
            currency=account_snapshot.get("currency"),
            email=account_snapshot.get("email"),
            connection_checked_at=state.connection_checked_at.isoformat()
            if state.connection_checked_at is not None
            else None,
            connection_status_source=source,
            real_ready=real_ready,
            real_block_reason=block_reason,
            real_balance_warning=real_balance_warning,
        ),
    )


@app.get("/robot/state")
async def robot_state(auth: dict[str, str] = Depends(require_headers)) -> JSONResponse:
    try:
        return await _robot_state_impl(auth)
    except Exception as exc:
        logger.info(
            "[ROBOT_STATE_RECOVERED] user_id=%s reason=%s",
            auth.get("user_id"),
            exc.__class__.__name__,
            exc_info=True,
        )
        return json_response(200, build_robot_state_fallback_payload(auth.get("user_id"), exc))


def build_robot_stats(items: list[dict[str, Any]]) -> dict[str, Any]:
    final_results = [
        str(item.get("final_result") or "").strip().upper()
        for item in items
        if str(item.get("final_result") or "").strip().upper() in {"WIN", "LOSS"}
    ]
    wins = sum(1 for result in final_results if result == "WIN")
    losses = sum(1 for result in final_results if result == "LOSS")
    total_trades = wins + losses
    profit = round(sum(float(item.get("profit") or 0) for item in items), 2)
    gross_profit = sum(max(0.0, float(item.get("profit") or 0)) for item in items)
    gross_loss = abs(sum(min(0.0, float(item.get("profit") or 0)) for item in items))
    profit_factor = round(gross_profit / gross_loss, 2) if gross_loss else round(gross_profit, 2)

    current_win_streak = 0
    current_loss_streak = 0
    if final_results:
        current_result = final_results[0]
        for result in final_results:
            if result != current_result:
                break
            if current_result == "WIN":
                current_win_streak += 1
            elif current_result == "LOSS":
                current_loss_streak += 1

    best_win_streak = 0
    best_loss_streak = 0
    win_streak = 0
    loss_streak = 0
    for result in reversed(final_results):
        if result == "WIN":
            win_streak += 1
            loss_streak = 0
            best_win_streak = max(best_win_streak, win_streak)
        elif result == "LOSS":
            loss_streak += 1
            win_streak = 0
            best_loss_streak = max(best_loss_streak, loss_streak)

    return {
        "wins": wins,
        "losses": losses,
        "total_trades": total_trades,
        "win_rate": round((wins / total_trades) * 100, 2) if total_trades else 0.0,
        "profit": profit,
        "profit_factor": profit_factor,
        "current_win_streak": current_win_streak,
        "current_loss_streak": current_loss_streak,
        "best_win_streak": best_win_streak,
        "best_loss_streak": best_loss_streak,
    }


def load_robot_history_items(user_id: str, days: int) -> list[dict[str, Any]]:
    persisted_items = robot_persistence.load_trade_history(user_id, days)
    items_by_order_id: dict[str, dict[str, Any]] = {}
    ordered_items: list[dict[str, Any]] = []
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, int(days)))

    for item in persisted_items:
        order_id = str(item.get("order_id") or "").strip()
        if not order_id:
            continue
        normalized = strip_ai_fields(dict(item))
        items_by_order_id[order_id] = normalized
        ordered_items.append(normalized)

    for trade in auto_trader.history(user_id).get("trades", []):
        order_id = str(trade.get("order_id") or "").strip()
        result = str(trade.get("result") or "").strip().upper()
        finished_at = parse_datetime(trade.get("finished_at"))
        if not order_id or order_id in items_by_order_id:
            continue
        if result not in {"WIN", "LOSS"} or finished_at is None or finished_at < cutoff:
            continue
        normalized = strip_ai_fields(dict(trade))
        ordered_items.append(normalized)
        items_by_order_id[order_id] = normalized

    return sorted(
        ordered_items,
        key=lambda item: parse_datetime(item.get("finished_at")) or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )


class RobotResetCycleRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")

    reset_score: bool = False
    reset_daily_profit: bool = True


@app.get("/robot/history")
async def robot_history(
    days: int = Query(default=30),
    auth: dict[str, str] = Depends(require_headers),
) -> JSONResponse:
    if days not in {1, 7, 30}:
        raise HTTPException(status_code=422, detail="days must be 1, 7, or 30")
    items = load_robot_history_items(auth["user_id"], days)
    return json_response(200, build_success({"items": items, "trades": items}))


@app.options("/robot/history")
async def robot_history_options() -> JSONResponse:
    return json_response(200, build_success({"preflight": True}))


@app.get("/robot/stats")
async def robot_stats(
    days: int = Query(default=30),
    auth: dict[str, str] = Depends(require_headers),
) -> JSONResponse:
    if days not in {1, 7, 30}:
        raise HTTPException(status_code=422, detail="days must be 1, 7, or 30")
    items = load_robot_history_items(auth["user_id"], days)
    return json_response(200, build_success(build_robot_stats(items)))


@app.options("/robot/stats")
async def robot_stats_options() -> JSONResponse:
    return json_response(200, build_success({"preflight": True}))


@app.get("/robot/persistence")
async def robot_persistence_status(auth: dict[str, str] = Depends(require_headers)) -> JSONResponse:
    try:
        payload = robot_persistence.get_restore_status(auth["user_id"])
    except Exception:
        logger.exception("[ROBOT PERSISTENCE ERROR] user_id=%s", auth["user_id"])
        payload = {"session_restored": False, "robot_restored": False, "last_restore_at": None}
    return JSONResponse(status_code=200, content=payload)


@app.get("/sessions/persistence-debug")
async def sessions_persistence_debug(_: None = Depends(require_api_key)) -> JSONResponse:
    status_code, payload = await call_bullex_service(
        "GET",
        "/sessions/persistence-debug",
        "persistence-debug",
    )
    if not payload.get("ok"):
        return json_response(status_code, payload)

    data = payload.get("data")
    if not isinstance(data, dict):
        return json_response(502, build_error("INVALID_BULLEX_RESPONSE"))
    return JSONResponse(status_code=status_code, content=data)


@app.get("/debug/bullex-connection-schema")
async def debug_bullex_connection_schema(
    auth: dict[str, str] = Depends(require_headers),
) -> JSONResponse:
    payload = build_connection_payload(
        {
            "email": "user@example.com",
            "connected": True,
            "requires_2fa": False,
            "active_mode": "REAL",
            "active_mode_from_bullex": "REAL",
            "currency": "BRL",
            "balance": 0,
        }
    )
    diagnostic = user_store.connection_upsert_diagnostic(auth["user_id"], payload)
    return json_response(200, build_success(diagnostic))


@app.get("/debug/user-isolation")
async def debug_user_isolation(
    auth: dict[str, str] = Depends(require_headers),
) -> JSONResponse:
    user_id = auth["user_id"]
    state = get_user_robot_state(user_id)
    return JSONResponse(
        status_code=200,
        content={
            "user_id": user_id,
            "has_state": auto_trader.has_state(user_id),
            "entry_value": state.entry_value,
            "stop_win": state.stop_win,
            "stop_loss": state.stop_loss,
            "source": auto_trader.source(user_id),
        },
    )


@app.get("/debug/robot-settings")
async def debug_robot_settings(
    auth: dict[str, str] = Depends(require_headers),
) -> JSONResponse:
    user_id = auth["user_id"]
    state = get_user_robot_state(user_id)
    return JSONResponse(
        status_code=200,
        content={
            "user_id": user_id,
            "source": auto_trader.source(user_id),
            "settings": extract_robot_settings(state.to_dict()),
        },
    )


@app.post("/robot/config")
async def robot_config(
    body: dict[str, Any] | None = Body(default=None),
    auth: dict[str, str] = Depends(require_headers),
) -> JSONResponse:
    user_id = auth["user_id"]
    get_user_robot_state(user_id)
    state = recover_sync_timeout_if_needed(user_id)
    if robot_config_locked(user_id, state):
        logger.warning(
            "[ROBOT_CONFIG_LOCKED] user_id=%s enabled=%s worker_running=%s operation_in_progress=%s",
            user_id,
            state.enabled,
            bool(robot_tasks.get(user_id) is not None and not robot_tasks[user_id].done()),
            state.operation_in_progress,
        )
        return json_response(409, CONFIG_LOCK_ERROR)
    raw_body = dict(body or {})
    raw_body["account_mode"] = "REAL"
    raw_body["allow_real"] = True
    raw_body["confirm_real"] = True
    logger.warning("[ROBOT_CONFIG_PAYLOAD] user_id=%s payload=%s", user_id, raw_body)
    logger.info("[REAL_CONFIG_RECEIVED] user_id=%s payload=%s", user_id, raw_body)
    ignored_ai_fields = ignored_ai_config_fields(raw_body)
    if ignored_ai_fields:
        logger.info(
            "[ROBOT_AI_FIELDS_IGNORED] user_id=%s fields=%s",
            user_id,
            ignored_ai_fields,
        )
    filtered_body = filter_robot_config_payload(raw_body)
    partial_update = RobotConfigUpdate.model_validate(filtered_body)
    if partial_update.entry_value is not None:
        if partial_update.entry_value < MIN_REAL_ENTRY:
            return json_response(400, build_error("ENTRY_VALUE_TOO_LOW"))
        if partial_update.entry_value > MAX_REAL_ENTRY:
            return json_response(400, build_error("ENTRY_VALUE_TOO_HIGH"))
    logger.warning(
        "[ROBOT_CONFIG_FILTERED_PAYLOAD] user_id=%s payload=%s",
        user_id,
        partial_update.model_dump(exclude_none=True),
    )
    state = auto_trader.update_config(
        user_id,
        partial_update,
    )
    saved_fields = partial_update.model_dump(exclude_none=True)
    persist_robot(user_id)
    logger.info(
        "[ROBOT_CONFIG_SAVED] user_id=%s saved_fields=%s",
        user_id,
        sorted(saved_fields.keys()),
    )
    logger.info(
        "[REAL_CONFIG_SAVED] user_id=%s account_mode=%s allow_real=%s confirm_real=%s",
        user_id,
        state.account_mode,
        state.allow_real,
        state.confirm_real,
    )
    if {"account_mode", "allow_real", "confirm_real"}.intersection(saved_fields):
        logger.info(
            "[REAL_CONFIRMATION_UPDATED] user_id=%s account_mode=%s allow_real=%s confirm_real=%s",
            user_id,
            state.account_mode,
            state.allow_real,
            state.confirm_real,
        )
    return json_response(200, build_robot_payload(state, user_id=user_id))


@app.post("/robot/settings")
async def robot_settings(
    body: dict[str, Any] | None = Body(default=None),
    auth: dict[str, str] = Depends(require_headers),
) -> JSONResponse:
    return await robot_config(body, auth)


async def _robot_start_impl(auth: dict[str, str]) -> JSONResponse:
    user_id = auth["user_id"]
    mark_user_active(user_id)
    logger.info("[ROBOT_START_REQUEST] user_id=%s", user_id)
    state = get_user_robot_state(user_id)
    state.account_mode = "REAL"
    state.allow_real = True
    state.confirm_real = True
    if is_stop_status(state.status):
        return json_response(409, build_error("RESET_CYCLE_REQUIRED"))
    if fresh_robot_connection(state) and state.connected and state.active_mode is not None:
        connected = True
        active_mode = state.active_mode
    else:
        _, _, state, connected, active_mode, _ = await fetch_and_sync_robot_connection(
            user_id,
            allow_session_restore=True,
        )
    if robot_connection_unavailable(connected, active_mode):
        state = auto_trader.disconnect_account(user_id)
        persist_robot(user_id)
        await stop_robot_worker(user_id)
        return json_response(409, build_error("BULLEX_NOT_CONNECTED"))
    if active_mode != "REAL":
        logger.warning(
            "[REAL_MODE_NOT_CONFIRMED] user_id=%s active_mode=%s",
            user_id,
            active_mode,
        )
        state = auto_trader.require_real_mode(user_id)
        persist_robot(user_id)
        await stop_robot_worker(user_id)
        return json_response(200, build_robot_payload(state, user_id=user_id))

    try:
        _, account_payload = await call_bullex_service("GET", "/account", user_id)
    except Exception as exc:
        logger.warning(
            "[REAL_MODE_NOT_CONFIRMED] user_id=%s reason=%s",
            user_id,
            exc.__class__.__name__,
        )
        state = auto_trader.require_real_mode(user_id)
        persist_robot(user_id)
        await stop_robot_worker(user_id)
        contract = build_real_account_contract(build_success({"connected": False}))
        contract["data"]["robot"] = build_robot_payload(state, user_id=user_id)["data"]
        return json_response(200, contract)
    account_contract = build_real_account_contract(account_payload)
    account_data = account_contract["data"]
    if not account_contract.get("ok"):
        state = auto_trader.require_real_mode(user_id)
        persist_robot(user_id)
        await stop_robot_worker(user_id)
        account_data["robot"] = build_robot_payload(state, user_id=user_id)["data"]
        return json_response(200, account_contract)
    real_balance = number_or_none(account_data.get("balance_real"))
    if real_balance is None or float(real_balance) <= 0:
        state = auto_trader.insufficient_balance(user_id)
        persist_robot(user_id)
        await stop_robot_worker(user_id)
        logger.warning(
            "[ROBOT_START_BLOCKED_INSUFFICIENT_BALANCE] user_id=%s balance=%s entry_value=%s",
            user_id,
            real_balance,
            state.entry_value,
        )
        logger.warning(
            "[INSUFFICIENT_BALANCE_REAL] user_id=%s balance=%s entry_value=%s",
            user_id,
            real_balance,
            state.entry_value,
        )
        return json_response(
            200,
            build_insufficient_balance_start_response(
                state,
                message=INSUFFICIENT_BALANCE_START_MESSAGE,
            ),
        )
    if float(real_balance) < float(state.entry_value):
        state = auto_trader.insufficient_balance(user_id)
        persist_robot(user_id)
        await stop_robot_worker(user_id)
        logger.warning(
            "[ROBOT_START_BLOCKED_INSUFFICIENT_BALANCE] user_id=%s balance=%s entry_value=%s",
            user_id,
            real_balance,
            state.entry_value,
        )
        logger.warning(
            "[INSUFFICIENT_BALANCE_REAL] user_id=%s balance=%s entry_value=%s",
            user_id,
            real_balance,
            state.entry_value,
        )
        return json_response(
            200,
            build_insufficient_balance_start_response(
                state,
                message=ENTRY_VALUE_EXCEEDS_BALANCE_MESSAGE,
            ),
        )
    clear_session_backoff(user_id)
    state = auto_trader.sync_connection(
        user_id,
        connected=True,
        active_mode="REAL",
        source=connection_source_from_payload(account_payload),
        align_status=True,
    )
    if state.account_mode == "REAL":
        logger.info(
            "[REAL MODE DETECTED] user_id=%s active_mode=%s confirm_real=%s",
            user_id,
            state.active_mode,
            state.confirm_real,
        )
        block_reason = real_block_reason(state, connected=connected, active_mode=active_mode, user_id=user_id)
        if block_reason is not None:
            auto_trader.lock_real(user_id, block_reason)
            persist_robot(user_id)
            logger.warning(
                "[REAL BUY BLOCKED reason=%s] user_id=%s",
                block_reason,
                user_id,
            )
            return json_response(403, build_error(block_reason))

    logger.info(
        "[ROBOT_START_VALIDATED] user_id=%s connected=%s active_mode=%s balance=%s entry_value=%s",
        user_id,
        connected,
        active_mode,
        real_balance,
        state.entry_value,
    )
    state = auto_trader.start(user_id)
    state.next_cycle_at = utc_now()
    state.status = STATUS_ANALYZING
    robot_worker_restart_attempted.discard(user_id)
    logger.info(
        "[CYCLE_START] user_id=%s cycle_id=%s next_cycle_at=%s cycle_minutes=%s",
        user_id,
        state.cycle_id,
        state.next_cycle_at,
        state.cycle_minutes,
    )
    logger.info(
        "[CYCLE_CONFIG] user_id=%s cycle_minutes=%s source=robot_start",
        user_id,
        state.cycle_minutes,
    )
    persist_robot(user_id)
    ensure_robot_worker(user_id)
    logger.info(
        "[ROBOT_START_NEW_CYCLE] user_id=%s cycle_id=%s current_cycle_started_at=%s next_cycle_at=%s",
        user_id,
        state.cycle_id,
        state.current_cycle_started_at,
        state.next_cycle_at,
    )
    logger.info(
        "[ROBOT_START_DELAYED] user_id=%s next_cycle_at=%s cycle_minutes=%s",
        user_id,
        state.next_cycle_at,
        state.cycle_minutes,
    )
    logger.info("[ROBOT START] user_id=%s", user_id)
    return json_response(200, build_robot_payload(auto_trader.get(user_id), user_id=user_id))


@app.post("/robot/start")
async def robot_start(auth: dict[str, str] = Depends(require_headers)) -> JSONResponse:
    try:
        return await _robot_start_impl(auth)
    except Exception as exc:
        logger.warning(
            "[ROBOT_START_RECOVERED] user_id=%s reason=%s",
            auth.get("user_id"),
            exc.__class__.__name__,
            exc_info=True,
        )
        return json_response(200, build_controlled_upstream_error(exc))


async def _robot_stop_impl(auth: dict[str, str]) -> JSONResponse:
    user_id = auth["user_id"]
    mark_user_active(user_id)
    logger.info("[ROBOT_STOP_REQUEST] user_id=%s", user_id)
    state = auto_trader.stop(user_id)
    persist_robot(user_id)
    await stop_robot_worker(user_id)
    logger.info("[ROBOT STOP] user_id=%s", user_id)
    return json_response(200, build_robot_payload(state, user_id=user_id))


@app.post("/robot/stop")
async def robot_stop(auth: dict[str, str] = Depends(require_headers)) -> JSONResponse:
    try:
        return await _robot_stop_impl(auth)
    except Exception as exc:
        logger.warning(
            "[ROBOT_STOP_RECOVERED] user_id=%s reason=%s",
            auth.get("user_id"),
            exc.__class__.__name__,
            exc_info=True,
        )
        return json_response(200, build_controlled_upstream_error(exc))


@app.post("/robot/reset-cycle")
async def robot_reset_cycle(
    body: dict[str, Any] | None = Body(default=None),
    auth: dict[str, str] = Depends(require_headers),
) -> JSONResponse:
    RobotResetCycleRequest.model_validate(body or {})
    user_id = auth["user_id"]
    async with auto_trader.lock(user_id):
        state = auto_trader.reset_cycle(user_id, reset_score=True, reset_daily_profit=True)
        robot_persistence.clear_finished_trades(user_id)
        robot_persistence.clear_trade_history(user_id)
        persist_robot(user_id)
    await stop_robot_worker(user_id)
    logger.info("[RESET_CYCLE] user_id=%s cycle_id=%s", user_id, state.cycle_id)
    logger.info(
        "[ROBOT_RESET_SCORE] user_id=%s wins=%s losses=%s profit=%s",
        user_id,
        state.wins,
        state.losses,
        state.profit,
    )
    logger.info(
        "[ROBOT_CYCLE_RESET] user_id=%s cycle_id=%s status=%s",
        user_id,
        state.cycle_id,
        state.status,
    )
    return json_response(200, build_robot_payload(state, user_id=user_id))


@app.options("/robot/reset-cycle")
async def robot_reset_cycle_options() -> JSONResponse:
    return json_response(200, build_success({"preflight": True}))


@app.post("/robot/reset-score")
async def robot_reset_score(auth: dict[str, str] = Depends(require_headers)) -> JSONResponse:
    user_id = auth["user_id"]
    async with auto_trader.lock(user_id):
        state = auto_trader.reset_score(user_id)
        robot_persistence.clear_finished_trades(user_id)
        robot_persistence.clear_trade_history(user_id)
        persist_robot(user_id)
    logger.info(
        "[ROBOT_SCORE_RESET] user_id=%s wins=%s losses=%s profit=%s",
        user_id,
        state.wins,
        state.losses,
        state.profit,
    )
    return json_response(200, build_robot_payload(state, user_id=user_id))


@app.options("/robot/reset-score")
async def robot_reset_score_options() -> JSONResponse:
    return json_response(200, build_success({"preflight": True}))


@app.post("/robot/tick")
async def robot_tick(auth: dict[str, str] = Depends(require_headers)) -> JSONResponse:
    status_code, payload = await execute_robot_cycle(auth["user_id"])
    return json_response(status_code, payload)


@app.post("/robot/sync-connection")
async def robot_sync_connection(auth: dict[str, str] = Depends(require_headers)) -> JSONResponse:
    user_id = auth["user_id"]
    get_user_robot_state(user_id)
    status_code, _, state, connected, active_mode, source = await fetch_and_sync_robot_connection(user_id)
    persist_robot(user_id)
    account_snapshot = await refresh_account_snapshot_if_needed(
        user_id,
        connected=connected,
        active_mode=active_mode,
    )
    block_reason = real_block_reason(state, connected=connected, active_mode=active_mode, user_id=user_id)
    real_balance_warning = get_real_balance_warning(user_id, state, active_mode)
    return json_response(
        200 if status_code < 500 else status_code,
        build_robot_payload(
            state,
            user_id=user_id,
            connected=connected,
            active_mode=active_mode,
            balance=account_snapshot.get("balance"),
            currency=account_snapshot.get("currency"),
            email=account_snapshot.get("email"),
            connection_checked_at=state.connection_checked_at.isoformat()
            if state.connection_checked_at is not None
            else None,
            connection_status_source=source,
            real_ready=block_reason is None,
            real_block_reason=block_reason,
            real_balance_warning=real_balance_warning,
        ),
    )


@app.post("/robot/execute-demo")
async def robot_execute_demo(auth: dict[str, str] = Depends(require_headers)) -> JSONResponse:
    return json_response(409, build_error("REAL_MODE_ONLY"))


@app.post("/robot/execute-real")
async def robot_execute_real(
    auth: dict[str, str] = Depends(require_headers),
) -> JSONResponse:
    status_code, payload = await execute_robot_cycle(
        auth["user_id"],
        required_mode="REAL",
    )
    return json_response(status_code, payload)


@app.websocket("/ws/market")
async def ws_market(websocket: WebSocket) -> None:
    api_key = normalize_ws_value(websocket.query_params.get("api_key"))
    user_id = normalize_ws_value(websocket.query_params.get("user_id"))
    active = normalize_ws_value(websocket.query_params.get("active"))

    logger.info("[MARKET WS CONNECTING] user_id=%s active=%s", user_id or "<missing>", active or "<missing>")

    if not config.panel_api_key:
        await websocket.accept()
        await close_market_websocket(websocket, {"type": "error", "error": "PANEL_API_KEY_NOT_CONFIGURED"})
        return
    if api_key != config.panel_api_key:
        await websocket.accept()
        await close_market_websocket(websocket, {"type": "error", "error": "INVALID_API_KEY"})
        return
    if not user_id:
        await websocket.accept()
        await close_market_websocket(websocket, {"type": "error", "error": "MISSING_USER_ID"})
        return
    if not active:
        await websocket.accept()
        await close_market_websocket(websocket, {"type": "error", "error": "MISSING_ACTIVE"})
        return
    if not is_binary_asset_allowed(active):
        await websocket.accept()
        await close_market_websocket(websocket, build_error(ASSET_NOT_ALLOWED))
        return
    active = normalize_binary_active(active)

    await manager.connect(user_id, active, websocket)
    logger.info("[MARKET WS CONNECTED] user_id=%s active=%s", user_id, active)

    try:
        await stream_market_updates(websocket, user_id, active)
    except WebSocketDisconnect:
        logger.info("[MARKET WS DISCONNECTED] user_id=%s active=%s", user_id, active)
    except Exception:
        logger.exception("[MARKET WS ERROR] user_id=%s active=%s error=UNHANDLED_WEBSOCKET_EXCEPTION", user_id, active)
        try:
            await websocket.send_json({"type": "warning", "error": "MARKET_STREAM_TEMPORARY_ERROR"})
        except Exception:
            logger.exception("falha ao enviar warning final do websocket de mercado")
    finally:
        await manager.disconnect(user_id, active, websocket)
        logger.info("[MARKET WS DISCONNECTED] user_id=%s active=%s", user_id, active)


async def _bullex_connect_impl(
    body: dict[str, Any],
    auth: dict[str, str],
) -> JSONResponse:
    user_id = auth["user_id"]
    logger.info("[CONNECT_REQUEST] user_id=%s", user_id)
    mark_user_active(user_id)
    clear_session_backoff(user_id)
    logger.info("[CONNECT_BACKOFF_CLEARED] user_id=%s", user_id)
    logger.info("[CONNECT_ATTEMPT] user_id=%s", user_id)
    logger.info("[REAL_MODE_REQUESTED] user_id=%s", user_id)
    connect_body = {
        **body,
        "account_mode": "REAL",
        "mode": "REAL",
    }
    status_code, payload = await call_bullex_service(
        "POST",
        "/sessions/connect",
        user_id,
        json_body=connect_body,
    )
    payload = normalize_service_payload(payload)
    if status_code >= 500:
        error = str(payload.get("error") or "").strip().upper()
        logger.warning(
            "[CONNECT_UPSTREAM_HANDLED] user_id=%s upstream_status=%s",
            user_id,
            status_code,
        )
        if error == "LOGIN_TIMEOUT":
            logger.warning("[CONNECT_TIMEOUT_HANDLED] user_id=%s source=gateway_endpoint", user_id)
        logger.warning(
            "[CONNECT_FAILED_HANDLED] user_id=%s detail=%s",
            user_id,
            error or BULLEX_TEMPORARY_UNAVAILABLE,
        )
        return json_response(
            200,
            build_controlled_upstream_error(error or BULLEX_TEMPORARY_UNAVAILABLE),
        )
    if not payload.get("ok"):
        detail = payload.get("error") or "LOGIN_FAILED"
        logger.warning("[CONNECT_FAILED_HANDLED] user_id=%s detail=%s", user_id, detail)
        if detail in {
            "BULLEX_ACCOUNT_STILL_PRACTICE",
            "BULLEX_ACTIVE_MODE_NOT_REAL",
            "REAL_BALANCE_NOT_DETECTED",
        }:
            state = auto_trader.require_real_mode(user_id)
            persist_robot(user_id)
            await stop_robot_worker(user_id)
            return json_response(
                200,
                {
                    **payload,
                    "data": {
                        **(payload.get("data") if isinstance(payload.get("data"), dict) else {}),
                        "robot": build_robot_payload(state, user_id=user_id)["data"],
                    },
                },
            )
        return json_response(200, build_controlled_upstream_error(detail))
    sync_user_store_from_payload(
        user_id,
        payload,
        connect_body.get("email"),
        is_new_connection=True,
    )
    connected, active_mode = extract_account_status(payload)
    if connected and active_mode != "REAL":
        logger.warning(
            "[REAL_MODE_NOT_CONFIRMED] user_id=%s active_mode=%s",
            user_id,
            active_mode,
        )
        state = auto_trader.require_real_mode(user_id)
        persist_robot(user_id)
        await stop_robot_worker(user_id)
        return json_response(
            200,
            {
                "ok": False,
                "data": {
                    "connected": connected,
                    "active_mode_from_bullex": active_mode,
                    "robot": build_robot_payload(state, user_id=user_id)["data"],
                },
                "error": "BULLEX_ACCOUNT_STILL_PRACTICE",
            },
        )
    if payload.get("ok") and connected:
        logger.info("[REAL_MODE_CONFIRMED] user_id=%s active_mode=%s", user_id, active_mode)
        state = auto_trader.get(user_id)
        state.account_mode = "REAL"
        state.allow_real = True
        state.confirm_real = True
        state = auto_trader.sync_connection(
            user_id,
            connected=True,
            active_mode=active_mode,
            source="bullex_service",
            align_status=True,
        )
        if user_id in robot_state_hydrated_users or state.enabled:
            persist_robot(user_id)
        logger.info(
            "[BULLEX_CONNECTED] user_id=%s active_mode=%s",
            user_id,
            active_mode,
        )
        logger.info(
            "[ROBOT_CONNECTION_SYNCED] user_id=%s connected=true active_mode=%s source=bullex_service",
            user_id,
            active_mode,
        )
        if state.enabled:
            logger.info("[ON_DEMAND_RESTORE_ONLY] user_id=%s worker_start=/robot/start", user_id)
        data = payload.get("data")
        if isinstance(data, dict):
            data["robot"] = build_robot_payload(
                state,
                connected=True,
                active_mode=active_mode,
                connection_checked_at=state.connection_checked_at.isoformat()
                if state.connection_checked_at is not None
                else None,
                connection_status_source=state.connection_status_source,
            )["data"]
        logger.info("[CONNECT_SUCCESS] user_id=%s active_mode=%s", user_id, active_mode)
    return json_response(status_code, payload)


@app.post("/bullex/connect")
async def bullex_connect(
    body: dict[str, Any],
    auth: dict[str, str] = Depends(require_headers),
) -> JSONResponse:
    try:
        return await _bullex_connect_impl(body, auth)
    except Exception as exc:
        logger.warning(
            "[CONNECT_FAILED_HANDLED] user_id=%s detail=%s",
            auth.get("user_id"),
            exc.__class__.__name__,
        )
        logger.warning(
            "[CONNECT_RECOVERED] user_id=%s reason=%s",
            auth.get("user_id"),
            exc.__class__.__name__,
            exc_info=True,
        )
        return json_response(200, build_controlled_upstream_error(exc))


async def _bullex_status_impl(auth: dict[str, str]) -> JSONResponse:
    user_id = auth["user_id"]
    mark_user_active(user_id)
    try:
        status_code, payload = await call_bullex_service("GET", "/sessions/status", user_id)
        payload = normalize_service_payload(payload)
    except Exception as exc:
        logger.warning(
            "[UPSTREAM_ERROR_HANDLED] user_id=%s path=/sessions/status reason=%s",
            user_id,
            exc.__class__.__name__,
            exc_info=True,
        )
        fallback = memory_status_fallback(user_id)
        fallback_data = fallback.get("data") if isinstance(fallback, dict) else None
        if isinstance(fallback_data, dict) and fallback_data.get("connected") is True:
            return json_response(200, fallback)
        return json_response(200, build_controlled_upstream_error(exc))
    if payload.get("ok") and isinstance(payload.get("data"), dict) and payload["data"].get("status") == "backoff":
        return json_response(200, payload)
    connected, active_mode = extract_account_status(payload)
    if payload.get("ok") and connected:
        if active_mode != "REAL":
            logger.warning(
                "[REAL_MODE_NOT_CONFIRMED] user_id=%s active_mode=%s",
                user_id,
                active_mode,
            )
            state = auto_trader.require_real_mode(user_id)
            state.connected = True
            state.active_mode = active_mode
            persist_robot(user_id)
            await stop_robot_worker(user_id)
        else:
            state = auto_trader.sync_connection(
                user_id,
                connected=True,
                active_mode=active_mode,
                source=connection_source_from_payload(payload),
                align_status=True,
            )
        data = payload.get("data")
        if isinstance(data, dict):
            data["robot"] = build_robot_payload(
                state,
                connected=True,
                active_mode=active_mode,
                connection_checked_at=state.connection_checked_at.isoformat()
                if state.connection_checked_at is not None
                else None,
                connection_status_source=state.connection_status_source,
            )["data"]
        return json_response(200, payload)

    fallback = memory_status_fallback(user_id)
    if fallback is not None:
        logger.warning(
            "[UPSTREAM_ERROR_HANDLED] user_id=%s path=/sessions/status reason=%s",
            user_id,
            payload.get("error") or "connected_false",
        )
        return json_response(200, fallback)
    if status_code == 404 or is_session_disconnected(payload) or payload_connected_state(payload) is False:
        return json_response(200, build_success({"connected": False}))
    return json_response(
        200,
        build_controlled_upstream_error(payload.get("error") or BULLEX_TEMPORARY_UNAVAILABLE),
    )


@app.get("/bullex/status")
async def bullex_status(auth: dict[str, str] = Depends(require_headers)) -> JSONResponse:
    try:
        return await _bullex_status_impl(auth)
    except Exception as exc:
        logger.warning(
            "[STATUS_RECOVERED] user_id=%s reason=%s",
            auth.get("user_id"),
            exc.__class__.__name__,
            exc_info=True,
        )
        return json_response(200, build_controlled_upstream_error(exc))


@app.get("/bullex/balance")
async def bullex_balance(auth: dict[str, str] = Depends(require_headers)) -> JSONResponse:
    return await _bullex_account_impl(auth)


@app.post("/bullex/change-mode")
async def bullex_change_mode(
    body: dict[str, Any],
    auth: dict[str, str] = Depends(require_headers),
) -> JSONResponse:
    status_code, payload = await call_bullex_service(
        "POST",
        "/account/change-mode",
        auth["user_id"],
        json_body={"mode": "REAL", "confirm_real": True},
    )
    sync_user_store_from_payload(auth["user_id"], payload)
    return json_response(status_code, payload)


@app.get("/bullex/assets")
async def bullex_assets(auth: dict[str, str] = Depends(require_headers)) -> JSONResponse:
    user_id = auth["user_id"]
    retry_remaining = assets_retry_remaining(user_id)
    if retry_remaining is not None:
        cached_payload = get_cached_assets_payload(user_id) or get_snapshot_assets_payload(user_id)
        if cached_payload is not None:
            logger.info(
                "[ASSETS_CACHE_HIT] user_id=%s source=%s retry_in=%.2f",
                user_id,
                ((cached_payload.get("meta") or {}).get("source") or "cache"),
                retry_remaining,
            )
            return json_response(200, cached_payload)

    status_code, payload = await call_bullex_service("GET", "/assets", user_id)
    payload = normalize_service_payload(payload)
    log_ignored_disconnect(user_id, "/assets", payload)
    if payload.get("ok") and isinstance(payload.get("data"), list):
        allowed_assets = normalize_allowed_assets_list(payload.get("data"))
        clear_assets_backoff(user_id)
        payload = build_assets_payload(allowed_assets, source="bullex_service", stale=False)
        get_session_cache(user_id).responses["/assets"] = BullexResponseCacheEntry(
            status_code=200,
            payload=payload,
            expires_at=utc_now() + timedelta(seconds=ASSETS_CACHE_TTL_SECONDS),
        )
        try:
            user_store.save_market_assets_snapshot(user_id, allowed_assets)
        except Exception:
            logger.exception("falha ao salvar snapshot de market_assets para %s", user_id)
        return json_response(200, payload)

    retry_seconds = schedule_assets_retry(user_id)
    cached_payload = get_cached_assets_payload(user_id) or get_snapshot_assets_payload(user_id)
    if cached_payload is not None:
        if account_still_connected(user_id):
            logger.info("[ACCOUNT_STILL_CONNECTED] user_id=%s source=assets_failure", user_id)
        logger.warning(
            "[ASSETS_FETCH_FAILED_USING_CACHE] user_id=%s retry_in=%ss error=%s",
            user_id,
            retry_seconds,
            payload.get("error"),
        )
        return json_response(200, cached_payload)

    if account_still_connected(user_id):
        logger.info("[ACCOUNT_STILL_CONNECTED] user_id=%s source=assets_failure_no_cache", user_id)
    logger.warning(
        "[ASSETS_FETCH_FAILED_USING_CACHE] user_id=%s retry_in=%ss error=%s source=empty_fallback",
        user_id,
        retry_seconds,
        payload.get("error"),
    )
    return json_response(
        200,
        build_assets_payload([], source="empty_fallback", stale=True),
    )


@app.get("/bullex/candles")
async def bullex_candles(
    active: str | None = None,
    interval: int | None = None,
    count: int | None = None,
    endtime: int | None = None,
    symbol: str | None = None,
    timeframe: str | None = None,
    limit: int | None = None,
    auth: dict[str, str] = Depends(require_headers),
) -> JSONResponse:
    resolved_symbol = normalize_binary_active(symbol or active or "")
    if resolved_symbol not in CHART_ALLOWED_ASSET_SET:
        logger.warning(
            "[CANDLES_ERROR_HANDLED] user_id=%s symbol=%s error=%s",
            auth["user_id"],
            resolved_symbol,
            ASSET_NOT_ALLOWED,
        )
        return json_response(200, build_chart_candles_unavailable())
    user_id = auth["user_id"]
    try:
        resolved_timeframe, resolved_interval = normalize_timeframe_seconds(timeframe, interval)
        resolved_limit = max(1, min(int(limit or count or 60), 500))
    except (TypeError, ValueError) as exc:
        logger.warning(
            "[CANDLES_ERROR_HANDLED] user_id=%s symbol=%s reason=%s",
            user_id,
            resolved_symbol,
            type(exc).__name__,
        )
        return json_response(200, build_chart_candles_unavailable())
    cache_key = (user_id, resolved_symbol, resolved_timeframe)
    params = {
        "active": resolved_symbol,
        "interval": resolved_interval,
        "count": resolved_limit,
    }
    if endtime is not None:
        params["endtime"] = endtime
    logger.info(
        "[CANDLES_FETCH] user_id=%s symbol=%s timeframe=%s count=%s",
        user_id,
        resolved_symbol,
        resolved_timeframe,
        resolved_limit,
    )
    timed_out = False
    try:
        status_code, payload = await asyncio.wait_for(
            call_bullex_service(
                "GET",
                "/candles",
                user_id,
                params=params,
            ),
            timeout=CANDLES_REQUEST_TIMEOUT_SECONDS,
        )
        payload = normalize_service_payload(
            payload,
            error="CANDLES_TEMPORARY_UNAVAILABLE",
        )
        if payload.get("ok"):
            server_timestamp = extract_server_timestamp(payload) or utc_now().timestamp()
            live_payload = build_live_candles_payload(
                resolved_symbol,
                resolved_timeframe,
                resolved_interval,
                resolved_limit,
                float(server_timestamp),
                payload,
            )
            chart_candles_cache[cache_key] = deepcopy(live_payload)
            logger.info(
                "[CANDLES_OK] user_id=%s symbol=%s candles=%s",
                user_id,
                resolved_symbol,
                len(live_payload["candles"]),
            )
            return json_response(
                200,
                build_chart_candles_success(
                    live_payload,
                    from_cache=False,
                    limit=resolved_limit,
                ),
            )
        logger.warning(
            "[CANDLES_ERROR_HANDLED] user_id=%s symbol=%s status=%s error=%s",
            user_id,
            resolved_symbol,
            status_code,
            payload.get("error"),
        )
    except asyncio.TimeoutError:
        timed_out = True
        logger.warning(
            "[CANDLES_TIMEOUT_HANDLED] user_id=%s symbol=%s timeout_seconds=%s",
            user_id,
            resolved_symbol,
            CANDLES_REQUEST_TIMEOUT_SECONDS,
        )
    except Exception as exc:
        logger.warning(
            "[CANDLES_ERROR_HANDLED] user_id=%s symbol=%s reason=%s",
            user_id,
            resolved_symbol,
            type(exc).__name__,
            exc_info=True,
        )

    cached = chart_candles_cache.get(cache_key)
    if cached is not None:
        logger.info(
            "[CANDLES_CACHE_RETURNED] user_id=%s symbol=%s timeframe=%s candles=%s",
            user_id,
            resolved_symbol,
            resolved_timeframe,
            len(cached.get("candles") or []),
        )
        return json_response(
            200,
            build_chart_candles_success(
                cached,
                from_cache=True,
                limit=resolved_limit,
            ),
        )
    if not timed_out:
        logger.warning(
            "[CANDLES_ERROR_HANDLED] user_id=%s symbol=%s error=CANDLES_TEMPORARY_UNAVAILABLE",
            user_id,
            resolved_symbol,
        )
    return json_response(200, build_chart_candles_unavailable())


@app.get("/debug/candles-live")
async def debug_candles_live(
    symbol: str,
    timeframe: str = "M1",
    auth: dict[str, str] = Depends(require_headers),
) -> JSONResponse:
    response = await bullex_candles(
        symbol=symbol,
        timeframe=timeframe,
        limit=60,
        auth=auth,
    )
    payload = json.loads(response.body)
    if not payload.get("ok"):
        return response
    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    candles = data.get("candles") if isinstance(data.get("candles"), list) else []
    last_candle = candles[-1] if candles else {}
    server_time = float(data.get("server_time") or utc_now().timestamp())
    last_candle_time = numeric_candle_time(last_candle) if isinstance(last_candle, dict) else None
    age_seconds = None if last_candle_time is None else max(0, round(server_time - last_candle_time, 3))
    _, interval = normalize_timeframe_seconds(timeframe)
    return json_response(
        200,
        build_success(
            {
                "symbol": normalize_binary_active(symbol),
                "timeframe": timeframe,
                "last_candle_time": int(last_candle_time) if last_candle_time is not None else None,
                "last_close": last_candle.get("close") if isinstance(last_candle, dict) else None,
                "server_time": server_time,
                "age_seconds": age_seconds,
                "is_realtime": bool(age_seconds is not None and age_seconds <= interval),
            }
        ),
    )


@app.get("/bullex/payouts")
async def bullex_payouts(
    active: str | None = None,
    auth: dict[str, str] = Depends(require_headers),
) -> JSONResponse:
    if active is not None and not is_binary_asset_allowed(active):
        return json_response(400, build_error(ASSET_NOT_ALLOWED))
    active = normalize_binary_active(active) if active is not None else None
    params = {"active": active} if active is not None else None
    user_id = auth["user_id"]
    status_code, payload = await call_bullex_service("GET", "/payouts", user_id, params=params)
    payload = normalize_service_payload(
        payload,
        error="PAYOUTS_TEMPORARY_UNAVAILABLE",
    )
    log_ignored_disconnect(user_id, "/payouts", payload)
    if payload.get("ok") and active and isinstance(payload.get("data"), list):
        payout_item = next(
            (
                item
                for item in payload["data"]
                if isinstance(item, dict) and item.get("symbol") == active and item.get("payout") is not None
            ),
            None,
        )
        if payout_item is not None:
            try:
                user_store.save_market_asset_payout(user_id, active, payout_item.get("payout"))
            except Exception:
                logger.exception("falha ao salvar payout de market_assets para %s %s", user_id, active)
    if payload.get("ok"):
        return json_response(200, payload)
    cached = cached_successful_response(user_id, build_cache_key("/payouts", params))
    if cached is not None:
        logger.info("[PAYOUT_CACHE_RETURNED] user_id=%s active=%s", user_id, active)
        return json_response(200, add_stale_warning(cached.payload))
    return json_response(
        200,
        {
            "ok": False,
            "data": [],
            "error": "PAYOUTS_TEMPORARY_UNAVAILABLE",
        },
    )


@app.post("/bullex/buy-demo")
async def bullex_buy_demo(
    body: dict[str, Any],
    auth: dict[str, str] = Depends(require_headers),
) -> JSONResponse:
    return json_response(409, build_error("REAL_MODE_ONLY"))


@app.post("/bullex/buy-real")
async def bullex_buy_real(
    body: dict[str, Any],
    auth: dict[str, str] = Depends(require_headers),
) -> JSONResponse:
    user_id = auth["user_id"]
    state = get_user_robot_state(user_id)
    logger.info(
        "[REAL MODE DETECTED] user_id=%s account_mode=%s confirm_real=%s",
        user_id,
        state.account_mode,
        body.get("confirm_real"),
    )
    logger.info(
        "[REAL BUY ATTEMPT] user_id=%s active=%s amount=%s",
        user_id,
        body.get("active"),
        body.get("amount"),
    )
    block_reason = real_buy_gateway_block_reason(user_id, state, body)
    if block_reason is not None:
        logger.warning("[REAL BUY BLOCKED reason=%s] user_id=%s", block_reason, user_id)
        return json_response(403, build_error(block_reason))

    status_code, payload = await call_bullex_service(
        "POST",
        "/orders/buy-real",
        user_id,
        json_body=body,
    )
    if payload.get("ok"):
        order_data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        logger.info("[REAL BUY SUCCESS order_id=%s] user_id=%s", order_data.get("order_id"), user_id)
    else:
        logger.warning(
            "[REAL BUY BLOCKED reason=%s] user_id=%s",
            payload.get("error") or "ORDER_FAILED",
            user_id,
        )
    return json_response(status_code, payload)


@app.post("/bullex/disconnect")
async def bullex_disconnect(auth: dict[str, str] = Depends(require_headers)) -> JSONResponse:
    user_id = auth["user_id"]
    status_code, payload = await call_bullex_service("POST", "/sessions/disconnect", user_id)
    payload = normalize_service_payload(payload)
    mark_session_failure(user_id, offline=True)
    auto_trader.disconnect_account(user_id)
    await stop_robot_worker(user_id)
    await manager.disconnect_user(user_id)
    if payload.get("ok"):
        user_store.disconnect(user_id)
    else:
        sync_user_store_from_payload(user_id, payload)
    return json_response(status_code, payload)


@app.post("/bullex/reconnect")
async def bullex_reconnect(auth: dict[str, str] = Depends(require_headers)) -> JSONResponse:
    clear_session_backoff(auth["user_id"])
    status_code, payload = await call_bullex_service("POST", "/sessions/reconnect", auth["user_id"])
    sync_user_store_from_payload(auth["user_id"], payload)
    return json_response(status_code, payload)


async def _bullex_account_impl(auth: dict[str, str]) -> JSONResponse:
    user_id = auth["user_id"]
    mark_user_active(user_id)
    try:
        status_code, payload = await call_bullex_service("GET", "/account", user_id)
        payload = normalize_service_payload(
            payload,
            error="ACCOUNT_TEMPORARY_UNAVAILABLE",
        )
    except Exception as exc:
        logger.warning(
            "[UPSTREAM_ERROR_HANDLED] user_id=%s path=/account reason=%s",
            user_id,
            exc.__class__.__name__,
            exc_info=True,
        )
        fallback = memory_account_fallback(user_id)
        if fallback is not None:
            logger.warning(
                "[ACCOUNT_FETCH_FALLBACK] user_id=%s source=memory reason=%s",
                user_id,
                exc.__class__.__name__,
            )
            contract = build_real_account_contract(fallback)
        else:
            contract = build_real_account_contract(build_success({"connected": False}))
        if not contract.get("ok"):
            state = auto_trader.require_real_mode(user_id)
            persist_robot(user_id)
            await stop_robot_worker(user_id)
            contract["data"]["robot"] = build_robot_payload(state, user_id=user_id)["data"]
        return json_response(200, contract)
    if payload.get("ok") and isinstance(payload.get("data"), dict) and payload["data"].get("status") == "backoff":
        return json_response(200, payload)
    if isinstance(payload.get("data"), dict):
        contract = build_real_account_contract(payload)
        data = contract["data"]
        if contract.get("ok"):
            sync_user_store_from_payload(user_id, contract)
            auto_trader.sync_connection(
                user_id,
                connected=True,
                active_mode="REAL",
                source=connection_source_from_payload(payload),
                align_status=True,
            )
        else:
            state = auto_trader.require_real_mode(user_id)
            persist_robot(user_id)
            await stop_robot_worker(user_id)
            data["robot"] = build_robot_payload(state, user_id=user_id)["data"]
        return json_response(200, contract)
    fallback = memory_account_fallback(user_id)
    if fallback is not None:
        logger.warning(
            "[ACCOUNT_FETCH_FALLBACK] user_id=%s source=memory reason=%s",
            user_id,
            payload.get("error") or "connected_false",
        )
        contract = build_real_account_contract(fallback)
        if not contract.get("ok"):
            state = auto_trader.require_real_mode(user_id)
            persist_robot(user_id)
            await stop_robot_worker(user_id)
            contract["data"]["robot"] = build_robot_payload(state, user_id=user_id)["data"]
        return json_response(200, contract)
    if status_code == 404 or is_session_disconnected(payload) or payload_connected_state(payload) is False:
        contract = build_real_account_contract(build_success({"connected": False}))
    else:
        contract = build_real_account_contract(payload)
    state = auto_trader.require_real_mode(user_id)
    persist_robot(user_id)
    await stop_robot_worker(user_id)
    contract["data"]["robot"] = build_robot_payload(state, user_id=user_id)["data"]
    return json_response(200, contract)


@app.get("/bullex/account")
async def bullex_account(auth: dict[str, str] = Depends(require_headers)) -> JSONResponse:
    try:
        return await _bullex_account_impl(auth)
    except Exception as exc:
        logger.warning(
            "[ACCOUNT_RECOVERED] user_id=%s reason=%s",
            auth.get("user_id"),
            exc.__class__.__name__,
            exc_info=True,
        )
        return json_response(200, build_controlled_upstream_error(exc))


@app.get("/bullex/order-result/{order_id}")
async def bullex_order_result(order_id: str, auth: dict[str, str] = Depends(require_headers)) -> JSONResponse:
    status_code, payload = await call_bullex_service("GET", f"/orders/{order_id}/result", auth["user_id"])
    return json_response(status_code, payload)
