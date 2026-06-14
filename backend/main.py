import logging
import math
import os
import asyncio
import json
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from backend.auto_trader import (
    AutoTrader,
    RobotConfigUpdate,
    STATUS_ACCOUNT_DISCONNECTED,
    STATUS_ANALYZING,
    STATUS_ORDER_REJECTED,
    STATUS_PENDING_RESULT,
    STATUS_SENDING_ORDER,
    STATUS_WAITING_ANALYSIS_WINDOW,
    STATUS_WAITING_ENTRY_WINDOW,
    STATUS_WAITING_NEXT_CYCLE,
    parse_datetime,
    utc_now,
)
from backend.openai_signal_reviewer import review_signal
from backend.robot_persistence import (
    RobotPersistence,
    create_robot_persistence,
    extract_robot_settings,
)
from backend.signal_engine import analyze_signal
from backend.trade_result_monitor import TradeResultMonitor
from backend.user_store import UserStore, create_user_store


logger = logging.getLogger("backend-gateway")

ASSET_NOT_ALLOWED = "ASSET_NOT_ALLOWED"
SESSION_NOT_FOUND = "SESSION_NOT_FOUND"
SESSION_DISCONNECTED = "SESSION_DISCONNECTED"
LOW_QUALITY_SIGNAL = "Sinal bloqueado por baixa qualidade"
MAX_ORDER_ATTEMPTS_PER_CYCLE = 3
NO_AVAILABLE_ASSET_ERROR = "Nenhum ativo disponível no momento da compra."
CRITICAL_TRADE_BLOCKS = {
    "ACCOUNT_DISCONNECTED",
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


def build_success(data: Any) -> dict[str, Any]:
    return {"ok": True, "data": data, "error": None}


def build_error(message: str) -> dict[str, Any]:
    return {"ok": False, "data": None, "error": message}


def normalize_binary_active(active: str) -> str:
    return (active or "").strip().upper()


def is_binary_asset_allowed(active: str) -> bool:
    return normalize_binary_active(active) in BINARY_ALLOWED_ASSET_SET


class ConnectionManager:
    def __init__(self) -> None:
        self._connections: dict[tuple[str, str], set[WebSocket]] = {}
        self._lock = asyncio.Lock()

    async def connect(self, user_id: str, active: str, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            key = (user_id, active)
            websockets = self._connections.setdefault(key, set())
            websockets.add(websocket)

    async def disconnect(self, user_id: str, active: str, websocket: WebSocket) -> None:
        async with self._lock:
            key = (user_id, active)
            websockets = self._connections.get(key)
            if not websockets:
                return
            websockets.discard(websocket)
            if not websockets:
                self._connections.pop(key, None)

    async def broadcast_to_user_active(self, user_id: str, active: str, payload: dict[str, Any]) -> None:
        async with self._lock:
            targets = list(self._connections.get((user_id, active), set()))
        for websocket in targets:
            try:
                await websocket.send_json(payload)
            except Exception:
                logger.exception("falha ao enviar payload WS para %s %s", user_id, active)
                await self.disconnect(user_id, active, websocket)


manager = ConnectionManager()


class GatewayConfig:
    def __init__(self) -> None:
        self.bullex_service_url = os.getenv("BULLEX_SERVICE_URL", "http://bullex-service:8000").rstrip("/")
        self.panel_api_key = os.getenv("PANEL_API_KEY", "")
        self.supabase_url = os.getenv("SUPABASE_URL", "").strip()
        self.supabase_service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
        self.robot_real_max_entry = float(os.getenv("ROBOT_REAL_MAX_ENTRY", "10"))
        self.cors_origins = [
            origin.strip()
            for origin in os.getenv(
                "CORS_ORIGINS",
                "https://www.elcapobot.online,https://elcapobot.online,http://localhost:5173",
            ).split(",")
            if origin.strip()
        ]


config = GatewayConfig()
app = FastAPI(title="backend-gateway", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
user_store: UserStore = create_user_store()
auto_trader = AutoTrader()
robot_persistence: RobotPersistence = create_robot_persistence()
robot_tasks: dict[str, asyncio.Task[None]] = {}
robot_worker_last_tick_at: dict[str, datetime] = {}
CONNECTION_GRACE_SECONDS = 30


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
        value = candle.get("from") or candle.get("at") or candle.get("id")
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
        "timeframe": timeframe,
        "server_time": server_time,
        "candles": candles[-limit:],
    }


def is_session_disconnected(payload: dict[str, Any]) -> bool:
    error = str(payload.get("error") or "").strip().upper()
    return error in {"SESSION_NOT_FOUND", "SESSION_DISCONNECTED"}


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
async def unhandled_error_handler(_: Request, __: Exception) -> JSONResponse:
    return JSONResponse(status_code=500, content=build_error("INTERNAL_ERROR"))


async def call_bullex_service(
    method: str,
    path: str,
    user_id: str,
    json_body: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
) -> tuple[int, dict[str, Any]]:
    headers = {"x-user-id": user_id}
    url = f"{config.bullex_service_url}{path}"

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.request(
                method=method,
                url=url,
                headers=headers,
                json=json_body,
                params=params,
            )
    except httpx.HTTPError:
        return 502, build_error("BULLEX_SERVICE_UNAVAILABLE")

    try:
        payload = response.json()
    except ValueError:
        payload = build_error("INVALID_BULLEX_RESPONSE")

    if not isinstance(payload, dict) or "ok" not in payload or "data" not in payload or "error" not in payload:
        payload = build_success(payload) if response.is_success else build_error("INVALID_BULLEX_RESPONSE")

    return response.status_code, payload


def json_response(status_code: int, payload: dict[str, Any]) -> JSONResponse:
    return JSONResponse(status_code=status_code, content=payload)


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
        "requires_2fa": "requires_2fa",
    }
    for source_field, target_field in field_map.items():
        if source_field in data:
            updates[target_field] = data[source_field]
    if updates.get("connected") is True:
        updates["last_connected_at"] = datetime.now(timezone.utc).isoformat()
    return updates


def sync_user_store_from_payload(
    user_id: str,
    payload: dict[str, Any],
    fallback_email: str | None = None,
    *,
    is_new_connection: bool = False,
) -> None:
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
    if payload.get("error") not in {SESSION_DISCONNECTED, SESSION_NOT_FOUND}:
        return
    try:
        user_store.disconnect(user_id)
    except Exception:
        logger.exception("falha ao marcar sessao desconectada para %s", user_id)


def extract_account_status(payload: dict[str, Any]) -> tuple[bool, str | None]:
    data = payload.get("data")
    if not payload.get("ok") or not isinstance(data, dict):
        return False, None
    connected = bool(data.get("connected"))
    mode = data.get("active_mode") or data.get("mode")
    return connected, str(mode).strip().upper() if mode else None


def connection_source_from_payload(payload: dict[str, Any], *, default: str = "bullex_service") -> str:
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
        state = auto_trader.disconnect_account(user_id)
        state.active_mode = account_active_mode or active_mode
        mark_disconnected_from_payload(user_id, payload)
        mark_disconnected_from_payload(user_id, account_payload)
        logger.warning(
            "[CONNECTION_CONFIRMED_DISCONNECTED] user_id=%s failures=%s account_status=%s",
            user_id,
            state.connection_failure_count,
            account_status,
        )
        return state, False, state.active_mode, "disconnected"

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


async def fetch_and_sync_robot_connection(user_id: str) -> tuple[int, dict[str, Any], Any, bool, str | None, str]:
    status_code, payload = await call_bullex_service("GET", "/sessions/status", user_id)
    state, connected, active_mode, source = await reconcile_robot_connection_from_payload(user_id, payload)
    return status_code, payload, state, connected, active_mode, source


def fresh_robot_connection(state: Any, *, max_age_seconds: int = 3) -> bool:
    checked_at = getattr(state, "connection_checked_at", None)
    if not getattr(state, "connected", False) or checked_at is None:
        return False
    age = (utc_now() - checked_at).total_seconds()
    return 0 <= age <= max_age_seconds


def cached_robot_connection_payload(state: Any) -> dict[str, Any]:
    return build_success(
        {
            "connected": True,
            "active_mode": getattr(state, "active_mode", None),
            "server_time": None,
            "connection_status_source": getattr(state, "connection_status_source", "cached"),
        }
    )


TIMEFRAME_SECONDS = {"M1": 60, "M5": 300, "M15": 900, "M30": 1800}
ENTRY_WINDOWS = {
    "M1": (25, 29),
    "M5": (265, 269),
    "M15": (865, 869),
    "M30": (1765, 1769),
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
    elif seconds_in_candle < window_start:
        seconds_until_entry_window = math.ceil(window_start - seconds_in_candle)
    else:
        seconds_until_entry_window = 0

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


def real_block_reason(state: Any, *, connected: bool, active_mode: str | None) -> str | None:
    if state.account_mode != "REAL":
        return "ACCOUNT_MODE_NOT_REAL"
    if not connected:
        return "BULLEX_NOT_CONNECTED"
    if active_mode != "REAL":
        return "BULLEX_ACTIVE_MODE_NOT_REAL"
    if not state.allow_real:
        return "ALLOW_REAL_REQUIRED"
    if not state.confirm_real:
        return "CONFIRM_REAL_REQUIRED"
    if state.entry_value > config.robot_real_max_entry:
        return "REAL_ENTRY_VALUE_EXCEEDS_MAX"
    return None


def extract_payout(payload: dict[str, Any], symbol: str) -> float | None:
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


def daily_stop_reason(user_id: str, state: Any) -> str | None:
    today = datetime.now(timezone.utc).date()
    daily_profit = 0.0
    daily_loss = 0.0
    for trade in auto_trader.history(user_id).get("trades", []):
        finished_at = parse_datetime(trade.get("finished_at"))
        if finished_at is None or finished_at.date() != today:
            continue
        trade_profit = float(trade.get("profit") or 0)
        daily_profit += trade_profit
        if trade_profit < 0:
            daily_loss += abs(trade_profit)
    if state.stop_loss > 0 and daily_loss >= state.stop_loss:
        return "STOP_LOSS_HIT"
    if state.stop_win > 0 and daily_profit >= state.stop_win:
        return "STOP_WIN_HIT"
    return None


def asset_cooldown_reason(user_id: str, symbol: str) -> str | None:
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
        return "STOP_WIN_HIT"
    if state.stop_loss > 0 and state.profit <= -state.stop_loss:
        return "STOP_LOSS_HIT"
    return None


def build_robot_payload(state: Any, **extra: Any) -> dict[str, Any]:
    data = state.to_dict()
    user_id = extra.pop("user_id", None)
    if user_id is not None:
        worker_task = robot_tasks.get(str(user_id))
        last_tick_at = robot_worker_last_tick_at.get(str(user_id))
        data["worker_running"] = bool(worker_task is not None and not worker_task.done())
        data["worker_last_tick_at"] = last_tick_at.isoformat() if last_tick_at is not None else None
    data.update(extra)
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
            "[SERVER_TIME_FALLBACK] user_id=%s reason=ANALYSIS_ERROR current_candle_seconds=%s",
            user_id,
            window["current_candle_seconds"],
        )
    friendly_error = readable_order_error(error)
    state = auto_trader.reject_order(
        user_id,
        "ANALYSIS_ERROR",
        last_order_error=friendly_error,
    )
    state.last_order_error = friendly_error
    logger.error("[ANALYSIS_ERROR] user_id=%s error=%s", user_id, friendly_error)
    logger.info("[ANALYSIS_ERROR_RECOVERED] user_id=%s next_cycle_at=%s", user_id, state.next_cycle_at)
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
        used.append("Ultimos Candles")
    if "VOLATILITY" in approved or "atr_pct" in candidate:
        used.append("Volatilidade")
    if candidate.get("payout") is not None:
        used.append("Payout")
    if not used:
        used = ["Score de Estrategias", "Payout"]

    strategy_name = "Confluência " + " + ".join(used)
    strategy_reason = str(
        candidate.get("reason")
        or candidate.get("signal_explanation")
        or "Maior score entre os ativos analisados."
    ).strip()
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


def persist_robot(user_id: str) -> None:
    try:
        state = auto_trader.get(user_id)
        state_payload = state.to_dict()
        robot_persistence.save_state(user_id, state_payload)
        save_settings = getattr(robot_persistence, "save_settings", None)
        if callable(save_settings):
            save_settings(user_id, extract_robot_settings(state_payload))
        if state.last_trade:
            robot_persistence.save_trade(user_id, state.last_trade)
    except Exception:
        logger.exception("[ROBOT PERSISTENCE ERROR] user_id=%s", user_id)


def robot_persistence_source() -> str:
    if robot_persistence.__class__.__name__ == "SupabaseRobotPersistence":
        return "supabase"
    return "memory"


def get_user_robot_state(user_id: str) -> Any:
    if auto_trader.has_state(user_id):
        return auto_trader.get(user_id)
    try:
        load_settings = getattr(robot_persistence, "load_settings", None)
        settings = load_settings(user_id) if callable(load_settings) else None
        payload = robot_persistence.load_state(user_id)
        if payload is not None:
            if settings is not None:
                payload = {**payload, **settings}
            trades = robot_persistence.load_trades(user_id)
            return auto_trader.restore(
                user_id,
                payload,
                trades,
                source=robot_persistence_source(),
            )
        if settings is not None:
            state = auto_trader.get(user_id)
            for field, value in settings.items():
                if hasattr(state, field):
                    setattr(state, field, value)
            auto_trader.mark_source(user_id, robot_persistence_source())
            return state
    except Exception:
        logger.exception("[ROBOT USER LOAD ERROR] user_id=%s", user_id)
    return auto_trader.get(user_id)


async def analyze_active_signal(
    user_id: str,
    symbol: str,
    timeframe: str = "M1",
    endtime: int | None = None,
    strategy_mode: str = "conservative",
) -> tuple[int, dict[str, Any]]:
    interval = TIMEFRAME_SECONDS[timeframe]
    candle_params: dict[str, Any] = {
        "active": symbol,
        "interval": interval,
        "count": 100,
    }
    if endtime is not None:
        candle_params["endtime"] = endtime
    status_code, payload = await call_bullex_service(
        "GET",
        "/candles",
        user_id,
        params=candle_params,
    )
    mark_disconnected_from_payload(user_id, payload)
    if not payload.get("ok"):
        if is_session_disconnected(payload):
            return 409, build_error(SESSION_DISCONNECTED)
        return status_code, payload

    payout_status, payout_payload = await call_bullex_service(
        "GET",
        "/payouts",
        user_id,
        params={"active": symbol},
    )
    mark_disconnected_from_payload(user_id, payout_payload)
    payout = extract_payout(payout_payload, symbol) if payout_payload.get("ok") else None
    signal = analyze_signal(
        symbol,
        extract_candles(payload),
        timeframe=timeframe,
        strategy_mode=strategy_mode,
        payout=payout,
    )
    if payout_status >= 400 and payout is None:
        signal["blocked_filters"] = list(signal.get("blocked_filters") or []) + ["PAYOUT_UNAVAILABLE"]
        signal["trade_allowed"] = False
        signal["quality_reason"] = LOW_QUALITY_SIGNAL
    logger.info("[SIGNAL ANALYZE] %s %s %s", symbol, signal["signal"], signal["confidence"])
    return 200, build_success(signal)


async def scan_local_signals(
    user_id: str,
    limit: int = 5,
    include_wait: bool = False,
    timeframe: str = "M1",
    endtime: int | None = None,
    strategy_mode: str = "conservative",
) -> tuple[int, dict[str, Any]]:
    logger.info("[SIGNAL SCAN START]")
    signals = []

    for symbol in BINARY_ALLOWED_ASSETS:
        try:
            status_code, payload = await analyze_active_signal(
                user_id,
                symbol,
                timeframe=timeframe,
                endtime=endtime,
                strategy_mode=strategy_mode,
            )
            if not payload.get("ok"):
                if is_session_disconnected(payload):
                    logger.warning("[SIGNAL ERROR] %s %s", symbol, payload.get("error"))
                    return status_code, payload
                logger.warning("[SIGNAL ERROR] %s %s", symbol, payload.get("error"))
                continue

            signal = payload["data"]
            if signal["confidence"] < 70 and not include_wait:
                continue
            if (signal["signal"] == "WAIT" or not signal.get("trade_allowed", True)) and not include_wait:
                continue
            signals.append(signal)
        except Exception as exc:
            logger.exception("[SIGNAL ERROR] %s %s", symbol, exc)
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


async def select_fallback_candidate(
    user_id: str,
    state: Any,
    *,
    endtime: int | None = None,
) -> dict[str, Any] | None:
    for symbol in BINARY_ALLOWED_ASSETS:
        try:
            payout_status, payout_payload = await call_bullex_service(
                "GET",
                "/payouts",
                user_id,
                params={"active": symbol},
            )
            mark_disconnected_from_payload(user_id, payout_payload)
            payout = (
                extract_payout(payout_payload, symbol)
                if payout_status < 400 and payout_payload.get("ok")
                else None
            )
            if payout is None:
                continue

            candle_params: dict[str, Any] = {
                "active": symbol,
                "interval": TIMEFRAME_SECONDS[state.timeframe],
                "count": 5,
            }
            if endtime is not None:
                candle_params["endtime"] = endtime
            candle_status, candle_payload = await call_bullex_service(
                "GET",
                "/candles",
                user_id,
                params=candle_params,
            )
            mark_disconnected_from_payload(user_id, candle_payload)
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
            "strategy_score": 1,
            "score": 1,
            "quality_score": 1,
            "confidence": 1,
            "payout": payout,
            "reason": "Fallback operacional pelo movimento simples das ultimas velas.",
            "entry_reason": "Fallback operacional pelo movimento simples das ultimas velas.",
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
    if not force and state.last_analysis_at is not None:
        elapsed = (now - state.last_analysis_at).total_seconds()
        if elapsed < 3:
            return state

    logger.info(
        "[CYCLE_ANALYSIS_TICK] user_id=%s cycle_id=%s seconds_until_next_cycle=%s",
        user_id,
        state.cycle_id,
        state.to_dict()["seconds_until_next_cycle"],
    )
    scan_status, scan_payload = await scan_local_signals(
        user_id,
        limit=len(BINARY_ALLOWED_ASSETS),
        include_wait=True,
        timeframe=state.timeframe,
        endtime=int(entry_window["server_timestamp"]),
        strategy_mode=state.strategy_mode,
    )
    if scan_payload.get("ok"):
        signals = [item for item in scan_payload.get("data", []) if isinstance(item, dict)]
    else:
        logger.warning(
            "[ANALYSIS_ERROR_RECOVERED] user_id=%s error=%s action=FALLBACK_CANDIDATE",
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
        payout = raw_signal.get("payout")
        if payout is None and is_binary_asset_allowed(symbol):
            payout_status, payout_payload = await call_bullex_service(
                "GET",
                "/payouts",
                user_id,
                params={"active": symbol},
            )
            mark_disconnected_from_payload(user_id, payout_payload)
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

    selectable = [
        candidate
        for candidate in candidates
        if candidate.get("trade_allowed")
        and candidate.get("direction") in {"CALL", "PUT"}
        and candidate_pre_order_block_reason(candidate) is None
    ]
    selected = (
        max(
            selectable,
            key=lambda item: (
                int(item.get("strategy_score") or 0),
                int(item.get("confidence") or 0),
                float(item.get("payout") or 0),
            ),
        )
        if selectable
        else None
    )
    previous_symbol = (state.best_candidate or {}).get("symbol") if state.best_candidate else None
    state = auto_trader.set_analysis_candidates(user_id, candidates, selected)
    if selected is not None and selected.get("symbol") != previous_symbol:
        logger.info(
            "[BEST_CANDIDATE_UPDATED] user_id=%s symbol=%s direction=%s strategy_score=%s",
            user_id,
            selected.get("symbol"),
            selected.get("direction"),
            selected.get("strategy_score"),
        )
    return state


async def fetch_trade_result(user_id: str, order_id: str) -> tuple[int, dict[str, Any]]:
    status_code, payload = await call_bullex_service("GET", f"/orders/{order_id}/result", user_id)
    mark_disconnected_from_payload(user_id, payload)
    return status_code, payload


async def finish_monitored_trade(user_id: str, order_id: str, result: str, profit: float) -> None:
    async with auto_trader.lock(user_id):
        finalized, state = auto_trader.finish_trade(user_id, order_id, result, profit)
        if finalized and state.last_trade:
            logger.info(
                "[RESULT_RECEIVED] user_id=%s order_id=%s result=%s profit=%s",
                user_id,
                order_id,
                result,
                state.last_trade.get("profit"),
            )
            try:
                robot_persistence.save_trade_history(user_id, state.last_trade)
            except Exception:
                logger.exception(
                    "[ROBOT HISTORY ERROR] user_id=%s order_id=%s",
                    user_id,
                    order_id,
                )
            logger.info(
                "[RESULT_DISPLAY_UNTIL] user_id=%s result_display_until=%s",
                user_id,
                state.result_display_until,
            )
        persist_robot(user_id)


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
    async with auto_trader.lock(user_id):
        state = auto_trader.get(user_id)
        had_pending_signal = state.pending_signal is not None
        running_analysis = state.analysis_result == "RUNNING" or state.last_analysis_result == "RUNNING"
        if not had_pending_signal and not running_analysis:
            can_run, state = auto_trader.prepare_cycle(user_id)
            if not can_run:
                if state.status != STATUS_WAITING_NEXT_CYCLE or not state.enabled:
                    return 200, build_robot_payload(state)
            elif state.to_dict()["seconds_until_next_cycle"] <= 0:
                logger.info(
                    "[CYCLE_DUE] user_id=%s cycle_id=%s current_cycle_started_at=%s",
                    user_id,
                    state.cycle_id,
                    state.current_cycle_started_at,
                )

        logger.info("[ROBOT TICK] user_id=%s", user_id)
        try:
            active_stop_reason = daily_stop_reason(user_id, state) or robot_stop_reason(state)
            if active_stop_reason is not None:
                if active_stop_reason.startswith("DAILY_STOP"):
                    state.enabled = False
                    logger.warning("[DAILY_STOP_HIT] user_id=%s reason=%s", user_id, active_stop_reason)
                state = auto_trader.reject(user_id, active_stop_reason)
                logger.info("[ROBOT SIGNAL REJECTED] user_id=%s reason=%s", user_id, active_stop_reason)
                return 200, build_robot_payload(state)

            if required_mode is not None and state.account_mode != required_mode:
                return 409, build_error(f"ACCOUNT_MODE_NOT_{required_mode}")

            status_code, account_payload, entry_window = await refresh_entry_window(user_id, state)
            state, connected, active_mode, connection_source = await reconcile_robot_connection_from_payload(
                user_id,
                account_payload,
            )
            if state.account_mode == "REAL":
                logger.info(
                    "[REAL_TRADE_ATTEMPT] user_id=%s entry_value=%s",
                    user_id,
                    state.entry_value,
                )
                block_reason = real_block_reason(
                    state,
                    connected=connected,
                    active_mode=active_mode,
                )
                if block_reason is not None:
                    auto_trader.lock_real(user_id, block_reason)
                    logger.warning(
                        "[REAL_TRADE_BLOCKED] user_id=%s reason=%s",
                        user_id,
                        block_reason,
                    )
                    return 403, build_error(block_reason)

            expected_bullex_mode = "PRACTICE" if state.account_mode == "DEMO" else "REAL"
            if not connected:
                if state.status == STATUS_ACCOUNT_DISCONNECTED:
                    mark_disconnected_from_payload(user_id, account_payload)
                    logger.warning("[ROBOT_BLOCKED_ACCOUNT_DISCONNECTED] user_id=%s", user_id)
                return 200, build_robot_payload(
                    state,
                    connected=False,
                    active_mode=active_mode,
                    connection_checked_at=state.connection_checked_at.isoformat()
                    if state.connection_checked_at is not None
                    else None,
                    connection_status_source=connection_source,
                )
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
                state.status = STATUS_SENDING_ORDER
                state.rejection_reason = None
            seconds_until_next_cycle = state.to_dict()["seconds_until_next_cycle"]
            if (
                selected is None
                and state.enabled
                and state.status == STATUS_WAITING_NEXT_CYCLE
                and not state.operation_in_progress
            ):
                if seconds_until_next_cycle > 0:
                    state = await update_cycle_analysis(user_id, state, entry_window)
                    logger.info(
                        "[ROBOT WAITING NEXT CYCLE] user_id=%s next_cycle_at=%s",
                        user_id,
                        state.next_cycle_at,
                    )
                    return 200, build_robot_payload(state)

                logger.info(
                    "[CYCLE_ENTRY_DUE] user_id=%s cycle_id=%s",
                    user_id,
                    state.cycle_id,
                )
                if state.best_candidate is None:
                    state = await update_cycle_analysis(user_id, state, entry_window, force=True)
                selected = dict(state.best_candidate) if state.best_candidate else None
                if selected is None:
                    state = auto_trader.reject_order(
                        user_id,
                        "NO_AVAILABLE_ASSET",
                        last_order_error=NO_AVAILABLE_ASSET_ERROR,
                    )
                    logger.error(
                        "[ORDER_SEND_FAILED] user_id=%s active=%s error=%s",
                        user_id,
                        None,
                        "NO_AVAILABLE_ASSET",
                    )
                    logger.info(
                        "[NEXT_CYCLE_SCHEDULED] user_id=%s next_cycle_at=%s",
                        user_id,
                        state.next_cycle_at,
                    )
                    return 200, build_robot_payload(state)
            if (
                selected is not None
                and state.status == STATUS_WAITING_ENTRY_WINDOW
                and not entry_window["entry_window_open"]
            ):
                if not entry_window["missed_entry_window"]:
                    logger.info(
                        "[ENTRY_WINDOW_WAIT] user_id=%s server_time=%s timeframe=%s "
                        "seconds_in_candle=%s seconds_until_entry_window=%s "
                        "window_start=%s window_end=%s",
                        user_id,
                        entry_window["server_time"],
                        state.timeframe,
                        entry_window["current_candle_seconds"],
                        entry_window["seconds_until_entry_window"],
                        entry_window["entry_window_start_second"],
                        entry_window["entry_window_end_second"],
                    )
                    return 200, build_robot_payload(state)
                logger.info(
                    "[PENDING_SIGNAL_CLEARED] user_id=%s symbol=%s reason=MISSED_ENTRY_WINDOW",
                    user_id,
                    selected.get("symbol"),
                )
                state = auto_trader.reject_strategy(
                    user_id,
                    "MISSED_ENTRY_WINDOW",
                    blocked_filters=["MISSED_ENTRY_WINDOW"],
                    approved_filters=list(selected.get("approved_filters") or []),
                    quality_score=int(selected.get("quality_score") or 0),
                )
                logger.info(
                    "[MISSED_ENTRY_WINDOW] user_id=%s timeframe=%s "
                    "current_candle_seconds=%s window_end=%s",
                    user_id,
                    state.timeframe,
                    entry_window["current_candle_seconds"],
                    entry_window["entry_window_end_second"],
                )
                logger.info(
                    "[NEXT_CYCLE_SCHEDULED] user_id=%s next_cycle_at=%s",
                    user_id,
                    state.next_cycle_at,
                )
                return 200, build_robot_payload(state)

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
                scan_status, scan_payload = await scan_local_signals(
                    user_id,
                    limit=len(BINARY_ALLOWED_ASSETS),
                    include_wait=True,
                    timeframe=state.timeframe,
                    endtime=int(entry_window["server_timestamp"]),
                    strategy_mode=state.strategy_mode,
                )
                scan_error = None
                if not scan_payload.get("ok"):
                    scan_error = str(scan_payload.get("error") or "SIGNAL_SCAN_FAILED")
                    logger.warning(
                        "[ANALYSIS_ERROR_RECOVERED] user_id=%s error=%s action=FALLBACK_CANDIDATE",
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
                        mark_disconnected_from_payload(user_id, payout_payload)
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
                            "ACCOUNT_DISCONNECTED",
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
                    return 200, build_robot_payload(state)

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
                    "[PENDING_SIGNAL_SET] user_id=%s symbol=%s signal=%s confidence=%s "
                    "payout=%s timeframe=%s",
                    user_id,
                    selected.get("symbol"),
                    selected.get("signal"),
                    selected.get("confidence"),
                    selected.get("payout"),
                    selected.get("timeframe"),
                )
                logger.info(
                    "[WAITING_ENTRY_WINDOW] user_id=%s symbol=%s cycle_id=%s",
                    user_id,
                    selected.get("symbol"),
                    state.cycle_id,
                )

                timing_status, timing_payload, entry_window = await refresh_entry_window(user_id, state)
                state, connected, active_mode, connection_source = await reconcile_robot_connection_from_payload(
                    user_id,
                    timing_payload,
                )
                if not connected or active_mode != expected_bullex_mode:
                    if not connected and state.status != STATUS_ACCOUNT_DISCONNECTED:
                        return 200, build_robot_payload(
                            state,
                            connected=False,
                            active_mode=active_mode,
                            connection_checked_at=state.connection_checked_at.isoformat()
                            if state.connection_checked_at is not None
                            else None,
                            connection_status_source=connection_source,
                        )
                    reason = (
                        "ACCOUNT_DISCONNECTED"
                        if not connected
                        else f"ACCOUNT_MODE_MUST_BE_{expected_bullex_mode}"
                    )
                    state = (
                        auto_trader.disconnect_account(user_id)
                        if reason == "ACCOUNT_DISCONNECTED"
                        else auto_trader.reject(user_id, reason)
                    )
                    return 200, build_robot_payload(state)
                if entry_window is None:
                    state = recover_analysis_error_to_window(user_id, "SERVER_TIME_UNAVAILABLE")
                    return timing_status, build_robot_payload(state)
                if not entry_window["entry_window_open"]:
                    if entry_window["missed_entry_window"]:
                        logger.info(
                            "[PENDING_SIGNAL_CLEARED] user_id=%s symbol=%s "
                            "reason=WAITING_NEXT_ANALYSIS_WINDOW",
                            user_id,
                            selected.get("symbol"),
                        )
                        state = auto_trader.wait_analysis_window(
                            user_id,
                            entry_window,
                            clear_pending=True,
                        )
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
                        "[ENTRY_WINDOW_WAIT] user_id=%s server_time=%s timeframe=%s "
                        "seconds_in_candle=%s seconds_until_entry_window=%s "
                        "window_start=%s window_end=%s",
                        user_id,
                        entry_window["server_time"],
                        state.timeframe,
                        entry_window["current_candle_seconds"],
                        entry_window["seconds_until_entry_window"],
                        entry_window["entry_window_start_second"],
                        entry_window["entry_window_end_second"],
                    )
                    return 200, build_robot_payload(state)

            logger.info(
                "[ENTRY_WINDOW_OPEN] user_id=%s server_time=%s timeframe=%s "
                "seconds_in_candle=%s window_start=%s window_end=%s buy_target_second=%s",
                user_id,
                entry_window["server_time"],
                state.timeframe,
                entry_window["current_candle_seconds"],
                entry_window["entry_window_start_second"],
                entry_window["entry_window_end_second"],
                entry_window["buy_target_second"],
            )
            order_path = "/bullex/buy-demo"
            if state.account_mode == "REAL":
                order_path = "/bullex/buy-real"

            skipped_candidates = 0
            last_order_status = 409
            last_order_reason = "NO_AVAILABLE_ASSET"
            last_friendly_error = NO_AVAILABLE_ASSET_ERROR
            attempted_unavailable = False
            for candidate in order_attempt_candidates(state, selected):
                if state.order_attempts >= MAX_ORDER_ATTEMPTS_PER_CYCLE:
                    break
                validation_reason = candidate_pre_order_block_reason(candidate)
                if validation_reason is not None:
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
                order_body = {
                    "active": symbol,
                    "action": direction.lower(),
                    "amount": state.entry_value,
                    "expiration": entry_window["expiration_minutes"],
                }
                if state.account_mode == "REAL":
                    order_body["confirm_real"] = True

                state = auto_trader.start_sending_order(user_id)
                logger.info(
                    "[SENDING_ORDER] user_id=%s symbol=%s direction=%s",
                    user_id,
                    symbol,
                    direction,
                )
                logger.info(
                    "[ORDER_SEND_START] user_id=%s path=%s active=%s direction=%s amount=%s expiration=%s attempt=%s",
                    user_id,
                    order_path,
                    symbol,
                    direction,
                    state.entry_value,
                    entry_window["expiration_minutes"],
                    state.order_attempts,
                )
                try:
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
                    return 502, build_robot_payload(state)

                sent_at = datetime.now(timezone.utc)
                expiration_window = entry_window
                try:
                    fresh_status, fresh_payload = await call_bullex_service("GET", "/sessions/status", user_id)
                    fresh_timestamp = extract_server_timestamp(fresh_payload)
                    if fresh_status < 500 and fresh_timestamp is not None:
                        expiration_window = get_entry_window(
                            state.timeframe,
                            fresh_timestamp,
                            server_time_source="bullex",
                        )
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
                    "amount": state.entry_value,
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
                }
                trade["timestamp"] = trade["sent_at"]
                state = auto_trader.record_trade(user_id, trade)
                state.pending_signal = None
                state.entry_window_open = False
                logger.info(
                    "[PENDING_SIGNAL_CLEARED] user_id=%s symbol=%s reason=TRADE_SENT",
                    user_id,
                    symbol,
                )
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
                    "[ORDER_SEND_SUCCESS] user_id=%s order_id=%s status=%s",
                    user_id,
                    order_id,
                    state.status,
                )
                logger.info(
                    "[PENDING_RESULT] user_id=%s order_id=%s",
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
                if state.account_mode == "REAL":
                    logger.info(
                        "[REAL_TRADE_SENT] user_id=%s order_id=%s",
                        user_id,
                        trade.get("order_id"),
                    )
                else:
                    logger.info(
                        "[ROBOT DEMO ORDER SENT] user_id=%s order_id=%s",
                        user_id,
                        trade.get("order_id"),
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
            logger.exception("[ROBOT ERROR] user_id=%s error=%s", user_id, exc)
            state = recover_analysis_error_to_window(
                user_id,
                error,
                locals().get("entry_window") if isinstance(locals().get("entry_window"), dict) else None,
            )
            return 500, build_robot_payload(state)
        finally:
            persist_robot(user_id)


async def run_analysis_now(user_id: str) -> tuple[int, dict[str, Any]]:
    state = auto_trader.get(user_id)
    if (
        not state.enabled
        or not state.connected
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
        while auto_trader.get(user_id).enabled:
            robot_worker_last_tick_at[user_id] = utc_now()
            logger.info("[ROBOT_WORKER_TICK] user_id=%s", user_id)
            await execute_robot_cycle(user_id)
            state = auto_trader.get(user_id)
            if state.status == STATUS_ACCOUNT_DISCONNECTED:
                state.enabled = False
                persist_robot(user_id)
                break
            if state.operation_in_progress:
                delay = 3
            elif state.status == STATUS_WAITING_ENTRY_WINDOW:
                delay = max(1, state.seconds_until_entry_window)
            elif state.status == STATUS_WAITING_ANALYSIS_WINDOW:
                delay = 3
            elif state.status == STATUS_ORDER_REJECTED and state.rejected_at is not None:
                delay = max(1, 5 - int((utc_now() - state.rejected_at).total_seconds()))
            elif state.status == STATUS_WAITING_NEXT_CYCLE and state.enabled:
                delay = max(0.2, min(3, float(state.to_dict()["seconds_until_next_cycle"])))
            else:
                delay = max(1, state.to_dict()["seconds_until_next_cycle"])
            await asyncio.sleep(delay)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("[ROBOT ERROR] user_id=%s error=WORKER_STOPPED", user_id)
        auto_trader.fail(user_id, "WORKER_STOPPED")
        persist_robot(user_id)
    finally:
        current = asyncio.current_task()
        if robot_tasks.get(user_id) is current:
            robot_tasks.pop(user_id, None)
        logger.info("[ROBOT_WORKER_STOPPED] user_id=%s", user_id)


def ensure_robot_worker(user_id: str) -> None:
    task = robot_tasks.get(user_id)
    if task is None or task.done():
        robot_tasks[user_id] = asyncio.create_task(robot_worker(user_id))
        logger.info("[ROBOT_WORKER_STARTED] user_id=%s", user_id)


def schedule_robot_tick(user_id: str) -> None:
    if not auto_trader.get(user_id).enabled:
        return

    async def run_tick() -> None:
        try:
            await execute_robot_cycle(user_id)
        except Exception:
            logger.exception("[ROBOT ERROR] user_id=%s error=INITIAL_TICK_FAILED", user_id)

    asyncio.create_task(run_tick())


async def stop_robot_worker(user_id: str) -> None:
    task = robot_tasks.pop(user_id, None)
    if task is None or task.done() or task is asyncio.current_task():
        return
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task


async def read_restored_session_status(user_id: str) -> bool:
    for attempt in range(5):
        _, payload = await call_bullex_service("GET", "/sessions/status", user_id)
        connected, _ = extract_account_status(payload)
        if connected:
            sync_user_store_from_payload(user_id, payload)
            return True
        if attempt < 4:
            await asyncio.sleep(2)
    return False


@app.on_event("startup")
async def restore_robot_states() -> None:
    persistence_debug_registered = any(
        getattr(route, "path", None) == "/sessions/persistence-debug"
        for route in app.routes
    )
    logger.info(
        "[SESSION_PERSISTENCE_ROUTE] service=backend-gateway "
        "path=/sessions/persistence-debug registered=%s",
        persistence_debug_registered,
    )
    try:
        persisted_states = robot_persistence.load_states()
    except Exception:
        logger.exception("[ROBOT_RESTORE] status=failed reason=load_error")
        return

    for user_id, payload in persisted_states:
        try:
            trades = robot_persistence.load_trades(user_id)
            state = auto_trader.restore(
                user_id,
                payload,
                trades,
                source=robot_persistence_source(),
            )
            session_restored = await read_restored_session_status(user_id)
            if session_restored:
                state = auto_trader.sync_connection(
                    user_id,
                    connected=True,
                    active_mode=state.active_mode or "PRACTICE",
                    source="bullex_service",
                    align_status=state.pending_signal is None,
                )
            if state.operation_in_progress and state.last_trade:
                order_id = state.last_trade.get("order_id")
                if order_id:
                    trade_result_monitor.start(
                        user_id,
                        order_id,
                        state.last_trade.get("expires_at"),
                    )
            if state.enabled:
                ensure_robot_worker(user_id)
            robot_persistence.save_restore_status(
                user_id,
                session_restored=session_restored,
                robot_restored=True,
            )
            logger.info("[ROBOT_RESTORE] user_id=%s enabled=%s", user_id, str(state.enabled).lower())
        except Exception:
            logger.exception("[ROBOT_RESTORE] user_id=%s enabled=false status=failed", user_id)
            try:
                robot_persistence.save_restore_status(
                    user_id,
                    session_restored=False,
                    robot_restored=False,
                )
            except Exception:
                logger.exception("[ROBOT PERSISTENCE ERROR] user_id=%s", user_id)


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
    review = await review_signal(signal)
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
        review = await review_signal(signal)
        reviewed.append({"signal": signal, "review": review})

    reviewed.sort(
        key=lambda item: (
            int(item["review"].get("quality") or 0),
            int(item["signal"].get("confidence") or 0),
        ),
        reverse=True,
    )
    return json_response(200, build_success(reviewed))


@app.get("/robot/state")
async def robot_state(auth: dict[str, str] = Depends(require_headers)) -> JSONResponse:
    user_id = auth["user_id"]
    state = get_user_robot_state(user_id)
    if fresh_robot_connection(state):
        session_payload = cached_robot_connection_payload(state)
        connected = True
        active_mode = state.active_mode
        source = state.connection_status_source
    else:
        _, session_payload, state, connected, active_mode, source = await fetch_and_sync_robot_connection(user_id)
    if not connected:
        if state.status == STATUS_ACCOUNT_DISCONNECTED:
            logger.warning("[ROBOT_BLOCKED_ACCOUNT_DISCONNECTED] user_id=%s", user_id)
        return json_response(
            200,
            build_robot_payload(
                state,
                user_id=user_id,
                connected=False,
                active_mode=active_mode,
                connection_checked_at=state.connection_checked_at.isoformat()
                if state.connection_checked_at is not None
                else None,
                connection_status_source=source,
                real_ready=False,
                real_block_reason="BULLEX_NOT_CONNECTED",
            ),
        )
    server_timestamp = extract_server_timestamp(session_payload)
    if server_timestamp is None:
        window = get_entry_window(
            state.timeframe,
            utc_now().timestamp(),
            server_time_source="vps_fallback",
        )
        logger.warning(
            "[SERVER_TIME_FALLBACK] user_id=%s source=robot_state current_candle_seconds=%s",
            user_id,
            window["current_candle_seconds"],
        )
    else:
        previous_source = getattr(state, "server_time_source", None)
        window = get_entry_window(
            state.timeframe,
            server_timestamp,
            server_time_source="bullex",
        )
        if previous_source == "vps_fallback" and getattr(state, "server_time", None):
            logger.info("[SERVER_TIME_BULLEX_RESTORED] user_id=%s", user_id)
    auto_trader.update_entry_window(
        user_id,
        window,
    )
    state = auto_trader.get(user_id)
    block_reason = real_block_reason(state, connected=connected, active_mode=active_mode)
    return json_response(
        200,
        build_robot_payload(
            state,
            user_id=user_id,
            connected=connected,
            active_mode=active_mode,
            connection_checked_at=state.connection_checked_at.isoformat()
            if state.connection_checked_at is not None
            else None,
            connection_status_source=source,
            real_ready=block_reason is None,
            real_block_reason=block_reason,
        ),
    )


def build_robot_stats(items: list[dict[str, Any]]) -> dict[str, Any]:
    wins = sum(1 for item in items if item.get("result") == "WIN")
    losses = sum(1 for item in items if item.get("result") == "LOSS")
    total_trades = wins + losses
    profit = round(sum(float(item.get("profit") or 0) for item in items), 2)
    gross_profit = sum(max(0.0, float(item.get("profit") or 0)) for item in items)
    gross_loss = abs(sum(min(0.0, float(item.get("profit") or 0)) for item in items))
    profit_factor = round(gross_profit / gross_loss, 2) if gross_loss else round(gross_profit, 2)

    current_win_streak = 0
    current_loss_streak = 0
    if items:
        current_result = items[0].get("result")
        for item in items:
            if item.get("result") != current_result:
                break
            if current_result == "WIN":
                current_win_streak += 1
            elif current_result == "LOSS":
                current_loss_streak += 1

    best_win_streak = 0
    best_loss_streak = 0
    win_streak = 0
    loss_streak = 0
    for item in reversed(items):
        if item.get("result") == "WIN":
            win_streak += 1
            loss_streak = 0
            best_win_streak = max(best_win_streak, win_streak)
        elif item.get("result") == "LOSS":
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


@app.get("/robot/history")
async def robot_history(
    days: int = Query(default=30),
    auth: dict[str, str] = Depends(require_headers),
) -> JSONResponse:
    if days not in {1, 7, 30}:
        raise HTTPException(status_code=422, detail="days must be 1, 7, or 30")
    items = robot_persistence.load_trade_history(auth["user_id"], days)
    return json_response(200, build_success({"items": items}))


@app.get("/robot/stats")
async def robot_stats(
    days: int = Query(default=30),
    auth: dict[str, str] = Depends(require_headers),
) -> JSONResponse:
    if days not in {1, 7, 30}:
        raise HTTPException(status_code=422, detail="days must be 1, 7, or 30")
    items = robot_persistence.load_trade_history(auth["user_id"], days)
    return json_response(200, build_success(build_robot_stats(items)))


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
            "active_mode": "PRACTICE",
            "currency": "USD",
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
    body: RobotConfigUpdate,
    auth: dict[str, str] = Depends(require_headers),
) -> JSONResponse:
    user_id = auth["user_id"]
    get_user_robot_state(user_id)
    state = auto_trader.update_config(user_id, body)
    logger.info(
        "[CYCLE_CONFIG] user_id=%s cycle_minutes=%s source=robot_config",
        user_id,
        state.cycle_minutes,
    )
    persist_robot(user_id)
    if state.enabled:
        ensure_robot_worker(user_id)
    else:
        await stop_robot_worker(user_id)
    return json_response(200, build_robot_payload(state))


@app.post("/robot/start")
async def robot_start(auth: dict[str, str] = Depends(require_headers)) -> JSONResponse:
    user_id = auth["user_id"]
    state = get_user_robot_state(user_id)
    if state.status == STATUS_ACCOUNT_DISCONNECTED or state.connection_failure_count > 0:
        _, _, state, _, _, _ = await fetch_and_sync_robot_connection(user_id)
    if state.account_mode == "REAL":
        if fresh_robot_connection(state):
            connected = True
            active_mode = state.active_mode
        else:
            _, session_payload, state, connected, active_mode, _ = await fetch_and_sync_robot_connection(user_id)
            mark_disconnected_from_payload(user_id, session_payload)
        block_reason = real_block_reason(state, connected=connected, active_mode=active_mode)
        if block_reason is not None:
            auto_trader.lock_real(user_id, block_reason)
            persist_robot(user_id)
            logger.warning(
                "[REAL_TRADE_BLOCKED] user_id=%s reason=%s",
                user_id,
                block_reason,
            )
            return json_response(403, build_error(block_reason))

    state = auto_trader.start(user_id)
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
    return json_response(200, build_robot_payload(state))


@app.post("/robot/stop")
async def robot_stop(auth: dict[str, str] = Depends(require_headers)) -> JSONResponse:
    state = auto_trader.stop(auth["user_id"])
    persist_robot(auth["user_id"])
    await stop_robot_worker(auth["user_id"])
    logger.info("[ROBOT STOP] user_id=%s", auth["user_id"])
    return json_response(200, build_robot_payload(state))


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
    block_reason = real_block_reason(state, connected=connected, active_mode=active_mode)
    return json_response(
        200 if status_code < 500 else status_code,
        build_robot_payload(
            state,
            connected=connected,
            active_mode=active_mode,
            connection_checked_at=state.connection_checked_at.isoformat()
            if state.connection_checked_at is not None
            else None,
            connection_status_source=source,
            real_ready=block_reason is None,
            real_block_reason=block_reason,
        ),
    )


@app.post("/robot/execute-demo")
async def robot_execute_demo(auth: dict[str, str] = Depends(require_headers)) -> JSONResponse:
    status_code, payload = await execute_robot_cycle(auth["user_id"], required_mode="DEMO")
    return json_response(status_code, payload)


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


@app.post("/bullex/connect")
async def bullex_connect(
    body: dict[str, Any],
    auth: dict[str, str] = Depends(require_headers),
) -> JSONResponse:
    user_id = auth["user_id"]
    status_code, payload = await call_bullex_service(
        "POST",
        "/sessions/connect",
        user_id,
        json_body=body,
    )
    sync_user_store_from_payload(
        user_id,
        payload,
        body.get("email"),
        is_new_connection=True,
    )
    connected, active_mode = extract_account_status(payload)
    if active_mode is None:
        requested_mode = body.get("active_mode") or body.get("mode")
        active_mode = str(requested_mode).strip().upper() if requested_mode else "PRACTICE"
    if payload.get("ok") and connected:
        state = get_user_robot_state(user_id)
        state = auto_trader.sync_connection(
            user_id,
            connected=True,
            active_mode=active_mode,
            source="bullex_service",
            align_status=True,
        )
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
            ensure_robot_worker(user_id)
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
    return json_response(status_code, payload)


@app.get("/bullex/status")
async def bullex_status(auth: dict[str, str] = Depends(require_headers)) -> JSONResponse:
    status_code, payload = await call_bullex_service("GET", "/sessions/status", auth["user_id"])
    sync_user_store_from_payload(auth["user_id"], payload)
    connected, active_mode = extract_account_status(payload)
    if payload.get("ok") and connected:
        state = auto_trader.sync_connection(
            auth["user_id"],
            connected=True,
            active_mode=active_mode,
            source=connection_source_from_payload(payload),
            align_status=True,
        )
        persist_robot(auth["user_id"])
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
    return json_response(status_code, payload)


@app.get("/bullex/balance")
async def bullex_balance(auth: dict[str, str] = Depends(require_headers)) -> JSONResponse:
    status_code, payload = await call_bullex_service("GET", "/account/balance", auth["user_id"])
    sync_user_store_from_payload(auth["user_id"], payload)
    return json_response(status_code, payload)


@app.post("/bullex/change-mode")
async def bullex_change_mode(
    body: dict[str, Any],
    auth: dict[str, str] = Depends(require_headers),
) -> JSONResponse:
    status_code, payload = await call_bullex_service(
        "POST",
        "/account/change-mode",
        auth["user_id"],
        json_body=body,
    )
    sync_user_store_from_payload(auth["user_id"], payload)
    return json_response(status_code, payload)


@app.get("/bullex/assets")
async def bullex_assets(auth: dict[str, str] = Depends(require_headers)) -> JSONResponse:
    status_code, payload = await call_bullex_service("GET", "/assets", auth["user_id"])
    mark_disconnected_from_payload(auth["user_id"], payload)
    if payload.get("ok") and isinstance(payload.get("data"), list):
        allowed_assets = [
            asset
            for asset in payload["data"]
            if isinstance(asset, dict) and is_binary_asset_allowed(str(asset.get("symbol") or ""))
        ]
        payload["data"] = allowed_assets
        try:
            user_store.save_market_assets_snapshot(auth["user_id"], allowed_assets)
        except Exception:
            logger.exception("falha ao salvar snapshot de market_assets para %s", auth["user_id"])
    return json_response(status_code, payload)


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
    if not is_binary_asset_allowed(resolved_symbol):
        return json_response(400, build_error(ASSET_NOT_ALLOWED))
    resolved_timeframe, resolved_interval = normalize_timeframe_seconds(timeframe, interval)
    resolved_limit = max(1, min(int(limit or count or 60), 500))

    status_code, session_payload = await call_bullex_service("GET", "/sessions/status", auth["user_id"])
    mark_disconnected_from_payload(auth["user_id"], session_payload)
    if not session_payload.get("ok"):
        return json_response(status_code, session_payload)
    server_timestamp = extract_server_timestamp(session_payload) or utc_now().timestamp()

    params = {
        "active": resolved_symbol,
        "interval": resolved_interval,
        "count": resolved_limit,
        "endtime": int(endtime or server_timestamp),
    }
    if endtime is not None:
        params["endtime"] = endtime
    status_code, payload = await call_bullex_service("GET", "/candles", auth["user_id"], params=params)
    mark_disconnected_from_payload(auth["user_id"], payload)
    if not payload.get("ok"):
        return json_response(status_code, payload)
    live_payload = build_live_candles_payload(
        resolved_symbol,
        resolved_timeframe,
        resolved_interval,
        resolved_limit,
        float(server_timestamp),
        payload,
    )
    return json_response(status_code, build_success(live_payload))


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
    status_code, payload = await call_bullex_service("GET", "/payouts", auth["user_id"], params=params)
    mark_disconnected_from_payload(auth["user_id"], payload)
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
                user_store.save_market_asset_payout(auth["user_id"], active, payout_item.get("payout"))
            except Exception:
                logger.exception("falha ao salvar payout de market_assets para %s %s", auth["user_id"], active)
    return json_response(status_code, payload)


@app.post("/bullex/buy-demo")
async def bullex_buy_demo(
    body: dict[str, Any],
    auth: dict[str, str] = Depends(require_headers),
) -> JSONResponse:
    status_code, payload = await call_bullex_service(
        "POST",
        "/orders/buy-demo",
        auth["user_id"],
        json_body=body,
    )
    return json_response(status_code, payload)


@app.post("/bullex/buy-real")
async def bullex_buy_real(
    body: dict[str, Any],
    auth: dict[str, str] = Depends(require_headers),
) -> JSONResponse:
    status_code, payload = await call_bullex_service(
        "POST",
        "/orders/buy-real",
        auth["user_id"],
        json_body=body,
    )
    return json_response(status_code, payload)


@app.post("/bullex/disconnect")
async def bullex_disconnect(auth: dict[str, str] = Depends(require_headers)) -> JSONResponse:
    status_code, payload = await call_bullex_service("POST", "/sessions/disconnect", auth["user_id"])
    if payload.get("ok"):
        user_store.disconnect(auth["user_id"])
    else:
        sync_user_store_from_payload(auth["user_id"], payload)
    return json_response(status_code, payload)


@app.post("/bullex/reconnect")
async def bullex_reconnect(auth: dict[str, str] = Depends(require_headers)) -> JSONResponse:
    status_code, payload = await call_bullex_service("POST", "/sessions/reconnect", auth["user_id"])
    sync_user_store_from_payload(auth["user_id"], payload)
    return json_response(status_code, payload)


@app.get("/bullex/account")
async def bullex_account(auth: dict[str, str] = Depends(require_headers)) -> JSONResponse:
    status_code, payload = await call_bullex_service("GET", "/account", auth["user_id"])
    sync_user_store_from_payload(auth["user_id"], payload)
    return json_response(status_code, payload)


@app.get("/bullex/order-result/{order_id}")
async def bullex_order_result(order_id: str, auth: dict[str, str] = Depends(require_headers)) -> JSONResponse:
    status_code, payload = await call_bullex_service("GET", f"/orders/{order_id}/result", auth["user_id"])
    return json_response(status_code, payload)
