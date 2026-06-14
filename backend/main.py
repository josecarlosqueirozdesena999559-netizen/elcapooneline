import logging
import math
import os
import asyncio
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
    STATUS_PENDING_RESULT,
    STATUS_WAITING_ENTRY_WINDOW,
    STATUS_WAITING_NEXT_CYCLE,
    parse_datetime,
)
from backend.openai_signal_reviewer import review_signal
from backend.robot_persistence import RobotPersistence, create_robot_persistence
from backend.signal_engine import analyze_signal
from backend.trade_result_monitor import TradeResultMonitor
from backend.user_store import UserStore, create_user_store


logger = logging.getLogger("backend-gateway")

ASSET_NOT_ALLOWED = "ASSET_NOT_ALLOWED"
SESSION_NOT_FOUND = "SESSION_NOT_FOUND"
SESSION_DISCONNECTED = "SESSION_DISCONNECTED"
LOW_QUALITY_SIGNAL = "Sinal bloqueado por baixa qualidade"
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


TIMEFRAME_SECONDS = {"M1": 60, "M5": 300, "M15": 900, "M30": 1800}
ENTRY_WINDOW_SECONDS = {"M1": 5, "M5": 10, "M15": 20, "M30": 30}
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


def get_entry_window(timeframe: str, server_timestamp: float | None = None) -> dict[str, Any]:
    normalized = str(timeframe).strip().upper()
    if normalized not in TIMEFRAME_SECONDS:
        raise ValueError("INVALID_TIMEFRAME")
    if server_timestamp is None:
        raise ValueError("SERVER_TIME_UNAVAILABLE")

    expiration_seconds = TIMEFRAME_SECONDS[normalized]
    window_seconds = ENTRY_WINDOW_SECONDS[normalized]
    seconds_in_candle = float(server_timestamp) % expiration_seconds
    seconds_until_close = expiration_seconds - seconds_in_candle
    window_start = expiration_seconds - window_seconds
    entry_window_open = seconds_in_candle >= window_start and seconds_until_close >= 1.0
    if entry_window_open:
        seconds_until_entry_window = 0
    elif seconds_in_candle < window_start:
        seconds_until_entry_window = math.ceil(window_start - seconds_in_candle)
    else:
        seconds_until_entry_window = math.ceil(seconds_until_close + window_start)

    return {
        "server_timestamp": float(server_timestamp),
        "server_time": datetime.fromtimestamp(server_timestamp, timezone.utc).isoformat(),
        "timeframe": normalized,
        "entry_window_open": entry_window_open,
        "seconds_until_entry_window": seconds_until_entry_window,
        "current_candle_seconds": round(seconds_in_candle, 3),
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
    mark_disconnected_from_payload(user_id, payload)
    timestamp = extract_server_timestamp(payload)
    if timestamp is None:
        return status_code if status_code >= 400 else 502, payload, None
    window = get_entry_window(state.timeframe, timestamp)
    auto_trader.update_entry_window(user_id, window)
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
        return "DAILY_STOP_LOSS_REACHED"
    if state.stop_win > 0 and daily_profit >= state.stop_win:
        return "DAILY_STOP_WIN_REACHED"
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
    blocked_filters = list(selected.get("blocked_filters") or [])
    approved_filters = list(selected.get("approved_filters") or [])

    if state.strategy_mode == "conservative":
        if payout is None or float(payout) < 85:
            if "MIN_PAYOUT" not in blocked_filters:
                blocked_filters.append("MIN_PAYOUT")
        elif "MIN_PAYOUT" not in approved_filters:
            approved_filters.append("MIN_PAYOUT")
        if int(selected.get("confidence") or 0) < 90 and "MIN_CONFIDENCE" not in blocked_filters:
            blocked_filters.append("MIN_CONFIDENCE")
        if int(selected.get("strength") or 0) < 20 and "TREND_CLEAR" not in blocked_filters:
            blocked_filters.append("TREND_CLEAR")
        if selected.get("trend") == "SIDEWAYS" and "SIDEWAYS_FILTER" not in blocked_filters:
            blocked_filters.append("SIDEWAYS_FILTER")

    direction = selected.get("signal")
    if {"ema9", "ema21"}.issubset(selected):
        ema9 = float(selected.get("ema9") or 0)
        ema21 = float(selected.get("ema21") or 0)
        ema_ok = (direction == "CALL" and ema9 > ema21) or (direction == "PUT" and ema9 < ema21)
        if not ema_ok and "EMA_TREND" not in blocked_filters:
            blocked_filters.append("EMA_TREND")
    if "rsi" in selected:
        rsi = float(selected.get("rsi") or 50)
        rsi_ok = (direction == "CALL" and 55 <= rsi <= 75) or (direction == "PUT" and 25 <= rsi <= 45)
        if not rsi_ok and "RSI_RANGE" not in blocked_filters:
            blocked_filters.append("RSI_RANGE")
    if "body_ratio" in selected and float(selected.get("body_ratio") or 0) < 0.55:
        if "CANDLE_STRENGTH" not in blocked_filters:
            blocked_filters.append("CANDLE_STRENGTH")
    if direction == "CALL" and "upper_wick_ratio" in selected and float(selected.get("upper_wick_ratio") or 0) > 0.45:
        if "WICK_REJECTION" not in blocked_filters:
            blocked_filters.append("WICK_REJECTION")
    if direction == "PUT" and "lower_wick_ratio" in selected and float(selected.get("lower_wick_ratio") or 0) > 0.45:
        if "WICK_REJECTION" not in blocked_filters:
            blocked_filters.append("WICK_REJECTION")
    if "atr_pct" in selected and float(selected.get("atr_pct") or 0) < 0.0001:
        if "VOLATILITY" not in blocked_filters:
            blocked_filters.append("VOLATILITY")
    if "directional_candles_5" in selected and int(selected.get("directional_candles_5") or 0) < 3:
        if "LAST_5_CONFIRMATION" not in blocked_filters:
            blocked_filters.append("LAST_5_CONFIRMATION")
    if selected.get("alternating_last_3") and "NO_ALTERNATING_LAST_3" not in blocked_filters:
        blocked_filters.append("NO_ALTERNATING_LAST_3")

    symbol = normalize_binary_active(str(selected.get("symbol") or ""))
    cooldown = asset_cooldown_reason(user_id, symbol)
    if cooldown is not None:
        blocked_filters.append(cooldown)
        logger.warning("[ASSET_COOLDOWN] user_id=%s symbol=%s", user_id, symbol)

    trade_allowed = bool(selected.get("trade_allowed", True)) and not blocked_filters
    selected["blocked_filters"] = blocked_filters
    selected["approved_filters"] = approved_filters
    selected["trade_allowed"] = trade_allowed
    selected["quality_score"] = int(selected.get("quality_score") or (100 if trade_allowed else 0))
    selected["quality_reason"] = "OK" if trade_allowed else LOW_QUALITY_SIGNAL
    if trade_allowed:
        logger.info(
            "[STRATEGY_FILTER_PASS] user_id=%s symbol=%s mode=%s quality_score=%s",
            user_id,
            symbol,
            state.strategy_mode,
            selected["quality_score"],
        )
        return True, selected, None

    logger.info(
        "[STRATEGY_FILTER_BLOCK] user_id=%s symbol=%s mode=%s blocked_filters=%s",
        user_id,
        symbol,
        state.strategy_mode,
        blocked_filters,
    )
    return False, selected, LOW_QUALITY_SIGNAL


def robot_stop_reason(state: Any) -> str | None:
    if state.operation_in_progress:
        return "OPERATION_IN_PROGRESS"
    if state.stop_win > 0 and state.profit >= state.stop_win:
        return "STOP_WIN_REACHED"
    if state.stop_loss > 0 and state.profit <= -state.stop_loss:
        return "STOP_LOSS_REACHED"
    return None


def build_robot_payload(state: Any, **extra: Any) -> dict[str, Any]:
    data = state.to_dict()
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
        robot_persistence.save_state(user_id, state.to_dict())
        if state.last_trade:
            robot_persistence.save_trade(user_id, state.last_trade)
    except Exception:
        logger.exception("[ROBOT PERSISTENCE ERROR] user_id=%s", user_id)


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
        previous_seconds_until_entry = state.seconds_until_entry_window
        if not had_pending_signal:
            can_run, state = auto_trader.prepare_cycle(user_id)
            if not can_run:
                if state.status == STATUS_WAITING_NEXT_CYCLE:
                    logger.info(
                        "[ROBOT WAITING NEXT CYCLE] user_id=%s next_cycle_at=%s",
                        user_id,
                        state.next_cycle_at,
                    )
                return 200, build_robot_payload(state)

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
            connected, active_mode = extract_account_status(account_payload)
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
                state = auto_trader.disconnect_account(user_id)
                logger.warning("[ROBOT_BLOCKED_ACCOUNT_DISCONNECTED] user_id=%s", user_id)
                return 200, build_robot_payload(state)
            if active_mode != expected_bullex_mode:
                state = auto_trader.reject(user_id, f"ACCOUNT_MODE_MUST_BE_{expected_bullex_mode}")
                logger.info(
                    "[ROBOT SIGNAL REJECTED] user_id=%s reason=ACCOUNT_MODE_MUST_BE_%s",
                    user_id,
                    expected_bullex_mode,
                )
                return 200, build_robot_payload(state)
            if entry_window is None:
                state = auto_trader.fail(user_id, "SERVER_TIME_UNAVAILABLE")
                logger.error("[ROBOT ERROR] user_id=%s error=SERVER_TIME_UNAVAILABLE", user_id)
                return status_code, build_robot_payload(state)
            selected = dict(state.pending_signal) if state.pending_signal else None
            if selected is not None and not entry_window["entry_window_open"]:
                missed_window = (
                    entry_window["seconds_until_entry_window"]
                    > previous_seconds_until_entry + ENTRY_WINDOW_SECONDS[state.timeframe]
                )
                if not missed_window:
                    logger.info(
                        "[ENTRY_WINDOW_WAIT] user_id=%s server_time=%s timeframe=%s "
                        "seconds_in_candle=%s seconds_until_close=%s expiration=%s",
                        user_id,
                        entry_window["server_time"],
                        state.timeframe,
                        entry_window["current_candle_seconds"],
                        entry_window["seconds_until_close"],
                        entry_window["expiration"],
                    )
                    return 200, build_robot_payload(state)
                logger.info(
                    "[PENDING_SIGNAL_CLEARED] user_id=%s symbol=%s reason=ENTRY_WINDOW_MISSED",
                    user_id,
                    selected.get("symbol"),
                )
                state = auto_trader.clear_pending_signal(user_id, analyze=True)
                return 200, build_robot_payload(state)

            if selected is None:
                scan_status, scan_payload = await scan_local_signals(
                    user_id,
                    limit=len(BINARY_ALLOWED_ASSETS),
                    include_wait=True,
                    timeframe=state.timeframe,
                    endtime=int(entry_window["server_timestamp"]),
                    strategy_mode=state.strategy_mode,
                )
                if not scan_payload.get("ok"):
                    state = auto_trader.fail(
                        user_id,
                        str(scan_payload.get("error") or "SIGNAL_SCAN_FAILED"),
                    )
                    logger.error("[ROBOT ERROR] user_id=%s error=%s", user_id, state.rejection_reason)
                    return scan_status, build_robot_payload(state)

                signals = [item for item in scan_payload.get("data", []) if isinstance(item, dict)]
                if not signals:
                    state = auto_trader.reject(user_id, "NO_SIGNAL")
                    logger.info("[ROBOT SIGNAL REJECTED] user_id=%s reason=NO_SIGNAL", user_id)
                    return 200, build_robot_payload(state)

                selected = max(
                    signals,
                    key=lambda item: (
                        int(item.get("confidence") or 0),
                        int(item.get("strength") or 0),
                    ),
                )
                symbol = normalize_binary_active(str(selected.get("symbol") or ""))
                payout = selected.get("payout")
                payout_status = 200
                payout_payload = build_success([])
                if payout is None:
                    payout_status, payout_payload = await call_bullex_service(
                        "GET",
                        "/payouts",
                        user_id,
                        params={"active": symbol},
                    )
                    mark_disconnected_from_payload(user_id, payout_payload)
                    payout = extract_payout(payout_payload, symbol) if payout_payload.get("ok") else None
                selected = {**selected, "symbol": symbol, "payout": payout}

                rejection = robot_stop_reason(state)
                if rejection is None and not is_binary_asset_allowed(symbol):
                    rejection = ASSET_NOT_ALLOWED
                if rejection is None and selected.get("signal") not in {"CALL", "PUT"}:
                    rejection = "SIGNAL_WAIT"
                if rejection is None and int(selected.get("confidence") or 0) < state.min_confidence:
                    rejection = "CONFIDENCE_BELOW_MINIMUM"
                if rejection is None and (payout is None or payout < state.min_payout):
                    rejection = "PAYOUT_BELOW_MINIMUM"
                if payout_status >= 400 and rejection is None:
                    rejection = str(payout_payload.get("error") or "PAYOUT_UNAVAILABLE")
                if rejection is None and not state.enabled:
                    rejection = "ROBOT_STOPPED"
                if rejection is None:
                    allowed, selected, strategy_rejection = apply_strategy_guard(
                        user_id,
                        state,
                        selected,
                        payout=payout,
                    )
                    if not allowed:
                        rejection = "SIGNAL_BLOCKED_LOW_QUALITY"

                if rejection is not None:
                    if rejection == "SIGNAL_BLOCKED_LOW_QUALITY":
                        last_rejection_reason = str(selected.get("quality_reason") or rejection)
                        last_rejection_reason = ",".join(selected.get("blocked_filters") or []) or LOW_QUALITY_SIGNAL
                        state = auto_trader.reject_strategy(
                            user_id,
                            rejection,
                            last_rejection_reason=last_rejection_reason,
                            blocked_filters=list(selected.get("blocked_filters") or []),
                            approved_filters=list(selected.get("approved_filters") or []),
                            quality_score=int(selected.get("quality_score") or 0),
                        )
                        logger.info(
                            "[SIGNAL_REJECTED] user_id=%s reason=%s last_rejection_reason=%s quality_score=%s blocked_filters=%s",
                            user_id,
                            rejection,
                            state.last_rejection_reason,
                            state.quality_score,
                            state.blocked_filters,
                        )
                        logger.info(
                            "[NEXT_CYCLE_SCHEDULED] user_id=%s next_cycle_at=%s",
                            user_id,
                            state.next_cycle_at,
                        )
                    else:
                        state = auto_trader.reject(user_id, rejection)
                    logger.info("[ROBOT SIGNAL REJECTED] user_id=%s reason=%s", user_id, rejection)
                    return 200, build_robot_payload(state)

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

                timing_status, timing_payload, entry_window = await refresh_entry_window(user_id, state)
                if entry_window is None:
                    state = auto_trader.fail(user_id, "SERVER_TIME_UNAVAILABLE")
                    logger.error("[ROBOT ERROR] user_id=%s error=SERVER_TIME_UNAVAILABLE", user_id)
                    return timing_status, build_robot_payload(state)
                connected, active_mode = extract_account_status(timing_payload)
                if not connected or active_mode != expected_bullex_mode:
                    reason = (
                        "ACCOUNT_DISCONNECTED"
                        if not connected
                        else f"ACCOUNT_MODE_MUST_BE_{expected_bullex_mode}"
                    )
                    state = auto_trader.reject(user_id, reason)
                    return 200, build_robot_payload(state)
                if not entry_window["entry_window_open"]:
                    logger.info(
                        "[ENTRY_WINDOW_WAIT] user_id=%s server_time=%s timeframe=%s "
                        "seconds_in_candle=%s seconds_until_close=%s expiration=%s",
                        user_id,
                        entry_window["server_time"],
                        state.timeframe,
                        entry_window["current_candle_seconds"],
                        entry_window["seconds_until_close"],
                        entry_window["expiration"],
                    )
                    return 200, build_robot_payload(state)

            symbol = str(selected["symbol"])
            direction = str(selected.get("signal") or selected.get("direction"))
            payout = selected.get("payout")
            logger.info(
                "[ENTRY_WINDOW_OPEN] user_id=%s server_time=%s timeframe=%s "
                "seconds_in_candle=%s seconds_until_close=%s expiration=%s",
                user_id,
                entry_window["server_time"],
                state.timeframe,
                entry_window["current_candle_seconds"],
                entry_window["seconds_until_close"],
                entry_window["expiration"],
            )

            order_body = {
                "active": symbol,
                "action": direction.lower(),
                "amount": state.entry_value,
                "expiration": entry_window["expiration_minutes"],
            }
            order_path = "/bullex/buy-demo"
            if state.account_mode == "REAL":
                order_path = "/bullex/buy-real"
                order_body["confirm_real"] = True

            state = auto_trader.start_sending_order(user_id)
            logger.info(
                "[ORDER_SEND_START] user_id=%s path=%s active=%s direction=%s amount=%s expiration=%s",
                user_id,
                order_path,
                symbol,
                direction,
                state.entry_value,
                entry_window["expiration_minutes"],
            )
            order_status, order_payload = await submit_bullex_order(
                user_id,
                order_path,
                order_body,
            )
            mark_disconnected_from_payload(user_id, order_payload)
            if not order_payload.get("ok"):
                reason = str(order_payload.get("error") or "ORDER_FAILED")
                state = auto_trader.reject_order(user_id, reason)
                logger.error("[ORDER_SEND_FAILED] user_id=%s error=%s", user_id, reason)
                return order_status, build_robot_payload(state)

            order_data = order_payload.get("data") if isinstance(order_payload.get("data"), dict) else {}
            order_id = order_data.get("order_id")
            if order_id is None or not str(order_id).strip():
                state = auto_trader.reject_order(user_id, "ORDER_ID_MISSING")
                logger.error("[ORDER_SEND_FAILED] user_id=%s error=ORDER_ID_MISSING", user_id)
                return 502, build_robot_payload(state)

            sent_at = datetime.now(timezone.utc)
            expected_expire_at, expiration_source = calculate_expected_expire_at(
                state.timeframe,
                order_data,
                entry_window,
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
                "server_time_at_send": entry_window.get("server_time"),
                "server_timestamp_at_send": entry_window.get("server_timestamp"),
            }
            trade["timestamp"] = trade["sent_at"]
            auto_trader.clear_pending_signal(user_id)
            logger.info(
                "[PENDING_SIGNAL_CLEARED] user_id=%s symbol=%s reason=TRADE_SENT",
                user_id,
                symbol,
            )
            state = auto_trader.record_trade(user_id, trade)
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
            trade_result_monitor.start(user_id, order_id)
            return 200, build_robot_payload(state)
        except Exception as exc:
            state = auto_trader.fail(user_id, "ROBOT_CYCLE_ERROR")
            logger.exception("[ROBOT ERROR] user_id=%s error=%s", user_id, exc)
            return 500, build_robot_payload(state)
        finally:
            persist_robot(user_id)


async def robot_worker(user_id: str) -> None:
    try:
        while auto_trader.get(user_id).enabled:
            await execute_robot_cycle(user_id)
            state = auto_trader.get(user_id)
            if state.operation_in_progress:
                delay = 3
            elif state.status == STATUS_WAITING_ENTRY_WINDOW:
                delay = max(1, state.seconds_until_entry_window)
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


def ensure_robot_worker(user_id: str) -> None:
    task = robot_tasks.get(user_id)
    if task is None or task.done():
        robot_tasks[user_id] = asyncio.create_task(robot_worker(user_id))


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
            state = auto_trader.restore(user_id, payload, trades)
            session_restored = await read_restored_session_status(user_id)
            if state.operation_in_progress and state.last_trade:
                order_id = state.last_trade.get("order_id")
                if order_id:
                    trade_result_monitor.start(user_id, order_id)
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
    _, session_payload = await call_bullex_service("GET", "/sessions/status", auth["user_id"])
    sync_user_store_from_payload(auth["user_id"], session_payload)
    connected, active_mode = extract_account_status(session_payload)
    state = auto_trader.get(auth["user_id"])
    if not connected:
        state = auto_trader.disconnect_account(auth["user_id"])
        logger.warning("[ROBOT_BLOCKED_ACCOUNT_DISCONNECTED] user_id=%s", auth["user_id"])
        return json_response(
            200,
            build_robot_payload(
                state,
                connected=False,
                active_mode=active_mode,
                real_ready=False,
                real_block_reason="BULLEX_NOT_CONNECTED",
            ),
        )
    server_timestamp = extract_server_timestamp(session_payload)
    if server_timestamp is not None:
        auto_trader.update_entry_window(
            auth["user_id"],
            get_entry_window(state.timeframe, server_timestamp),
        )
    block_reason = real_block_reason(state, connected=connected, active_mode=active_mode)
    return json_response(
        200,
        build_robot_payload(
            state,
            connected=connected,
            active_mode=active_mode,
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


@app.post("/robot/config")
async def robot_config(
    body: RobotConfigUpdate,
    auth: dict[str, str] = Depends(require_headers),
) -> JSONResponse:
    state = auto_trader.update_config(auth["user_id"], body)
    persist_robot(auth["user_id"])
    if state.enabled:
        ensure_robot_worker(auth["user_id"])
    else:
        await stop_robot_worker(auth["user_id"])
    return json_response(200, build_robot_payload(state))


@app.post("/robot/start")
async def robot_start(auth: dict[str, str] = Depends(require_headers)) -> JSONResponse:
    user_id = auth["user_id"]
    state = auto_trader.get(user_id)
    if state.account_mode == "REAL":
        _, session_payload = await call_bullex_service("GET", "/sessions/status", user_id)
        mark_disconnected_from_payload(user_id, session_payload)
        connected, active_mode = extract_account_status(session_payload)
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
    persist_robot(user_id)
    ensure_robot_worker(user_id)
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
    status_code, payload = await call_bullex_service(
        "POST",
        "/sessions/connect",
        auth["user_id"],
        json_body=body,
    )
    sync_user_store_from_payload(
        auth["user_id"],
        payload,
        body.get("email"),
        is_new_connection=True,
    )
    return json_response(status_code, payload)


@app.get("/bullex/status")
async def bullex_status(auth: dict[str, str] = Depends(require_headers)) -> JSONResponse:
    status_code, payload = await call_bullex_service("GET", "/sessions/status", auth["user_id"])
    sync_user_store_from_payload(auth["user_id"], payload)
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
    active: str,
    interval: int,
    count: int,
    endtime: int | None = None,
    auth: dict[str, str] = Depends(require_headers),
) -> JSONResponse:
    if not is_binary_asset_allowed(active):
        return json_response(400, build_error(ASSET_NOT_ALLOWED))
    active = normalize_binary_active(active)
    params = {"active": active, "interval": interval, "count": count}
    if endtime is not None:
        params["endtime"] = endtime
    status_code, payload = await call_bullex_service("GET", "/candles", auth["user_id"], params=params)
    mark_disconnected_from_payload(auth["user_id"], payload)
    return json_response(status_code, payload)


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
