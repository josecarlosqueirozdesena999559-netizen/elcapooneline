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


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bullex-service")

ALLOWED_BALANCE_MODES = {"PRACTICE", "REAL", "TOURNAMENT"}
ALLOWED_ACTIONS = {"call", "put"}
SESSION_NOT_FOUND = "SESSION_NOT_FOUND"
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
    desired_mode: str = "PRACTICE"
    requires_2fa: bool = False
    state: SessionState = field(default_factory=SessionState)


class SessionManager:
    def __init__(self) -> None:
        self.sessions: dict[str, ManagedSession] = {}
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

    def upsert(self, session: ManagedSession) -> ManagedSession:
        self.sessions[session.user_id] = session
        return session

    def remove(self, user_id: str) -> None:
        self.sessions.pop(user_id, None)

    def require(self, user_id: str) -> ManagedSession:
        session = self.get(user_id)
        if session is None:
            raise ServiceError(SESSION_NOT_FOUND, 404)
        return session

    def run(self, user_id: str, operation):
        session = self.require(user_id)
        with self._session_context(session):
            return operation(session)

    def connect(self, user_id: str, payload: ConnectRequest) -> ManagedSession:
        existing = self.get(user_id)

        if payload.sms_code and existing and not payload.email and not payload.password:
            session = existing
            session.desired_mode = normalize_mode(payload.account_mode)
            with self._session_context(session):
                ok, reason = session.client.connect_2fa(payload.sms_code)
                self._finalize_connect(session, ok, reason)
            return session

        if not payload.email or not payload.password:
            raise ServiceError("email e password sao obrigatorios para conectar")

        new_session = ManagedSession(
            user_id=user_id,
            client=Bullex(payload.email, payload.password),
            email=payload.email,
            desired_mode=normalize_mode(payload.account_mode),
        )

        if existing is not None:
            self._close_session(existing)

        self.upsert(new_session)
        with self._session_context(new_session):
            ok, reason = new_session.client.connect(payload.sms_code)
            self._finalize_connect(new_session, ok, reason)
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
        return user_id

    def reconnect(self, user_id: str) -> ManagedSession:
        session = self.require(user_id)
        with self._session_context(session):
            ok, reason = session.client.connect()
            self._finalize_connect(session, ok, reason)
        return session

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
        raise ServiceError("sessao desconectada", 409)


def parse_order_id(order_id: str) -> int | str:
    return int(order_id) if order_id.isdigit() else order_id


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
        account["balance"] = session.client.get_balance()
        account["currency"] = session.client.get_currency()
        account["mode"] = session.client.get_balance_mode()

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
    except Exception:
        logger.exception("falha ao consultar payout digital de %s", active)
        return None
    return payout if payout else None


app = FastAPI(title="bullex-service", version="0.1.0")
session_manager = SessionManager()


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

    connected = False
    active_mode = None
    if not session.requires_2fa:
        active_mode = session_manager.run(user_id, lambda s: s.client.get_balance_mode())
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
def session_status(x_user_id: str | None = Header(default=None)) -> dict[str, Any]:
    user_id = require_user_id(x_user_id)

    def operation(current: ManagedSession) -> dict[str, Any]:
        connected = bool(current.client.check_connect())
        active_mode = current.client.get_balance_mode() if connected and not current.requires_2fa else None
        return {
            "user_id": user_id,
            "connected": connected,
            "requires_2fa": current.requires_2fa,
            "email": current.email,
            "active_mode": active_mode,
        }

    return build_success(session_manager.run(user_id, operation))


@app.post("/sessions/disconnect")
def disconnect_session(x_user_id: str | None = Header(default=None)) -> dict[str, Any]:
    user_id = require_user_id(x_user_id)
    session_manager.disconnect(user_id)
    return build_success({"user_id": user_id, "connected": False})


@app.post("/sessions/reconnect")
def reconnect_session(x_user_id: str | None = Header(default=None)) -> dict[str, Any]:
    user_id = require_user_id(x_user_id)
    session_manager.reconnect(user_id)
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

    def operation(session: ManagedSession) -> dict[str, Any]:
        return build_account_payload(session)

    return build_success(session_manager.run(user_id, operation))


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
        return {"mode": active_mode}

    return build_success(session_manager.run(user_id, operation))


@app.get("/assets")
def list_assets(x_user_id: str | None = Header(default=None)) -> dict[str, Any]:
    user_id = require_user_id(x_user_id)

    def operation(session: ManagedSession) -> list[dict[str, Any]]:
        ensure_session_ready(session)
        try:
            return read_assets(session.client)
        except Exception as exc:
            logger.exception("falha ao listar ativos")
            raise ServiceError("BULLEX_ASSETS_ERROR", 502) from exc

    return build_success(session_manager.run(user_id, operation))


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

    def operation(session: ManagedSession) -> list[dict[str, Any]]:
        ensure_session_ready(session)
        try:
            candles = session.client.get_candles(active, interval, count, resolved_endtime)
        except Exception as exc:
            logger.exception("falha ao obter candles de %s", active)
            raise ServiceError("BULLEX_CANDLES_ERROR", 502) from exc
        if candles is None:
            raise ServiceError("nao foi possivel obter candles", 502)
        return normalize_candles(candles)

    return build_success(session_manager.run(user_id, operation))


@app.get("/payouts")
def get_payouts(active: str | None = None, x_user_id: str | None = Header(default=None)) -> dict[str, Any]:
    user_id = require_user_id(x_user_id)
    active = ensure_binary_asset_allowed(active) if active else None

    def operation(session: ManagedSession) -> list[dict[str, Any]]:
        ensure_session_ready(session)
        if active:
            symbols = [active]
        else:
            try:
                symbols = [asset["symbol"] for asset in read_assets(session.client)]
            except Exception as exc:
                logger.exception("falha ao listar ativos para payouts")
                raise ServiceError("BULLEX_PAYOUTS_ERROR", 502) from exc

        return [
            {
                "symbol": symbol,
                "payout": read_digital_payout(session.client, symbol) if active else None,
                "type": "digital",
            }
            for symbol in symbols
        ]

    return build_success(session_manager.run(user_id, operation))


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
    if not payload.confirm_real:
        raise ServiceError("operacao REAL bloqueada sem confirm_real=true", 403)

    def operation(session: ManagedSession) -> dict[str, Any]:
        ensure_session_ready(session)
        ensure_mode_matches(session.client, "REAL")
        ok, order_id = session.client.buy(payload.amount, payload.active, payload.action, payload.expiration)
        if not ok:
            raise ServiceError(f"falha ao criar ordem real: {order_id}", 409)
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
        result, profit = session.client.check_win_v4(parsed_order_id)
        return {"order_id": parsed_order_id, "result": result, "profit": profit}

    return build_success(session_manager.run(user_id, operation))
