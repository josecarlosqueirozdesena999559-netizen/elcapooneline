import logging
import os
from typing import Any

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from backend.user_store import UserStore, create_user_store


logger = logging.getLogger("backend-gateway")


def build_success(data: Any) -> dict[str, Any]:
    return {"ok": True, "data": data, "error": None}


def build_error(message: str) -> dict[str, Any]:
    return {"ok": False, "data": None, "error": message}


class GatewayConfig:
    def __init__(self) -> None:
        self.bullex_service_url = os.getenv("BULLEX_SERVICE_URL", "http://bullex-service:8000").rstrip("/")
        self.panel_api_key = os.getenv("PANEL_API_KEY", "")
        self.supabase_url = os.getenv("SUPABASE_URL", "").strip()
        self.supabase_service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
        self.cors_origins = [
            origin.strip()
            for origin in os.getenv(
                "CORS_ORIGINS",
                "http://localhost:5173,http://localhost:5174,http://127.0.0.1:5173,http://127.0.0.1:5174",
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


async def require_headers(
    x_api_key: str | None = Header(default=None),
    x_user_id: str | None = Header(default=None),
) -> dict[str, str]:
    if not config.panel_api_key:
        raise HTTPException(status_code=500, detail="PANEL_API_KEY_NOT_CONFIGURED")
    if x_api_key != config.panel_api_key:
        raise HTTPException(status_code=401, detail="INVALID_API_KEY")

    user_id = (x_user_id or "").strip()
    if not user_id:
        raise HTTPException(status_code=400, detail="MISSING_USER_ID")

    return {"user_id": user_id}


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
    return updates


def sync_user_store_from_payload(
    user_id: str,
    payload: dict[str, Any],
    fallback_email: str | None = None,
    *,
    is_new_connection: bool = False,
) -> None:
    if not payload.get("ok"):
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


@app.get("/health")
async def health() -> dict[str, Any]:
    return build_success({"status": "healthy", "service": "backend-gateway"})


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
    if payload.get("ok") and isinstance(payload.get("data"), list):
        try:
            user_store.save_market_assets_snapshot(auth["user_id"], payload["data"])
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
    params = {"active": active, "interval": interval, "count": count}
    if endtime is not None:
        params["endtime"] = endtime
    status_code, payload = await call_bullex_service("GET", "/candles", auth["user_id"], params=params)
    return json_response(status_code, payload)


@app.get("/bullex/payouts")
async def bullex_payouts(
    active: str | None = None,
    auth: dict[str, str] = Depends(require_headers),
) -> JSONResponse:
    params = {"active": active} if active is not None else None
    status_code, payload = await call_bullex_service("GET", "/payouts", auth["user_id"], params=params)
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
