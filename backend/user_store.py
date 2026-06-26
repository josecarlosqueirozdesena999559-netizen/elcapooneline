import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from typing import Any
from urllib.parse import quote

import httpx


logger = logging.getLogger("backend-gateway")

BULLEX_CONNECTION_TABLE = "bullex_connections"
BULLEX_CONNECTION_FIELDS = (
    "user_id",
    "bullex_email",
    "connected",
    "requires_2fa",
    "account_mode",
    "currency",
    "last_balance",
    "last_connected_at",
)
BULLEX_CONNECTION_OPTIONAL_FIELDS = {"last_connected_at"}


@dataclass
class BullExUserRecord:
    user_id: str
    bullex_email: str | None = None
    connected: bool | None = None
    requires_2fa: bool | None = None
    account_mode: str | None = None
    currency: str | None = None
    last_balance: float | None = None
    last_connected_at: str | None = None


class UserStore(ABC):
    @abstractmethod
    def get_user(self, user_id: str) -> BullExUserRecord | None:
        raise NotImplementedError

    @abstractmethod
    def save_connection(self, user_id: str, payload: dict[str, Any]) -> BullExUserRecord:
        raise NotImplementedError

    @abstractmethod
    def update_connection(self, user_id: str, payload: dict[str, Any]) -> BullExUserRecord:
        raise NotImplementedError

    @abstractmethod
    def disconnect(self, user_id: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def save_market_assets_snapshot(self, user_id: str, assets: list[dict[str, Any]]) -> None:
        raise NotImplementedError

    @abstractmethod
    def save_market_asset_payout(self, user_id: str, symbol: str, payout: int | float | None) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_market_assets_snapshot(self, user_id: str) -> list[dict[str, Any]]:
        raise NotImplementedError

    def connection_upsert_diagnostic(
        self,
        user_id: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        body = {"user_id": user_id, **(payload or {})}
        filtered = {key: value for key, value in body.items() if key in BULLEX_CONNECTION_FIELDS}
        return {
            "table": BULLEX_CONNECTION_TABLE,
            "fields": list(BULLEX_CONNECTION_FIELDS),
            "payload": filtered,
        }


class InMemoryUserStore(UserStore):
    def __init__(self) -> None:
        self.users: dict[str, BullExUserRecord] = {}
        self.market_assets: dict[str, dict[str, dict[str, Any]]] = {}

    def get_user(self, user_id: str) -> BullExUserRecord | None:
        return self.users.get(user_id)

    def save_connection(self, user_id: str, payload: dict[str, Any]) -> BullExUserRecord:
        return self._upsert(user_id, payload)

    def update_connection(self, user_id: str, payload: dict[str, Any]) -> BullExUserRecord:
        return self._upsert(user_id, payload)

    def disconnect(self, user_id: str) -> None:
        record = self.users.get(user_id) or BullExUserRecord(user_id=user_id)
        record.connected = False
        record.requires_2fa = False
        self.users[user_id] = record

    def save_market_assets_snapshot(self, user_id: str, assets: list[dict[str, Any]]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        existing_assets = self.market_assets.setdefault(user_id, {})
        for asset in assets:
            symbol = asset.get("symbol")
            if not symbol:
                continue
            previous = existing_assets.get(symbol, {})
            incoming_payout = asset.get("payout")
            existing_assets[symbol] = {
                "user_id": user_id,
                "active_id": asset.get("active_id"),
                "symbol": symbol,
                "name": asset.get("name") or symbol,
                "enabled": asset.get("enabled", True),
                "payout": incoming_payout if incoming_payout is not None else previous.get("payout"),
                "last_seen_at": now,
                "updated_at": now,
            }

    def save_market_asset_payout(self, user_id: str, symbol: str, payout: int | float | None) -> None:
        if payout is None:
            return
        now = datetime.now(timezone.utc).isoformat()
        existing_assets = self.market_assets.setdefault(user_id, {})
        previous = existing_assets.get(symbol, {})
        existing_assets[symbol] = {
            "user_id": user_id,
            "active_id": previous.get("active_id"),
            "symbol": symbol,
            "name": previous.get("name") or symbol,
            "enabled": previous.get("enabled", True),
            "payout": payout,
            "last_seen_at": now,
            "updated_at": now,
        }
        logger.info("MARKET_ASSET_PAYOUT_UPDATED %s %s %s", user_id, symbol, payout)

    def get_market_assets_snapshot(self, user_id: str) -> list[dict[str, Any]]:
        assets = self.market_assets.get(user_id, {})
        return [dict(asset) for _, asset in sorted(assets.items())]

    def _upsert(self, user_id: str, payload: dict[str, Any]) -> BullExUserRecord:
        record = self.users.get(user_id) or BullExUserRecord(user_id=user_id)
        for key, value in payload.items():
            if hasattr(record, key):
                setattr(record, key, value)
        self.users[user_id] = record
        return record


class SupabaseUserStore(UserStore):
    def __init__(self, supabase_url: str, service_role_key: str) -> None:
        self.base_url = supabase_url.rstrip("/")
        self.service_role_key = service_role_key
        self.rest_url = f"{self.base_url}/rest/v1"
        self.headers = {
            "apikey": self.service_role_key,
            "Authorization": f"Bearer {self.service_role_key}",
            "Content-Type": "application/json",
        }

    def get_user(self, user_id: str) -> BullExUserRecord | None:
        self._ensure_user_row(user_id)
        rows = self._request(
            "GET",
            f"/bullex_connections?user_id=eq.{quote(user_id, safe='')}&select=user_id,bullex_email,connected,requires_2fa,account_mode,currency,last_balance,last_connected_at",
        )
        if not rows:
            return None
        return self._to_record(rows[0])

    def save_connection(self, user_id: str, payload: dict[str, Any]) -> BullExUserRecord:
        self._ensure_user_row(user_id)
        return self._upsert_connection(user_id, payload)

    def update_connection(self, user_id: str, payload: dict[str, Any]) -> BullExUserRecord:
        self._ensure_user_row(user_id)
        return self._upsert_connection(user_id, payload)

    def disconnect(self, user_id: str) -> None:
        self._ensure_user_row(user_id)
        body = {"user_id": user_id, "connected": False, "requires_2fa": False}
        self._request(
            "POST",
            "/bullex_connections?on_conflict=user_id",
            json=body,
            extra_headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
        )

    def save_market_assets_snapshot(self, user_id: str, assets: list[dict[str, Any]]) -> None:
        self._ensure_user_row(user_id)
        rows = []
        now = datetime.now(timezone.utc).isoformat()
        existing_payouts = self._get_existing_market_asset_payouts(user_id)
        for asset in assets:
            symbol = asset.get("symbol")
            if not symbol:
                continue
            incoming_payout = asset.get("payout")
            rows.append(
                {
                    "user_id": user_id,
                    "active_id": asset.get("active_id"),
                    "symbol": symbol,
                    "name": asset.get("name") or symbol,
                    "enabled": asset.get("enabled", True),
                    "payout": incoming_payout if incoming_payout is not None else existing_payouts.get(symbol),
                    "last_seen_at": now,
                }
            )
        if not rows:
            return
        self._request(
            "POST",
            "/market_assets?on_conflict=user_id,symbol",
            json=rows,
            extra_headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
        )

    def save_market_asset_payout(self, user_id: str, symbol: str, payout: int | float | None) -> None:
        self._ensure_user_row(user_id)
        if payout is None:
            return
        now = datetime.now(timezone.utc).isoformat()
        self._request(
            "POST",
            "/market_assets?on_conflict=user_id,symbol",
            json={
                "user_id": user_id,
                "symbol": symbol,
                "payout": payout,
                "last_seen_at": now,
            },
            extra_headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
        )
        logger.info("MARKET_ASSET_PAYOUT_UPDATED %s %s %s", user_id, symbol, payout)

    def get_market_assets_snapshot(self, user_id: str) -> list[dict[str, Any]]:
        self._ensure_user_row(user_id)
        rows = self._request(
            "GET",
            (
                f"/market_assets?user_id=eq.{quote(user_id, safe='')}"
                "&select=user_id,active_id,symbol,name,enabled,payout,last_seen_at,updated_at"
                "&order=symbol.asc"
            ),
        )
        return [row for row in rows if isinstance(row, dict)]

    def _ensure_user_row(self, user_id: str) -> None:
        body = {"id": user_id}
        self._request(
            "POST",
            "/users?on_conflict=id",
            json=body,
            extra_headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
        )

    def _upsert_connection(self, user_id: str, payload: dict[str, Any]) -> BullExUserRecord:
        diagnostic = self.connection_upsert_diagnostic(user_id, payload)
        body = diagnostic["payload"]
        path = f"/{BULLEX_CONNECTION_TABLE}?on_conflict=user_id"
        headers = {"Prefer": "resolution=merge-duplicates,return=representation"}
        try:
            rows = self._request("POST", path, json=body, extra_headers=headers)
        except httpx.HTTPStatusError as exc:
            if not self._is_optional_field_schema_error(exc, body):
                raise
            fallback_body = {
                key: value for key, value in body.items() if key not in BULLEX_CONNECTION_OPTIONAL_FIELDS
            }
            logger.warning(
                "[SUPABASE UPSERT RETRY] table=%s removed_fields=%s",
                BULLEX_CONNECTION_TABLE,
                sorted(BULLEX_CONNECTION_OPTIONAL_FIELDS.intersection(body)),
            )
            rows = self._request("POST", path, json=fallback_body, extra_headers=headers)
            body = fallback_body
        return self._to_record(rows[0] if rows else body)

    def _is_optional_field_schema_error(
        self,
        exc: httpx.HTTPStatusError,
        body: dict[str, Any],
    ) -> bool:
        if exc.response.status_code != 400:
            return False
        response_text = exc.response.text.lower()
        return any(
            field in body and field.lower() in response_text
            for field in BULLEX_CONNECTION_OPTIONAL_FIELDS
        )

    def _get_existing_market_asset_payouts(self, user_id: str) -> dict[str, Any]:
        rows = self._request(
            "GET",
            f"/market_assets?user_id=eq.{quote(user_id, safe='')}&select=symbol,payout",
        )
        payouts: dict[str, Any] = {}
        for row in rows:
            symbol = row.get("symbol")
            if symbol:
                payouts[symbol] = row.get("payout")
        return payouts

    def _to_record(self, row: dict[str, Any]) -> BullExUserRecord:
        return BullExUserRecord(
            user_id=row["user_id"],
            bullex_email=row.get("bullex_email"),
            connected=row.get("connected"),
            requires_2fa=row.get("requires_2fa"),
            account_mode=row.get("account_mode"),
            currency=row.get("currency"),
            last_balance=row.get("last_balance"),
            last_connected_at=row.get("last_connected_at"),
        )

    def _request(
        self,
        method: str,
        path: str,
        json: Any = None,
        extra_headers: dict[str, str] | None = None,
    ) -> Any:
        headers = dict(self.headers)
        if extra_headers:
            headers.update(extra_headers)

        with httpx.Client(timeout=20.0) as client:
            response = client.request(
                method=method,
                url=f"{self.rest_url}{path}",
                headers=headers,
                json=json,
            )

        if response.status_code >= 400:
            logger.warning(
                "[SUPABASE HTTP ERROR] status=%s url=%s payload=%s response=%s",
                response.status_code,
                response.request.url,
                json,
                response.text,
            )
        response.raise_for_status()
        if not response.content:
            return []
        return response.json()


def create_user_store() -> UserStore:
    supabase_url = os.getenv("SUPABASE_URL", "").strip()
    service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    if supabase_url and service_role_key:
        return SupabaseUserStore(supabase_url, service_role_key)
    return InMemoryUserStore()
