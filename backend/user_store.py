import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import httpx


@dataclass
class BullExUserRecord:
    user_id: str
    bullex_email: str | None = None
    connected: bool | None = None
    requires_2fa: bool | None = None
    account_mode: str | None = None
    currency: str | None = None
    last_balance: float | None = None


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


class InMemoryUserStore(UserStore):
    def __init__(self) -> None:
        self.users: dict[str, BullExUserRecord] = {}

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
            f"/bullex_connections?user_id=eq.{quote(user_id, safe='')}&select=user_id,bullex_email,connected,requires_2fa,account_mode,currency,last_balance",
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

    def _ensure_user_row(self, user_id: str) -> None:
        body = {"id": user_id}
        self._request(
            "POST",
            "/users?on_conflict=id",
            json=body,
            extra_headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
        )

    def _upsert_connection(self, user_id: str, payload: dict[str, Any]) -> BullExUserRecord:
        body = {"user_id": user_id, **payload}
        rows = self._request(
            "POST",
            "/bullex_connections?on_conflict=user_id",
            json=body,
            extra_headers={"Prefer": "resolution=merge-duplicates,return=representation"},
        )
        return self._to_record(rows[0] if rows else body)

    def _to_record(self, row: dict[str, Any]) -> BullExUserRecord:
        return BullExUserRecord(
            user_id=row["user_id"],
            bullex_email=row.get("bullex_email"),
            connected=row.get("connected"),
            requires_2fa=row.get("requires_2fa"),
            account_mode=row.get("account_mode"),
            currency=row.get("currency"),
            last_balance=row.get("last_balance"),
        )

    def _request(
        self,
        method: str,
        path: str,
        json: dict[str, Any] | None = None,
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
