import json
import os
import sqlite3
import tempfile
from abc import ABC, abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


ROBOT_SETTING_FIELDS = (
    "entry_value",
    "stop_win",
    "stop_loss",
    "cycle_minutes",
    "min_confidence",
    "min_payout",
    "strategy_mode",
    "account_mode",
    "allow_real",
    "confirm_real",
    "max_entries_per_cycle",
)


def require_user_id(user_id: str) -> str:
    normalized = str(user_id or "").strip()
    if not normalized:
        raise ValueError("USER_ID_REQUIRED")
    return normalized


def extract_robot_settings(state: dict[str, Any]) -> dict[str, Any]:
    return {field: state[field] for field in ROBOT_SETTING_FIELDS if field in state}


class RobotPersistence(ABC):
    @abstractmethod
    def save_state(self, user_id: str, state: dict[str, Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    def load_states(self) -> list[tuple[str, dict[str, Any]]]:
        raise NotImplementedError

    @abstractmethod
    def load_state(self, user_id: str) -> dict[str, Any] | None:
        raise NotImplementedError

    @abstractmethod
    def save_settings(self, user_id: str, settings: dict[str, Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    def load_settings(self, user_id: str) -> dict[str, Any] | None:
        raise NotImplementedError

    @abstractmethod
    def save_trade(self, user_id: str, trade: dict[str, Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    def load_trades(self, user_id: str) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def save_trade_history(self, user_id: str, trade: dict[str, Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    def load_trade_history(self, user_id: str, days: int) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def save_restore_status(
        self,
        user_id: str,
        *,
        session_restored: bool,
        robot_restored: bool,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_restore_status(self, user_id: str) -> dict[str, Any]:
        raise NotImplementedError


class SQLiteRobotPersistence(RobotPersistence):
    def __init__(self, database_path: str) -> None:
        self.database_path = database_path
        Path(database_path).parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def save_state(self, user_id: str, state: dict[str, Any]) -> None:
        user_id = require_user_id(user_id)
        with self._connect() as connection:
            connection.execute(
                """
                insert into robot_states (user_id, state_json, updated_at)
                values (?, ?, ?)
                on conflict(user_id) do update set
                    state_json = excluded.state_json,
                    updated_at = excluded.updated_at
                """,
                (user_id, json.dumps(state), utc_iso()),
            )

    def load_states(self) -> list[tuple[str, dict[str, Any]]]:
        with self._connect() as connection:
            rows = connection.execute(
                "select user_id, state_json from robot_states order by user_id"
            ).fetchall()
        return [(row["user_id"], json.loads(row["state_json"])) for row in rows]

    def load_state(self, user_id: str) -> dict[str, Any] | None:
        user_id = require_user_id(user_id)
        with self._connect() as connection:
            row = connection.execute(
                "select state_json from robot_states where user_id = ?",
                (user_id,),
            ).fetchone()
        return json.loads(row["state_json"]) if row is not None else None

    def save_settings(self, user_id: str, settings: dict[str, Any]) -> None:
        user_id = require_user_id(user_id)
        values = extract_robot_settings(settings)
        now = utc_iso()
        with self._connect() as connection:
            connection.execute(
                """
                insert into robot_user_settings (
                    user_id, entry_value, stop_win, stop_loss, cycle_minutes,
                    min_confidence, min_payout, strategy_mode, account_mode,
                    allow_real, confirm_real, max_entries_per_cycle,
                    created_at, updated_at
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                on conflict(user_id) do update set
                    entry_value = excluded.entry_value,
                    stop_win = excluded.stop_win,
                    stop_loss = excluded.stop_loss,
                    cycle_minutes = excluded.cycle_minutes,
                    min_confidence = excluded.min_confidence,
                    min_payout = excluded.min_payout,
                    strategy_mode = excluded.strategy_mode,
                    account_mode = excluded.account_mode,
                    allow_real = excluded.allow_real,
                    confirm_real = excluded.confirm_real,
                    max_entries_per_cycle = excluded.max_entries_per_cycle,
                    updated_at = excluded.updated_at
                """,
                (
                    user_id,
                    values.get("entry_value", 2),
                    values.get("stop_win", 50),
                    values.get("stop_loss", 30),
                    values.get("cycle_minutes", 5),
                    values.get("min_confidence", 94),
                    values.get("min_payout", 88),
                    values.get("strategy_mode", "conservative"),
                    values.get("account_mode", "DEMO"),
                    int(bool(values.get("allow_real", False))),
                    int(bool(values.get("confirm_real", False))),
                    values.get("max_entries_per_cycle", 1),
                    now,
                    now,
                ),
            )

    def load_settings(self, user_id: str) -> dict[str, Any] | None:
        user_id = require_user_id(user_id)
        with self._connect() as connection:
            row = connection.execute(
                """
                select entry_value, stop_win, stop_loss, cycle_minutes,
                       min_confidence, min_payout, strategy_mode, account_mode,
                       allow_real, confirm_real, max_entries_per_cycle
                from robot_user_settings where user_id = ?
                """,
                (user_id,),
            ).fetchone()
        if row is None:
            return None
        settings = dict(row)
        settings["allow_real"] = bool(settings["allow_real"])
        settings["confirm_real"] = bool(settings["confirm_real"])
        return settings

    def save_trade(self, user_id: str, trade: dict[str, Any]) -> None:
        order_id = str(trade.get("order_id") or "").strip()
        if not order_id:
            return
        with self._connect() as connection:
            connection.execute(
                """
                insert into robot_trades (user_id, order_id, trade_json, created_at, updated_at)
                values (?, ?, ?, ?, ?)
                on conflict(user_id, order_id) do update set
                    trade_json = excluded.trade_json,
                    updated_at = excluded.updated_at
                """,
                (user_id, order_id, json.dumps(trade), trade.get("sent_at") or utc_iso(), utc_iso()),
            )

    def load_trades(self, user_id: str) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                select trade_json from robot_trades
                where user_id = ?
                order by created_at desc
                limit 100
                """,
                (user_id,),
            ).fetchall()
        return list(reversed([json.loads(row["trade_json"]) for row in rows]))

    def save_trade_history(self, user_id: str, trade: dict[str, Any]) -> None:
        item = build_trade_history_item(user_id, trade)
        with self._connect() as connection:
            connection.execute(
                """
                insert into robot_trade_history (
                    user_id, created_at, account_mode, active, direction, amount,
                    confidence, payout, order_id, result, profit, opened_at,
                    finished_at, timeframe
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                on conflict(user_id, order_id) do update set
                    account_mode = excluded.account_mode,
                    active = excluded.active,
                    direction = excluded.direction,
                    amount = excluded.amount,
                    confidence = excluded.confidence,
                    payout = excluded.payout,
                    result = excluded.result,
                    profit = excluded.profit,
                    opened_at = excluded.opened_at,
                    finished_at = excluded.finished_at,
                    timeframe = excluded.timeframe
                """,
                (
                    item["user_id"],
                    item["created_at"],
                    item["account_mode"],
                    item["active"],
                    item["direction"],
                    item["amount"],
                    item["confidence"],
                    item["payout"],
                    item["order_id"],
                    item["result"],
                    item["profit"],
                    item["opened_at"],
                    item["finished_at"],
                    item["timeframe"],
                ),
            )

    def load_trade_history(self, user_id: str, days: int) -> list[dict[str, Any]]:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        with self._connect() as connection:
            rows = connection.execute(
                """
                select * from robot_trade_history
                where user_id = ? and finished_at >= ?
                order by finished_at desc, id desc
                """,
                (user_id, cutoff),
            ).fetchall()
        return [dict(row) for row in rows]

    def save_restore_status(
        self,
        user_id: str,
        *,
        session_restored: bool,
        robot_restored: bool,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                insert into robot_restore_status (
                    user_id, session_restored, robot_restored, last_restore_at
                ) values (?, ?, ?, ?)
                on conflict(user_id) do update set
                    session_restored = excluded.session_restored,
                    robot_restored = excluded.robot_restored,
                    last_restore_at = excluded.last_restore_at
                """,
                (user_id, int(session_restored), int(robot_restored), utc_iso()),
            )

    def get_restore_status(self, user_id: str) -> dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute(
                """
                select session_restored, robot_restored, last_restore_at
                from robot_restore_status where user_id = ?
                """,
                (user_id,),
            ).fetchone()
        if row is None:
            return {"session_restored": False, "robot_restored": False, "last_restore_at": None}
        return {
            "session_restored": bool(row["session_restored"]),
            "robot_restored": bool(row["robot_restored"]),
            "last_restore_at": row["last_restore_at"],
        }

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                create table if not exists robot_states (
                    user_id text primary key,
                    state_json text not null,
                    updated_at text not null
                );
                create table if not exists robot_user_settings (
                    user_id text primary key,
                    entry_value real not null default 2,
                    stop_win real not null default 50,
                    stop_loss real not null default 30,
                    cycle_minutes integer not null default 5,
                    min_confidence integer not null default 94,
                    min_payout real not null default 88,
                    strategy_mode text not null default 'conservative',
                    account_mode text not null default 'DEMO',
                    allow_real integer not null default 0,
                    confirm_real integer not null default 0,
                    max_entries_per_cycle integer not null default 1,
                    created_at text not null,
                    updated_at text not null
                );
                create table if not exists robot_trades (
                    user_id text not null,
                    order_id text not null,
                    trade_json text not null,
                    created_at text not null,
                    updated_at text not null,
                    primary key (user_id, order_id)
                );
                create table if not exists robot_trade_history (
                    id integer primary key autoincrement,
                    user_id text not null,
                    created_at text not null,
                    account_mode text not null,
                    active text not null,
                    direction text not null,
                    amount real not null,
                    confidence real not null,
                    payout real not null,
                    order_id text not null,
                    result text not null,
                    profit real not null,
                    opened_at text not null,
                    finished_at text not null,
                    timeframe text not null,
                    unique (user_id, order_id)
                );
                create index if not exists robot_trade_history_user_finished_idx
                    on robot_trade_history (user_id, finished_at desc);
                create table if not exists robot_restore_status (
                    user_id text primary key,
                    session_restored integer not null default 0,
                    robot_restored integer not null default 0,
                    last_restore_at text
                );
                """
            )

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.database_path)
        connection.row_factory = sqlite3.Row
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()


class SupabaseRobotPersistence(RobotPersistence):
    def __init__(self, supabase_url: str, service_role_key: str) -> None:
        self.rest_url = f"{supabase_url.rstrip('/')}/rest/v1"
        self.headers = {
            "apikey": service_role_key,
            "Authorization": f"Bearer {service_role_key}",
            "Content-Type": "application/json",
        }

    def save_state(self, user_id: str, state: dict[str, Any]) -> None:
        user_id = require_user_id(user_id)
        self._ensure_user(user_id)
        body = {
            "user_id": user_id,
            "enabled": state.get("enabled", False),
            "account_mode": state.get("account_mode", "DEMO"),
            "entry_value": state.get("entry_value", 2),
            "cycle_minutes": state.get("cycle_minutes", 5),
            "min_confidence": state.get("min_confidence", 94),
            "min_payout": state.get("min_payout", 88),
            "stop_win": state.get("stop_win", 50),
            "stop_loss": state.get("stop_loss", 30),
            "wins": state.get("wins", 0),
            "losses": state.get("losses", 0),
            "profit": state.get("profit", 0),
            "accuracy": state.get("accuracy", 0),
            "state_json": state,
        }
        self._request(
            "POST",
            "/robot_states?on_conflict=user_id",
            json=body,
            extra_headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
        )

    def load_states(self) -> list[tuple[str, dict[str, Any]]]:
        rows = self._request("GET", "/robot_states?select=user_id,state_json")
        return [(row["user_id"], row.get("state_json") or {}) for row in rows]

    def load_state(self, user_id: str) -> dict[str, Any] | None:
        user_id = require_user_id(user_id)
        rows = self._request(
            "GET",
            f"/robot_states?user_id=eq.{quote(user_id, safe='')}"
            "&select=state_json&limit=1",
        )
        if not rows:
            return None
        return rows[0].get("state_json") or {}

    def save_settings(self, user_id: str, settings: dict[str, Any]) -> None:
        user_id = require_user_id(user_id)
        self._ensure_user(user_id)
        body = {"user_id": user_id, **extract_robot_settings(settings)}
        self._request(
            "POST",
            "/robot_user_settings?on_conflict=user_id",
            json=body,
            extra_headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
        )

    def load_settings(self, user_id: str) -> dict[str, Any] | None:
        user_id = require_user_id(user_id)
        fields = ",".join(ROBOT_SETTING_FIELDS)
        rows = self._request(
            "GET",
            f"/robot_user_settings?user_id=eq.{quote(user_id, safe='')}"
            f"&select={fields}&limit=1",
        )
        return rows[0] if rows else None

    def save_trade(self, user_id: str, trade: dict[str, Any]) -> None:
        self._ensure_user(user_id)
        order_id = str(trade.get("order_id") or "").strip()
        if not order_id:
            return
        body = {
            "user_id": user_id,
            "order_id": order_id,
            "active": trade.get("active"),
            "direction": trade.get("direction"),
            "entry_value": trade.get("amount"),
            "result": trade.get("result"),
            "payout": trade.get("payout"),
            "profit": trade.get("profit"),
            "executed_at": trade.get("sent_at") or utc_iso(),
            "trade_json": trade,
        }
        self._request(
            "POST",
            "/robot_trades?on_conflict=user_id,order_id",
            json=body,
            extra_headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
        )

    def load_trades(self, user_id: str) -> list[dict[str, Any]]:
        rows = self._request(
            "GET",
            f"/robot_trades?user_id=eq.{quote(user_id, safe='')}&select=trade_json&order=executed_at.desc&limit=100",
        )
        return list(reversed([row.get("trade_json") or {} for row in rows]))

    def save_trade_history(self, user_id: str, trade: dict[str, Any]) -> None:
        self._ensure_user(user_id)
        item = build_trade_history_item(user_id, trade)
        item.pop("id", None)
        self._request(
            "POST",
            "/robot_trade_history?on_conflict=user_id,order_id",
            json=item,
            extra_headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
        )

    def load_trade_history(self, user_id: str, days: int) -> list[dict[str, Any]]:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        return self._request(
            "GET",
            f"/robot_trade_history?user_id=eq.{quote(user_id, safe='')}"
            f"&finished_at=gte.{quote(cutoff, safe=':-')}"
            "&select=*&order=finished_at.desc,id.desc",
        )

    def save_restore_status(
        self,
        user_id: str,
        *,
        session_restored: bool,
        robot_restored: bool,
    ) -> None:
        self._ensure_user(user_id)
        self._request(
            "POST",
            "/robot_restore_status?on_conflict=user_id",
            json={
                "user_id": user_id,
                "session_restored": session_restored,
                "robot_restored": robot_restored,
                "last_restore_at": utc_iso(),
            },
            extra_headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
        )

    def get_restore_status(self, user_id: str) -> dict[str, Any]:
        rows = self._request(
            "GET",
            f"/robot_restore_status?user_id=eq.{quote(user_id, safe='')}"
            "&select=session_restored,robot_restored,last_restore_at",
        )
        if not rows:
            return {"session_restored": False, "robot_restored": False, "last_restore_at": None}
        return rows[0]

    def _ensure_user(self, user_id: str) -> None:
        self._request(
            "POST",
            "/users?on_conflict=id",
            json={"id": user_id},
            extra_headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
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
            response = client.request(method, f"{self.rest_url}{path}", headers=headers, json=json)
        response.raise_for_status()
        return response.json() if response.content else []


def create_robot_persistence() -> RobotPersistence:
    supabase_url = os.getenv("SUPABASE_URL", "").strip()
    service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    if supabase_url and service_role_key:
        return SupabaseRobotPersistence(supabase_url, service_role_key)
    default_path = str(Path(tempfile.gettempdir()) / "elcapo-backend-persistence.db")
    database_path = os.getenv("ROBOT_DB_PATH", default_path).strip()
    return SQLiteRobotPersistence(database_path)


def build_trade_history_item(user_id: str, trade: dict[str, Any]) -> dict[str, Any]:
    result = str(trade.get("result") or "").strip().upper()
    if result not in {"WIN", "LOSS"}:
        raise ValueError("TRADE_RESULT_NOT_FINAL")
    order_id = str(trade.get("order_id") or "").strip()
    if not order_id:
        raise ValueError("ORDER_ID_MISSING")
    opened_at = str(trade.get("sent_at") or trade.get("timestamp") or "").strip()
    finished_at = str(trade.get("finished_at") or "").strip()
    if not opened_at or not finished_at:
        raise ValueError("TRADE_TIMESTAMPS_MISSING")
    return {
        "user_id": user_id,
        "created_at": finished_at,
        "account_mode": str(trade.get("mode") or trade.get("account_mode") or "DEMO").upper(),
        "active": str(trade.get("active") or ""),
        "direction": str(trade.get("direction") or "").upper(),
        "amount": float(trade.get("amount") or 0),
        "confidence": float(trade.get("confidence") or 0),
        "payout": float(trade.get("payout") or 0),
        "order_id": order_id,
        "result": result,
        "profit": float(trade.get("profit") or 0),
        "opened_at": opened_at,
        "finished_at": finished_at,
        "timeframe": str(trade.get("expiration") or trade.get("timeframe") or "M1").upper(),
    }
