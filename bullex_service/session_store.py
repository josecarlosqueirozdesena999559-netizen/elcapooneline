import base64
import hashlib
import logging
import os
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from collections.abc import Iterator

from cryptography.fernet import Fernet, InvalidToken


logger = logging.getLogger("bullex-service")


@dataclass
class PersistedSession:
    user_id: str
    email: str
    account_mode: str
    session_token: str
    last_connected_at: str | None


class SessionStore:
    def __init__(self, database_path: str, encryption_secret: str) -> None:
        self.database_path = database_path
        self._fernet = Fernet(self._derive_key(encryption_secret))
        Path(database_path).parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def save_connected(
        self,
        user_id: str,
        email: str,
        account_mode: str,
        session_token: str,
    ) -> None:
        encrypted_token = self._fernet.encrypt(session_token.encode("utf-8")).decode("ascii")
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as connection:
            connection.execute(
                """
                insert into bullex_sessions (
                    user_id, email, account_mode, connected, encrypted_session_token,
                    last_connected_at, updated_at
                ) values (?, ?, ?, 1, ?, ?, ?)
                on conflict(user_id) do update set
                    email = excluded.email,
                    account_mode = excluded.account_mode,
                    connected = 1,
                    encrypted_session_token = excluded.encrypted_session_token,
                    last_connected_at = excluded.last_connected_at,
                    updated_at = excluded.updated_at
                """,
                (user_id, email, account_mode, encrypted_token, now, now),
            )
        self._log_session_value("SESSION_SAVE", user_id, session_token)

    def mark_disconnected(self, user_id: str, *, revoke_token: bool = False) -> None:
        now = datetime.now(timezone.utc).isoformat()
        token_update = ", encrypted_session_token = null" if revoke_token else ""
        with self._connect() as connection:
            connection.execute(
                f"""
                update bullex_sessions
                set connected = 0, updated_at = ?{token_update}
                where user_id = ?
                """,
                (now, user_id),
            )

    def load_connected_user(self, user_id: str) -> PersistedSession | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                select user_id, email, account_mode, encrypted_session_token, last_connected_at
                from bullex_sessions
                where user_id = ? and connected = 1 and encrypted_session_token is not null
                limit 1
                """,
                (user_id,),
            ).fetchone()
        if row is None:
            return None
        try:
            token = self._fernet.decrypt(row["encrypted_session_token"].encode("ascii")).decode("utf-8")
        except (InvalidToken, UnicodeError):
            self._log_session_value("SESSION_LOAD", user_id, None)
            self.mark_disconnected(user_id)
            return None
        self._log_session_value("SESSION_LOAD", user_id, token)
        return PersistedSession(
            user_id=row["user_id"],
            email=row["email"],
            account_mode=row["account_mode"],
            session_token=token,
            last_connected_at=row["last_connected_at"],
        )

    def persistence_debug(self) -> dict[str, object]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                select user_id, email, encrypted_session_token, last_connected_at
                from bullex_sessions
                order by user_id
                """
            ).fetchall()

        users = []
        for row in rows:
            encrypted_token = row["encrypted_session_token"]
            ssid_present = False
            if encrypted_token:
                try:
                    token = self._fernet.decrypt(encrypted_token.encode("ascii")).decode("utf-8")
                    ssid_present = bool(token)
                except (InvalidToken, UnicodeError):
                    ssid_present = False
            users.append(
                {
                    "user_id": row["user_id"],
                    "ssid_present": ssid_present,
                    "session_file_exists": Path(self.database_path).is_file(),
                    "last_connected_at": row["last_connected_at"],
                }
            )

        return {
            "stored_sessions": len(users),
            "users": users,
        }

    def _log_session_value(self, event: str, user_id: str, ssid: str | None) -> None:
        logger.info(
            "[%s] user_id=%s ssid_present=%s ssid_length=%s path=%s "
            "persisted_fields=ssid token_present=False cookies_present=False "
            "session_data_present=False ssid_fingerprint=%s",
            event,
            user_id,
            bool(ssid),
            len(ssid) if ssid else 0,
            self.database_path,
            hashlib.sha256(ssid.encode("utf-8")).hexdigest()[:12] if ssid else "none",
        )

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                create table if not exists bullex_sessions (
                    user_id text primary key,
                    email text not null,
                    account_mode text not null,
                    connected integer not null default 0,
                    encrypted_session_token text,
                    last_connected_at text,
                    updated_at text not null
                )
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

    @staticmethod
    def _derive_key(secret: str) -> bytes:
        return base64.urlsafe_b64encode(hashlib.sha256(secret.encode("utf-8")).digest())


def create_session_store() -> SessionStore | None:
    secret = os.getenv("BULLEX_SESSION_ENCRYPTION_KEY", "").strip()
    if not secret:
        return None
    database_path = os.getenv("BULLEX_SESSION_DB_PATH", "/data/bullex-sessions.db").strip()
    return SessionStore(database_path, secret)
