from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class BullExUserRecord:
    user_id: str
    email: str | None = None
    connected: bool | None = None
    balance: float | None = None
    currency: str | None = None
    mode: str | None = None
    requires_2fa: bool | None = None


class UserStore(ABC):
    @abstractmethod
    def get(self, user_id: str) -> BullExUserRecord | None:
        raise NotImplementedError

    @abstractmethod
    def upsert(self, user_id: str, **fields: Any) -> BullExUserRecord:
        raise NotImplementedError

    @abstractmethod
    def remove(self, user_id: str) -> None:
        raise NotImplementedError


class InMemoryUserStore(UserStore):
    def __init__(self) -> None:
        self.users: dict[str, BullExUserRecord] = {}

    def get(self, user_id: str) -> BullExUserRecord | None:
        return self.users.get(user_id)

    def upsert(self, user_id: str, **fields: Any) -> BullExUserRecord:
        record = self.users.get(user_id) or BullExUserRecord(user_id=user_id)
        for key, value in fields.items():
            if hasattr(record, key):
                setattr(record, key, value)
        self.users[user_id] = record
        return record

    def remove(self, user_id: str) -> None:
        self.users.pop(user_id, None)


def serialize_user(record: BullExUserRecord | None) -> dict[str, Any] | None:
    if record is None:
        return None
    return asdict(record)
