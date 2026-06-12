import asyncio
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


AccountMode = Literal["DEMO", "REAL"]

STATUS_STOPPED = "STOPPED"
STATUS_WAITING_NEXT_CYCLE = "WAITING_NEXT_CYCLE"
STATUS_ANALYZING = "ANALYZING"
STATUS_SIGNAL_REJECTED = "SIGNAL_REJECTED"
STATUS_ENTRY_SENT = "ENTRY_SENT"
STATUS_PENDING_RESULT = "PENDING_RESULT"
STATUS_ERROR = "ERROR"
STATUS_REAL_TRADING_LOCKED = "REAL_TRADING_LOCKED"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class RobotConfigUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool | None = None
    account_mode: AccountMode | None = None
    entry_value: float | None = Field(default=None, gt=0)
    cycle_minutes: int | None = Field(default=None, ge=1)
    min_confidence: int | None = Field(default=None, ge=0, le=100)
    min_payout: float | None = Field(default=None, ge=0, le=100)
    stop_win: float | None = Field(default=None, ge=0)
    stop_loss: float | None = Field(default=None, ge=0)
    max_entries_per_cycle: int | None = Field(default=None, ge=1, le=1)
    allow_real: bool | None = None
    confirm_real: bool | None = None


@dataclass
class RobotState:
    enabled: bool = False
    account_mode: AccountMode = "DEMO"
    entry_value: float = 2.0
    cycle_minutes: int = 10
    min_confidence: int = 85
    min_payout: float = 80.0
    stop_win: float = 50.0
    stop_loss: float = 30.0
    max_entries_per_cycle: int = 1
    allow_real: bool = False
    confirm_real: bool = False
    wins: int = 0
    losses: int = 0
    profit: float = 0.0
    current_cycle_started_at: datetime | None = None
    next_cycle_at: datetime | None = None
    last_entry_at: datetime | None = None
    operation_in_progress: bool = False
    last_signal: dict[str, Any] | None = None
    last_trade: dict[str, Any] | None = None
    status: str = STATUS_STOPPED
    rejection_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        for key in ("current_cycle_started_at", "next_cycle_at", "last_entry_at"):
            value = data[key]
            data[key] = value.isoformat() if value is not None else None
        now = utc_now()
        data["seconds_until_next_cycle"] = (
            max(0, int((self.next_cycle_at - now).total_seconds()))
            if self.next_cycle_at is not None
            else 0
        )
        return data


@dataclass
class AutoTrader:
    _states: dict[str, RobotState] = field(default_factory=dict)
    _locks: dict[str, asyncio.Lock] = field(default_factory=dict)

    def get(self, user_id: str) -> RobotState:
        return self._states.setdefault(user_id, RobotState())

    def lock(self, user_id: str) -> asyncio.Lock:
        return self._locks.setdefault(user_id, asyncio.Lock())

    def update_config(self, user_id: str, update: RobotConfigUpdate) -> RobotState:
        state = self.get(user_id)
        changes = update.model_dump(exclude_none=True)
        for key, value in changes.items():
            setattr(state, key, value)

        if "cycle_minutes" in changes and state.next_cycle_at is not None:
            base = state.current_cycle_started_at or utc_now()
            state.next_cycle_at = base + timedelta(minutes=state.cycle_minutes)

        if "enabled" in changes:
            if state.enabled:
                state.status = STATUS_WAITING_NEXT_CYCLE
                state.next_cycle_at = utc_now()
            else:
                state.status = STATUS_STOPPED
        return state

    def start(self, user_id: str) -> RobotState:
        state = self.get(user_id)
        state.enabled = True
        state.status = STATUS_WAITING_NEXT_CYCLE
        state.rejection_reason = None
        state.next_cycle_at = utc_now()
        return state

    def stop(self, user_id: str) -> RobotState:
        state = self.get(user_id)
        state.enabled = False
        state.status = STATUS_STOPPED
        state.rejection_reason = None
        return state

    def prepare_cycle(self, user_id: str) -> tuple[bool, RobotState]:
        state = self.get(user_id)
        now = utc_now()
        if not state.enabled:
            state.status = STATUS_STOPPED
            return False, state
        if state.next_cycle_at is not None and now < state.next_cycle_at:
            state.status = STATUS_WAITING_NEXT_CYCLE
            return False, state

        state.current_cycle_started_at = now
        state.next_cycle_at = now + timedelta(minutes=state.cycle_minutes)
        state.status = STATUS_ANALYZING
        state.rejection_reason = None
        return True, state

    def reject(self, user_id: str, reason: str) -> RobotState:
        state = self.get(user_id)
        state.status = STATUS_SIGNAL_REJECTED
        state.rejection_reason = reason
        return state

    def fail(self, user_id: str, reason: str) -> RobotState:
        state = self.get(user_id)
        state.status = STATUS_ERROR
        state.rejection_reason = reason
        return state

    def select_signal(self, user_id: str, signal: dict[str, Any]) -> RobotState:
        state = self.get(user_id)
        state.last_signal = signal
        state.rejection_reason = None
        return state

    def record_trade(self, user_id: str, trade: dict[str, Any]) -> RobotState:
        state = self.get(user_id)
        state.last_trade = trade
        state.last_entry_at = utc_now()
        state.operation_in_progress = True
        state.status = STATUS_PENDING_RESULT
        state.rejection_reason = None
        return state

    def lock_real(self, user_id: str, reason: str = "REAL_TRADING_LOCKED") -> RobotState:
        state = self.get(user_id)
        state.status = STATUS_REAL_TRADING_LOCKED
        state.rejection_reason = reason
        return state
