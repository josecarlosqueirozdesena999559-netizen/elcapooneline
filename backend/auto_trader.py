import asyncio
import math
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


AccountMode = Literal["DEMO", "REAL"]
Timeframe = Literal["M1", "M5", "M15", "M30"]

STATUS_STOPPED = "STOPPED"
STATUS_WAITING_NEXT_CYCLE = "WAITING_NEXT_CYCLE"
STATUS_ANALYZING = "ANALYZING"
STATUS_SIGNAL_REJECTED = "SIGNAL_REJECTED"
STATUS_ENTRY_SENT = "ENTRY_SENT"
STATUS_PENDING_RESULT = "PENDING_RESULT"
STATUS_ERROR = "ERROR"
STATUS_REAL_TRADING_LOCKED = "REAL_TRADING_LOCKED"
STATUS_WAITING_ENTRY_WINDOW = "WAITING_ENTRY_WINDOW"

TIMEFRAME_SECONDS = {"M1": 60, "M5": 300, "M15": 900, "M30": 1800}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


class RobotConfigUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool | None = None
    account_mode: AccountMode | None = None
    timeframe: Timeframe | None = None
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
    timeframe: Timeframe = "M1"
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
    server_time: str | None = None
    entry_window_open: bool = False
    seconds_until_entry_window: int = 0
    current_candle_seconds: float = 0.0
    expiration_seconds: int = 60

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
        configured_expiration = TIMEFRAME_SECONDS[self.timeframe]
        data["expiration_seconds"] = configured_expiration
        if self.last_trade is not None:
            trade = dict(self.last_trade)
            trade["result"] = trade.get("result") or STATUS_PENDING_RESULT
            data["last_trade"] = trade
            if self.operation_in_progress:
                expires_at = parse_datetime(trade.get("expires_at"))
                if expires_at is None:
                    sent_at = parse_datetime(trade.get("sent_at") or trade.get("timestamp"))
                    if sent_at is not None:
                        expires_at = sent_at + timedelta(seconds=configured_expiration)
                if expires_at is not None:
                    data["expiration_seconds"] = max(
                        0,
                        math.ceil((expires_at - now).total_seconds()),
                    )
        total = self.wins + self.losses
        data["accuracy"] = round((self.wins / total) * 100, 2) if total else 0.0
        return data


@dataclass
class AutoTrader:
    _states: dict[str, RobotState] = field(default_factory=dict)
    _locks: dict[str, asyncio.Lock] = field(default_factory=dict)
    _histories: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    _completed_order_ids: dict[str, set[str]] = field(default_factory=dict)

    def get(self, user_id: str) -> RobotState:
        return self._states.setdefault(user_id, RobotState())

    def restore(
        self,
        user_id: str,
        payload: dict[str, Any],
        trades: list[dict[str, Any]] | None = None,
    ) -> RobotState:
        state = RobotState()
        datetime_fields = {"current_cycle_started_at", "next_cycle_at", "last_entry_at"}
        for key, value in payload.items():
            if not hasattr(state, key) or key in {"accuracy", "seconds_until_next_cycle"}:
                continue
            if key in datetime_fields and isinstance(value, str):
                value = datetime.fromisoformat(value.replace("Z", "+00:00"))
            setattr(state, key, value)

        if state.enabled and not state.operation_in_progress:
            state.status = STATUS_WAITING_NEXT_CYCLE
            state.rejection_reason = None
        self._states[user_id] = state

        restored_trades = [dict(trade) for trade in (trades or [])]
        self._histories[user_id] = [
            trade for trade in restored_trades if trade.get("result") in {"WIN", "LOSS", "TIMEOUT"}
        ][-100:]
        self._completed_order_ids[user_id] = {
            str(trade.get("order_id"))
            for trade in self._histories[user_id]
            if trade.get("order_id") is not None
        }
        return state

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

        if changes.get("account_mode") == "REAL":
            state.enabled = False
            state.status = STATUS_STOPPED
            state.next_cycle_at = None

        if "enabled" in changes:
            if state.enabled and state.account_mode == "DEMO":
                state.status = STATUS_WAITING_NEXT_CYCLE
                state.next_cycle_at = utc_now()
            else:
                state.enabled = False
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
        if state.operation_in_progress:
            state.status = STATUS_PENDING_RESULT
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
        trade = dict(trade)
        order_id = str(trade.get("order_id") or "").strip()
        if not order_id or order_id in self._completed_order_ids.setdefault(user_id, set()):
            return state
        if state.operation_in_progress:
            return state
        sent_at = parse_datetime(trade.get("sent_at") or trade.get("timestamp")) or utc_now()
        trade["sent_at"] = sent_at.isoformat()
        trade.setdefault("timestamp", trade["sent_at"])
        trade["result"] = trade.get("result") or STATUS_PENDING_RESULT
        trade.setdefault(
            "expires_at",
            (sent_at + timedelta(seconds=TIMEFRAME_SECONDS[state.timeframe])).isoformat(),
        )
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

    def update_entry_window(self, user_id: str, window: dict[str, Any]) -> RobotState:
        state = self.get(user_id)
        state.server_time = window["server_time"]
        state.entry_window_open = bool(window["entry_window_open"])
        state.seconds_until_entry_window = int(window["seconds_until_entry_window"])
        state.current_candle_seconds = float(window["current_candle_seconds"])
        state.expiration_seconds = int(window["expiration_seconds"])
        if not state.entry_window_open and state.enabled and not state.operation_in_progress:
            state.status = STATUS_WAITING_ENTRY_WINDOW
            state.rejection_reason = STATUS_WAITING_ENTRY_WINDOW
            state.next_cycle_at = utc_now() + timedelta(seconds=state.seconds_until_entry_window)
        elif state.entry_window_open and state.status == STATUS_WAITING_ENTRY_WINDOW:
            state.status = STATUS_WAITING_NEXT_CYCLE if state.enabled else STATUS_STOPPED
            state.rejection_reason = None
        return state

    def finish_trade(self, user_id: str, order_id: Any, result: str, profit: float) -> tuple[bool, RobotState]:
        state = self.get(user_id)
        normalized_order_id = str(order_id or "").strip()
        completed = self._completed_order_ids.setdefault(user_id, set())
        if not normalized_order_id or normalized_order_id in completed:
            return False, state
        if not state.operation_in_progress or not state.last_trade:
            return False, state
        if str(state.last_trade.get("order_id") or "").strip() != normalized_order_id:
            return False, state

        normalized_result = str(result or "").strip().upper()
        if normalized_result not in {"WIN", "LOSS"}:
            return False, state

        trade = dict(state.last_trade)
        amount = float(trade.get("amount") or 0)
        trade_profit = float(profit)
        if normalized_result == "WIN":
            state.wins += 1
            state.profit += trade_profit
        else:
            state.losses += 1
            trade_profit = trade_profit if trade_profit < 0 else -amount
            state.profit += trade_profit

        trade.update(
            {
                "result": normalized_result,
                "profit": round(trade_profit, 2),
                "finished_at": utc_now().isoformat(),
            }
        )
        completed.add(normalized_order_id)
        state.last_trade = trade
        state.operation_in_progress = False
        state.status = STATUS_WAITING_NEXT_CYCLE if state.enabled else STATUS_STOPPED
        state.rejection_reason = None
        state.profit = round(state.profit, 2)
        history = self._histories.setdefault(user_id, [])
        history.append(dict(trade))
        del history[:-100]
        return True, state

    def timeout_trade(self, user_id: str, order_id: Any) -> tuple[bool, RobotState]:
        state = self.get(user_id)
        normalized_order_id = str(order_id or "").strip()
        completed = self._completed_order_ids.setdefault(user_id, set())
        if not normalized_order_id or normalized_order_id in completed:
            return False, state
        if not state.operation_in_progress or not state.last_trade:
            return False, state
        if str(state.last_trade.get("order_id") or "").strip() != normalized_order_id:
            return False, state

        trade = dict(state.last_trade)
        trade.update(
            {
                "result": "TIMEOUT",
                "profit": 0.0,
                "finished_at": utc_now().isoformat(),
            }
        )
        completed.add(normalized_order_id)
        state.last_trade = trade
        state.operation_in_progress = False
        state.status = STATUS_WAITING_NEXT_CYCLE if state.enabled else STATUS_STOPPED
        state.rejection_reason = "TRADE_RESULT_TIMEOUT"
        history = self._histories.setdefault(user_id, [])
        history.append(dict(trade))
        del history[:-100]
        return True, state

    def history(self, user_id: str) -> dict[str, Any]:
        state = self.get(user_id)
        total = state.wins + state.losses
        return {
            "wins": state.wins,
            "losses": state.losses,
            "profit": state.profit,
            "accuracy": round((state.wins / total) * 100, 2) if total else 0.0,
            "trades": list(reversed(self._histories.get(user_id, []))),
        }
