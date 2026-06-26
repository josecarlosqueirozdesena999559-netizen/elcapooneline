import asyncio
import math
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field

AI_FIELD_PREFIX = "ai_"


def is_ai_field_key(key: Any) -> bool:
    text = str(key or "").strip()
    normalized = text.lower()
    return normalized.startswith(AI_FIELD_PREFIX) or text.startswith("ai")


AccountMode = Literal["DEMO", "REAL"]
Timeframe = Literal["M1", "M5", "M15", "M30"]
StrategyMode = Literal["aggressive", "balanced", "conservative"]
StateSource = Literal["memory", "supabase", "default"]

STATUS_STOPPED = "STOPPED"
STATUS_WAITING_NEXT_CYCLE = "WAITING_NEXT_CYCLE"
STATUS_ANALYZING = "ANALYZING"
STATUS_SIGNAL_REJECTED = "SIGNAL_REJECTED"
STATUS_ENTRY_SENT = "ENTRY_SENT"
STATUS_SENDING_ORDER = "SENDING_ORDER"
STATUS_PENDING_RESULT = "PENDING_RESULT"
STATUS_RESULT_RECEIVED = "RESULT_RECEIVED"
STATUS_WAITING_GALE_ENTRY = "WAITING_GALE_ENTRY"
STATUS_SENDING_GALE_ORDER = "SENDING_GALE_ORDER"
STATUS_PENDING_GALE_RESULT = "PENDING_GALE_RESULT"
STATUS_GALE_RESULT_RECEIVED = "GALE_RESULT_RECEIVED"
STATUS_ORDER_REJECTED = "ORDER_REJECTED"
STATUS_ERROR = "ERROR"
STATUS_REAL_TRADING_LOCKED = "REAL_TRADING_LOCKED"
STATUS_WAITING_NEXT_CANDLE_ENTRY = "WAITING_NEXT_CANDLE_ENTRY"
STATUS_WAITING_ENTRY_WINDOW = STATUS_WAITING_NEXT_CANDLE_ENTRY
STATUS_WAITING_ANALYSIS_WINDOW = "WAITING_ANALYSIS_WINDOW"
STATUS_SIGNAL_EXPIRED = "SIGNAL_EXPIRED"
STATUS_STOP_WIN_HIT = "STOP_WIN_HIT"
STATUS_STOP_LOSS_HIT = "STOP_LOSS_HIT"
STATUS_ACCOUNT_DISCONNECTED = "DISCONNECTED"
STATUS_SYNCING = "SYNCING"
STATUS_SYNCING_PT = "SINCRONIZANDO"
STATUS_ANALYSIS_TIMEOUT = "ANALYSIS_TIMEOUT"
STATUS_ANALYSIS_ERROR = "ANALYSIS_ERROR"
STATUS_NO_CANDIDATES = "NO_CANDIDATES"
STATUS_NO_CANDIDATE_THIS_CANDLE = "NO_CANDIDATE_THIS_CANDLE"
STATUS_CONNECTION_BACKOFF = "CONNECTION_BACKOFF"
STATUS_WAITING_RECOVERY = "WAITING_RECOVERY"
STATUS_ACTIVE_COOLDOWN = "ACTIVE_COOLDOWN"
STATUS_PAYOUT_COOLDOWN = "PAYOUT_COOLDOWN"

TEMPORARY_WAIT_STATUSES = {
    STATUS_CONNECTION_BACKOFF,
    STATUS_WAITING_RECOVERY,
    STATUS_ACTIVE_COOLDOWN,
    STATUS_PAYOUT_COOLDOWN,
    STATUS_SIGNAL_EXPIRED,
}

TIMEFRAME_SECONDS = {"M1": 60, "M5": 300, "M15": 900, "M30": 1800}
RESULT_WAITING_MESSAGE = "Aguardando resultado..."
ANALYSIS_MESSAGE = "Analisando mercado..."
DISCONNECTED_MESSAGE = "Conta BullEx desconectada"
STOP_WIN_MESSAGE = "Stop Win atingido. Clique em Reiniciar ciclo para continuar."
STOP_LOSS_MESSAGE = "Stop Loss atingido. Clique em Reiniciar ciclo para continuar."
SIGNAL_EXPIRED_MESSAGE = "Entrada perdida por atraso. Aguardando novo sinal."
ANALYSIS_TIMEOUT_SECONDS = 10
SYNC_TIMEOUT_SECONDS = 30
ANALYSIS_TIMEOUT_MESSAGE = "Análise demorou demais, aguardando próxima vela."
NO_MINIMUM_SCORE_MESSAGE = "Nenhum ativo atingiu score mínimo."


def format_mm_ss(seconds: int) -> str:
    safe_seconds = max(0, int(seconds))
    minutes, remaining_seconds = divmod(safe_seconds, 60)
    return f"{minutes:02d}:{remaining_seconds:02d}"


def strip_ai_fields(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: strip_ai_fields(item)
            for key, item in value.items()
            if not is_ai_field_key(key)
        }
    if isinstance(value, list):
        return [strip_ai_fields(item) for item in value]
    return value


def format_best_candidate_summary(candidate: dict[str, Any] | None) -> str | None:
    if not isinstance(candidate, dict):
        return None
    symbol = str(candidate.get("symbol") or "").strip()
    direction = str(candidate.get("direction") or candidate.get("signal") or "").strip().upper()
    try:
        confidence = int(candidate.get("confidence") or 0)
    except (TypeError, ValueError):
        confidence = 0
    if not symbol or direction not in {"CALL", "PUT"}:
        return None
    return f"{symbol} {direction} confianca {confidence}"


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
    model_config = ConfigDict(extra="ignore")

    enabled: bool | None = None
    account_mode: AccountMode | None = Field(default=None, validation_alias=AliasChoices("account_mode", "accountMode"))
    timeframe: Timeframe | None = None
    strategy_mode: StrategyMode | None = Field(default=None, validation_alias=AliasChoices("strategy_mode", "strategyMode"))
    entry_value: float | None = Field(default=None, gt=0, validation_alias=AliasChoices("entry_value", "entryValue"))
    cycle_minutes: int | None = Field(default=None, ge=1, validation_alias=AliasChoices("cycle_minutes", "cycleMinutes"))
    min_confidence: int | None = Field(default=None, ge=0, le=100, validation_alias=AliasChoices("min_confidence", "minConfidence"))
    min_payout: float | None = Field(default=None, ge=0, le=100, validation_alias=AliasChoices("min_payout", "minPayout"))
    stop_win: float | None = Field(default=None, ge=0, validation_alias=AliasChoices("stop_win", "stopWin"))
    stop_loss: float | None = Field(default=None, ge=0, validation_alias=AliasChoices("stop_loss", "stopLoss"))
    max_entries_per_cycle: int | None = Field(default=None, ge=1, le=1, validation_alias=AliasChoices("max_entries_per_cycle", "maxEntriesPerCycle"))
    allow_real: bool | None = Field(default=None, validation_alias=AliasChoices("allow_real", "allowReal"))
    confirm_real: bool | None = Field(default=None, validation_alias=AliasChoices("confirm_real", "confirmReal"))
    martingale_enabled: bool | None = Field(default=None, validation_alias=AliasChoices("martingale_enabled", "martingaleEnabled"))
    martingale_steps: int | None = Field(default=None, ge=1, le=1, validation_alias=AliasChoices("martingale_steps", "martingaleSteps"))
    martingale_multiplier: float | None = Field(default=None, gt=0, validation_alias=AliasChoices("martingale_multiplier", "martingaleMultiplier"))


@dataclass
class RobotState:
    enabled: bool = False
    account_mode: AccountMode = "DEMO"
    timeframe: Timeframe = "M1"
    strategy_mode: StrategyMode = "conservative"
    entry_value: float = 2.0
    cycle_minutes: int = 5
    min_confidence: int = 80
    min_payout: float = 80.0
    stop_win: float = 50.0
    stop_loss: float = 30.0
    max_entries_per_cycle: int = 1
    allow_real: bool = False
    confirm_real: bool = False
    martingale_enabled: bool = False
    martingale_steps: int = 1
    martingale_multiplier: float = 2.0
    wins: int = 0
    losses: int = 0
    profit: float = 0.0
    cycle_id: str | None = None
    current_cycle_started_at: datetime | None = None
    next_cycle_at: datetime | None = None
    last_entry_at: datetime | None = None
    last_analysis_at: datetime | None = None
    last_analysis_result: str | None = None
    analysis_started_at: datetime | None = None
    analysis_result: str | None = None
    analysis_message: str | None = None
    rejected_at: datetime | None = None
    result_received_at: datetime | None = None
    result_display_until: datetime | None = None
    stop_reset_at: datetime | None = None
    operation_in_progress: bool = False
    last_signal: dict[str, Any] | None = None
    pending_signal: dict[str, Any] | None = None
    last_trade: dict[str, Any] | None = None
    status: str = STATUS_STOPPED
    rejection_reason: str | None = None
    server_time: str | None = None
    server_time_source: str = "vps_fallback"
    connected: bool = False
    active_mode: str | None = None
    connection_checked_at: datetime | None = None
    last_connected_at: datetime | None = None
    connection_grace_until: datetime | None = None
    connection_status_source: str = "cached"
    connection_failure_count: int = 0
    analysis_window_open: bool = False
    seconds_until_analysis_window: int = 0
    analysis_window_start_second: int = 5
    analysis_window_end_second: int = 20
    entry_window_open: bool = False
    seconds_until_entry_window: int = 0
    current_candle_seconds: float = 0.0
    entry_window_start_second: int = 0
    entry_window_end_second: int = 3
    buy_target_second: int = 0
    expiration_seconds: int = 60
    last_rejection_reason: str | None = None
    last_order_error: str | None = None
    gale_pending: bool = False
    gale_step: int = 0
    gale_amount: float = 0.0
    gale_active: bool = False
    gale_direction: str | None = None
    gale_original_order_id: str | None = None
    gale_parent_trade: dict[str, Any] | None = None
    cycle_result: str | None = None
    order_attempts: int = 0
    fallback_candidate_used: bool = False
    blocked_filters: list[str] = field(default_factory=list)
    approved_filters: list[str] = field(default_factory=list)
    quality_score: int = 0
    strategy_score: int = 0
    candidates_count: int = 0
    candidates: list[dict[str, Any]] = field(default_factory=list)
    best_candidate: dict[str, Any] | None = None
    cycle_best_candidate: dict[str, Any] | None = None
    cycle_best_trade_candidate: dict[str, Any] | None = None
    strategy_name: str | None = None
    strategy_reason: str | None = None
    used_strategies: list[str] = field(default_factory=list)
    candle_reading: str | None = None
    entry_reason: str | None = None
    block_reasons: list[str] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)
    sync_started_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        data = strip_ai_fields(asdict(self))
        for key in (
            "current_cycle_started_at",
            "next_cycle_at",
            "last_entry_at",
            "last_analysis_at",
            "analysis_started_at",
            "rejected_at",
            "result_received_at",
            "result_display_until",
            "stop_reset_at",
            "connection_checked_at",
            "last_connected_at",
            "connection_grace_until",
            "sync_started_at",
        ):
            value = data[key]
            data[key] = value.isoformat() if value is not None else None
        now = utc_now()
        if (
            self.status in TEMPORARY_WAIT_STATUSES
            and self.next_cycle_at is not None
            and now >= self.next_cycle_at
            and self.enabled
            and not self.operation_in_progress
        ):
            self.status = STATUS_WAITING_NEXT_CYCLE
            self.rejection_reason = None
            data["status"] = self.status
            data["rejection_reason"] = None
        if self.status == STATUS_WAITING_ANALYSIS_WINDOW:
            self.status = STATUS_WAITING_NEXT_CYCLE if self.enabled else STATUS_STOPPED
            self.rejection_reason = None
            data["status"] = self.status
            data["rejection_reason"] = None
        if self.status == STATUS_SIGNAL_EXPIRED and self.next_cycle_at is not None and now >= self.next_cycle_at:
            self.status = STATUS_WAITING_NEXT_CYCLE if self.enabled else STATUS_STOPPED
            self.rejection_reason = None
            data["status"] = self.status
            data["rejection_reason"] = None
        if data["status"] == STATUS_WAITING_NEXT_CYCLE and data.get("analysis_result") == "RUNNING":
            data["analysis_result"] = self.analysis_result = None
            data["last_analysis_result"] = self.last_analysis_result = None
            data["analysis_message"] = self.analysis_message = None
        if (
            self.status in {STATUS_RESULT_RECEIVED, STATUS_GALE_RESULT_RECEIVED}
            and self.result_display_until is not None
            and now >= self.result_display_until
        ):
            if self.status == STATUS_GALE_RESULT_RECEIVED:
                self.gale_pending = False
                self.gale_step = 0
                self.gale_amount = 0.0
                self.gale_active = False
                self.gale_direction = None
                self.gale_original_order_id = None
                self.gale_parent_trade = None
                data["gale_pending"] = False
                data["gale_step"] = 0
                data["gale_amount"] = 0.0
                data["gale_active"] = False
                data["gale_direction"] = None
                data["gale_original_order_id"] = None
                data["gale_parent_trade"] = None
            self.status = STATUS_WAITING_NEXT_CYCLE if self.enabled else STATUS_STOPPED
            self.next_cycle_at = now + timedelta(minutes=self.cycle_minutes) if self.enabled else None
            self.rejection_reason = None
            self.pending_signal = None
            self.best_candidate = None
            self.candidates = []
            self.candidates_count = 0
            self.strategy_score = 0
            self.strategy_name = None
            self.strategy_reason = None
            self.used_strategies = []
            self.candle_reading = None
            self.entry_reason = None
            self.block_reasons = []
            self.metrics = {}
            data["status"] = self.status
            data["next_cycle_at"] = self.next_cycle_at.isoformat() if self.next_cycle_at is not None else None
            data["pending_signal"] = None
            data["candidates"] = []
            data["candidates_count"] = 0
            data["strategy_score"] = 0
            data["strategy_name"] = None
            data["strategy_reason"] = None
        if self.status in {STATUS_SIGNAL_REJECTED, STATUS_ORDER_REJECTED} and self.rejected_at is not None:
            if (now - self.rejected_at).total_seconds() >= 5:
                data["status"] = STATUS_WAITING_NEXT_CYCLE if self.enabled else STATUS_STOPPED
                data["rejection_reason"] = None
        data["seconds_until_next_cycle"] = (
            max(0, int((self.next_cycle_at - now).total_seconds()))
            if self.next_cycle_at is not None
            else 0
        )
        configured_expiration = TIMEFRAME_SECONDS[self.timeframe]
        data["expiration_seconds"] = configured_expiration
        data["result_waiting"] = bool(
            self.operation_in_progress
            and str((self.last_trade or {}).get("result") or "").upper() not in {"WIN", "LOSS", "TIMEOUT"}
        )
        data["operation_message"] = None
        data["expiration_display"] = None
        data["show_expiration_countdown"] = False
        if self.status == STATUS_ACCOUNT_DISCONNECTED:
            data["enabled"] = False
            data["connected"] = False
            data["active_mode"] = None
            data["operation_in_progress"] = False
            data["result_waiting"] = False
            data["operation_message"] = DISCONNECTED_MESSAGE
            data["analysis_message"] = None
            data["status_message"] = None
            data["display_countdown_label"] = None
            data["display_countdown_seconds"] = 0
            data["best_candidate_summary"] = None
            data["pending_signal"] = None
            data["last_signal"] = None
            data["last_trade"] = None
        elif self.status == STATUS_STOP_WIN_HIT:
            data["enabled"] = False
            data["operation_in_progress"] = False
            data["result_waiting"] = False
            data["operation_message"] = STOP_WIN_MESSAGE
        elif self.status == STATUS_STOP_LOSS_HIT:
            data["enabled"] = False
            data["operation_in_progress"] = False
            data["result_waiting"] = False
            data["operation_message"] = STOP_LOSS_MESSAGE
        elif self.status == STATUS_SIGNAL_EXPIRED:
            data["operation_message"] = SIGNAL_EXPIRED_MESSAGE
        elif self.status == STATUS_SIGNAL_REJECTED and self.last_order_error == "PAYOUT_TOO_LOW":
            data["operation_message"] = "Entrada bloqueada: payout abaixo do minimo."
        if self.status == STATUS_ANALYZING:
            data["last_analysis_result"] = "RUNNING"
            data["analysis_result"] = "RUNNING"
            data["analysis_message"] = ANALYSIS_MESSAGE
        display_countdown_label = None
        display_countdown_seconds = 0
        if data["status"] == STATUS_WAITING_NEXT_CYCLE:
            display_countdown_label = "Entrada em"
            display_countdown_seconds = max(0, int(data["seconds_until_next_cycle"]))
        elif data["status"] in {STATUS_WAITING_NEXT_CANDLE_ENTRY, STATUS_WAITING_GALE_ENTRY}:
            display_countdown_label = "Entrada no inicio da proxima vela em"
            display_countdown_seconds = max(0, int(data["seconds_until_entry_window"]))
        elif data["status"] in TEMPORARY_WAIT_STATUSES:
            display_countdown_label = "Recuperando em"
            display_countdown_seconds = max(0, int(data["seconds_until_next_cycle"]))
        elif data["status"] == STATUS_SIGNAL_EXPIRED:
            display_countdown_label = "Novo sinal em"
            display_countdown_seconds = max(0, int(data["seconds_until_next_cycle"]))
        data["display_countdown_label"] = display_countdown_label
        data["display_countdown_seconds"] = display_countdown_seconds
        data["status_message"] = None
        data["entry_target"] = "NEXT_CANDLE_OPEN"
        data["seconds_until_entry"] = max(0, int(data["seconds_until_entry_window"]))
        data["best_candidate_summary"] = format_best_candidate_summary(
            strip_ai_fields(self.cycle_best_candidate or self.best_candidate)
        )
        data["voice_message"] = None
        data["voice_event_id"] = None
        voice_signal = self.pending_signal or self.best_candidate
        if self.status in {STATUS_WAITING_NEXT_CANDLE_ENTRY, STATUS_WAITING_GALE_ENTRY} and voice_signal:
            data["status_message"] = "Sinal preparado"
            data["voice_message"] = "Entrada preparada. Vamos entrar no inicio da proxima vela."
            symbol = str(voice_signal.get("symbol") or "")
            direction = str(voice_signal.get("direction") or voice_signal.get("signal") or "")
            score = int(voice_signal.get("strategy_score") or voice_signal.get("score") or 0)
            data["voice_event_id"] = f"{self.cycle_id or ''}:{symbol}:{direction}:{score}:prepared"
        if self.status in {STATUS_SENDING_ORDER, STATUS_SENDING_GALE_ORDER} and voice_signal:
            symbol = str(voice_signal.get("symbol") or "")
            direction = str(voice_signal.get("direction") or voice_signal.get("signal") or "")
            score = int(voice_signal.get("strategy_score") or voice_signal.get("score") or 0)
            reason = str(voice_signal.get("strategy_reason") or voice_signal.get("reason") or "").strip()
            reason_text = f" Motivo: {reason}." if reason else ""
            data["voice_message"] = (
                f"Entrada liberada agora. Ativo {symbol}. Direção {direction}. Score {score}."
                f"{reason_text}"
            )
            data["voice_event_id"] = f"{self.cycle_id or ''}:{symbol}:{direction}:{score}:sending"
        if self.status in {STATUS_RESULT_RECEIVED, STATUS_GALE_RESULT_RECEIVED} and self.last_trade is not None:
            final_result = str(self.last_trade.get("final_result") or self.last_trade.get("result") or "").upper()
            gale_step = int(self.last_trade.get("gale_step") or 0)
            if final_result == "WIN":
                data["operation_message"] = "WIN"
                data["voice_message"] = "WIN no Gale 1" if gale_step == 1 else "WIN"
            elif final_result == "LOSS":
                data["operation_message"] = "LOSS no Gale 1" if gale_step == 1 else "LOSS"
                data["voice_message"] = "LOSS no Gale 1" if gale_step == 1 else "LOSS"
        if self.status == STATUS_WAITING_NEXT_CYCLE and not self.operation_in_progress:
            data["strategy_reason"] = "Analisando mercado em silêncio. A entrada será revelada quando o contador zerar."
            data["used_strategies"] = []
            data["candle_reading"] = None
            data["entry_reason"] = None
            data["block_reasons"] = []
            data["metrics"] = {}
            data["last_signal"] = None
            data["pending_signal"] = None
            data["analysis_message"] = "Analisando mercado em silêncio..."
            if data.get("analysis_result") == "NO_TRADE":
                data["analysis_message"] = "Sem entrada neste ciclo"
                data["status_message"] = "Sem entrada neste ciclo"
            elif data.get("best_candidate_summary"):
                data["analysis_message"] = ANALYSIS_MESSAGE
                data["status_message"] = f"Melhor candidato: {data['best_candidate_summary']}"
        for deprecated_key in (
            "analysis_window_open",
            "seconds_until_analysis_window",
            "analysis_window_start_second",
            "analysis_window_end_second",
        ):
            data.pop(deprecated_key, None)
        if self.last_trade is not None:
            trade = strip_ai_fields(dict(self.last_trade))
            trade["result"] = trade.get("result") or STATUS_PENDING_RESULT
            data["last_trade"] = trade
            if self.operation_in_progress:
                expires_at = parse_datetime(trade.get("expected_expire_at"))
                if expires_at is None:
                    expires_at = parse_datetime(trade.get("expires_at"))
                if expires_at is None:
                    sent_at = parse_datetime(trade.get("sent_at") or trade.get("timestamp"))
                    if sent_at is not None:
                        expires_at = sent_at + timedelta(seconds=configured_expiration)
                if expires_at is not None:
                    expiration_seconds = max(
                        0,
                        math.ceil((expires_at - now).total_seconds()),
                    )
                    data["expiration_seconds"] = expiration_seconds
                    result = str(trade.get("result") or "").strip().upper()
                    data["result_waiting"] = result not in {"WIN", "LOSS", "TIMEOUT"}
                    if data["result_waiting"]:
                        data["status"] = STATUS_PENDING_RESULT
                result = str(trade.get("result") or "").strip().upper()
                if result not in {"WIN", "LOSS"}:
                    if int(data["expiration_seconds"]) <= 0:
                        data["status"] = STATUS_PENDING_RESULT
                        data["result_waiting"] = True
                        data["operation_message"] = RESULT_WAITING_MESSAGE
                        data["expiration_display"] = RESULT_WAITING_MESSAGE
                        data["show_expiration_countdown"] = False
                    else:
                        countdown = format_mm_ss(int(data["expiration_seconds"]))
                        data["operation_message"] = f"Expira em {countdown}"
                        data["expiration_display"] = countdown
                        data["show_expiration_countdown"] = True
        total = self.wins + self.losses
        data["accuracy"] = round((self.wins / total) * 100, 2) if total else 0.0
        return data


@dataclass
class AutoTrader:
    _states: dict[str, RobotState] = field(default_factory=dict)
    _locks: dict[str, asyncio.Lock] = field(default_factory=dict)
    _histories: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    _completed_order_ids: dict[str, set[str]] = field(default_factory=dict)
    _sources: dict[str, StateSource] = field(default_factory=dict)

    def get(self, user_id: str) -> RobotState:
        state = self._states.get(user_id)
        if state is None:
            state = RobotState()
            self._states[user_id] = state
            self._sources[user_id] = "default"
        return state

    def has_state(self, user_id: str) -> bool:
        return user_id in self._states

    def source(self, user_id: str) -> StateSource:
        return self._sources.get(user_id, "default")

    def mark_source(self, user_id: str, source: StateSource) -> None:
        self._sources[user_id] = source

    @staticmethod
    def _next_cycle_base(state: RobotState) -> datetime | None:
        if state.last_trade:
            finished_at = parse_datetime(state.last_trade.get("finished_at"))
            if finished_at is not None:
                return finished_at
        return state.last_entry_at

    def _schedule_next_cycle(self, state: RobotState, base: datetime | None = None) -> None:
        cycle_base = base or self._next_cycle_base(state) or utc_now()
        state.next_cycle_at = cycle_base + timedelta(minutes=state.cycle_minutes)

    @staticmethod
    def _new_cycle_id() -> str:
        return uuid.uuid4().hex

    def restore(
        self,
        user_id: str,
        payload: dict[str, Any],
        trades: list[dict[str, Any]] | None = None,
        *,
        source: StateSource = "memory",
    ) -> RobotState:
        state = RobotState()
        payload = strip_ai_fields(payload)
        datetime_fields = {
            "current_cycle_started_at",
            "next_cycle_at",
            "last_entry_at",
            "last_analysis_at",
            "analysis_started_at",
            "rejected_at",
            "result_received_at",
            "result_display_until",
            "stop_reset_at",
            "connection_checked_at",
            "last_connected_at",
            "connection_grace_until",
            "sync_started_at",
        }
        for key, value in payload.items():
            if not hasattr(state, key) or key in {"accuracy", "seconds_until_next_cycle"}:
                continue
            if key in datetime_fields and isinstance(value, str):
                value = datetime.fromisoformat(value.replace("Z", "+00:00"))
            setattr(state, key, value)
        if state.status == "WAITING_ENTRY_WINDOW":
            state.status = STATUS_WAITING_NEXT_CANDLE_ENTRY
        if state.status == STATUS_PENDING_RESULT and state.gale_active:
            state.status = STATUS_PENDING_GALE_RESULT
        if state.status in {STATUS_SYNCING, STATUS_SYNCING_PT} and state.sync_started_at is None:
            state.sync_started_at = state.analysis_started_at or state.last_analysis_at or state.current_cycle_started_at
        if state.status == STATUS_RESULT_RECEIVED and bool((state.last_trade or {}).get("is_gale")):
            state.status = STATUS_GALE_RESULT_RECEIVED

        result_visible = (
            state.status in {STATUS_RESULT_RECEIVED, STATUS_GALE_RESULT_RECEIVED}
            and state.result_display_until is not None
            and utc_now() < state.result_display_until
        )
        if result_visible:
            state.operation_in_progress = False
        elif state.enabled and state.pending_signal:
            state.status = STATUS_WAITING_GALE_ENTRY if state.gale_pending else STATUS_WAITING_NEXT_CANDLE_ENTRY
            state.rejection_reason = None
        elif state.enabled and not state.operation_in_progress:
            state.status = STATUS_WAITING_NEXT_CYCLE
            state.rejection_reason = None
            if state.next_cycle_at is None:
                self._schedule_next_cycle(state)
            state.entry_window_open = False
        self._states[user_id] = state
        self._sources[user_id] = source

        restored_trades = [strip_ai_fields(dict(trade)) for trade in (trades or [])]
        self._histories[user_id] = [
            trade for trade in restored_trades if trade.get("result") in {"WIN", "LOSS", "TIMEOUT"}
        ][-100:]
        self._completed_order_ids[user_id] = {
            str(trade.get("order_id"))
            for trade in self._histories[user_id]
            if trade.get("order_id") is not None
        }
        return state

    def recover_sync_timeout(self, user_id: str) -> tuple[bool, RobotState]:
        state = self.get(user_id)
        if str(state.status).upper() not in {STATUS_SYNCING, STATUS_SYNCING_PT}:
            state.sync_started_at = None
            return False, state
        now = utc_now()
        if state.sync_started_at is None:
            state.sync_started_at = now
            return False, state
        if (now - state.sync_started_at).total_seconds() <= SYNC_TIMEOUT_SECONDS:
            return False, state
        state.sync_started_at = None
        state.analysis_started_at = None
        state.analysis_result = None
        state.last_analysis_result = None
        state.analysis_message = None
        state.rejection_reason = None
        if state.connected and state.enabled:
            state.status = STATUS_ANALYZING
            state.analysis_started_at = now
            state.last_analysis_at = now
            state.analysis_result = "RUNNING"
            state.last_analysis_result = "RUNNING"
            state.analysis_message = ANALYSIS_MESSAGE
        elif not state.connected:
            state.enabled = False
            state.status = STATUS_ACCOUNT_DISCONNECTED
            state.rejection_reason = STATUS_ACCOUNT_DISCONNECTED
            state.last_rejection_reason = STATUS_ACCOUNT_DISCONNECTED
            state.pending_signal = None
            state.last_signal = None
            state.operation_in_progress = False
            state.entry_window_open = False
            state.seconds_until_entry_window = 0
            state.next_cycle_at = None
        else:
            state.status = STATUS_STOPPED
        return True, state

    def lock(self, user_id: str) -> asyncio.Lock:
        return self._locks.setdefault(user_id, asyncio.Lock())

    def update_config(self, user_id: str, update: RobotConfigUpdate) -> RobotState:
        state = self.get(user_id)
        changes = update.model_dump(exclude_none=True)
        if "martingale_steps" in changes:
            changes["martingale_steps"] = 1
        for key, value in changes.items():
            setattr(state, key, value)

        if "cycle_minutes" in changes and state.next_cycle_at is not None:
            base = self._next_cycle_base(state) or state.current_cycle_started_at or utc_now()
            self._schedule_next_cycle(state, base)

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
        if changes:
            self._sources[user_id] = "memory"
        return state

    def start(self, user_id: str) -> RobotState:
        state = self.get(user_id)
        now = utc_now()
        state.enabled = True
        state.status = STATUS_WAITING_NEXT_CYCLE
        state.rejection_reason = None
        state.last_rejection_reason = None
        state.last_order_error = None
        state.result_received_at = None
        state.result_display_until = None
        state.order_attempts = 0
        state.fallback_candidate_used = False
        state.rejected_at = None
        state.pending_signal = None
        state.last_signal = None
        state.last_analysis_at = None
        state.last_analysis_result = None
        state.analysis_started_at = None
        state.analysis_result = None
        state.operation_in_progress = False
        state.gale_pending = False
        state.gale_step = 0
        state.gale_amount = 0.0
        state.gale_active = False
        state.gale_direction = None
        state.gale_original_order_id = None
        state.gale_parent_trade = None
        state.cycle_result = None
        state.entry_window_open = False
        state.blocked_filters = []
        state.approved_filters = []
        state.quality_score = 0
        state.strategy_score = 0
        state.candidates_count = 0
        state.candidates = []
        state.best_candidate = None
        state.cycle_best_candidate = None
        state.cycle_best_trade_candidate = None
        state.strategy_name = None
        state.strategy_reason = None
        state.used_strategies = []
        state.candle_reading = None
        state.entry_reason = None
        state.block_reasons = []
        state.metrics = {}
        state.analysis_message = None
        state.cycle_id = self._new_cycle_id()
        state.current_cycle_started_at = now
        state.next_cycle_at = now + timedelta(minutes=state.cycle_minutes)
        return state

    def stop(self, user_id: str) -> RobotState:
        state = self.get(user_id)
        trade_result = str((state.last_trade or {}).get("result") or "").strip().upper()
        pending_trade = state.operation_in_progress and trade_result not in {"WIN", "LOSS", "TIMEOUT"}
        state.enabled = False
        state.status = STATUS_STOPPED
        state.rejection_reason = None
        state.last_rejection_reason = None
        state.last_order_error = None
        state.analysis_started_at = None
        state.analysis_result = None
        state.last_analysis_result = None
        state.analysis_message = None
        state.pending_signal = None
        state.last_signal = None
        state.next_cycle_at = None
        state.operation_in_progress = pending_trade
        state.entry_window_open = False
        state.seconds_until_entry_window = 0
        state.analysis_window_open = False
        state.seconds_until_analysis_window = 0
        if not pending_trade:
            state.result_received_at = None
            state.result_display_until = None
        state.order_attempts = 0
        state.fallback_candidate_used = False
        if not pending_trade:
            self._clear_gale_state(state)
        else:
            state.gale_pending = False
        return state

    def prepare_cycle(self, user_id: str) -> tuple[bool, RobotState]:
        state = self.get(user_id)
        now = utc_now()
        if not state.enabled:
            if state.status not in {STATUS_STOP_WIN_HIT, STATUS_STOP_LOSS_HIT}:
                state.status = STATUS_STOPPED
            return False, state
        if state.status in TEMPORARY_WAIT_STATUSES:
            if state.next_cycle_at is not None and now < state.next_cycle_at:
                return False, state
            state.status = STATUS_WAITING_NEXT_CYCLE
            state.rejection_reason = None
        if state.status in {STATUS_RESULT_RECEIVED, STATUS_GALE_RESULT_RECEIVED} and state.result_display_until is not None:
            if now < state.result_display_until:
                return False, state
            if state.status == STATUS_GALE_RESULT_RECEIVED:
                self._clear_gale_state(state)
            state.status = STATUS_WAITING_NEXT_CYCLE
            state.next_cycle_at = now + timedelta(minutes=state.cycle_minutes)
            state.pending_signal = None
            state.best_candidate = None
            state.cycle_best_candidate = None
            state.cycle_best_trade_candidate = None
            state.candidates = []
            state.candidates_count = 0
            state.strategy_score = 0
            state.strategy_name = None
            state.strategy_reason = None
            state.used_strategies = []
            state.candle_reading = None
            state.entry_reason = None
            state.block_reasons = []
            state.metrics = {}
        last_trade_result = str((state.last_trade or {}).get("result") or "").upper()
        result_waiting = (
            state.status in {STATUS_PENDING_RESULT, STATUS_PENDING_GALE_RESULT}
            and last_trade_result not in {"WIN", "LOSS", "TIMEOUT"}
        )
        if state.operation_in_progress or result_waiting:
            state.operation_in_progress = True
            state.status = STATUS_PENDING_GALE_RESULT if state.gale_active else STATUS_PENDING_RESULT
            return False, state
        if state.pending_signal:
            state.status = STATUS_WAITING_GALE_ENTRY if state.gale_pending else STATUS_WAITING_NEXT_CANDLE_ENTRY
            return True, state
        if state.next_cycle_at is not None and now < state.next_cycle_at:
            state.status = STATUS_WAITING_NEXT_CYCLE
            return False, state

        if state.next_cycle_at is None:
            state.current_cycle_started_at = now
            state.cycle_id = self._new_cycle_id()
            state.next_cycle_at = now + timedelta(minutes=state.cycle_minutes)
            state.status = STATUS_WAITING_NEXT_CYCLE
            return False, state
        state.status = STATUS_WAITING_NEXT_CYCLE
        state.rejection_reason = None
        state.analysis_message = None
        state.order_attempts = 0
        state.fallback_candidate_used = False
        return True, state

    def start_analysis(self, user_id: str) -> RobotState:
        state = self.get(user_id)
        if (
            not state.enabled
            or not state.connected
            or not state.analysis_window_open
            or state.operation_in_progress
            or state.pending_signal is not None
        ):
            return state
        now = utc_now()
        state.status = STATUS_ANALYZING
        state.rejection_reason = None
        state.last_analysis_at = now
        state.last_analysis_result = "RUNNING"
        state.analysis_started_at = now
        state.analysis_result = "RUNNING"
        state.analysis_message = ANALYSIS_MESSAGE
        return state

    def reject_analysis(
        self,
        user_id: str,
        reason: str,
        *,
        last_rejection_reason: str,
        last_order_error: str | None = None,
    ) -> RobotState:
        state = self.get(user_id)
        rejected_at = utc_now()
        state.status = STATUS_SIGNAL_REJECTED
        state.rejection_reason = reason
        state.last_rejection_reason = last_rejection_reason
        state.last_order_error = last_order_error
        state.rejected_at = rejected_at
        state.pending_signal = None
        state.last_signal = None
        state.operation_in_progress = False
        state.entry_window_open = False
        state.seconds_until_entry_window = 0
        state.next_cycle_at = rejected_at + timedelta(minutes=state.cycle_minutes)
        state.last_analysis_at = rejected_at
        state.last_analysis_result = reason
        state.analysis_result = reason
        state.analysis_message = None
        return state

    def reject_analysis_timeout(self, user_id: str) -> RobotState:
        return self.reject_analysis(
            user_id,
            STATUS_ANALYSIS_TIMEOUT,
            last_rejection_reason=ANALYSIS_TIMEOUT_MESSAGE,
        )

    def reject_analysis_error(self, user_id: str, error: str) -> RobotState:
        return self.reject_analysis(
            user_id,
            STATUS_ANALYSIS_ERROR,
            last_rejection_reason=error,
            last_order_error=error,
        )

    def reject_no_candidates(
        self,
        user_id: str,
        *,
        last_rejection_reason: str,
        blocked_filters: list[str] | None = None,
        approved_filters: list[str] | None = None,
        quality_score: int = 0,
    ) -> RobotState:
        state = self.reject_analysis(
            user_id,
            STATUS_NO_CANDIDATES,
            last_rejection_reason=last_rejection_reason,
        )
        state.blocked_filters = list(blocked_filters or [])
        state.approved_filters = list(approved_filters or [])
        state.quality_score = int(quality_score or 0)
        state.strategy_score = 0
        state.candidates_count = 0
        state.candidates = []
        state.best_candidate = None
        state.cycle_best_candidate = None
        state.cycle_best_trade_candidate = None
        state.strategy_name = None
        state.strategy_reason = None
        state.used_strategies = []
        state.candle_reading = None
        state.entry_reason = None
        state.block_reasons = []
        state.metrics = {}
        return state

    def recover_timed_out_analysis(self, user_id: str) -> tuple[bool, RobotState]:
        state = self.get(user_id)
        if state.analysis_result != "RUNNING" and state.last_analysis_result != "RUNNING":
            return False, state
        started_at = state.analysis_started_at or state.last_analysis_at or state.current_cycle_started_at
        if started_at is None:
            return False, state
        if (utc_now() - started_at).total_seconds() <= ANALYSIS_TIMEOUT_SECONDS:
            return False, state
        return True, self.reject_analysis_timeout(user_id)

    def reject(self, user_id: str, reason: str) -> RobotState:
        state = self.get(user_id)
        state.status = STATUS_SIGNAL_REJECTED
        state.rejection_reason = reason
        state.last_rejection_reason = reason
        state.rejected_at = utc_now()
        return state

    def reject_strategy(
        self,
        user_id: str,
        reason: str,
        *,
        last_rejection_reason: str | None = None,
        blocked_filters: list[str] | None = None,
        approved_filters: list[str] | None = None,
        quality_score: int = 0,
    ) -> RobotState:
        state = self.reject(user_id, reason)
        if last_rejection_reason:
            state.last_rejection_reason = last_rejection_reason
        state.blocked_filters = list(blocked_filters or [])
        state.approved_filters = list(approved_filters or [])
        state.quality_score = int(quality_score or 0)
        state.strategy_score = 0
        state.candidates_count = 0
        state.candidates = []
        state.best_candidate = None
        state.cycle_best_candidate = None
        state.cycle_best_trade_candidate = None
        state.strategy_name = None
        state.strategy_reason = None
        state.used_strategies = []
        state.candle_reading = None
        state.entry_reason = None
        state.block_reasons = []
        state.metrics = {}
        state.analysis_message = None
        state.pending_signal = None
        state.last_signal = None
        state.operation_in_progress = False
        state.entry_window_open = False
        state.next_cycle_at = state.rejected_at + timedelta(minutes=state.cycle_minutes)
        state.last_analysis_at = state.rejected_at
        state.last_analysis_result = reason
        state.analysis_result = reason
        return state

    def reject_no_valid_signal(
        self,
        user_id: str,
        last_rejection_reason: str,
        *,
        blocked_filters: list[str] | None = None,
        approved_filters: list[str] | None = None,
        quality_score: int = 0,
    ) -> RobotState:
        return self.reject_strategy(
            user_id,
            "NO_VALID_SIGNAL",
            last_rejection_reason=last_rejection_reason,
            blocked_filters=blocked_filters,
            approved_filters=approved_filters,
            quality_score=quality_score,
        )

    def fail(self, user_id: str, reason: str) -> RobotState:
        state = self.get(user_id)
        state.status = STATUS_ERROR
        state.rejection_reason = reason
        state.last_rejection_reason = reason
        state.last_analysis_result = reason
        state.analysis_result = reason
        return state

    def set_pending_signal(self, user_id: str, signal: dict[str, Any]) -> RobotState:
        state = self.get(user_id)
        signal = strip_ai_fields(signal)
        pending_signal = {
            "symbol": signal["symbol"],
            "direction": signal.get("direction") or signal["signal"],
            "signal": signal.get("signal") or signal["direction"],
            "confidence": signal["confidence"],
            "payout": signal["payout"],
            "strategy_score": int(signal.get("strategy_score") or 0),
            "score": int(signal.get("strategy_score") or signal.get("score") or 0),
            "reason": signal.get("reason") or signal.get("signal_explanation"),
            "entry_reason": signal.get("entry_reason")
            or signal.get("reason")
            or signal.get("signal_explanation"),
            "candle_reading": signal.get("candle_reading"),
            "block_reasons": list(signal.get("block_reasons") or signal.get("blocked_filters") or []),
            "metrics": dict(signal.get("metrics") or {}),
            "strategy_name": signal.get("strategy_name"),
            "strategy_reason": signal.get("strategy_reason")
            or signal.get("reason")
            or signal.get("signal_explanation"),
            "used_strategies": list(signal.get("used_strategies") or []),
            "timeframe": state.timeframe,
            "quality_score": signal.get("quality_score", 0),
            "blocked_filters": list(signal.get("blocked_filters") or []),
            "approved_filters": list(signal.get("approved_filters") or []),
            "strategy_mode": signal.get("strategy_mode", state.strategy_mode),
            "cycle_id": state.cycle_id,
            "created_at": utc_now().isoformat(),
            "target_entry_second": state.buy_target_second,
            "entry_window_start_second": state.entry_window_start_second,
            "entry_window_end_second": state.entry_window_end_second,
        }
        state.last_signal = dict(pending_signal)
        state.pending_signal = pending_signal
        state.status = STATUS_WAITING_GALE_ENTRY if state.gale_pending else STATUS_WAITING_NEXT_CANDLE_ENTRY
        state.rejection_reason = None
        state.last_rejection_reason = None
        state.last_analysis_at = utc_now()
        state.last_analysis_result = "BEST_CANDIDATE_SELECTED"
        state.analysis_result = "BEST_CANDIDATE_SELECTED"
        state.analysis_message = None
        state.blocked_filters = list(pending_signal["blocked_filters"])
        state.approved_filters = list(pending_signal["approved_filters"])
        state.quality_score = int(pending_signal["quality_score"] or 0)
        state.strategy_score = int(pending_signal["strategy_score"] or 0)
        state.best_candidate = dict(pending_signal)
        state.cycle_best_candidate = dict(pending_signal)
        state.cycle_best_trade_candidate = dict(pending_signal)
        state.strategy_name = pending_signal["strategy_name"]
        state.strategy_reason = pending_signal["strategy_reason"]
        state.used_strategies = list(pending_signal["used_strategies"])
        state.candle_reading = pending_signal["candle_reading"]
        state.entry_reason = pending_signal["entry_reason"]
        state.block_reasons = list(pending_signal["block_reasons"])
        state.metrics = dict(pending_signal["metrics"])
        return state

    def set_order_attempt(self, user_id: str, candidate: dict[str, Any], attempt: int) -> RobotState:
        state = self.get(user_id)
        state.order_attempts = attempt
        state.fallback_candidate_used = attempt > 1
        state.last_order_error = None
        return self.set_pending_signal(user_id, candidate)

    def wait_analysis_window(
        self,
        user_id: str,
        window: dict[str, Any],
        *,
        clear_pending: bool = False,
        analysis_result: str = "WAITING_NEXT_ANALYSIS_WINDOW",
        rejection_reason: str = "WAITING_NEXT_ANALYSIS_WINDOW",
        last_rejection_reason: str | None = None,
        force_next: bool = False,
    ) -> RobotState:
        state = self.get(user_id)
        if clear_pending:
            state.pending_signal = None
            state.last_signal = None
            state.best_candidate = None
            state.strategy_score = 0
            state.candidates_count = 0
            state.candidates = []
        state.status = STATUS_WAITING_ANALYSIS_WINDOW
        state.rejection_reason = rejection_reason
        state.last_rejection_reason = last_rejection_reason or "WAITING_NEXT_ANALYSIS_WINDOW"
        state.analysis_result = analysis_result
        state.last_analysis_result = analysis_result
        state.analysis_message = None
        state.operation_in_progress = False
        state.analysis_window_open = bool(window["analysis_window_open"]) and not force_next
        if force_next:
            seconds_until_analysis_window = math.ceil(
                float(window["expiration_seconds"])
                - float(window["current_candle_seconds"])
                + float(window["analysis_window_start_second"])
            )
        else:
            seconds_until_analysis_window = int(window["seconds_until_analysis_window"])
        state.seconds_until_analysis_window = max(1, int(seconds_until_analysis_window))
        state.analysis_window_start_second = int(window["analysis_window_start_second"])
        state.analysis_window_end_second = int(window["analysis_window_end_second"])
        state.current_candle_seconds = float(window["current_candle_seconds"])
        state.expiration_seconds = int(window["expiration_seconds"])
        state.analysis_started_at = None
        state.next_cycle_at = utc_now() + timedelta(seconds=state.seconds_until_analysis_window)
        return state

    def recover_running_analysis(
        self,
        user_id: str,
        window: dict[str, Any],
    ) -> tuple[str | None, RobotState]:
        state = self.get(user_id)
        running = state.status == STATUS_ANALYZING or state.analysis_result == "RUNNING" or state.last_analysis_result == "RUNNING"
        if not running or state.pending_signal or state.best_candidate:
            return None, state

        current_second = float(window["current_candle_seconds"])
        analysis_end = float(window["analysis_window_end_second"])
        if current_second > analysis_end or not bool(window["analysis_window_open"]):
            return "OUTSIDE_ANALYSIS_WINDOW", self.wait_analysis_window(
                user_id,
                window,
                clear_pending=True,
                analysis_result="WAITING_NEXT_ANALYSIS_WINDOW",
                last_rejection_reason="WAITING_NEXT_ANALYSIS_WINDOW",
            )

        started_at = state.analysis_started_at or state.last_analysis_at or state.current_cycle_started_at
        if started_at is None:
            return None, state
        if (utc_now() - started_at).total_seconds() <= ANALYSIS_TIMEOUT_SECONDS:
            return None, state
        return STATUS_ANALYSIS_TIMEOUT, self.wait_analysis_window(
            user_id,
            window,
            clear_pending=True,
            analysis_result=STATUS_ANALYSIS_TIMEOUT,
            rejection_reason=STATUS_ANALYSIS_TIMEOUT,
            last_rejection_reason=ANALYSIS_TIMEOUT_MESSAGE,
            force_next=True,
        )

    def set_analysis_candidates(
        self,
        user_id: str,
        candidates: list[dict[str, Any]],
        best_candidate: dict[str, Any] | None,
    ) -> RobotState:
        state = self.get(user_id)
        state.candidates_count = len(candidates)
        state.candidates = [strip_ai_fields(dict(candidate)) for candidate in candidates]
        state.best_candidate = strip_ai_fields(dict(best_candidate)) if best_candidate is not None else None
        state.cycle_best_candidate = strip_ai_fields(dict(best_candidate)) if best_candidate is not None else None
        state.cycle_best_trade_candidate = (
            strip_ai_fields(dict(best_candidate))
            if best_candidate is not None and bool(best_candidate.get("trade_allowed", True))
            else None
        )
        state.strategy_score = int((best_candidate or {}).get("strategy_score") or 0)
        state.strategy_name = (best_candidate or {}).get("strategy_name")
        state.strategy_reason = (best_candidate or {}).get("strategy_reason")
        state.used_strategies = list((best_candidate or {}).get("used_strategies") or [])
        state.candle_reading = (best_candidate or {}).get("candle_reading")
        state.entry_reason = (best_candidate or {}).get("entry_reason")
        state.block_reasons = list(
            (best_candidate or {}).get("block_reasons")
            or (best_candidate or {}).get("blocked_filters")
            or []
        )
        state.metrics = dict((best_candidate or {}).get("metrics") or {})
        state.last_analysis_at = utc_now()
        state.last_analysis_result = "BEST_CANDIDATE_UPDATED" if best_candidate is not None else "NO_CANDIDATES"
        state.analysis_result = state.last_analysis_result
        state.analysis_message = None
        return state

    def clear_pending_signal(self, user_id: str, *, analyze: bool = False) -> RobotState:
        state = self.get(user_id)
        state.pending_signal = None
        state.last_signal = None
        self._clear_gale_state(state)
        state.status = STATUS_ANALYZING if analyze else (
            STATUS_WAITING_NEXT_CYCLE if state.enabled else STATUS_STOPPED
        )
        if analyze:
            state.next_cycle_at = utc_now()
        state.rejection_reason = None
        state.blocked_filters = []
        state.approved_filters = []
        state.quality_score = 0
        state.strategy_score = 0
        state.best_candidate = None
        state.cycle_best_candidate = None
        state.cycle_best_trade_candidate = None
        state.strategy_name = None
        state.strategy_reason = None
        state.used_strategies = []
        state.candle_reading = None
        state.entry_reason = None
        state.block_reasons = []
        state.metrics = {}
        state.analysis_message = ANALYSIS_MESSAGE if analyze else None
        return state

    def complete_cycle_without_trade(self, user_id: str, reason: str = "NO_TRADE") -> RobotState:
        state = self.get(user_id)
        now = utc_now()
        state.status = STATUS_WAITING_NEXT_CYCLE if state.enabled else STATUS_STOPPED
        state.rejection_reason = None
        state.last_rejection_reason = reason
        state.last_analysis_at = now
        state.last_analysis_result = reason
        state.analysis_result = reason
        state.analysis_message = "Sem entrada neste ciclo"
        state.pending_signal = None
        state.last_signal = None
        state.operation_in_progress = False
        state.entry_window_open = False
        state.seconds_until_entry_window = 0
        state.order_attempts = 0
        state.fallback_candidate_used = False
        state.current_cycle_started_at = now
        state.cycle_id = self._new_cycle_id()
        state.next_cycle_at = now + timedelta(minutes=state.cycle_minutes) if state.enabled else None
        state.candidates = []
        state.candidates_count = 0
        state.best_candidate = None
        state.cycle_best_candidate = None
        state.cycle_best_trade_candidate = None
        return state

    def pause_by_stop(self, user_id: str, reason: str) -> RobotState:
        state = self.get(user_id)
        state.enabled = False
        state.status = reason
        state.rejection_reason = None
        state.last_rejection_reason = reason
        state.analysis_message = None
        state.operation_in_progress = False
        state.entry_window_open = False
        state.seconds_until_entry_window = 0
        state.analysis_window_open = False
        state.seconds_until_analysis_window = 0
        state.pending_signal = None
        state.next_cycle_at = None
        if reason == STATUS_STOP_WIN_HIT:
            state.last_order_error = STATUS_STOP_WIN_HIT
        elif reason == STATUS_STOP_LOSS_HIT:
            state.last_order_error = STATUS_STOP_LOSS_HIT
        self._clear_gale_state(state)
        return state

    def reset_cycle(
        self,
        user_id: str,
        *,
        reset_score: bool = False,
        reset_daily_profit: bool = True,
    ) -> RobotState:
        state = self.get(user_id)
        now = utc_now()
        state.enabled = False
        state.status = STATUS_STOPPED
        state.rejection_reason = None
        state.last_rejection_reason = None
        state.cycle_result = None
        state.rejected_at = None
        state.result_received_at = None
        state.result_display_until = None
        state.last_trade = None
        state.pending_signal = None
        state.last_signal = None
        state.analysis_started_at = None
        state.analysis_result = None
        state.last_analysis_result = None
        state.analysis_message = None
        state.last_order_error = None
        state.operation_in_progress = False
        state.sync_started_at = None
        state.analysis_window_open = False
        state.seconds_until_analysis_window = 0
        state.entry_window_open = False
        state.seconds_until_entry_window = 0
        state.order_attempts = 0
        state.fallback_candidate_used = False
        state.current_cycle_started_at = None
        state.cycle_id = self._new_cycle_id()
        state.next_cycle_at = None
        state.blocked_filters = []
        state.approved_filters = []
        state.quality_score = 0
        state.strategy_score = 0
        state.candidates_count = 0
        state.candidates = []
        state.best_candidate = None
        state.cycle_best_candidate = None
        state.cycle_best_trade_candidate = None
        state.strategy_name = None
        state.strategy_reason = None
        state.used_strategies = []
        state.candle_reading = None
        state.entry_reason = None
        state.block_reasons = []
        state.metrics = {}
        self._clear_gale_state(state)

        if reset_score:
            state.wins = 0
            state.losses = 0
            self._histories[user_id] = []
        if reset_score or reset_daily_profit:
            state.profit = 0.0
            state.stop_reset_at = now

        self._sources[user_id] = "memory"
        return state

    def expire_pending_signal(
        self,
        user_id: str,
        *,
        reason: str = "ENTRY_WINDOW_MISSED",
        wait_seconds: int = 5,
    ) -> RobotState:
        state = self.get(user_id)
        now = utc_now()
        state.status = STATUS_SIGNAL_EXPIRED
        state.rejection_reason = STATUS_SIGNAL_EXPIRED
        state.last_rejection_reason = STATUS_SIGNAL_EXPIRED
        state.last_order_error = reason
        state.pending_signal = None
        state.last_signal = None
        state.operation_in_progress = False
        state.entry_window_open = False
        state.seconds_until_entry_window = 0
        state.next_cycle_at = now + timedelta(seconds=max(1, int(wait_seconds)))
        state.analysis_message = None
        self._clear_gale_state(state)
        return state

    def disconnect_account(self, user_id: str) -> RobotState:
        state = self.get(user_id)
        state.enabled = False
        state.connected = False
        state.active_mode = None
        state.connection_status_source = "disconnected"
        state.connection_checked_at = utc_now()
        state.connection_grace_until = None
        state.pending_signal = None
        state.last_signal = None
        state.operation_in_progress = False
        state.entry_window_open = False
        state.seconds_until_entry_window = 0
        state.next_cycle_at = None
        state.current_cycle_started_at = None
        state.status = STATUS_ACCOUNT_DISCONNECTED
        state.rejection_reason = STATUS_ACCOUNT_DISCONNECTED
        state.last_rejection_reason = STATUS_ACCOUNT_DISCONNECTED
        state.last_trade = None
        state.last_analysis_at = None
        state.last_analysis_result = None
        state.analysis_started_at = None
        state.analysis_result = None
        state.analysis_message = None
        state.blocked_filters = []
        state.approved_filters = []
        state.quality_score = 0
        state.strategy_score = 0
        state.candidates_count = 0
        state.candidates = []
        state.best_candidate = None
        state.cycle_best_candidate = None
        state.cycle_best_trade_candidate = None
        state.candle_reading = None
        state.entry_reason = None
        state.block_reasons = []
        state.metrics = {}
        return state

    def defer_cycle(
        self,
        user_id: str,
        status: str,
        *,
        wait_seconds: int,
        rejection_reason: str | None = None,
        last_rejection_reason: str | None = None,
        last_order_error: str | None = None,
    ) -> RobotState:
        state = self.get(user_id)
        now = utc_now()
        state.status = status
        state.rejection_reason = rejection_reason or status
        state.last_rejection_reason = last_rejection_reason or state.rejection_reason
        state.last_order_error = last_order_error
        state.operation_in_progress = False
        state.entry_window_open = False
        state.seconds_until_entry_window = 0
        state.next_cycle_at = now + timedelta(seconds=max(1, int(wait_seconds)))
        state.analysis_message = None
        return state

    def sync_connection(
        self,
        user_id: str,
        *,
        connected: bool,
        active_mode: str | None = None,
        source: str = "cached",
        checked_at: datetime | None = None,
        align_status: bool = False,
    ) -> RobotState:
        state = self.get(user_id)
        now = checked_at or utc_now()
        state.connected = connected
        state.active_mode = active_mode
        state.connection_checked_at = now
        state.connection_status_source = source
        if connected:
            state.connection_failure_count = 0
            state.last_connected_at = now
            state.connection_grace_until = now + timedelta(seconds=30)
            if align_status or state.status == STATUS_ACCOUNT_DISCONNECTED:
                state.rejection_reason = None
                state.last_rejection_reason = None
            if align_status and not state.operation_in_progress:
                state.status = STATUS_WAITING_NEXT_CYCLE if state.enabled else STATUS_STOPPED
        else:
            state.connection_failure_count += 1
        return state

    def start_sending_order(self, user_id: str) -> RobotState:
        state = self.get(user_id)
        if not state.enabled:
            raise RuntimeError("ROBOT_STOPPED")
        if state.status not in {
            STATUS_WAITING_NEXT_CANDLE_ENTRY,
            STATUS_WAITING_GALE_ENTRY,
            STATUS_SENDING_ORDER,
            STATUS_SENDING_GALE_ORDER,
        } or not state.pending_signal:
            raise RuntimeError("INVALID_ORDER_STATE_TRANSITION")
        state.status = STATUS_SENDING_GALE_ORDER if state.gale_pending else STATUS_SENDING_ORDER
        state.rejection_reason = None
        state.rejected_at = None
        state.last_order_error = None
        return state

    def reject_order(
        self,
        user_id: str,
        reason: str,
        *,
        last_order_error: str | None = None,
    ) -> RobotState:
        state = self.get(user_id)
        rejected_at = utc_now()
        state.pending_signal = None
        state.operation_in_progress = False
        state.order_attempts = max(1, state.order_attempts)
        state.status = STATUS_ORDER_REJECTED
        state.rejection_reason = reason
        state.last_rejection_reason = reason
        state.last_order_error = last_order_error or reason
        state.rejected_at = rejected_at
        state.next_cycle_at = rejected_at + timedelta(minutes=state.cycle_minutes)
        state.entry_window_open = False
        state.seconds_until_entry_window = 0
        state.last_analysis_result = STATUS_ORDER_REJECTED
        self._clear_gale_state(state)
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
        trade.setdefault("expiration", state.timeframe)
        trade.setdefault(
            "expected_expire_at",
            (sent_at + timedelta(seconds=TIMEFRAME_SECONDS[state.timeframe])).isoformat(),
        )
        trade.setdefault("expires_at", trade["expected_expire_at"])
        state.last_trade = trade
        state.last_entry_at = sent_at
        state.result_received_at = None
        state.result_display_until = None
        state.operation_in_progress = True
        is_gale = bool(trade.get("is_gale"))
        state.status = STATUS_PENDING_GALE_RESULT if is_gale else STATUS_PENDING_RESULT
        state.rejection_reason = None
        state.last_order_error = None
        if is_gale:
            state.gale_pending = False
            state.gale_active = True
            state.gale_step = int(trade.get("gale_step") or 1)
            state.gale_amount = float(trade.get("gale_amount") or trade.get("amount") or 0)
        return state

    def lock_real(self, user_id: str, reason: str = "REAL_TRADING_LOCKED") -> RobotState:
        state = self.get(user_id)
        state.status = STATUS_REAL_TRADING_LOCKED
        state.rejection_reason = reason
        state.last_rejection_reason = reason
        state.analysis_result = None
        state.last_analysis_result = reason
        state.analysis_message = None
        state.pending_signal = None
        state.operation_in_progress = False
        state.entry_window_open = False
        state.seconds_until_entry_window = 0
        state.last_order_error = reason
        return state

    def update_entry_window(self, user_id: str, window: dict[str, Any]) -> RobotState:
        state = self.get(user_id)
        state.server_time = window["server_time"]
        state.server_time_source = str(window.get("server_time_source") or "bullex")
        waiting_next_cycle = (
            state.status == STATUS_WAITING_NEXT_CYCLE
            and not state.pending_signal
            and not state.operation_in_progress
        )
        state.analysis_window_open = bool(window["analysis_window_open"])
        state.seconds_until_analysis_window = int(window["seconds_until_analysis_window"])
        state.analysis_window_start_second = int(window["analysis_window_start_second"])
        state.analysis_window_end_second = int(window["analysis_window_end_second"])
        state.entry_window_open = (
            bool(window["entry_window_open"])
            and not waiting_next_cycle
            and state.status not in {STATUS_RESULT_RECEIVED, STATUS_GALE_RESULT_RECEIVED}
        )
        state.seconds_until_entry_window = int(window["seconds_until_entry_window"])
        state.current_candle_seconds = float(window["current_candle_seconds"])
        state.entry_window_start_second = int(window["entry_window_start_second"])
        state.entry_window_end_second = int(window["entry_window_end_second"])
        state.buy_target_second = int(window["buy_target_second"])
        state.expiration_seconds = int(window["expiration_seconds"])
        if (
            state.status == STATUS_ANALYZING
            and state.analysis_result != "RUNNING"
            and state.pending_signal is None
            and not state.operation_in_progress
        ):
            state.status = STATUS_WAITING_NEXT_CYCLE if state.enabled else STATUS_STOPPED
            state.rejection_reason = None
            state.analysis_message = None
            state.analysis_started_at = None
        if (
            not state.entry_window_open
            and state.enabled
            and not state.operation_in_progress
            and state.pending_signal
        ):
            state.status = STATUS_WAITING_GALE_ENTRY if state.gale_pending else STATUS_WAITING_NEXT_CANDLE_ENTRY
            state.rejection_reason = None
        elif (
            state.entry_window_open
            and state.status in {STATUS_WAITING_NEXT_CANDLE_ENTRY, STATUS_WAITING_GALE_ENTRY}
            and not state.pending_signal
        ):
            state.status = STATUS_WAITING_NEXT_CYCLE if state.enabled else STATUS_STOPPED
            state.rejection_reason = None
        return state

    def _build_gale_signal(self, state: RobotState, trade: dict[str, Any], gale_amount: float) -> dict[str, Any]:
        direction = str(trade.get("direction") or trade.get("signal") or "").upper()
        return {
            "symbol": str(trade.get("active") or ""),
            "direction": direction,
            "signal": direction,
            "confidence": float(trade.get("confidence") or 0),
            "payout": float(trade.get("payout") or 0),
            "strategy_score": int(trade.get("strategy_score") or trade.get("score") or 0),
            "score": int(trade.get("strategy_score") or trade.get("score") or 0),
            "reason": trade.get("entry_reason") or trade.get("reason") or "GALE_1",
            "entry_reason": trade.get("entry_reason") or trade.get("reason") or "GALE_1",
            "candle_reading": trade.get("candle_reading"),
            "block_reasons": list(trade.get("block_reasons") or []),
            "metrics": dict(trade.get("metrics") or {}),
            "strategy_name": trade.get("strategy_name") or "Martingale G1",
            "strategy_reason": trade.get("strategy_reason") or trade.get("entry_reason") or "Martingale G1",
            "used_strategies": list(trade.get("used_strategies") or []),
            "timeframe": str(trade.get("timeframe") or trade.get("expiration") or state.timeframe),
            "quality_score": int(trade.get("quality_score") or trade.get("strategy_score") or trade.get("score") or 0),
            "blocked_filters": list(trade.get("blocked_filters") or []),
            "approved_filters": list(trade.get("approved_filters") or []),
            "strategy_mode": trade.get("strategy_mode", state.strategy_mode),
            "cycle_id": state.cycle_id,
            "created_at": utc_now().isoformat(),
            "target_entry_second": state.buy_target_second,
            "entry_window_start_second": state.entry_window_start_second,
            "entry_window_end_second": state.entry_window_end_second,
            "is_gale": True,
            "gale_step": 1,
            "gale_amount": gale_amount,
            "parent_order_id": str(trade.get("order_id") or "").strip(),
            "original_amount": float(trade.get("amount") or 0),
        }

    def trigger_gale(self, user_id: str, order_id: Any, profit: float) -> tuple[bool, RobotState]:
        state = self.get(user_id)
        normalized_order_id = str(order_id or "").strip()
        completed = self._completed_order_ids.setdefault(user_id, set())
        if not normalized_order_id or normalized_order_id in completed:
            return False, state
        if not state.enabled:
            return False, state
        if not state.operation_in_progress or not state.last_trade:
            return False, state
        if str(state.last_trade.get("order_id") or "").strip() != normalized_order_id:
            return False, state

        parent_trade = dict(state.last_trade)
        amount = float(parent_trade.get("amount") or 0)
        loss_profit = float(profit)
        if loss_profit >= 0:
            loss_profit = -amount
        finished_at = utc_now()
        parent_trade.update(
            {
                "result": "LOSS",
                "profit": round(loss_profit, 2),
                "finished_at": finished_at.isoformat(),
                "final_result": None,
                "cycle_result": None,
            }
        )
        gale_amount = round(amount * float(state.martingale_multiplier or 2), 2)
        completed.add(normalized_order_id)
        state.last_trade = parent_trade
        state.operation_in_progress = False
        state.pending_signal = self._build_gale_signal(state, parent_trade, gale_amount)
        state.last_signal = dict(state.pending_signal)
        state.status = STATUS_WAITING_GALE_ENTRY
        state.rejection_reason = None
        state.last_rejection_reason = None
        state.result_received_at = None
        state.result_display_until = None
        state.entry_window_open = False
        state.seconds_until_entry_window = 0
        state.next_cycle_at = None
        state.gale_pending = True
        state.gale_active = True
        state.gale_step = 1
        state.gale_amount = gale_amount
        state.gale_direction = str(parent_trade.get("direction") or "").upper() or None
        state.gale_original_order_id = normalized_order_id
        state.gale_parent_trade = dict(parent_trade)
        state.cycle_result = None
        history = self._histories.setdefault(user_id, [])
        history.append(dict(parent_trade))
        del history[:-100]
        return True, state

    def _clear_gale_state(self, state: RobotState, *, preserve_context: bool = False) -> None:
        state.gale_pending = False
        state.gale_active = False
        if not preserve_context:
            state.gale_step = 0
            state.gale_amount = 0.0
            state.gale_direction = None
            state.gale_original_order_id = None
            state.gale_parent_trade = None

    def finish_trade(self, user_id: str, order_id: Any, result: str, profit: float) -> tuple[bool, RobotState]:
        state = self.get(user_id)
        normalized_order_id = str(order_id or "").strip()
        completed = self._completed_order_ids.setdefault(user_id, set())
        if not normalized_order_id or normalized_order_id in completed:
            return False, state
        if not state.last_trade:
            return False, state
        if str(state.last_trade.get("order_id") or "").strip() != normalized_order_id:
            return False, state
        trade_result = str((state.last_trade or {}).get("result") or "").strip().upper()
        if not state.operation_in_progress and trade_result in {"WIN", "LOSS", "TIMEOUT"}:
            return False, state

        normalized_result = str(result or "").strip().upper()
        if normalized_result not in {"WIN", "LOSS"}:
            return False, state

        trade = dict(state.last_trade)
        is_gale_trade = bool(trade.get("is_gale"))
        amount = float(trade.get("amount") or 0)
        projected_trade_profit = float(profit)
        if normalized_result == "WIN":
            projected_trade_profit = projected_trade_profit if projected_trade_profit > 0 else amount
        else:
            projected_trade_profit = projected_trade_profit if projected_trade_profit < 0 else -amount
        projected_cycle_profit = projected_trade_profit
        if is_gale_trade:
            projected_cycle_profit = round(
                float((state.gale_parent_trade or {}).get("profit") or 0) + projected_trade_profit,
                2,
            )
        projected_total_profit = round(float(state.profit) + projected_cycle_profit, 2)
        should_trigger_gale = (
            normalized_result == "LOSS"
            and state.enabled
            and not is_gale_trade
            and state.martingale_enabled
            and int(state.martingale_steps or 1) >= 1
            and not state.gale_active
            and not (state.stop_loss > 0 and projected_total_profit <= -state.stop_loss)
        )
        if should_trigger_gale:
            triggered, triggered_state = self.trigger_gale(user_id, normalized_order_id, profit)
            if triggered:
                return False, triggered_state
            return False, state

        trade_profit = float(profit)
        if normalized_result == "WIN":
            trade_profit = trade_profit if trade_profit > 0 else amount
            state.wins += 1
            state.cycle_result = "WIN"
        else:
            state.losses += 1
            trade_profit = trade_profit if trade_profit < 0 else -amount
            state.cycle_result = "LOSS"

        cycle_profit = trade_profit
        if is_gale_trade:
            cycle_profit = round(float((state.gale_parent_trade or {}).get("profit") or 0) + trade_profit, 2)
        state.profit += cycle_profit

        finished_at = utc_now()
        trade.update(
            {
                "result": normalized_result,
                "profit": round(trade_profit, 2),
                "finished_at": finished_at.isoformat(),
                "cycle_result": state.cycle_result,
                "final_result": normalized_result,
                "is_gale": is_gale_trade,
                "gale_step": int(trade.get("gale_step") or (1 if is_gale_trade else 0)),
                "parent_order_id": trade.get("parent_order_id") or state.gale_original_order_id,
                "original_amount": float(
                    trade.get("original_amount")
                    or (state.gale_parent_trade or {}).get("amount")
                    or trade.get("amount")
                    or 0
                ),
                "gale_amount": float(trade.get("gale_amount") or (trade.get("amount") if is_gale_trade else 0) or 0),
            }
        )
        completed.add(normalized_order_id)
        state.last_trade = trade
        state.operation_in_progress = False
        state.status = (
            STATUS_GALE_RESULT_RECEIVED if is_gale_trade else STATUS_RESULT_RECEIVED
        ) if state.enabled else STATUS_STOPPED
        state.rejection_reason = None
        state.last_rejection_reason = None
        state.result_received_at = finished_at
        state.result_display_until = finished_at + timedelta(seconds=5) if state.enabled else None
        state.entry_window_open = False
        state.seconds_until_entry_window = 0
        state.next_cycle_at = None
        state.profit = round(state.profit, 2)
        self._clear_gale_state(state, preserve_context=True)
        if state.enabled:
            if state.stop_win > 0 and state.profit >= state.stop_win:
                state = self.pause_by_stop(user_id, STATUS_STOP_WIN_HIT)
            elif state.stop_loss > 0 and state.profit <= -state.stop_loss:
                state = self.pause_by_stop(user_id, STATUS_STOP_LOSS_HIT)
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
        if not state.last_trade:
            return False, state
        if str(state.last_trade.get("order_id") or "").strip() != normalized_order_id:
            return False, state
        trade_result = str((state.last_trade or {}).get("result") or "").strip().upper()
        if not state.operation_in_progress and trade_result in {"WIN", "LOSS", "TIMEOUT"}:
            return False, state

        trade = dict(state.last_trade)
        finished_at = utc_now()
        trade.update(
            {
                "result": "TIMEOUT",
                "profit": 0.0,
                "finished_at": finished_at.isoformat(),
            }
        )
        completed.add(normalized_order_id)
        state.last_trade = trade
        state.operation_in_progress = False
        state.status = STATUS_WAITING_NEXT_CYCLE if state.enabled else STATUS_STOPPED
        state.rejection_reason = "TRADE_RESULT_TIMEOUT"
        state.last_rejection_reason = "TRADE_RESULT_TIMEOUT"
        state.entry_window_open = False
        state.seconds_until_entry_window = 0
        self._clear_gale_state(state)
        self._schedule_next_cycle(state, finished_at)
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
