import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from time import monotonic
from typing import Any


logger = logging.getLogger("backend-gateway")

PENDING_RESULT = "PENDING_RESULT"
FINAL_RESULTS = {"WIN", "LOSS", "TIMEOUT"}

FetchResult = Callable[[str, str], Awaitable[tuple[int, dict[str, Any]]]]
FinishTrade = Callable[[str, str, str, float], Awaitable[None]]
TimeoutTrade = Callable[[str, str], Awaitable[None]]


def normalize_trade_result(payload: dict[str, Any]) -> tuple[str, float] | None:
    if not payload.get("ok"):
        return None
    data = payload.get("data")
    if not isinstance(data, dict):
        return None

    raw_result = str(data.get("result") or "").strip().lower()
    if raw_result in {"", "pending", "pending_result", "open"}:
        return None

    try:
        profit = float(data.get("profit") or 0)
    except (TypeError, ValueError):
        profit = 0.0

    has_profit = "profit" in data

    if raw_result in {"timeout", "timed_out", "expired"}:
        return "TIMEOUT", 0.0
    if raw_result in {"profit"}:
        if profit > 0:
            return "WIN", profit
        if profit < 0:
            return "LOSS", profit
        return None
    if raw_result in {"win", "won"}:
        if has_profit and profit < 0:
            return "LOSS", profit
        return "WIN", profit
    if raw_result in {"loose", "lose", "loss", "lost", "loss_amount", "equal", "draw"}:
        return "LOSS", profit
    if "profit" in data:
        if profit > 0:
            return "WIN", profit
        if profit < 0:
            return "LOSS", profit
    return None


@dataclass
class TradeResultMonitor:
    fetch_result: FetchResult
    finish_trade: FinishTrade
    timeout_trade: TimeoutTrade
    poll_seconds: float = 1.0
    timeout_seconds: float = 2100.0
    _tasks: dict[str, asyncio.Task[None]] = field(default_factory=dict)

    def _task_key(self, user_id: str, order_id: str) -> str:
        return f"{str(user_id).strip()}:{str(order_id).strip()}"

    def start(self, user_id: str, order_id: Any, expires_at: Any = None) -> bool:
        normalized_order_id = str(order_id or "").strip()
        if not normalized_order_id:
            return False

        key = self._task_key(user_id, normalized_order_id)
        task = self._tasks.get(key)
        if task is not None and not task.done():
            logger.info(
                "[ORDER_RESULT_MONITOR_REUSED] user_id=%s order_id=%s",
                user_id,
                normalized_order_id,
            )
            return False

        self._tasks[key] = asyncio.create_task(self._monitor(user_id, normalized_order_id, expires_at))
        logger.info("[ORDER_RESULT_MONITOR_STARTED] user_id=%s order_id=%s", user_id, normalized_order_id)
        return True

    async def shutdown(self) -> None:
        tasks = list(self._tasks.values())
        self._tasks.clear()
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _monitor(self, user_id: str, order_id: str, expires_at: Any = None) -> None:
        key = self._task_key(user_id, order_id)
        logger.info("[ROBOT TRADE MONITOR START] user_id=%s order_id=%s", user_id, order_id)
        try:
            timeout_seconds = float(self.timeout_seconds)
            poll_seconds = max(1.0, float(self.poll_seconds))
            if expires_at is not None:
                try:
                    expiration = (
                        expires_at
                        if isinstance(expires_at, datetime)
                        else datetime.fromisoformat(str(expires_at).replace("Z", "+00:00"))
                    )
                    if expiration.tzinfo is None:
                        expiration = expiration.replace(tzinfo=timezone.utc)
                    delay = max(0.0, (expiration - datetime.now(timezone.utc)).total_seconds())
                    timeout_seconds = delay + 15.0
                except (TypeError, ValueError):
                    logger.warning(
                        "[RESULT_MONITOR_EXPIRATION_INVALID] user_id=%s order_id=%s expires_at=%s",
                        user_id,
                        order_id,
                        expires_at,
                    )
            started_at = monotonic()
            while monotonic() - started_at < timeout_seconds:
                try:
                    _, payload = await self.fetch_result(user_id, order_id)
                    normalized = normalize_trade_result(payload)
                    if normalized is not None:
                        result, profit = normalized
                        if result == "TIMEOUT":
                            await self.timeout_trade(user_id, order_id)
                        else:
                            await self.finish_trade(user_id, order_id, result, profit)
                        logger.info(
                            "[ORDER_RESULT_MONITOR_FINISHED] user_id=%s order_id=%s result=%s profit=%s",
                            user_id,
                            order_id,
                            result,
                            profit,
                        )
                        logger.info(
                            "[ROBOT TRADE %s] user_id=%s order_id=%s profit=%s",
                            result,
                            user_id,
                            order_id,
                            profit,
                        )
                        return
                    if not payload.get("ok"):
                        logger.error(
                            "[ROBOT TRADE ERROR] user_id=%s order_id=%s error=%s",
                            user_id,
                            order_id,
                            payload.get("error"),
                        )
                    else:
                        logger.info("[ROBOT TRADE PENDING] user_id=%s order_id=%s", user_id, order_id)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.exception(
                        "[ROBOT TRADE ERROR] user_id=%s order_id=%s error=%s",
                        user_id,
                        order_id,
                        exc,
                    )
                await asyncio.sleep(poll_seconds)

            await self.timeout_trade(user_id, order_id)
            logger.info(
                "[ORDER_RESULT_MONITOR_FINISHED] user_id=%s order_id=%s result=TIMEOUT profit=0.0",
                user_id,
                order_id,
            )
            logger.warning("[ROBOT TRADE TIMEOUT] user_id=%s order_id=%s", user_id, order_id)
        finally:
            current = asyncio.current_task()
            if self._tasks.get(key) is current:
                self._tasks.pop(key, None)
