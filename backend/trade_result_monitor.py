import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from time import monotonic
from typing import Any


logger = logging.getLogger("backend-gateway")

PENDING_RESULT = "PENDING_RESULT"
FINAL_RESULTS = {"WIN", "LOSS"}

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

    if raw_result in {"win", "won"}:
        return "WIN", profit
    if raw_result in {"loose", "lose", "loss", "lost", "equal", "draw"}:
        return "LOSS", profit
    return None


@dataclass
class TradeResultMonitor:
    fetch_result: FetchResult
    finish_trade: FinishTrade
    timeout_trade: TimeoutTrade
    poll_seconds: float = 3.0
    timeout_seconds: float = 2100.0
    _tasks: dict[tuple[str, str], asyncio.Task[None]] = field(default_factory=dict)

    def start(self, user_id: str, order_id: Any) -> bool:
        normalized_order_id = str(order_id or "").strip()
        if not normalized_order_id:
            return False

        key = (user_id, normalized_order_id)
        task = self._tasks.get(key)
        if task is not None and not task.done():
            return False

        self._tasks[key] = asyncio.create_task(self._monitor(user_id, normalized_order_id))
        logger.info("[RESULT_MONITOR_START] user_id=%s order_id=%s", user_id, normalized_order_id)
        return True

    async def shutdown(self) -> None:
        tasks = list(self._tasks.values())
        self._tasks.clear()
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _monitor(self, user_id: str, order_id: str) -> None:
        key = (user_id, order_id)
        started_at = monotonic()
        logger.info("[ROBOT TRADE MONITOR START] user_id=%s order_id=%s", user_id, order_id)
        try:
            while monotonic() - started_at < self.timeout_seconds:
                try:
                    _, payload = await self.fetch_result(user_id, order_id)
                    normalized = normalize_trade_result(payload)
                    if normalized is not None:
                        result, profit = normalized
                        await self.finish_trade(user_id, order_id, result, profit)
                        logger.info(
                            "[RESULT_MONITOR_FINISHED] user_id=%s order_id=%s result=%s profit=%s",
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
                await asyncio.sleep(self.poll_seconds)

            await self.timeout_trade(user_id, order_id)
            logger.warning("[ROBOT TRADE TIMEOUT] user_id=%s order_id=%s", user_id, order_id)
        finally:
            current = asyncio.current_task()
            if self._tasks.get(key) is current:
                self._tasks.pop(key, None)
