import asyncio
import unittest
from unittest.mock import AsyncMock

from backend.auto_trader import AutoTrader, STATUS_PENDING_RESULT, STATUS_WAITING_NEXT_CYCLE
from backend.trade_result_monitor import TradeResultMonitor, normalize_trade_result


def pending_trade(order_id: str, amount: float = 2.0) -> dict:
    return {
        "order_id": order_id,
        "active": "EURUSD-OTC",
        "direction": "CALL",
        "amount": amount,
        "confidence": 90,
        "payout": 88,
        "result": STATUS_PENDING_RESULT,
        "sent_at": "2026-06-12T00:00:00+00:00",
    }


class AutoTraderResultTests(unittest.TestCase):
    def test_win_updates_state_history_and_accuracy_once(self) -> None:
        trader = AutoTrader()
        trader.start("user-win")
        trader.record_trade("user-win", pending_trade("101"))

        first, state = trader.finish_trade("user-win", "101", "WIN", 1.76)
        second, duplicate_state = trader.finish_trade("user-win", "101", "WIN", 1.76)
        history = trader.history("user-win")

        self.assertTrue(first)
        self.assertFalse(second)
        self.assertFalse(state.operation_in_progress)
        self.assertEqual(state.status, STATUS_WAITING_NEXT_CYCLE)
        self.assertEqual(duplicate_state.wins, 1)
        self.assertEqual(history["wins"], 1)
        self.assertEqual(history["losses"], 0)
        self.assertEqual(history["profit"], 1.76)
        self.assertEqual(history["accuracy"], 100.0)
        self.assertEqual(len(history["trades"]), 1)
        self.assertEqual(history["trades"][0]["result"], "WIN")

    def test_loss_subtracts_entry_amount(self) -> None:
        trader = AutoTrader()
        trader.start("user-loss")
        trader.record_trade("user-loss", pending_trade("102", amount=2.0))

        finalized, state = trader.finish_trade("user-loss", "102", "LOSS", 0)

        self.assertTrue(finalized)
        self.assertEqual(state.losses, 1)
        self.assertEqual(state.profit, -2.0)
        self.assertEqual(state.last_trade["profit"], -2.0)

    def test_history_keeps_only_last_one_hundred_trades(self) -> None:
        trader = AutoTrader()
        trader.start("user-history")

        for index in range(105):
            order_id = str(index)
            trader.record_trade("user-history", pending_trade(order_id))
            trader.finish_trade("user-history", order_id, "WIN", 1)

        history = trader.history("user-history")
        self.assertEqual(len(history["trades"]), 100)
        self.assertEqual(history["trades"][0]["order_id"], "104")
        self.assertEqual(history["trades"][-1]["order_id"], "5")

    def test_timeout_releases_operation_without_counting_result(self) -> None:
        trader = AutoTrader()
        trader.start("user-timeout")
        trader.record_trade("user-timeout", pending_trade("103"))

        finalized, state = trader.timeout_trade("user-timeout", "103")

        self.assertTrue(finalized)
        self.assertFalse(state.operation_in_progress)
        self.assertEqual(state.last_trade["result"], "TIMEOUT")
        self.assertEqual(state.wins, 0)
        self.assertEqual(state.losses, 0)


class TradeResultMonitorTests(unittest.IsolatedAsyncioTestCase):
    def test_normalizes_bullex_results(self) -> None:
        self.assertEqual(
            normalize_trade_result({"ok": True, "data": {"result": "win", "profit": 1.76}}),
            ("WIN", 1.76),
        )
        self.assertEqual(
            normalize_trade_result({"ok": True, "data": {"result": "loose", "profit": -2}}),
            ("LOSS", -2.0),
        )
        self.assertIsNone(
            normalize_trade_result({"ok": True, "data": {"result": "PENDING_RESULT", "profit": None}})
        )

    async def test_monitor_polls_until_win(self) -> None:
        fetch = AsyncMock(
            side_effect=[
                (200, {"ok": True, "data": {"result": "PENDING_RESULT"}, "error": None}),
                (200, {"ok": True, "data": {"result": "win", "profit": 1.76}, "error": None}),
            ]
        )
        finish = AsyncMock()
        timeout = AsyncMock()
        monitor = TradeResultMonitor(fetch, finish, timeout, poll_seconds=0, timeout_seconds=1)

        self.assertTrue(monitor.start("user-monitor", "104"))
        self.assertFalse(monitor.start("user-monitor", "104"))
        await asyncio.gather(*list(monitor._tasks.values()))

        self.assertEqual(fetch.await_count, 2)
        finish.assert_awaited_once_with("user-monitor", "104", "WIN", 1.76)
        timeout.assert_not_awaited()

    async def test_monitor_times_out_and_releases_trade(self) -> None:
        fetch = AsyncMock(return_value=(200, {"ok": True, "data": {"result": "PENDING_RESULT"}, "error": None}))
        finish = AsyncMock()
        timeout = AsyncMock()
        monitor = TradeResultMonitor(fetch, finish, timeout, poll_seconds=0, timeout_seconds=0.001)

        monitor.start("user-monitor", "105")
        await asyncio.gather(*list(monitor._tasks.values()))

        finish.assert_not_awaited()
        timeout.assert_awaited_once_with("user-monitor", "105")
