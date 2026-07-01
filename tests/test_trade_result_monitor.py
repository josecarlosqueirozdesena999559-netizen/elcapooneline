import asyncio
import unittest
from datetime import timedelta
from unittest.mock import AsyncMock, patch

from backend.auto_trader import (
    AutoTrader,
    STATUS_GALE_RESULT_RECEIVED,
    STATUS_PENDING_GALE_RESULT,
    STATUS_ANALYZING,
    STATUS_ACCOUNT_DISCONNECTED,
    STATUS_PENDING_RESULT,
    STATUS_RESULT_RECEIVED,
    STATUS_SYNCING,
    STATUS_WAITING_GALE_ENTRY,
    STATUS_WAITING_NEXT_CYCLE,
    parse_datetime,
    utc_now,
)
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
        self.assertEqual(state.status, "WIN")
        self.assertEqual(duplicate_state.wins, 1)
        self.assertEqual(history["wins"], 1)
        self.assertEqual(history["losses"], 0)
        self.assertEqual(history["profit"], 1.76)
        self.assertEqual(history["accuracy"], 100.0)
        self.assertEqual(len(history["trades"]), 1)
        self.assertEqual(history["trades"][0]["result"], "WIN")
        finished_at = state.last_trade["finished_at"]
        self.assertIsNotNone(finished_at)
        self.assertIsNone(state.next_cycle_at)
        self.assertEqual(state.result_received_at, parse_datetime(finished_at))
        self.assertEqual(state.result_display_until, parse_datetime(finished_at) + timedelta(seconds=5))
        payload = state.to_dict()
        self.assertEqual(payload["status"], "WIN")
        self.assertIsNotNone(payload["result_received_at"])
        self.assertIsNotNone(payload["result_display_until"])
        self.assertFalse(payload["result_waiting"])
        self.assertFalse(payload["operation_in_progress"])
        self.assertFalse(payload["entry_window_open"])
        self.assertEqual(payload["seconds_until_next_cycle"], 0)

        state.result_display_until = utc_now() - timedelta(seconds=1)
        waiting_payload = state.to_dict()
        self.assertEqual(waiting_payload["status"], STATUS_WAITING_NEXT_CYCLE)
        self.assertGreaterEqual(waiting_payload["seconds_until_next_cycle"], 299)
        self.assertLessEqual(waiting_payload["seconds_until_next_cycle"], 300)

    def test_loss_subtracts_entry_amount(self) -> None:
        trader = AutoTrader()
        trader.start("user-loss")
        trader.record_trade("user-loss", pending_trade("102", amount=2.0))

        finalized, state = trader.finish_trade("user-loss", "102", "LOSS", 0)

        self.assertTrue(finalized)
        self.assertEqual(state.losses, 1)
        self.assertEqual(state.profit, -2.0)
        self.assertEqual(state.last_trade["profit"], -2.0)

    def test_loss_triggers_single_gale_when_enabled(self) -> None:
        trader = AutoTrader()
        state = trader.start("user-gale")
        state.martingale_enabled = True
        state.martingale_multiplier = 2
        trader.record_trade("user-gale", pending_trade("201", amount=2.0))

        finalized, state = trader.finish_trade("user-gale", "201", "LOSS", 0)

        self.assertFalse(finalized)
        self.assertEqual(state.status, STATUS_WAITING_GALE_ENTRY)
        self.assertEqual(state.losses, 0)
        self.assertTrue(state.gale_pending)
        self.assertTrue(state.gale_active)
        self.assertEqual(state.gale_step, 1)
        self.assertEqual(state.gale_amount, 4.0)
        self.assertEqual(state.gale_direction, "CALL")
        self.assertEqual(state.gale_original_order_id, "201")
        self.assertEqual(state.pending_signal["symbol"], "EURUSD-OTC")
        self.assertEqual(state.pending_signal["signal"], "CALL")
        self.assertEqual(state.pending_signal["gale_amount"], 4.0)
        payload = state.to_dict()
        self.assertTrue(payload["martingale_enabled"])
        self.assertTrue(payload["gale_pending"])
        self.assertEqual(payload["gale_step"], 1)
        self.assertEqual(payload["gale_amount"], 4.0)
        self.assertTrue(payload["gale_active"])
        self.assertEqual(payload["gale_direction"], "CALL")
        self.assertEqual(payload["gale_original_order_id"], "201")
        self.assertIsNotNone(payload["gale_parent_trade"])
        self.assertEqual(trader.history("user-gale")["trades"][0]["result"], "LOSS")
        self.assertIsNone(trader.history("user-gale")["trades"][0]["final_result"])

    def test_gale_win_counts_final_win_and_returns_result_status(self) -> None:
        trader = AutoTrader()
        state = trader.start("user-gale-win")
        state.martingale_enabled = True
        state.martingale_multiplier = 2
        trader.record_trade("user-gale-win", pending_trade("301", amount=2.0))
        trader.finish_trade("user-gale-win", "301", "LOSS", 0)
        trader.record_trade(
            "user-gale-win",
            {
                **pending_trade("302", amount=4.0),
                "is_gale": True,
                "gale_step": 1,
                "parent_order_id": "301",
                "original_amount": 2.0,
                "gale_amount": 4.0,
            },
        )

        finalized, state = trader.finish_trade("user-gale-win", "302", "WIN", 3.52)

        self.assertTrue(finalized)
        self.assertEqual(state.status, "WIN")
        self.assertEqual(state.wins, 1)
        self.assertEqual(state.losses, 0)
        self.assertEqual(state.cycle_result, "WIN")
        self.assertEqual(state.last_trade["parent_order_id"], "301")
        self.assertTrue(state.last_trade["is_gale"])
        self.assertEqual(state.last_trade["final_result"], "WIN")
        self.assertEqual(state.last_trade["original_amount"], 2.0)
        self.assertEqual(state.last_trade["gale_amount"], 4.0)
        self.assertEqual(state.profit, 1.52)

    def test_gale_loss_counts_single_final_loss(self) -> None:
        trader = AutoTrader()
        state = trader.start("user-gale-loss")
        state.martingale_enabled = True
        state.martingale_multiplier = 2
        trader.record_trade("user-gale-loss", pending_trade("401", amount=2.0))
        trader.finish_trade("user-gale-loss", "401", "LOSS", 0)
        gale_state = trader.record_trade(
            "user-gale-loss",
            {
                **pending_trade("402", amount=4.0),
                "is_gale": True,
                "gale_step": 1,
                "parent_order_id": "401",
                "original_amount": 2.0,
                "gale_amount": 4.0,
            },
        )

        self.assertEqual(gale_state.status, STATUS_PENDING_GALE_RESULT)
        finalized, state = trader.finish_trade("user-gale-loss", "402", "LOSS", 0)

        self.assertTrue(finalized)
        self.assertEqual(state.losses, 1)
        self.assertEqual(state.wins, 0)
        self.assertEqual(state.profit, -6.0)
        self.assertEqual(state.cycle_result, "LOSS")
        self.assertEqual(state.last_trade["final_result"], "LOSS")
        self.assertEqual(state.last_trade["profit"], -4.0)

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

    def test_next_operation_is_blocked_until_cycle_delay_finishes(self) -> None:
        trader = AutoTrader()
        state = trader.start("user-cycle-delay")
        state.cycle_minutes = 10
        trader.record_trade("user-cycle-delay", pending_trade("cycle-delay"))
        trader.finish_trade("user-cycle-delay", "cycle-delay", "LOSS", -2)

        can_run, waiting = trader.prepare_cycle("user-cycle-delay")

        self.assertFalse(can_run)
        self.assertEqual(waiting.status, "LOSS")
        waiting.result_display_until = utc_now() - timedelta(seconds=1)
        can_run, waiting = trader.prepare_cycle("user-cycle-delay")
        self.assertFalse(can_run)
        self.assertEqual(waiting.status, STATUS_WAITING_NEXT_CYCLE)
        waiting.next_cycle_at = utc_now() - timedelta(seconds=1)

        can_run, waiting_analysis = trader.prepare_cycle("user-cycle-delay")

        self.assertTrue(can_run)
        self.assertEqual(waiting_analysis.status, STATUS_WAITING_NEXT_CYCLE)

    def test_reset_cycle_after_result_clears_attempts_and_pending_state(self) -> None:
        trader = AutoTrader()
        user_id = "user-reset-after-result"
        state = trader.start(user_id)
        state.order_attempts = 3
        state.fallback_candidate_used = True
        state.pending_signal = {"symbol": "EURUSD-OTC", "signal": "CALL"}
        state.last_signal = dict(state.pending_signal)
        state.operation_in_progress = True
        state.status = "WIN"
        state.result_display_until = utc_now() - timedelta(seconds=1)

        reset = trader.reset_cycle_after_result(user_id)

        self.assertFalse(reset.operation_in_progress)
        self.assertIsNone(reset.pending_signal)
        self.assertIsNone(reset.last_signal)
        self.assertEqual(reset.order_attempts, 0)
        self.assertFalse(reset.fallback_candidate_used)
        self.assertIsNone(reset.analysis_started_at)
        self.assertIsNone(reset.sync_started_at)
        self.assertIsNotNone(reset.cycle_id)
        self.assertIsNotNone(reset.next_cycle_at)

    def test_syncing_timeout_recovers_to_analysis_when_connected_and_enabled(self) -> None:
        trader = AutoTrader()
        state = trader.start("user-sync")
        state.connected = True
        state.status = STATUS_SYNCING
        state.sync_started_at = utc_now() - timedelta(seconds=31)

        recovered, state = trader.recover_sync_timeout("user-sync")

        self.assertTrue(recovered)
        self.assertEqual(state.status, STATUS_ANALYZING)
        self.assertEqual(state.analysis_result, "RUNNING")

    def test_syncing_timeout_disconnects_when_connection_is_down(self) -> None:
        trader = AutoTrader()
        state = trader.start("user-sync-down")
        state.connected = False
        state.status = STATUS_SYNCING
        state.sync_started_at = utc_now() - timedelta(seconds=31)

        recovered, state = trader.recover_sync_timeout("user-sync-down")

        self.assertTrue(recovered)
        self.assertEqual(state.status, STATUS_ACCOUNT_DISCONNECTED)
        self.assertFalse(state.enabled)


class TradeResultMonitorTests(unittest.IsolatedAsyncioTestCase):
    def test_default_poll_interval_is_realtime(self) -> None:
        monitor = TradeResultMonitor(AsyncMock(), AsyncMock(), AsyncMock())
        self.assertEqual(monitor.poll_seconds, 0.5)

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
        monitor = TradeResultMonitor(fetch, finish, timeout, poll_seconds=0.5, timeout_seconds=1)

        self.assertTrue(monitor.start("user-monitor", "104"))
        self.assertFalse(monitor.start("user-monitor", "104"))
        with patch("backend.trade_result_monitor.asyncio.sleep", new=AsyncMock()):
            await asyncio.gather(*list(monitor._tasks.values()))

        self.assertEqual(fetch.await_count, 2)
        finish.assert_awaited_once_with("user-monitor", "104", "WIN", 1.76)
        timeout.assert_not_awaited()

    async def test_monitor_fetches_immediately_instead_of_waiting_expiration(self) -> None:
        fetch = AsyncMock(
            return_value=(200, {"ok": True, "data": {"result": "win", "profit": 1.5}, "error": None})
        )
        finish = AsyncMock()
        timeout = AsyncMock()
        monitor = TradeResultMonitor(fetch, finish, timeout, poll_seconds=1, timeout_seconds=1)
        expires_at = utc_now() + timedelta(seconds=10)

        with patch("backend.trade_result_monitor.asyncio.sleep", new=AsyncMock()) as sleep:
            monitor.start("user-monitor-delay", "106", expires_at.isoformat())
            await asyncio.gather(*list(monitor._tasks.values()))

        sleep.assert_not_awaited()
        fetch.assert_awaited_once_with("user-monitor-delay", "106")
        finish.assert_awaited_once_with("user-monitor-delay", "106", "WIN", 1.5)

    async def test_monitor_times_out_and_releases_trade(self) -> None:
        fetch = AsyncMock(return_value=(200, {"ok": True, "data": {"result": "PENDING_RESULT"}, "error": None}))
        finish = AsyncMock()
        timeout = AsyncMock()
        monitor = TradeResultMonitor(fetch, finish, timeout, poll_seconds=0.5, timeout_seconds=0.001)

        monitor.start("user-monitor", "105")
        with patch("backend.trade_result_monitor.asyncio.sleep", new=AsyncMock()):
            await asyncio.gather(*list(monitor._tasks.values()))

        finish.assert_not_awaited()
        timeout.assert_awaited_once_with("user-monitor", "105")

    async def test_monitor_uses_expiration_timeout_instead_of_default_ceiling(self) -> None:
        fetch = AsyncMock(return_value=(200, {"ok": True, "data": {"result": "PENDING_RESULT"}, "error": None}))
        finish = AsyncMock()
        timeout = AsyncMock()
        monitor = TradeResultMonitor(fetch, finish, timeout_seconds=2100, timeout_trade=timeout, poll_seconds=0)
        expires_at = utc_now() - timedelta(seconds=1)

        with (
            patch("backend.trade_result_monitor.monotonic", side_effect=[0, 0, 31]),
            patch("backend.trade_result_monitor.asyncio.sleep", new=AsyncMock()),
        ):
            monitor.start("user-monitor-expiration", "107", expires_at.isoformat())
            await asyncio.gather(*list(monitor._tasks.values()))

        fetch.assert_awaited_once_with("user-monitor-expiration", "107")
        finish.assert_not_awaited()
        timeout.assert_awaited_once_with("user-monitor-expiration", "107")
