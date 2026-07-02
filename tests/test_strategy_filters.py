import json
import unittest
from datetime import timedelta
from unittest.mock import AsyncMock, patch

from backend import main
from backend.auto_trader import AutoTrader, utc_now


class StrategyFilterTests(unittest.TestCase):
    def setUp(self) -> None:
        main.auto_trader = AutoTrader()

    def test_trend_clear_reduces_score_without_blocking(self) -> None:
        signal = {
            "symbol": "EURUSD-OTC",
            "signal": "CALL",
            "confidence": 95,
            "trend": "SIDEWAYS",
            "strength": 5,
        }

        allowed, selected, _ = main.apply_strategy_guard(
            "user-trend",
            main.auto_trader.start("user-trend"),
            signal,
            payout=90,
        )

        self.assertTrue(allowed)
        self.assertTrue(selected["trade_allowed"])
        self.assertIn("TREND_CLEAR", selected["blocked_filters"])
        self.assertIn("SIDEWAYS_FILTER", selected["blocked_filters"])
        self.assertLess(selected["strategy_score"], selected["confidence"])

    def test_neutral_rsi_reduces_score_without_blocking(self) -> None:
        signal = {
            "symbol": "EURUSD-OTC",
            "signal": "CALL",
            "confidence": 95,
            "trend": "UP",
            "strength": 35,
            "ema9": 1.02,
            "ema21": 1.01,
            "rsi": 50,
            "body_ratio": 0.7,
            "upper_wick_ratio": 0.1,
            "lower_wick_ratio": 0.1,
            "atr_pct": 0.001,
            "directional_candles_5": 4,
            "alternating_last_3": False,
        }

        allowed, selected, _ = main.apply_strategy_guard(
            "user-rsi",
            main.auto_trader.start("user-rsi"),
            signal,
            payout=90,
        )

        self.assertTrue(allowed)
        self.assertIn("RSI_RANGE", selected["blocked_filters"])
        self.assertLess(selected["strategy_score"], selected["confidence"])

    def test_wick_against_direction_reduces_score_without_blocking(self) -> None:
        signal = {
            "symbol": "EURUSD-OTC",
            "signal": "CALL",
            "confidence": 95,
            "trend": "UP",
            "strength": 35,
            "ema9": 1.02,
            "ema21": 1.01,
            "rsi": 60,
            "body_ratio": 0.6,
            "upper_wick_ratio": 0.6,
            "lower_wick_ratio": 0.1,
            "atr_pct": 0.001,
            "directional_candles_5": 4,
            "alternating_last_3": False,
        }

        allowed, selected, _ = main.apply_strategy_guard(
            "user-wick",
            main.auto_trader.start("user-wick"),
            signal,
            payout=90,
        )

        self.assertTrue(allowed)
        self.assertIn("WICK_REJECTION", selected["blocked_filters"])
        self.assertLess(selected["strategy_score"], selected["confidence"])

    def test_two_consecutive_losses_reduce_asset_score(self) -> None:
        user_id = "user-cooldown"
        trader = main.auto_trader
        state = trader.start(user_id)
        for index in range(2):
            trader.record_trade(
                user_id,
                {
                    "order_id": f"loss-{index}",
                    "active": "EURUSD-OTC",
                    "direction": "CALL",
                    "amount": 2,
                    "sent_at": (utc_now() - timedelta(minutes=1)).isoformat(),
                },
            )
            trader.finish_trade(user_id, f"loss-{index}", "LOSS", -2)

        allowed, selected, _ = main.apply_strategy_guard(
            user_id,
            state,
            {
                "symbol": "EURUSD-OTC",
                "signal": "CALL",
                "confidence": 95,
                "trade_allowed": True,
                "blocked_filters": [],
                "approved_filters": [],
                "quality_score": 95,
            },
            payout=90,
        )

        self.assertTrue(allowed)
        self.assertIn("ASSET_COOLDOWN", selected["blocked_filters"])
        self.assertLess(selected["strategy_score"], selected["confidence"])

    def test_daily_stop_loss_stops_robot(self) -> None:
        user_id = "user-daily-loss"
        trader = main.auto_trader
        state = trader.start(user_id)
        state.stop_loss = 4
        for index in range(2):
            trader.record_trade(
                user_id,
                {"order_id": f"daily-loss-{index}", "active": "EURUSD-OTC", "amount": 2},
            )
            trader.finish_trade(user_id, f"daily-loss-{index}", "LOSS", -2)

        self.assertEqual(main.daily_stop_reason(user_id, state), "STOP_LOSS_HIT")

    def test_daily_stop_win_stops_robot(self) -> None:
        user_id = "user-daily-win"
        trader = main.auto_trader
        state = trader.start(user_id)
        state.stop_win = 3
        trader.record_trade(
            user_id,
            {"order_id": "daily-win-loss-1", "active": "EURUSD-OTC", "amount": 2},
        )
        trader.finish_trade(user_id, "daily-win-loss-1", "LOSS", -2)
        trader.record_trade(
            user_id,
            {"order_id": "daily-win-1", "active": "EURUSD-OTC", "amount": 2},
        )
        trader.finish_trade(user_id, "daily-win-1", "WIN", 3)

        self.assertEqual(main.daily_stop_reason(user_id, state), "STOP_WIN_HIT")

    def test_daily_management_summary_uses_gross_profit_and_loss(self) -> None:
        user_id = "user-daily-management"
        trader = main.auto_trader
        state = trader.start(user_id)
        state.stop_win = 4
        state.stop_loss = 3
        trader.record_trade(
            user_id,
            {"order_id": "daily-management-loss", "active": "EURUSD-OTC", "amount": 3},
        )
        trader.finish_trade(user_id, "daily-management-loss", "LOSS", -3)
        trader.record_trade(
            user_id,
            {"order_id": "daily-management-win", "active": "EURUSD-OTC", "amount": 4},
        )
        trader.finish_trade(user_id, "daily-management-win", "WIN", 4)

        summary = main.build_management_summary(user_id, state)

        self.assertEqual(summary["gross_profit"], 4.0)
        self.assertEqual(summary["gross_loss"], 3.0)
        self.assertEqual(summary["net_profit"], 1.0)


class StrategyRejectedCycleTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        main.auto_trader = AutoTrader()

    async def test_low_confidence_candidate_stays_triggered_waiting_entry_window(self) -> None:
        user_id = "user-low-confidence"
        state = main.auto_trader.start(user_id)
        state.next_cycle_at = utc_now() - timedelta(seconds=1)

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
            if path == "/sessions/status":
                return 200, main.build_success(
                    {"connected": True, "active_mode": "PRACTICE", "server_time": 20.0}
                )
            if path == "/payouts":
                return 200, main.build_success([{"symbol": "EURUSD-OTC", "payout": 90}])
            raise AssertionError(f"unexpected path: {path}")

        scan_payload = main.build_success(
            [
                {
                    "symbol": "EURUSD-OTC",
                    "signal": "CALL",
                    "confidence": 80,
                    "strength": 10,
                    "payout": 90,
                    "trade_allowed": False,
                    "blocked_filters": ["TREND_CLEAR", "MIN_CONFIDENCE"],
                    "approved_filters": [],
                    "quality_score": 40,
                    "quality_reason": "TREND_CLEAR",
                }
            ]
        )

        with (
            patch.object(main, "call_bullex_service", side_effect=fake_bullex),
            patch.object(main, "scan_local_signals", new=AsyncMock(return_value=(200, scan_payload))),
            self.assertLogs("backend-gateway", level="INFO") as logs,
        ):
            status_code, payload = await main.execute_robot_cycle(user_id)

        data = payload["data"]
        pending = data["pending_signal"]
        self.assertEqual(status_code, 200)
        self.assertEqual(data["status"], "WAITING_NEXT_CANDLE_ENTRY")
        self.assertIsNone(data["rejection_reason"])
        self.assertEqual(data["last_analysis_result"], "BEST_CANDIDATE_SELECTED")
        self.assertIsNotNone(pending)
        self.assertEqual(pending["symbol"], "EURUSD-OTC")
        self.assertEqual(pending["signal"], "CALL")
        self.assertEqual(pending["confidence"], 80)
        self.assertEqual(pending["payout"], 90)
        self.assertEqual(pending["strategy_score"], 70)
        self.assertEqual(pending["score"], 70)
        self.assertLess(pending["strategy_score"], pending["confidence"])
        self.assertIn("TREND_CLEAR", pending["blocked_filters"])
        self.assertIn("MIN_CONFIDENCE", pending["blocked_filters"])
        self.assertEqual(pending["target_entry_second"], 0)
        self.assertEqual(pending["entry_window_start_second"], 0)
        self.assertEqual(pending["entry_window_end_second"], 3)
        output = "\n".join(logs.output)
        self.assertIn("[ANALYSIS_CANDIDATES]", output)
        self.assertIn("[BEST_CANDIDATE_SELECTED]", output)
        self.assertIn("[CYCLE_FINISHED_SIGNAL_LOCKED]", output)

    async def test_open_analysis_window_forces_analysis_from_robot_state(self) -> None:
        user_id = "user-force-open-window"
        state = main.auto_trader.start(user_id)
        state.status = "WAITING_ANALYSIS_WINDOW"
        state.next_cycle_at = utc_now() + timedelta(seconds=40)

        session_payload = main.build_success(
            {"connected": True, "active_mode": "PRACTICE", "server_time": 10.0}
        )
        forced_result = (200, main.build_robot_payload(state))
        with (
            patch.object(
                main,
                "call_bullex_service",
                new=AsyncMock(return_value=(200, session_payload)),
            ),
            patch.object(
                main,
                "run_analysis_now",
                new=AsyncMock(return_value=forced_result),
            ) as forced_analysis,
        ):
            response = await main.robot_state({"user_id": user_id})

        self.assertEqual(response.status_code, 200)
        forced_analysis.assert_awaited_once_with(user_id)

    async def test_scan_error_uses_open_asset_fallback_and_creates_pending_signal(self) -> None:
        user_id = "user-fallback-signal"
        state = main.auto_trader.start(user_id)
        state.next_cycle_at = utc_now() - timedelta(seconds=1)

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
            if path == "/sessions/status":
                return 200, main.build_success(
                    {"connected": True, "active_mode": "PRACTICE", "server_time": 10.0}
                )
            if path == "/payouts":
                symbol = (params or {}).get("active")
                data = [{"symbol": symbol, "payout": 90}] if symbol == "EURUSD-OTC" else []
                return 200, main.build_success(data)
            if path == "/candles":
                return 200, main.build_success(
                    [{"close": 1.0}, {"close": 1.1}, {"close": 1.2}]
                )
            raise AssertionError(f"unexpected path: {path}")

        with (
            patch.object(main, "call_bullex_service", side_effect=fake_bullex),
            patch.object(
                main,
                "scan_local_signals",
                new=AsyncMock(return_value=(500, main.build_error("SCAN_FAILED"))),
            ),
            self.assertLogs("backend-gateway", level="INFO") as logs,
        ):
            status_code, payload = await main.execute_robot_cycle(user_id)

        data = payload["data"]
        self.assertEqual(status_code, 200)
        self.assertEqual(data["status"], "WAITING_NEXT_CANDLE_ENTRY")
        self.assertEqual(data["pending_signal"]["symbol"], "EURUSD-OTC")
        self.assertEqual(data["pending_signal"]["direction"], "CALL")
        self.assertIsNotNone(data["best_candidate"])
        output = "\n".join(logs.output)
        self.assertIn("[FALLBACK_CANDIDATE_SELECTED]", output)
        self.assertIn("[WAITING_NEXT_CANDLE_ENTRY]", output)

    async def test_missing_candles_still_blocks_candidate(self) -> None:
        user_id = "user-no-candles"
        state = main.auto_trader.start(user_id)
        state.next_cycle_at = utc_now() - timedelta(seconds=1)

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
            if path == "/sessions/status":
                return 200, main.build_success(
                    {"connected": True, "active_mode": "PRACTICE", "server_time": 20.0}
                )
            raise AssertionError(f"unexpected path: {path}")

        scan_payload = main.build_success(
            [
                {
                    "symbol": "EURUSD-OTC",
                    "signal": "WAIT",
                    "confidence": 95,
                    "strength": 95,
                    "payout": 90,
                    "blocked_filters": ["CANDLES_UNAVAILABLE"],
                    "approved_filters": [],
                    "quality_score": 95,
                }
            ]
        )

        with (
            patch.object(main, "call_bullex_service", side_effect=fake_bullex),
            patch.object(main, "scan_local_signals", new=AsyncMock(return_value=(200, scan_payload))),
        ):
            status_code, payload = await main.execute_robot_cycle(user_id)

        data = payload["data"]
        self.assertEqual(status_code, 200)
        self.assertEqual(data["status"], "WAITING_ANALYSIS_WINDOW")
        self.assertEqual(data["analysis_result"], "NO_CANDIDATE_THIS_CANDLE")
        self.assertEqual(data["last_rejection_reason"], "CANDLES_UNAVAILABLE")
        self.assertIn("CANDLES_UNAVAILABLE", data["blocked_filters"])
        self.assertIsNone(data["pending_signal"])
        self.assertGreaterEqual(data["seconds_until_analysis_window"], 44)

    async def test_missing_payout_still_blocks_candidate(self) -> None:
        user_id = "user-no-payout"
        state = main.auto_trader.start(user_id)
        state.next_cycle_at = utc_now() - timedelta(seconds=1)

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
            if path == "/sessions/status":
                return 200, main.build_success(
                    {"connected": True, "active_mode": "PRACTICE", "server_time": 20.0}
                )
            if path == "/payouts":
                return 500, main.build_error("PAYOUT_UNAVAILABLE")
            raise AssertionError(f"unexpected path: {path}")

        scan_payload = main.build_success(
            [{"symbol": "EURUSD-OTC", "signal": "CALL", "confidence": 95, "strength": 95}]
        )

        with (
            patch.object(main, "call_bullex_service", side_effect=fake_bullex),
            patch.object(main, "scan_local_signals", new=AsyncMock(return_value=(200, scan_payload))),
        ):
            status_code, payload = await main.execute_robot_cycle(user_id)

        data = payload["data"]
        self.assertEqual(status_code, 200)
        self.assertEqual(data["status"], "WAITING_ANALYSIS_WINDOW")
        self.assertEqual(data["analysis_result"], "NO_CANDIDATE_THIS_CANDLE")
        self.assertEqual(data["last_rejection_reason"], "ACTIVE_CLOSED")
        self.assertIn("PAYOUT_UNAVAILABLE", data["blocked_filters"])
        self.assertIsNone(data["pending_signal"])

    async def test_closed_asset_still_blocks_candidate(self) -> None:
        user_id = "user-closed-asset"
        state = main.auto_trader.start(user_id)
        state.next_cycle_at = utc_now() - timedelta(seconds=1)

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
            if path == "/sessions/status":
                return 200, main.build_success(
                    {"connected": True, "active_mode": "PRACTICE", "server_time": 20.0}
                )
            if path == "/payouts":
                return 200, main.build_success([{"symbol": "EURUSD-OTC", "payout": 90}])
            raise AssertionError(f"unexpected path: {path}")

        scan_payload = main.build_success(
            [
                {
                    "symbol": "EURUSD-OTC",
                    "signal": "CALL",
                    "confidence": 95,
                    "strength": 95,
                    "payout": 90,
                    "is_open": False,
                }
            ]
        )

        with (
            patch.object(main, "call_bullex_service", side_effect=fake_bullex),
            patch.object(main, "scan_local_signals", new=AsyncMock(return_value=(200, scan_payload))),
        ):
            status_code, payload = await main.execute_robot_cycle(user_id)

        data = payload["data"]
        self.assertEqual(status_code, 200)
        self.assertEqual(data["status"], "WAITING_ANALYSIS_WINDOW")
        self.assertEqual(data["analysis_result"], "NO_CANDIDATE_THIS_CANDLE")
        self.assertEqual(data["last_rejection_reason"], "ACTIVE_CLOSED")
        self.assertIn("ACTIVE_CLOSED", data["blocked_filters"])
        self.assertIsNone(data["pending_signal"])

    async def test_low_quality_rejection_returns_waiting_after_five_seconds(self) -> None:
        user_id = "user-low-quality-state"
        state = main.auto_trader.start(user_id)
        state = main.auto_trader.reject_no_valid_signal(
            user_id,
            last_rejection_reason="TREND_CLEAR",
            blocked_filters=["TREND_CLEAR"],
            quality_score=40,
        )
        state.rejected_at = utc_now() - timedelta(seconds=6)
        state.next_cycle_at = utc_now() + timedelta(minutes=9, seconds=54)

        with (
            patch.object(
                main,
                "call_bullex_service",
                new=AsyncMock(
                    return_value=(
                        200,
                        main.build_success(
                            {"connected": True, "active_mode": "PRACTICE", "server_time": 10.0}
                        ),
                    )
                ),
            ),
            patch.object(main, "sync_user_store_from_payload"),
        ):
            response = await main.robot_state({"user_id": user_id})

        data = json.loads(response.body)["data"]
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data["status"], "WAITING_NEXT_CYCLE")
        self.assertIsNone(data["rejection_reason"])
        self.assertEqual(data["last_rejection_reason"], "TREND_CLEAR")
        self.assertGreaterEqual(data["seconds_until_next_cycle"], 590)
        self.assertIsNone(data["pending_signal"])
