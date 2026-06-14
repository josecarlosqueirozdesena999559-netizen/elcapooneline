import json
import unittest
from datetime import timedelta
from unittest.mock import AsyncMock, patch

from backend import main
from backend.auto_trader import AutoTrader, utc_now
from backend.signal_engine import analyze_signal


def candle(open_price: float, close_price: float, low: float | None = None, high: float | None = None) -> dict:
    return {
        "open": open_price,
        "close": close_price,
        "min": min(open_price, close_price) if low is None else low,
        "max": max(open_price, close_price) if high is None else high,
        "volume": 1,
    }


class StrategyFilterTests(unittest.TestCase):
    def setUp(self) -> None:
        main.auto_trader = AutoTrader()

    def test_sideways_signal_is_blocked(self) -> None:
        candles = [candle(1.0, 1.0, 0.9999, 1.0001) for _ in range(40)]

        signal = analyze_signal("EURUSD-OTC", candles, payout=90)

        self.assertFalse(signal["trade_allowed"])
        self.assertIn("SIDEWAYS_FILTER", signal["blocked_filters"])
        self.assertIn("Sinal bloqueado", signal["signal_explanation"])
        self.assertEqual(signal["narrator_text"], signal["signal_explanation"])

    def test_neutral_rsi_is_blocked(self) -> None:
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

        self.assertFalse(allowed)
        self.assertIn("RSI_RANGE", selected["blocked_filters"])

    def test_wick_against_direction_is_blocked(self) -> None:
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

        self.assertFalse(allowed)
        self.assertIn("WICK_REJECTION", selected["blocked_filters"])

    def test_two_consecutive_losses_block_asset_for_thirty_minutes(self) -> None:
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

        self.assertFalse(allowed)
        self.assertIn("ASSET_COOLDOWN", selected["blocked_filters"])

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

        self.assertEqual(main.daily_stop_reason(user_id, state), "DAILY_STOP_LOSS_REACHED")

    def test_daily_stop_win_stops_robot(self) -> None:
        user_id = "user-daily-win"
        trader = main.auto_trader
        state = trader.start(user_id)
        state.stop_win = 3
        trader.record_trade(
            user_id,
            {"order_id": "daily-win-1", "active": "EURUSD-OTC", "amount": 2},
        )
        trader.finish_trade(user_id, "daily-win-1", "WIN", 3)

        self.assertEqual(main.daily_stop_reason(user_id, state), "DAILY_STOP_WIN_REACHED")


class StrategyRejectedCycleTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        main.auto_trader = AutoTrader()

    async def test_low_quality_block_schedules_next_cycle_and_clears_signal(self) -> None:
        user_id = "user-low-quality"
        state = main.auto_trader.start(user_id)
        state.next_cycle_at = utc_now() - timedelta(seconds=1)

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
            if path == "/sessions/status":
                return 200, main.build_success(
                    {"connected": True, "active_mode": "PRACTICE", "server_time": 56.0}
                )
            raise AssertionError(f"unexpected path: {path}")

        scan_payload = main.build_success(
            [
                {
                    "symbol": "EURUSD-OTC",
                    "signal": "CALL",
                    "confidence": 92,
                    "strength": 10,
                    "payout": 90,
                    "trade_allowed": False,
                    "blocked_filters": ["TREND_CLEAR"],
                    "approved_filters": ["MIN_CONFIDENCE"],
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
        self.assertEqual(status_code, 200)
        self.assertEqual(data["status"], "SIGNAL_REJECTED")
        self.assertEqual(data["rejection_reason"], "SIGNAL_BLOCKED_LOW_QUALITY")
        self.assertEqual(data["last_rejection_reason"], "TREND_CLEAR")
        self.assertEqual(data["blocked_filters"], ["TREND_CLEAR"])
        self.assertEqual(data["quality_score"], 40)
        self.assertIsNone(data["pending_signal"])
        self.assertGreaterEqual(data["seconds_until_next_cycle"], 599)
        output = "\n".join(logs.output)
        self.assertIn("[SIGNAL_REJECTED]", output)
        self.assertIn("[NEXT_CYCLE_SCHEDULED]", output)

    async def test_low_quality_rejection_returns_waiting_after_five_seconds(self) -> None:
        user_id = "user-low-quality-state"
        state = main.auto_trader.start(user_id)
        state = main.auto_trader.reject_strategy(
            user_id,
            "SIGNAL_BLOCKED_LOW_QUALITY",
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
