import unittest

from backend.auto_trader import AutoTrader
from backend.signal_engine import analyze_signal


def make_candles(count: int = 40) -> list[dict[str, float]]:
    candles = []
    price = 1.1000
    for index in range(count):
        step = 0.00035 if index % 5 != 0 else -0.00008
        open_price = price
        close_price = price + step
        high = max(open_price, close_price) + 0.00005
        low = min(open_price, close_price) - 0.00008
        candles.append(
            {
                "open": round(open_price, 6),
                "close": round(close_price, 6),
                "max": round(high, 6),
                "min": round(low, 6),
                "volume": 1,
            }
        )
        price = close_price
    return candles


def make_support_reversal_candles() -> list[dict[str, float]]:
    candles = []
    price = 1.1000
    for index in range(35):
        step = 0.00012 if index % 3 else -0.00003
        open_price = price
        close_price = price + step
        candles.append(
            {
                "open": round(open_price, 6),
                "close": round(close_price, 6),
                "max": round(max(open_price, close_price) + 0.00004, 6),
                "min": round(min(open_price, close_price) - 0.00004, 6),
                "volume": 1,
            }
        )
        price = close_price
    price = candles[-1]["close"]
    tail = []
    for _ in range(5):
        open_price = price
        close_price = price - 0.0003
        tail.append(
            {
                "open": round(open_price, 6),
                "close": round(close_price, 6),
                "max": round(open_price + 0.00003, 6),
                "min": round(close_price - 0.00004, 6),
                "volume": 1,
            }
        )
        price = close_price
    open_price = price - 0.00005
    close_price = price + 0.00045
    tail.append(
        {
            "open": round(open_price, 6),
            "close": round(close_price, 6),
            "max": round(close_price + 0.00004, 6),
            "min": round(open_price - 0.00025, 6),
            "volume": 1,
        }
    )
    return candles[:-6] + tail


def make_bad_wick_candles() -> list[dict[str, float]]:
    candles = make_candles(39)
    price = candles[-1]["close"]
    candles.append(
        {
            "open": price,
            "close": price + 0.00008,
            "max": price + 0.0007,
            "min": price - 0.00005,
            "volume": 1,
        }
    )
    return candles


def make_resistance_chase_candles() -> list[dict[str, float]]:
    candles = []
    price = 1.1000
    for index in range(36):
        step = 0.00006 if index % 2 == 0 else -0.00002
        open_price = price
        close_price = price + step
        candles.append(
            {
                "open": round(open_price, 6),
                "close": round(close_price, 6),
                "max": round(max(open_price, close_price) + 0.00004, 6),
                "min": round(min(open_price, close_price) - 0.00004, 6),
                "volume": 1,
            }
        )
        price = close_price
    resistance = max(candle["max"] for candle in candles[-20:])
    open_price = resistance - 0.00018
    close_price = resistance - 0.00001
    candles.append(
        {
            "open": round(open_price, 6),
            "close": round(close_price, 6),
            "max": round(resistance + 0.00001, 6),
            "min": round(open_price - 0.00004, 6),
            "volume": 1,
        }
    )
    return candles


class CandleAnalysisTests(unittest.TestCase):
    def test_analysis_returns_direction_score_and_entry_reason(self) -> None:
        signal = analyze_signal(
            "EURUSD-OTC",
            make_candles(),
            timeframe="M1",
            strategy_mode="conservative",
            payout=90,
        )

        self.assertIn(signal["direction"], {"CALL", "PUT"})
        self.assertIsInstance(signal["score"], int)
        self.assertGreater(signal["score"], 0)
        self.assertTrue(signal["entry_reason"])
        self.assertTrue(signal["candle_reading"])
        self.assertIsInstance(signal["block_reasons"], list)
        self.assertIn("metrics", signal)
        self.assertIn("ema9", signal["metrics"])
        self.assertIn("rsi14", signal["metrics"])
        self.assertIn("price_action_setup", signal["metrics"])

    def test_support_reversal_setup_allows_reversal_entry(self) -> None:
        signal = analyze_signal(
            "EURUSD-OTC",
            make_support_reversal_candles(),
            timeframe="M1",
            strategy_mode="conservative",
            payout=90,
        )

        self.assertEqual(signal["direction"], "CALL")
        self.assertEqual(signal["price_action_setup"], "REVERSAL")
        self.assertTrue(signal["near_support_resistance"])
        self.assertTrue(signal["level_rejection_confirmed"])
        self.assertTrue(signal["zigzag_reversal"])
        self.assertTrue(signal["trade_allowed"])

    def test_call_into_resistance_without_rejection_is_blocked(self) -> None:
        signal = analyze_signal(
            "EURUSD-OTC",
            make_resistance_chase_candles(),
            timeframe="M1",
            strategy_mode="conservative",
            payout=90,
        )

        self.assertEqual(signal["direction"], "CALL")
        self.assertTrue(signal["level_conflict"])
        self.assertFalse(signal["trade_allowed"])
        self.assertIn("LEVEL_CONFLICT", signal["blocked_filters"])
        self.assertTrue(signal["metrics"]["near_resistance"])
        self.assertFalse(signal["metrics"]["near_support"])

    def test_large_wick_and_weak_body_block_entry(self) -> None:
        signal = analyze_signal(
            "EURUSD-OTC",
            make_bad_wick_candles(),
            timeframe="M1",
            strategy_mode="conservative",
            payout=90,
        )

        self.assertFalse(signal["trade_allowed"])
        self.assertIn("WICK_REJECTION", signal["blocked_filters"])
        self.assertIn("CANDLE_STRENGTH", signal["blocked_filters"])
        self.assertIn("PRICE_ACTION_SETUP", signal["blocked_filters"])

    def test_robot_state_exposes_candle_analysis_fields(self) -> None:
        trader = AutoTrader()
        state = trader.start("user-candle-state")
        signal = analyze_signal(
            "EURUSD-OTC",
            make_candles(),
            timeframe="M1",
            strategy_mode=state.strategy_mode,
            payout=90,
        )

        state = trader.set_pending_signal("user-candle-state", signal)
        payload = state.to_dict()

        self.assertEqual(payload["pending_signal"]["entry_reason"], signal["entry_reason"])
        self.assertEqual(payload["candle_reading"], signal["candle_reading"])
        self.assertEqual(payload["entry_reason"], signal["entry_reason"])
        self.assertEqual(payload["block_reasons"], signal["block_reasons"])
        self.assertEqual(payload["metrics"]["symbol"], "EURUSD-OTC")


if __name__ == "__main__":
    unittest.main()
