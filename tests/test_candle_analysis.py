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
