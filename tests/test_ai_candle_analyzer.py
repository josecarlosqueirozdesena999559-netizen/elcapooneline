import json
import unittest
from datetime import timedelta
from unittest.mock import AsyncMock, patch

from backend import main
from backend.auto_trader import AutoTrader, utc_now


def make_signal(symbol: str = "EURUSD-OTC") -> dict[str, object]:
    return {
        "symbol": symbol,
        "signal": "CALL",
        "direction": "CALL",
        "confidence": 95,
        "trend": "UP",
        "strength": 35,
        "ema9": 1.102,
        "ema21": 1.101,
        "rsi": 61,
        "rsi14": 61,
        "atr": 0.0008,
        "atr_pct": 0.001,
        "body_ratio": 0.7,
        "upper_wick_ratio": 0.1,
        "lower_wick_ratio": 0.1,
        "directional_candles_5": 4,
        "alternating_last_3": False,
        "payout": 90,
        "reason": "Confluencia local forte.",
        "entry_reason": "Confluencia local forte.",
        "candle_reading": "Leitura local forte.",
        "metrics": {"volatility": "NORMAL", "atr": 0.0008},
    }


def make_candles() -> list[dict[str, float]]:
    candles = []
    base = 1.1
    for index in range(60):
        candles.append(
            {
                "time": 1700000000 + index * 60,
                "open": base + index * 0.0001,
                "high": base + index * 0.0001 + 0.0003,
                "low": base + index * 0.0001 - 0.0002,
                "close": base + index * 0.0001 + 0.00015,
            }
        )
    return candles


class AICandleAnalyzerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        main.auto_trader = AutoTrader()

    async def test_ai_approval_updates_best_candidate_state(self) -> None:
        user_id = "user-ai-approved"
        state = main.auto_trader.start(user_id)
        state.ai_analysis_enabled = True
        state.ai_confirmation_required = True
        entry_window = main.get_entry_window("M1", 20.0)

        with (
            patch.object(
                main,
                "scan_local_signals",
                new=AsyncMock(return_value=(200, main.build_success([make_signal()]))),
            ),
            patch.object(main, "fetch_ai_candles", new=AsyncMock(return_value=make_candles())),
            patch.object(
                main,
                "analyze_candle_candidate",
                new=AsyncMock(
                    return_value={
                        "ok": True,
                        "reason": None,
                        "cached": False,
                        "result": {
                            "approved": True,
                            "direction": "CALL",
                            "confidence": 84,
                            "risk_level": "LOW",
                            "entry_reason": "Leitura compradora alinhada.",
                            "voice_text": "EMA e RSI confirmam continuidade de alta.",
                            "block_reason": None,
                            "candle_reading": "Candles com corpo comprador e pouca rejeicao.",
                            "strategy_alignment": "A estrategia local esta alinhada com a leitura dos candles.",
                        },
                    }
                ),
            ),
        ):
            updated = await main.update_cycle_analysis(user_id, state, entry_window, force=True)

        self.assertEqual(updated.best_candidate["ai_approved"], True)
        self.assertEqual(updated.best_candidate["ai_confidence"], 84)
        self.assertEqual(updated.ai_risk_level, "LOW")
        self.assertEqual(updated.ai_voice_text, "EMA e RSI confirmam continuidade de alta.")
        self.assertIn("alinhada", updated.ai_strategy_alignment.lower())

    async def test_ai_timeout_keeps_local_candidate(self) -> None:
        user_id = "user-ai-timeout"
        state = main.auto_trader.start(user_id)
        state.ai_analysis_enabled = True
        candidate = make_signal()

        with (
            patch.object(main, "fetch_ai_candles", new=AsyncMock(return_value=make_candles())),
            patch.object(
                main,
                "analyze_candle_candidate",
                new=AsyncMock(return_value={"ok": False, "reason": "timeout", "result": None, "cached": False}),
            ),
        ):
            updated = await main.maybe_analyze_best_candidate_with_ai(user_id, state, candidate)

        self.assertEqual(updated["symbol"], "EURUSD-OTC")
        self.assertIsNone(updated.get("ai_approved"))
        self.assertEqual(updated["direction"], "CALL")

    async def test_ai_error_keeps_local_candidate(self) -> None:
        user_id = "user-ai-error"
        state = main.auto_trader.start(user_id)
        state.ai_analysis_enabled = True
        candidate = make_signal()

        with (
            patch.object(main, "fetch_ai_candles", new=AsyncMock(return_value=make_candles())),
            patch.object(
                main,
                "analyze_candle_candidate",
                new=AsyncMock(
                    return_value={"ok": False, "reason": "request_error:boom", "result": None, "cached": False}
                ),
            ),
        ):
            updated = await main.maybe_analyze_best_candidate_with_ai(user_id, state, candidate)

        self.assertEqual(updated["symbol"], "EURUSD-OTC")
        self.assertIsNone(updated.get("ai_block_reason"))

    async def test_ai_block_prevents_order_when_confirmation_is_required(self) -> None:
        user_id = "user-ai-blocked"
        state = main.auto_trader.start(user_id)
        state.ai_analysis_enabled = True
        state.ai_confirmation_required = True
        state.next_cycle_at = utc_now() - timedelta(seconds=1)
        blocked_candidate = {
            **make_signal(),
            "ai_approved": False,
            "ai_confidence": 91,
            "ai_risk_level": "LOW",
            "ai_block_reason": "AI_DIRECTION_MISMATCH",
            "ai_voice_text": "A leitura da IA discorda da direcao local.",
            "blocked_filters": ["AI_ENTRY_BLOCKED", "AI_DIRECTION_MISMATCH"],
        }
        state.best_candidate = dict(blocked_candidate)
        state.candidates = [dict(blocked_candidate)]
        state.candidates_count = 1
        state.ai_block_reason = "AI_DIRECTION_MISMATCH"

        calls: list[str] = []

        async def fake_bullex(method, path, call_user_id, json_body=None, params=None):
            calls.append(path)
            if path == "/sessions/status":
                return 200, main.build_success(
                    {"connected": True, "active_mode": "PRACTICE", "server_time": 0.0}
                )
            raise AssertionError(f"unexpected path: {path}")

        with patch.object(main, "call_bullex_service", side_effect=fake_bullex):
            status_code, payload = await main.execute_robot_cycle(user_id)

        self.assertEqual(status_code, 200)
        self.assertEqual(payload["data"]["status"], "WAITING_NEXT_CYCLE")
        self.assertEqual(payload["data"]["last_analysis_result"], "AI_ENTRY_BLOCKED")
        self.assertNotIn("/orders/buy-demo", calls)

    async def test_robot_state_exposes_ai_voice_text_for_narration(self) -> None:
        user_id = "user-ai-voice"
        state = main.auto_trader.start(user_id)
        state.ai_analysis_enabled = True
        main.auto_trader.set_pending_signal(
            user_id,
            {
                **make_signal(),
                "ai_approved": True,
                "ai_confidence": 83,
                "ai_risk_level": "LOW",
                "ai_entry_reason": "Entrada apoiada por tendencia e momentum.",
                "ai_voice_text": "EMA e RSI confirmam a entrada de compra.",
                "ai_candle_reading": "Candles seguem compradores.",
                "ai_strategy_alignment": "IA e estrategia local estao alinhadas.",
            },
        )
        main.auto_trader.start_sending_order(user_id)

        with (
            patch.object(
                main,
                "call_bullex_service",
                new=AsyncMock(
                    return_value=(200, main.build_success({"connected": True, "active_mode": "PRACTICE"}))
                ),
            ),
            patch.object(main, "sync_user_store_from_payload"),
        ):
            response = await main.robot_state({"user_id": user_id})

        data = json.loads(response.body)["data"]
        self.assertEqual(data["ai_voice_text"], "EMA e RSI confirmam a entrada de compra.")
        self.assertIn("Motivo: EMA e RSI confirmam a entrada de compra.", data["voice_message"])
