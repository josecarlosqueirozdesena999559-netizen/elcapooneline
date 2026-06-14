from datetime import datetime, timezone
from typing import Any, Literal


SignalValue = Literal["CALL", "PUT", "WAIT"]
TrendValue = Literal["UP", "DOWN", "SIDEWAYS"]
StrategyMode = Literal["aggressive", "balanced", "conservative"]


STRATEGY_PROFILES = {
    "aggressive": {"confidence": 70, "payout": 70, "strength": 8, "body_ratio": 0.25},
    "balanced": {"confidence": 80, "payout": 80, "strength": 12, "body_ratio": 0.45},
    "conservative": {"confidence": 90, "payout": 85, "strength": 20, "body_ratio": 0.55},
}


def analyze_signal(
    symbol: str,
    candles: list[dict[str, Any]],
    timeframe: str = "M1",
    *,
    strategy_mode: StrategyMode = "conservative",
    payout: float | None = None,
) -> dict[str, Any]:
    normalized = [_normalize_candle(candle) for candle in candles]
    normalized = [candle for candle in normalized if candle is not None]
    last_price = normalized[-1]["close"] if normalized else None

    if len(normalized) < 30:
        signal = _build_signal(
            symbol=symbol,
            signal="WAIT",
            confidence=0,
            reason="Menos de 30 candles disponiveis.",
            last_price=last_price,
            trend="SIDEWAYS",
            strength=0,
            timeframe=timeframe,
        )
        signal["insufficient_candles"] = True
        return _apply_quality_filters(signal, normalized, strategy_mode, payout)

    closes = [candle["close"] for candle in normalized]
    ema9 = _ema(closes, 9)
    ema21 = _ema(closes, 21)
    rsi14 = _rsi(closes, 14)
    trend, trend_strength = _trend(ema9[-1], ema21[-1], closes[-1])
    last_candle = normalized[-1]
    avg_range = _average_range(normalized[-20:])
    atr_pct = (avg_range / abs(last_price)) if last_price else 0.0
    extreme_candle = _is_extreme_candle(last_candle, avg_range)

    call_score, call_reasons = _score_direction("CALL", normalized, ema9[-1], ema21[-1], rsi14)
    put_score, put_reasons = _score_direction("PUT", normalized, ema9[-1], ema21[-1], rsi14)

    if call_score >= put_score:
        selected_signal: SignalValue = "CALL"
        confidence = call_score
        reasons = call_reasons
    else:
        selected_signal = "PUT"
        confidence = put_score
        reasons = put_reasons

    if extreme_candle:
        reasons.append("Ultimo candle exagerado reduz o score.")

    signal = _build_signal(
        symbol=symbol,
        signal=selected_signal,
        confidence=confidence,
        reason=" ".join(reasons),
        last_price=last_price,
        trend=trend,
        strength=trend_strength,
        timeframe=timeframe,
    )
    _attach_indicators(signal, normalized, ema9[-1], ema21[-1], rsi14, avg_range, atr_pct)
    return _apply_quality_filters(signal, normalized, strategy_mode, payout)


def _build_signal(
    *,
    symbol: str,
    signal: SignalValue,
    confidence: int,
    reason: str,
    last_price: float | None,
    trend: TrendValue,
    strength: int,
    timeframe: str,
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "signal": signal,
        "confidence": max(0, min(100, int(round(confidence)))),
        "reason": reason,
        "signal_explanation": reason,
        "narrator_text": reason,
        "timeframe": timeframe,
        "last_price": last_price,
        "trend": trend,
        "strength": max(0, min(100, int(round(strength)))),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def _normalize_candle(candle: dict[str, Any]) -> dict[str, float] | None:
    try:
        return {
            "open": float(candle["open"]),
            "close": float(candle["close"]),
            "min": float(candle["min"]),
            "max": float(candle["max"]),
            "volume": float(candle.get("volume") or 0),
        }
    except (KeyError, TypeError, ValueError):
        return None


def _ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    multiplier = 2 / (period + 1)
    result = [values[0]]
    for value in values[1:]:
        result.append((value - result[-1]) * multiplier + result[-1])
    return result


def _rsi(values: list[float], period: int) -> float:
    if len(values) <= period:
        return 50.0

    gains = []
    losses = []
    for index in range(1, period + 1):
        change = values[index] - values[index - 1]
        gains.append(max(change, 0))
        losses.append(abs(min(change, 0)))

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    for index in range(period + 1, len(values)):
        change = values[index] - values[index - 1]
        gain = max(change, 0)
        loss = abs(min(change, 0))
        avg_gain = ((avg_gain * (period - 1)) + gain) / period
        avg_loss = ((avg_loss * (period - 1)) + loss) / period

    if avg_loss == 0:
        return 100.0
    relative_strength = avg_gain / avg_loss
    return 100 - (100 / (1 + relative_strength))


def _trend(ema9: float, ema21: float, last_price: float) -> tuple[TrendValue, int]:
    if last_price == 0:
        return "SIDEWAYS", 0
    distance_pct = abs(ema9 - ema21) / abs(last_price)
    strength = min(100, distance_pct * 100000)
    if strength < 8:
        return "SIDEWAYS", int(round(strength))
    return ("UP" if ema9 > ema21 else "DOWN"), int(round(strength))


def _average_range(candles: list[dict[str, float]]) -> float:
    ranges = [max(candle["max"] - candle["min"], 0) for candle in candles]
    return sum(ranges) / len(ranges) if ranges else 0


def _is_extreme_candle(candle: dict[str, float], avg_range: float) -> bool:
    candle_range = max(candle["max"] - candle["min"], 0)
    if avg_range <= 0:
        return False
    return candle_range > avg_range * 2.5


def _score_direction(
    direction: SignalValue,
    candles: list[dict[str, float]],
    ema9: float,
    ema21: float,
    rsi: float,
) -> tuple[int, list[str]]:
    score = 0
    reasons = []

    if direction == "CALL":
        if ema9 > ema21:
            score += 35
            reasons.append("EMA9 acima da EMA21.")
        if 50 <= rsi <= 70:
            score += 25
            reasons.append(f"RSI favoravel ({rsi:.1f}).")
    else:
        if ema9 < ema21:
            score += 35
            reasons.append("EMA9 abaixo da EMA21.")
        if 30 <= rsi <= 50:
            score += 25
            reasons.append(f"RSI favoravel ({rsi:.1f}).")

    sequence_score = _sequence_score(direction, candles[-3:])
    score += sequence_score
    if sequence_score:
        reasons.append("Sequencia dos ultimos 3 candles confirma direcao.")

    force_score = _current_candle_force_score(direction, candles[-1])
    score += force_score
    if force_score:
        reasons.append("Ultimo candle tem forca na direcao.")

    return score, reasons or ["Sem confluencias suficientes."]


def _sequence_score(direction: SignalValue, candles: list[dict[str, float]]) -> int:
    if len(candles) < 3:
        return 0
    if direction == "CALL":
        directional = sum(1 for candle in candles if candle["close"] > candle["open"])
        closes_confirm = candles[-1]["close"] >= candles[0]["close"]
    else:
        directional = sum(1 for candle in candles if candle["close"] < candle["open"])
        closes_confirm = candles[-1]["close"] <= candles[0]["close"]
    if directional == 3 and closes_confirm:
        return 20
    if directional >= 2 and closes_confirm:
        return 14
    return 0


def _attach_indicators(
    signal: dict[str, Any],
    candles: list[dict[str, float]],
    ema9: float,
    ema21: float,
    rsi: float,
    atr: float,
    atr_pct: float,
) -> None:
    last = candles[-1]
    candle_range = max(last["max"] - last["min"], 0)
    body = abs(last["close"] - last["open"])
    upper_wick = last["max"] - max(last["open"], last["close"])
    lower_wick = min(last["open"], last["close"]) - last["min"]
    body_ratio = body / candle_range if candle_range else 0.0
    upper_wick_ratio = upper_wick / candle_range if candle_range else 0.0
    lower_wick_ratio = lower_wick / candle_range if candle_range else 0.0
    direction = signal.get("signal")
    directional_last_5 = _directional_count(str(direction), candles[-5:])

    signal.update(
        {
            "ema9": round(ema9, 6),
            "ema21": round(ema21, 6),
            "rsi": round(rsi, 2),
            "atr": round(atr, 6),
            "atr_pct": round(atr_pct, 8),
            "body_ratio": round(body_ratio, 4),
            "upper_wick_ratio": round(upper_wick_ratio, 4),
            "lower_wick_ratio": round(lower_wick_ratio, 4),
            "directional_candles_5": directional_last_5,
            "alternating_last_3": _alternating_last_3(candles[-3:]),
        }
    )


def _apply_quality_filters(
    signal: dict[str, Any],
    candles: list[dict[str, float]],
    strategy_mode: StrategyMode,
    payout: float | None,
) -> dict[str, Any]:
    mode = strategy_mode if strategy_mode in STRATEGY_PROFILES else "conservative"
    profile = STRATEGY_PROFILES[mode]
    blocked: list[str] = []
    approved: list[str] = []
    direction = str(signal.get("signal") or "WAIT")

    penalties = {
        "TREND_CLEAR": 10,
        "SIDEWAYS_FILTER": 10,
        "EMA_TREND": 8,
        "RSI_RANGE": 8,
        "WICK_REJECTION": 8,
        "CANDLE_STRENGTH": 8,
        "DOJI_FILTER": 5,
        "VOLATILITY": 5,
        "LAST_5_CONFIRMATION": 5,
        "NO_ALTERNATING_LAST_3": 5,
    }

    def check(name: str, passed: bool) -> None:
        if passed:
            approved.append(name)
        else:
            blocked.append(name)

    if direction not in {"CALL", "PUT"}:
        blocked.append("CANDLES_UNAVAILABLE")
    if signal.get("insufficient_candles"):
        blocked.append("CANDLES_UNAVAILABLE")
    check("MIN_CONFIDENCE", int(signal.get("confidence") or 0) >= profile["confidence"])
    if payout is None:
        blocked.append("PAYOUT_UNAVAILABLE")
    else:
        check("MIN_PAYOUT", float(payout) >= profile["payout"])
    check("TREND_CLEAR", signal.get("trend") != "SIDEWAYS" and int(signal.get("strength") or 0) >= profile["strength"])
    check("SIDEWAYS_FILTER", signal.get("trend") != "SIDEWAYS")

    ema9 = float(signal.get("ema9") or 0)
    ema21 = float(signal.get("ema21") or 0)
    rsi = float(signal.get("rsi") or 50)
    if direction == "CALL":
        check("EMA_TREND", ema9 > ema21)
        check("RSI_RANGE", 55 <= rsi <= 75)
        check("WICK_REJECTION", float(signal.get("upper_wick_ratio") or 1) <= 0.45)
    elif direction == "PUT":
        check("EMA_TREND", ema9 < ema21)
        check("RSI_RANGE", 25 <= rsi <= 45)
        check("WICK_REJECTION", float(signal.get("lower_wick_ratio") or 1) <= 0.45)

    body_ratio = float(signal.get("body_ratio") or 0)
    check("CANDLE_STRENGTH", body_ratio >= profile["body_ratio"])
    check("DOJI_FILTER", body_ratio >= profile["body_ratio"])
    check("VOLATILITY", float(signal.get("atr_pct") or 0) >= 0.0001)
    check("LAST_5_CONFIRMATION", int(signal.get("directional_candles_5") or 0) >= 3)
    check("NO_ALTERNATING_LAST_3", not bool(signal.get("alternating_last_3")))

    confidence = int(signal.get("confidence") or 0)
    strategy_score = max(0, confidence - sum(penalties.get(name, 0) for name in blocked))
    hard_blocks = [
        name
        for name in blocked
        if name
        in {
            "ACCOUNT_DISCONNECTED",
            "STOP_WIN_HIT",
            "STOP_LOSS_HIT",
            "ACTIVE_CLOSED",
            "ACTIVE_SUSPENDED",
            "PAYOUT_UNAVAILABLE",
            "OPERATION_IN_PROGRESS",
            "CANDLES_UNAVAILABLE",
        }
    ]
    quality_score = strategy_score
    trade_allowed = not hard_blocks
    signal.update(
        {
            "strategy_mode": mode,
            "payout": payout,
            "direction": direction,
            "strategy_score": strategy_score,
            "quality_score": quality_score,
            "blocked_filters": blocked,
            "approved_filters": approved,
            "trade_allowed": trade_allowed,
            "quality_reason": "OK" if trade_allowed else ",".join(hard_blocks),
        }
    )
    if trade_allowed:
        penalties_text = ", ".join(blocked)
        signal["signal_explanation"] = signal.get("reason") or "Sinal aprovado."
        if penalties_text:
            signal["signal_explanation"] += f" Penalizacoes no score: {penalties_text}."
    else:
        filters = ", ".join(hard_blocks) if hard_blocks else "sem direcao valida"
        signal["signal_explanation"] = f"Sinal sem entrada: {filters}."
    signal["narrator_text"] = signal["signal_explanation"]
    return signal


def _directional_count(direction: str, candles: list[dict[str, float]]) -> int:
    if direction == "CALL":
        return sum(1 for candle in candles if candle["close"] > candle["open"])
    if direction == "PUT":
        return sum(1 for candle in candles if candle["close"] < candle["open"])
    return 0


def _alternating_last_3(candles: list[dict[str, float]]) -> bool:
    if len(candles) < 3:
        return False
    colors = []
    for candle in candles:
        if candle["close"] > candle["open"]:
            colors.append("GREEN")
        elif candle["close"] < candle["open"]:
            colors.append("RED")
        else:
            colors.append("DOJI")
    return colors[0] != "DOJI" and colors[1] != "DOJI" and colors[2] != "DOJI" and colors[0] != colors[1] and colors[1] != colors[2]


def _current_candle_force_score(direction: SignalValue, candle: dict[str, float]) -> int:
    candle_range = max(candle["max"] - candle["min"], 0)
    if candle_range <= 0:
        return 0
    body = abs(candle["close"] - candle["open"])
    body_ratio = body / candle_range
    if direction == "CALL" and candle["close"] <= candle["open"]:
        return 0
    if direction == "PUT" and candle["close"] >= candle["open"]:
        return 0
    if body_ratio >= 0.65:
        return 20
    if body_ratio >= 0.45:
        return 14
    if body_ratio >= 0.25:
        return 8
    return 0
