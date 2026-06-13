from datetime import datetime, timezone
from typing import Any, Literal


SignalValue = Literal["CALL", "PUT", "WAIT"]
TrendValue = Literal["UP", "DOWN", "SIDEWAYS"]


def analyze_signal(symbol: str, candles: list[dict[str, Any]], timeframe: str = "M1") -> dict[str, Any]:
    normalized = [_normalize_candle(candle) for candle in candles]
    normalized = [candle for candle in normalized if candle is not None]
    last_price = normalized[-1]["close"] if normalized else None

    if len(normalized) < 30:
        return _build_signal(
            symbol=symbol,
            signal="WAIT",
            confidence=0,
            reason="Menos de 30 candles disponiveis.",
            last_price=last_price,
            trend="SIDEWAYS",
            strength=0,
            timeframe=timeframe,
        )

    closes = [candle["close"] for candle in normalized]
    ema9 = _ema(closes, 9)
    ema21 = _ema(closes, 21)
    rsi14 = _rsi(closes, 14)
    trend, trend_strength = _trend(ema9[-1], ema21[-1], closes[-1])
    last_candle = normalized[-1]
    avg_range = _average_range(normalized[-20:])
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

    wait_reasons = []
    if rsi14 > 75 or rsi14 < 25:
        wait_reasons.append(f"RSI extremo ({rsi14:.1f}).")
    if trend == "SIDEWAYS":
        wait_reasons.append("Tendencia lateral.")
    if confidence < 70:
        wait_reasons.append(f"Confianca abaixo de 70 ({confidence}).")
    if extreme_candle:
        wait_reasons.append("Ultimo candle exagerado.")

    if wait_reasons:
        return _build_signal(
            symbol=symbol,
            signal="WAIT",
            confidence=confidence,
            reason=" ".join(wait_reasons),
            last_price=last_price,
            trend=trend,
            strength=trend_strength,
            timeframe=timeframe,
        )

    return _build_signal(
        symbol=symbol,
        signal=selected_signal,
        confidence=confidence,
        reason=" ".join(reasons),
        last_price=last_price,
        trend=trend,
        strength=trend_strength,
        timeframe=timeframe,
    )


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
