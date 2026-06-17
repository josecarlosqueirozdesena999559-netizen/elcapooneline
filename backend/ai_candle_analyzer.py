import json
import os
import time
from typing import Any

import httpx


OPENAI_CHAT_COMPLETIONS_URL = "https://api.openai.com/v1/chat/completions"
OPENAI_TIMEOUT_SECONDS = 8.0
AI_CACHE_TTL_SECONDS = 30.0
DEFAULT_OPENAI_MODEL = "gpt-5.5"
ALLOWED_DIRECTIONS = {"CALL", "PUT"}
ALLOWED_RISK_LEVELS = {"LOW", "MEDIUM", "HIGH"}
_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}


def ai_analysis_unavailable(reason: str) -> dict[str, Any]:
    return {
        "ok": False,
        "reason": reason,
        "result": None,
        "cached": False,
    }


async def analyze_candle_candidate(payload: dict[str, Any]) -> dict[str, Any]:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        return ai_analysis_unavailable("missing_api_key")

    model = os.getenv("OPENAI_MODEL", DEFAULT_OPENAI_MODEL).strip() or DEFAULT_OPENAI_MODEL
    normalized_input = _sanitize_payload(payload)
    cache_key = _cache_key(normalized_input)
    cached = _get_cached(cache_key)
    if cached is not None:
        return {"ok": True, "reason": None, "result": cached, "cached": True}

    request_payload = _build_request_payload(model, normalized_input)
    try:
        async with httpx.AsyncClient(timeout=OPENAI_TIMEOUT_SECONDS) as client:
            response = await client.post(
                OPENAI_CHAT_COMPLETIONS_URL,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json=request_payload,
            )
            response.raise_for_status()
    except httpx.TimeoutException:
        return ai_analysis_unavailable("timeout")
    except Exception as exc:
        return ai_analysis_unavailable(f"request_error:{exc}")

    try:
        raw_response = response.json()
        content = raw_response["choices"][0]["message"]["content"]
        parsed = json.loads(content)
    except Exception as exc:
        return ai_analysis_unavailable(f"invalid_response:{exc}")

    normalized = _normalize_response(parsed, normalized_input)
    _CACHE[cache_key] = (time.monotonic(), normalized)
    return {"ok": True, "reason": None, "result": normalized, "cached": False}


def _cache_key(payload: dict[str, Any]) -> str:
    symbol = str(payload.get("symbol") or "").upper()
    timeframe = str(payload.get("timeframe") or "").upper()
    direction = str(payload.get("candidate_direction") or "").upper()
    candles = payload.get("candles") or []
    last_time = ""
    if candles:
        last_time = str((candles[-1] or {}).get("time") or "")
    return f"{symbol}|{timeframe}|{direction}|{last_time}"


def _get_cached(cache_key: str) -> dict[str, Any] | None:
    cached = _CACHE.get(cache_key)
    if cached is None:
        return None
    created_at, result = cached
    if time.monotonic() - created_at > AI_CACHE_TTL_SECONDS:
        _CACHE.pop(cache_key, None)
        return None
    return dict(result)


def _sanitize_payload(payload: dict[str, Any]) -> dict[str, Any]:
    candles = payload.get("candles") or []
    normalized_candles = []
    for candle in candles[-60:]:
        if not isinstance(candle, dict):
            continue
        normalized_candles.append(
            {
                "time": candle.get("time"),
                "open": _safe_float(candle.get("open")),
                "high": _safe_float(candle.get("high")),
                "low": _safe_float(candle.get("low")),
                "close": _safe_float(candle.get("close")),
            }
        )

    indicators = payload.get("indicators") or {}
    return {
        "symbol": str(payload.get("symbol") or "").strip(),
        "timeframe": str(payload.get("timeframe") or "M1").strip().upper(),
        "candidate_direction": str(payload.get("candidate_direction") or "").strip().upper(),
        "strategy_score": _clamp_int(payload.get("strategy_score")),
        "confidence": _clamp_int(payload.get("confidence")),
        "payout": _safe_float(payload.get("payout")),
        "used_strategies": [str(item) for item in (payload.get("used_strategies") or [])][:10],
        "indicators": {
            "ema9": _safe_float(indicators.get("ema9")),
            "ema21": _safe_float(indicators.get("ema21")),
            "rsi14": _safe_float(indicators.get("rsi14")),
            "atr": _safe_float(indicators.get("atr")),
            "trend": str(indicators.get("trend") or "").upper(),
            "volatility": str(indicators.get("volatility") or "").upper(),
        },
        "candles": normalized_candles,
        "ai_min_confidence": _clamp_int(payload.get("ai_min_confidence") or 70),
    }


def _build_request_payload(model: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "model": model,
        "response_format": {"type": "json_object"},
        "messages": [
            {
                "role": "developer",
                "content": (
                    "Voce analisa candles e indicadores para confirmar uma entrada ja escolhida por estrategias locais. "
                    "Nao gere novos sinais, nao mencione login, usuario, email, senha, cookies, sessao ou BullEx. "
                    "Use apenas os candles, indicadores, payout, score e direcao candidata. "
                    "Responda somente JSON valido com as chaves: approved, direction, confidence, risk_level, "
                    "entry_reason, voice_text, block_reason, candle_reading, strategy_alignment."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "task": [
                            "Confirmar ou discordar da direcao candidata.",
                            "Classificar risco da entrada.",
                            "Explicar leitura dos candles de forma curta.",
                            "Gerar texto em portugues para narracao.",
                            "Se a leitura estiver fraca, marcar approved false.",
                        ],
                        "input": payload,
                    },
                    ensure_ascii=True,
                ),
            },
        ],
    }


def _normalize_response(response: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    candidate_direction = str(payload.get("candidate_direction") or "").upper()
    min_confidence = _clamp_int(payload.get("ai_min_confidence") or 70)
    direction = str(response.get("direction") or candidate_direction).strip().upper()
    confidence = _clamp_int(response.get("confidence"))
    risk_level = str(response.get("risk_level") or "HIGH").strip().upper()
    approved = bool(response.get("approved"))
    block_reason = _clean_text(response.get("block_reason"))

    if direction not in ALLOWED_DIRECTIONS:
        direction = candidate_direction if candidate_direction in ALLOWED_DIRECTIONS else "CALL"
    if risk_level not in ALLOWED_RISK_LEVELS:
        risk_level = "HIGH"

    if direction != candidate_direction:
        approved = False
        block_reason = block_reason or "AI_DIRECTION_MISMATCH"
    if risk_level == "HIGH":
        approved = False
        block_reason = block_reason or "AI_HIGH_RISK"
    if confidence < min_confidence:
        approved = False
        block_reason = block_reason or "AI_LOW_CONFIDENCE"

    return {
        "approved": approved,
        "direction": direction,
        "confidence": confidence,
        "risk_level": risk_level,
        "entry_reason": _clean_text(response.get("entry_reason")) or "Analise da IA concluida.",
        "voice_text": _clean_text(response.get("voice_text")) or "Leitura da IA indisponivel para narracao.",
        "block_reason": block_reason,
        "candle_reading": _clean_text(response.get("candle_reading")) or "Leitura dos candles indisponivel.",
        "strategy_alignment": _clean_text(response.get("strategy_alignment")) or "Sem alinhamento detalhado.",
    }


def _clean_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _clamp_int(value: Any) -> int:
    try:
        numeric = int(round(float(value)))
    except (TypeError, ValueError):
        numeric = 0
    return max(0, min(100, numeric))
