import json
import logging
import os
from typing import Any

import httpx


logger = logging.getLogger("backend-gateway")

OPENAI_CHAT_COMPLETIONS_URL = "https://api.openai.com/v1/chat/completions"
OPENAI_TIMEOUT_SECONDS = 10.0
ALLOWED_RISKS = {"LOW", "MEDIUM", "HIGH"}
ALLOWED_RECOMMENDATIONS = {"VALID_SIGNAL", "CAUTION", "REJECT_SIGNAL"}


def unavailable_review() -> dict[str, Any]:
    return {
        "approved": False,
        "risk": "UNKNOWN",
        "quality": 0,
        "summary": "OpenAI unavailable",
        "warnings": ["OPENAI_UNAVAILABLE"],
        "recommendation": "CAUTION",
    }


async def review_signal(signal: dict[str, Any]) -> dict[str, Any]:
    logger.info("[OPENAI REVIEW START]")
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    model = os.getenv("OPENAI_MODEL", "gpt-5.5").strip() or "gpt-5.5"
    if not api_key:
        logger.warning("[OPENAI REVIEW FAIL] missing_api_key")
        return unavailable_review()

    payload = _build_payload(model, signal)
    try:
        async with httpx.AsyncClient(timeout=OPENAI_TIMEOUT_SECONDS) as client:
            response = await client.post(
                OPENAI_CHAT_COMPLETIONS_URL,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json=payload,
            )
            response.raise_for_status()
    except httpx.TimeoutException:
        logger.warning("[OPENAI REVIEW TIMEOUT]")
        return unavailable_review()
    except Exception as exc:
        logger.warning("[OPENAI REVIEW FAIL] %s", exc)
        return unavailable_review()

    try:
        data = response.json()
        content = data["choices"][0]["message"]["content"]
        review = json.loads(content)
    except Exception as exc:
        logger.warning("[OPENAI REVIEW FAIL] invalid_response %s", exc)
        return unavailable_review()

    normalized = _normalize_review(review)
    final_review = _apply_automatic_rules(signal, normalized)
    logger.info("[OPENAI REVIEW OK]")
    return final_review


def _build_payload(model: str, signal: dict[str, Any]) -> dict[str, Any]:
    review_input = {
        "symbol": signal.get("symbol"),
        "signal": signal.get("signal"),
        "confidence": signal.get("confidence"),
        "trend": signal.get("trend"),
        "strength": signal.get("strength"),
        "reason": signal.get("reason"),
    }
    return {
        "model": model,
        "response_format": {"type": "json_object"},
        "messages": [
            {
                "role": "system",
                "content": (
                    "Voce e uma camada de revisao de risco para sinais binarios. "
                    "Nao gere sinais novos, nao substitua o motor local, nao recomende compra, "
                    "nao abra operacoes e nao altere conta. Revise apenas o sinal recebido. "
                    "Responda somente JSON valido com: approved boolean, risk LOW/MEDIUM/HIGH, "
                    "quality 0-100, summary string curta, warnings array, recommendation "
                    "VALID_SIGNAL/CAUTION/REJECT_SIGNAL."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "task": [
                            "Avaliar consistencia do sinal.",
                            "Classificar risco.",
                            "Detectar conflito entre indicadores.",
                            "Produzir explicacao curta.",
                            "Aprovar ou rejeitar o sinal.",
                        ],
                        "signal": review_input,
                    },
                    ensure_ascii=True,
                ),
            },
        ],
    }


def _normalize_review(review: dict[str, Any]) -> dict[str, Any]:
    risk = str(review.get("risk") or "HIGH").strip().upper()
    recommendation = str(review.get("recommendation") or "CAUTION").strip().upper()
    warnings = review.get("warnings")
    if not isinstance(warnings, list):
        warnings = []

    return {
        "approved": bool(review.get("approved")),
        "risk": risk if risk in ALLOWED_RISKS else "HIGH",
        "quality": _clamp_int(review.get("quality")),
        "summary": str(review.get("summary") or "Revisao concluida.").strip(),
        "warnings": [str(warning) for warning in warnings],
        "recommendation": recommendation if recommendation in ALLOWED_RECOMMENDATIONS else "CAUTION",
    }


def _apply_automatic_rules(signal: dict[str, Any], review: dict[str, Any]) -> dict[str, Any]:
    confidence = _clamp_int(signal.get("confidence"))
    signal_value = str(signal.get("signal") or "").strip().upper()

    if confidence >= 85 and review["quality"] < 70:
        review["quality"] = 70

    if confidence < 70:
        review["recommendation"] = "CAUTION"
        review["approved"] = False
        _append_warning(review, "LOW_CONFIDENCE")

    if signal_value == "WAIT":
        review["recommendation"] = "REJECT_SIGNAL"
        review["approved"] = False
        _append_warning(review, "WAIT_SIGNAL")

    if review["recommendation"] == "REJECT_SIGNAL":
        review["approved"] = False
    elif review["recommendation"] == "VALID_SIGNAL":
        review["approved"] = True

    return review


def _append_warning(review: dict[str, Any], warning: str) -> None:
    if warning not in review["warnings"]:
        review["warnings"].append(warning)


def _clamp_int(value: Any) -> int:
    try:
        numeric = int(round(float(value)))
    except (TypeError, ValueError):
        numeric = 0
    return max(0, min(100, numeric))
