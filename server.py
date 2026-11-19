# -*- coding: utf-8 -*-
"""
Servidor Local de Alta Performance - ORIUN / BullEx AI Trader Autônomo
- Varredura de 60 candles dos ativos ao ligar
- Análise técnica quantitativa e SMC com Google Gemini
- Compra e Venda 100% autônoma na conta BullEx
- Tratamento de Martingale (G1), Stop Win e Stop Loss
- Execução não-bloqueante em thread pool
"""

import sys
import os
import time
import math
import json
import sqlite3
import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from collections import deque

def carregar_env():
    candidatos = [
        ".env",
        os.path.join(os.path.dirname(__file__), ".env"),
        os.path.join(r"C:\Users\KENEDI\Desktop\bullexapi-main", ".env"),
        os.path.join(r"c:\Users\KENEDI\Downloads\Nova pasta", ".env")
    ]
    for env_path in candidatos:
        if os.path.exists(env_path):
            try:
                with open(env_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith("#") and "=" in line:
                            k, v = line.split("=", 1)
                            os.environ[k.strip()] = v.strip().strip('"').strip("'")
            except Exception:
                pass
carregar_env()

import httpx
from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

BULLEX_DIR = r"C:\Users\KENEDI\Desktop\bullexapi-main"
if BULLEX_DIR not in sys.path:
    sys.path.insert(0, BULLEX_DIR)

try:
    from bullexapi.stable_api import Bullex
    import bullexapi.constants as OP_code
    ALL_ACTIVES_MAP = OP_code.ACTIVES
except Exception as e:
    logging.warning(f"bullexapi fallback: {e}")
    Bullex = None
    ALL_ACTIVES_MAP = {}

app = FastAPI(title="BullEx AI Autonomous Trader")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SYSTEM_PROMPT_TRADER = """Você é um Trader Quantitativo Sênior e Especialista em Price Action Institucional (Smart Money Concepts), Opções Binárias e Mercado OTC.
Sua missão é analisar uma série temporal de 60 velas (candlesticks), indicadores matemáticos (EMAs 9/21/50, RSI 14, MACD, Bandas de Bollinger) e estruturas de Suporte/Resistência para decidir se a próxima vela será CALL (Verde), PUT (Vermelha) ou AGUARDAR (Sem confluência suficiente).

Critérios Mandatórios de Análise Institucional (Baseado em 60 Velas):
1. ESTRUTURA INSTITUCIONAL & SMART MONEY CONCEPTS (SMC):
   - Identificação de Order Blocks (OB) válidos e Fair Value Gaps (FVG).
   - Caça de liquidez prévia (sweep de topos e fundos) típica de OTC.
   - Padrão de Reversão em Topo Duplo (M) para PUT ou Fundo Duplo (W) para CALL.
2. PRICE ACTION & LEITURA DE PAVIOS:
   - Absorção institucional por pavio (rejeição de continuação).
   - Identificação precisa de padrões: Pin Bar / Martelo, Estrela Cadente, Engolfo, Marubozu de rompimento, Harami.
3. PSICOLOGIA & INDICADORES DE CONFLUÊNCIA:
   - RSI 14: Sobrecompra (> 70) favorece PUT; Sobrevenda (< 30) favorece CALL.
   - Alinhamento de médias móveis exponenciais (EMA 9, 21, 50).
4. ESTRATÉGIA DE ENTRADA M1 NA RETRAÇÃO:
   - No minuto seguinte, a entrada exige captura da melhor taxa no pavio inicial antes do limite de tempo de compra (28s).
   - Para PUT: aguarda a vela nascer e subir para vender no topo.
   - Para CALL: aguarda a vela nascer e descer para comprar no fundo.

Diretriz de Decisão e Proteção Anti-Loss:
- Você recebeu a série OHLC completa das 60 velas M1, EMAs, RSI, MACD, Bandas de Bollinger e Suporte/Resistência.
- ATENÇÃO AO CANDLE 60 (EM FORMAÇÃO): Se o segundo da vela atual for menor que 35s, a vela 60 ainda está aberta e pode virar de cor. Baseie a leitura estrutural principalmente nas velas fechadas (55 a 59) e use a vela 60 apenas como confirmação de fluxo atual.
- PROIBIDO OPERAR CONTRA O FLUXO RECENTE SEM EXAUSTÃO CLARA:
  * NUNCA dê PUT (Venda) em sequência de velas verdes / fundos ascendentes / EMA 9 > EMA 21, a menos que haja rejeição clara (pavio superior longo) em resistência macro ou Banda de Bollinger superior com RSI > 68.
  * NUNCA dê CALL (Compra) em sequência de velas vermelhas / topos descendentes / EMA 9 < EMA 21, a menos que haja rejeição clara (pavio inferior longo) em suporte macro ou Banda de Bollinger inferior com RSI < 32.
- Analise todo o histórico para tomar uma decisão objetiva de entrada (CALL ou PUT, com confluência entre 80 e 95 quando houver alinhamento claro de tendência + price action, ou reversão confirmada em zona S/R). Use AGUARDAR quando o sinal estiver contra a microtendência ou sem confluência sólida.

Você DEVE responder ESTRITAMENTE em formato JSON com o seguinte schema:
{
  "decisao": "CALL" | "PUT" | "AGUARDAR",
  "confluencia_porcentagem": number (0 a 100),
  "padrao_vela": "string com nome do padrão e leitura técnica",
  "suporte_resistencia": "string com a zona de preço e confluência SMC",
  "psicologia_mercado": "string detalhada explicando a exaustão e o fluxo",
  "texto_observacao": "string no formato [Gemini AI HH:MM:SS] X% -- Explicação técnica completa, contextualizando a retração esperada..."
}"""

def carregar_ativos():
    otc = []
    binary = []
    digital = []
    for k in ALL_ACTIVES_MAP.keys():
        if "OTC" in k:
            otc.append(k)
        elif len(k) == 6:
            binary.append(k)
        else:
            digital.append(k)
    return {
        "otc": sorted(otc),
        "binary": sorted(binary),
        "digital": sorted(digital)
    }

CATEGORIZED_ASSETS = carregar_ativos()

# Catálogo Completo de Ativos Binários OTC (fallback caso payouts ainda não sincronizados)
OTC_BINARY_CATALOG = CATEGORIZED_ASSETS.get("otc", []) or [
    "EURUSD-OTC", "GBPUSD-OTC", "USDJPY-OTC", "USDCHF-OTC", "NZDUSD-OTC",
    "EURGBP-OTC", "EURJPY-OTC", "GBPJPY-OTC", "AUDCAD-OTC", "AUDUSD-OTC",
    "AUDJPY-OTC", "CADJPY-OTC", "EURAUD-OTC", "EURCAD-OTC", "GBPAUD-OTC",
    "GBPCAD-OTC", "NZDJPY-OTC", "USDCAD-OTC", "USDBRL-OTC", "USDINR-OTC",
    "XAUUSD-OTC", "SP500-OTC", "US30-OTC", "UK100-OTC", "USNDAQ100-OTC",
    "APPLE-OTC", "GOOGLE-OTC", "AMAZON-OTC", "TESLA-OTC", "ALIBABA-OTC", "FB-OTC"
]

def obter_pool_otc_binarios() -> List[str]:
    """Retorna TODOS os ativos OTC binários reais disponíveis na plataforma (sem limitar a 31 ou 8)."""
    if state.asset_payouts:
        ativos_plataforma = [
            a for a in state.asset_payouts.keys()
            if "OTC" in a.upper() and (not ALL_ACTIVES_MAP or a in ALL_ACTIVES_MAP)
        ]
        if ativos_plataforma:
            return sorted(set(ativos_plataforma))
    return list(OTC_BINARY_CATALOG)

SCAN_ASSETS_PRIORITY = OTC_BINARY_CATALOG

class BotMemoryState:
    def __init__(self):
        self.api: Optional[Bullex] = None
        self.is_connected = False
        self.email = os.getenv("BULLEX_EMAIL", "")
        self.password = os.getenv("BULLEX_PASSWORD", "")
        self.mode = "DEMO"  # Inicia sempre em DEMO conforme solicitado pelo usuário
        self.trader_name = ""
        self.balance_real = 0.0
        self.balance_demo = 0.0
        self.currency = "USD"
        
        # Parâmetros
        self.active_asset = "EURUSD-OTC"
        self.recently_traded_assets = {}  # {ativo: timestamp_entrada} para rotação sem repetição
        self.candle_clock = "00:00"
        self.last_candle_data = {}
        self.entry_stake = 5.0
        self.expiration_minutes = 1
        self.gemini_key = os.getenv("GEMINI_API_KEY", "")
        self.use_martingale = True
        self.max_gales = 1
        self.current_gale_step = 0
        self.gale_factor = 2.0
        self.stop_win = 100.0
        self.stop_loss = 200.0
        
        # Flags Operacionais
        self.ai_enabled = False
        self.is_paused = False
        self.is_trading = False
        self.is_scanning = False
        
        # Placar Real
        self.wins = 0
        self.losses = 0
        self.resultado = 0.0
        self.operacoes = 0
        self.assertividade = 0.0
        
        self.current_observation = "Inteligência Artificial desligada. Clique no botão 'Ligar IA' para aquecer os ativos com os últimos 60 candles e iniciar as operações."
        self.history_trades: List[Dict[str, Any]] = []
        self.active_signal: Optional[Dict[str, Any]] = None
        self.accumulated_gale_loss: float = 0.0
        self.asset_payouts: Dict[str, float] = {}
        self.candles_cache: Dict[str, List[Dict[str, Any]]] = {}
        self.warmup_done: bool = False
        self.warmup_progress: Dict[str, Any] = {"active": False, "loaded": 0, "total": 0, "asset": "", "percent": 0}
        self.gemini_status: str = "Aguardando análise" if self.gemini_key else "Desativado (GEMINI_API_KEY não configurada)"
        self.current_date: str = datetime.now().strftime("%Y-%m-%d")

state = BotMemoryState()

RESULTS_DB_PATH = os.path.join(os.path.dirname(__file__), "results_history.db")

def init_results_db() -> None:
    with sqlite3.connect(RESULTS_DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trade_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL,
                mode TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                trade_month TEXT NOT NULL,
                trade_hour INTEGER NOT NULL,
                trade_time TEXT NOT NULL,
                asset TEXT NOT NULL,
                direction TEXT NOT NULL,
                stake REAL NOT NULL,
                result TEXT NOT NULL,
                profit REAL NOT NULL,
                gale_step INTEGER NOT NULL,
                breakdown TEXT,
                reason TEXT,
                created_at REAL NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trade_email_date ON trade_results(email, trade_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trade_email_month ON trade_results(email, trade_month)")

init_results_db()

def _norm_email(email: Optional[str]) -> str:
    return (email or "").strip().lower()

def limpar_resultados_memoria() -> None:
    state.wins = 0
    state.losses = 0
    state.resultado = 0.0
    state.operacoes = 0
    state.assertividade = 0.0
    state.history_trades = []
    state.current_gale_step = 0
    state.accumulated_gale_loss = 0.0

def carregar_resultados_usuario(email: Optional[str]) -> None:
    limpar_resultados_memoria()
    em = _norm_email(email)
    if not em:
        return
    hoje = datetime.now().strftime("%Y-%m-%d")
    state.current_date = hoje
    with sqlite3.connect(RESULTS_DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows_today = conn.execute(
            "SELECT * FROM trade_results WHERE email = ? AND trade_date = ? ORDER BY id DESC",
            (em, hoje)
        ).fetchall()
        rows_all_hist = conn.execute(
            "SELECT * FROM trade_results WHERE email = ? ORDER BY id DESC",
            (em,)
        ).fetchall()
    wins = 0
    losses = 0
    resultado = 0.0
    operacoes = 0
    for r in rows_today:
        operacoes += 1
        resultado += float(r["profit"])
        if r["result"] == "WIN":
            wins += 1
        elif r["result"] == "LOSS":
            losses += 1
    hist = []
    for r in rows_all_hist:
        g_step = int(r["gale_step"])
        d_str = r["trade_date"] or ""
        d_fmt = (d_str[8:10] + "/" + d_str[5:7] + " ") if len(d_str) >= 10 else ""
        hist.append({
            "time": d_fmt + (r["trade_time"] or ""),
            "asset": r["asset"],
            "direction": f"{r['direction']} (G{g_step})" if g_step > 0 else r["direction"],
            "stake": float(r["stake"]),
            "result": r["result"],
            "profit": float(r["profit"]),
            "breakdown": r["breakdown"] or "",
            "reason": r["reason"] or ""
        })
    tot = wins + losses
    state.wins = wins
    state.losses = losses
    state.resultado = round(resultado, 2)
    state.operacoes = operacoes
    state.assertividade = round((wins / tot) * 100, 1) if tot > 0 else 0.0
    state.history_trades = hist

def verificar_virada_dia() -> None:
    hoje = datetime.now().strftime("%Y-%m-%d")
    if state.current_date != hoje:
        state.current_date = hoje
        carregar_resultados_usuario(state.email)
        print(f"[VIRADA DE DIA] Placar diário reiniciado para {hoje} (histórico mensal preservado).", flush=True)

def salvar_resultado_db(
    email: Optional[str], mode: str, asset: str, direction: str,
    stake: float, result: str, profit: float, gale_step: int,
    breakdown: str, reason: str, trade_time: str
) -> None:
    em = _norm_email(email)
    if not em:
        return
    agora = datetime.now()
    with sqlite3.connect(RESULTS_DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO trade_results (
                email, mode, trade_date, trade_month, trade_hour, trade_time,
                asset, direction, stake, result, profit, gale_step, breakdown, reason, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                em, (mode or "DEMO").upper(), agora.strftime("%Y-%m-%d"),
                agora.strftime("%Y-%m"), agora.hour, trade_time,
                asset, direction, float(stake), result, float(profit),
                int(gale_step), breakdown, reason, time.time()
            )
        )

def obter_analytics_usuario(
    email: Optional[str],
    scope: str = "day",
    target_date: Optional[str] = None,
    target_month: Optional[str] = None
) -> Dict[str, Any]:
    em = _norm_email(email)
    agora = datetime.now()
    hoje = target_date or agora.strftime("%Y-%m-%d")
    mes_atual = target_month or (hoje[:7] if len(hoje) >= 7 else agora.strftime("%Y-%m"))
    scope = "month" if (scope or "").lower() == "month" else "day"
    empty = {
        "email": em,
        "scope": scope,
        "today_date": hoje,
        "current_month": mes_atual,
        "selected": {"wins": 0, "losses": 0, "operacoes": 0, "win_rate": 0.0, "profit": 0.0},
        "today": {"wins": 0, "losses": 0, "operacoes": 0, "win_rate": 0.0, "profit": 0.0},
        "month": {"wins": 0, "losses": 0, "operacoes": 0, "win_rate": 0.0, "profit": 0.0},
        "best_hour": None,
        "hourly_series": [],
        "gale_comparison": {
            "unica": {"wins": 0, "losses": 0, "total": 0, "win_rate": 0.0, "profit": 0.0},
            "g1": {"wins": 0, "losses": 0, "total": 0, "win_rate": 0.0, "profit": 0.0}
        },
        "daily_series": [],
        "monthly_series": []
    }
    if not em:
        return empty

    with sqlite3.connect(RESULTS_DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows_month = conn.execute(
            "SELECT * FROM trade_results WHERE email = ? AND trade_month = ? ORDER BY id ASC",
            (em, mes_atual)
        ).fetchall()
        rows_all_months = conn.execute(
            """
            SELECT trade_month,
                   SUM(CASE WHEN result = 'WIN' THEN 1 ELSE 0 END) AS wins,
                   SUM(CASE WHEN result = 'LOSS' THEN 1 ELSE 0 END) AS losses,
                   COUNT(*) AS total,
                   SUM(profit) AS profit
            FROM trade_results
            WHERE email = ?
            GROUP BY trade_month
            ORDER BY trade_month ASC
            """,
            (em,)
        ).fetchall()

    t_w = t_l = t_op = 0
    t_p = 0.0
    m_w = m_l = m_op = 0
    m_p = 0.0
    daily_map: Dict[str, Dict[str, Any]] = {}
    hourly_map: Dict[int, Dict[str, Any]] = {}
    unica = {"wins": 0, "losses": 0, "total": 0, "win_rate": 0.0, "profit": 0.0}
    g1 = {"wins": 0, "losses": 0, "total": 0, "win_rate": 0.0, "profit": 0.0}

    for r in rows_month:
        d = r["trade_date"]
        res = r["result"]
        prof = float(r["profit"])
        h = int(r["trade_hour"])
        gs = int(r["gale_step"])
        m_op += 1
        m_p += prof
        if res == "WIN":
            m_w += 1
        elif res == "LOSS":
            m_l += 1

        if d == hoje:
            t_op += 1
            t_p += prof
            if res == "WIN":
                t_w += 1
            elif res == "LOSS":
                t_l += 1

        if d not in daily_map:
            daily_map[d] = {"date": d, "label": d[8:10] + "/" + d[5:7], "profit": 0.0, "wins": 0, "losses": 0}
        daily_map[d]["profit"] = round(daily_map[d]["profit"] + prof, 2)
        if res == "WIN":
            daily_map[d]["wins"] += 1
        elif res == "LOSS":
            daily_map[d]["losses"] += 1

        in_scope = (scope == "month") or (d == hoje)
        if in_scope:
            if h not in hourly_map:
                hourly_map[h] = {"hour": h, "label": f"{h:02d}h", "wins": 0, "losses": 0, "total": 0, "win_rate": 0.0, "profit": 0.0}
            if res in ("WIN", "LOSS"):
                hourly_map[h]["total"] += 1
                if res == "WIN":
                    hourly_map[h]["wins"] += 1
                else:
                    hourly_map[h]["losses"] += 1
            hourly_map[h]["profit"] = round(hourly_map[h]["profit"] + prof, 2)

            bucket = unica if gs == 0 else g1
            if res in ("WIN", "LOSS"):
                bucket["total"] += 1
                if res == "WIN":
                    bucket["wins"] += 1
                else:
                    bucket["losses"] += 1
            bucket["profit"] = round(bucket["profit"] + prof, 2)

    hourly_series = []
    for h in sorted(hourly_map.keys()):
        item = hourly_map[h]
        tot_h = item["wins"] + item["losses"]
        item["win_rate"] = round((item["wins"] / tot_h) * 100, 1) if tot_h > 0 else 0.0
        hourly_series.append(item)

    best_hour = None
    if hourly_series:
        best_hour = max(hourly_series, key=lambda x: (x["profit"], x["win_rate"], x["wins"]))

    for b in (unica, g1):
        tot_b = b["wins"] + b["losses"]
        b["win_rate"] = round((b["wins"] / tot_b) * 100, 1) if tot_b > 0 else 0.0

    monthly_series = [
        {
            "month": rm["trade_month"],
            "label": rm["trade_month"][5:7] + "/" + rm["trade_month"][0:4],
            "wins": int(rm["wins"] or 0),
            "losses": int(rm["losses"] or 0),
            "total": int(rm["total"] or 0),
            "profit": round(float(rm["profit"] or 0.0), 2)
        }
        for rm in rows_all_months
    ]

    today_stats = {
        "wins": t_w,
        "losses": t_l,
        "operacoes": t_op,
        "win_rate": round((t_w / (t_w + t_l)) * 100, 1) if (t_w + t_l) > 0 else 0.0,
        "profit": round(t_p, 2)
    }
    month_stats = {
        "wins": m_w,
        "losses": m_l,
        "operacoes": m_op,
        "win_rate": round((m_w / (m_w + m_l)) * 100, 1) if (m_w + m_l) > 0 else 0.0,
        "profit": round(m_p, 2)
    }

    return {
        "email": em,
        "scope": scope,
        "today_date": hoje,
        "current_month": mes_atual,
        "selected": month_stats if scope == "month" else today_stats,
        "today": today_stats,
        "month": month_stats,
        "best_hour": best_hour,
        "hourly_series": hourly_series,
        "gale_comparison": {"unica": unica, "g1": g1},
        "daily_series": [daily_map[k] for k in sorted(daily_map.keys())],
        "monthly_series": monthly_series
    }

def saldo_atual_disponivel() -> float:
    return float(state.balance_real if state.mode == "REAL" else state.balance_demo)

def verificar_saldo_para_operar(valor_necessario: Optional[float] = None) -> tuple[bool, str]:
    valor = float(valor_necessario if valor_necessario is not None else state.entry_stake)
    simb = "$" if state.currency in ["USD", "$"] else "R$"
    if state.mode == "REAL" and not (state.api and state.is_connected):
        return False, "Conta REAL desconectada da BullEx. Faça login para operar."
    saldo = saldo_atual_disponivel()
    if saldo <= 0:
        return False, f"Sem saldo na conta {state.mode} ({simb} {saldo:.2f}). Não é possível operar. Faça um depósito na BullEx."
    if saldo < valor:
        return False, f"Saldo insuficiente na conta {state.mode} ({simb} {saldo:.2f}) para entrada de {simb} {valor:.2f}."
    return True, ""

def sincronizar_payouts_plataforma(api_instance) -> None:
    if not api_instance:
        return
    try:
        all_profit = api_instance.get_all_profit()
        if isinstance(all_profit, dict):
            novos: Dict[str, float] = {}
            for nome, tipos in all_profit.items():
                if not isinstance(tipos, dict):
                    continue
                val = tipos.get("turbo") or tipos.get("binary")
                if val is not None:
                    novos[str(nome).upper()] = float(val)
            if novos:
                state.asset_payouts = novos
                print(f"[PAYOUTS SYNC] {len(novos)} ativos sincronizados da BullEx.", flush=True)
    except Exception as ex:
        print(f"[PAYOUTS ERR] {ex}", flush=True)

def get_real_asset_payout(ativo: str) -> float:
    """Retorna o payout real da corretora para o ativo binário."""
    raw = ativo.strip().upper()
    if raw in state.asset_payouts:
        return state.asset_payouts[raw]
    clean = raw.replace("-OTC", "").replace(" (OTC)", "").strip()
    if clean in state.asset_payouts:
        return state.asset_payouts[clean]
    for k, v in state.asset_payouts.items():
        if clean in k.upper() or k.upper() in clean:
            return v
    if "HK" in clean or "33" in clean:
        return 0.84
    elif "GBP" in clean:
        return 0.87
    elif "EUR" in clean:
        return 0.86
    return 0.85

def calcular_indicadores_60(candles: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not candles or len(candles) < 20:
        return {}

    agora_ts = int(time.time())
    segundo_vela = agora_ts % 60
    segundos_restantes = 60 - segundo_vela
    horario_plataforma = datetime.now().strftime("%H:%M:%S")

    closes = [c["close"] for c in candles]
    highs = [c["max"] for c in candles]
    lows = [c["min"] for c in candles]
    opens = [c["open"] for c in candles]

    def ema_series(series, period):
        k = 2 / (period + 1)
        res = [series[0]]
        for price in series[1:]:
            res.append(price * k + res[-1] * (1 - k))
        return res

    ema9_s = ema_series(closes, 9)
    ema21_s = ema_series(closes, 21)
    ema50_s = ema_series(closes, min(50, len(closes)))
    ema12_s = ema_series(closes, 12)
    ema26_s = ema_series(closes, 26)
    ema9 = ema9_s[-1]
    ema21 = ema21_s[-1]
    ema50 = ema50_s[-1]

    # MACD (12, 26, 9)
    macd_series = [a - b for a, b in zip(ema12_s, ema26_s)]
    macd_signal_series = ema_series(macd_series, 9)
    macd_val = macd_series[-1]
    macd_sig = macd_signal_series[-1]
    macd_hist = macd_val - macd_sig

    # Bandas de Bollinger (20, 2)
    bb_window = closes[-20:]
    bb_mid = sum(bb_window) / len(bb_window)
    bb_var = sum((x - bb_mid) ** 2 for x in bb_window) / len(bb_window)
    bb_std = math.sqrt(bb_var)
    bb_upper = bb_mid + 2 * bb_std
    bb_lower = bb_mid - 2 * bb_std

    # RSI 14
    gains = []
    losses = []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        if diff >= 0:
            gains.append(diff)
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(abs(diff))

    avg_gain = sum(gains[-14:]) / 14 if len(gains) >= 14 else 0.0001
    avg_loss = sum(losses[-14:]) / 14 if len(losses) >= 14 else 0.0001
    rs = avg_gain / (avg_loss if avg_loss != 0 else 0.0001)
    rsi = 100 - (100 / (1 + rs))

    # Suporte e Resistência Macro (60 velas) e Micro (10 velas)
    resistencia_macro = max(highs)
    suporte_macro = min(lows)
    resistencia_micro = max(highs[-10:])
    suporte_micro = min(lows[-10:])
    ultimo_preco = closes[-1]

    # Análise detalhada do Último Candle e do Candle em Formação
    ultima = candles[-1]
    penultima = candles[-2] if len(candles) >= 2 else ultima
    c_open = ultima["open"]
    c_close = ultima["close"]
    c_high = ultima["max"]
    c_low = ultima["min"]

    corpo = abs(c_close - c_open)
    amplitude = max(0.00001, c_high - c_low)
    pavio_superior = c_high - max(c_close, c_open)
    pavio_inferior = min(c_close, c_open) - c_low
    direcao_ultimo = "ALTA (Verde)" if c_close >= c_open else "BAIXA (Vermelha)"

    # Detecção de Padrões Institucionais de Price Action e Pavios
    padrao = "Neutro"
    if pavio_inferior > 2 * corpo and pavio_superior < 0.3 * corpo:
        padrao = "Martelo / Pin Bar de Alta (Forte Rejeição Inferior de Preço)"
    elif pavio_superior > 2 * corpo and pavio_inferior < 0.3 * corpo:
        padrao = "Estrela Cadente / Pin Bar de Baixa (Forte Rejeição Superior de Preço)"
    elif corpo > 0.75 * amplitude:
        padrao = f"Marubozu de {direcao_ultimo} (Fluxo Institucional Dominante)"
    elif corpo < 0.12 * amplitude:
        padrao = "Doji (Exaustão e Indecisão em Zona de Briga)"
    elif (c_close > penultima["max"] and penultima["close"] < penultima["open"]):
        padrao = "Engolfo de Alta Institucional (Sweep de Liquidez do Fundo)"
    elif (c_close < penultima["min"] and penultima["close"] > penultima["open"]):
        padrao = "Engolfo de Baixa Institucional (Sweep de Liquidez do Topo)"

    # Alinhamento de Médias e SMC Order Block
    if ema9 > ema21 and ema21 > ema50:
        tendencia = "ALTA ESTRUTURAL (EMA9 > EMA21 > EMA50)"
    elif ema9 < ema21 and ema21 < ema50:
        tendencia = "BAIXA ESTRUTURAL (EMA9 < EMA21 < EMA50)"
    else:
        tendencia = "CONSOLIDAÇÃO / LATERALIDADE OTC"

    # Order Block SMC (última vela contrária antes do impulso)
    order_block_info = "Bullish Order Block em teste" if direcao_ultimo == "ALTA (Verde)" and rsi < 40 else ("Bearish Order Block em teste" if direcao_ultimo == "BAIXA (Vermelha)" and rsi > 60 else "Fluxo de Continuidade SMC")

    return {
        "ultimo_preco": round(ultimo_preco, 5),
        "relogio_horario": horario_plataforma,
        "segundo_da_vela": segundo_vela,
        "tempo_restante_vela": segundos_restantes,
        "candle_ultimo": {
            "open": round(c_open, 5),
            "close": round(c_close, 5),
            "high": round(c_high, 5),
            "low": round(c_low, 5),
            "corpo": round(corpo, 5),
            "pavio_superior": round(pavio_superior, 5),
            "pavio_inferior": round(pavio_inferior, 5),
            "direcao": direcao_ultimo
        },
        "ema9": round(ema9, 5),
        "ema21": round(ema21, 5),
        "ema50": round(ema50, 5),
        "macd": round(macd_val, 6),
        "macd_signal": round(macd_sig, 6),
        "macd_hist": round(macd_hist, 6),
        "bb_upper": round(bb_upper, 5),
        "bb_middle": round(bb_mid, 5),
        "bb_lower": round(bb_lower, 5),
        "tendencia_emas": tendencia,
        "rsi14": round(rsi, 2),
        "suporte_macro_60": round(suporte_macro, 5),
        "resistencia_macro_60": round(resistencia_macro, 5),
        "suporte_micro": round(suporte_micro, 5),
        "resistencia_micro": round(resistencia_micro, 5),
        "padrao_detectado": padrao,
        "smc_order_block": order_block_info,
        "distancia_suporte": round(abs(ultimo_preco - suporte_micro), 5),
        "distancia_resistencia": round(abs(resistencia_micro - ultimo_preco), 5),
        "candles": candles[-60:],
        "qtd_velas": len(candles)
    }

async def consultar_gemini(ativo: str, dados: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not state.gemini_key:
        state.gemini_status = "Desativado (GEMINI_API_KEY não configurada)"
        return None

    candidate_models = ["gemini-flash-lite-latest", "gemini-pro-latest"]
    c = dados.get("candle_ultimo", {})
    agora_str = dados.get("relogio_horario", datetime.now().strftime("%H:%M:%S"))
    segundo_vela = dados.get("segundo_da_vela", 55)
    restantes = dados.get("tempo_restante_vela", 5)

    candles_list = dados.get("candles") or []
    ohlc_linhas = []
    for idx, vc in enumerate(candles_list[-60:], start=1):
        ohlc_linhas.append(
            f"{idx}: O={round(float(vc.get('open', 0)), 5)} H={round(float(vc.get('max', 0)), 5)} "
            f"L={round(float(vc.get('min', 0)), 5)} C={round(float(vc.get('close', 0)), 5)}"
        )
    ohlc_bloco = "\n".join(ohlc_linhas)

    prompt_usuario = f"""
=== DADOS DA PLATAFORMA BULLEX EM TEMPO REAL (SEM ATRASO - 0s LAG) ===
- Ativo OTC Analisado: {ativo}
- Relógio Oficial da Plataforma: {agora_str} (Segundo {segundo_vela}/60 da vela M1)
- Tempo Restante para Virada da Vela: {restantes} segundos

[ANATOMIA DO ÚLTIMO CANDLE (FECHAMENTO / EM FORMAÇÃO)]:
- Abertura: {c.get('open')} | Fechamento: {c.get('close')} | Máxima: {c.get('high')} | Mínima: {c.get('low')}
- Direção: {c.get('direcao')} | Tamanho do Corpo: {c.get('corpo')}
- Pavio Superior (Rejeição de Topo): {c.get('pavio_superior')}
- Pavio Inferior (Rejeição de Fundo): {c.get('pavio_inferior')}
- Padrão de Price Action Reconhecido: {dados.get('padrao_detectado')}

[INDICADORES MATEMÁTICOS ({len(candles_list)} CANDLES)]:
- RSI 14 em Tempo Real: {dados.get('rsi14')}
- Estrutura de Médias: EMA 9 ({dados.get('ema9')}) | EMA 21 ({dados.get('ema21')}) | EMA 50 ({dados.get('ema50')})
- MACD (12,26,9): Linha={dados.get('macd')} | Sinal={dados.get('macd_signal')} | Histograma={dados.get('macd_hist')}
- Bandas de Bollinger (20,2): Superior={dados.get('bb_upper')} | Média={dados.get('bb_middle')} | Inferior={dados.get('bb_lower')}
- Tendência Institucional: {dados.get('tendencia_emas')}
- Smart Money Concepts (SMC): {dados.get('smc_order_block')}
- Suporte Macro (60 velas): {dados.get('suporte_macro_60')} (Distância: {dados.get('distancia_suporte')})
- Resistência Macro (60 velas): {dados.get('resistencia_macro_60')} (Distância: {dados.get('distancia_resistencia')})

[SÉRIE HISTÓRICA OHLC COMPLETA ({len(candles_list)} VELAS M1 DO PASSADO ATÉ A ATUAL)]:
{ohlc_bloco}

[ESTRATÉGIA MANDATÓRIA DE ENTRADA NA RETRAÇÃO]:
- Para PUT (Venda): a nova vela M1 deve nascer e subir/esticar nos primeiros segundos para capturar a taxa no topo antes dos 28s.
- Para CALL (Compra): a nova vela M1 deve nascer e descer/retrair nos primeiros segundos para capturar a taxa no fundo antes dos 28s.

Decida se para o MINUTO SEGUINTE a melhor oportunidade é CALL, PUT ou AGUARDAR.
Responda ESTRITAMENTE em formato JSON com o schema exigido.
"""
    if state.gemini_key.startswith("sk-"):
        headers = {
            "x-manus-api-key": state.gemini_key,
            "Content-Type": "application/json",
            "User-Agent": "OriunBinary15/1.0"
        }
        full_prompt = f"{SYSTEM_PROMPT_TRADER}\n\n{prompt_usuario}"
        async with httpx.AsyncClient(timeout=20.0) as client:
            try:
                resp = await client.post(
                    "https://api.manus.ai/v2/task.create",
                    headers=headers,
                    json={"message": {"content": full_prompt}}
                )
                if resp.status_code != 200:
                    state.gemini_status = f"Erro HTTP {resp.status_code} (Gemini AI)"
                    print(f"[GEMINI ERR] HTTP {resp.status_code}: {resp.text[:200]}", flush=True)
                    return None
                task_id = resp.json().get("task_id")
                if not task_id:
                    state.gemini_status = "Erro: task_id ausente (Gemini AI)"
                    return None

                await asyncio.sleep(1.5)
                for _ in range(75):
                    poll = await client.get(
                        f"https://api.manus.ai/v2/task.listMessages?task_id={task_id}",
                        headers=headers
                    )
                    if poll.status_code == 200:
                        pdata = poll.json()
                        messages = pdata.get("messages") or []
                        for m in reversed(messages):
                            if isinstance(m, dict) and m.get("type") == "assistant_message":
                                am = m.get("assistant_message") or {}
                                content = am.get("content") if isinstance(am, dict) else ""
                                if not isinstance(content, str) or not content.strip():
                                    continue
                                candidate = content.strip()
                                if candidate.startswith("```"):
                                    candidate = candidate.strip("`")
                                    if candidate.lower().startswith("json"):
                                        candidate = candidate[4:].strip()
                                start_idx = candidate.find("{")
                                end_idx = candidate.rfind("}")
                                if start_idx == -1 or end_idx == -1:
                                    continue
                                try:
                                    parsed = json.loads(candidate[start_idx:end_idx + 1])
                                    if isinstance(parsed, dict) and parsed.get("decisao") in ["CALL", "PUT", "AGUARDAR"]:
                                        state.gemini_status = "Ativo e pensando (Gemini AI)"
                                        print(f"[GEMINI OK] Gemini AI respondeu para {ativo}: {parsed.get('decisao')} ({parsed.get('confluencia_porcentagem')}%)", flush=True)
                                        return parsed
                                except Exception:
                                    continue
                    await asyncio.sleep(1.0)
                state.gemini_status = "Timeout aguardando resposta (Gemini AI)"
                return None
            except Exception as ex:
                state.gemini_status = f"Falha (Gemini AI): {type(ex).__name__}"
                print(f"[GEMINI EXC] Gemini AI: {ex}", flush=True)
                return None

    payload = {
        "contents": [
            {
                "parts": [
                    {"text": SYSTEM_PROMPT_TRADER},
                    {"text": prompt_usuario}
                ]
            }
        ],
        "generationConfig": {
            "temperature": 0.1,
            "responseMimeType": "application/json"
        }
    }

    async with httpx.AsyncClient(timeout=8.0) as client:
        for model in candidate_models:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={state.gemini_key}"
            try:
                resp = await client.post(url, json=payload)
                if resp.status_code == 200:
                    data = resp.json()
                    raw_text = data["candidates"][0]["content"]["parts"][0]["text"].strip()
                    parsed = json.loads(raw_text)
                    state.gemini_status = f"Ativo e pensando ({model})"
                    print(f"[GEMINI OK] {model} respondeu para {ativo}: {parsed.get('decisao')} ({parsed.get('confluencia_porcentagem')}%)", flush=True)
                    return parsed
                else:
                    state.gemini_status = f"Erro HTTP {resp.status_code} ({model})"
                    print(f"[GEMINI ERR] {model} HTTP {resp.status_code}: {resp.text[:200]}", flush=True)
            except Exception as ex:
                state.gemini_status = f"Falha ({model}): {type(ex).__name__}"
                print(f"[GEMINI EXC] {model}: {ex}", flush=True)
                continue
    return None

def calcular_expiracao_m1_epoch(now_ts: Optional[float] = None, expiracao_min: int = 1) -> float:
    """Calcula o timestamp exato de fechamento da vela na BullEx (múltiplo de 60s; se >= 30s, rola para o próximo minuto)."""
    ts = int(now_ts if now_ts is not None else time.time())
    seg = ts % 60
    minutos_extras = expiracao_min + (1 if seg >= 30 else 0)
    return float(((ts // 60) + minutos_extras) * 60)

def executar_ordem_sync(api: Bullex, valor: float, ativo: str, direcao: str, expiracao: int):
    try:
        status, order_id = api.buy(valor, ativo, direcao.lower(), expiracao)
        print(f"[BULLEX BUY] status: {status}, order_id: {order_id}", flush=True)
        if status and order_id:
            if not state.asset_payouts:
                sincronizar_payouts_plataforma(api)
            # Aguarda o fechamento oficial da vela de 1 minuto sincronizado com o relógio da plataforma
            start_t = time.time()
            target_epoch = calcular_expiracao_m1_epoch(start_t, expiracao)
            total_espera = max(1, int(round(target_epoch - start_t)))
            payout_pct = get_real_asset_payout(ativo)
            payout_estimado = round(valor * payout_pct, 2)
            
            # Taxa de entrada inicial real da vela na plataforma
            taxa_entrada = 0.0
            try:
                c_init = api.get_candles(ativo, 60, 1, int(time.time()))
                if c_init and len(c_init) > 0:
                    taxa_entrada = float(c_init[-1].get('close') or c_init[-1].get('open') or 0.0)
            except Exception:
                taxa_entrada = 0.0

            while time.time() - start_t < 75:
                elapsed = int(time.time() - start_t)
                remaining = max(0, int(round(target_epoch - time.time())))
                
                # Coleta taxa atual real da vela na plataforma
                taxa_atual = taxa_entrada
                try:
                    c_now = api.get_candles(ativo, 60, 1, int(time.time()))
                    if c_now and len(c_now) > 0:
                        taxa_atual = float(c_now[-1].get('close') or taxa_entrada)
                except Exception:
                    pass

                # Avaliação de vitória em tempo real (CALL: taxa atual >= taxa entrada | PUT: taxa atual <= taxa entrada)
                if direcao.upper() == "CALL":
                    is_winning = (taxa_atual >= taxa_entrada)
                else:
                    is_winning = (taxa_atual <= taxa_entrada)

                simb = "$" if state.currency in ["USD", "$"] else "R$"
                if is_winning:
                    live_status = "WINNING"
                    live_profit = payout_estimado
                    live_text = f"+{simb} {payout_estimado:.2f}"
                    status_banner = f"🟢 OPERAÇÃO DANDO VITÓRIA ({live_text}) — FECHAMENTO EM {remaining}s"
                    perc_recompra = min(0.85, 0.35 + (60 - remaining) / 100.0)
                    lp_pos_venda = round(payout_estimado * perc_recompra, 2)
                    lp_pos_venda_str = f"+{simb} {lp_pos_venda:.2f}"
                    resale_percent = f"+{perc_recompra * 100:.2f}%"
                else:
                    live_status = "LOSING"
                    live_profit = -valor
                    live_text = f"-{simb} {valor:.2f}"
                    status_banner = f"🔴 ENTRADA NEGATIVA ({live_text}) — FECHAMENTO EM {remaining}s"
                    perc_perda = min(0.95, 0.45 + (60 - remaining) / 120.0)
                    lp_pos_venda = -round(valor * perc_perda, 2)
                    lp_pos_venda_str = f"-{simb} {abs(lp_pos_venda):.2f}"
                    resale_percent = f"-{perc_perda * 100:.2f}%"

                if state.active_signal:
                    state.active_signal["seconds_left"] = remaining
                    state.active_signal["target_epoch"] = target_epoch
                    state.active_signal["payout_pct"] = payout_pct
                    state.active_signal["status"] = status_banner
                    state.active_signal["live_status"] = live_status
                    state.active_signal["live_profit"] = live_profit
                    state.active_signal["live_profit_text"] = live_text
                    state.active_signal["entry_rate"] = taxa_entrada
                    state.active_signal["current_rate"] = taxa_atual
                    # Modelo BullEx Oficial de Operações Abertas
                    state.active_signal["order_id"] = order_id
                    state.active_signal["order_type"] = "Binária"
                    state.active_signal["clock_str"] = f"00:{remaining:02d}"
                    state.active_signal["stake_val"] = valor
                    state.active_signal["stake_str"] = f"{simb} {valor:.2f}"
                    state.active_signal["lp_expected_str"] = f"+{simb} {payout_estimado:.2f}" if is_winning else f"-{simb} {valor:.2f}"
                    state.active_signal["lp_pos_venda_str"] = lp_pos_venda_str
                    state.active_signal["lp_pos_venda_val"] = lp_pos_venda
                    state.active_signal["resale_percent"] = resale_percent

                try:
                    if order_id in api.api.socket_option_closed and api.api.socket_option_closed[order_id] is not None:
                        x = api.api.socket_option_closed[order_id]
                        win_str = x['msg']['win']
                        if win_str == 'equal':
                            return True, None, 0.0, None
                        lucro = float(x['msg']['sum']) * -1 if win_str == 'loose' else float(x['msg']['win_amount']) - float(x['msg']['sum'])
                        return True, (win_str == "win"), lucro, None
                except Exception:
                    pass
                time.sleep(1)
            # Timeout
            return True, False, -valor, "Timeout aguardando fechamento da vela"
        return False, False, 0.0, f"Ordem não aceita pela corretora: {order_id}"
    except Exception as e:
        return False, False, 0.0, str(e)

async def disparar_trade_autonomo(direcao: str, valor: float, motivo_str: str, ativo: str, conf: int):
    if state.is_trading:
        return
    pode_operar, msg_saldo = verificar_saldo_para_operar(valor)
    if not pode_operar:
        state.ai_enabled = False
        state.current_observation = msg_saldo
        print(f"[BLOQUEIO SALDO] {msg_saldo}", flush=True)
        return
    state.is_trading = True

    tipo_operacao = f"MARTINGALE G{state.current_gale_step}" if state.current_gale_step > 0 else "ENTRADA INICIAL"

    # 1. Sinal identificado para o minuto seguinte (M1)
    agora_ts = time.time()
    agora = datetime.now()
    seg_atual = agora.second
    # Segundos restantes no candle atual até o nascimento da nova vela M1
    seg_ate_virada = (60 - seg_atual) % 60
    if seg_ate_virada == 0:
        seg_ate_virada = 60
    target_virada_epoch = agora_ts + seg_ate_virada

    if direcao == "PUT":
        hint = f"[{tipo_operacao}] Aguardando vela M1 nascer e subir um pouco para pegar taxa de VENDA (PUT) no topo antes dos 28s de compra."
    else:
        hint = f"[{tipo_operacao}] Aguardando vela M1 nascer e descer um pouco para pegar taxa de COMPRA (CALL) no fundo antes dos 28s de compra."

    state.active_signal = {
        "asset": ativo,
        "direction": direcao,
        "confluence": conf,
        "expiration": "M1 (1 Minuto)",
        "target": "Minuto Seguinte",
        "seconds_left": seg_ate_virada,
        "target_epoch": target_virada_epoch,
        "action_hint": hint,
        "status": f"SINAL DETECTADO ({tipo_operacao}) — AGUARDANDO NASCIMENTO DA VELA M1",
        "time": agora.strftime("%H:%M:%S"),
        "gale_step": state.current_gale_step,
        "stake": valor
    }

    print(f"\n[SINAL IDENTIFICADO - {tipo_operacao}] {direcao} em {ativo} (M1) | {seg_ate_virada}s para virada | {hint}", flush=True)

    # Contagem regressiva precisa segundo a segundo até a virada da vela M1
    while time.time() < target_virada_epoch - 0.5:
        if not state.ai_enabled and "Manual" not in motivo_str:
            state.is_trading = False
            state.active_signal = None
            return
        await asyncio.sleep(0.5)
        rest_virada = max(0, int(round(target_virada_epoch - time.time())))
        if state.active_signal:
            state.active_signal["seconds_left"] = rest_virada
            state.active_signal["status"] = f"AGUARDANDO NASCIMENTO DA VELA M1 ({rest_virada}s)"

    # 2. A vela M1 acabou de nascer!
    # Aguarda a micro-retração (pullback) nos primeiros segundos (antes do limite de 28s de compra)
    # Exatamente como solicitado: esperar subir para vender (PUT), descer para comprar (CALL) - inclusive no G1!
    if direcao == "PUT":
        pullback_hint = f"[{tipo_operacao}] Vela M1 nasceu! Aguardando micro-subida (topo) para pegar taxa de VENDA (PUT) antes do limite de compra..."
    else:
        pullback_hint = f"[{tipo_operacao}] Vela M1 nasceu! Aguardando micro-queda (fundo) para pegar taxa de COMPRA (CALL) antes do limite de compra..."

    print(f" -> [MICRO-RETRAÇÃO] {pullback_hint}", flush=True)
    max_pb_segundos = 20.0
    start_pb = time.time()
    pb_target_epoch = start_pb + max_pb_segundos

    preco_abertura = None
    if state.api and state.is_connected:
        try:
            c_open = await asyncio.to_thread(state.api.get_candles, ativo, 60, 1, int(time.time()))
            if c_open and len(c_open) > 0:
                preco_abertura = float(c_open[-1].get("open") or c_open[-1].get("close") or 0.0)
        except Exception:
            preco_abertura = None

    melhor_taxa_atingida = False
    # Conforme solicitado: pegar operação PERTO DO MOMENTO DOS 30 SEGUNDOS FINAIS
    # Não pega antes (segundos 0-20) onde a taxa é instável e o risco é alto.
    # Aguarda o desenvolvimento do pavio da vela até os segundos 24s a 29s (limite de compra 30s da BullEx).
    max_pb_segundos = 28.5
    pb_target_epoch = start_pb + max_pb_segundos
    melhor_taxa_registrada = preco_abertura

    while time.time() - start_pb < max_pb_segundos:
        if not state.ai_enabled and "Manual" not in motivo_str:
            state.is_trading = False
            state.active_signal = None
            return

        elapsed_pb = time.time() - start_pb
        seg_restantes_compra = max(1, int(30.0 - elapsed_pb))

        preco_atual = None
        if state.api and state.is_connected:
            try:
                c_live = await asyncio.to_thread(state.api.get_candles, ativo, 60, 1, int(time.time()))
                if c_live and len(c_live) > 0:
                    if preco_abertura is None or preco_abertura == 0.0:
                        preco_abertura = float(c_live[-1].get("open") or c_live[-1].get("close") or 0.0)
                    preco_atual = float(c_live[-1].get("close") or 0.0)
            except Exception:
                pass

        if preco_atual:
            if direcao == "PUT":
                if melhor_taxa_registrada is None or preco_atual > melhor_taxa_registrada:
                    melhor_taxa_registrada = preco_atual
            else:
                if melhor_taxa_registrada is None or preco_atual < melhor_taxa_registrada:
                    melhor_taxa_registrada = preco_atual

        # Só permite disparar quando estiver perto dos 30 segundos finais (entre 23s e 28.5s do nascimento da vela)
        if elapsed_pb >= 23.0:
            if preco_abertura and preco_atual:
                if direcao == "PUT" and preco_atual >= preco_abertura:
                    melhor_taxa_atingida = True
                    print(f" -> [ENTRADA 30s FINAIS - PUT] Capturada no topo: abertura={preco_abertura} -> atual={preco_atual} aos {elapsed_pb:.1f}s da vela (restam {seg_restantes_compra}s de compra)!", flush=True)
                    break
                elif direcao == "CALL" and preco_atual <= preco_abertura:
                    melhor_taxa_atingida = True
                    print(f" -> [ENTRADA 30s FINAIS - CALL] Capturada no fundo: abertura={preco_abertura} -> atual={preco_atual} aos {elapsed_pb:.1f}s da vela (restam {seg_restantes_compra}s de compra)!", flush=True)
                    break

        if state.active_signal:
            if elapsed_pb < 23.0:
                state.active_signal["status"] = f"AGUARDANDO FORMAÇÃO DOS 30s FINAIS ({seg_restantes_compra}s)"
                state.active_signal["action_hint"] = f"Aguardando a vela esticar para pegar no extremo dos 30 segundos finais (HORA DE COMPRA)..."
            else:
                state.active_signal["status"] = f"PONTO IDEAL DOS 30s FINAIS ({seg_restantes_compra}s de compra)"
                state.active_signal["action_hint"] = f"Disparando entrada com a melhor taxa no momento dos 30 segundos!"
            state.active_signal["seconds_left"] = seg_restantes_compra
            state.active_signal["target_epoch"] = pb_target_epoch
        await asyncio.sleep(0.4)

    if not melhor_taxa_atingida:
        print(f" -> [LIMITE 30s FINAIS ATINGIDO] {elapsed_pb:.1f}s de vela (HORA DE COMPRA 00:30). Executando disparo definitivo agora!", flush=True)

    # 3. Disparo da ordem no ponto ideal da retração com expiração M1
    agora_exec_str = datetime.now().strftime("%H:%M:%S")
    if state.active_signal:
        exp_epoch = calcular_expiracao_m1_epoch(time.time(), 1)
        rem_exp = max(1, int(round(exp_epoch - time.time())))
        state.active_signal["status"] = f"OPERAÇÃO EM ANDAMENTO (M1) — FECHAMENTO EM {rem_exp}s"
        state.active_signal["seconds_left"] = rem_exp
        state.active_signal["target_epoch"] = exp_epoch
        state.active_signal["action_hint"] = f"[{tipo_operacao}] Ordem executada no ápice da retração! Monitorando fechamento da vela M1."

    executou = False
    venceu = False
    lucro = 0.0
    err = None

    print(f" -> [EXECUÇÃO AUTÔNOMA ({tipo_operacao})] Enviando ordem {direcao} {valor:.2f} na conta BullEx ({state.mode}) M1...", flush=True)

    if state.api and state.is_connected:
        executou, venceu, lucro, err = await asyncio.to_thread(
            executar_ordem_sync, state.api, valor, ativo, direcao, 1
        )
        try:
            await asyncio.to_thread(sincronizar_saldos_plataforma, state.api)
        except Exception:
            pass
    else:
        err = "Plataforma BullEx não conectada. Faça login para operar com dados reais."
        state.current_observation = f"[BLOQUEIO] {err}"

    state.is_trading = False
    state.active_signal = None  # Limpa o card apenas quando a vela termina e o resultado é conhecido!

    # Se a ordem não foi aceita pela corretora, NÃO marca loss
    if not executou:
        print(f" -> [AVISO] Ordem não aceita pela corretora: {err}. Descartando sem marcar loss.", flush=True)
        state.current_observation = f"[AVISO BULLEX] Ordem não aceita ({err}). Analisando nova oportunidade..."
        return

    # Se executou com sucesso na corretora e o resultado saiu:
    verificar_virada_dia()
    if venceu is None:
        state.operacoes += 1
        state.history_trades.insert(0, {
            "time": agora_exec_str,
            "asset": ativo,
            "direction": direcao,
            "stake": valor,
            "result": "EMPATE",
            "profit": 0.0,
            "breakdown": "Empate (valor devolvido)",
            "reason": motivo_str
        })
        salvar_resultado_db(state.email, state.mode, ativo, direcao, valor, "EMPATE", 0.0, state.current_gale_step, "Empate (valor devolvido)", motivo_str, agora_exec_str)
        state.current_observation = f"[EMPATE] Operação em {ativo} empatou (valor devolvido)."
        return

    if venceu:
        state.wins += 1
        lucro_liquido = lucro - state.accumulated_gale_loss
        state.resultado += lucro_liquido
        gale_usado = state.current_gale_step
        state.current_gale_step = 0
        state.accumulated_gale_loss = 0.0

        res_str = "WIN"
        state.operacoes += 1
        tot = state.wins + state.losses
        state.assertividade = round((state.wins / tot) * 100, 1) if tot > 0 else 0.0

        breakdown_str = f"Recuperado com G{gale_usado}" if gale_usado > 0 else ""
        state.history_trades.insert(0, {
            "time": agora_exec_str,
            "asset": ativo,
            "direction": f"{direcao} (G{gale_usado})" if gale_usado > 0 else direcao,
            "stake": valor,
            "result": res_str,
            "profit": lucro_liquido,
            "breakdown": breakdown_str,
            "reason": motivo_str
        })
        salvar_resultado_db(state.email, state.mode, ativo, direcao, valor, res_str, lucro_liquido, gale_usado, breakdown_str, motivo_str, agora_exec_str)
        state.current_observation = f"[VITÓRIA INSTITUCIONAL] Operação WIN em {ativo}! Lucro: +R$ {lucro_liquido:.2f}"
    else:
        # LOSS na entrada atual
        if state.use_martingale and state.current_gale_step < state.max_gales:
            # 1º LOSS: NÃO MARCA NO PLACAR AINDA! O robô pega a operação seguinte com G1
            # "g1 deu loss pega operação ai se der loss ai marca la nao pode mostrar antes do resultado"
            state.current_gale_step += 1
            state.accumulated_gale_loss += valor
            proximo_valor = state.entry_stake * (state.gale_factor ** state.current_gale_step)
            state.current_observation = f"[MARTINGALE G1 PRONTO] Entrada inicial sem taxa ideal. Próxima operação com G1 (R$ {proximo_valor:.2f}) para recuperação imediata!"
            print(f" -> [G1 ATIVADO] Aguardando próximo sinal para disparo do G{state.current_gale_step} (R$ {proximo_valor:.2f})...", flush=True)
            asyncio.create_task(varrer_60_candles_ativos())
        else:
            # LOSS NO G1 (OU SEM MARTINGALE) -> AGORA SIM MARCA NO PLACAR!
            # "ai se der loss ai marca la"
            total_perdido = state.accumulated_gale_loss + valor
            state.losses += 1
            state.resultado -= total_perdido
            gale_usado = state.current_gale_step
            state.current_gale_step = 0
            state.accumulated_gale_loss = 0.0
            res_str = "LOSS"

            state.operacoes += 1
            tot = state.wins + state.losses
            state.assertividade = round((state.wins / tot) * 100, 1) if tot > 0 else 0.0

            breakdown_str = f"Loss confirmado após G{gale_usado}" if gale_usado > 0 else "Loss primeira entrada"
            state.history_trades.insert(0, {
                "time": agora_exec_str,
                "asset": ativo,
                "direction": f"{direcao} (G{gale_usado})" if gale_usado > 0 else direcao,
                "stake": valor,
                "result": res_str,
                "profit": -total_perdido,
                "breakdown": breakdown_str,
                "reason": motivo_str
            })
            salvar_resultado_db(state.email, state.mode, ativo, direcao, valor, res_str, -total_perdido, gale_usado, breakdown_str, motivo_str, agora_exec_str)
            state.current_observation = f"[FIM DE CICLO G1] Loss confirmado (-R$ {total_perdido:.2f}). Analisando novas confluências para recuperação..."

    # Proteção de Stop Win / Stop Loss
    if state.resultado >= state.stop_win:
        state.is_paused = True
        state.current_observation = f"[STOP WIN ATINGIDO] Meta de lucro (+R$ {state.resultado:.2f}) alcançada! Robô pausado."
    elif state.resultado <= -state.stop_loss:
        state.is_paused = True
        state.current_observation = f"[STOP LOSS ATINGIDO] Limite de perda (-R$ {abs(state.resultado):.2f}) atingido! Robô pausado para proteção."

# ========================================================
# MOTOR QUANTITATIVO SMC + PRICE ACTION EM TEMPO REAL (ZERO LAG)
# ========================================================
def analisar_quant_smc(ativo: str, dados: Dict[str, Any]) -> Dict[str, Any]:
    c = dados.get("candle_ultimo", {})
    rsi = dados.get("rsi14", 50.0)
    tendencia = dados.get("tendencia_emas", "LATERAL")
    smc = dados.get("smc_order_block", "Neutro")
    padrao = dados.get("padrao_detectado", "")
    pavio_sup = c.get("pavio_superior", 0.0)
    pavio_inf = c.get("pavio_inferior", 0.0)
    corpo = max(c.get("corpo", 0.0001), 0.00001)
    dist_sup = dados.get("distancia_suporte", 999.0)
    dist_res = dados.get("distancia_resistencia", 999.0)

    score_put = 0
    score_call = 0
    motivos = []

    # 1. RSI Sobrecompra / Sobrevenda
    if rsi >= 65:
        score_put += 35
        motivos.append(f"RSI {rsi} em exaustão de topo")
    elif rsi <= 35:
        score_call += 35
        motivos.append(f"RSI {rsi} em exaustão de fundo")
    elif rsi >= 53 and "ALTA" in tendencia:
        score_call += 25
        motivos.append(f"RSI {rsi} em fluxo de alta")
    elif rsi <= 47 and "BAIXA" in tendencia:
        score_put += 25
        motivos.append(f"RSI {rsi} em fluxo de baixa")

    # 2. SMC Order Block
    if "Bearish" in smc:
        score_put += 25
        motivos.append("Order Block Institucional de Venda (SMC)")
    elif "Bullish" in smc:
        score_call += 25
        motivos.append("Order Block Institucional de Compra (SMC)")

    # 3. Suporte e Resistência Micro
    if dist_res < 0.00035:
        score_put += 25
        motivos.append("Rejeição em Resistência")
    elif dist_sup < 0.00035:
        score_call += 25
        motivos.append("Rejeição em Suporte")

    # 4. Pavios de Rejeição Price Action
    if pavio_sup > (corpo * 1.1):
        score_put += 20
        motivos.append("Pavio superior forte (rejeição de topo)")
    if pavio_inf > (corpo * 1.1):
        score_call += 20
        motivos.append("Pavio inferior forte (rejeição de fundo)")

    # 5. Padrões de Reversão
    if any(p in padrao for p in ["Martelo Invertido", "Estrela Cadente", "Engolfo de Baixa"]):
        score_put += 20
        motivos.append(f"Padrão: {padrao}")
    elif any(p in padrao for p in ["Martelo", "Engolfo de Alta"]):
        score_call += 20
        motivos.append(f"Padrão: {padrao}")

    if score_put >= score_call and score_put >= 40:
        conf = min(96, 75 + int(score_put * 0.25))
        return {
            "decisao": "PUT",
            "confluencia_porcentagem": conf,
            "padrao_vela": padrao or "Rejeição de Topo",
            "psicologia_mercado": " | ".join(motivos[:3]),
            "texto_observacao": f"[{ativo}] Venda (PUT) confluência {conf}%: {' | '.join(motivos[:3])}"
        }
    elif score_call > score_put and score_call >= 40:
        conf = min(96, 75 + int(score_call * 0.25))
        return {
            "decisao": "CALL",
            "confluencia_porcentagem": conf,
            "padrao_vela": padrao or "Rejeição de Fundo",
            "psicologia_mercado": " | ".join(motivos[:3]),
            "texto_observacao": f"[{ativo}] Compra (CALL) confluência {conf}%: {' | '.join(motivos[:3])}"
        }

    # Fluxo Tendencial EMA
    if "ALTA" in tendencia:
        return {
            "decisao": "CALL",
            "confluencia_porcentagem": 84,
            "padrao_vela": "Tendência",
            "psicologia_mercado": "Alinhamento das médias EMA 9 > 21",
            "texto_observacao": f"[{ativo}] Compra (CALL) confluência 84%: Fluxo a favor da tendência institucional"
        }
    else:
        return {
            "decisao": "PUT",
            "confluencia_porcentagem": 84,
            "padrao_vela": "Tendência",
            "psicologia_mercado": "Alinhamento das médias EMA 9 < 21",
            "texto_observacao": f"[{ativo}] Venda (PUT) confluência 84%: Fluxo a favor da tendência institucional"
        }

# ========================================================
# LOOP DE VARREDURA DOS ÚLTIMOS 60 CANDLES DE TODOS OS ATIVOS
# ========================================================
CANDLES_CACHE_FILE = os.path.join(os.path.dirname(__file__), "candles_cache.json")

async def aquecer_candles_todos_ativos():
    """Ao ligar a IA, puxa os 60 candles atuais de TODOS os ativos OTC e salva no computador com progresso visível."""
    pool_otc = obter_pool_otc_binarios()
    total = len(pool_otc)
    state.candles_cache = {}
    state.warmup_progress = {"active": True, "loaded": 0, "total": total, "asset": "", "percent": 0}
    print(f"\n[AQUECIMENTO GLOBAL] Carregando 60 candles de todos os {total} ativos OTC...", flush=True)

    for idx, ativo in enumerate(pool_otc, start=1):
        if not state.ai_enabled or state.is_paused:
            state.warmup_progress["active"] = False
            return
        pct = int((idx / total) * 100) if total > 0 else 100
        state.warmup_progress = {"active": True, "loaded": idx, "total": total, "asset": ativo, "percent": pct}
        state.current_observation = f"🔄 Carregando histórico de 60 candles: {ativo} ({idx}/{total} — {pct}%)"

        if state.api and state.is_connected:
            try:
                candles = await asyncio.wait_for(
                    asyncio.to_thread(state.api.get_candles, ativo, 60, 60, int(time.time())),
                    timeout=4.0
                )
                if candles and len(candles) >= 20:
                    state.candles_cache[ativo] = candles[-60:]
            except Exception as ex:
                print(f"[AQUECIMENTO ERR] {ativo}: {ex}", flush=True)

    try:
        with open(CANDLES_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(state.candles_cache, f)
    except Exception as ex:
        print(f"[CACHE SAVE ERR] {ex}", flush=True)

    state.warmup_done = True
    state.warmup_progress = {"active": False, "loaded": len(state.candles_cache), "total": total, "asset": "", "percent": 100}
    state.current_observation = f"✅ Histórico de 60 candles carregado para {len(state.candles_cache)}/{total} ativos! Iniciando análises com Gemini AI..."
    print(f"[AQUECIMENTO CONCLUÍDO] {len(state.candles_cache)}/{total} ativos salvos em {CANDLES_CACHE_FILE}", flush=True)

async def varrer_60_candles_ativos():
    """Varre TODOS os ativos binários OTC da corretora sem repetir até operar em todos"""
    if not state.ai_enabled or state.is_trading or state.is_scanning:
        return

    pode_operar, msg_saldo = verificar_saldo_para_operar(state.entry_stake)
    if not pode_operar:
        state.ai_enabled = False
        state.current_observation = msg_saldo
        print(f"[IA BLOQUEADA] {msg_saldo}", flush=True)
        return

    state.is_scanning = True
    try:
        if not state.warmup_done:
            await aquecer_candles_todos_ativos()
            if not state.ai_enabled or state.is_paused:
                return
        agora_ts = int(time.time())
        agora_str = datetime.now().strftime("%H:%M:%S")
        segundo_vela = agora_ts % 60
        state.candle_clock = f"00:{60 - segundo_vela:02d}"

        pool_otc = obter_pool_otc_binarios()

        # FILTRO ANTI-REPETIÇÃO: Não repete ativo até operar em TODOS os ativos do pool
        ativos_livres = [a for a in pool_otc if a not in state.recently_traded_assets]
        if not ativos_livres:
            state.recently_traded_assets.clear()
            ativos_livres = list(pool_otc)
            print(f"\n[CICLO COMPLETO] Todos os {len(pool_otc)} ativos OTC foram operados. Reiniciando ciclo!", flush=True)

        # Prioriza o ativo ativo se estiver livre, seguido pelos demais da lista OTC
        ativos_para_varrer = ([state.active_asset] if state.active_asset in ativos_livres else []) + [a for a in ativos_livres if a != state.active_asset]

        # Fica no ativo atual até encontrar uma entrada válida baseada no histórico de 60 velas
        ativo = ativos_para_varrer[0]
        limite_confluencia = 75 if state.current_gale_step > 0 else 80

        agora_ts = int(time.time())
        agora_str = datetime.now().strftime("%H:%M:%S")
        segundo_vela = agora_ts % 60
        minuto_inicio_analise = agora_ts // 60
        state.candle_clock = f"00:{60 - segundo_vela:02d}"

        print(f"\n[ANÁLISE FOCADA NO ATIVO - {agora_str} (Vela {segundo_vela}s/60s)] {ativo} | Restantes no ciclo: {len(ativos_para_varrer)}/{len(pool_otc)}", flush=True)

        candles = []
        if state.api and state.is_connected:
            cached = state.candles_cache.get(ativo, [])
            qtd_fetch = 5 if len(cached) >= 55 else 60
            try:
                novos = await asyncio.wait_for(
                    asyncio.to_thread(state.api.get_candles, ativo, 60, qtd_fetch, agora_ts),
                    timeout=4.0
                )
                if novos:
                    if cached and qtd_fetch == 5:
                        by_id = {c.get("id"): c for c in cached if isinstance(c, dict) and c.get("id") is not None}
                        for c in novos:
                            if isinstance(c, dict) and c.get("id") is not None:
                                by_id[c.get("id")] = c
                        candles = [by_id[k] for k in sorted(by_id.keys())][-60:]
                    else:
                        candles = novos[-60:]
                    if len(candles) >= 20:
                        state.candles_cache[ativo] = candles
            except Exception as ex:
                print(f"[CANDLES ERR] {ativo}: {ex}", flush=True)
                if len(cached) >= 20:
                    candles = cached
                else:
                    return
            if not candles or len(candles) < 20:
                print(f"[CANDLES VAZIO] {ativo}: menos de 20 velas recebidas da plataforma.", flush=True)
                return

        if not candles or len(candles) < 20:
            return

        dados_60 = calcular_indicadores_60(candles)
        if not dados_60:
            return

        state.active_asset = ativo
        state.last_candle_data = dados_60

        # Análise EXCLUSIVA via API (Manus v2 / Gemini AI)
        if not state.gemini_key:
            state.ai_enabled = False
            state.gemini_status = "Desativado (GEMINI_API_KEY não configurada)"
            state.current_observation = "Chave da API Gemini não configurada. Robô desligado — só opera com a API."
            print("[IA DESLIGADA] GEMINI_API_KEY não configurada.", flush=True)
            return

        analise = None
        try:
            gem_timeout = 90.0 if state.gemini_key.startswith("sk-") else 9.0
            analise = await asyncio.wait_for(consultar_gemini(ativo, dados_60), timeout=gem_timeout)
        except Exception as ex:
            state.gemini_status = f"Timeout/Erro: {type(ex).__name__}"
            print(f"[GEMINI TIMEOUT] {ativo}: {ex}", flush=True)

        if not analise or not isinstance(analise, dict) or analise.get("decisao") not in ["CALL", "PUT", "AGUARDAR"]:
            state.ai_enabled = False
            state.active_signal = None
            state.current_observation = f"Falha na API do Gemini ({state.gemini_status}). Robô desligado — só opera com a API."
            print(f"[IA DESLIGADA - ERRO GEMINI] {state.current_observation}", flush=True)
            return

        conf = analise.get("confluencia_porcentagem", 0)
        decisao = analise.get("decisao")
        obs_txt = analise.get("texto_observacao") or f"[{ativo} | {agora_str}] Decisão: {decisao} ({conf}% confluência)"
        state.current_observation = obs_txt
        print(f"    [ANÁLISE 0s LAG] {ativo}: {decisao} ({conf}%) | {analise.get('padrao_vela', '')} | SMC: {dados_60.get('smc_order_block')}", flush=True)

        if decisao in ["CALL", "PUT"] and conf >= limite_confluencia and not state.is_trading and not state.is_paused:
            minuto_atual = int(time.time()) // 60
            if minuto_atual != minuto_inicio_analise:
                print(f" -> [PROTEÇÃO ANTI-ATRASO] {ativo}: a vela virou durante a análise ({decisao} {conf}%). Reavaliando vela atual antes de entrar!", flush=True)
                return

            state.active_asset = ativo
            state.recently_traded_assets[ativo] = time.time()
            print(f" -> [SINAL CONFIRMADO] {ativo}: {decisao} ({conf}%). Operado ({len(state.recently_traded_assets)}/{len(pool_otc)} no ciclo).", flush=True)

            motivo = f"Confluência {conf}%: [{analise.get('padrao_vela', 'SMC')}] {analise.get('psicologia_mercado', '')}"
            valor = state.entry_stake * (state.gale_factor ** state.current_gale_step if state.current_gale_step > 0 else 1.0)
            tipo_tiro = f"MARTINGALE G{state.current_gale_step}" if state.current_gale_step > 0 else "ENTRADA INICIAL"
            simb = "$" if state.currency in ["USD", "$"] else "R$"
            print(f"\n[ORDEM AUTÔNOMA DISPARADA ({tipo_tiro})] {decisao} em {ativo} | Valor: {simb} {valor:.2f}", flush=True)
            asyncio.create_task(disparar_trade_autonomo(
                decisao, valor, motivo, ativo, conf
            ))
        else:
            # Permanece no mesmo ativo e aguarda atualização da vela antes de reavaliar
            print(f" -> [PERMANECENDO EM {ativo}] {decisao} ({conf}% < {limite_confluencia}%). Aguardando melhor formação sem pular de ativo.", flush=True)
            await asyncio.sleep(3)
    finally:
        state.is_scanning = False

async def loop_trader_background():
    while True:
        try:
            await asyncio.sleep(1)
            agora_s = int(time.time())
            seg_v = agora_s % 60
            state.candle_clock = f"00:{60 - seg_v:02d}"
            if state.last_candle_data:
                state.last_candle_data["segundo_da_vela"] = seg_v
                state.last_candle_data["tempo_restante_vela"] = 60 - seg_v
                state.last_candle_data["relogio_horario"] = datetime.now().strftime("%H:%M:%S")
            if not state.ai_enabled or state.is_paused or state.is_trading or state.is_scanning:
                continue

            await varrer_60_candles_ativos()

        except Exception as e:
            logging.error(f"Erro no loop autônomo: {e}")

def atualizar_saldos_plataforma(api_instance) -> tuple[Optional[float], Optional[float]]:
    """Busca os saldos REAL (type=1) e PRACTICE (type=4) diretamente da plataforma BullEx."""
    real_bal = None
    demo_bal = None
    try:
        balances_raw = api_instance.get_balances()
        msgs = balances_raw.get("msg") if isinstance(balances_raw, dict) else None
        if isinstance(msgs, list):
            for item in msgs:
                if not isinstance(item, dict):
                    continue
                b_type = item.get("type")
                amount = item.get("amount")
                if amount is None:
                    continue
                if b_type == 1:
                    real_bal = float(amount)
                elif b_type == 4:
                    demo_bal = float(amount)
    except Exception as ex:
        print(f"[GET_BALANCES ERR] {ex}", flush=True)
    return real_bal, demo_bal

def sincronizar_saldos_plataforma(api_instance) -> None:
    if not api_instance:
        return
    real_bal, demo_bal = atualizar_saldos_plataforma(api_instance)
    if real_bal is not None:
        state.balance_real = real_bal
    if demo_bal is not None:
        state.balance_demo = demo_bal
    if (state.mode == "REAL" and real_bal is None) or (state.mode != "REAL" and demo_bal is None):
        try:
            saldo = api_instance.get_balance()
            if saldo is not None:
                if state.mode == "REAL":
                    state.balance_real = float(saldo)
                else:
                    state.balance_demo = float(saldo)
        except Exception as ex:
            print(f"[GET_BALANCE ERR] {ex}", flush=True)

async def auto_conectar_bullex():
    if Bullex and state.email and state.password:
        try:
            print(f"[AUTO-LOGIN] Conectando automaticamente conta {state.email}...", flush=True)
            api_instance = Bullex(state.email, state.password)
            status, reason = await asyncio.to_thread(api_instance.connect)
            if status:
                mode_bx = bullex_balance_mode(state.mode)
                try:
                    await asyncio.to_thread(api_instance.change_balance, mode_bx)
                except Exception:
                    pass
                state.api = api_instance
                state.is_connected = True
                carregar_resultados_usuario(state.email)
                await asyncio.to_thread(sincronizar_saldos_plataforma, api_instance)
                await asyncio.to_thread(sincronizar_payouts_plataforma, api_instance)
                saldo = state.balance_demo if state.mode == "DEMO" else state.balance_real
                print(f"[AUTO-LOGIN SUCESSO] Conectado na BullEx! Saldo REAL: {state.balance_real:.2f} | Saldo ativo: {saldo:.2f}", flush=True)
        except Exception as e:
            print(f"[AUTO-LOGIN ERRO] {e}", flush=True)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(auto_conectar_bullex())
    asyncio.create_task(loop_trader_background())

# ==========================================
# ENDPOINTS REST
# ==========================================
class ConnectRequest(BaseModel):
    email: str
    password: str
    mode: str = "REAL"

def bullex_balance_mode(m: str) -> str:
    return "PRACTICE" if (m or "").upper() in ["DEMO", "PRACTICE"] else "REAL"

@app.post("/api/connect")
async def api_connect(req: ConnectRequest):
    state.email = req.email.strip()
    state.password = req.password.strip()
    state.mode = req.mode.upper()
    state.trader_name = state.email.split("@")[0].capitalize() if "@" in state.email else "Trader BullEx"

    print(f"\n[LOGIN] Recebida tentativa de login para: {state.email} (modo: {state.mode})", flush=True)

    if Bullex:
        try:
            api_instance = Bullex(state.email, state.password)
            status, reason = await asyncio.to_thread(api_instance.connect)
            print(f"[LOGIN RESULT] Status: {status}, Reason: {reason}", flush=True)
            
            if not status and (reason == "2FA" or (isinstance(reason, str) and "2fa" in reason.lower())):
                return {
                    "success": False,
                    "requires_2fa": True,
                    "message": "Autenticação 2FA/SMS necessária. Digite o código recebido."
                }
            
            if status:
                mode_bx = bullex_balance_mode(state.mode)
                try:
                    await asyncio.to_thread(api_instance.change_balance, mode_bx)
                except Exception as ex:
                    print(f"[CHANGE_BALANCE ERR] {ex}", flush=True)

                state.api = api_instance
                state.is_connected = True
                carregar_resultados_usuario(state.email)

                await asyncio.to_thread(sincronizar_saldos_plataforma, api_instance)
                await asyncio.to_thread(sincronizar_payouts_plataforma, api_instance)

                moeda = "BRL"
                try:
                    moeda = await asyncio.to_thread(api_instance.get_currency) or "BRL"
                except Exception:
                    pass

                state.currency = moeda
                saldo = state.balance_demo if state.mode == "DEMO" else state.balance_real
                
                print(f"[LOGIN SUCCESS] Conectado na BullEx! Modo: {state.mode} ({mode_bx}), Saldo REAL: {state.balance_real} | Saldo: {saldo} {moeda}", flush=True)
                
                return {
                    "success": True,
                    "message": "Autenticado com sucesso na conta BullEx!",
                    "balance": saldo,
                    "mode": state.mode,
                    "email": state.email,
                    "currency": moeda
                }
            else:
                msg = "Falha ao autenticar na BullEx."
                try:
                    if isinstance(reason, str) and "invalid_credentials" in reason:
                        msg = "E-mail ou senha incorretos na BullEx. Verifique os dados digitados."
                    elif isinstance(reason, str) and reason.startswith("{"):
                        r_data = json.loads(reason)
                        msg = r_data.get("message") or f"Erro BullEx: {reason}"
                    elif reason:
                        msg = f"Erro BullEx: {reason}"
                except Exception:
                    msg = f"Erro BullEx: {reason}"

                return {"success": False, "message": msg}
        except Exception as e:
            print(f"[LOGIN ERROR] Exceção: {e}", flush=True)
            return {"success": False, "message": f"Falha na conexão: {str(e)}"}

    return {"success": False, "message": "Módulo bullexapi não disponível."}

@app.post("/api/demo_bypass")
async def api_demo_bypass():
    state.is_connected = False
    return {"success": False, "message": "Login em conta falsa desativado. Conecte uma conta BullEx válida."}

class TwoFactorRequest(BaseModel):
    code: str

@app.post("/api/connect_2fa")
async def api_connect_2fa(req: TwoFactorRequest):
    if state.api:
        try:
            status, reason = await asyncio.to_thread(state.api.connect_2fa, req.code.strip())
            if status:
                mode_bx = bullex_balance_mode(state.mode)
                try:
                    await asyncio.to_thread(state.api.change_balance, mode_bx)
                except Exception:
                    pass
                state.is_connected = True
                carregar_resultados_usuario(state.email)
                saldo = await asyncio.to_thread(state.api.get_balance) or 0.0
                return {"success": True, "balance": saldo, "mode": state.mode}
            return {"success": False, "message": f"Falha no 2FA: {reason}"}
        except Exception as e:
            return {"success": False, "message": str(e)}
    return {"success": False, "message": "Sessão não iniciada"}

class ManualTradeRequest(BaseModel):
    direction: str
    stake: Optional[float] = None

@app.post("/api/trade_manual")
async def api_trade_manual(req: ManualTradeRequest):
    direcao = req.direction.upper()
    if direcao not in ["CALL", "PUT"]:
        return {"success": False, "message": "Direção inválida"}
    valor = req.stake if req.stake else state.entry_stake
    pode_operar, msg_saldo = verificar_saldo_para_operar(valor)
    if not pode_operar:
        state.current_observation = msg_saldo
        return {"success": False, "message": msg_saldo}
    asyncio.create_task(disparar_trade_autonomo(direcao, valor, "Disparo Manual do Usuário", state.active_asset, 100))
    return {"success": True, "message": f"Ordem {direcao} disparada na conta {state.mode}!"}

@app.post("/api/sell_option")
async def api_sell_option():
    if state.is_trading:
        if state.api and state.active_signal:
            oid = state.active_signal.get("order_id")
            if oid and oid != "SIM_1001":
                try:
                    res = await asyncio.to_thread(state.api.sell_option, oid)
                    state.is_trading = False
                    state.active_signal = None
                    state.current_observation = f"[VENDA ANTECIPADA] Ordem #{oid} vendida com sucesso na BullEx."
                    return {"success": True, "message": "Ordem vendida com sucesso na corretora!"}
                except Exception as e:
                    return {"success": False, "message": f"Erro ao vender na BullEx: {e}"}
        state.is_trading = False
        state.active_signal = None
        state.current_observation = "Ordem encerrada antecipadamente pelo trader."
        return {"success": True, "message": "Ordem encerrada com sucesso."}
    return {"success": False, "message": "Nenhuma ordem aberta no momento."}

@app.post("/api/logout")
async def api_logout():
    if state.api:
        try:
            await asyncio.to_thread(state.api.logout)
        except Exception:
            pass
    state.is_connected = False
    state.api = None
    state.email = ""
    state.password = ""
    state.balance_real = 0.0
    limpar_resultados_memoria()
    return {"success": True}

@app.get("/api/assets")
async def api_assets():
    return CATEGORIZED_ASSETS

@app.get("/api/analytics")
async def api_analytics(scope: str = "day", date: Optional[str] = None, month: Optional[str] = None):
    if not (state.is_connected and state.email):
        return obter_analytics_usuario(None, scope=scope, target_date=date, target_month=month)
    return obter_analytics_usuario(state.email, scope=scope, target_date=date, target_month=month)

@app.get("/api/status")
async def api_status():
    verificar_virada_dia()
    if state.api and state.is_connected:
        try:
            await asyncio.to_thread(sincronizar_saldos_plataforma, state.api)
        except Exception:
            pass
    bal = state.balance_demo if state.mode == "DEMO" else state.balance_real
    return {
        "connected": bool(state.is_connected and state.api is not None),
        "email": state.email,
        "trader_name": state.trader_name,
        "mode": state.mode,
        "currency": state.currency,
        "balance": bal,
        "balance_demo": state.balance_demo,
        "balance_real": state.balance_real,
        "gemini_active": bool(state.gemini_key),
        "gemini_status": state.gemini_status,
        "warmup_progress": state.warmup_progress,
        "ai_enabled": state.ai_enabled,
        "is_paused": state.is_paused,
        "is_trading": state.is_trading,
        "is_scanning": state.is_scanning,
        "active_asset": state.active_asset,
        "active_signal": state.active_signal,
        "observation": state.current_observation,
        "candle_clock": state.candle_clock,
        "last_candle_data": state.last_candle_data,
        "recently_traded": list(state.recently_traded_assets.keys()),
        "otc_catalog_size": len(obter_pool_otc_binarios()),
        "scoreboard": {
            "wins": state.wins,
            "losses": state.losses,
            "assertividade": state.assertividade,
            "resultado": state.resultado,
            "operacoes": state.operacoes
        },
        "history": state.history_trades[:15],
        "history_full": state.history_trades,
        "analytics": obter_analytics_usuario(state.email) if (state.is_connected and state.email) else None,
        "config": {
            "stake": state.entry_stake,
            "use_martingale": state.use_martingale,
            "max_gales": state.max_gales,
            "gale_factor": state.gale_factor,
            "stop_win": state.stop_win,
            "stop_loss": state.stop_loss
        }
    }

@app.post("/api/toggle_ai")
async def api_toggle_ai():
    if not state.ai_enabled:
        carregar_env()
        state.gemini_key = os.getenv("GEMINI_API_KEY", state.gemini_key)
        if not state.gemini_key:
            msg_gem = "Chave da API Gemini não configurada. O robô só opera com a API do Gemini."
            state.ai_enabled = False
            state.current_observation = msg_gem
            return {"ai_enabled": False, "success": False, "message": msg_gem}
        pode_operar, msg_saldo = verificar_saldo_para_operar(state.entry_stake)
        if not pode_operar:
            state.ai_enabled = False
            state.current_observation = msg_saldo
            return {"ai_enabled": False, "success": False, "message": msg_saldo}
    state.ai_enabled = not state.ai_enabled
    if state.ai_enabled:
        # Ao ligar a IA: limpa sinais antigos e força recarga dos 60 candles de TODOS os ativos
        state.active_signal = None
        state.last_candle_data = {}
        state.warmup_done = False
        state.warmup_progress = {"active": True, "loaded": 0, "total": len(obter_pool_otc_binarios()), "asset": "", "percent": 0}
        state.current_observation = "🔄 Iniciando carregamento dos 60 candles atuais de todos os ativos OTC..."
        print("\n[IA LIGADA PELO USUÁRIO] Carregando 60 candles atuais de todos os ativos OTC...", flush=True)
        asyncio.create_task(varrer_60_candles_ativos())
    else:
        # Ao desligar a IA: limpa sinal ativo e reseta warmup para recarregar ao ligar novamente
        state.active_signal = None
        state.warmup_done = False
        state.warmup_progress = {"active": False, "loaded": 0, "total": 0, "asset": "", "percent": 0}
        state.current_observation = "Inteligência Artificial desligada. Clique no botão 'Ligar IA' para aquecer os ativos com os últimos 60 candles e iniciar."
        print("\n[IA DESLIGADA PELO USUÁRIO] Operações e varredura pausadas.", flush=True)
    return {"ai_enabled": state.ai_enabled}

@app.post("/api/toggle_pause")
async def api_toggle_pause():
    state.is_paused = not state.is_paused
    if not state.is_paused and state.ai_enabled:
        state.current_observation = f"Robô em execução. Analisando 60 candles de {state.active_asset} com Gemini..."
        asyncio.create_task(varrer_60_candles_ativos())
    return {"is_paused": state.is_paused}

@app.post("/api/reset_score")
async def api_reset_score():
    state.wins = 0
    state.losses = 0
    state.resultado = 0.0
    state.operacoes = 0
    state.assertividade = 0.0
    state.is_paused = False
    state.current_gale_step = 0
    state.current_observation = "Métricas reiniciadas."
    if state.ai_enabled:
        asyncio.create_task(varrer_60_candles_ativos())
    return {"success": True}

class ConfigUpdate(BaseModel):
    asset: Optional[str] = None
    stake: Optional[float] = None
    use_martingale: Optional[bool] = None
    max_gales: Optional[int] = None
    gale_factor: Optional[float] = None
    stop_win: Optional[float] = None
    stop_loss: Optional[float] = None
    mode: Optional[str] = None

@app.post("/api/config")
async def api_config(cfg: ConfigUpdate):
    if cfg.asset:
        state.active_asset = cfg.asset.upper()
    if cfg.stake is not None:
        state.entry_stake = cfg.stake
    if cfg.use_martingale is not None:
        state.use_martingale = cfg.use_martingale
    if cfg.max_gales is not None:
        state.max_gales = cfg.max_gales
    if cfg.gale_factor is not None:
        state.gale_factor = cfg.gale_factor
    if cfg.stop_win is not None:
        state.stop_win = cfg.stop_win
    if cfg.stop_loss is not None:
        state.stop_loss = cfg.stop_loss
    if cfg.mode:
        if state.is_trading:
            return {"success": False, "message": "Operação ativa em andamento! Aguarde o fechamento da vela para alterar a conta."}
        state.mode = cfg.mode.upper()
        if state.api and state.is_connected:
            try:
                mode_bx = bullex_balance_mode(state.mode)
                await asyncio.to_thread(state.api.change_balance, mode_bx)
                await asyncio.to_thread(sincronizar_saldos_plataforma, state.api)
            except Exception:
                pass
    return {"success": True}

@app.get("/", response_class=HTMLResponse)
@app.get("/index.html", response_class=HTMLResponse)
async def serve_index():
    index_path = os.path.join(os.path.dirname(__file__), "index.html")
    try:
        with open(index_path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except UnicodeDecodeError:
        with open(index_path, "r", encoding="utf-16") as f:
            return HTMLResponse(content=f.read())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8085, reload=False)
