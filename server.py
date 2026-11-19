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

SYSTEM_PROMPT_TRADER = """Você é um Trader Quantitativo Sênior e Especialista em Price Action Institucional (Smart Money Concepts), Opções Binárias M1 e Mercado OTC.
Sua missão é analisar uma série temporal de 60 velas, indicadores matemáticos avançados, FRACTAIS DE WILLIAMS e estratégias institucionais de alta assertividade.

REGRAS DE PROTEÇÃO ANTI-LOSS (CRUCIAIS PARA MERCADO OTC):
1. RESPEITE A TENDÊNCIA ESTRUTURAL (NUNCA VÁ CONTRA O FLUXO OTC SEM REVERSÃO EXTREMA):
   - Se a tendência for de ALTA (EMA9 > EMA21 > EMA50) e ADX >= 20: DÊ PRIORIDADE TOTAL PARA CALL! Opere pullbacks nas médias, fundos fractais ou rompimento de topos fractais. NÃO venda PUT contra tendência de alta a menos que haja rejeição extrema em topo macro com RSI > 75 e divergência MACD.
   - Se a tendência for de BAIXA (EMA9 < EMA21 < EMA50) and ADX >= 20: DÊ PRIORIDADE TOTAL PARA PUT! Opere pullbacks nas médias, topos fractais ou rompimento de fundos fractais. NÃO compre CALL contra tendência de baixa a menos que haja rejeição extrema em fundo macro com RSI < 25 e divergência MACD.
2. O 'OLHO DE DEUS' É APENAS UM PROMPT EXTRA: NÃO trave o robô esperando por ele. Se houver Fractal + Price Action ou Rompimento + MACD, emita CALL ou PUT com confluência >= 75%!
3. VELAS DE INDECISÃO / DOJI: Se o corpo da vela for insignificante (baixo volume), retorne AGUARDAR.

GATILHOS INSTITUCIONAIS DE ALTA ASSERTIVIDADE:
1. FRACTAIS DE WILLIAMS (5 VELAS) - NÍVEIS DINÂMICOS DE EXAUSTÃO E SUPORTE:
   - Rejeição em Fundo Fractal: O preço testou o suporte do Fundo Fractal anterior e retraiu deixando longo pavio inferior -> CALL (80% a 92%).
   - Rejeição em Topo Fractal: O preço testou a resistência do Topo Fractal anterior e retraiu deixando longo pavio superior -> PUT (80% a 92%).
   - Rompimento de Topo Fractal a favor da tendência de alta -> CALL (80% a 88%).
   - Rompimento de Fundo Fractal a favor da tendência de baixa -> PUT (80% a 88%).

2. SMART MONEY CONCEPTS (SMC) & PRICE ACTION INSTITUCIONAL:
   - Bullish Order Block (OB): Absorção compradora em zona de demanda -> CALL (80% a 90%).
   - Bearish Order Block (OB): Absorção vendedora em zona de oferta -> PUT (80% a 90%).
   - Martelo / Pin Bar de Alta em suporte ou fundo fractal -> CALL (80% a 92%).
   - Estrela Cadente / Pin Bar de Baixa em resistência ou topo fractal -> PUT (80% a 92%).
   - Rompimento Marubozu com Volume > SMA 5 na direção da tendência -> Seguir o fluxo (CALL verde / PUT vermelha) (80% a 88%).

3. CONFLUÊNCIA COM BANDAS DE BOLLINGER, RSI E MACD:
   - Compra (CALL): Preço tocando Banda Inferior de Bollinger com retração + RSI sobrevendido (< 38) + MACD favorável.
   - Venda (PUT): Preço tocando Banda Superior de Bollinger com retração + RSI sobrecomprado (> 62) + MACD favorável.

4. OLHO DE DEUS A+ (ENGULFING):
   - Quando ocorrer Engolfo Puro A+ alinhado à tendência e MACD, atribua confluência máxima A+ (90% a 98%).

5. FILTRO DE MOMENTUM MACD (12, 26, 9):
   - CALL: Linha MACD > Sinal E/OU Histograma crescendo.
   - PUT: Linha MACD < Sinal E/OU Histograma decrescendo.

6. ESCALA DE CONFLUÊNCIA:
   - 90% a 98%: Olho de Deus A+ OU Rejeição Extrema em Fractal/Order Block com MACD forte.
   - 75% a 89%: Qualquer gatilho de Fractal, Price Action, Rompimento ou Bollinger com MACD alinhado.
   - Abaixo de 75%: Mercado incerto ou contra a tendência -> AGUARDAR.

Você DEVE responder ESTRITAMENTE em formato JSON com o seguinte schema:
{
  "decisao": "CALL" | "PUT" | "AGUARDAR",
  "confluencia_porcentagem": number (0 a 100),
  "padrao_vela": "string com o padrão (ex: 'Rejeição em Fundo Fractal', 'Bullish Order Block SMC', 'Pin Bar de Alta', 'Olho de Deus A+')",
  "suporte_resistencia": "string com os níveis de Fractal e suporte/resistência",
  "psicologia_mercado": "string detalhada explicando o fluxo e por que o trade tem alta probabilidade",
  "texto_observacao": "string no formato [Gemini AI HH:MM:SS] X% -- Explicação técnica completa destacando os Fractais e indicadores..."
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

# Chave Global Pré-Configurada para Execução Direta em Qualquer Computador
DEFAULT_GEMINI_B64 = "QVEuQWI4Uk42TGJXbm5yR2pIOUVHd2hwNmJqblQzWmFiV2tCbk5JNjR3VHJ4ZzlNT2JXV3c="

def obter_gemini_api_key() -> str:
    k = (os.getenv("GEMINI_API_KEY") or "").strip()
    if not k:
        try:
            import base64
            k = base64.b64decode(DEFAULT_GEMINI_B64.encode("utf-8")).decode("utf-8").strip()
        except Exception:
            k = ""
    return k

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
        self.gemini_key = obter_gemini_api_key()
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

    closes = [float(c["close"]) for c in candles]
    highs = [float(c["max"]) for c in candles]
    lows = [float(c["min"]) for c in candles]
    opens = [float(c["open"]) for c in candles]
    volumes = [float(c.get("volume", 1.0) or 1.0) for c in candles]

    def ema_series(series, period):
        k = 2 / (period + 1)
        res = [series[0]]
        for price in series[1:]:
            res.append(price * k + res[-1] * (1 - k))
        return res

    # 1. EMAs de Estrutura Curta e Média
    ema9_s = ema_series(closes, 9)
    ema21_s = ema_series(closes, 21)
    ema50_s = ema_series(closes, min(50, len(closes)))
    ema12_s = ema_series(closes, 12)
    ema26_s = ema_series(closes, 26)
    ema9 = ema9_s[-1]
    ema21 = ema21_s[-1]
    ema50 = ema50_s[-1]

    # 2. FILTRO DE TENDÊNCIA MACRO (MA Trend 200 e WMA 50)
    trend_period = min(200, len(closes))
    ma_trend = sum(closes[-trend_period:]) / trend_period

    wma_period = min(50, len(closes))
    w_sub = closes[-wma_period:]
    wma50 = sum(w * p for w, p in enumerate(w_sub, 1)) / sum(range(1, wma_period + 1))

    # 3. FILTRO MANDATÓRIO MACD (12, 26, 9) - Com detecção de expansão do histograma
    macd_series = [a - b for a, b in zip(ema12_s, ema26_s)]
    macd_signal_series = ema_series(macd_series, 9)
    macd_hist_series = [m - s for m, s in zip(macd_series, macd_signal_series)]
    macd_val = macd_series[-1]
    macd_sig = macd_signal_series[-1]
    macd_hist = macd_hist_series[-1]
    macd_hist_prev = macd_hist_series[-2] if len(macd_hist_series) >= 2 else macd_hist

    macd_expanding_bull = (macd_val > macd_sig) and (macd_hist > macd_hist_prev)
    macd_expanding_bear = (macd_val < macd_sig) and (macd_hist < macd_hist_prev)

    # 4. Bandas de Bollinger (20, 2)
    bb_window = closes[-20:]
    bb_mid = sum(bb_window) / len(bb_window)
    bb_var = sum((x - bb_mid) ** 2 for x in bb_window) / len(bb_window)
    bb_std = math.sqrt(bb_var)
    bb_upper = bb_mid + 2 * bb_std
    bb_lower = bb_mid - 2 * bb_std

    # 5. RSI 7 (Momento curto Olho de Deus) e RSI 14
    def calc_rsi(series, period):
        gains = []
        losses = []
        for i in range(1, len(series)):
            diff = series[i] - series[i - 1]
            if diff >= 0:
                gains.append(diff)
                losses.append(0.0)
            else:
                gains.append(0.0)
                losses.append(abs(diff))
        avg_gain = sum(gains[-period:]) / period if len(gains) >= period else 0.0001
        avg_loss = sum(losses[-period:]) / period if len(losses) >= period else 0.0001
        rs = avg_gain / (avg_loss if avg_loss != 0 else 0.0001)
        return 100 - (100 / (1 + rs))

    rsi7 = calc_rsi(closes, 7)
    rsi14 = calc_rsi(closes, 14)

    # 6. ATR (14) - Volatilidade em Expansão
    tr_list = []
    for i in range(1, len(candles)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1])
        )
        tr_list.append(tr)
    atr14 = sum(tr_list[-14:]) / 14 if len(tr_list) >= 14 else (sum(tr_list) / len(tr_list) if tr_list else 0.0001)
    atr14_prev = sum(tr_list[-15:-1]) / 14 if len(tr_list) >= 15 else atr14
    atr_expansao = atr14 > atr14_prev

    # 7. Volume Acima da Média de 5
    vol_avg_5 = sum(volumes[-5:]) / 5 if len(volumes) >= 5 else 1.0
    vol_now = volumes[-1]
    vol_acima_media = vol_now > vol_avg_5

    # 8. Análise das Velas: Atual (Vela M1) e Penúltima
    ultima = candles[-1]
    penultima = candles[-2] if len(candles) >= 2 else ultima
    c_open = float(ultima["open"])
    c_close = float(ultima["close"])
    c_high = float(ultima["max"])
    c_low = float(ultima["min"])

    p_open = float(penultima["open"])
    p_close = float(penultima["close"])

    corpo = abs(c_close - c_open)
    corpo_prev = abs(p_close - p_open)
    amplitude = max(0.00001, c_high - c_low)
    pavio_superior = c_high - max(c_close, c_open)
    pavio_inferior = min(c_close, c_open) - c_low
    direcao_ultimo = "ALTA (Verde)" if c_close >= c_open else "BAIXA (Vermelha)"

    # 9. Padrão Engolfo Puro (Olho de Deus)
    engulf_buy = (
        (c_close > c_open) and (p_close < p_open) and
        (c_close >= p_open and c_open <= p_close) and
        (corpo >= corpo_prev)
    )
    engulf_sell = (
        (c_close < c_open) and (p_close > p_open) and
        (c_close <= p_open and c_open >= p_close) and
        (corpo >= corpo_prev)
    )

    # 10. Destaque de Força A+ Institucional
    bodies = [abs(float(c["close"]) - float(c["open"])) for c in candles]
    avg_body_10 = sum(bodies[-10:]) / 10 if len(bodies) >= 10 else (sum(bodies) / len(bodies) if bodies else 0.0001)
    force_now = (corpo >= 1.20 * avg_body_10) or (corpo >= 0.45 * atr14)

    # 11. Estocástico (14, 3, 3) & Williams %R (14) & CCI (14)
    lowest_low_14 = min(lows[-14:])
    highest_high_14 = max(highs[-14:])
    diff_stoch = (highest_high_14 - lowest_low_14) or 0.0001
    stoch_k = ((c_close - lowest_low_14) / diff_stoch) * 100
    stoch_d = stoch_k
    willr14 = ((highest_high_14 - c_close) / diff_stoch) * -100

    tp_list = [(h + l + c) / 3 for h, l, c in zip(highs, lows, closes)]
    tp_14 = tp_list[-14:]
    sma_tp = sum(tp_14) / 14
    mean_dev = sum(abs(x - sma_tp) for x in tp_14) / 14 or 0.0001
    cci14 = (tp_list[-1] - sma_tp) / (0.015 * mean_dev)

    # 12. ADX (14)
    dm_plus = []
    dm_minus = []
    for i in range(1, len(candles)):
        up_move = highs[i] - highs[i - 1]
        down_move = lows[i - 1] - lows[i]
        dm_plus.append(up_move if up_move > down_move and up_move > 0 else 0.0)
        dm_minus.append(down_move if down_move > up_move and down_move > 0 else 0.0)
    avg_dm_plus = sum(dm_plus[-14:]) / 14 if len(dm_plus) >= 14 else 0.0001
    avg_dm_minus = sum(dm_minus[-14:]) / 14 if len(dm_minus) >= 14 else 0.0001
    di_plus = (avg_dm_plus / atr14) * 100 if atr14 else 0
    di_minus = (avg_dm_minus / atr14) * 100 if atr14 else 0
    dx = abs(di_plus - di_minus) / ((di_plus + di_minus) or 0.0001) * 100
    adx14 = dx

    # 13. Classificação Olho de Deus Institucional
    olho_deus_status = "AGUARDAR"
    if engulf_buy and (c_close > ma_trend or c_close > ema21) and macd_expanding_bull:
        olho_deus_status = "⭐ OLHO DE DEUS A+ COMPRA (CALL) - ENGOLFO + TENDÊNCIA + MACD EXPANSIVO" if force_now else "🟢 OLHO DE DEUS COMPRA (CALL) - ENGOLFO CONFIRMADO COM MACD"
    elif engulf_sell and (c_close < ma_trend or c_close < ema21) and macd_expanding_bear:
        olho_deus_status = "⭐ OLHO DE DEUS A+ VENDA (PUT) - ENGOLFO + TENDÊNCIA + MACD EXPANSIVO" if force_now else "🔴 OLHO DE DEUS VENDA (PUT) - ENGOLFO CONFIRMADO COM MACD"
    elif engulf_buy:
        olho_deus_status = "ENGOLFO DE ALTA (AGUARDANDO CONFIRMAÇÃO MACD/TENDÊNCIA)"
    elif engulf_sell:
        olho_deus_status = "ENGOLFO DE BAIXA (AGUARDANDO CONFIRMAÇÃO MACD/TENDÊNCIA)"

    # 14. FRACTAIS DE WILLIAMS (5 VELAS) - NÍVEIS DINÂMICOS INSTITUCIONAIS
    fractais_topo = []
    fractais_fundo = []
    for i in range(2, len(candles) - 2):
        h = highs[i]
        l = lows[i]
        if h > highs[i-1] and h > highs[i-2] and h > highs[i+1] and h > highs[i+2]:
            fractais_topo.append(h)
        if l < lows[i-1] and l < lows[i-2] and l < lows[i+1] and l < lows[i+2]:
            fractais_fundo.append(l)

    ultimo_fractal_topo = fractais_topo[-1] if fractais_topo else max(highs[-10:])
    ultimo_fractal_fundo = fractais_fundo[-1] if fractais_fundo else min(lows[-10:])

    # Rejeição e Rompimento de Fractal na Vela Atual
    rejeicao_fractal_topo = (c_high >= ultimo_fractal_topo * 0.9998 and c_close < ultimo_fractal_topo and pavio_superior > corpo * 0.8)
    rejeicao_fractal_fundo = (c_low <= ultimo_fractal_fundo * 1.0002 and c_close > ultimo_fractal_fundo and pavio_inferior > corpo * 0.8)
    rompimento_fractal_alta = (c_close > ultimo_fractal_topo and p_close <= ultimo_fractal_topo)
    rompimento_fractal_baixa = (c_close < ultimo_fractal_fundo and p_close >= ultimo_fractal_fundo)

    fractal_status = "Neutro"
    if rejeicao_fractal_fundo:
        fractal_status = f"⭐ REJEIÇÃO EM FUNDO FRACTAL ({ultimo_fractal_fundo:.5f}) - Suporte Dinâmico Ativado (CALL)"
    elif rejeicao_fractal_topo:
        fractal_status = f"⭐ REJEIÇÃO EM TOPO FRACTAL ({ultimo_fractal_topo:.5f}) - Resistência Dinâmica Ativada (PUT)"
    elif rompimento_fractal_alta:
        fractal_status = f"🟢 ROMPIMENTO DE TOPO FRACTAL ({ultimo_fractal_topo:.5f}) - Fluxo Comprador (CALL)"
    elif rompimento_fractal_baixa:
        fractal_status = f"🔴 ROMPIMENTO DE FUNDO FRACTAL ({ultimo_fractal_fundo:.5f}) - Fluxo Vendedor (PUT)"

    # Suporte e Resistência Macro (60 velas) e Micro (10 velas)
    resistencia_macro = max(highs)
    suporte_macro = min(lows)
    resistencia_micro = max(highs[-10:])
    suporte_micro = min(lows[-10:])
    ultimo_preco = closes[-1]

    # Padrão Adicional de Pavios / Price Action
    padrao = "Neutro"
    if pavio_inferior > 2 * corpo and pavio_superior < 0.3 * corpo:
        padrao = "Martelo / Pin Bar de Alta (Forte Rejeição Inferior de Preço)"
    elif pavio_superior > 2 * corpo and pavio_inferior < 0.3 * corpo:
        padrao = "Estrela Cadente / Pin Bar de Baixa (Forte Rejeição Superior de Preço)"
    elif corpo > 0.75 * amplitude:
        padrao = f"Marubozu de {direcao_ultimo} (Fluxo Institucional Dominante)"
    elif corpo < 0.12 * amplitude:
        padrao = "Doji (Exaustão e Indecisão em Zona de Briga)"
    elif engulf_buy:
        padrao = "Engolfo de Alta Institucional (Olho de Deus)"
    elif engulf_sell:
        padrao = "Engolfo de Baixa Institucional (Olho de Deus)"

    if ema9 > ema21 and ema21 > ema50:
        tendencia = "ALTA ESTRUTURAL (EMA9 > EMA21 > EMA50)"
    elif ema9 < ema21 and ema21 < ema50:
        tendencia = "BAIXA ESTRUTURAL (EMA9 < EMA21 < EMA50)"
    else:
        tendencia = "CONSOLIDAÇÃO / LATERALIDADE OTC"

    order_block_info = "Bullish Order Block em teste" if direcao_ultimo == "ALTA (Verde)" and rsi14 < 40 else ("Bearish Order Block em teste" if direcao_ultimo == "BAIXA (Vermelha)" and rsi14 > 60 else "Fluxo de Continuidade SMC")

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
        "engulf_buy": engulf_buy,
        "engulf_sell": engulf_sell,
        "body_now": round(corpo, 5),
        "body_prev": round(corpo_prev, 5),
        "avg_body_10": round(avg_body_10, 5),
        "atr_14": round(atr14, 5),
        "force_now": force_now,
        "is_aplus": force_now,
        "ma_trend_200": round(ma_trend, 5),
        "wma_50": round(wma50, 5),
        "ema9": round(ema9, 5),
        "ema21": round(ema21, 5),
        "ema50": round(ema50, 5),
        "macd": round(macd_val, 6),
        "macd_signal": round(macd_sig, 6),
        "macd_hist": round(macd_hist, 6),
        "macd_hist_prev": round(macd_hist_prev, 6),
        "macd_expanding_bull": macd_expanding_bull,
        "macd_expanding_bear": macd_expanding_bear,
        "rsi7": round(rsi7, 2),
        "rsi14": round(rsi14, 2),
        "adx14": round(adx14, 2),
        "adx_forte": adx14 >= 22,
        "stoch_k": round(stoch_k, 2),
        "stoch_d": round(stoch_d, 2),
        "cci14": round(cci14, 2),
        "willr14": round(willr14, 2),
        "vol_now": round(vol_now, 2),
        "vol_avg_5": round(vol_avg_5, 2),
        "vol_acima_media": vol_acima_media,
        "olho_deus_classificacao": olho_deus_status,
        "bb_upper": round(bb_upper, 5),
        "bb_middle": round(bb_mid, 5),
        "bb_lower": round(bb_lower, 5),
        "tendencia_emas": tendencia,
        "suporte_macro_60": round(suporte_macro, 5),
        "resistencia_macro_60": round(resistencia_macro, 5),
        "suporte_micro": round(suporte_micro, 5),
        "resistencia_micro": round(resistencia_micro, 5),
        "padrao_detectado": padrao,
        "smc_order_block": order_block_info,
        "ultimo_fractal_topo": round(ultimo_fractal_topo, 5),
        "ultimo_fractal_fundo": round(ultimo_fractal_fundo, 5),
        "fractal_status": fractal_status,
        "rejeicao_fractal_topo": rejeicao_fractal_topo,
        "rejeicao_fractal_fundo": rejeicao_fractal_fundo,
        "rompimento_fractal_alta": rompimento_fractal_alta,
        "rompimento_fractal_baixa": rompimento_fractal_baixa,
        "distancia_suporte": round(abs(ultimo_preco - suporte_micro), 5),
        "distancia_resistencia": round(abs(resistencia_micro - ultimo_preco), 5),
        "candles": candles[-60:],
        "qtd_velas": len(candles)
    }

async def consultar_gemini(ativo: str, dados: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not state.gemini_key:
        state.gemini_status = "Desativado (GEMINI_API_KEY não configurada)"
        return None

    candidate_models = ["gemini-2.5-flash", "gemini-3-flash-preview", "gemini-flash-lite-latest"]
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

[ESTRATÉGIA 'OLHO DE DEUS - ENGULFING + TENDÊNCIA A+' (v3)]:
- Padrão Engolfo Puro:
  * Engolfo de Alta (Bullish Engulfing): {dados.get('engulf_buy')}
  * Engolfo de Baixa (Bearish Engulfing): {dados.get('engulf_sell')}
  * Tamanho Corpo Atual: {dados.get('body_now')} | Corpo Anterior: {dados.get('body_prev')}
- Selo de Força A+ Institucional:
  * Status A+: {dados.get('is_aplus')} (Corpo >= 1.2x Média 10 [{dados.get('avg_body_10')}] OU >= 0.45x ATR14 [{dados.get('atr_14')}])
- FILTRO MANDATÓRIO MACD (12, 26, 9) PARA ACERTAR MAIS:
  * Linha MACD: {dados.get('macd')} | Linha Sinal: {dados.get('macd_signal')}
  * Histograma Atual: {dados.get('macd_hist')} | Histograma Anterior: {dados.get('macd_hist_prev')}
  * MACD Expansivo de Alta (Linha > Sinal e Histograma Crescente): {dados.get('macd_expanding_bull')}
  * MACD Expansivo de Baixa (Linha < Sinal e Histograma Decrescente): {dados.get('macd_expanding_bear')}
- Classificação Direta Olho de Deus: {dados.get('olho_deus_classificacao')}

[ANATOMIA DO ÚLTIMO CANDLE]:
- Abertura: {c.get('open')} | Fechamento: {c.get('close')} | Máxima: {c.get('high')} | Mínima: {c.get('low')}
- Direção: {c.get('direcao')} | Tamanho do Corpo: {c.get('corpo')}
- Pavio Superior (Rejeição de Topo): {c.get('pavio_superior')}
- Pavio Inferior (Rejeição de Fundo): {c.get('pavio_inferior')}
- Padrão de Price Action Reconhecido: {dados.get('padrao_detectado')}

[FRACTAIS DE WILLIAMS & NÍVEIS DINÂMICOS]:
- Topo Fractal Mais Recente (Resistência Dinâmica): {dados.get('ultimo_fractal_topo')}
- Fundo Fractal Mais Recente (Suporte Dinâmico): {dados.get('ultimo_fractal_fundo')}
- Status e Confluência de Fractais: {dados.get('fractal_status')}
- Rejeição de Topo Fractal (Gatilho PUT): {dados.get('rejeicao_fractal_topo')}
- Rejeição de Fundo Fractal (Gatilho CALL): {dados.get('rejeicao_fractal_fundo')}
- Rompimento de Topo Fractal (Fluxo CALL): {dados.get('rompimento_fractal_alta')}
- Rompimento de Fundo Fractal (Fluxo PUT): {dados.get('rompimento_fractal_baixa')}

[INDICADORES TÉCNICOS E ESTRUTURA]:
- Filtro de Tendência Macro MA 200: {dados.get('ma_trend_200')} | WMA 50: {dados.get('wma_50')}
- Estrutura de Médias Exponenciais: EMA 9 ({dados.get('ema9')}) | EMA 21 ({dados.get('ema21')}) | EMA 50 ({dados.get('ema50')})
- RSI 7 (Curto): {dados.get('rsi7')} | RSI 14: {dados.get('rsi14')}
- ADX 14: {dados.get('adx14')} (Tendência Forte: {dados.get('adx_forte')})
- Estocástico (14,3,3): %K={dados.get('stoch_k')} | %D={dados.get('stoch_d')}
- CCI 14: {dados.get('cci14')} | Williams %R: {dados.get('willr14')}
- Volume vs SMA 5: Atual={dados.get('vol_now')} | Média={dados.get('vol_avg_5')} (Acima da Média: {dados.get('vol_acima_media')})
- Bandas de Bollinger (20,2): Superior={dados.get('bb_upper')} | Média={dados.get('bb_middle')} | Inferior={dados.get('bb_lower')}
- Tendência Institucional: {dados.get('tendencia_emas')}
- Smart Money Concepts (SMC): {dados.get('smc_order_block')}
- Suporte Macro: {dados.get('suporte_macro_60')} | Resistência Macro: {dados.get('resistencia_macro_60')}

[SÉRIE HISTÓRICA OHLC ({len(candles_list)} VELAS M1)]:
{ohlc_bloco}

[ESTRATÉGIA MANDATÓRIA DE ENTRADA NOS 30 SEGUNDOS FINAIS]:
- A ordem será executada perto dos 30 segundos finais da vela (24s a 28.5s) na melhor taxa de retração do pavio antes da virada da vela M1.
- Para PUT: A vela deve esticar para cima para vender no topo.
- Para CALL: A vela deve retrair para baixo para comprar no fundo.

Decida se a melhor oportunidade é CALL, PUT ou AGUARDAR avaliando TODOS OS GATILHOS INSTITUCIONAIS (Smart Money Concepts / Order Blocks, Price Action / Pin Bar / Reversões / Rompimento Marubozu, Bandas de Bollinger com RSI, Olho de Deus A+ e o filtro de momentum do MACD).
IMPORTANTE: O 'Olho de Deus' é apenas mais um prompt de força extra (A+), NÃO espere por ele! Se qualquer outro padrão institucional de Price Action, Order Block ou Bollinger/RSI estiver presente com MACD favorável, emita CALL ou PUT com confluência >= 75%!
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

    async with httpx.AsyncClient(timeout=httpx.Timeout(12.0, connect=5.0)) as client:
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
                elif resp.status_code == 429:
                    state.gemini_status = f"Cota temporária ({model})"
                    continue
                else:
                    state.gemini_status = f"Erro HTTP {resp.status_code} ({model})"
                    print(f"[GEMINI ERR] {model} HTTP {resp.status_code}: {resp.text[:200]}", flush=True)
            except Exception as ex:
                state.gemini_status = f"Falha ({model}): {type(ex).__name__}"
                print(f"[GEMINI EXC] {model}: {type(ex).__name__} ({ex})", flush=True)
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

    # 1. Sinal identificado (M1 com entrada nos 30s finais)
    agora_ts = time.time()
    agora = datetime.now()
    seg_atual = agora.second

    if seg_atual <= 22:
        # A vela acabou de nascer ou está nos primeiros segundos: executa na retração dos 30s desta mesma vela!
        seg_ate_virada = 0
        target_virada_epoch = agora_ts
    else:
        # A vela já passou dos 22 segundos: programa para o nascimento da próxima vela M1
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
    start_pb = time.time() - (datetime.now().second if seg_ate_virada == 0 else 0)
    max_pb_segundos = 28.5
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
        limite_confluencia = 70 if state.current_gale_step > 0 else 75

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
            gem_timeout = 90.0 if state.gemini_key.startswith("sk-") else 24.0
            analise = await asyncio.wait_for(consultar_gemini(ativo, dados_60), timeout=gem_timeout)
        except Exception as ex:
            state.gemini_status = f"Atraso/Timeout: {type(ex).__name__}"
            print(f"[GEMINI TIMEOUT] {ativo}: {ex}", flush=True)

        if not analise or not isinstance(analise, dict) or analise.get("decisao") not in ["CALL", "PUT", "AGUARDAR"]:
            # NÃO desliga o robô por timeout ou atraso passageiro! Mantém o robô ativo e reanalisa no próximo ciclo.
            state.active_signal = None
            state.current_observation = f"[Gemini AI] {state.gemini_status}. Reanalisando próximo candle de {ativo} sem desligar..."
            print(f"[IA REAVALIANDO] {state.current_observation}", flush=True)
            await asyncio.sleep(2)
            return

        conf = analise.get("confluencia_porcentagem", 0)
        decisao = analise.get("decisao")
        olho_badge = dados_60.get("olho_deus_classificacao", "")
        obs_txt = analise.get("texto_observacao") or f"[{ativo} | {agora_str}] Decisão: {decisao} ({conf}% confluência) - {olho_badge}"
        state.current_observation = obs_txt
        print(f"    [ANÁLISE 0s LAG] {ativo}: {decisao} ({conf}%) | {analise.get('padrao_vela', '')} | {olho_badge} | MACD: {dados_60.get('macd')}/{dados_60.get('macd_hist')}", flush=True)

        if decisao in ["CALL", "PUT"] and conf >= limite_confluencia and not state.is_trading and not state.is_paused:
            seg_agora = int(time.time()) % 60
            # Se virou a vela durante a análise, mas ainda está no início (<= 25s), entra com perfeição nesta nova vela!
            if seg_agora > 29 and (int(time.time()) // 60) != minuto_inicio_analise:
                print(f" -> [ATRASO EXCESSIVO] {ativo}: a vela virou e já passou da janela de 30s ({seg_agora}s). Reavaliando próximo candle...", flush=True)
                return

            state.active_asset = ativo
            state.recently_traded_assets[ativo] = time.time()
            print(f" -> [SINAL CONFIRMADO] {ativo}: {decisao} ({conf}%). Operado ({len(state.recently_traded_assets)}/{len(pool_otc)} no ciclo).", flush=True)

            motivo = f"Confluência {conf}%: [{analise.get('padrao_vela', 'Olho de Deus A+')}] {analise.get('psicologia_mercado', '')} | {olho_badge}"
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
            await asyncio.sleep(6)
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
    # Inicia desconectado para exibir sempre a Tela de Login inicial
    state.is_connected = False
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
    state.history_trades = []
    state.current_observation = "Métricas e histórico limpos. Robô pronto para novas oportunidades institucionais."
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM trade_results")
        conn.commit()
        cursor.execute("VACUUM")
        conn.close()
    except Exception as e:
        print(f"[DB RESET ERR] {e}", flush=True)
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
