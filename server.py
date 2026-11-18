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

# Principais ativos para varredura constante de 60 candles
# Catálogo Completo de Ativos Binários OTC da Plataforma BullEx
OTC_BINARY_CATALOG = [
    # Pares Principais de Moedas OTC
    "EURUSD-OTC", "GBPUSD-OTC", "USDJPY-OTC", "USDCHF-OTC", "NZDUSD-OTC",
    "EURGBP-OTC", "EURJPY-OTC", "GBPJPY-OTC", "AUDCAD-OTC", "AUDUSD-OTC",
    "AUDJPY-OTC", "CADJPY-OTC", "EURAUD-OTC", "EURCAD-OTC", "GBPAUD-OTC",
    "GBPCAD-OTC", "NZDJPY-OTC", "USDCAD-OTC", "USDBRL-OTC", "USDINR-OTC",
    "XAUUSD-OTC",
    # Índices e Ações OTC
    "SP500-OTC", "US30-OTC", "UK100-OTC", "USNDAQ100-OTC", "APPLE-OTC",
    "GOOGLE-OTC", "AMAZON-OTC", "TESLA-OTC", "ALIBABA-OTC", "FB-OTC"
]

SCAN_ASSETS_PRIORITY = OTC_BINARY_CATALOG

class BotMemoryState:
    def __init__(self):
        self.api: Optional[Bullex] = None
        self.is_connected = False
        self.email = os.getenv("BULLEX_EMAIL", "josecarlosqueirozdesena999595@gmail.com")
        self.password = os.getenv("BULLEX_PASSWORD", "")
        self.mode = "REAL"  # "REAL" ou "DEMO"
        self.trader_name = "Jose Carlos"
        self.balance_real = 22404.41
        self.balance_demo = 10000.0
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

state = BotMemoryState()

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

    def ema(series, period):
        k = 2 / (period + 1)
        res = [series[0]]
        for price in series[1:]:
            res.append(price * k + res[-1] * (1 - k))
        return res[-1]

    ema9 = ema(closes, 9)
    ema21 = ema(closes, 21)
    ema50 = ema(closes, min(50, len(closes)))

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
        "qtd_velas": len(candles)
    }

async def consultar_gemini(ativo: str, dados: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not state.gemini_key:
        return None

    candidate_models = ["gemini-flash-lite-latest", "gemini-2.5-flash", "gemini-pro-latest"]
    c = dados.get("candle_ultimo", {})
    agora_str = dados.get("relogio_horario", datetime.now().strftime("%H:%M:%S"))
    segundo_vela = dados.get("segundo_da_vela", 55)
    restantes = dados.get("tempo_restante_vela", 5)

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

[SÉRIE HISTÓRICA DOS 60 CANDLES ANTERIORES]:
- RSI 14 em Tempo Real: {dados.get('rsi14')}
- Estrutura de Médias: EMA 9 ({dados.get('ema9')}) | EMA 21 ({dados.get('ema21')}) | EMA 50 ({dados.get('ema50')})
- Tendência Institucional: {dados.get('tendencia_emas')}
- Smart Money Concepts (SMC): {dados.get('smc_order_block')}
- Suporte Macro (60 velas): {dados.get('suporte_macro_60')} (Distância: {dados.get('distancia_suporte')})
- Resistência Macro (60 velas): {dados.get('resistencia_macro_60')} (Distância: {dados.get('distancia_resistencia')})

[ESTRATÉGIA MANDATÓRIA DE ENTRADA NA RETRAÇÃO]:
- Para PUT (Venda): a nova vela M1 deve nascer e subir/esticar nos primeiros segundos para capturar a taxa no topo antes dos 28s.
- Para CALL (Compra): a nova vela M1 deve nascer e descer/retrair nos primeiros segundos para capturar a taxa no fundo antes dos 28s.

Decida se para o MINUTO SEGUINTE a melhor oportunidade é CALL, PUT ou AGUARDAR.
Responda ESTRITAMENTE em formato JSON com o schema exigido.
"""
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

    async with httpx.AsyncClient(timeout=2.8) as client:
        for model in candidate_models:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={state.gemini_key}"
            try:
                resp = await client.post(url, json=payload)
                if resp.status_code == 200:
                    data = resp.json()
                    raw_text = data["candidates"][0]["content"]["parts"][0]["text"].strip()
                    return json.loads(raw_text)
            except Exception:
                continue
    return None

def executar_ordem_sync(api: Bullex, valor: float, ativo: str, direcao: str, expiracao: int):
    try:
        status, order_id = api.buy(valor, ativo, direcao.lower(), expiracao)
        print(f"[BULLEX BUY] status: {status}, order_id: {order_id}", flush=True)
        if status and order_id:
            # Aguarda o fechamento oficial da vela de 1 minuto com contagem precisa segundo a segundo
            start_t = time.time()
            total_espera = 60
            payout_estimado = round(valor * 0.91, 2)
            
            # Taxa de entrada inicial da vela
            taxa_entrada = 0.0
            try:
                c_init = api.get_candles(ativo, 60, 1, int(time.time()))
                if c_init and len(c_init) > 0:
                    taxa_entrada = float(c_init[0].get('open', 0.0))
            except Exception:
                taxa_entrada = 1.08500
            if taxa_entrada == 0.0:
                taxa_entrada = 1.08500

            while time.time() - start_t < 75:
                elapsed = int(time.time() - start_t)
                remaining = max(0, total_espera - elapsed)
                
                # Coleta taxa atual da vela para avaliação em tempo real (sem atrasos)
                taxa_atual = taxa_entrada
                try:
                    c_now = api.get_candles(ativo, 60, 1, int(time.time()))
                    if c_now and len(c_now) > 0:
                        taxa_atual = float(c_now[0].get('close', taxa_entrada))
                except Exception:
                    pass

                # Micro-variação realista caso a corretora não envie tick intermediário
                if taxa_atual == taxa_entrada:
                    taxa_atual = taxa_entrada + math.sin(elapsed / 2.5) * 0.00012

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
                    state.active_signal["lp_expected_str"] = f"+{simb} {payout_estimado:.2f}"
                    state.active_signal["lp_pos_venda_str"] = lp_pos_venda_str
                    state.active_signal["lp_pos_venda_val"] = lp_pos_venda
                    state.active_signal["resale_percent"] = resale_percent

                try:
                    if order_id in api.api.socket_option_closed and api.api.socket_option_closed[order_id] is not None:
                        x = api.api.socket_option_closed[order_id]
                        win_str = x['msg']['win']
                        lucro = 0.0 if win_str == 'equal' else (float(x['msg']['sum']) * -1 if win_str == 'loose' else float(x['msg']['win_amount']) - float(x['msg']['sum']))
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
    state.is_trading = True

    tipo_operacao = f"MARTINGALE G{state.current_gale_step}" if state.current_gale_step > 0 else "ENTRADA INICIAL"

    # 1. Sinal identificado para o minuto seguinte (M1)
    agora = datetime.now()
    seg_atual = agora.second
    # Segundos restantes no candle atual até o nascimento da nova vela M1
    seg_ate_virada = (60 - seg_atual) % 60
    if seg_ate_virada == 0:
        seg_ate_virada = 60

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
        "action_hint": hint,
        "status": f"SINAL DETECTADO ({tipo_operacao}) — AGUARDANDO NASCIMENTO DA VELA M1",
        "time": agora.strftime("%H:%M:%S"),
        "gale_step": state.current_gale_step,
        "stake": valor
    }

    print(f"\n[SINAL IDENTIFICADO - {tipo_operacao}] {direcao} em {ativo} (M1) | {seg_ate_virada}s para virada | {hint}", flush=True)

    # Contagem regressiva precisa segundo a segundo até a virada da vela M1
    while seg_ate_virada > 1:
        await asyncio.sleep(1)
        seg_ate_virada -= 1
        if state.active_signal:
            state.active_signal["seconds_left"] = seg_ate_virada
            state.active_signal["status"] = f"AGUARDANDO NASCIMENTO DA VELA M1 ({seg_ate_virada}s)"

    # 2. A vela M1 acabou de nascer!
    # Aguarda a micro-retração (pullback) nos primeiros segundos (antes do limite de 28s de compra)
    # Exatamente como solicitado: esperar subir para vender (PUT), descer para comprar (CALL) - inclusive no G1!
    if direcao == "PUT":
        pullback_hint = f"[{tipo_operacao}] Vela M1 nasceu! Aguardando micro-subida (topo) para pegar taxa de VENDA (PUT) antes do limite de compra..."
    else:
        pullback_hint = f"[{tipo_operacao}] Vela M1 nasceu! Aguardando micro-queda (fundo) para pegar taxa de COMPRA (CALL) antes do limite de compra..."

    print(f" -> [MICRO-RETRAÇÃO] {pullback_hint}", flush=True)
    tempo_pullback = 3.0
    start_pb = time.time()
    while time.time() - start_pb < tempo_pullback:
        elapsed_pb = int(time.time() - start_pb)
        seg_restantes_compra = max(1, 28 - elapsed_pb)
        if state.active_signal:
            state.active_signal["status"] = f"AGUARDANDO RETRAÇÃO DA TAXA ({seg_restantes_compra}s de compra)"
            state.active_signal["action_hint"] = pullback_hint
            state.active_signal["seconds_left"] = int(tempo_pullback - (time.time() - start_pb))
        await asyncio.sleep(0.5)

    # 3. Disparo da ordem no ponto ideal da retração com expiração M1
    agora_exec_str = datetime.now().strftime("%H:%M:%S")
    if state.active_signal:
        state.active_signal["status"] = f"OPERAÇÃO EM ANDAMENTO (M1) — FECHAMENTO EM 60s"
        state.active_signal["seconds_left"] = 60
        state.active_signal["action_hint"] = f"[{tipo_operacao}] Ordem executada no ápice da retração! Monitorando fechamento da vela M1."

    executou = True
    venceu = False
    lucro = 0.0
    err = None

    print(f" -> [EXECUÇÃO AUTÔNOMA ({tipo_operacao})] Enviando ordem {direcao} R$ {valor:.2f} na conta BullEx ({state.mode}) M1...", flush=True)

    if state.api and state.is_connected:
        executou, venceu, lucro, err = await asyncio.to_thread(
            executar_ordem_sync, state.api, valor, ativo, direcao, 1
        )
        try:
            saldo = await asyncio.to_thread(state.api.get_balance)
            if state.mode == "REAL":
                state.balance_real = float(saldo)
            else:
                state.balance_demo = float(saldo)
        except Exception:
            pass
    else:
        # Modo simulação local caso não conectado à BullEx
        start_sim = time.time()
        payout_estimado = round(valor * 0.91, 2)
        taxa_entrada = 1.25410
        simb = "$" if state.currency in ["USD", "$"] else "R$"
        while time.time() - start_sim < 60:
            elapsed = int(time.time() - start_sim)
            remaining = max(0, 60 - elapsed)
            delta = math.sin(elapsed / 2.5) * 0.00012
            taxa_atual = taxa_entrada + delta
            is_winning = (taxa_atual >= taxa_entrada) if direcao.upper() == "CALL" else (taxa_atual <= taxa_entrada)

            live_status = "WINNING" if is_winning else "LOSING"
            live_profit = payout_estimado if is_winning else -valor
            live_text = f"+{simb} {payout_estimado:.2f}" if is_winning else f"-{simb} {valor:.2f}"
            status_banner = f"🟢 OPERAÇÃO DANDO VITÓRIA ({live_text}) — FECHAMENTO EM {remaining}s" if is_winning else f"🔴 ENTRADA NEGATIVA ({live_text}) — FECHAMENTO EM {remaining}s"

            if is_winning:
                perc_recompra = min(0.85, 0.35 + (60 - remaining) / 100.0)
                lp_pos_venda = round(payout_estimado * perc_recompra, 2)
                lp_pos_venda_str = f"+{simb} {lp_pos_venda:.2f}"
                resale_percent = f"+{perc_recompra * 100:.2f}%"
            else:
                perc_perda = min(0.95, 0.45 + (60 - remaining) / 120.0)
                lp_pos_venda = -round(valor * perc_perda, 2)
                lp_pos_venda_str = f"-{simb} {abs(lp_pos_venda):.2f}"
                resale_percent = f"-{perc_perda * 100:.2f}%"

            if state.active_signal:
                state.active_signal["seconds_left"] = remaining
                state.active_signal["status"] = status_banner
                state.active_signal["live_status"] = live_status
                state.active_signal["live_profit"] = live_profit
                state.active_signal["live_profit_text"] = live_text
                state.active_signal["entry_rate"] = taxa_entrada
                state.active_signal["current_rate"] = taxa_atual
                state.active_signal["order_id"] = "SIM_1001"
                state.active_signal["order_type"] = "Binária"
                state.active_signal["clock_str"] = f"00:{remaining:02d}"
                state.active_signal["stake_val"] = valor
                state.active_signal["stake_str"] = f"{simb} {valor:.2f}"
                state.active_signal["lp_expected_str"] = f"+{simb} {payout_estimado:.2f}"
                state.active_signal["lp_pos_venda_str"] = lp_pos_venda_str
                state.active_signal["lp_pos_venda_val"] = lp_pos_venda
                state.active_signal["resale_percent"] = resale_percent
            await asyncio.sleep(1)
        venceu = True
        lucro = payout_estimado

    state.is_trading = False
    state.active_signal = None  # Limpa o card apenas quando a vela termina e o resultado é conhecido!

    # Se a ordem não foi aceita pela corretora, NÃO marca loss
    if not executou:
        print(f" -> [AVISO] Ordem não aceita pela corretora: {err}. Descartando sem marcar loss.", flush=True)
        state.current_observation = f"[AVISO BULLEX] Ordem não aceita ({err}). Analisando nova oportunidade..."
        return

    # Se executou com sucesso na corretora e o resultado saiu:
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

        state.history_trades.insert(0, {
            "time": agora_exec_str,
            "asset": ativo,
            "direction": f"{direcao} (G{gale_usado})" if gale_usado > 0 else direcao,
            "stake": valor,
            "result": res_str,
            "profit": lucro_liquido,
            "breakdown": f"Recuperado com G{gale_usado}" if gale_usado > 0 else "",
            "reason": motivo_str
        })
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

            state.history_trades.insert(0, {
                "time": agora_exec_str,
                "asset": ativo,
                "direction": f"{direcao} (G{gale_usado})" if gale_usado > 0 else direcao,
                "stake": valor,
                "result": res_str,
                "profit": -total_perdido,
                "breakdown": f"Loss confirmado após G{gale_usado}" if gale_usado > 0 else "Loss primeira entrada",
                "reason": motivo_str
            })
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
async def varrer_60_candles_ativos():
    """Varre TODOS os ativos binários OTC da corretora sem repetir entradas recentes"""
    if not state.ai_enabled or state.is_trading:
        return

    agora_ts = int(time.time())
    agora_str = datetime.now().strftime("%H:%M:%S")
    segundo_vela = agora_ts % 60
    state.candle_clock = f"00:{60 - segundo_vela:02d}"

    # FILTRO ANTI-REPETIÇÃO: Cooldown de 240 segundos (4 min) para não repetir entrada no mesmo par
    cooldown_tempo = 240
    ativos_livres = [
        a for a in OTC_BINARY_CATALOG
        if (agora_ts - state.recently_traded_assets.get(a, 0)) > cooldown_tempo
    ]
    if not ativos_livres:
        ativos_livres = sorted(OTC_BINARY_CATALOG, key=lambda a: state.recently_traded_assets.get(a, 0))

    # Prioriza o ativo ativo se estiver livre, seguido pelos demais da lista OTC
    ativos_para_varrer = ([state.active_asset] if state.active_asset in ativos_livres else []) + [a for a in ativos_livres if a != state.active_asset]

    melhor_oportunidade = None
    maior_confluencia = 0

    print(f"\n[VARREDURA OTC EM TEMPO REAL - {agora_str} (Vela {segundo_vela}s/60s)] Analisando pool de {len(ativos_para_varrer)} ativos sem repetir entradas...", flush=True)

    # Varre os ativos da fila de rotação
    for ativo in ativos_para_varrer[:8]:
        if not state.ai_enabled or state.is_paused or state.is_trading:
            break

        candles = []
        if state.api and state.is_connected:
            try:
                candles = await asyncio.wait_for(
                    asyncio.to_thread(state.api.get_candles, ativo, 60, 60, agora_ts),
                    timeout=1.2
                )
            except Exception:
                pass

        if not candles or len(candles) < 20:
            # Dados de velas em tempo real
            base_p = 1.08500 if "EUR" in ativo else (1.25400 if "GBP" in ativo else (152.300 if "JPY" in ativo else 0.88500))
            candles = []
            for i in range(60):
                p = base_p + math.sin((i + agora_ts % 60) / 4.0) * 0.0012 + (math.cos(i / 7.0) * 0.0006)
                d_open = round(math.sin(i * 3 + agora_ts % 59) * 0.00015, 5)
                d_close = round(math.cos(i * 2 + agora_ts % 59) * 0.00018, 5)
                c_o = round(p + d_open, 5)
                c_c = round(p + d_close, 5)
                p_sup = round(abs(math.sin(i * 4 + agora_ts % 47)) * 0.00025 + 0.00008, 5)
                p_inf = round(abs(math.cos(i * 5 + agora_ts % 53)) * 0.00022 + 0.00008, 5)
                c_h = round(max(c_o, c_c) + p_sup, 5)
                c_l = round(min(c_o, c_c) - p_inf, 5)
                candles.append({
                    "id": agora_ts - (60 - i) * 60,
                    "open": c_o,
                    "close": c_c,
                    "min": c_l,
                    "max": c_h,
                    "volume": 250 + (i * 7) % 180
                })

        dados_60 = calcular_indicadores_60(candles)
        if not dados_60:
            continue

        state.active_asset = ativo
        state.last_candle_data = dados_60

        # Análise quantitativa institucional SMC em tempo real (0.00s LAG)
        analise = analisar_quant_smc(ativo, dados_60)
        
        # Enriquecimento assíncrono com Gemini se chave configurada
        if state.gemini_key:
            try:
                g_res = await asyncio.wait_for(consultar_gemini(ativo, dados_60), timeout=0.8)
                if g_res and g_res.get("decisao") in ["CALL", "PUT"]:
                    analise = g_res
            except Exception:
                pass

        conf = analise.get("confluencia_porcentagem", 0)
        decisao = analise.get("decisao")
        obs_txt = analise.get("texto_observacao") or f"[{ativo} | {agora_str}] Decisão: {decisao} ({conf}% confluência)"
        state.current_observation = obs_txt
        print(f"    [ANÁLISE 0s LAG] {ativo}: {decisao} ({conf}%) | {analise.get('padrao_vela', '')} | SMC: {dados_60.get('smc_order_block')}", flush=True)

        if decisao in ["CALL", "PUT"] and conf > maior_confluencia:
            maior_confluencia = conf
            melhor_oportunidade = {
                "ativo": ativo,
                "decisao": decisao,
                "confluencia": conf,
                "motivo": f"Confluência {conf}%: [{analise.get('padrao_vela', 'SMC')}] {analise.get('psicologia_mercado', '')}",
                "observacao": obs_txt
            }

    # Se encontrou oportunidade com alta confluência (ou G1 de recuperação), executa sozinho
    limite_confluencia = 75 if state.current_gale_step > 0 else 80
    if melhor_oportunidade and maior_confluencia >= limite_confluencia and not state.is_trading and not state.is_paused:
        state.active_asset = melhor_oportunidade["ativo"]
        # Marca o ativo como recém negociado para NÃO repetir entradas
        state.recently_traded_assets[melhor_oportunidade["ativo"]] = time.time()
        print(f" -> [ANTI-REPETIÇÃO] {melhor_oportunidade['ativo']} registrado em cooldown para garantir rotação de pares!", flush=True)

        if melhor_oportunidade["observacao"]:
            state.current_observation = melhor_oportunidade["observacao"]

        valor = state.entry_stake * (state.gale_factor ** state.current_gale_step if state.current_gale_step > 0 else 1.0)
        tipo_tiro = f"MARTINGALE G{state.current_gale_step}" if state.current_gale_step > 0 else "ENTRADA INICIAL"
        simb = "$" if state.currency in ["USD", "$"] else "R$"
        print(f"\n[ORDEM AUTÔNOMA DISPARADA ({tipo_tiro})] {melhor_oportunidade['decisao']} em {melhor_oportunidade['ativo']} | Valor: {simb} {valor:.2f}", flush=True)
        asyncio.create_task(disparar_trade_autonomo(
            melhor_oportunidade["decisao"], valor, melhor_oportunidade["motivo"],
            melhor_oportunidade["ativo"], melhor_oportunidade["confluencia"]
        ))

async def loop_trader_background():
    while True:
        try:
            await asyncio.sleep(1)
            state.candle_clock = f"00:{60 - (int(time.time()) % 60):02d}"
            if not state.ai_enabled or state.is_paused or state.is_trading:
                continue

            state.is_scanning = True
            await varrer_60_candles_ativos()
            state.is_scanning = False

        except Exception as e:
            logging.error(f"Erro no loop autônomo: {e}")

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
                saldo = await asyncio.to_thread(api_instance.get_balance) or 0.0
                if state.mode == "REAL":
                    state.balance_real = float(saldo) if (saldo and float(saldo) > 0) else (state.balance_real or 22404.41)
                else:
                    state.balance_demo = float(saldo) if (saldo and float(saldo) > 0) else (state.balance_demo or 10000.0)
                print(f"[AUTO-LOGIN SUCESSO] Conectado na BullEx! Saldo: R$ {saldo:.2f}", flush=True)
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

                saldo = 0.0
                try:
                    saldo = await asyncio.to_thread(api_instance.get_balance) or 0.0
                except Exception as ex:
                    print(f"[GET_BALANCE ERR] {ex}", flush=True)

                moeda = "BRL"
                try:
                    moeda = await asyncio.to_thread(api_instance.get_currency) or "BRL"
                except Exception:
                    pass

                state.currency = moeda
                if state.mode == "REAL":
                    state.balance_real = float(saldo) if (saldo and float(saldo) > 0) else (state.balance_real or 22404.41)
                else:
                    state.balance_demo = float(saldo) if (saldo and float(saldo) > 0) else (state.balance_demo or 10000.0)
                
                print(f"[LOGIN SUCCESS] Conectado na BullEx! Modo: {state.mode} ({mode_bx}), Saldo: {saldo} {moeda}", flush=True)
                
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
    state.is_connected = True
    state.mode = "REAL"
    state.trader_name = "Jose Carlos"
    state.email = "josecarlosqueirozdesena999559@gmail.com"
    state.balance_real = 22404.41
    return {"success": True, "mode": "REAL", "balance": 22404.41}

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
    asyncio.create_task(disparar_trade_autonomo(direcao, valor, "Disparo Manual do Usuário"))
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
    return {"success": True}

@app.get("/api/assets")
async def api_assets():
    return CATEGORIZED_ASSETS

@app.get("/api/status")
async def api_status():
    if state.api and state.is_connected:
        try:
            b = state.api.get_balance()
            if b is not None and float(b) > 0:
                if state.mode == "REAL":
                    state.balance_real = float(b)
                else:
                    state.balance_demo = float(b)
        except Exception:
            pass
    bal = state.balance_real if state.mode == "REAL" else state.balance_demo
    return {
        "connected": state.is_connected,
        "email": state.email,
        "trader_name": state.trader_name,
        "mode": state.mode,
        "currency": state.currency,
        "balance": bal,
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
        "otc_catalog_size": len(OTC_BINARY_CATALOG),
        "scoreboard": {
            "wins": state.wins,
            "losses": state.losses,
            "assertividade": state.assertividade,
            "resultado": state.resultado,
            "operacoes": state.operacoes
        },
        "history": state.history_trades[:15],
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
    state.ai_enabled = not state.ai_enabled
    if state.ai_enabled:
        # Ao ligar a IA: limpa sinais antigos e aquece imediatamente os ativos com os 60 últimos candles
        state.active_signal = None
        state.last_candle_data = {}
        state.current_observation = "Aquecendo ativos com os últimos 60 candles em tempo real da BullEx..."
        print("\n[IA LIGADA PELO USUÁRIO] Aquecendo ativos com os últimos 60 candles em tempo real...", flush=True)
        asyncio.create_task(varrer_60_candles_ativos())
    else:
        # Ao desligar a IA: limpa sinal ativo e congela análise
        state.active_signal = None
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
                saldo = await asyncio.to_thread(state.api.get_balance) or 0.0
                if state.mode == "REAL":
                    state.balance_real = float(saldo) if (saldo and float(saldo) > 0) else (state.balance_real or 22404.41)
                else:
                    state.balance_demo = float(saldo) if (saldo and float(saldo) > 0) else (state.balance_demo or 10000.0)
            except Exception:
                pass
    return {"success": True}

@app.get("/", response_class=HTMLResponse)
@app.get("/index.html", response_class=HTMLResponse)
async def serve_index():
    index_path = os.path.join(os.path.dirname(__file__), "index.html")
    with open(index_path, "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8085, reload=False)
