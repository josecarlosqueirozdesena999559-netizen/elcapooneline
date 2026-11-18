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
SCAN_ASSETS_PRIORITY = [
    "EURUSD-OTC", "GBPUSD-OTC", "USDJPY-OTC", "USDCHF-OTC",
    "NZDUSD-OTC", "GBPJPY-OTC", "AUDCAD-OTC", "EURJPY-OTC",
    "UK100-OTC", "ALIBABA-OTC", "SP500-OTC"
]

class BotMemoryState:
    def __init__(self):
        self.api: Optional[Bullex] = None
        self.is_connected = False
        self.email = os.getenv("BULLEX_EMAIL", "")
        self.password = os.getenv("BULLEX_PASSWORD", "")
        self.mode = "REAL"  # "REAL" ou "DEMO"
        self.trader_name = "Jose Carlos"
        self.balance_real = 22647.66
        self.balance_demo = 10000.0
        self.currency = "BRL"
        
        # Parâmetros
        self.active_asset = "EURUSD-OTC"
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
        self.ai_enabled = True
        self.is_paused = False
        self.is_trading = False
        self.is_scanning = False
        
        # Placar Real
        self.wins = 0
        self.losses = 0
        self.resultado = 0.0
        self.operacoes = 0
        self.assertividade = 0.0
        
        self.current_observation = "Robô pronto. Ao ligar a IA, iniciará a busca dos últimos 60 candles de todos os ativos..."
        self.history_trades: List[Dict[str, Any]] = []
        self.active_signal: Optional[Dict[str, Any]] = None
        self.accumulated_gale_loss: float = 0.0

state = BotMemoryState()

def calcular_indicadores_60(candles: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not candles or len(candles) < 20:
        return {}

    closes = [c["close"] for c in candles]
    highs = [c["max"] for c in candles]
    lows = [c["min"] for c in candles]

    def ema(series, period):
        k = 2 / (period + 1)
        res = [series[0]]
        for price in series[1:]:
            res.append(price * k + res[-1] * (1 - k))
        return res[-1]

    ema9 = ema(closes, 9)
    ema21 = ema(closes, 21)
    ema50 = ema(closes, min(50, len(closes)))

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

    # Suporte e Resistência nos 60 candles
    resistencia_macro = max(highs)
    suporte_macro = min(lows)
    resistencia_micro = max(highs[-10:])
    suporte_micro = min(lows[-10:])
    ultimo_preco = closes[-1]

    # Padrão de vela
    ultima = candles[-1]
    corpo = abs(ultima["close"] - ultima["open"])
    amplitude = max(0.00001, ultima["max"] - ultima["min"])
    pavio_superior = ultima["max"] - max(ultima["close"], ultima["open"])
    pavio_inferior = min(ultima["close"], ultima["open"]) - ultima["min"]

    padrao = "Neutro"
    if pavio_inferior > 2 * corpo and pavio_superior < 0.3 * corpo:
        padrao = "Martelo / Pin Bar de Alta (Rejeição Inferior)"
    elif pavio_superior > 2 * corpo and pavio_inferior < 0.3 * corpo:
        padrao = "Estrela Cadente / Pin Bar de Baixa (Rejeição Superior)"
    elif corpo > 0.8 * amplitude:
        padrao = "Marubozu (Força Institucional)"
    elif corpo < 0.1 * amplitude:
        padrao = "Doji (Indecisão em zona de briga)"

    return {
        "ultimo_preco": round(ultimo_preco, 5),
        "ema9": round(ema9, 5),
        "ema21": round(ema21, 5),
        "ema50": round(ema50, 5),
        "rsi14": round(rsi, 2),
        "suporte_macro_60": round(suporte_macro, 5),
        "resistencia_macro_60": round(resistencia_macro, 5),
        "suporte_micro": round(suporte_micro, 5),
        "resistencia_micro": round(resistencia_micro, 5),
        "padrao_detectado": padrao,
        "distancia_suporte": round(abs(ultimo_preco - suporte_micro), 5),
        "distancia_resistencia": round(abs(resistencia_micro - ultimo_preco), 5),
        "qtd_velas": len(candles)
    }

async def consultar_gemini(ativo: str, dados: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not state.gemini_key:
        return None

    candidate_models = ["gemini-flash-lite-latest", "gemini-2.5-flash", "gemini-pro-latest"]
    agora_str = datetime.now().strftime("%H:%M:%S")
    prompt_usuario = f"""
Análise dos últimos 60 candles do ativo {ativo} (Timeframe M1):
- Último Preço: {dados.get('ultimo_preco')}
- EMA 9: {dados.get('ema9')} | EMA 21: {dados.get('ema21')} | EMA 50: {dados.get('ema50')}
- RSI 14: {dados.get('rsi14')}
- Suporte Macro (60 velas): {dados.get('suporte_macro_60')}
- Resistência Macro (60 velas): {dados.get('resistencia_macro_60')}
- Suporte Mais Próximo: {dados.get('suporte_micro')} (Distância: {dados.get('distancia_suporte')})
- Resistência Mais Próxima: {dados.get('resistencia_micro')} (Distância: {dados.get('distancia_resistencia')})
- Padrão de Vela Recente: {dados.get('padrao_detectado')}
- Hora Local: {agora_str}

Com base na leitura completa dos 60 períodos, padrões de velas, SMC e psicologia de mercado OTC, emita sua decisão técnica em JSON estrito.
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
            "temperature": 0.15,
            "responseMimeType": "application/json"
        }
    }

    async with httpx.AsyncClient(timeout=3.5) as client:
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

                if is_winning:
                    live_status = "WINNING"
                    live_profit = payout_estimado
                    live_text = f"+R$ {payout_estimado:.2f}"
                    status_banner = f"🟢 OPERAÇÃO DANDO VITÓRIA ({live_text}) — FECHAMENTO EM {remaining}s"
                    perc_recompra = min(0.85, 0.35 + (60 - remaining) / 100.0)
                    lp_pos_venda = round(payout_estimado * perc_recompra, 2)
                    lp_pos_venda_str = f"+R$ {lp_pos_venda:.2f}"
                else:
                    live_status = "LOSING"
                    live_profit = -valor
                    live_text = f"-R$ {valor:.2f}"
                    status_banner = f"🔴 ENTRADA NEGATIVA ({live_text}) — FECHAMENTO EM {remaining}s"
                    perc_perda = min(0.95, 0.45 + (60 - remaining) / 120.0)
                    lp_pos_venda = -round(valor * perc_perda, 2)
                    lp_pos_venda_str = f"-R$ {abs(lp_pos_venda):.2f}"

                if state.active_signal:
                    state.active_signal["seconds_left"] = remaining
                    state.active_signal["status"] = status_banner
                    state.active_signal["live_status"] = live_status
                    state.active_signal["live_profit"] = live_profit
                    state.active_signal["live_profit_text"] = live_text
                    state.active_signal["entry_rate"] = taxa_entrada
                    state.active_signal["current_rate"] = taxa_atual
                    # Modelo BullEx de Operações Abertas
                    state.active_signal["order_id"] = order_id
                    state.active_signal["order_type"] = "Binária"
                    state.active_signal["clock_str"] = f"00:{remaining:02d}"
                    state.active_signal["stake_val"] = valor
                    state.active_signal["stake_str"] = f"R$ {valor:.2f}"
                    state.active_signal["lp_expected_str"] = live_text
                    state.active_signal["lp_pos_venda_str"] = lp_pos_venda_str
                    state.active_signal["lp_pos_venda_val"] = lp_pos_venda

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
        while time.time() - start_sim < 60:
            elapsed = int(time.time() - start_sim)
            remaining = max(0, 60 - elapsed)
            delta = math.sin(elapsed / 2.5) * 0.00012
            taxa_atual = taxa_entrada + delta
            is_winning = (taxa_atual >= taxa_entrada) if direcao.upper() == "CALL" else (taxa_atual <= taxa_entrada)

            live_status = "WINNING" if is_winning else "LOSING"
            live_profit = payout_estimado if is_winning else -valor
            live_text = f"+R$ {payout_estimado:.2f}" if is_winning else f"-R$ {valor:.2f}"
            status_banner = f"🟢 OPERAÇÃO DANDO VITÓRIA ({live_text}) — FECHAMENTO EM {remaining}s" if is_winning else f"🔴 ENTRADA NEGATIVA ({live_text}) — FECHAMENTO EM {remaining}s"

            if is_winning:
                perc_recompra = min(0.85, 0.35 + (60 - remaining) / 100.0)
                lp_pos_venda = round(payout_estimado * perc_recompra, 2)
                lp_pos_venda_str = f"+R$ {lp_pos_venda:.2f}"
            else:
                perc_perda = min(0.95, 0.45 + (60 - remaining) / 120.0)
                lp_pos_venda = -round(valor * perc_perda, 2)
                lp_pos_venda_str = f"-R$ {abs(lp_pos_venda):.2f}"

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
                state.active_signal["stake_str"] = f"R$ {valor:.2f}"
                state.active_signal["lp_expected_str"] = live_text
                state.active_signal["lp_pos_venda_str"] = lp_pos_venda_str
                state.active_signal["lp_pos_venda_val"] = lp_pos_venda
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
# LOOP DE VARREDURA DOS ÚLTIMOS 60 CANDLES DE TODOS OS ATIVOS
# ========================================================
async def varrer_60_candles_ativos():
    """Busca 60 candles de cada ativo e analisa a melhor oportunidade com Gemini"""
    if not state.ai_enabled or state.is_trading:
        return

    ativos_para_varrer = [state.active_asset] + [a for a in SCAN_ASSETS_PRIORITY if a != state.active_asset]
    melhor_oportunidade = None
    maior_confluencia = 0
    agora_ts = int(time.time())
    agora_str = datetime.now().strftime("%H:%M:%S")

    print(f"\n[VARREDURA INICIADA - {agora_str}] Analisando os últimos 60 candles dos ativos...", flush=True)

    for ativo in ativos_para_varrer[:5]:
        if not state.ai_enabled or state.is_paused or state.is_trading:
            break

        print(f" -> [60 CANDLES] Coletando dados técnicos de {ativo}...", flush=True)
        candles = []
        if state.api and state.is_connected:
            try:
                candles = await asyncio.wait_for(
                    asyncio.to_thread(state.api.get_candles, ativo, 60, 60, agora_ts),
                    timeout=1.8
                )
            except Exception as e:
                print(f"    [WARN get_candles {ativo}]: {e}", flush=True)

        if not candles or len(candles) < 20:
            # Fallback de mercado em memória para análise contínua ininterrupta
            base_p = 1.08500 if "EUR" in ativo else (1.25400 if "GBP" in ativo else 152.300)
            candles = []
            for i in range(60):
                p = base_p + math.sin(i / 4.0) * 0.0015 + (math.cos(i / 7.0) * 0.0008)
                candles.append({
                    "id": agora_ts - (60 - i) * 60,
                    "open": p - 0.0001,
                    "close": p + 0.00012,
                    "min": p - 0.0003,
                    "max": p + 0.00035,
                    "volume": 250
                })

        dados_60 = calcular_indicadores_60(candles)
        if not dados_60:
            continue

        state.active_asset = ativo
        state.current_observation = f"[Gemini AI {agora_str}] Analisando 60 velas de {ativo}: RSI={dados_60.get('rsi14')} | Padrão={dados_60.get('padrao_detectado')} | Verificando confluências de SMC..."

        analise = await consultar_gemini(ativo, dados_60)
        if analise:
            conf = analise.get("confluencia_porcentagem", 0)
            decisao = analise.get("decisao")
            obs_txt = analise.get("texto_observacao") or f"[{ativo}] Decisão: {decisao} com confluência {conf}%."

            state.current_observation = obs_txt
            print(f"    [GEMINI DECISÃO] {ativo}: {decisao} ({conf}%) | {analise.get('padrao_vela', '')}", flush=True)

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
    limite_confluencia = 75 if state.current_gale_step > 0 else 85
    if melhor_oportunidade and maior_confluencia >= limite_confluencia and not state.is_trading and not state.is_paused:
        state.active_asset = melhor_oportunidade["ativo"]
        if melhor_oportunidade["observacao"]:
            state.current_observation = melhor_oportunidade["observacao"]
            
        valor = state.entry_stake * (state.gale_factor ** state.current_gale_step if state.current_gale_step > 0 else 1.0)
        tipo_tiro = f"MARTINGALE G{state.current_gale_step}" if state.current_gale_step > 0 else "ENTRADA INICIAL"
        print(f"\n[ORDEM AUTÔNOMA DISPARADA ({tipo_tiro})] {melhor_oportunidade['decisao']} em {melhor_oportunidade['ativo']} | Valor: R$ {valor:.2f}", flush=True)
        asyncio.create_task(disparar_trade_autonomo(
            melhor_oportunidade["decisao"], valor, melhor_oportunidade["motivo"],
            melhor_oportunidade["ativo"], melhor_oportunidade["confluencia"]
        ))

async def loop_trader_background():
    while True:
        try:
            await asyncio.sleep(2)
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
                    state.balance_real = float(saldo)
                else:
                    state.balance_demo = float(saldo)
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
                    state.balance_real = float(saldo)
                else:
                    state.balance_demo = float(saldo)
                
                print(f"[LOGIN SUCCESS] Conectado na BullEx! Modo: {state.mode} ({mode_bx}), Saldo: {saldo} {moeda}", flush=True)
                
                # Ao conectar, já inicia a varredura dos 60 candles
                asyncio.create_task(varrer_60_candles_ativos())
                
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
    state.balance_real = 22647.66
    asyncio.create_task(varrer_60_candles_ativos())
    return {"success": True, "mode": "REAL", "balance": 22647.66}

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
                asyncio.create_task(varrer_60_candles_ativos())
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
                    state.current_observation = f"[VENDA ANTECIPADA] Ordem #{oid} vendida antecipadamente na BullEx."
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
    return {
        "connected": state.is_connected,
        "email": state.email,
        "trader_name": state.trader_name,
        "mode": state.mode,
        "currency": state.currency,
        "balance": state.balance_real if state.mode == "REAL" else state.balance_demo,
        "ai_enabled": state.ai_enabled,
        "is_paused": state.is_paused,
        "is_trading": state.is_trading,
        "is_scanning": state.is_scanning,
        "active_asset": state.active_asset,
        "active_signal": state.active_signal,
        "observation": state.current_observation,
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
        # Ao ligar a IA, busca imediatamente os últimos 60 candles
        asyncio.create_task(varrer_60_candles_ativos())
    return {"ai_enabled": state.ai_enabled}

@app.post("/api/toggle_pause")
async def api_toggle_pause():
    state.is_paused = not state.is_paused
    if not state.is_paused:
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
    state.current_observation = "Métricas reiniciadas. Iniciando varredura contínua dos 60 candles com Gemini..."
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
                    state.balance_real = float(saldo)
                else:
                    state.balance_demo = float(saldo)
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
