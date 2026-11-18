# -*- coding: utf-8 -*-
"""
Robô de Operações BullEx (Conta Real)
Integração direta com a API bullexapi sem telas adicionais.
Solicita email e senha da plataforma e opera diretamente na conta REAL.
"""

import os
import sys
import time
import getpass
from datetime import datetime

# Garante importação da bullexapi local ou instalada
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

try:
    from bullexapi.stable_api import Bullex
    import bullexapi.constants as OP_code
except ImportError as err:
    print(f"\n[ERRO] Não foi possível importar a biblioteca bullexapi: {err}")
    sys.exit(1)


def limpar_tela():
    os.system("cls" if os.name == "nt" else "clear")


def pedir_credenciais():
    """
    Solicita o email e senha real do usuário diretamente no terminal.
    """
    print("\n" + "=" * 65)
    print("      ROBÔ DE OPERAÇÕES BULL-EX - AUTENTICAÇÃO CONTA REAL")
    print("=" * 65)
    print("Insira as suas credenciais reais da plataforma BullEx:\n")

    email = input("  Email da BullEx: ").strip()
    while not email:
        print("  [!] O email não pode ficar vazio.")
        email = input("  Email da BullEx: ").strip()

    try:
        senha = getpass.getpass("  Senha da BullEx: ")
    except Exception:
        # Fallback caso getpass não funcione em algum terminal específico
        senha = input("  Senha da BullEx: ")

    while not senha:
        print("  [!] A senha não pode ficar vazia.")
        try:
            senha = getpass.getpass("  Senha da BullEx: ")
        except Exception:
            senha = input("  Senha da BullEx: ")

    return email, senha


def conectar_plataforma(email, senha):
    """
    Inicializa a Bullex, conecta e troca o saldo para a conta REAL.
    """
    print("\n[+] Inicializando conexão com a BullEx...")
    api = Bullex(email, senha)

    status, reason = api.connect()

    # Tratamento para 2FA caso esteja ativado na conta
    if not status and reason == "2FA":
        print("\n[!] Autenticação de 2 Fatores (2FA/SMS) solicitada pela plataforma.")
        codigo_2fa = input("  Digite o código SMS/2FA recebido: ").strip()
        status, reason = api.connect_2fa(codigo_2fa)

    if not status:
        print(f"\n[FALHA] Não foi possível conectar: {reason}")
        return None

    print("[✓] Conexão estabelecida com sucesso!")

    # Seleciona obrigatoriamente a Conta REAL
    print("[+] Alternando modo da conta para REAL...")
    try:
        api.change_balance("REAL")
        time.sleep(1)
    except Exception as e:
        print(f"[!] Erro ao alternar para conta REAL: {e}")
        return None

    # Validação do modo e saldo
    modo_ativo = api.get_balance_mode()
    saldo = api.get_balance()
    moeda = api.get_currency() or "BRL"

    print("\n" + "-" * 50)
    print(f"  Status da Conexão: CONECTADO")
    print(f"  Tipo de Conta:    {modo_ativo} (Ativa)")
    print(f"  Moeda da Conta:   {moeda}")
    print(f"  Saldo Atual REAL: {moeda} {saldo:.2f}")
    print("-" * 50)

    # Atualiza lista de ativos disponíveis
    try:
        print("[+] Atualizando códigos de ativos disponíveis...")
        api.update_ACTIVES_OPCODE()
    except Exception:
        pass

    return api


def executar_ordem(api, ativo, valor, direcao, expiracao_minutos):
    """
    Executa uma ordem binária e monitora o resultado.
    direcao: 'call' ou 'put'
    """
    moeda = api.get_currency() or "BRL"
    print(f"\n[ENVIANDO ORDEM] {ativo} | {direcao.upper()} | Valor: {moeda} {valor:.2f} | Exp: {expiracao_minutos}m")
    
    status, order_id = api.buy(valor, ativo, direcao.lower(), expiracao_minutos)
    if not status or not order_id:
        print(f"[FALHA NA ORDEM] A plataforma rejeitou a entrada: {order_id}")
        return False, 0.0

    print(f"[ORDEM REGISTRADA] ID: {order_id}. Aguardando fechamento da vela...")

    try:
        resultado, lucro = api.check_win_v4(order_id)
        if resultado == "win":
            print(f" [WIN] Parabéns! Resultado Positivo: +{moeda} {lucro:.2f}")
            return True, float(lucro)
        elif resultado == "loose":
            print(f" [LOSS] Resultado Negativo: -{moeda} {abs(lucro):.2f}")
            return False, -abs(float(valor))
        else:
            print(f" [EMPATE] Ordem empatada: {moeda} 0.00")
            return True, 0.0
    except Exception as e:
        print(f"[!] Erro ao checar resultado da ordem: {e}")
        return False, -abs(float(valor))


def analisar_tendencia_simples(api, ativo, timeframe_segundos=60):
    """
    Análise técnica rápida baseada nas últimas velas para definir direção (CALL ou PUT).
    """
    try:
        timestamp_atual = int(time.time())
        candles = api.get_candles(ativo, timeframe_segundos, 10, timestamp_atual)
        if not candles or len(candles) < 5:
            return None

        # Calcula média das últimas velas e direção das últimas 3
        fechamentos = [c["close"] for c in candles]
        ultimos_3 = candles[-3:]
        verdes = sum(1 for c in ultimos_3 if c["close"] > c["open"])
        vermelhas = sum(1 for c in ultimos_3 if c["close"] < c["open"])

        if verdes >= 2 and fechamentos[-1] > fechamentos[-4]:
            return "call"
        elif vermelhas >= 2 and fechamentos[-1] < fechamentos[-4]:
            return "put"
        return None
    except Exception:
        return None


def loop_operacoes(api):
    """
    Menu operacional do robô no terminal.
    """
    moeda = api.get_currency() or "BRL"

    print("\n" + "=" * 65)
    print("                 CONFIGURAÇÃO DO ROBÔ")
    print("=" * 65)

    ativo_padrao = "EURUSD"
    ativo_input = input(f"  Ativo a operar [Padrão: {ativo_padrao}]: ").strip().upper()
    ativo = ativo_input if ativo_input else ativo_padrao

    valor_str = input("  Valor da Entrada (ex: 5.0): ").strip()
    try:
        valor_base = float(valor_str) if valor_str else 5.0
    except ValueError:
        valor_base = 5.0

    exp_str = input("  Expiração em minutos (1, 5, etc.) [Padrão: 1]: ").strip()
    try:
        expiracao = int(exp_str) if exp_str else 1
    except ValueError:
        expiracao = 1

    stop_win_str = input(f"  Stop Win ({moeda}) [Padrão: 50.0]: ").strip()
    try:
        stop_win = float(stop_win_str) if stop_win_str else 50.0
    except ValueError:
        stop_win = 50.0

    stop_loss_str = input(f"  Stop Loss ({moeda}) [Padrão: 30.0]: ").strip()
    try:
        stop_loss = float(stop_loss_str) if stop_loss_str else 30.0
    except ValueError:
        stop_loss = 30.0

    usar_gale_str = input("  Ativar Martingale? (S/N) [Padrão: S]: ").strip().upper()
    usar_gale = usar_gale_str != "N"

    max_gale = 1
    mult_gale = 2.0
    if usar_gale:
        gale_steps_str = input("    Quantidade de Martingale (1 ou 2) [Padrão: 1]: ").strip()
        try:
            max_gale = int(gale_steps_str) if gale_steps_str else 1
        except ValueError:
            max_gale = 1

        gale_mult_str = input("    Multiplicador Martingale [Padrão: 2.0]: ").strip()
        try:
            mult_gale = float(gale_mult_str) if gale_mult_str else 2.0
        except ValueError:
            mult_gale = 2.0

    print("\n" + "-" * 65)
    print("  RESUMO DA OPERAÇÃO REAL:")
    print(f"  Ativo:       {ativo}")
    print(f"  Entrada:     {moeda} {valor_base:.2f}")
    print(f"  Expiração:   {expiracao} minuto(s)")
    print(f"  Stop Win:    +{moeda} {stop_win:.2f}")
    print(f"  Stop Loss:   -{moeda} {stop_loss:.2f}")
    print(f"  Martingale:  {'Ativado (Até G' + str(max_gale) + ' x' + str(mult_gale) + ')' if usar_gale else 'Desativado'}")
    print("-" * 65)

    print("\nEscolha o Modo de Operação:")
    print("  [1] Execução Manual de Ordem (Você define o momento e a direção CALL/PUT)")
    print("  [2] Modo Automático por Estratégia de Tendência (Velas M1)")
    print("  [0] Encerrar e Desconectar")

    opcao = input("\nSelecione a opção: ").strip()

    lucro_total = 0.0

    if opcao == "1":
        # Modo manual direto no terminal
        while True:
            saldo_atual = api.get_balance()
            print(f"\n[SALDO REAL]: {moeda} {saldo_atual:.2f} | [LUCRO SESSÃO]: {moeda} {lucro_total:+.2f}")
            direcao = input("Digite a direção para entrar na REAL ('C' para CALL, 'P' para PUT, 'S' para Sair): ").strip().upper()

            if direcao == "S":
                break
            if direcao not in ["C", "CALL", "P", "PUT"]:
                print("[!] Direção inválida. Digite C ou P.")
                continue

            dir_str = "call" if direcao in ["C", "CALL"] else "put"
            valor_atual = valor_base
            venceu, lucro = executar_ordem(api, ativo, valor_atual, dir_str, expiracao)
            lucro_total += lucro

            # Tratamento de Gale no modo manual
            if not venceu and usar_gale:
                for step in range(1, max_gale + 1):
                    valor_atual = valor_atual * mult_gale
                    print(f"\n[!] Acionando Martingale {step} (G{step}) no valor de {moeda} {valor_atual:.2f}...")
                    venceu_gale, lucro_gale = executar_ordem(api, ativo, valor_atual, dir_str, expiracao)
                    lucro_total += lucro_gale
                    if venceu_gale:
                        break

            # Verificação de Stops
            if lucro_total >= stop_win:
                print(f"\n[META ATINGIDA] Stop Win alcançado! Lucro total: {moeda} {lucro_total:.2f}")
                break
            if lucro_total <= -stop_loss:
                print(f"\n[STOP LOSS] Limite de perda atingido! Prejuízo acumulado: {moeda} {lucro_total:.2f}")
                break

    elif opcao == "2":
        # Modo automático por tendência
        print("\n[+] Modo Automático Ativado. Pressione Ctrl+C para parar a qualquer momento.")
        try:
            while True:
                saldo_atual = api.get_balance()
                print(f"\n[ANALISANDO {ativo}] Saldo: {moeda} {saldo_atual:.2f} | Sessão: {moeda} {lucro_total:+.2f}")
                direcao = analisar_tendencia_simples(api, ativo, timeframe_segundos=expiracao * 60)

                if direcao:
                    print(f"[SINAL DETECTADO] Tendência identificada: {direcao.upper()}")
                    valor_atual = valor_base
                    venceu, lucro = executar_ordem(api, ativo, valor_atual, direcao, expiracao)
                    lucro_total += lucro

                    if not venceu and usar_gale:
                        for step in range(1, max_gale + 1):
                            valor_atual = valor_atual * mult_gale
                            print(f"[!] Entrando no Gale {step} (G{step}) com {moeda} {valor_atual:.2f}...")
                            venceu_gale, lucro_gale = executar_ordem(api, ativo, valor_atual, direcao, expiracao)
                            lucro_total += lucro_gale
                            if venceu_gale:
                                break

                    # Checagem de Stops
                    if lucro_total >= stop_win:
                        print(f"\n[META ATINGIDA] Stop Win atingido! Lucro: {moeda} {lucro_total:.2f}")
                        break
                    if lucro_total <= -stop_loss:
                        print(f"\n[STOP LOSS] Limite de perda atingido! Prejuízo: {moeda} {lucro_total:.2f}")
                        break

                    print("[AGUARDANDO] Aguardando 15 segundos para próxima análise...")
                    time.sleep(15)
                else:
                    print("[AGUARDANDO] Sem sinal com confirmação de tendência. Aguardando 10 segundos...")
                    time.sleep(10)

        except KeyboardInterrupt:
            print("\n[!] Robô interrompido pelo usuário.")

    print("\n" + "=" * 65)
    print(f" Sessão Finalizada. Resultado Final: {moeda} {lucro_total:+.2f}")
    try:
        api.logout()
        print("[✓] Desconectado da plataforma BullEx com segurança.")
    except Exception:
        pass
    print("=" * 65)


def main():
    limpar_tela()
    email, senha = pedir_credenciais()
    api = conectar_plataforma(email, senha)

    if not api:
        print("\n[!] Encerrando robô devido a falha de conexão.")
        return

    try:
        loop_operacoes(api)
    except KeyboardInterrupt:
        print("\n[!] Encerrado.")
    except Exception as e:
        print(f"\n[ERRO INESPERADO]: {e}")


if __name__ == "__main__":
    main()
