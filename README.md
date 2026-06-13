# README - Documentação da API Bullex


<p align="center">
  <img src="https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcQ95L-8X9C0asoHEhh43alOrujfSpafjBJzX2Li-fxcnAmu9Ey4FHXwSJ3ibdzXLI7tKh8&usqp=CAU" alt="Logo Bullex API" width="200">
</p>


Este arquivo contém a documentação dos métodos disponíveis na classe `Bullex` e exemplos de como utilizá-los.

## Classe Principal
A classe principal para interagir com a API da Bullex é a `Bullex`.

---

## Inicialização
Para começar, inicialize a classe `Bullex` com seu e-mail e senha:

```python
from bullexapi.stable_api import Bullex

email = "seu_email@example.com"
senha = "sua_senha"
api = Bullex(email, senha)
```

---

## Métodos Disponíveis

### 1. Conexão e Sessão
- **`connect(sms_code=None)`**  
  Conecta à API. Retorna `(True, None)` em caso de sucesso ou `(False, mensagem)` em caso de falha.

- **`connect_2fa(sms_code)`**  
  Conecta utilizando autenticação de dois fatores (2FA).

- **`check_connect()`**  
  Verifica se a conexão está ativa. Retorna `True` ou `False`.

- **`set_session(header, cookie)`**  
  Define cabeçalhos e cookies para a sessão.

#### Exemplo:
```python
status, message = api.connect()
if status:
    print("Conectado com sucesso!")
else:
    print(f"Erro na conexão: {message}")
```

---

### 2. Informações da Conta
- **`get_balance()`**  
  Retorna o saldo da conta ativa.

- **`get_balance_mode()`**  
  Retorna o tipo de conta ativa (`REAL`, `PRACTICE`, `TOURNAMENT`).

- **`get_currency()`**  
  Retorna a moeda da conta ativa.

- **`change_balance(Balance_MODE)`**  
  Altera o tipo de conta ativa (`REAL`, `PRACTICE`, `TOURNAMENT`).

- **`reset_practice_balance()`**  
  Reseta o saldo da conta prática.

#### Exemplo:
```python
api.change_balance("PRACTICE")
print("Saldo:", api.get_balance())
print("Tipo de conta:", api.get_balance_mode())
```

---

### 3. Ativos e Instrumentos
- **`update_ACTIVES_OPCODE()`**  
  Atualiza os códigos dos ativos disponíveis.

- **`get_all_ACTIVES_OPCODE()`**  
  Retorna todos os ativos disponíveis.

- **`get_instruments(type)`**  
  Retorna instrumentos disponíveis para um tipo específico (`crypto`, `forex`, `cfd`).

- **`get_name_by_activeId(activeId)`**  
  Retorna o nome do ativo pelo ID.

#### Exemplo:
```python
api.update_ACTIVES_OPCODE()
ativos = api.get_all_ACTIVES_OPCODE()
print("Ativos disponíveis:", ativos)
```

---

### 4. Operações Binárias
- **`buy(price, ACTIVES, ACTION, expirations)`**  
  Executa uma operação binária.
  - `price`: Valor da operação.
  - `ACTIVES`: Nome do ativo.
  - `ACTION`: Direção (`"call"` ou `"put"`).
  - `expirations`: Tempo de expiração em minutos.

- **`check_win_v4(order_id)`**  
  Verifica o resultado de uma operação binária.

#### Exemplo:
```python
status, order_id = api.buy(1, "EURUSD", "call", 1)
if status:
    print("Ordem executada com sucesso!")
    result = api.check_win_v4(order_id)
    print("Resultado:", result)
```

---

### 5. Operações Digitais
- **`buy_digital_spot(active, amount, action, duration)`**  
  Executa uma operação digital.
  - `active`: Nome do ativo.
  - `amount`: Valor da operação.
  - `action`: Direção (`"call"` ou `"put"`).
  - `duration`: Duração em minutos.

- **`check_win_digital_v2(order_id)`**  
  Verifica o resultado de uma operação digital.

#### Exemplo:
```python
status, order_id = api.buy_digital_spot("EURUSD", 1, "call", 1)
if status:
    print("Ordem digital executada com sucesso!")
    result = api.check_win_digital_v2(order_id)
    print("Resultado:", result)
```

---

### 6. Histórico e Velas
- **`get_candles(ACTIVES, interval, count, endtime)`**  
  Retorna o histórico de candles.
  - `ACTIVES`: Nome do ativo.
  - `interval`: Intervalo em segundos (ex.: `60` para 1 minuto).
  - `count`: Número de candles.
  - `endtime`: Timestamp final.

- **`start_candles_stream(ACTIVE, size, maxdict)`**  
  Inicia o stream de candles em tempo real.

- **`stop_candles_stream(ACTIVE, size)`**  
  Para o stream de candles.

#### Exemplo:
```python
candles = api.get_candles("EURUSD", 60, 10, int(time.time()))
for candle in candles:
    print(candle)
```

---

### 7. Outros Métodos
- **`get_digital_payout(active, seconds=0)`**  
  Retorna o payout digital para um ativo.

- **`get_position_history(instrument_type)`**  
  Retorna o histórico de posições.

- **`logout()`**  
  Encerra a sessão.

- **`buy_blitz(active, price, direction, expiration)`**  
  Executa uma operação Blitz.
  - `active`: Nome do ativo (ex: `"GBPCAD-OTC"`).
  - `price`: Valor da operação.
  - `direction`: Direção (`"call"` ou `"put"`).
  - `expiration`: Tempo de expiração em segundos (ex: `3`, `5`, `10`).

#### Exemplo:
```python
resultado, id_ordem = api.buy_blitz("GBPCAD-OTC", 1, "call", 5)
if resultado:
    print("Ordem Blitz executada com sucesso! ID:", id_ordem)
else:
    print("Erro ao executar Blitz.")
```

---

## Notas
- Certifique-se de que a conexão está ativa antes de executar qualquer operação.
- Use `try-except` para capturar erros e garantir que o programa não seja interrompido inesperadamente.
---

## bullex-service MVP Safe Mode
- O `bullex-service` mantem sessoes em memoria por `user_id`.
- Nesta fase MVP, a `bullexapi` usa estado global interno. Por isso, toda chamada BullEx roda protegida por lock de processo.
- O isolamento atual e por `user_id` em memoria, protegido por lock, e nao por isolamento real de processo por usuario.
- A variavel de ambiente `BULLEX_MAX_CONCURRENT_API_CALLS` controla o limite de chamadas BullEx concorrentes.
- O valor recomendado para MVP e `BULLEX_MAX_CONCURRENT_API_CALLS=1`, que serializa as chamadas.
- Quando uma sessao nao existir para o `x-user-id`, a API retorna:

```json
{
  "ok": false,
  "data": null,
  "error": "SESSION_NOT_FOUND"
}
```

## Manual test disconnect flow
- `POST /bullex/connect`
- `GET /bullex/status` deve retornar `connected: true`
- `POST /bullex/disconnect`
- `GET /bullex/status` deve retornar `SESSION_NOT_FOUND` ou `connected: false`

## Manual test stable BullEx session
```bash
curl -X POST -H "x-api-key: CHAVE" -H "x-user-id: teste1" -H "Content-Type: application/json" \
  -d "{\"email\":\"EMAIL_BULLEX\",\"password\":\"SENHA_BULLEX\",\"account_mode\":\"PRACTICE\"}" \
  http://localhost:8080/bullex/connect

curl -H "x-api-key: CHAVE" -H "x-user-id: teste1" http://localhost:8080/bullex/account

docker compose restart bullex-service

curl -H "x-api-key: CHAVE" -H "x-user-id: teste1" http://localhost:8080/bullex/account

curl -X POST -H "x-api-key: CHAVE" -H "x-user-id: teste1" -H "Content-Type: application/json" \
  -d "{\"email\":\"EMAIL_BULLEX\",\"password\":\"SENHA_BULLEX\",\"account_mode\":\"PRACTICE\"}" \
  http://localhost:8080/bullex/connect
```

Esperado:
- Antes da queda, `/bullex/account` retorna os dados da conta.
- Apos queda/restart do `bullex-service`, `/bullex/account` retorna erro controlado `SESSION_DISCONNECTED` ou `SESSION_NOT_FOUND`, nunca 500.
- Apos reconectar, `/bullex/account` volta a retornar dados da conta.

## Manual test market flow
```bash
curl http://localhost:8080/health

curl -H "x-api-key: CHAVE" -H "x-user-id: teste1" http://localhost:8080/bullex/assets

curl -H "x-api-key: CHAVE" -H "x-user-id: teste1" "http://localhost:8080/bullex/candles?active=EURUSD-OTC&interval=60&count=10"

curl -H "x-api-key: CHAVE" -H "x-user-id: teste1" "http://localhost:8080/bullex/candles?active=APPLE&interval=60&count=10"
```

Esperado:
- `assets` retorna no maximo 21 ativos binarios/OTC permitidos.
- `EURUSD-OTC` retorna candles quando a sessao BullEx esta conectada.
- `APPLE` retorna:

```json
{
  "ok": false,
  "data": null,
  "error": "ASSET_NOT_ALLOWED"
}
```

## Manual test market websocket
```bash
curl http://localhost:8080/health

curl -H "x-api-key: CHAVE" -H "x-user-id: teste1" "http://localhost:8080/bullex/candles?active=EURUSD-OTC&interval=60&count=2"

npx wscat -c "ws://localhost:8080/ws/market?user_id=teste1&active=EURUSD-OTC&api_key=CHAVE"
```

## Manual test signal engine
```bash
curl -H "x-api-key: CHAVE" -H "x-user-id: teste1" "http://localhost:8080/signals/analyze?active=EURUSD-OTC"

curl -H "x-api-key: CHAVE" -H "x-user-id: teste1" "http://localhost:8080/signals/scan"
```

## Robot cycle

O estado e mantido em memoria por `x-user-id`. O modo padrao e `DEMO`, o ciclo
padrao e de 10 minutos e operacoes REAL permanecem bloqueadas ate que todas as
confirmacoes sejam fornecidas.

Endpoints:
- `GET /robot/state`
- `GET /robot/history`
- `GET /robot/stats`
- `POST /robot/config`
- `POST /robot/start`
- `POST /robot/stop`
- `POST /robot/tick`
- `POST /robot/execute-demo`
- `POST /robot/execute-real`

Exemplo de configuracao DEMO:
```bash
curl -X POST \
  -H "x-api-key: CHAVE" \
  -H "x-user-id: teste1" \
  -H "Content-Type: application/json" \
  -d "{\"enabled\":true,\"account_mode\":\"DEMO\",\"entry_value\":2,\"cycle_minutes\":10,\"min_confidence\":85,\"min_payout\":80,\"stop_win\":50,\"stop_loss\":30}" \
  http://localhost:8080/robot/config
```

Uma ordem DEMO aceita fica com `operation_in_progress: true` e
`status: PENDING_RESULT`. O backend consulta o resultado real a cada 3 segundos
por ate 180 segundos. Quando a BullEx informa o fechamento:
- WIN incrementa `wins` e soma o lucro real.
- LOSS incrementa `losses` e desconta o valor perdido.
- `operation_in_progress` volta para `false`.
- O status volta para `WAITING_NEXT_CYCLE`.

`GET /robot/history?days=30` retorna `{"items": [...]}` com as operacoes
WIN/LOSS persistidas do usuario, da mais recente para a mais antiga. Os filtros
aceitos sao `days=1`, `days=7` e `days=30`. Um `order_id` finalizado nunca e
gravado duas vezes.

`GET /robot/stats?days=30` retorna wins, losses, total de operacoes, win rate,
lucro, profit factor e sequencias atuais/melhores de WIN e LOSS.

Para REAL, configure explicitamente `account_mode: REAL`, `allow_real: true` e
`confirm_real: true`, depois use `POST /robot/start`. O backend tambem exige a
sessao BullEx conectada em modo `REAL`. `entry_value` nao pode ultrapassar
`ROBOT_REAL_MAX_ENTRY`, cujo padrao e `10`.

`GET /robot/state` retorna `active_mode`, `real_ready` e
`real_block_reason` para o painel habilitar a acao REAL somente quando todas as
travas estiverem satisfeitas.

O campo `timeframe` aceita `M1`, `M5`, `M15` ou `M30` e controla tanto os
candles analisados quanto a expiracao enviada para a BullEx. Antes de cada
ordem, o robo usa `server_time` da sessao BullEx e so compra na janela final do
candle. Fora dela, o estado fica `WAITING_ENTRY_WINDOW` e informa
`seconds_until_entry_window`.

## Fase 17 - persistencia de sessao e robo

Antes de subir os containers, configure uma chave longa e estavel:

```env
BULLEX_SESSION_ENCRYPTION_KEY=uma-chave-secreta-longa-e-aleatoria
```

Essa chave cifra somente o SSID de sessao necessario para o reconnect. A senha
BullEx, a chave OpenAI e as chaves Supabase nunca sao gravadas no banco de
persistencia.

Rode novamente `backend/supabase_schema.sql` no Supabase. O script e
idempotente e cria as tabelas `robot_states`, `robot_trades`,
`robot_trade_history` e `robot_restore_status`.

O Docker Compose usa volumes nomeados para preservar o fallback SQLite:

- `backend-data`: estado, metricas, diagnostico e historico do robo.
- `bullex-session-data`: metadados e SSID BullEx cifrado.

Depois de um restart:

- `GET /robot/state` mantem `enabled=true`, restaura o ciclo e inclui
  `connected=true` quando a sessao BullEx foi recuperada.
- `GET /robot/persistence` retorna `session_restored`, `robot_restored` e
  `last_restore_at`.

## Manual test OpenAI signal reviewer
```bash
curl -H "x-api-key: CHAVE" -H "x-user-id: teste1" "http://localhost:8080/signals/review?active=EURUSD-OTC"

curl -H "x-api-key: CHAVE" -H "x-user-id: teste1" "http://localhost:8080/signals/top-reviewed"
```

## Supabase integration
- O gateway aceita `SUPABASE_URL` e `SUPABASE_SERVICE_ROLE_KEY`.
- Quando essas variaveis estiverem definidas, o backend troca o `InMemoryUserStore` por `SupabaseUserStore`.
- O schema base para criar `users`, `bullex_connections` e `market_assets` esta em `backend/supabase_schema.sql`.
- O backend usa a `service_role key` no servidor. Nao use `publishable key` ou `anon key` para essa integracao.
