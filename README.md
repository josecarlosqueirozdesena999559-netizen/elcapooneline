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

curl -H "x-api-key: 40402400kkj" -H "x-user-id: teste1" "http://localhost:8080/bullex/candles?active=EURUSD-OTC&interval=60&count=2"

npx wscat -c "ws://2.25.187.128:8080/ws/market?user_id=teste1&active=EURUSD-OTC&api_key=40402400kkj"
```

## Supabase integration
- O gateway aceita `SUPABASE_URL` e `SUPABASE_SERVICE_ROLE_KEY`.
- Quando essas variaveis estiverem definidas, o backend troca o `InMemoryUserStore` por `SupabaseUserStore`.
- O schema base para criar `users`, `bullex_connections` e `market_assets` esta em `backend/supabase_schema.sql`.
- O backend usa a `service_role key` no servidor. Nao use `publishable key` ou `anon key` para essa integracao.
