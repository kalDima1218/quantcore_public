# brokersim — локальный брокер Finam Trade API для отладки инцидентов

Локальный gRPC-сервер, реализующий сервисы `tradeapi.v1` (auth, orders,
marketdata, accounts, assets, usage metrics) поверх **официального SDK**
`github.com/FinamWeb/finam-trade-api/go` — тех же protobuf-типов, которыми
говорит боевой клиент. Бот запускается против сима **без единого изменения
кода** — переключается одна переменная окружения.

Зачем: вся отказоустойчивость `execengine` (impaired-режим, идемпотентная
постановка, реплеи стримов, reconcile) в юнит-тестах проверяется фейками
Go-интерфейсов, а слой ниже — `grpcclient` + `trade/finam` (JWT-рефреш,
реконнекты стримов, сборка стакана из дельт, восстановление потерянных
ответов через GetOrders) — до сима можно было проверить только на живом
брокере с реальными деньгами. Сим замыкает этот разрыв: любой инцидент
воспроизводится локально, детерминированно и бесплатно.

## Быстрый старт

```bash
# 1. Поднять сим (дефолты: счёт SIM-1 / секрет sim-secret, символы создаются на лету)
go run ./cmd/finamsim

# 2. Направить бота на сим (plaintext, только loopback-отладка) — флаг зависит
#    от диспетчера конкретного бота, здесь для примера
QUANTCORE_FINAM_ADDR=127.0.0.1:50051 go run . -your-strategy-flag
```

В `config_strategy_a.json` при этом ставится `secret`/`account_id` из конфига
сима. Пустой `QUANTCORE_FINAM_ADDR` (дефолт) — боевой `api.finam.ru:443` с TLS;
переключение на сим громко логируется в `logs/finam.log`.

Конфиг сима (`go run ./cmd/finamsim -config sim.json`):

```json
{
  "accounts": [
    {"secret": "sim-secret", "account_id": "1000001", "initial_cash": 1000000, "portfolio_kind": "forts"}
  ],
  "symbols": [
    {
      "symbol": "LEGA@RTSX", "min_step": 1, "lot_size": 1, "margin_per_lot": 8000,
      "expiration_date": "2026-12-18",
      "auto_market": {"enabled": true, "mid": 90000, "spread": 2, "step": 1,
                       "levels": 5, "level_vol": 50, "interval": "200ms",
                       "walk": 3, "trade_prob": 0.3}
    },
    {
      "symbol": "SIM6@RTSX", "min_step": 1,
      "auto_market": {"enabled": true, "mid": 89800, "spread": 2, "step": 1,
                       "levels": 5, "level_vol": 50, "interval": "200ms",
                       "walk": 3, "trade_prob": 0.3}
    }
  ],
  "token_ttl": "15m",
  "exec_latency": "20ms",
  "place_quota_limit": 200
}
```

`auto_market` держит стакан и ленту живыми (случайное блуждание мида);
без него книга управляется только руками через control-plane — удобно для
детерминированных сценариев.

## Что воспроизведено из семантики боевого API

- **JWT**: `Auth(secret) → token` с TTL 15m, `TokenDetails` с настоящим
  `expires_at` (планировщик рефреша клиента работает как в проде); остальные
  RPC требуют metadata `Authorization` (голый токен, `Bearer` тоже принимается);
  протухший/отсутствующий токен — `UNAUTHENTICATED`. Стрим, открытый на токене,
  умирает вместе с ним (клиентские реконнекты пересоздают его уже с новым JWT).
- **PlaceOrder**: ответ `NEW` сразу, судьба ордера — асинхронно стримом
  (`exec_latency` спустя): пост-онли (GOOD_TILL_CROSSING), кроссящий спред,
  снимается биржей `REJECTED_BY_EXCHANGE`; маркет-ордер ест книгу, при
  частичной ликвидности недобор снимается (`CANCELED`, executed < initial) —
  сценарий «подтверждённо-мёртвого хеджа». `client_order_id` (≤20 символов,
  уникален в пределах счёта и дня, дубль — `ALREADY_EXISTS`) эхом возвращается
  в `OrderState.Order` — на нём работает идемпотентная постановка placer'а.
- **GetOrders** — только **активные** заявки (как документирован боевой API):
  восстановление потерянного ответа находит стоящий ордер, но у мгновенно
  исполнившегося остаётся документированная слепая зона fill-and-vanish
  (страхуется reconcile). `orders_list_includes_terminal: true` убирает её.
- **SubscribeOrders / SubscribeTrades**: снапшот активных ордеров / **реплей
  всех сегодняшних сделок** при подписке, дальше live. Реплей при каждом
  реконнекте — ровно тот burst, который обязан фильтровать `TradeDedup`.
- **SubscribeOrderBook**: первое сообщение — полный снапшот ADD-строками,
  дальше дельты (ADD/UPDATE/REMOVE, REMOVE несёт сторону в oneof); пустые
  ответы-heartbeat'ы каждые 10s.
- **Счёт**: `GetAccount`/`SubscribeAccount` с позициями (точные лоты, средняя),
  кэшем и портфелем `forts`/`mc`/`cash`/`none` (переключается `portfolio_kind` —
  для отладки всех веток `accountMargin`).
- **Квоты**: `GetUsageMetrics` с квотой `OrdersService.placeOrder`
  (лимит 200/мин, как в документации), постановки её расходуют, исчерпание —
  чистый `RESOURCE_EXHAUSTED`.
- **Schedule**: тип сессии (`CORE_TRADING` по умолчанию) переключается на лету —
  симуляция закрытия рынка для `IsMarketOpen`/`MonitorMarketOpen`.

Недокументированные боевым API детали (начальный статус NEW vs PENDING_NEW,
реакция на дубль client_order_id, судьба GTX при кроссе) зафиксированы в симе
наиболее консервативным для движка образом и помечены комментариями в коде.

## Control-plane: сценарии инцидентов

HTTP на `127.0.0.1:8062` (флаг `-control`). Все ручки — JSON.

| Ручка | Что делает |
|---|---|
| `GET /v1/state` | полный срез: счета, позиции, ордера, книги, активные сбои |
| `GET /v1/orders?account=` | ордера счёта |
| `POST /v1/faults` | добавить правило сбоя (см. ниже) |
| `GET /v1/faults` / `DELETE /v1/faults[?id=N]` | список / снятие правил |
| `POST /v1/book` | задать стакан целиком `{"symbol","bids":[[цена,объём],…],"asks":…}` |
| `POST /v1/book/cross` | вбросить кроссирующий бид (порча книги «пропущенным REMOVE») |
| `POST /v1/trade` | публичная печать `{"symbol","price","size","side"}` — уходит в ленту и филит задетые лимитники |
| `POST /v1/fill` | форс-фил `{"order_id","lots","price"}` (lots 0 — весь остаток) |
| `POST /v1/trade/inject` | вброс филла СВЕРХ терминала `{"order_id","lots","price"}` — брокер противоречит своему ack |
| `POST /v1/order/kill` | терминировать ордер брокером `{"order_id","status":"canceled\|rejected\|expired\|…"}` |
| `POST /v1/session` | тип сессии `{"type":"CLOSED"}` — закрыть рынок |
| `POST /v1/tokens/expire` | протухнуть все JWT (стримы на них умрут UNAUTHENTICATED) |
| `POST /v1/quota` | остаток квоты placeOrder `{"remaining":0}` |
| `POST /v1/automarket` | перенастроить/включить авторынок символа |

Правило сбоя: `{"method","action","code","message","delay","count","probability"}`.
`method` — короткое имя RPC (`PlaceOrder`, `CancelOrder`, `GetOrders`,
`SubscribeTrades`, `SubscribeOrderBook`, … или `*`), `count` — сколько раз
сработать (по умолчанию 1; `-1` — пока не снимут; для `silence`/`dup_events`
дефолт `-1`), `code` — число или строка (`"unavailable"`, `"deadline_exceeded"`…).

Действия:

- `error` — вернуть ошибку, **не применяя** запрос (чистый деловой отказ;
  дефолтный код `invalid_argument` — вне maybeDelivered-класса клиента,
  placer не пойдёт искать «потерянный» ордер);
- `drop_after_apply` — **применить** запрос и вернуть транспортную ошибку
  (потерянный ответ; дефолтный код `unavailable` — ровно из ambiguous-класса);
- `delay` — задержать ответ на `delay` (таймауты RPC);
- `kill_stream` — оборвать стрим (в т.ч. простаивающий);
- `silence` — молча перестать слать события и heartbeat'ы, держа стрим
  открытым (мёртвый фид);
- `dup_events` — доставлять каждое событие дважды;
- `blank_order_id` / `reuse_order_id` (только PlaceOrder) — применить постановку,
  но вернуть в ответе ПУСТОЙ / уже выданный order id (ордер живёт под настоящим
  id и стримит филлы под ним) — брокер вернул битый ack, движок уходит в
  `unverified`.

## Книга рецептов

**Потерянный ответ постановки** (главный инцидент идемпотентного placer'а —
ордер встал, клиент получил `UNAVAILABLE`; placer обязан найти его по
client_order_id и «усыновить»):

```bash
curl -s -X POST localhost:8062/v1/faults \
  -d '{"method":"PlaceOrder","action":"drop_after_apply"}'
```

**Реплей сделок при реконнекте** (проверка `TradeDedup` и `engine.Own`):

```bash
curl -s -X POST localhost:8062/v1/faults \
  -d '{"method":"SubscribeTrades","action":"kill_stream"}'
# обёртка клиента переподключится через ~1s и получит реплей всех сделок дня
```

**Мёртвый market-data фид** (клиентский heartbeat-timeout 30s → PullOnStaleBook):

```bash
curl -s -X POST localhost:8062/v1/faults \
  -d '{"method":"SubscribeOrderBook","action":"silence"}'
# снять: curl -s -X DELETE localhost:8062/v1/faults
```

**Кроссированная книга** (клиент не эмитит мусорный срез и после 50 подряд
кроссов форсирует переподписку):

```bash
curl -s -X POST localhost:8062/v1/book/cross -d '{"symbol":"LEGA@RTSX"}'
```

**Неподтверждённая отмена / impaired-режим** (retireQ движка):

```bash
curl -s -X POST localhost:8062/v1/faults \
  -d '{"method":"CancelOrder","action":"drop_after_apply","count":3}'
```

**Хедж, убитый биржей с недобором** (перехеджирование по факту executed):
поставить в книгу меньше объёма, чем хеджует движок (`POST /v1/book`), и дать
тейкеру исполниться частично; остаток снимется `CANCELED`.

**Протухший JWT посреди сессии**:

```bash
curl -s -X POST localhost:8062/v1/tokens/expire
```

**Исчерпание квоты постановки** (гейтинг QuotaLimiter):

```bash
curl -s -X POST localhost:8062/v1/quota -d '{"remaining":0}'
```

**Закрытие рынка** (снятие котировок раннерами):

```bash
curl -s -X POST localhost:8062/v1/session -d '{"type":"CLOSED"}'
```

**Расхождение позиций / suspect в reconcile** — налить на счёт «чужую» сделку
печатью об лимитник другой стратегии либо форс-филом ордера, о котором движок
не знает (`POST /v1/fill`), и смотреть, как reconcile приостанавливает торговлю
до схождения.

## Из Go-тестов

`brokersim.Start(cfg, "127.0.0.1:0", "")` + `t.Setenv(finam.EnvAddr, srv.Addr())` —
и настоящий `finam.NewClient` ходит в сим. Все ручки control-plane доступны
методами `Sim` напрямую (`AddFault`, `SetBook`, `FillOrder`, `PublicTrade`,
`CrossBook`, `SetPosition`, `ExpireTokens`, …). Готовые примеры — `e2e_test.go`:
сквозные сценарии от авторизации до восстановления потерянного ответа настоящим
placer'ом из `execengine`.

## Экстрим-харнесс движка (`engine_extreme_test.go`)

Отдельный набор гоняет **настоящий `execengine.Engine`** (стейт-машину
post-at-touch / hedge-on-fill / impaired / retireQ / hedge-debts / reconcile) с
`NewFinamMaker`/`NewFinamTaker` поверх реального gRPC против сима, управляемый
скриптовым `Decider` (детерминированные входы/выходы). Пути отказоустойчивости
движка в юнит-тестах покрыты фейками интерфейсов; здесь они проверяются против
реального транспорта и брокерской семантики, где расходятся предположения и
данные. После каждого сценария проверяются **инварианты движка** против наземной
правды брокера:

- вера движка о позиции (`Engine.Position` + leg B net) == фактические позиции
  брокера по обеим ногам — **никакого молчаливого расхождения**;
- ноги 1:1 (`legA == -legB`), когда движок сообщает здоровье;
- `impaired` входит на неподтверждённых операциях и **сам выходит**, когда
  брокер ответил (`retireQ`/`debts` дренированы), через `unverified` → чистый
  `reconcile`;
- `suspect`/`unverified` снимаются сами по схождении позиций.

Сценарии (~30, дизайн: состязательный workflow по углам стейт-машины):

- **База/связь:** happy-path; чистый отказ хеджа → hedge-debt → impaired →
  recovery; потерянный ответ тейкера → оверхедж → suspect → ремонт; потерянная
  отмена → `deferRetire` → impaired → recovery; reconcile-фантом → suspect →
  авто-возврат; шторм обрывов стримов; комбинированный шторм.
- **Учёт позиции:** counterpart-ahead multi-lot → свод к target, НЕ оверкап
  (инцидент 6:6); repeg-fold ловит исполнившийся пассив (нет двойной постановки);
  cancel-ack выучил фил → реплей дедупится; флип лонг→флэт→шорт; **чужой филл**
  на общем счёте не считается → suspect.
- **Хедж/тейкер:** частичная ликвидность → недобор перехеджен РОВНО; серия
  dead-short (≥3) → impaired-долг; **пустой order id** → слепой кредит →
  unverified; **переиспользованный id** → клоббер аккаунта → unverified; филлы
  СВЕРХ терминала (брокер противоречит ack) → докредит + suspect.
- **Market-data:** stale-book pull ловит невидимый мейкер-фил (нет голой ноги);
  stale force-close по мёртвой книге → impaired, НЕ молча-флэт; квота=0 блокирует
  опены, но не хедж работающего клипа.
- **Оператор/kill-switch:** стойкое РЕАЛЬНОЕ расхождение → вечный suspect, НИКОГДА
  не угадывает; фил после `Halt` НЕ хеджится (голая нога оставлена оператору);
  `Halt` поверх impaired замораживает hedge-debt; impaired-во-время-impaired →
  обе очереди (retireQ+debts) дренируются.

Плюс **soak-фаззер** `TestEngineFaultStormSoak` (opt-in `BROKERSIM_SOAK=1`) —
циклы вход/выход под случайными сбоями с проверкой инварианта на каждой границе.

Для этих сценариев сим расширен: фолты `blank_order_id`/`reuse_order_id` (битый
order id в ответе PlaceOrder — путь `unverified` движка) и control-op
`Sim.InjectFill` / `POST /v1/trade/inject` (фабрикованный филл сверх терминала).

```
go test ./brokersim/ -run TestEngine                 # экстрим-сценарии движка (~60s)
BROKERSIM_SOAK=1 go test ./brokersim/ -run Soak       # soak-фаззер (~140s, opt-in)
```

## Ограничения

- Матчинг упрощён: публичный стакан экзогенный (авторынок/control-plane),
  собственные лимитники в него не подмешиваются и филятся печатями ленты,
  форс-филами или кроссом маркет-ордеров об книгу. Очередь FIFO по времени
  постановки, приоритета по цене между своими ордерами нет.
- Реализованный PnL оседает в кэш сразу (без клиринга/вариационки FORTS);
  `money_reserved` = `margin_per_lot × |позиция|`.
- Стоп-заявки (`PlaceSLTPOrder`), `Bars`, `SubscribeQuote`, `SubscribeBars`,
  reports и corporate actions не реализованы (`UNIMPLEMENTED`) — бот их не
  использует.
- Plaintext gRPC: сим слушает loopback и не предназначен ни для чего, кроме
  локальной отладки.
