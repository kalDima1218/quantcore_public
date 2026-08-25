// Package brokersim — локальный брокер, реализующий gRPC-сервисы Finam Trade API
// (tradeapi.v1) поверх официального SDK. Предназначен ТОЛЬКО для отладки: даёт
// воспроизводить инциденты (потерянные ответы, обрывы стримов, реплеи, протухшие
// токены, кроссированный стакан, исчерпание квот) против настоящего клиентского
// кода QuantCore без сети и реальных денег. Бот направляется на сим переменной
// окружения QUANTCORE_FINAM_ADDR (см. trade/finam.NewClient).
//
// Управление сценариями — HTTP control-plane (control.go) либо прямые методы Sim
// из Go-тестов. Семантика намеренно повторяет документированное поведение API:
// JWT с TTL и заголовком Authorization, эхо client_order_id в OrderState.Order,
// снапшот+дельты стакана, реплей сегодняшних ордеров/сделок при подписке,
// асинхронные подтверждения исполнения через стримы.
package brokersim

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	v1 "github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1"
	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/orders"
	"google.golang.org/genproto/googleapis/type/decimal"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Duration — time.Duration с JSON-парсингом из строк вида "200ms", "15m".
type Duration time.Duration

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		v, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		*d = Duration(v)
		return nil
	}
	var n float64
	if err := json.Unmarshal(b, &n); err != nil {
		return fmt.Errorf("duration: want \"1s\" style string or nanoseconds number, got %s", b)
	}
	*d = Duration(time.Duration(n))
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

func (d Duration) or(def time.Duration) time.Duration {
	if d == 0 {
		return def
	}
	return time.Duration(d)
}

// AccountConfig описывает один счёт сима: секрет для Auth и стартовые средства.
type AccountConfig struct {
	Secret        string  `json:"secret"`
	AccountID     string  `json:"account_id"`
	InitialCash   float64 `json:"initial_cash"`
	PortfolioKind string  `json:"portfolio_kind"` // "forts" (default) | "mc" | "cash" | "none" — какой вариант портфеля отдаёт GetAccount
}

// AutoMarketConfig — параметры автогенератора рынка: случайное блуждание мида,
// перестройка стакана и случайные публичные сделки, чтобы стримы жили без ручного
// управления.
type AutoMarketConfig struct {
	Enabled   bool     `json:"enabled"`
	Mid       float64  `json:"mid"`        // стартовый мид
	Spread    float64  `json:"spread"`     // расстояние bid1-ask1
	Step      float64  `json:"step"`       // шаг между уровнями
	Levels    int      `json:"levels"`     // уровней на сторону
	LevelVol  float64  `json:"level_vol"`  // объём уровня
	Interval  Duration `json:"interval"`   // период тика
	Walk      float64  `json:"walk"`       // максимальный сдвиг мида за тик (равномерный шум)
	TradeProb float64  `json:"trade_prob"` // вероятность публичной сделки на тике
}

// SymbolConfig — статические свойства инструмента.
type SymbolConfig struct {
	Symbol         string            `json:"symbol"`
	MinStep        float64           `json:"min_step"`        // шаг цены (для GetAsset)
	Decimals       int32             `json:"decimals"`        // знаков после запятой
	LotSize        float64           `json:"lot_size"`        // размер лота
	MarginPerLot   float64           `json:"margin_per_lot"`  // ГО за лот, ₽ — питает money_reserved
	ExpirationDate string            `json:"expiration_date"` // "YYYY-MM-DD", опционально
	AutoMarket     *AutoMarketConfig `json:"auto_market,omitempty"`
}

// Config — конфигурация сима целиком. Нулевое значение работает: один счёт
// (secret "sim-secret", account "SIM-1"), символы создаются на лету.
type Config struct {
	Accounts []AccountConfig `json:"accounts"`
	Symbols  []SymbolConfig  `json:"symbols"`

	TokenTTL    Duration `json:"token_ttl"`    // TTL JWT (default 15m — как у Finam)
	ExecLatency Duration `json:"exec_latency"` // задержка асинхронного матчинга/реджекта после PlaceOrder (default 20ms)
	CancelAsync bool     `json:"cancel_async"` // true: CancelOrder отвечает PENDING_CANCEL, CANCELED приходит стримом позже

	HeartbeatInterval Duration `json:"heartbeat_interval"` // пустые keepalive-ответы стриму стакана (default 10s)

	SessionType string `json:"session_type"` // тип текущей сессии для Schedule (default CORE_TRADING)

	PlaceQuotaLimit  int      `json:"place_quota_limit"`  // квота OrdersService.placeOrder на окно (default 200 — документированный лимит «200 запросов в минуту»)
	PlaceQuotaWindow Duration `json:"place_quota_window"` // окно квоты (default 1m)

	// OrdersListIncludesTerminal: включать ли терминальные (filled/canceled/…)
	// ордера дня в GetOrders. Боевой API документирован как «список АКТИВНЫХ
	// заявок» (MIGRATION_GUIDE FinamWeb/finam-trade-api), поэтому default —
	// false: у восстановления по client_order_id остаётся слепая зона
	// «fill-and-vanish», ровно как в проде (страхует reconcile). true расширяет
	// список всеми ордерами дня — удобно для сценариев без этой слепой зоны.
	OrdersListIncludesTerminal bool `json:"orders_list_includes_terminal,omitempty"`

	// TapeReplayDepth — сколько последних публичных сделок реплеится новому
	// подписчику SubscribeLatestTrades (default 50).
	TapeReplayDepth int `json:"tape_replay_depth"`

	// AutoCreateSymbols — создавать ли инструмент при первом обращении к
	// незнакомому символу (default true; false отдаёт NotFound, как боевой API
	// на опечатку).
	AutoCreateSymbols *bool `json:"auto_create_symbols,omitempty"`
}

func (c *Config) tokenTTL() time.Duration          { return c.TokenTTL.or(15 * time.Minute) }
func (c *Config) execLatency() time.Duration       { return c.ExecLatency.or(20 * time.Millisecond) }
func (c *Config) heartbeatInterval() time.Duration { return c.HeartbeatInterval.or(10 * time.Second) }
func (c *Config) sessionType() string {
	if c.SessionType == "" {
		return "CORE_TRADING"
	}
	return c.SessionType
}
func (c *Config) placeQuotaLimit() int {
	if c.PlaceQuotaLimit == 0 {
		return 200
	}
	return c.PlaceQuotaLimit
}
func (c *Config) placeQuotaWindow() time.Duration  { return c.PlaceQuotaWindow.or(time.Minute) }
func (c *Config) ordersListIncludesTerminal() bool { return c.OrdersListIncludesTerminal }
func (c *Config) tapeReplayDepth() int {
	if c.TapeReplayDepth == 0 {
		return 50
	}
	return c.TapeReplayDepth
}
func (c *Config) autoCreateSymbols() bool {
	return c.AutoCreateSymbols == nil || *c.AutoCreateSymbols
}

// position — точная позиция счёта по одному символу (лоты со знаком).
type position struct {
	qty float64 // подписанное количество лотов
	avg float64 // средняя цена открытой части
}

// account — состояние одного счёта.
type account struct {
	id            string
	secret        string
	portfolioKind string
	cash          float64 // свободные средства, ₽; реализованный PnL оседает здесь
	positions     map[string]*position
	orders        []*simOrder          // все ордера дня в порядке постановки
	trades        []*v1.AccountTrade   // все сделки дня в порядке исполнения
	clientIDs     map[string]*simOrder // client_order_id -> ордер (уникальность в пределах дня)
}

// simOrder — внутреннее состояние ордера.
type simOrder struct {
	id            string
	execID        string
	accountID     string
	symbol        string
	side          v1.Side
	typ           orders.OrderType
	tif           orders.TimeInForce
	clientOrderID string
	limitPrice    float64
	qty           float64 // лоты
	executed      float64
	status        orders.OrderStatus
	placedAt      time.Time
	updatedAt     time.Time
	withdrawAt    time.Time
}

func (o *simOrder) terminal() bool {
	switch o.status {
	case orders.OrderStatus_ORDER_STATUS_FILLED,
		orders.OrderStatus_ORDER_STATUS_CANCELED,
		orders.OrderStatus_ORDER_STATUS_REJECTED,
		orders.OrderStatus_ORDER_STATUS_REJECTED_BY_EXCHANGE,
		orders.OrderStatus_ORDER_STATUS_DENIED_BY_BROKER,
		orders.OrderStatus_ORDER_STATUS_EXPIRED,
		orders.OrderStatus_ORDER_STATUS_FAILED,
		orders.OrderStatus_ORDER_STATUS_DONE_FOR_DAY:
		return true
	}
	return false
}

// active — ордер стоит в книге и может исполняться.
func (o *simOrder) active() bool {
	switch o.status {
	case orders.OrderStatus_ORDER_STATUS_NEW,
		orders.OrderStatus_ORDER_STATUS_PARTIALLY_FILLED,
		orders.OrderStatus_ORDER_STATUS_PENDING_CANCEL:
		return true
	}
	return false
}

// token — выданный JWT.
type token struct {
	value     string
	accountID string
	createdAt time.Time
	expiresAt time.Time
}

// Sim — ядро локального брокера: счета, ордера, книги, токены, квоты и
// подписчики. Все мутации — под одним мьютексом; события уходят подписчикам
// неблокирующе (буферизованные каналы).
type Sim struct {
	mu  sync.Mutex
	cfg Config
	now func() time.Time

	accounts     map[string]*account
	secrets      map[string]*account // secret -> счёт
	tokens       map[string]*token
	tokenSeq     uint64
	revokeBefore int64 // unix-nanos: токены, выпущенные не позже этого момента, отозваны (ExpireTokens)

	symbols map[string]*symbolState
	orders  map[string]*simOrder
	seq     uint64

	faults faultTable

	orderSubs   map[*eventSub[*orders.OrderState]]struct{}
	tradeSubs   map[*eventSub[*v1.AccountTrade]]struct{}
	accountSubs map[*eventSub[struct{}]]struct{} // сигнал «счёт изменился»; снапшот собирает сам стрим
	bookSubs    map[*bookSub]struct{}
	tapeSubs    map[*tapeSub]struct{}

	quotaUsed  int
	quotaStart time.Time

	placeCount  int // монотонно: все дошедшие до сима PlaceOrder (см. PlaceCount)
	cancelCount int // монотонно: все дошедшие до сима CancelOrder

	sessionType string

	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

const subBuffer = 4096

// tokenExpiry — потокобезопасный дедлайн жизни стрима: пишется при подписке и
// из ExpireTokens (под s.mu), читается тикерами стрим-горутин без s.mu.
type tokenExpiry struct{ v atomic.Int64 }

func (x *tokenExpiry) set(t time.Time)            { x.v.Store(t.UnixNano()) }
func (x *tokenExpiry) expired(now time.Time) bool { return now.UnixNano() > x.v.Load() }

// eventSub — подписчик потока событий, отфильтрованных по счёту.
type eventSub[T any] struct {
	accountID string
	ch        chan T
	tokenExp  tokenExpiry // стрим умирает, когда его токен протухает (как боевой)
}

// NewSim строит сим по конфигу и запускает фоновые генераторы (auto-market).
func NewSim(cfg Config) *Sim {
	s := &Sim{
		cfg:         cfg,
		now:         time.Now,
		accounts:    make(map[string]*account),
		secrets:     make(map[string]*account),
		tokens:      make(map[string]*token),
		symbols:     make(map[string]*symbolState),
		orders:      make(map[string]*simOrder),
		orderSubs:   make(map[*eventSub[*orders.OrderState]]struct{}),
		tradeSubs:   make(map[*eventSub[*v1.AccountTrade]]struct{}),
		accountSubs: make(map[*eventSub[struct{}]]struct{}),
		bookSubs:    make(map[*bookSub]struct{}),
		tapeSubs:    make(map[*tapeSub]struct{}),
		sessionType: cfg.sessionType(),
		stop:        make(chan struct{}),
	}
	s.quotaStart = s.now()

	if len(cfg.Accounts) == 0 {
		cfg.Accounts = []AccountConfig{{Secret: "sim-secret", AccountID: "SIM-1", InitialCash: 1_000_000}}
	}
	for _, ac := range cfg.Accounts {
		a := &account{
			id:            ac.AccountID,
			secret:        ac.Secret,
			portfolioKind: ac.PortfolioKind,
			cash:          ac.InitialCash,
			positions:     make(map[string]*position),
			clientIDs:     make(map[string]*simOrder),
		}
		if a.portfolioKind == "" {
			a.portfolioKind = "forts"
		}
		s.accounts[a.id] = a
		s.secrets[a.secret] = a
	}
	for _, sc := range cfg.Symbols {
		s.symbols[sc.Symbol] = newSymbolState(sc)
	}

	for _, st := range s.symbols {
		s.startAutoMarket(st)
	}
	return s
}

// Close останавливает фоновые горутины сима.
func (s *Sim) Close() {
	s.stopOnce.Do(func() { close(s.stop) })
	s.wg.Wait()
}

// PlaceCount возвращает число дошедших до сима PlaceOrder за всю его жизнь —
// включая отвергнутые (фолтом, квотой, валидацией): счётчик меряет НАГРУЗКУ на
// брокера, а не успешные ордера, поэтому шторм ретраев по реджектам виден.
// В отличие от внутреннего quotaUsed он монотонный — не обнуляется на границе
// окна квоты, и тест может измерить темп размещений на любом интервале.
func (s *Sim) PlaceCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.placeCount
}

// CancelCount — то же для CancelOrder.
func (s *Sim) CancelCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cancelCount
}

// countPlace/countCancel вызываются В НАЧАЛЕ хендлеров, до fault-гейта.
func (s *Sim) countPlace() {
	s.mu.Lock()
	s.placeCount++
	s.mu.Unlock()
}

func (s *Sim) countCancel() {
	s.mu.Lock()
	s.cancelCount++
	s.mu.Unlock()
}

// nextSeq — общий счётчик идентификаторов. Вызывать под mu.
func (s *Sim) nextSeq() uint64 {
	s.seq++
	return s.seq
}

// symbolLocked возвращает состояние символа, при необходимости создавая его
// (AutoCreateSymbols). Вызывать под mu.
func (s *Sim) symbolLocked(symbol string) (*symbolState, bool) {
	st, ok := s.symbols[symbol]
	if !ok && s.cfg.autoCreateSymbols() {
		st = newSymbolState(SymbolConfig{Symbol: symbol})
		s.symbols[symbol] = st
		ok = true
	}
	return st, ok
}

// ---- конвертация в protobuf ----

func dec(v float64) *decimal.Decimal {
	return &decimal.Decimal{Value: strconv.FormatFloat(v, 'f', -1, 64)}
}

func ts(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

// orderState собирает OrderState с эхом исходного ордера — ровно как боевой API
// (client_order_id возвращается внутри OrderState.Order, на этом стоит
// восстановление потерянных ответов).
func (o *simOrder) orderState() *orders.OrderState {
	echo := &orders.Order{
		AccountId:     o.accountID,
		Symbol:        o.symbol,
		Quantity:      dec(o.qty),
		Side:          o.side,
		Type:          o.typ,
		TimeInForce:   o.tif,
		ClientOrderId: o.clientOrderID,
	}
	if o.typ == orders.OrderType_ORDER_TYPE_LIMIT {
		echo.LimitPrice = dec(o.limitPrice)
	}
	return &orders.OrderState{
		OrderId:           o.id,
		ExecId:            o.execID,
		Status:            o.status,
		Order:             echo,
		TransactAt:        ts(o.placedAt),
		AcceptAt:          ts(o.placedAt),
		WithdrawAt:        ts(o.withdrawAt),
		InitialQuantity:   dec(o.qty),
		ExecutedQuantity:  dec(o.executed),
		RemainingQuantity: dec(o.qty - o.executed),
	}
}

// ---- рассылка событий ----

// emitOrderLocked рассылает текущее состояние ордера подписчикам его счёта.
// Вызывать под mu.
func (s *Sim) emitOrderLocked(o *simOrder) {
	st := o.orderState()
	for sub := range s.orderSubs {
		if sub.accountID != o.accountID {
			continue
		}
		sendClone(sub.ch, st)
	}
}

// emitTradeLocked рассылает сделку счёта подписчикам SubscribeTrades. Вызывать под mu.
func (s *Sim) emitTradeLocked(t *v1.AccountTrade) {
	for sub := range s.tradeSubs {
		if sub.accountID != t.AccountId {
			continue
		}
		sendClone(sub.ch, t)
	}
}

// emitAccountLocked сигналит подписчикам SubscribeAccount, что счёт изменился.
// Вызывать под mu.
func (s *Sim) emitAccountLocked(accountID string) {
	for sub := range s.accountSubs {
		if sub.accountID != accountID {
			continue
		}
		select {
		case sub.ch <- struct{}{}:
		default: // сигнал уже стоит в очереди — снапшот и так будет свежим
		}
	}
}

// sendClone кладёт в канал подписчика глубокую копию сообщения: подписчики
// сериализуют его из своих горутин, а оригинал может мутировать под mu.
func sendClone[T proto.Message](ch chan T, m T) {
	select {
	case ch <- proto.Clone(m).(T):
	default: // подписчик безнадёжно отстал — событие теряется (отражает реальный обрыв)
	}
}
