package brokersim

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	v1 "github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1"
	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/orders"
	"google.golang.org/grpc/codes"
)

// killStatuses — допустимые терминальные статусы для KillOrder.
var killStatuses = map[string]orders.OrderStatus{
	"canceled":             orders.OrderStatus_ORDER_STATUS_CANCELED,
	"rejected":             orders.OrderStatus_ORDER_STATUS_REJECTED,
	"rejected_by_exchange": orders.OrderStatus_ORDER_STATUS_REJECTED_BY_EXCHANGE,
	"denied_by_broker":     orders.OrderStatus_ORDER_STATUS_DENIED_BY_BROKER,
	"expired":              orders.OrderStatus_ORDER_STATUS_EXPIRED,
	"failed":               orders.OrderStatus_ORDER_STATUS_FAILED,
	"done_for_day":         orders.OrderStatus_ORDER_STATUS_DONE_FOR_DAY,
}

// control.go — HTTP control-plane сима: curl-совместимый JSON API для
// сценариев инцидентов. Все ручки под /v1/:
//
//	GET  /v1/state            — полный срез: счета, ордера, книги, сбои
//	GET  /v1/orders?account=  — ордера счёта
//	POST /v1/faults           — добавить правило сбоя (тело — Fault)
//	GET  /v1/faults           — активные правила
//	DELETE /v1/faults[?id=N]  — снять правило / все правила
//	POST /v1/book             — {"symbol","bids":[[p,s]..],"asks":[[p,s]..]} — задать книгу
//	POST /v1/book/cross       — {"symbol"} — вбросить кроссирующий бид (порча книги)
//	POST /v1/trade            — {"symbol","price","size","side":"buy|sell"} — публичная печать (задевает лимитники)
//	POST /v1/fill             — {"order_id","lots","price"} — форс-фил ордера (lots 0 = весь остаток, price 0 = лимит/последняя)
//	POST /v1/order/kill       — {"order_id","status":"canceled|rejected|expired"} — терминировать ордер брокером
//	POST /v1/session          — {"type":"CORE_TRADING"|"CLOSED"|...} — тип сессии для Schedule
//	POST /v1/tokens/expire    — протухнуть все токены (и убить их стримы)
//	POST /v1/quota            — {"remaining":N} — выставить остаток квоты placeOrder
//	POST /v1/automarket       — {"symbol","enabled",...} — включить/настроить авторынок
type controlServer struct {
	s *Sim
}

// startControl поднимает control-plane; возвращает функцию остановки.
func (s *Sim) startControl(addr string) (func() error, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("brokersim: control listen %s: %w", addr, err)
	}
	c := &controlServer{s: s}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/state", c.state)
	mux.HandleFunc("GET /v1/orders", c.orders)
	mux.HandleFunc("POST /v1/faults", c.addFault)
	mux.HandleFunc("GET /v1/faults", c.listFaults)
	mux.HandleFunc("DELETE /v1/faults", c.deleteFaults)
	mux.HandleFunc("POST /v1/book", c.setBook)
	mux.HandleFunc("POST /v1/book/cross", c.crossBook)
	mux.HandleFunc("POST /v1/trade", c.publicTrade)
	mux.HandleFunc("POST /v1/fill", c.fill)
	mux.HandleFunc("POST /v1/trade/inject", c.injectFill)
	mux.HandleFunc("POST /v1/order/kill", c.killOrder)
	mux.HandleFunc("POST /v1/position", c.position)
	mux.HandleFunc("POST /v1/session", c.session)
	mux.HandleFunc("POST /v1/tokens/expire", c.expireTokens)
	mux.HandleFunc("POST /v1/quota", c.quota)
	mux.HandleFunc("POST /v1/automarket", c.autoMarket)
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(lis) }()
	// Останов — Shutdown, не Close: дожидается in-flight хендлеров, чтобы
	// поздний ConfigureAutoMarket не делал wg.Add наперегонки с wg.Wait в
	// Sim.Close (Server.Close зовёт нас ДО Sim.Close).
	return func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(ctx)
	}, nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func badRequest(w http.ResponseWriter, format string, args ...any) {
	writeJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf(format, args...)})
}

func readBody[T any](w http.ResponseWriter, r *http.Request) (T, bool) {
	var v T
	if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
		badRequest(w, "bad json: %v", err)
		return v, false
	}
	return v, true
}

// ---- срезы состояния ----

type orderView struct {
	OrderID       string  `json:"order_id"`
	AccountID     string  `json:"account_id"`
	Symbol        string  `json:"symbol"`
	Side          string  `json:"side"`
	Type          string  `json:"type"`
	TimeInForce   string  `json:"time_in_force"`
	ClientOrderID string  `json:"client_order_id"`
	LimitPrice    float64 `json:"limit_price,omitempty"`
	Quantity      float64 `json:"quantity"`
	Executed      float64 `json:"executed"`
	Status        string  `json:"status"`
	PlacedAt      string  `json:"placed_at"`
}

func viewOrder(o *simOrder) orderView {
	return orderView{
		OrderID:       o.id,
		AccountID:     o.accountID,
		Symbol:        o.symbol,
		Side:          o.side.String(),
		Type:          o.typ.String(),
		TimeInForce:   o.tif.String(),
		ClientOrderID: o.clientOrderID,
		LimitPrice:    o.limitPrice,
		Quantity:      o.qty,
		Executed:      o.executed,
		Status:        o.status.String(),
		PlacedAt:      o.placedAt.UTC().Format(time.RFC3339Nano),
	}
}

func (c *controlServer) state(w http.ResponseWriter, r *http.Request) {
	s := c.s
	s.mu.Lock()
	defer s.mu.Unlock()
	type posView struct {
		Qty float64 `json:"qty"`
		Avg float64 `json:"avg"`
	}
	type accView struct {
		Cash      map[string]float64 `json:"-"`
		CashRUB   float64            `json:"cash_rub"`
		Positions map[string]posView `json:"positions"`
		Orders    int                `json:"orders"`
		Trades    int                `json:"trades"`
	}
	accs := map[string]accView{}
	for id, a := range s.accounts {
		pv := map[string]posView{}
		for sym, p := range a.positions {
			if p.qty != 0 {
				pv[sym] = posView{Qty: p.qty, Avg: p.avg}
			}
		}
		accs[id] = accView{CashRUB: a.cash, Positions: pv, Orders: len(a.orders), Trades: len(a.trades)}
	}
	type bookView struct {
		Bids []Level `json:"bids"`
		Asks []Level `json:"asks"`
		Last float64 `json:"last"`
	}
	books := map[string]bookView{}
	for sym, st := range s.symbols {
		bv := bookView{Last: st.last}
		for _, p := range sortedPrices(st.bids, true) {
			bv.Bids = append(bv.Bids, Level{Price: p, Size: st.bids[p]})
		}
		for _, p := range sortedPrices(st.asks, false) {
			bv.Asks = append(bv.Asks, Level{Price: p, Size: st.asks[p]})
		}
		books[sym] = bv
	}
	var ords []orderView
	for _, a := range s.accounts {
		for _, o := range a.orders {
			ords = append(ords, viewOrder(o))
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"accounts":     accs,
		"symbols":      books,
		"orders":       ords,
		"faults":       s.faults.List(),
		"session_type": s.sessionType,
		"quota_used":   s.quotaUsed,
	})
}

func (c *controlServer) orders(w http.ResponseWriter, r *http.Request) {
	accID := r.URL.Query().Get("account")
	s := c.s
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []orderView
	for _, a := range s.accounts {
		if accID != "" && a.id != accID {
			continue
		}
		for _, o := range a.orders {
			out = append(out, viewOrder(o))
		}
	}
	writeJSON(w, http.StatusOK, out)
}

// ---- сбои ----

// faultBody — JSON-обёртка Fault с кодом gRPC строкой или числом.
type faultBody struct {
	Method      string  `json:"method"`
	Action      string  `json:"action"`
	Code        any     `json:"code,omitempty"` // число (14) или строка ("unavailable")
	Message     string  `json:"message,omitempty"`
	Delay       string  `json:"delay,omitempty"`
	Count       int     `json:"count,omitempty"`
	Probability float64 `json:"probability,omitempty"`
}

var codeNames = map[string]codes.Code{
	"canceled": codes.Canceled, "unknown": codes.Unknown, "invalid_argument": codes.InvalidArgument,
	"deadline_exceeded": codes.DeadlineExceeded, "not_found": codes.NotFound, "already_exists": codes.AlreadyExists,
	"permission_denied": codes.PermissionDenied, "resource_exhausted": codes.ResourceExhausted,
	"failed_precondition": codes.FailedPrecondition, "aborted": codes.Aborted, "internal": codes.Internal,
	"unavailable": codes.Unavailable, "data_loss": codes.DataLoss, "unauthenticated": codes.Unauthenticated,
}

func (c *controlServer) addFault(w http.ResponseWriter, r *http.Request) {
	body, ok := readBody[faultBody](w, r)
	if !ok {
		return
	}
	f := Fault{
		Method:      body.Method,
		Action:      body.Action,
		Message:     body.Message,
		Count:       body.Count,
		Probability: body.Probability,
	}
	switch v := body.Code.(type) {
	case nil:
	case float64:
		f.Code = codes.Code(v)
	case string:
		code, known := codeNames[v]
		if !known {
			badRequest(w, "unknown grpc code %q", v)
			return
		}
		f.Code = code
	default:
		badRequest(w, "code must be number or string")
		return
	}
	if body.Delay != "" {
		d, err := time.ParseDuration(body.Delay)
		if err != nil {
			badRequest(w, "bad delay: %v", err)
			return
		}
		f.Delay = Duration(d)
	}
	added, err := c.s.faults.Add(f)
	if err != nil {
		badRequest(w, "%v", err)
		return
	}
	writeJSON(w, http.StatusOK, added)
}

func (c *controlServer) listFaults(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, c.s.faults.List())
}

func (c *controlServer) deleteFaults(w http.ResponseWriter, r *http.Request) {
	id := int64(-1)
	if q := r.URL.Query().Get("id"); q != "" {
		v, err := strconv.ParseInt(q, 10, 64)
		if err != nil {
			badRequest(w, "bad id: %v", err)
			return
		}
		id = v
	}
	writeJSON(w, http.StatusOK, map[string]int{"removed": c.s.faults.Remove(id)})
}

// ---- рынок ----

type bookBody struct {
	Symbol string       `json:"symbol"`
	Bids   [][2]float64 `json:"bids"` // [[price, size], ...]
	Asks   [][2]float64 `json:"asks"`
}

// SetBook задаёт книгу символа целиком, рассылая дельты подписчикам.
func (s *Sim) SetBook(symbol string, bids, asks []Level) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.symbolLocked(symbol)
	if !ok {
		return fmt.Errorf("unknown symbol %s", symbol)
	}
	rows := st.setBook(bids, asks, s.now())
	s.broadcastBookLocked(symbol, rows)
	return nil
}

func toLevels(pairs [][2]float64) []Level {
	out := make([]Level, 0, len(pairs))
	for _, p := range pairs {
		out = append(out, Level{Price: p[0], Size: p[1]})
	}
	return out
}

func (c *controlServer) setBook(w http.ResponseWriter, r *http.Request) {
	body, ok := readBody[bookBody](w, r)
	if !ok {
		return
	}
	if err := c.s.SetBook(body.Symbol, toLevels(body.Bids), toLevels(body.Asks)); err != nil {
		badRequest(w, "%v", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// CrossBook вбрасывает в книгу бид выше лучшего аска — порча инкрементального
// стакана «пропущенным REMOVE». Клиент обязан не эмитить кросс и переподключиться.
func (s *Sim) CrossBook(symbol string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.symbolLocked(symbol)
	if !ok {
		return fmt.Errorf("unknown symbol %s", symbol)
	}
	s.broadcastBookLocked(symbol, st.crossRows(s.now()))
	return nil
}

func (c *controlServer) crossBook(w http.ResponseWriter, r *http.Request) {
	body, ok := readBody[struct {
		Symbol string `json:"symbol"`
	}](w, r)
	if !ok {
		return
	}
	if err := c.s.CrossBook(body.Symbol); err != nil {
		badRequest(w, "%v", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// PublicTrade печатает публичную сделку: уходит в ленту и задевает лимитники,
// чья цена кроссится с ценой печати.
func (s *Sim) PublicTrade(symbol string, price, size float64, buy bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.symbolLocked(symbol)
	if !ok {
		return fmt.Errorf("unknown symbol %s", symbol)
	}
	side := v1.Side_SIDE_SELL
	if buy {
		side = v1.Side_SIDE_BUY
	}
	tr := publicTrade("T"+itoa(s.nextSeq()), price, size, side, s.now())
	st.appendTape(tr)
	st.last = price
	s.broadcastTapeLocked(symbol, tr)
	s.matchPublicPrintLocked(st, tr)
	return nil
}

func (c *controlServer) publicTrade(w http.ResponseWriter, r *http.Request) {
	body, ok := readBody[struct {
		Symbol string  `json:"symbol"`
		Price  float64 `json:"price"`
		Size   float64 `json:"size"`
		Side   string  `json:"side"`
	}](w, r)
	if !ok {
		return
	}
	if body.Price <= 0 || body.Size <= 0 {
		badRequest(w, "price and size must be positive")
		return
	}
	if err := c.s.PublicTrade(body.Symbol, body.Price, body.Size, body.Side != "sell"); err != nil {
		badRequest(w, "%v", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// FillOrder форс-филит ордер: lots<=0 — весь остаток, price<=0 — лимитная цена
// (или последняя цена символа для маркета).
func (s *Sim) FillOrder(orderID string, lots, price float64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	o := s.orders[orderID]
	if o == nil {
		return fmt.Errorf("order %s not found", orderID)
	}
	if !o.active() {
		return fmt.Errorf("order %s is %s", orderID, o.status)
	}
	if lots <= 0 {
		lots = o.qty - o.executed
	}
	if price <= 0 {
		price = o.limitPrice
		if price <= 0 {
			if st := s.symbols[o.symbol]; st != nil {
				price = st.last
			}
		}
		if price <= 0 {
			return fmt.Errorf("no price for order %s: pass explicit price", orderID)
		}
	}
	now := s.now()
	if st := s.symbols[o.symbol]; st != nil {
		tr := publicTrade("T"+itoa(s.nextSeq()), price, lots, o.side, now)
		st.appendTape(tr)
		s.broadcastTapeLocked(o.symbol, tr)
	}
	s.fillOrderLocked(o, lots, price, now)
	return nil
}

func (c *controlServer) fill(w http.ResponseWriter, r *http.Request) {
	body, ok := readBody[struct {
		OrderID string  `json:"order_id"`
		Lots    float64 `json:"lots"`
		Price   float64 `json:"price"`
	}](w, r)
	if !ok {
		return
	}
	if err := c.s.FillOrder(body.OrderID, body.Lots, body.Price); err != nil {
		badRequest(w, "%v", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// InjectFill фабрикует АccountTrade на существующий ордер СВЕРХ его нормального
// исполнения — брокер противоречит собственному терминальному ack (лишние филлы
// после «мёртвого» тейкера). Сделка уходит подписчикам SubscribeTrades и двигает
// позицию счёта, но НЕ трогает исполненное самого ордера (это внешний вброс).
func (s *Sim) InjectFill(orderID string, lots, price float64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	o := s.orders[orderID]
	if o == nil {
		return fmt.Errorf("order %s not found", orderID)
	}
	if lots <= 0 {
		return fmt.Errorf("lots must be positive")
	}
	now := s.now()
	acc := s.accounts[o.accountID]
	s.applyFillToAccountLocked(acc, o.symbol, o.side, lots, price)
	trade := &v1.AccountTrade{
		TradeId:   "AT" + itoa(s.nextSeq()),
		Symbol:    o.symbol,
		Price:     dec(price),
		Size:      dec(lots),
		Side:      o.side,
		Timestamp: ts(now),
		OrderId:   o.id,
		AccountId: o.accountID,
	}
	acc.trades = append(acc.trades, trade)
	s.emitTradeLocked(trade)
	s.emitAccountLocked(o.accountID)
	return nil
}

func (c *controlServer) injectFill(w http.ResponseWriter, r *http.Request) {
	body, ok := readBody[struct {
		OrderID string  `json:"order_id"`
		Lots    float64 `json:"lots"`
		Price   float64 `json:"price"`
	}](w, r)
	if !ok {
		return
	}
	if err := c.s.InjectFill(body.OrderID, body.Lots, body.Price); err != nil {
		badRequest(w, "%v", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// KillOrder терминирует активный ордер со стороны брокера/биржи заданным
// статусом ("canceled" | "rejected" | "expired" | "done_for_day").
func (s *Sim) KillOrder(orderID, how string) error {
	st, ok := killStatuses[how]
	if !ok {
		return fmt.Errorf("unknown kill status %q", how)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	o := s.orders[orderID]
	if o == nil {
		return fmt.Errorf("order %s not found", orderID)
	}
	if !o.active() {
		return fmt.Errorf("order %s is %s", orderID, o.status)
	}
	now := s.now()
	o.status = st
	o.withdrawAt = now
	o.updatedAt = now
	s.emitOrderLocked(o)
	return nil
}

func (c *controlServer) killOrder(w http.ResponseWriter, r *http.Request) {
	body, ok := readBody[struct {
		OrderID string `json:"order_id"`
		Status  string `json:"status"`
	}](w, r)
	if !ok {
		return
	}
	if body.Status == "" {
		body.Status = "canceled"
	}
	if err := c.s.KillOrder(body.OrderID, body.Status); err != nil {
		badRequest(w, "%v", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// SetPosition выставляет позицию счёта по символу НАПРЯМУЮ, минуя ордера и
// сделки — фантомная позиция для сценария расхождения в reconcile: движок про
// неё не знает, брокер (сим) её показывает, reconcile обязан увести бота в
// suspect до схождения.
func (s *Sim) SetPosition(accountID, symbol string, qty, avg float64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	acc, ok := s.accounts[accountID]
	if !ok {
		return fmt.Errorf("unknown account %s", accountID)
	}
	if _, ok := s.symbolLocked(symbol); !ok {
		return fmt.Errorf("unknown symbol %s", symbol)
	}
	if qty == 0 {
		delete(acc.positions, symbol)
	} else {
		acc.positions[symbol] = &position{qty: qty, avg: avg}
	}
	s.emitAccountLocked(accountID)
	return nil
}

func (c *controlServer) position(w http.ResponseWriter, r *http.Request) {
	body, ok := readBody[struct {
		Account string  `json:"account"`
		Symbol  string  `json:"symbol"`
		Qty     float64 `json:"qty"`
		Avg     float64 `json:"avg"`
	}](w, r)
	if !ok {
		return
	}
	if err := c.s.SetPosition(body.Account, body.Symbol, body.Qty, body.Avg); err != nil {
		badRequest(w, "%v", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// SetSession меняет тип сессии, который Schedule отдаёт клиенту.
func (s *Sim) SetSession(sessionType string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessionType = sessionType
}

func (c *controlServer) session(w http.ResponseWriter, r *http.Request) {
	body, ok := readBody[struct {
		Type string `json:"type"`
	}](w, r)
	if !ok {
		return
	}
	if body.Type == "" {
		badRequest(w, "type is required")
		return
	}
	c.s.SetSession(body.Type)
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (c *controlServer) expireTokens(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]int{"expired": c.s.ExpireTokens()})
}

// SetQuotaRemaining выставляет остаток квоты placeOrder в текущем окне.
func (s *Sim) SetQuotaRemaining(remaining int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	used := s.cfg.placeQuotaLimit() - remaining
	if used < 0 {
		used = 0
	}
	s.quotaUsed = used
	s.quotaStart = s.now()
}

func (c *controlServer) quota(w http.ResponseWriter, r *http.Request) {
	body, ok := readBody[struct {
		Remaining int `json:"remaining"`
	}](w, r)
	if !ok {
		return
	}
	c.s.SetQuotaRemaining(body.Remaining)
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ConfigureAutoMarket перенастраивает авторынок символа на лету; включение
// стартует генератор, если он ещё не крутится (гейт autoRunning внутри
// startAutoMarket). Работающий генератор подхватывает новые параметры на
// следующем тике, кроме interval — он фиксируется при старте горутины.
func (s *Sim) ConfigureAutoMarket(symbol string, cfg AutoMarketConfig) error {
	s.mu.Lock()
	st, ok := s.symbolLocked(symbol)
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("unknown symbol %s", symbol)
	}
	if cfg.Mid == 0 {
		cfg.Mid = st.autoMid // не сбрасывать блуждание при перенастройке
	}
	st.auto = cfg
	st.autoMid = cfg.Mid
	s.mu.Unlock()
	if cfg.Enabled {
		s.startAutoMarket(st)
	}
	return nil
}

func (c *controlServer) autoMarket(w http.ResponseWriter, r *http.Request) {
	body, ok := readBody[struct {
		Symbol string `json:"symbol"`
		AutoMarketConfig
	}](w, r)
	if !ok {
		return
	}
	if err := c.s.ConfigureAutoMarket(body.Symbol, body.AutoMarketConfig); err != nil {
		badRequest(w, "%v", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
