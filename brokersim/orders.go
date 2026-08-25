package brokersim

import (
	"context"
	"strconv"
	"time"

	v1 "github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1"
	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/orders"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const maxClientOrderID = 20 // документированный предел длины client_order_id

// ordersService реализует tradeapi.v1.orders.OrdersService.
type ordersService struct {
	orders.UnimplementedOrdersServiceServer
	s *Sim
}

// PlaceOrder принимает ордер, отвечает состоянием NEW и исполняет его
// асинхронно через ExecLatency — как боевой брокер: постановка подтверждается
// сразу, судьба (фил, пост-онли-реджект биржей) приходит стримом. client_order_id
// (если задан) обязан быть уникальным в пределах счёта и дня и эхом возвращается
// в OrderState.Order — на этом держится восстановление потерянных ответов.
func (svc *ordersService) PlaceOrder(ctx context.Context, req *orders.Order) (*orders.OrderState, error) {
	svc.s.countPlace() // до unaryGate: попытка, зарубленная фолтом, — тоже нагрузка на брокера
	after, mangle, abort := svc.s.unaryGate("PlaceOrder")
	if abort != nil {
		return nil, abort
	}
	s := svc.s
	acc, _, err := s.checkAccount(ctx, req.GetAccountId())
	if err != nil {
		return nil, err
	}

	qty := parseDec(req.GetQuantity().GetValue())
	if qty <= 0 {
		return nil, status.Error(codes.InvalidArgument, "quantity must be positive")
	}
	if req.GetSide() != v1.Side_SIDE_BUY && req.GetSide() != v1.Side_SIDE_SELL {
		return nil, status.Error(codes.InvalidArgument, "side must be BUY or SELL")
	}
	cid := req.GetClientOrderId()
	if len(cid) > maxClientOrderID {
		return nil, status.Errorf(codes.InvalidArgument, "client_order_id longer than %d chars", maxClientOrderID)
	}
	var limitPrice float64
	switch req.GetType() {
	case orders.OrderType_ORDER_TYPE_LIMIT:
		limitPrice = parseDec(req.GetLimitPrice().GetValue())
		if limitPrice <= 0 {
			return nil, status.Error(codes.InvalidArgument, "limit order requires positive limit_price")
		}
	case orders.OrderType_ORDER_TYPE_MARKET:
	default:
		return nil, status.Errorf(codes.InvalidArgument, "order type %s is not supported by brokersim", req.GetType())
	}

	s.mu.Lock()
	if _, ok := s.symbolLocked(req.GetSymbol()); !ok {
		s.mu.Unlock()
		return nil, status.Errorf(codes.NotFound, "unknown symbol %s", req.GetSymbol())
	}
	if cid != "" {
		if _, dup := acc.clientIDs[cid]; dup {
			s.mu.Unlock()
			return nil, status.Errorf(codes.AlreadyExists, "client_order_id %q already used today", cid)
		}
	}
	if err := s.consumePlaceQuotaLocked(); err != nil {
		s.mu.Unlock()
		return nil, err
	}

	now := s.now()
	o := &simOrder{
		id:            itoa(100_000_000 + s.nextSeq()),
		execID:        "E" + itoa(s.nextSeq()),
		accountID:     acc.id,
		symbol:        req.GetSymbol(),
		side:          req.GetSide(),
		typ:           req.GetType(),
		tif:           req.GetTimeInForce(),
		clientOrderID: cid,
		limitPrice:    limitPrice,
		qty:           qty,
		status:        orders.OrderStatus_ORDER_STATUS_NEW,
		placedAt:      now,
		updatedAt:     now,
	}
	if cid == "" {
		o.clientOrderID = "b" + itoa(s.nextSeq()) // брокерский автогенерат, как в боевом API
	}
	s.orders[o.id] = o
	// order id для ОТВЕТА (не для внутреннего ордера): фолт может отдать клиенту
	// пустой/переиспользованный id, тогда как ордер живёт под настоящим o.id и
	// стримит филлы под ним — брокер вернул битый ack. Определяем до append.
	respOrderID := o.id
	switch mangle {
	case mangleBlank:
		respOrderID = ""
	case mangleReuse:
		if len(acc.orders) > 0 {
			respOrderID = acc.orders[len(acc.orders)-1].id // id ПРЕДЫДУЩЕГО ордера счёта
		}
	}
	acc.orders = append(acc.orders, o)
	acc.clientIDs[o.clientOrderID] = o
	s.emitOrderLocked(o)
	resp := o.orderState()
	resp.OrderId = respOrderID
	s.mu.Unlock()

	s.scheduleExec(o.id)

	if err := after(); err != nil {
		return nil, err // потерянный ответ: ордер встал, клиент получил транспортную ошибку
	}
	return resp, nil
}

// scheduleExec запускает отложенный матчинг ордера (ExecLatency спустя постановку).
func (s *Sim) scheduleExec(orderID string) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		select {
		case <-time.After(s.cfg.execLatency()):
		case <-s.stop:
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if o := s.orders[orderID]; o != nil {
			s.execOrderLocked(o)
		}
	}()
}

// execOrderLocked — первичный матчинг ордера об экзогенную книгу. Вызывать под mu.
func (s *Sim) execOrderLocked(o *simOrder) {
	if !o.active() {
		return
	}
	st, ok := s.symbols[o.symbol]
	if !ok {
		return
	}
	now := s.now()
	buy := o.side == v1.Side_SIDE_BUY

	if o.typ == orders.OrderType_ORDER_TYPE_LIMIT {
		crossing := false
		if buy {
			if ask, has := st.bestAsk(); has && o.limitPrice >= ask {
				crossing = true
			}
		} else {
			if bid, has := st.bestBid(); has && o.limitPrice <= bid {
				crossing = true
			}
		}
		if o.tif == orders.TimeInForce_TIME_IN_FORCE_GOOD_TILL_CROSSING {
			if crossing {
				// Пост-онли пересёк спред: биржа снимает ордер целиком.
				o.status = orders.OrderStatus_ORDER_STATUS_REJECTED_BY_EXCHANGE
				o.withdrawAt = now
				o.updatedAt = now
				s.emitOrderLocked(o)
			}
			return // не кроссит — стоит в книге и ждёт
		}
		if crossing {
			s.takerExecLocked(o, st, o.limitPrice, now)
		}
		return
	}

	// Маркет-ордер: съедает доступную ликвидность; пустая книга — реджект биржи,
	// частичная ликвидность — фил на сколько есть и снятие остатка (инцидент
	// «подтверждённо-мёртвый хедж с недобором», который движок обязан допарировать).
	s.takerExecLocked(o, st, 0, now)
}

// takerExecLocked исполняет ордер как тейкер против книги: limit>0 ограничивает
// цену уровня, 0 — маркет без ограничения. Остаток маркет-ордера снимается.
func (s *Sim) takerExecLocked(o *simOrder, st *symbolState, limit float64, now time.Time) {
	buy := o.side == v1.Side_SIDE_BUY
	fills, rows := st.consumeBounded(buy, o.qty-o.executed, limit, now)
	s.broadcastBookLocked(o.symbol, rows)
	for _, f := range fills {
		tr := publicTrade("T"+itoa(s.nextSeq()), f.Price, f.Size, o.side, now)
		st.appendTape(tr)
		s.broadcastTapeLocked(o.symbol, tr)
		s.fillOrderLocked(o, f.Size, f.Price, now)
	}
	if o.typ == orders.OrderType_ORDER_TYPE_MARKET && o.active() {
		if o.executed == 0 {
			o.status = orders.OrderStatus_ORDER_STATUS_REJECTED_BY_EXCHANGE // нет ликвидности
		} else {
			o.status = orders.OrderStatus_ORDER_STATUS_CANCELED // недобор снят биржей
		}
		o.withdrawAt = now
		o.updatedAt = now
		s.emitOrderLocked(o)
	}
}

// fillOrderLocked применяет один фил: состояние ордера, позиция и кэш счёта,
// AccountTrade, события в стримы ордеров/сделок/счёта. Вызывать под mu.
func (s *Sim) fillOrderLocked(o *simOrder, lots, price float64, now time.Time) {
	if lots <= 0 || !o.active() {
		return
	}
	if rest := o.qty - o.executed; lots > rest {
		lots = rest
	}
	o.executed += lots
	o.updatedAt = now
	o.execID = "E" + itoa(s.nextSeq())
	if o.executed >= o.qty {
		o.status = orders.OrderStatus_ORDER_STATUS_FILLED
	} else {
		o.status = orders.OrderStatus_ORDER_STATUS_PARTIALLY_FILLED
	}

	acc := s.accounts[o.accountID]
	s.applyFillToAccountLocked(acc, o.symbol, o.side, lots, price)

	trade := &v1.AccountTrade{
		TradeId:   "AT" + itoa(s.nextSeq()),
		Symbol:    o.symbol,
		Price:     dec(price),
		Size:      dec(lots),
		Side:      o.side,
		Timestamp: timestamppb.New(now),
		OrderId:   o.id,
		AccountId: o.accountID,
	}
	acc.trades = append(acc.trades, trade)
	if st := s.symbols[o.symbol]; st != nil {
		st.last = price
	}

	s.emitOrderLocked(o)
	s.emitTradeLocked(trade)
	s.emitAccountLocked(o.accountID)
}

// applyFillToAccountLocked обновляет позицию и кэш: усреднение при наборе,
// реализованный PnL в кэш при сокращении/перевороте.
func (s *Sim) applyFillToAccountLocked(acc *account, symbol string, side v1.Side, lots, price float64) {
	pos := acc.positions[symbol]
	if pos == nil {
		pos = &position{}
		acc.positions[symbol] = pos
	}
	signed := lots
	if side == v1.Side_SIDE_SELL {
		signed = -lots
	}
	switch {
	case pos.qty == 0 || (pos.qty > 0) == (signed > 0):
		// набор позиции: усредняем
		total := abs(pos.qty) + lots
		pos.avg = (pos.avg*abs(pos.qty) + price*lots) / total
		pos.qty += signed
	default:
		// сокращение/переворот: реализуем PnL закрытой части
		closeLots := lots
		if closeLots > abs(pos.qty) {
			closeLots = abs(pos.qty)
		}
		dir := 1.0
		if pos.qty < 0 {
			dir = -1
		}
		acc.cash += (price - pos.avg) * closeLots * dir
		pos.qty += signed
		if pos.qty == 0 {
			pos.avg = 0
		} else if (pos.qty > 0) != (dir > 0) {
			pos.avg = price // перевернулись: остаток открыт по цене сделки
		}
	}
}

func abs(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}

// CancelOrder снимает ордер. Синхронный режим (default): применяет отмену и
// отвечает CANCELED + событие в стрим. CancelAsync: отвечает PENDING_CANCEL,
// CANCELED приходит стримом через ExecLatency — режим для отладки неподтверждённых
// отмен (retireQ движка).
func (svc *ordersService) CancelOrder(ctx context.Context, req *orders.CancelOrderRequest) (*orders.OrderState, error) {
	svc.s.countCancel() // до unaryGate: см. countPlace в PlaceOrder
	after, _, abort := svc.s.unaryGate("CancelOrder")
	if abort != nil {
		return nil, abort
	}
	s := svc.s
	if _, _, err := s.checkAccount(ctx, req.GetAccountId()); err != nil {
		return nil, err
	}
	s.mu.Lock()
	o := s.orders[req.GetOrderId()]
	if o == nil || o.accountID != req.GetAccountId() {
		s.mu.Unlock()
		return nil, status.Errorf(codes.NotFound, "order %s not found", req.GetOrderId())
	}
	if o.terminal() {
		s.mu.Unlock()
		return nil, status.Errorf(codes.FailedPrecondition, "order %s is already %s", o.id, o.status)
	}
	now := s.now()
	if s.cfg.CancelAsync {
		o.status = orders.OrderStatus_ORDER_STATUS_PENDING_CANCEL
		o.updatedAt = now
		s.emitOrderLocked(o)
		resp := o.orderState()
		s.mu.Unlock()
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			select {
			case <-time.After(s.cfg.execLatency()):
			case <-s.stop:
				return
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			// active(), не ==PENDING_CANCEL: частичный фил в окне задержки
			// переводит статус в PARTIALLY_FILLED, но отмена остатка обязана
			// состояться — как у боевого брокера (CANCELED c executed<qty).
			// Полный фил/терминал в окне — отмена опоздала, ничего не делаем.
			if o.active() {
				o.status = orders.OrderStatus_ORDER_STATUS_CANCELED
				o.withdrawAt = s.now()
				o.updatedAt = o.withdrawAt
				s.emitOrderLocked(o)
			}
		}()
		if err := after(); err != nil {
			return nil, err
		}
		return resp, nil
	}
	o.status = orders.OrderStatus_ORDER_STATUS_CANCELED
	o.withdrawAt = now
	o.updatedAt = now
	s.emitOrderLocked(o)
	resp := o.orderState()
	s.mu.Unlock()
	if err := after(); err != nil {
		return nil, err // отмена применена, ответ «потерян»
	}
	return resp, nil
}

func (svc *ordersService) GetOrder(ctx context.Context, req *orders.GetOrderRequest) (*orders.OrderState, error) {
	if err := svc.s.gateReadOnly("GetOrder"); err != nil {
		return nil, err
	}
	s := svc.s
	if _, _, err := s.checkAccount(ctx, req.GetAccountId()); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	o := s.orders[req.GetOrderId()]
	if o == nil || o.accountID != req.GetAccountId() {
		return nil, status.Errorf(codes.NotFound, "order %s not found", req.GetOrderId())
	}
	return o.orderState(), nil
}

// GetOrders возвращает список заявок счёта. Default — только АКТИВНЫЕ, как
// документирован боевой API («Возвращает список активных заявок для счета»):
// восстановление по client_order_id находит стоящий ордер, но не мгновенно
// исполнившийся (слепая зона «fill-and-vanish», страхуемая reconcile).
// OrdersListIncludesTerminal=true расширяет список всеми ордерами дня.
func (svc *ordersService) GetOrders(ctx context.Context, req *orders.OrdersRequest) (*orders.OrdersResponse, error) {
	if err := svc.s.gateReadOnly("GetOrders"); err != nil {
		return nil, err
	}
	s := svc.s
	acc, _, err := s.checkAccount(ctx, req.GetAccountId())
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	resp := &orders.OrdersResponse{}
	for _, o := range acc.orders {
		if !s.cfg.ordersListIncludesTerminal() && o.terminal() {
			continue
		}
		resp.Orders = append(resp.Orders, o.orderState())
	}
	return resp, nil
}

// SubscribeOrders — стрим состояний ордеров счёта: снапшот активных при
// подписке, дальше live-события. Стрим умирает вместе со своим токеном.
func (svc *ordersService) SubscribeOrders(req *orders.SubscribeOrdersRequest, stream grpc.ServerStreamingServer[orders.SubscribeOrdersResponse]) error {
	s := svc.s
	if err := s.gateReadOnly("SubscribeOrders"); err != nil {
		return err
	}
	acc, tokenExp, err := s.checkAccount(stream.Context(), req.GetAccountId())
	if err != nil {
		return err
	}
	sub := &eventSub[*orders.OrderState]{accountID: acc.id, ch: make(chan *orders.OrderState, subBuffer)}
	sub.tokenExp.set(tokenExp)

	s.mu.Lock()
	var snapshot []*orders.OrderState
	for _, o := range acc.orders {
		if o.active() {
			snapshot = append(snapshot, o.orderState())
		}
	}
	s.orderSubs[sub] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.orderSubs, sub)
		s.mu.Unlock()
	}()

	if len(snapshot) > 0 {
		if err := streamSend(s, stream, "SubscribeOrders", &orders.SubscribeOrdersResponse{Orders: snapshot}); err != nil {
			return err
		}
	}
	return runEventStream(s, stream, sub, "SubscribeOrders", func(o *orders.OrderState) *orders.SubscribeOrdersResponse {
		return &orders.SubscribeOrdersResponse{Orders: []*orders.OrderState{o}}
	})
}

// SubscribeTrades — стрим собственных сделок счёта. При подписке РЕПЛЕИТ все
// сегодняшние сделки (как боевой API — этот burst обязана фильтровать
// клиентская дедупликация), дальше live.
func (svc *ordersService) SubscribeTrades(req *orders.SubscribeTradesRequest, stream grpc.ServerStreamingServer[orders.SubscribeTradesResponse]) error {
	s := svc.s
	if err := s.gateReadOnly("SubscribeTrades"); err != nil {
		return err
	}
	acc, tokenExp, err := s.checkAccount(stream.Context(), req.GetAccountId())
	if err != nil {
		return err
	}
	sub := &eventSub[*v1.AccountTrade]{accountID: acc.id, ch: make(chan *v1.AccountTrade, subBuffer)}
	sub.tokenExp.set(tokenExp)

	s.mu.Lock()
	replay := make([]*v1.AccountTrade, 0, len(acc.trades))
	for _, t := range acc.trades {
		replay = append(replay, proto.Clone(t).(*v1.AccountTrade))
	}
	s.tradeSubs[sub] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.tradeSubs, sub)
		s.mu.Unlock()
	}()

	if len(replay) > 0 {
		if err := streamSend(s, stream, "SubscribeTrades", &orders.SubscribeTradesResponse{Trades: replay}); err != nil {
			return err
		}
	}
	return runEventStream(s, stream, sub, "SubscribeTrades", func(t *v1.AccountTrade) *orders.SubscribeTradesResponse {
		return &orders.SubscribeTradesResponse{Trades: []*v1.AccountTrade{t}}
	})
}

// runEventStream — общий цикл доставки событий подписчику: события из канала,
// стрим-сбои (kill/silence/dup), смерть стрима по протухшему токену.
func runEventStream[E any, R any](s *Sim, stream grpc.ServerStreamingServer[R], sub *eventSub[E], method string, wrap func(E) *R) error {
	tokenCheck := time.NewTicker(time.Second)
	defer tokenCheck.Stop()
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-s.stop:
			return status.Error(codes.Unavailable, "brokersim shutting down")
		case <-tokenCheck.C:
			if sub.tokenExp.expired(s.now()) {
				return status.Error(codes.Unauthenticated, "token expired")
			}
			// kill_stream рвёт и простаивающий стрим (реальный reset соединения
			// не ждёт следующего события).
			if err := s.faults.gateKill(method); err != nil {
				return err
			}
		case ev := <-sub.ch:
			if err := streamSend(s, stream, method, wrap(ev)); err != nil {
				return err
			}
		}
	}
}

// streamSend отправляет одно сообщение стрима с учётом стрим-сбоев
// (kill_stream/silence/dup_events).
func streamSend[R any](s *Sim, stream grpc.ServerStreamingServer[R], method string, msg *R) error {
	if err := s.faults.gateKill(method); err != nil {
		return err
	}
	d := s.faults.peek(method)
	if d.silence {
		return nil // события молча теряются, стрим жив — мёртвый фид
	}
	if err := stream.Send(msg); err != nil {
		return err
	}
	if d.dup {
		// Повторная отправка того же сообщения безопасна: Send только
		// сериализует его, а после streamSend сообщение никто не мутирует.
		return stream.Send(msg)
	}
	return nil
}

// consumePlaceQuotaLocked — учёт квоты OrdersService.placeOrder (окно
// PlaceQuotaWindow, лимит PlaceQuotaLimit). Вызывать под mu.
func (s *Sim) consumePlaceQuotaLocked() error {
	now := s.now()
	if now.Sub(s.quotaStart) >= s.cfg.placeQuotaWindow() {
		s.quotaStart = now
		s.quotaUsed = 0
	}
	if s.quotaUsed >= s.cfg.placeQuotaLimit() {
		return status.Error(codes.ResourceExhausted, "placeOrder quota exhausted")
	}
	s.quotaUsed++
	return nil
}

func parseDec(v string) float64 {
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0
	}
	return f
}
