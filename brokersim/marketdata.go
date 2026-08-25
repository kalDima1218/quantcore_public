package brokersim

import (
	"context"
	"time"

	v1 "github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1"
	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/marketdata"
	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/orders"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// mdService реализует tradeapi.v1.marketdata.MarketDataService (используемое
// ботом подмножество: стрим стакана и публичной ленты + унарные срезы).
type mdService struct {
	marketdata.UnimplementedMarketDataServiceServer
	s *Sim
}

// bookSub — подписчик стрима стакана одного символа.
type bookSub struct {
	symbol   string
	ch       chan *marketdata.SubscribeOrderBookResponse
	tokenExp tokenExpiry
}

// tapeSub — подписчик публичной ленты одного символа.
type tapeSub struct {
	symbol   string
	ch       chan *marketdata.Trade
	tokenExp tokenExpiry
}

// broadcastBookLocked рассылает дельты книги подписчикам символа. Вызывать под mu.
func (s *Sim) broadcastBookLocked(symbol string, rows []*marketdata.StreamOrderBook_Row) {
	if len(rows) == 0 {
		return
	}
	resp := &marketdata.SubscribeOrderBookResponse{
		OrderBook: []*marketdata.StreamOrderBook{{Symbol: symbol, Rows: rows}},
	}
	for sub := range s.bookSubs {
		if sub.symbol != symbol {
			continue
		}
		sendClone(sub.ch, resp)
	}
}

// broadcastTapeLocked рассылает публичную сделку подписчикам ленты. Вызывать под mu.
func (s *Sim) broadcastTapeLocked(symbol string, tr *marketdata.Trade) {
	for sub := range s.tapeSubs {
		if sub.symbol != symbol {
			continue
		}
		sendClone(sub.ch, tr)
	}
}

// matchPublicPrintLocked исполняет лимитники, задетые публичной сделкой: печать
// по цене P задевает покупки с лимитом >= P и продажи с лимитом <= P (FIFO по
// времени постановки), расходуя объём печати. Вызывать под mu.
func (s *Sim) matchPublicPrintLocked(st *symbolState, tr *marketdata.Trade) {
	price := parseDec(tr.GetPrice().GetValue())
	size := parseDec(tr.GetSize().GetValue())
	if price <= 0 || size <= 0 {
		return
	}
	now := s.now()
	for _, o := range s.ordersByTimeLocked(st.cfg.Symbol) {
		if size <= 0 {
			break
		}
		if !o.active() || o.typ != orders.OrderType_ORDER_TYPE_LIMIT {
			continue
		}
		buy := o.side == v1.Side_SIDE_BUY
		if (buy && o.limitPrice >= price) || (!buy && o.limitPrice <= price) {
			lots := o.qty - o.executed
			if lots > size {
				lots = size
			}
			// Фил по лимитной цене ордера (пассив стоит в книге по своей цене).
			s.fillOrderLocked(o, lots, o.limitPrice, now)
			size -= lots
		}
	}
}

// ordersByTimeLocked — активные ордера символа в порядке постановки. Вызывать под mu.
func (s *Sim) ordersByTimeLocked(symbol string) []*simOrder {
	var out []*simOrder
	for _, acc := range s.accounts {
		for _, o := range acc.orders {
			if o.symbol == symbol && o.active() {
				out = append(out, o)
			}
		}
	}
	// Порядок постановки = порядок id (общий счётчик), сортировка вставками:
	// активных ордеров единицы.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1].placedAt.After(out[j].placedAt); j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}

// SubscribeOrderBook: первое сообщение — полный снапшот книги ADD-строками,
// дальше инкрементальные дельты (ADD/UPDATE/REMOVE). Периодические пустые
// ответы служат heartbeat'ом; fault "silence" глушит и их (инцидент мёртвого
// фида — клиентский heartbeat-timeout обязан переподключиться).
func (svc *mdService) SubscribeOrderBook(req *marketdata.SubscribeOrderBookRequest, stream grpc.ServerStreamingServer[marketdata.SubscribeOrderBookResponse]) error {
	s := svc.s
	if err := s.gateReadOnly("SubscribeOrderBook"); err != nil {
		return err
	}
	_, tokenExp, err := s.checkAuth(stream.Context())
	if err != nil {
		return err
	}

	s.mu.Lock()
	st, ok := s.symbolLocked(req.GetSymbol())
	if !ok {
		s.mu.Unlock()
		return status.Errorf(codes.NotFound, "unknown symbol %s", req.GetSymbol())
	}
	sub := &bookSub{symbol: req.GetSymbol(), ch: make(chan *marketdata.SubscribeOrderBookResponse, subBuffer)}
	sub.tokenExp.set(tokenExp)
	snapshot := &marketdata.SubscribeOrderBookResponse{
		OrderBook: []*marketdata.StreamOrderBook{{Symbol: req.GetSymbol(), Rows: st.snapshotRows(s.now())}},
	}
	s.bookSubs[sub] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.bookSubs, sub)
		s.mu.Unlock()
	}()

	if err := streamSend(s, stream, "SubscribeOrderBook", snapshot); err != nil {
		return err
	}

	heartbeat := time.NewTicker(s.cfg.heartbeatInterval())
	defer heartbeat.Stop()
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
			if err := s.faults.gateKill("SubscribeOrderBook"); err != nil {
				return err
			}
		case <-heartbeat.C:
			if err := streamSend(s, stream, "SubscribeOrderBook", &marketdata.SubscribeOrderBookResponse{}); err != nil {
				return err
			}
		case resp := <-sub.ch:
			if err := streamSend(s, stream, "SubscribeOrderBook", resp); err != nil {
				return err
			}
		}
	}
}

// SubscribeLatestTrades: реплей последних TapeReplayDepth публичных сделок,
// дальше live-лента.
func (svc *mdService) SubscribeLatestTrades(req *marketdata.SubscribeLatestTradesRequest, stream grpc.ServerStreamingServer[marketdata.SubscribeLatestTradesResponse]) error {
	s := svc.s
	if err := s.gateReadOnly("SubscribeLatestTrades"); err != nil {
		return err
	}
	_, tokenExp, err := s.checkAuth(stream.Context())
	if err != nil {
		return err
	}

	s.mu.Lock()
	st, ok := s.symbolLocked(req.GetSymbol())
	if !ok {
		s.mu.Unlock()
		return status.Errorf(codes.NotFound, "unknown symbol %s", req.GetSymbol())
	}
	sub := &tapeSub{symbol: req.GetSymbol(), ch: make(chan *marketdata.Trade, subBuffer)}
	sub.tokenExp.set(tokenExp)
	depth := s.cfg.tapeReplayDepth()
	start := len(st.tape) - depth
	if start < 0 {
		start = 0
	}
	replay := make([]*marketdata.Trade, 0, len(st.tape)-start)
	for _, t := range st.tape[start:] {
		replay = append(replay, proto.Clone(t).(*marketdata.Trade))
	}
	s.tapeSubs[sub] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.tapeSubs, sub)
		s.mu.Unlock()
	}()

	if len(replay) > 0 {
		if err := streamSend(s, stream, "SubscribeLatestTrades", &marketdata.SubscribeLatestTradesResponse{Symbol: req.GetSymbol(), Trades: replay}); err != nil {
			return err
		}
	}
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
			if err := s.faults.gateKill("SubscribeLatestTrades"); err != nil {
				return err
			}
		case tr := <-sub.ch:
			if err := streamSend(s, stream, "SubscribeLatestTrades", &marketdata.SubscribeLatestTradesResponse{Symbol: req.GetSymbol(), Trades: []*marketdata.Trade{tr}}); err != nil {
				return err
			}
		}
	}
}

// OrderBook — унарный срез книги.
func (svc *mdService) OrderBook(ctx context.Context, req *marketdata.OrderBookRequest) (*marketdata.OrderBookResponse, error) {
	if err := svc.s.gateReadOnly("OrderBook"); err != nil {
		return nil, err
	}
	s := svc.s
	if _, _, err := s.checkAuth(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.symbolLocked(req.GetSymbol())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown symbol %s", req.GetSymbol())
	}
	now := timestamppb.New(s.now())
	rows := make([]*marketdata.OrderBook_Row, 0, len(st.bids)+len(st.asks))
	for _, p := range sortedPrices(st.bids, true) {
		rows = append(rows, &marketdata.OrderBook_Row{
			Price:     dec(p),
			Side:      &marketdata.OrderBook_Row_BuySize{BuySize: dec(st.bids[p])},
			Action:    marketdata.OrderBook_Row_ACTION_ADD,
			Timestamp: now,
		})
	}
	for _, p := range sortedPrices(st.asks, false) {
		rows = append(rows, &marketdata.OrderBook_Row{
			Price:     dec(p),
			Side:      &marketdata.OrderBook_Row_SellSize{SellSize: dec(st.asks[p])},
			Action:    marketdata.OrderBook_Row_ACTION_ADD,
			Timestamp: now,
		})
	}
	return &marketdata.OrderBookResponse{Symbol: req.GetSymbol(), Orderbook: &marketdata.OrderBook{Rows: rows}}, nil
}
