package finambroker

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/orders"

	"QuantCore/strategies/execengine2"
	"QuantCore/trade/finam"
)

const (
	idRandomBytes = 7
	idRandomChars = 12
	idCountChars  = 7
	maxIDCount    = uint64(78_364_164_095) // 36^7 - 1
	sendTries     = cidRetryRounds
	findTries     = ghostProbes
	findWait      = ghostProbeGap
)

type clientIDs struct {
	prefix string
	count  atomic.Uint64
}

func newClientIDs(source io.Reader) (*clientIDs, error) {
	var raw [idRandomBytes]byte
	if _, err := io.ReadFull(source, raw[:]); err != nil {
		return nil, fmt.Errorf("reading client-id nonce: %w", err)
	}
	encoded := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(raw[:])
	prefix := strings.ToLower(encoded)
	if len(prefix) != idRandomChars {
		return nil, fmt.Errorf("client-id nonce length = %d, want %d", len(prefix), idRandomChars)
	}
	return &clientIDs{prefix: prefix}, nil
}

func (g *clientIDs) Next() (string, error) {
	count := g.count.Add(1)
	if count > maxIDCount {
		return "", errors.New("client-order-id sequence exhausted")
	}
	encoded := strconv.FormatUint(count, 36)
	id := "q" + g.prefix + strings.Repeat("0", idCountChars-len(encoded)) + encoded
	if len(id) != 20 {
		return "", fmt.Errorf("client order id length = %d, want 20", len(id))
	}
	return id, nil
}

type apiOrder struct {
	id     string
	symbol string
	side   execengine2.Side
	kind   execengine2.OrderKind
	lots   int
	price  float64
	filled int
	done   bool
}

type ordersAPI interface {
	Place(context.Context, execengine2.OrderRequest, string) (apiOrder, error)
	Find(context.Context, string) (apiOrder, bool, error)
	Cancel(context.Context, string) (apiOrder, error)
	Status(context.Context, string) (apiOrder, error)
}

type waiter interface {
	Wait(context.Context, time.Duration) error
}

type timerWait struct{}

func (timerWait) Wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Gateway — единая точка работы execengine2 с заявками Finam.
// В нём хранятся API, генератор ID, таймер и метка лога.
type Gateway struct {
	api    ordersAPI
	ids    *clientIDs
	waiter waiter
	limit  execengine2.SendLimit
	logTag string
}

// NewGateway создаёт Broker для Finam. limit должен быть тем же объектом,
// который передан в Engine: Gateway списывает из него внутренние повторы.
func NewGateway(
	client *finam.Client,
	limit execengine2.SendLimit,
	logTag string,
) (*Gateway, error) {
	if client == nil {
		return nil, errors.New("finam client is required")
	}
	if limit == nil {
		return nil, errors.New("send limit is required")
	}
	ids, err := newClientIDs(rand.Reader)
	if err != nil {
		return nil, err
	}
	return &Gateway{
		api: &finamAPI{client: client}, ids: ids, waiter: timerWait{}, limit: limit, logTag: logTag,
	}, nil
}

func (g *Gateway) logf(format string, args ...any) {
	mlog.Printf("[execengine2]"+g.logTag+" "+format, args...)
}

func (g *Gateway) critical(format string, args ...any) {
	mlog.Critical("[execengine2]"+g.logTag+" "+format, args...)
}

// Place повторяет отправку только с тем же client ID.
// При отмене ctx проверка сразу останавливается.
func (g *Gateway) Place(
	ctx context.Context,
	req execengine2.OrderRequest,
) (string, error) {
	if ctx == nil {
		return "", errors.New("nil context")
	}
	if err := checkRequest(req); err != nil {
		return "", execengine2.NotPlaced(err)
	}
	if err := ctx.Err(); err != nil {
		return "", execengine2.NotPlaced(err)
	}
	clientID, err := g.ids.Next()
	if err != nil {
		return "", execengine2.NotPlaced(err)
	}

	var sendErr error
	for round := 0; round < sendTries; round++ {
		if round > 0 && (g.limit == nil || !g.limit.Take(1, retryKind(req))) {
			err := errors.New("send limit blocked retry")
			return "", execengine2.OrderUnknown(clientID, errors.Join(sendErr, err))
		}
		order, placeErr := g.api.Place(ctx, req, clientID)
		if placeErr == nil {
			if !sameOrder(order, req) {
				err := errors.New("successful placement response does not match request")
				g.critical("client id %s returned a mismatched placement response", clientID)
				return "", execengine2.OrderUnknown(clientID, err)
			}
			return order.id, nil
		}
		if !ambiguous(placeErr) {
			return "", execengine2.NotPlaced(placeErr)
		}
		if sendErr == nil {
			sendErr = placeErr
		}
		for range findTries {
			if waitErr := g.waiter.Wait(ctx, findWait); waitErr != nil {
				return "", execengine2.OrderUnknown(clientID, errors.Join(sendErr, waitErr))
			}
			order, found, findErr := g.api.Find(ctx, clientID)
			if findErr != nil {
				continue
			}
			if !found {
				continue // active-list absence does not prove the order never existed
			}
			if !sameOrder(order, req) {
				g.critical("client id %s resolved to a mismatched order; refusing adoption", clientID)
				continue
			}
			g.logf("client id %s resolved to broker order %s", clientID, order.id)
			return order.id, nil
		}
		if round < sendTries-1 {
			if waitErr := g.waiter.Wait(ctx, findWait); waitErr != nil {
				return "", execengine2.OrderUnknown(clientID, errors.Join(sendErr, waitErr))
			}
		}
	}
	return "", execengine2.OrderUnknown(clientID, sendErr)
}

func retryKind(req execengine2.OrderRequest) execengine2.LimitKind {
	switch req.Role {
	case execengine2.RoleHedge, execengine2.RoleFix, execengine2.RoleLateFill:
		return execengine2.LimitMust
	default:
		return execengine2.LimitNormal
	}
}

// Cancel отменяет заявку.
func (g *Gateway) Cancel(ctx context.Context, orderID string) (execengine2.CancelResult, error) {
	if ctx == nil {
		return execengine2.CancelResult{}, errors.New("nil context")
	}
	if orderID == "" {
		return execengine2.CancelResult{}, errors.New("empty order id")
	}
	order, err := g.api.Cancel(ctx, orderID)
	if err != nil {
		return execengine2.CancelResult{}, err
	}
	return execengine2.CancelResult{Filled: order.filled}, nil
}

// Status получает состояние заявки.
func (g *Gateway) Status(ctx context.Context, orderID string) (execengine2.OrderStatus, error) {
	if ctx == nil {
		return execengine2.OrderStatus{}, errors.New("nil context")
	}
	if orderID == "" {
		return execengine2.OrderStatus{}, errors.New("empty order id")
	}
	order, err := g.api.Status(ctx, orderID)
	if err != nil {
		return execengine2.OrderStatus{}, err
	}
	return execengine2.OrderStatus{Filled: order.filled, Done: order.done}, nil
}

func checkRequest(req execengine2.OrderRequest) error {
	if req.Symbol == "" || req.Lots <= 0 {
		return errors.New("order needs a symbol and positive lots")
	}
	if req.Side != execengine2.SideBuy && req.Side != execengine2.SideSell {
		return errors.New("bad order side")
	}
	if req.Kind != execengine2.OrderLimit && req.Kind != execengine2.OrderMarket {
		return errors.New("bad order kind")
	}
	if req.Kind == execengine2.OrderLimit && req.Price <= 0 {
		return errors.New("limit order needs a positive price")
	}
	return nil
}

func sameOrder(order apiOrder, req execengine2.OrderRequest) bool {
	if order.id == "" || order.symbol != req.Symbol || order.side != req.Side ||
		order.kind != req.Kind || order.lots != req.Lots {
		return false
	}
	return req.Kind != execengine2.OrderLimit || math.Abs(order.price-req.Price) <= 1e-6
}

type finamAPI struct {
	client *finam.Client
}

func (a *finamAPI) Place(
	ctx context.Context,
	req execengine2.OrderRequest,
	clientID string,
) (apiOrder, error) {
	ticker := finam.Ticker{Symbol: req.Symbol, Vol: req.Lots}
	var (
		state *orders.OrderState
		err   error
	)
	switch {
	case req.Kind == execengine2.OrderLimit && req.Side == execengine2.SideBuy:
		state, err = finam.PlaceLimitOrderBuyContext(ctx, a.client, ticker, req.Price, clientID)
	case req.Kind == execengine2.OrderLimit:
		state, err = finam.PlaceLimitOrderSellContext(ctx, a.client, ticker, req.Price, clientID)
	case req.Side == execengine2.SideBuy:
		state, err = finam.PlaceMarketOrderBuyContext(ctx, a.client, ticker, clientID)
	default:
		state, err = finam.PlaceMarketOrderSellContext(ctx, a.client, ticker, clientID)
	}
	if err != nil {
		return apiOrder{}, err
	}
	return fromFinam(state)
}

func (a *finamAPI) Find(
	ctx context.Context,
	clientID string,
) (apiOrder, bool, error) {
	state, found, err := finam.FindOrderByClientIDContext(ctx, a.client, clientID)
	if err != nil || !found {
		return apiOrder{}, found, err
	}
	order, err := fromFinam(state)
	return order, err == nil, err
}

func (a *finamAPI) Cancel(ctx context.Context, orderID string) (apiOrder, error) {
	state, err := finam.CancelOrderContext(ctx, a.client, orderID)
	if err != nil {
		return apiOrder{}, err
	}
	return fromFinam(state)
}

func (a *finamAPI) Status(ctx context.Context, orderID string) (apiOrder, error) {
	state, err := finam.GetOrderContext(ctx, a.client, orderID)
	if err != nil {
		return apiOrder{}, err
	}
	return fromFinam(state)
}

func fromFinam(state *orders.OrderState) (apiOrder, error) {
	if state == nil || state.GetOrder() == nil {
		return apiOrder{}, errors.New("finam returned an empty order")
	}
	finamOrder := state.GetOrder()
	side := execengine2.SideSell
	if finam.SideMatches(state, true) {
		side = execengine2.SideBuy
	}
	kind := execengine2.OrderLimit
	switch finamOrder.GetType() {
	case orders.OrderType_ORDER_TYPE_LIMIT:
	case orders.OrderType_ORDER_TYPE_MARKET:
		kind = execengine2.OrderMarket
	default:
		return apiOrder{}, errors.New("finam returned an unknown order type")
	}
	return apiOrder{
		id: state.GetOrderId(), symbol: finamOrder.GetSymbol(), side: side, kind: kind,
		price: finam.ParseDecimal(finamOrder.GetLimitPrice().GetValue()),
		lots:  finam.InitialLots(state), filled: finam.ExecutedLots(state),
		done: terminalOrderStatus(state.GetStatus()),
	}, nil
}

var _ execengine2.Broker = (*Gateway)(nil)
