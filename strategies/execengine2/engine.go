package execengine2

import (
	"context"
	"errors"
	"fmt"
	"time"

	"QuantCore/strategies/execengine2/internal/hedges"
	"QuantCore/strategies/execengine2/internal/model"
	"QuantCore/strategies/execengine2/internal/orders"
	"QuantCore/strategies/execengine2/internal/quotes"
	"QuantCore/strategies/execengine2/internal/run"
	"QuantCore/strategies/execengine2/internal/trade"
)

// Setup содержит всё, что нужно движку для запуска.
type Setup struct {
	Broker   Broker
	Limit    SendLimit
	Clock    Clock
	Strategy Strategy
	Changes  Updates
	Logger   Logger
}

// Engine принимает события и передаёт работу нужной части.
// Каждое изменяемое состояние хранится только в одной части.
//
// Все методы Engine надо вызывать из одной горутины цикла событий.
// Сам движок не запускает горутины и не держит мьютексы.
type Engine struct {
	config   Config
	broker   Broker
	limit    SendLimit
	clock    Clock
	strategy Strategy
	updates  Updates
	logger   Logger

	quotes *quotes.Book
	trade  *trade.Trade
	orders *orders.List
	hedges *hedges.List
	state  *run.State
}

// Info — короткий снимок состояния движка.
type Info struct {
	Code          RunState
	Reason        string
	HasTrade      bool
	Position      int
	LimitLeft     int64
	MarketOrders  int
	Hedges        int
	OrdersToClose int
}

// NewEngine проверяет настройки и собирает движок.
func NewEngine(config Config, deps Setup) (*Engine, error) {
	config = config.normalized()
	if err := config.validate(); err != nil {
		return nil, fmt.Errorf("invalid execengine2 config: %w", err)
	}
	if deps.Broker == nil {
		return nil, errors.New("execengine2 broker is required")
	}
	if deps.Strategy == nil {
		return nil, errors.New("execengine2 decider is required")
	}
	if deps.Limit == nil {
		deps.Limit = noLimit{}
	}
	if deps.Clock == nil {
		deps.Clock = realClock{}
	}
	if deps.Logger == nil {
		deps.Logger = newLogger(config.LogTag)
	}
	return &Engine{
		config: config, broker: deps.Broker, limit: deps.Limit, clock: deps.Clock,
		strategy: deps.Strategy, updates: deps.Changes, logger: deps.Logger,
		quotes: quotes.New(config.LegA, config.LegB), trade: &trade.Trade{},
		orders: &orders.List{}, hedges: &hedges.List{},
		state: &run.State{},
	}, nil
}

// OnBook принимает новые цены и при необходимости меняет лимитную заявку.
func (e *Engine) OnBook(
	ctx context.Context,
	symbol string,
	at time.Time,
	bestBid float64,
	bestAsk float64,
) error {
	if ctx == nil {
		return errors.New("nil context")
	}
	if !e.quotes.Update(symbol, at, bestBid, bestAsk) {
		return nil
	}
	if !e.HasTrade() {
		return nil
	}
	staleA, staleB := e.quotes.TooOld(at, e.config.BookMaxAge)
	if staleA || staleB {
		return e.stopTrade(ctx, "invalid or stale order book", true)
	}
	leg := model.LegA
	if symbol == e.config.LegB {
		leg = model.LegB
	}
	next, ok := e.trade.NewPrice(
		leg, e.quotes.Prices(leg), at, e.config.PriceWait, e.config.MinRest,
	)
	if !ok {
		return nil
	}
	if !e.limit.Take(1, model.LimitNormal) {
		e.logger.Warnf("repeg of %s denied by placement budget", symbol)
		return nil
	}

	change, err := e.closeOrder(ctx, next.OldOrderID)
	if err != nil {
		return fmt.Errorf("retiring %s for repeg: %w", next.OldOrderID, err)
	}
	if change.Lots != 0 {
		if err := e.useLimitFill(ctx, change, true); err != nil {
			return err
		}
		return nil // исполнение пришло раньше отмены; дальше работает обычный путь сделки
	}

	orderID, err := e.broker.Place(ctx, next.Request)
	if err != nil {
		e.placeFailed(err, "repeg placement")
		abortErr := e.stopTrade(ctx, "repeg placement failed", !OrderMayExist(err))
		return errors.Join(fmt.Errorf("placing repeg: %w", err), abortErr)
	}
	if err := e.addOrder(orderID, next.Request); err != nil {
		return err
	}
	if err := e.trade.SetNewOrder(next, orderID, e.clock.Now()); err != nil {
		e.halt("replacement order could not be attached to its clip: " + err.Error())
		return err
	}
	e.logger.Infof("repegged %s from %s to %s at %.6f", symbol, next.OldOrderID, orderID, next.Request.Price)
	return nil
}

// OnSignal читает новый сигнал стратегии и начинает сделку, если это безопасно.
func (e *Engine) OnSignal(ctx context.Context, state Signal) error {
	if ctx == nil {
		return errors.New("nil context")
	}
	if !e.state.CanOpen() || e.HasTrade() || e.hedges.MarketCount() > 0 ||
		!e.quotes.Ready(state.Time, e.config.BookMaxAge) {
		return nil
	}
	plan := e.strategy.Peek(state)
	if plan.Action == 0 {
		return nil
	}
	lots := plan.Lots
	if lots == 0 {
		lots = e.config.Lots
	}
	if lots <= 0 {
		return errors.New("decider returned a non-positive clip size")
	}
	mode := plan.Mode
	if mode == model.ModeDefault {
		mode = e.config.Mode
	}
	count := orderCount(mode)
	if count == 0 {
		return fmt.Errorf("unsupported execution mode %d", mode)
	}
	if !e.limit.Take(int64(count), model.LimitNormal) {
		e.quotes.BlockOpen(state.Time.Add(e.config.RetryWait))
		e.logger.Warnf("clip open denied by placement budget; remaining=%d", e.limit.Remaining())
		return nil
	}

	requests, err := e.trade.Start(
		plan, mode, e.config.LegA, e.config.LegB,
		e.quotes.Prices(model.LegA), e.quotes.Prices(model.LegB),
		lots, e.config.Ratio, state.Time, e.config.TradeTimeout,
	)
	if err != nil {
		return err
	}
	for _, req := range requests {
		orderID, placeErr := e.broker.Place(ctx, req)
		if placeErr != nil {
			e.placeFailed(placeErr, "opening placement")
			e.quotes.BlockOpen(state.Time.Add(e.config.RetryWait))
			abortErr := e.stopTrade(ctx, "opening placement failed", !OrderMayExist(placeErr))
			return errors.Join(fmt.Errorf("placing opening order on %s: %w", req.Symbol, placeErr), abortErr)
		}
		if err := e.trade.Attach(req.Leg, orderID, e.clock.Now()); err != nil {
			e.halt("placed order could not be attached to its clip: " + err.Error())
			return err
		}
		if err := e.addOrder(orderID, req); err != nil {
			return err
		}
	}
	if e.trade.Full() {
		e.finishTrade(state.Time, false)
	}
	return nil
}

// OnFill принимает одно исполнение нашей заявки.
func (e *Engine) OnFill(ctx context.Context, fill Fill) error {
	if ctx == nil {
		return errors.New("nil context")
	}
	change := e.orders.AddFill(fill)
	if !change.Known || change.Again {
		return nil
	}
	e.sendChange(change, "fill")
	if change.Conflict {
		e.logger.Criticalf("order %s executed beyond its terminal acknowledgement", fill.OrderID)
	}
	if change.Extra > 0 {
		e.logger.Criticalf("order %s reported %d impossible lots beyond placed size; dropped", fill.OrderID, change.Extra)
	}
	if change.Order.Request.Kind == model.OrderMarket {
		e.hedges.SeeFill(fill.OrderID, change.Order.Filled)
		if change.Lots != 0 {
			e.trade.FixMarket(
				change.Order.Request.TradeID,
				change.Order.Request.Leg,
				change.Lots,
			)
		}
		return nil
	}
	return e.useLimitFill(ctx, change, true)
}

// OnOrderStatus принимает состояние заявки напрямую от брокера.
func (e *Engine) OnOrderStatus(ctx context.Context, orderID string, status OrderStatus) error {
	if ctx == nil {
		return errors.New("nil context")
	}
	snap, own := e.orders.Info(orderID)
	if !own {
		return nil
	}
	if snap.Request.Kind == model.OrderMarket {
		return e.useMarketStatus(ctx, orderID, status)
	}
	if !status.Done {
		return nil
	}
	change := e.orders.Close(orderID, status.Filled)
	wasCurrent := e.trade.CloseOrder(orderID)
	e.sendChange(change, "maker terminal status")
	if change.Lots != 0 {
		return e.useLimitFill(ctx, change, true)
	}
	if wasCurrent {
		clip, ok := e.trade.Info()
		if ok && clip.First == model.LegNone {
			return e.stopTrade(ctx, "resting maker became terminal before any fill", true)
		}
	}
	return nil
}

// OnTick проверяет таймауты, рыночные заявки и отложенную работу.
func (e *Engine) OnTick(ctx context.Context, now time.Time) error {
	if ctx == nil {
		return errors.New("nil context")
	}
	var result error
	if e.HasTrade() {
		staleA, staleB := e.quotes.TooOld(now, e.config.BookMaxAge)
		if staleA || staleB || e.trade.TooLate(now) {
			result = errors.Join(result, e.stopTrade(ctx, "clip timeout or stale book", true))
		}
	}
	result = errors.Join(result, e.checkMarketOrders(ctx, now))
	if e.state.FixDue(now) {
		result = errors.Join(result, e.fixWork(ctx, now))
	}
	return result
}

// StopTrade безопасно останавливает текущую сделку.
func (e *Engine) StopTrade(ctx context.Context) error {
	if ctx == nil {
		return errors.New("nil context")
	}
	return e.stopTrade(ctx, "cancel requested", true)
}

// Stop запрещает новые сделки и снимает открытые заявки.
// Уже найденная позиция всё равно будет закрыта хеджем.
func (e *Engine) Stop(ctx context.Context, reason string) error {
	if ctx == nil {
		return errors.New("nil context")
	}
	e.halt(reason)
	return e.stopTrade(ctx, "engine halted", true)
}

// CheckPositions сравнивает позицию брокера с позицией стратегии.
func (e *Engine) CheckPositions(actualA, actualB int) bool {
	expectedA := e.strategy.Position()
	expectedB := -expectedA * e.config.Ratio
	hasWork := e.HasTrade() || len(e.orders.OrdersToClose()) > 0 || e.hedges.HasWork()
	ok := e.state.CheckPositions(actualA, actualB, expectedA, expectedB, hasWork)
	if !ok {
		e.logger.Warnf(
			"reconcile mismatch: actual=(%d,%d) expected=(%d,%d) obligations=%v",
			actualA, actualB, expectedA, expectedB, hasWork,
		)
	}
	return ok
}

// HasTrade сообщает, есть ли сейчас незаконченная сделка.
func (e *Engine) HasTrade() bool {
	_, ok := e.trade.Info()
	return ok
}

// HasOrder проверяет, принадлежит ли заявка этому движку.
func (e *Engine) HasOrder(orderID string) bool { return e.orders.HasOrder(orderID) }

// Position возвращает позицию стратегии.
func (e *Engine) Position() int { return e.strategy.Position() }

// Code возвращает текущее состояние движка.
func (e *Engine) Code() RunState { return e.state.Info().Code }

// Info возвращает снимок состояния движка.
func (e *Engine) Info() Info {
	state := e.state.Info()
	return Info{
		Code: state.Code, Reason: state.Reason, HasTrade: e.HasTrade(),
		Position: e.Position(), LimitLeft: e.limit.Remaining(),
		MarketOrders: e.hedges.MarketCount(), Hedges: len(e.hedges.All()),
		OrdersToClose: len(e.orders.OrdersToClose()),
	}
}

func orderCount(mode model.Mode) int {
	switch mode {
	case model.ModeTwoLimits, model.ModeMarket:
		return 2
	case model.ModeLimitA, model.ModeLimitB:
		return 1
	default:
		return 0
	}
}

func (e *Engine) addOrder(orderID string, req model.OrderRequest) error {
	countNow := req.Kind == model.OrderMarket
	change, err := e.orders.Add(orderID, req, e.clock.Now(), req.Price, countNow)
	if err != nil {
		e.halt("broker returned an unusable order id: " + err.Error())
		return err
	}
	e.sendChange(change, "taker placement")
	if !countNow {
		return nil
	}
	if err := e.hedges.AddMarket(orderID, req, e.clock.Now()); err != nil {
		e.halt("taker could not be tracked: " + err.Error())
		return err
	}
	e.trade.AddMarket(req.TradeID, req.Leg, req.Lots)
	return nil
}

func (e *Engine) sendChange(change orders.Change, reason string) {
	if e.updates == nil || !change.Known {
		return
	}
	price := change.FillPrice
	if price == 0 {
		price = change.Order.GuessPrice
	}
	if change.Lots != 0 {
		e.updates.Apply(model.PositionChange{
			OrderID: change.Order.ID,
			Symbol:  change.Order.Request.Symbol,
			Lots:    change.Lots * change.Order.Request.Side.Sign(),
			Price:   price,
			Reason:  reason,
		})
	}
	if change.PriceLots > 0 && change.FillPrice != 0 &&
		change.FillPrice != change.Order.GuessPrice {
		e.updates.Amend(model.PriceChange{
			OrderID: change.Order.ID,
			Symbol:  change.Order.Request.Symbol,
			Lots:    change.PriceLots,
			From:    change.Order.GuessPrice,
			To:      change.FillPrice,
		})
	}
}

func (e *Engine) useLimitFill(
	ctx context.Context,
	change orders.Change,
	closeOther bool,
) error {
	if change.Lots <= 0 {
		return nil
	}
	transition := e.trade.AddFill(change.Order.ID, change.Lots)
	if !transition.Known {
		return e.fixLateFill(ctx, change.Order.Request, change.Lots)
	}
	if closeOther && transition.First && transition.CloseOrderID != "" {
		other, err := e.closeOrder(ctx, transition.CloseOrderID)
		if err != nil {
			return err
		}
		if other.Lots != 0 {
			if err := e.useLimitFill(ctx, other, false); err != nil {
				return err
			}
		}
	}
	if err := e.hedgeTrade(ctx, model.RoleHedge); err != nil {
		return err
	}
	if e.trade.Full() {
		e.finishTrade(e.clock.Now(), false)
	}
	return nil
}

func (e *Engine) fixLateFill(ctx context.Context, maker model.OrderRequest, lots int) error {
	req := model.OrderRequest{
		Kind: model.OrderMarket, Role: model.RoleLateFill,
		Side: maker.Side.Other(), TradeID: 0,
	}
	switch maker.Leg {
	case model.LegA:
		req.Leg = model.LegB
		req.Symbol = e.config.LegB
		req.Lots = lots * e.config.Ratio
		req.Price = e.quotes.Prices(model.LegB).MarketPrice(req.Side)
	case model.LegB:
		if lots%e.config.Ratio != 0 {
			reason := fmt.Sprintf("stray leg B fill %d is not divisible by hedge ratio %d", lots, e.config.Ratio)
			e.halt(reason)
			return errors.New(reason)
		}
		req.Leg = model.LegA
		req.Symbol = e.config.LegA
		req.Lots = lots / e.config.Ratio
		req.Price = e.quotes.Prices(model.LegA).MarketPrice(req.Side)
	default:
		return errors.New("stray maker order has no pair leg")
	}
	e.logger.Criticalf("stray maker fill on %s x%d; placing mandatory hedge", maker.Symbol, lots)
	return e.placeHedge(ctx, req)
}

func (e *Engine) hedgeTrade(ctx context.Context, role model.OrderRole) error {
	for range 2 {
		req, ok, err := e.trade.Hedge(role)
		if err != nil {
			e.halt(err.Error())
			return err
		}
		if !ok {
			return nil
		}
		if err := e.placeHedge(ctx, req); err != nil {
			return err
		}
	}
	if !e.trade.IsPaired() {
		return errors.New("clip remained imbalanced after two hedge effects")
	}
	return nil
}

func (e *Engine) placeHedge(ctx context.Context, req model.OrderRequest) error {
	var lastErr error
	for range e.config.HedgeTries {
		if !e.limit.Take(1, model.LimitMust) {
			reason := "placement budget rejected a mandatory hedge"
			e.halt(reason)
			return errors.New(reason)
		}
		orderID, err := e.broker.Place(ctx, req)
		if err == nil {
			return e.addOrder(orderID, req)
		}
		lastErr = err
		if OrderMayExist(err) {
			e.state.NeedCheck("mandatory placement outcome is unknown")
			e.logger.Criticalf("mandatory hedge on %s x%d is unresolved: %v", req.Symbol, req.Lots, err)
			return err
		}
	}
	e.hedges.Add(req, lastErr)
	e.state.StartFix(e.clock.Now(), e.config.RetryWait, "mandatory hedge placement failed")
	e.logger.Warnf("queued hedge debt on %s x%d after %d definitive failures", req.Symbol, req.Lots, e.config.HedgeTries)
	return lastErr
}

func (e *Engine) closeOrder(ctx context.Context, orderID string) (orders.Change, error) {
	snap, ok := e.orders.Info(orderID)
	if !ok {
		return orders.Change{}, fmt.Errorf("cannot retire unknown order %q", orderID)
	}
	result, cancelErr := e.broker.Cancel(ctx, orderID)
	if cancelErr != nil {
		status, statusErr := e.broker.Status(ctx, orderID)
		if statusErr != nil || !status.Done {
			e.orders.NeedClose(orderID)
			e.state.StartFix(e.clock.Now(), e.config.RetryWait, "maker retirement unresolved")
			return orders.Change{}, errors.Join(cancelErr, statusErr)
		}
		result.Filled = status.Filled
	}
	change := e.orders.Close(orderID, result.Filled)
	e.trade.CloseOrder(orderID)
	e.sendChange(change, "maker retirement")
	if change.Conflict {
		e.logger.Criticalf("terminal result for %s contradicts already reported executions", orderID)
	}
	if snap.Request.Kind == model.OrderMarket && change.Lots != 0 {
		e.trade.FixMarket(snap.Request.TradeID, snap.Request.Leg, change.Lots)
	}
	return change, nil
}

func (e *Engine) stopTrade(ctx context.Context, reason string, allowBalance bool) error {
	ids := e.trade.Stop()
	var result error
	for _, orderID := range ids {
		change, err := e.closeOrder(ctx, orderID)
		if err != nil {
			result = errors.Join(result, err)
			continue
		}
		if change.Lots != 0 {
			result = errors.Join(result, e.useLimitFill(ctx, change, false))
		}
	}
	if !allowBalance {
		e.state.NeedCheck(reason + ": placement outcome unknown")
		e.trade.DropEmpty()
		return result
	}
	result = errors.Join(result, e.hedgeTrade(ctx, model.RoleFix))
	if e.trade.IsPaired() {
		e.finishTrade(e.clock.Now(), true)
	}
	e.trade.DropEmpty()
	return result
}

func (e *Engine) finishTrade(at time.Time, allowPartial bool) {
	plan, ok := e.trade.Finish(allowPartial)
	if !ok {
		return
	}
	e.strategy.Commit(plan, at)
	if saver, ok := e.strategy.(Saver); ok {
		saver.SaveLots()
	}
	e.logger.Infof("committed clip action=%d lots=%d position=%d", plan.Action, plan.Lots, e.strategy.Position())
}

func (e *Engine) useMarketStatus(ctx context.Context, orderID string, status model.OrderStatus) error {
	result := e.hedges.SetStatus(orderID, status)
	if !result.Known {
		return nil
	}
	if status.Done {
		change := e.orders.Close(orderID, status.Filled)
		e.sendChange(change, "taker terminal correction")
		if change.Lots != 0 {
			e.trade.FixMarket(
				change.Order.Request.TradeID,
				change.Order.Request.Leg,
				change.Lots,
			)
		}
	}
	if result.Missing.Lots > 0 {
		return e.placeHedge(ctx, result.Missing)
	}
	return nil
}

func (e *Engine) checkMarketOrders(ctx context.Context, now time.Time) error {
	checks := e.hedges.Checks(now, e.config.MarketCheckAfter, e.config.MarketCheckEvery)
	var result error
	for _, check := range checks {
		status, err := e.broker.Status(ctx, check.OrderID)
		if err != nil {
			e.state.StartFix(now, e.config.RetryWait, "taker confirmation unavailable")
			result = errors.Join(result, err)
			continue
		}
		result = errors.Join(result, e.useMarketStatus(ctx, check.OrderID, status))
	}
	return result
}

func (e *Engine) fixWork(ctx context.Context, now time.Time) error {
	didWork := false
	var result error
	for _, orderID := range e.orders.OrdersToClose() {
		change, err := e.closeOrder(ctx, orderID)
		if err != nil {
			result = errors.Join(result, err)
			continue
		}
		didWork = true
		if change.Lots != 0 {
			result = errors.Join(result, e.useLimitFill(ctx, change, true))
		}
	}
	for _, debt := range e.hedges.All() {
		if !e.limit.Take(1, model.LimitMust) {
			e.halt("placement budget rejected a recovery hedge")
			return errors.Join(result, errors.New("placement budget rejected a recovery hedge"))
		}
		orderID, err := e.broker.Place(ctx, debt.Request)
		if err != nil {
			if OrderMayExist(err) {
				e.hedges.Done(debt.ID)
				e.state.NeedCheck("recovery hedge placement outcome is unknown")
				e.logger.Criticalf("recovery hedge became ambiguous: %v", err)
			} else {
				e.hedges.Fail(debt.ID, err)
			}
			result = errors.Join(result, err)
			continue
		}
		if err := e.addOrder(orderID, debt.Request); err != nil {
			return errors.Join(result, err)
		}
		e.hedges.Done(debt.ID)
		didWork = true
	}
	remaining := len(e.orders.OrdersToClose()) + len(e.hedges.All()) + e.hedges.MarketCount()
	e.state.FixDone(
		now, remaining, didWork, e.config.RetryWait, e.config.RetryMax,
	)
	if e.trade.Full() {
		e.finishTrade(now, false)
	}
	return result
}

func (e *Engine) placeFailed(err error, operation string) {
	if OrderMayExist(err) {
		e.state.NeedCheck(operation + " outcome is unknown")
		e.logger.Criticalf("%s is ambiguous: %v", operation, err)
		return
	}
	e.logger.Warnf("%s was definitively rejected: %v", operation, err)
}

func (e *Engine) halt(reason string) {
	if e.state.Stop(reason) {
		e.logger.Criticalf("engine halted: %s", reason)
	}
}
