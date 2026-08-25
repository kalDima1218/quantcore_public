package execengine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/orders"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"QuantCore/trade/finam"
)

// NewFinamMaker builds a Maker backed by live Finam GOOD_TILL_CROSSING limit orders.
// Placements are idempotent against the lost-response race — see placer.
func NewFinamMaker(c *finam.Client) Maker { return finamMaker{p: newPlacer(c)} }

// NewFinamTaker builds a Taker backed by live Finam market orders. Placements are
// idempotent against the lost-response race — see placer.
func NewFinamTaker(c *finam.Client) Taker { return finamTaker{p: newPlacer(c)} }

// orderKind selects which of the four placement RPCs a placer call issues.
type orderKind int

const (
	kindLimitBid orderKind = iota
	kindLimitAsk
	kindMarketBuy
	kindMarketSell
)

const (
	// ghostProbes / ghostProbeGap pace the client-id resolution after an ambiguous
	// placement error: the order (if it was delivered) needs a moment to appear in the
	// account's order list, and each probe is one GetOrders RPC. Worst case this blocks
	// the event loop ~3×(1s+RPC) — only on transport errors, where the alternative used
	// to be an untracked ghost order.
	ghostProbes   = 3
	ghostProbeGap = time.Second
)

// placer wraps live order placement with client-order-id idempotency, closing the
// LOST-RESPONSE race: a place RPC that dies in transport (timeout, connection reset) may
// still have delivered its order to the broker, and the old code's "error ⇒ not placed"
// assumption left that GHOST resting untracked — its fills looked foreign and were
// ignored until reconcile flagged the divergence. Every order is therefore tagged with a
// unique client_order_id chosen BEFORE the RPC (the one handle that survives a lost
// response; the API echoes it back inside OrderState.Order). On an AMBIGUOUS transport
// error the placer resolves the truth by scanning the account's orders for the tag:
// found → the order exists and is returned as a normal success (the engine tracks it);
// authoritatively absent → the placement genuinely failed and the error propagates.
// Definitive business rejections (invalid args, permission, insufficient funds) skip the
// scan — the broker answered, nothing was placed.
type placer struct {
	c     *finam.Client
	nonce string        // per-placer random tag baked into every client id: uniqueness across processes and restarts
	seq   atomic.Uint64 // per-placer counter: uniqueness within the process; taker-only mints both legs' id concurrently off this counter

	// RPC seams (production defaults target the finam package; tests inject).
	place func(kind orderKind, symbol string, lots int, price float64, clientOrderID string) (*orders.OrderState, error)
	find  func(clientOrderID string) (orderID string, found bool, err error)
	sleep func(time.Duration)
	nowFn func() time.Time
}

func newPlacer(c *finam.Client) *placer {
	p := &placer{
		c:     c,
		nonce: randNonce(),
		sleep: time.Sleep,
		nowFn: time.Now,
	}
	p.place = func(kind orderKind, symbol string, lots int, price float64, cid string) (*orders.OrderState, error) {
		t := finam.Ticker{Symbol: symbol, Vol: lots}
		switch kind {
		case kindLimitBid:
			return finam.PlaceLimitOrderBuy(c, t, price, cid)
		case kindLimitAsk:
			return finam.PlaceLimitOrderSell(c, t, price, cid)
		case kindMarketBuy:
			return finam.PlaceMarketOrderBuy(c, t, cid)
		default:
			return finam.PlaceMarketOrderSell(c, t, cid)
		}
	}
	p.find = func(cid string) (string, bool, error) {
		st, found, err := finam.FindOrderByClientID(c, cid)
		if err != nil || !found {
			return "", found, err
		}
		return st.GetOrderId(), true, nil
	}
	return p
}

// client returns the underlying Finam client for the non-placement RPCs (cancel/status).
func (p *placer) client() *finam.Client { return p.c }

// randNonce returns the placer's random instance tag (6 hex chars): two strategies
// sharing an account, or the same bot restarted, can never mint colliding client ids.
func randNonce() string {
	var b [3]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failing is effectively fatal elsewhere; degrade to a time-based tag.
		return strconv.FormatInt(time.Now().UnixNano()%0xFFFFFF, 16)
	}
	return hex.EncodeToString(b[:])
}

// nextClientID mints one client_order_id: "q" + nonce(6) + unix-seconds(base36, 6) +
// seq(base36) — unique within the process (seq), across restarts (timestamp) and across
// processes (nonce), and comfortably inside the API's 20-character cap.
func (p *placer) nextClientID() string {
	seq := p.seq.Add(1)
	return "q" + p.nonce + strconv.FormatInt(p.nowFn().Unix(), 36) + strconv.FormatUint(seq, 36)
}

// maybeDelivered reports whether a placement error leaves the order's fate UNKNOWN: the
// request may have reached the broker even though the response never made it back.
// Definitive business rejections mean the broker answered and nothing rests.
func maybeDelivered(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled,
		codes.Unknown, codes.Internal, codes.DataLoss, codes.Aborted:
		return true
	}
	return false
}

// placeResolved issues one placement and, on an ambiguous transport error, resolves the
// order's true fate by client id before reporting anything to the engine: the engine
// must only ever see "placed, here is its id" or "confirmed not placed" — never a guess.
func (p *placer) placeResolved(kind orderKind, symbol string, lots int, price float64) (string, error) {
	cid := p.nextClientID()
	st, err := p.place(kind, symbol, lots, price, cid)
	if err == nil {
		return st.GetOrderId(), nil
	}
	if !maybeDelivered(err) {
		return "", err // the broker answered: rejected, nothing rests
	}
	mlog.Printf("[execengine] place %s x%d failed in transport (%v) — the order MAY have reached the broker; resolving by client id %s", symbol, lots, err, cid)
	sawAbsent := false
	for probe := 0; probe < ghostProbes; probe++ {
		p.sleep(ghostProbeGap) // give a delivered order a moment to appear in the account list
		id, found, ferr := p.find(cid)
		if ferr != nil {
			continue // the account list is unreachable too — keep probing
		}
		if found {
			mlog.Printf("[execengine] client id %s resolved: order %s EXISTS at the broker — adopting it as a normal placement", cid, id)
			return id, nil
		}
		sawAbsent = true
	}
	if sawAbsent {
		// The account's order list answered and does not carry the tag: not placed. (A
		// same-second fill-and-vanish could in principle evade a list that omits done
		// orders — reconcile remains the net for that sliver.)
		mlog.Printf("[execengine] client id %s resolved: absent from the account's orders — treating the placement as failed", cid)
		return "", err
	}
	// Neither the placement nor the resolution could reach the broker: report failure —
	// the engine's own machinery (backoff / hedge debt / reconcile) owns the wait. If a
	// ghost did land, reconcile will surface it.
	mlog.Printf("[execengine] CRITICAL: client id %s UNRESOLVED (order list unreachable) — reporting the placement as failed; reconcile is the net if a ghost landed", cid)
	return "", fmt.Errorf("placement unresolved (transport error and order list unreachable): %w", err)
}

// finamMaker places post-only (GOOD_TILL_CROSSING) limit orders through the live Finam
// API, satisfying Maker.
type finamMaker struct{ p *placer }

func (m finamMaker) PlaceBid(symbol string, lots int, price float64) (string, error) {
	return m.p.placeResolved(kindLimitBid, symbol, lots, price)
}

func (m finamMaker) PlaceAsk(symbol string, lots int, price float64) (string, error) {
	return m.p.placeResolved(kindLimitAsk, symbol, lots, price)
}

func (m finamMaker) Cancel(orderID string) (int, error) {
	st, err := finam.CancelOrder(m.p.client(), orderID)
	if err != nil {
		return 0, err
	}
	return finam.ExecutedLots(st), nil
}

// terminalOrderStatus reports whether a Finam order status admits no further fills.
// PENDING_CANCEL/NEW/PARTIALLY_FILLED/FORWARDING/WAIT/WATCHING etc. stay non-terminal;
// REPLACED is treated as live too (we never replace orders, so err on the safe side).
func terminalOrderStatus(s orders.OrderStatus) bool {
	switch s {
	case orders.OrderStatus_ORDER_STATUS_FILLED,
		orders.OrderStatus_ORDER_STATUS_DONE_FOR_DAY,
		orders.OrderStatus_ORDER_STATUS_CANCELED,
		orders.OrderStatus_ORDER_STATUS_REJECTED,
		orders.OrderStatus_ORDER_STATUS_EXPIRED,
		orders.OrderStatus_ORDER_STATUS_FAILED,
		orders.OrderStatus_ORDER_STATUS_DENIED_BY_BROKER,
		orders.OrderStatus_ORDER_STATUS_REJECTED_BY_EXCHANGE,
		orders.OrderStatus_ORDER_STATUS_EXECUTED,
		orders.OrderStatus_ORDER_STATUS_DISABLED:
		return true
	}
	return false
}

func (m finamMaker) Status(orderID string) (int, bool, error) {
	st, err := finam.GetOrder(m.p.client(), orderID)
	if err != nil {
		return 0, false, err
	}
	return finam.ExecutedLots(st), terminalOrderStatus(st.GetStatus()), nil
}

// finamTaker crosses the spread with market orders, satisfying Taker.
type finamTaker struct{ p *placer }

func (t finamTaker) Buy(symbol string, lots int) (string, error) {
	return t.p.placeResolved(kindMarketBuy, symbol, lots, 0)
}

func (t finamTaker) Sell(symbol string, lots int) (string, error) {
	return t.p.placeResolved(kindMarketSell, symbol, lots, 0)
}

// IsDeadStatus reports whether an order status is terminal WITHOUT a fill — the exchange
// rejected, cancelled or expired the order, so a resting clip order at this status is
// gone. FILLED is excluded (handled by the fill stream). It is the live runners' shared
// filter for feeding Engine.OnOrderStatus.
func IsDeadStatus(s orders.OrderStatus) bool {
	switch s {
	case orders.OrderStatus_ORDER_STATUS_CANCELED,
		orders.OrderStatus_ORDER_STATUS_REJECTED,
		orders.OrderStatus_ORDER_STATUS_REJECTED_BY_EXCHANGE,
		orders.OrderStatus_ORDER_STATUS_DENIED_BY_BROKER,
		orders.OrderStatus_ORDER_STATUS_EXPIRED,
		orders.OrderStatus_ORDER_STATUS_FAILED:
		return true
	}
	return false
}

// ReconcileFromBroker fetches both legs' current broker positions and hands them to
// e.Reconcile — the shared body of the runners' periodic reconcile pass. On a fetch
// failure nothing is reconciled and the returned error names the failing leg; the
// caller logs it with its own prefix and simply skips this pass.
func ReconcileFromBroker(e *Engine, client *finam.Client, legA, legB string) error {
	a, _, err := finam.GetPosition(client, legA)
	if err != nil {
		return fmt.Errorf("%s position fetch failed: %w", legA, err)
	}
	b, _, err := finam.GetPosition(client, legB)
	if err != nil {
		return fmt.Errorf("%s position fetch failed: %w", legB, err)
	}
	e.Reconcile(int(math.Round(a.Quantity)), int(math.Round(b.Quantity)))
	return nil
}

// TradeDedup remembers account-trade ids so a fill re-delivered by a stream reconnect
// is folded only once. Process-lifetime, like Engine.own: dropping an id early would
// risk double-counting a late re-delivery.
type TradeDedup map[string]struct{}

// Seen reports whether tid was already recorded, recording it otherwise. An empty id
// (no dedup key) is never treated as a duplicate.
func (d TradeDedup) Seen(tid string) bool {
	if tid == "" {
		return false
	}
	if _, dup := d[tid]; dup {
		return true
	}
	d[tid] = struct{}{}
	return false
}

const (
	placeOrderQuota = "OrdersService.placeOrder" // usage-metrics name of the order-placement quota
	quotaRefresh    = 5 * time.Second            // how often to re-poll the remaining budget
	quotaRPCTimeout = 2 * time.Second            // bound on each usage-metrics RPC
)

// RefreshQuota polls Finam's placeOrder usage quota and feeds it to lim until ctx ends,
// so the engine can gate order bursts on the real remaining budget instead of guessing.
// It polls once up front, before the first tick, so the first quotes are already gated.
// Run it on its own goroutine; both live runners share it.
func RefreshQuota(ctx context.Context, client *finam.Client, lim *QuotaLimiter) {
	poll := func() {
		cctx, cancel := context.WithTimeout(ctx, quotaRPCTimeout)
		defer cancel()
		quotas, err := finam.GetUsageMetrics(cctx, client)
		if err != nil {
			return
		}
		for _, q := range quotas {
			if q.GetName() == placeOrderQuota {
				lim.Set(int(q.GetRemaining()), q.GetResetTime().AsTime())
				return
			}
		}
	}
	poll()
	t := time.NewTicker(quotaRefresh)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			poll()
		}
	}
}
