package execengine

import "time"

// Maker places post-only (maker) limit orders and cancels them by id. The live
// implementation is backed by Finam GOOD_TILL_CROSSING limit orders.
type Maker interface {
	PlaceBid(symbol string, lots int, price float64) (orderID string, err error)
	PlaceAsk(symbol string, lots int, price float64) (orderID string, err error)
	// Cancel pulls a resting order and reports how many lots it had already filled
	// before the cancel took effect (0 in the common case).
	Cancel(orderID string) (executed int, err error)
	// Status reads an order's current executed count and whether it is in a terminal
	// state (filled/cancelled/expired/rejected — no further fills possible). It is the
	// fallback truth source when Cancel errors: cancelling an already-filled order IS
	// an error at the broker, and assuming executed=0 there loses real inventory.
	Status(orderID string) (executed int, terminal bool, err error)
}

// Taker crosses the spread with a market order to hedge a just-filled maker leg. It
// returns the placed order's id (like Maker) so the engine can record its own order ids
// and the live runner can tell an own fill from a foreign one on a shared account.
type Taker interface {
	Buy(symbol string, lots int) (orderID string, err error)
	Sell(symbol string, lots int) (orderID string, err error)
}

// Limiter gates discretionary order-op bursts so the strategy stays inside the broker's
// request quota. Allow reports whether `ops` order placements may be issued at time now;
// when it declines it returns the time to retry (the quota window's reset). Spend books
// every placement RPC the engine ACTUALLY issues — gated or not (taker hedges, top-ups,
// failed attempts) — so the limiter's budget view tracks the engine's own consumption
// instead of waiting on the next broker refresh.
type Limiter interface {
	Allow(now time.Time, ops int) (ok bool, retryAt time.Time)
	// Spend books ops placement RPCs actually issued at now — including UNGATED spends
	// (a taker hedge never checked by Allow first) — so an implementation with a
	// self-managed window can roll it over before debiting rather than after.
	Spend(now time.Time, ops int)
}

// FillSink receives the engine's authoritative own-execution accounting, exactly once per
// lot (live only — see SetFillSink). It exists because the fill STREAM is not the only way
// the engine learns its orders executed: a retired order's cancel-ack carries a terminal
// executed count, and a placed taker is treated as filled by the engine's own model. A
// position derived from stream fills alone therefore LAGS the engine's real inventory
// whenever the stream stalls — the 2026-07-16 over-cap incident: clips kept opening against
// a stale position while their fills sat undelivered, until the stream caught up 8 lots past
// the cap. The sink is fed at the moment the engine ACTS, so the position the Decider caps
// against moves in lock-step with the engine:
//
//   - Fill: lots newly learned — a stream fill event, a cancel-ack gap (priced at the
//     order's resting limit, which is exact for a passive), or a just-placed taker (priced
//     at the touch it crosses — an estimate).
//   - Amend: a fill event caught up with lots that were already credited ahead of it; only
//     the cash difference between the credit-time price and the actual fill price is
//     applied, never inventory.
type FillSink interface {
	Fill(symbol string, buy bool, lots int, price float64)
	Amend(symbol string, buy bool, lots int, from, to float64)
}

// Intent is the read-only outcome of a decision: the action the strategy would take on
// the current bar, whether it reduces an existing position (a close) or opens a new lot,
// and the price a new lot would be booked at. It is the seam the dual-maker engine needs
// — Peek computes it without mutating the caller's book, the engine works the passive
// orders, and Commit applies the same intent only once a fill actually lands. A Hold
// carries Action == 0 and is a no-op for Commit.
// ExecMode names the shape ONE clip is executed in, overriding the engine-wide
// SoloMakerLeg/TakerOnly configuration for that clip alone. It exists because a strategy can
// have a phase whose execution differs from its normal trading: basis's force-flatten posts
// passively while the day still has time and crosses only near the close (force_flatten_exec /
// force_flatten_taker_min). The zero value defers to the engine's own config, so an Intent that
// never sets it behaves exactly as before.
type ExecMode uint8

const (
	ExecDefault       ExecMode = iota // use the engine's configured mode (SoloMakerLeg/TakerOnly)
	ExecTaker                         // both legs cross the book as market orders
	ExecSoloMaker                     // LegA rests as the sole passive; LegB is taker-hedged on its fill
	ExecDualPassive                   // both legs rest passively
	ExecSoloMakerLegB                 // LegB rests as the sole passive; LegA is taker-hedged on its fill
)

// String renders the mode for logs; the names match basis's force_flatten_exec values.
func (m ExecMode) String() string {
	switch m {
	case ExecTaker:
		return "taker"
	case ExecSoloMaker:
		return "solo_maker"
	case ExecDualPassive:
		return "maker"
	case ExecSoloMakerLegB:
		return "solo_maker_perp"
	default:
		return "default"
	}
}

type Intent struct {
	Action    int     // strategy-defined direction (e.g. buy/sell/hold); 0 means hold
	IsClose   bool    // true → the intent reduces the position; false → opens a lot
	OpenPrice float64 // price to book an opening lot at (unused for closes/holds)
	Lots      int     // contracts this clip should work; 0 → the engine uses EngineConfig.OrderVol.
	//                    A strategy that sizes each clip (e.g. basis, capping at the remaining room
	//                    or the lots left to unwind) sets it; a fixed-lot strategy (spread) leaves it 0.
	ExecMode ExecMode // execution shape for THIS clip; ExecDefault (0) = whatever the engine is configured for
}

// actionHold mirrors the strategy-side "no action" sentinel (e.g. spread.ActionHold):
// execengine only needs to recognize a no-op intent, not the strategy's action polarity.
const actionHold = 0

// RowState is one per-bar input the engine folds in. The engine itself reads only Time
// (for freshness/backoff/lot timestamps); the strategy's signal for the bar rides in the
// opaque Signal payload, which the concrete Decider type-asserts back to its own state
// type in Peek. This keeps the engine strategy-agnostic: the spread strategy stuffs its
// six spread outputs in Signal, the basis strategy stuffs its z-score/ready, and neither
// leaks into the engine.
type RowState struct {
	Time   time.Time
	Signal any
}

// Lot is one open position lot booked by a Decider.
type Lot struct {
	Price float64
	Size  int
	Time  time.Time
}

// Decision is the applied outcome of a committed Intent.
type Decision struct {
	Decision       int
	ClosedPosition *Lot
}

// Decider is the strategy-owned signal the engine drives: Peek computes the intent for
// one bar without mutating the lot book (so the engine can decide whether to work a clip
// before any fill exists), and Commit applies a previously peeked intent once a clip
// actually fills. Position lets the engine report the position without knowing anything
// about the strategy's own bookkeeping. Persistence is optional — see Persister.
type Decider interface {
	// Peek computes the intent for one bar without mutating the lot book.
	Peek(s RowState) Intent
	// Commit applies a previously peeked intent (records the opened lot or drops the
	// closed one). The engine calls it only when a clip has actually filled.
	Commit(in Intent, t time.Time) Decision
	// Position returns the signed position in contracts (+ = long leg A / short leg B).
	Position() int
}

// Persister is the OPTIONAL persistence half of a Decider. When a Decider also implements
// it, the engine calls SaveLots after each commit so the strategy's lot book survives a
// restart (the live spread trader). A Decider without persistence (backtests, or the basis
// strategy, which holds no lot book) simply omits it and the engine skips the call.
type Persister interface {
	// SaveLots persists the lot book.
	SaveLots()
}

// touch is the last-known best bid/ask for one leg, with the time it was observed.
type touch struct {
	bid, ask float64
	ts       time.Time
	ok       bool
}

// valid reports whether the touch is a sane, non-crossed, positive-priced quote.
func (t touch) valid() bool { return t.ok && t.bid > 0 && t.ask > 0 && t.ask >= t.bid }

// sidePrice returns the price an order on the given side rests at: the bid for a
// resting bid, the ask for a resting ask.
func (t touch) sidePrice(isBid bool) float64 {
	if isBid {
		return t.bid
	}
	return t.ask
}

// legOrder is one leg's resting passive order: its id, the price it rests at and which
// side it is (a bid we buy on / an ask we sell on), plus when it was last re-pegged (so a
// fast-moving leg's re-pegs can be throttled) and when its CURRENT order was placed (each
// re-peg replaces the order and resets it) — the clock the MinRest guarantee runs on.
type legOrder struct {
	id        string
	price     float64
	isBid     bool // true → a resting bid (we buy); false → a resting ask (we sell)
	lastRepeg time.Time
	placedAt  time.Time
}

// clip is the pair of resting passive orders the engine is currently working. It targets
// `target` contracts on each leg in direction `dir`; fills arrive incrementally, so it
// tracks which leg became the maker and how many contracts of it have filled. The intent
// is the Decider move this clip realizes; it is committed to the lot book only once the
// clip actually completes.
type clip struct {
	dir         int      // direction this clip moves the position by (+1 buy, −1 sell)
	intent      Intent   // the Decider intent to commit on completion
	target      int      // contracts this clip aims to fill on each leg
	legA        legOrder // resting passive order on leg A
	legB        legOrder // resting passive order on leg B
	makerID     string   // the leg that filled first (its passive order id); "" until first fill
	makerFilled int      // contracts of the maker leg filled so far
	mode        ExecMode // resolved execution shape this clip was opened in — never ExecDefault (see clipExecMode);
	//                      the re-peg gate reads it rather than the engine config, because which legs actually rest
	//                      is a property of THIS clip once an Intent may override the engine-wide mode
	deadline time.Time
}
