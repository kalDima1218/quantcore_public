// Package quotebook is the engine's continuously-updated view of both legs' books: the
// latest touch (bid/ask/ts) per leg, independent of whether a clip is open, and the
// rate-limit/failure backoff that suppresses opening a NEW clip.
//
// This is the engine's full quoting-eligibility surface (Update/CanQuote/CrossPrice/Stale),
// enforced as a real package boundary: mutation only ever happens through Update/SetBackoff.
// Re-peg decisions need a working clip's OWN state too (which leg is resting, at what
// price), so that logic stays on the engine and reads touches back out via TouchA/TouchB —
// see strategies/execengine/CLAUDE.md's Engine-decomposition note for why some transitions
// cannot honestly live in a single component.
package quotebook

import "time"

// Touch is a leg's most recent best bid/ask, or the zero value before any book update.
type Touch struct {
	Bid, Ask float64
	TS       time.Time
	OK       bool
}

// Valid reports whether the touch is a sane, non-crossed, positive-priced quote.
func (t Touch) Valid() bool { return t.OK && t.Bid > 0 && t.Ask > 0 && t.Ask >= t.Bid }

// SidePrice returns the price an order on the given side rests at: the bid for a resting
// bid, the ask for a resting ask.
func (t Touch) SidePrice(isBid bool) float64 {
	if isBid {
		return t.Bid
	}
	return t.Ask
}

// Book tracks two symbols' touches and the open-new-clip backoff.
type Book struct {
	legA, legB     string // the two tracked symbols, fixed at construction
	touchA, touchB Touch
	backoffUntil   time.Time
}

// New builds a Book tracking legA/legB (the engine's two configured symbols).
func New(legA, legB string) *Book {
	return &Book{legA: legA, legB: legB}
}

// Update folds a fresh best-bid/ask for symbol into the book. Reports whether the update
// was actually applied: an update strictly older than the stored touch is an out-of-order
// or replayed snapshot (stream reconnects re-deliver) and is dropped — folding it would
// regress the touch to stale prices and re-peg/quote against a book that no longer exists.
// Newest data wins; equal timestamps still apply (intra-timestamp updates arrive in order).
// An unrecognized symbol is also a no-op.
func (b *Book) Update(symbol string, ts time.Time, bestBid, bestAsk float64) bool {
	switch symbol {
	case b.legA:
		if b.touchA.OK && ts.Before(b.touchA.TS) {
			return false
		}
		b.touchA = Touch{Bid: bestBid, Ask: bestAsk, TS: ts, OK: true}
		return true
	case b.legB:
		if b.touchB.OK && ts.Before(b.touchB.TS) {
			return false
		}
		b.touchB = Touch{Bid: bestBid, Ask: bestAsk, TS: ts, OK: true}
		return true
	default:
		return false
	}
}

// TouchA reports legA's current touch.
func (b *Book) TouchA() Touch { return b.touchA }

// TouchB reports legB's current touch.
func (b *Book) TouchB() Touch { return b.touchB }

// CanQuote reports whether both legs carry a valid, fresh-enough touch to price a new
// clip. maxStaleness<=0 disables the freshness check (a backtest feed's quiet gaps are not
// outages — see EngineConfig.MaxStaleness).
func (b *Book) CanQuote(ts time.Time, maxStaleness time.Duration) bool {
	if !b.touchA.Valid() || !b.touchB.Valid() {
		return false
	}
	if maxStaleness > 0 && (ts.Sub(b.touchA.TS) > maxStaleness || ts.Sub(b.touchB.TS) > maxStaleness) {
		return false
	}
	return true
}

// Stale reports which legs' books have gone stale (no touch yet, or older than
// maxStaleness) — the caller pulls a working clip when either has, since resting orders
// priced off a dead feed are adverse-selection bait.
func (b *Book) Stale(now time.Time, maxStaleness time.Duration) (staleA, staleB bool) {
	staleA = !b.touchA.OK || now.Sub(b.touchA.TS) > maxStaleness
	staleB = !b.touchB.OK || now.Sub(b.touchB.TS) > maxStaleness
	return staleA, staleB
}

// CrossPrice is the touch a taker on symbol crosses at (buy → the ask, sell → the bid) —
// the credit-time price estimate for a placed taker's sink credit. 0 when the leg has no
// touch yet.
func (b *Book) CrossPrice(symbol string, buy bool) float64 {
	t := b.touchA
	if symbol == b.legB {
		t = b.touchB
	}
	return t.SidePrice(!buy) // a buy crosses the resting ask, a sell the resting bid
}

// BackoffUntil reports when a new clip may next be opened (rate-limit / failure backoff).
func (b *Book) BackoffUntil() time.Time { return b.backoffUntil }

// SetBackoff suppresses opening a new clip until until.
func (b *Book) SetBackoff(until time.Time) { b.backoffUntil = until }
