// Passive quoting: keeping a working clip's resting orders on the current touch, which
// lives in the quotebook package now (see quotebook.Book). Nothing here decides WHETHER to
// be in the market — only where a decided clip's passives must sit.

package execengine

import (
	"time"

	"QuantCore/strategies/execengine/quotebook"
)

// OnBook folds a fresh best-bid/ask for one leg into the engine. It updates the leg's
// touch and, if a clip's passive on that leg has fallen behind the new touch, re-pegs it.
// Strategy decisions themselves are driven by OnState (the signal's per-bar cadence), not
// by every book tick.
func (e *Engine) OnBook(symbol string, ts time.Time, bestBid, bestAsk float64) {
	e.advanceNow(ts)
	if !e.quote.Update(symbol, ts, bestBid, bestAsk) {
		return
	}
	e.serviceImpaired()
	e.maybeRepeg(symbol, ts)
}

// repegThrottle is the minimum gap between successive re-pegs of the SAME leg — the guard
// that stops a fast-moving leg (one touch can change far more often than the other) from
// re-posting on every tick and exhausting the order-placement quota.
func (e *Engine) repegThrottle() time.Duration {
	if e.cfg.RepegThrottle > 0 {
		return e.cfg.RepegThrottle
	}
	return 500 * time.Millisecond
}

// maybeRepeg re-pegs the working clip's passive on symbol if a better price has appeared
// on its side (someone out-quoted us, so our order drifted off the touch). It runs only
// before the first fill — once a leg fills we commit to it and taker-hedge the rest, so
// there is nothing to keep at the touch. Each re-peg is throttled per leg and gated on the
// rate limiter, so re-pegging never starves the mandatory hedge/cancel budget.
func (e *Engine) maybeRepeg(symbol string, ts time.Time) {
	c := e.clip
	if e.recovery.Halted() || c == nil || c.makerID != "" || e.cfg.DisableRepeg {
		return
	}
	// Which legs actually rest is a property of THIS clip (an Intent may override the
	// engine-wide mode — see Intent.ExecMode), so the gate reads the clip's resolved shape.
	switch symbol {
	case e.cfg.LegA:
		if c.mode == ExecSoloMakerLegB {
			return // LegA is never rested when the perp is the sole passive
		}
		e.repegLeg(&c.legA, e.cfg.LegA, e.quote.TouchA(), ts)
	case e.cfg.LegB:
		if c.mode == ExecSoloMaker {
			return // LegB is never rested in single-passive mode — nothing to re-peg
		}
		e.repegLeg(&c.legB, e.cfg.LegB, e.quote.TouchB(), ts)
	}
}

// repegLeg moves lo to the current touch when it has been out-quoted: a resting bid is
// behind once a higher bid appears, a resting ask once a lower ask appears. On the (rare)
// re-place failure the whole clip is abandoned — the old order is already cancelled, so
// the pair can no longer be maintained — leaving the engine flat and idle after a backoff.
func (e *Engine) repegLeg(lo *legOrder, sym string, t quotebook.Touch, ts time.Time) {
	if !t.Valid() {
		return
	}
	target := t.Ask
	behind := t.Ask < lo.price // resting ask out-quoted by a lower ask
	if lo.isBid {
		target = t.Bid
		behind = t.Bid > lo.price // resting bid out-quoted by a higher bid
	}
	if !behind {
		return
	}
	// MinRest: the current resting order is guaranteed its hang time before being moved —
	// see EngineConfig.MinRest. placedAt refreshes on every re-place, so the guarantee is
	// per-order, not per-clip.
	if e.cfg.MinRest > 0 && ts.Sub(lo.placedAt) < e.cfg.MinRest {
		return
	}
	if !lo.lastRepeg.IsZero() && ts.Sub(lo.lastRepeg) < e.repegThrottle() {
		return
	}
	// One placeOrder op; the limiter keeps its margin in reserve for hedges/cancels, so a
	// tight quota simply skips the re-peg rather than risking a stuck leg later. Allow is
	// checked on the processing clock, not ts (event clock) — same clock domain as Spend
	// and tryOpenClip's Allow (see clock.go); retryAt is discarded here so there is no
	// cross-domain backoffUntil write to translate.
	if ok, _ := e.limiter.Allow(e.clock.Now(), 1); !ok {
		return
	}
	if gap := e.retireOrder(lo.id); gap > 0 {
		// The out-quoted passive actually filled while we pulled it: this leg is the
		// de-facto maker. Fold the fill and finish the clip the way a partial resolve
		// does (once a leg begins filling, the clip completes) — re-posting the full
		// size on top of it would double the position.
		c := e.clip
		c.makerID = lo.id
		c.makerFilled = gap
		realA, realB := gap, 0
		if lo == &c.legB {
			realA, realB = 0, gap
		}
		e.settleClip(realA, realB, true)
		if !e.recovery.Halted() {
			e.commitClip(ts)
		}
		return
	}
	if e.recovery.Halted() || e.recovery.Impaired() || e.clip == nil {
		// The retire could not complete (halt, or a deferred cancel put the engine into
		// impaired mode): the old order may still rest, so nothing may be re-placed on top
		// of it. Impaired teardown pulls the clip at the next handler entry.
		return
	}
	lo.price = target
	if err := e.placeLeg(lo, sym, e.clip.target); err != nil {
		e.logf("re-peg place %s failed: %v — abandoning clip", sym, err)
		e.CancelClip() // settles the other leg; the re-pegged leg is already retired
		e.quote.SetBackoff(ts.Add(e.placeBackoff()))
		return
	}
	lo.lastRepeg = ts
}

// checkStaleBooks pulls the working clip when either leg's book has gone stale — market
// data this old means the connection is in trouble, and resting orders priced off a dead
// feed are adverse-selection bait. The orders are simply pulled (the same teardown a lost
// signal runs); new clips are already blocked by canQuote until fresh books arrive, so
// trading resumes by itself with the data. Gated on PullOnStaleBook (live runners only:
// a backtest feed's quiet gaps are not outages) and on MaxStaleness > 0.
func (e *Engine) checkStaleBooks(now time.Time) {
	if !e.cfg.PullOnStaleBook || e.cfg.MaxStaleness <= 0 || e.clip == nil || e.recovery.Halted() {
		return
	}
	staleA, staleB := e.quote.Stale(now, e.cfg.MaxStaleness)
	if !staleA && !staleB {
		return
	}
	e.logf("book stale (legA=%v legB=%v beyond %s) — pulling resting orders until market data resumes", staleA, staleB, e.cfg.MaxStaleness)
	e.resolveClip(now)
}
