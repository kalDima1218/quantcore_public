// Taker hedging: crossing the spread to make the pair delta-neutral after a passive
// fills, plus the placement-confirmation machinery a fire-and-forget taker needs (the
// broker may ack, stay silent, or contradict its own terminal count). Owns pending,
// debts, nextRetryAt/retryGap and takerDeadStreak.

package execengine

import (
	"time"
)

// takerConfirmAfter is how long a placed taker may go without confirming fill events
// before the engine treats its hedge as unconfirmed (new clips wait; status polled).
func (e *Engine) takerConfirmAfter() time.Duration {
	if e.cfg.TakerConfirmTimeout > 0 {
		return e.cfg.TakerConfirmTimeout
	}
	return 10 * time.Second
}

// takerConfirmed reports whether a taker account's placement credit has been confirmed by
// data: the fill stream covered its placed size, or a terminal status settled it.
func takerConfirmed(acct *ordAcct) bool { return acct.final >= 0 || acct.seen >= acct.placed }

// checkPendingTakers is the confirmation watchdog for placed takers. The engine's model
// books a placed taker as done the moment it is accepted — necessary so the cap position
// never trails the broker — but done-ness stays an ASSUMPTION until the broker's own data
// confirms it. Fill events covering the placed size confirm silently (the common case,
// within milliseconds). A taker still unconfirmed past the confirm window is actively
// verified: its status is polled (throttled per order) until the broker answers. A
// non-terminal answer means the order can still fill — keep waiting, never guess. A
// terminal answer settles the account via settleTaker: fully executed → confirmed; short →
// the phantom credit is reversed and the shortfall re-hedged. While any taker is overdue
// the engine opens no new clips (see OnState) — waiting on data, not luck.
func (e *Engine) checkPendingTakers(now time.Time) {
	for id := range e.pending {
		acct := e.own[id]
		if acct == nil || acct.final >= 0 {
			delete(e.pending, id)
			continue
		}
		if acct.seen >= acct.placed {
			// The fill stream confirmed every placed lot — the terminal count is known by
			// arithmetic (an order can never execute beyond its placed size).
			acct.final = acct.placed
			e.recovery.takerDeadStreak = 0
			delete(e.pending, id)
			continue
		}
		if !acct.deadSeen && now.Sub(acct.placedAt) < e.takerConfirmAfter() {
			continue // inside the confirm window — give the fill stream its normal latency
		}
		if !acct.lastProbe.IsZero() && now.Sub(acct.lastProbe) < takerProbeEvery {
			continue
		}
		acct.lastProbe = now
		if !acct.probeLogged {
			acct.probeLogged = true
			e.logf("taker %s (%s buy=%v) unconfirmed: %d/%d lots seen after %s — polling status; new clips wait for confirmation",
				id, acct.sym, acct.isBuy, acct.seen, acct.placed, now.Sub(acct.placedAt))
		}
		executed, terminal, err := e.maker.Status(id)
		if err != nil {
			continue // no data — keep waiting and keep new clips suspended; the next probe retries
		}
		if !terminal {
			continue // still live at the broker: its fills can still arrive — wait, don't guess
		}
		e.settleTaker(id, acct, executed)
		if e.recovery.halted {
			return
		}
	}
}

// awaitingTaker reports whether any placed taker is past its confirm window with neither
// fill confirmation nor a terminal status — the state in which the position is not backed
// by data and no new clips may be opened.
func (e *Engine) awaitingTaker(now time.Time) bool {
	for id := range e.pending {
		acct := e.own[id]
		if acct == nil || takerConfirmed(acct) {
			continue
		}
		if acct.deadSeen || now.Sub(acct.placedAt) >= e.takerConfirmAfter() {
			return true
		}
	}
	return false
}

// confirmDeadTaker handles a terminal-dead order-stream status for an own taker: the
// executed count is read from Status (the stream event says only "dead", and the count
// must come from data, not assumption). If the count cannot be read or the broker does not
// yet agree the order is terminal, the taker is flagged so the watchdog treats it as
// overdue immediately — new clips wait while it keeps probing.
func (e *Engine) confirmDeadTaker(orderID string, acct *ordAcct) {
	executed, terminal, err := e.maker.Status(orderID)
	if err != nil || !terminal {
		acct.deadSeen = true
		e.logf("own taker %s reported dead but its executed count is unconfirmed (err=%v terminal=%v) — suspending new clips and polling until the broker answers", orderID, err, terminal)
		return
	}
	e.settleTaker(orderID, acct, executed)
}

// settleTaker folds a taker's broker-confirmed terminal executed count into its account.
// Fully executed → the placement credit was right all along. Short → the engine assumed a
// hedge that never (fully) happened: the phantom lots are un-credited from the sink at
// their credit price (exact reversal — they were never amended, having had no fill events)
// and the shortfall is re-hedged with a fresh taker so the book returns to 1:1. A streak
// of takerDeadLimit consecutive dead-short takers halts instead: the broker is accepting
// and then killing our hedges, and re-hedging forever is churn, not progress.
func (e *Engine) settleTaker(orderID string, acct *ordAcct, executed int) {
	if acct.placed > 0 && executed > acct.placed {
		e.logf("CRITICAL: taker %s terminal executed count %d exceeds its placed size %d — clamping (corrupt ack?)", orderID, executed, acct.placed)
		executed = acct.placed
	}
	if executed < acct.seen {
		// The broker's terminal count contradicts its own fill stream downward. The stream's
		// lots are real (they were credited and are part of the hedged book); keep them.
		e.logf("CRITICAL: taker %s terminal executed count %d is BELOW the %d lots its fill stream reported — keeping the stream's count (broker self-contradiction; reconcile will confirm)", orderID, executed, acct.seen)
		executed = acct.seen
	}
	acct.final = executed
	delete(e.pending, orderID)
	shortfall := acct.placed - executed
	if shortfall <= 0 {
		e.recovery.takerDeadStreak = 0
		return // fully executed — the placement-time credit is confirmed
	}
	// Reverse the placement credit for the lots that never executed. They carry the
	// credit-time price untouched (only stream-reported lots are ever amended), so the
	// opposite-side fill at the same price cancels both inventory and cash exactly.
	if uncredit := acct.credited - executed; uncredit > 0 && e.sink != nil {
		e.sink.Fill(acct.sym, !acct.isBuy, uncredit, acct.price)
		acct.credited = executed
	}
	e.recovery.takerDeadStreak++
	if e.recovery.takerDeadStreak >= takerDeadLimit {
		// The broker keeps accepting and then killing our hedges. Re-hedging at full speed
		// would churn the request quota; giving up (the old kill-switch) would strand the
		// book naked until an operator arrived. Autonomous middle: the shortfall becomes a
		// DEBT paid at the impaired backoff pace — the engine pulls its orders and keeps
		// re-trying the hedge, however long the broker misbehaves.
		e.logf("CRITICAL: taker %s died with %d/%d lots executed on %s — %d consecutive takers confirmed dead short; slowing re-hedges to the impaired debt pace", orderID, executed, acct.placed, acct.sym, e.recovery.takerDeadStreak)
		e.deferHedge(acct.sym, acct.isBuy, shortfall)
		return
	}
	e.logf("CRITICAL: taker %s died with %d/%d lots executed on %s — credit reversed; re-hedging the %d-lot shortfall", orderID, executed, acct.placed, acct.sym, shortfall)
	e.takerRetry(acct.sym, acct.isBuy, shortfall)
}

// hedge crosses the spread on symbol for lots to complete a clip increment.
func (e *Engine) hedge(symbol string, buy bool, lots int) {
	e.takerRetry(symbol, buy, lots)
}

// hedgeRatio is how many LegB contracts hedge ONE LegA contract (see
// EngineConfig.HedgeRatio). Unset, 1, or anything below is the symmetric 1:1 pairing.
func (e *Engine) hedgeRatio() int {
	if e.cfg.HedgeRatio > 1 {
		return e.cfg.HedgeRatio
	}
	return 1
}

// hedgeLots converts a count the engine holds in LegA contracts — the unit EVERYTHING
// internal is counted in: the Decider's position, clip targets, makerFilled, settle's
// `want` — into the contracts of sym. It is the single conversion point: a LegB order is
// sized through it or it is not sized correctly. LegA counts pass through unchanged, so at
// ratio 1 every call is the identity and the whole feature is inert.
func (e *Engine) hedgeLots(sym string, lotsA int) int {
	if sym == e.cfg.LegB {
		return lotsA * e.hedgeRatio()
	}
	return lotsA
}

// hedgeStrayMakerFill crosses the OTHER leg with the opposite side for lots — the same
// taker cross OnFill's clip path would have issued — keeping the book hedged 1:1. It is
// the LAST-RESORT valve for maker fills the account layer could not attribute (a live
// order with no owning clip, or lots beyond a retired order's terminal count); callers
// pre-check the order's account and log their own context. While halted the kill-switch
// stays absolute: nothing is placed, only a loud log, since the operator is already
// investigating a frozen book.
func (e *Engine) hedgeStrayMakerFill(orderID, symbol string, buy bool, lots int) {
	if lots <= 0 {
		return
	}
	var other string
	switch symbol {
	case e.cfg.LegA:
		other = e.cfg.LegB
	case e.cfg.LegB:
		other = e.cfg.LegA
	default:
		return
	}
	if e.recovery.halted {
		e.logf("CRITICAL: own passive %s filled %d on %s while halted — NOT hedged (kill-switch); hedge manually", orderID, lots, symbol)
		return
	}
	// LegB → LegA at a ratio has no whole-lot answer: R contracts of LegB are one LegA lot,
	// and a stray count that is not a multiple of R cannot be paired without rounding. Either
	// direction of rounding leaves a permanent position divergence — the two-pass reconcile
	// suspend — so the engine states the problem and places nothing, the same "wait, never
	// guess" rule the deferred-retire path follows. Unreachable in practice: a ratio requires
	// SoloMakerLeg, which never rests a LegB passive, and takers never reach this valve.
	if symbol == e.cfg.LegB && e.hedgeRatio() > 1 {
		e.logf("CRITICAL: stray passive fill of %d on LegB %s (order %s) at hedge ratio %d cannot be converted to whole LegA lots — NOT hedged; the book is naked by %d contracts, fix manually",
			lots, symbol, orderID, e.hedgeRatio(), lots)
		return
	}
	e.hedge(other, !buy, e.hedgeLots(other, lots))
}

// takerRetry crosses the spread on symbol for lots, retrying up to HedgeRetries times.
// If every attempt fails, the hedge becomes a DEBT: the engine enters impaired mode
// (all orders pulled, no new clips) and the obligation loop keeps trying to place it
// until the broker accepts — waiting out the outage instead of tripping a kill-switch
// on what is almost always connection trouble. Returns whether the hedge was placed NOW.
func (e *Engine) takerRetry(symbol string, buy bool, lots int) bool {
	if e.recovery.halted {
		// The kill-switch is absolute. Callers pre-check halted, but a halt can fire MID
		// sequence — settleClip issues up to two top-ups — so the choke point itself must
		// refuse to trade.
		e.logf("CRITICAL: taker %s (buy=%v x%d) suppressed while halted — NOT hedged (kill-switch); hedge manually", symbol, buy, lots)
		return false
	}
	return e.takerRetryFrom(symbol, buy, lots, 0)
}

// takerRetryFrom continues the retry loop for symbol/buy/lots starting at attempt index
// `from` (0-based) out of EngineConfig.HedgeRetries total attempts. The taker-only parallel
// path uses `from=1` to resume after a first attempt it already raced concurrently outside
// this loop (attempt 0); every other caller goes through takerRetry, which starts fresh at
// 0. Exhausting all attempts queues the shortfall as a hedge debt (see deferHedge) instead
// of tripping a kill-switch — almost always connection trouble, not a reason to strand the
// book. Callers passing from>0 are responsible for their own halted check (takerRetry's
// check does not run here).
//
// When RejectRetryLotStep>0, lots is first run through shrinkTakerChase: a hedge is an
// amount already OWED (a fill, or a sibling leg, already moved), not a size being decided,
// so nothing requires it to arrive as one order — splitting it into installments that sum
// to the same total is safe regardless of whether the clip that created the obligation was
// an entry or an exit (contrast tryOpenClip's ladder, which only ever applies to a CLOSING
// clip's first placement, because there the size IS still being decided). Only the leftover
// shrinkTakerChase could not place falls through to the unchanged attempts loop below.
func (e *Engine) takerRetryFrom(symbol string, buy bool, lots int, from int) bool {
	if e.cfg.RejectRetryLotStep > 0 {
		if remaining := e.shrinkTakerChase(symbol, buy, lots); remaining <= 0 {
			return true
		} else {
			lots = remaining
		}
	}
	return e.takerRetryPlain(symbol, buy, lots, from)
}

// takerRetryPlain is takerRetryFrom without the shrink-chase step: attempts-from retries at
// the SAME lots, then defers the shortfall as debt (the whole of takerRetryFrom, before the
// ladder existed). openTakerOnlyClip calls it directly — bypassing takerRetryFrom's shrink —
// for the one case where shrinking is NOT safe despite RejectRetryLotStep>0: an OPENING
// clip (!Intent.IsClose) whose FIRST concurrent attempt lands on NEITHER leg. There, no
// fill has happened yet on either side — the size is still the Decider's entry decision,
// the same one tryOpenClip's own ladder refuses to touch for an opening clip. The instant
// either leg DOES land (including later, on a retry from here), its sibling's takerRetryFrom
// call is chasing a REAL, already-placed amount and shrinking it back into installments is
// safe again — that asymmetric case goes through takerRetryFrom, not this.
func (e *Engine) takerRetryPlain(symbol string, buy bool, lots int, from int) bool {
	attempts := e.cfg.HedgeRetries
	if attempts < 1 {
		attempts = 1
	}
	for i := from; i < attempts; i++ {
		if e.tryPlaceTaker(symbol, buy, lots) {
			return true
		}
	}
	e.logf("CRITICAL: taker %s (buy=%v x%d) failed %d attempts — queued as hedge debt; pulling orders and retrying until the broker answers", symbol, buy, lots, attempts)
	e.deferHedge(symbol, buy, lots)
	return false
}

// shrinkTakerChase tries to place lots of symbol/buy, and on a DEFINITIVE reject
// (MaybeDelivered==false) shrinks by RejectRetryLotStep and tries again, accumulating
// every partial placement, down to RejectRetryMinLots (floor 1 if unset). Returns the
// amount still unplaced (0 = fully placed). An ambiguous failure (MaybeDelivered==true)
// stops the ladder immediately without shrinking — the order may already rest at the
// broker, and guessing smaller on top of that uncertainty risks a double placement; the
// caller's ordinary same-size attempts loop picks up the rest instead.
func (e *Engine) shrinkTakerChase(symbol string, buy bool, lots int) int {
	floor := e.cfg.RejectRetryMinLots
	if floor < 1 {
		floor = 1
	}
	size := lots
	for size >= floor && size > 0 {
		id, err := e.placeTakerRPC(symbol, buy, size)
		if e.commitTakerPlacement(symbol, buy, size, id, err) {
			lots -= size
			if lots <= 0 {
				return 0
			}
			if size > lots {
				size = lots
			}
			continue
		}
		if MaybeDelivered(err) {
			return lots
		}
		e.logf("taker %s (buy=%v) rejected at %d lots — retrying smaller at %d", symbol, buy, size, size-e.cfg.RejectRetryLotStep)
		size -= e.cfg.RejectRetryLotStep
	}
	return lots
}

// tryPlaceTaker issues ONE market-order placement and, on acceptance, opens the order's
// fill account, registers it for confirmation tracking and credits the sink. It is the
// single placement path shared by takerRetry and the impaired obligation loop.
func (e *Engine) tryPlaceTaker(symbol string, buy bool, lots int) bool {
	id, err := e.placeTakerRPC(symbol, buy, lots)
	return e.commitTakerPlacement(symbol, buy, lots, id, err)
}

// placeTakerRPC issues ONE market-order placement RPC and reports its outcome — the id on
// acceptance, or the error. It touches NO engine state (no e.own/e.pending/e.sink/
// e.limiter), which is exactly what makes it safe to run off the engine's single goroutine:
// the taker-only path races two legs' placeTakerRPC calls concurrently, then joins before
// either result reaches commitTakerPlacement.
func (e *Engine) placeTakerRPC(symbol string, buy bool, lots int) (string, error) {
	if buy {
		return e.taker.Buy(symbol, lots)
	}
	return e.taker.Sell(symbol, lots)
}

// commitTakerPlacement folds one placeTakerRPC outcome into engine state: spends the
// limiter's placement budget, and on acceptance opens the order's fill account, registers
// it for confirmation tracking and credits the sink; on failure just logs. Reports whether
// the placement succeeded. MUST be called from the engine's single goroutine only — unlike
// placeTakerRPC, it mutates e.own/e.pending/e.sink/e.limiter.
func (e *Engine) commitTakerPlacement(symbol string, buy bool, lots int, id string, err error) bool {
	// Every attempt is one placeOrder RPC against the broker's quota, hedges included —
	// book it so the limiter's budget view doesn't wait on the next refresh poll. Processing
	// clock, not e.now — see clock.go and engine_clip.go's placeLeg.
	e.limiter.Spend(e.clock.Now(), 1)
	if err != nil {
		e.logf("taker %s (buy=%v x%d) attempt failed: %v", symbol, buy, lots, err)
		return false
	}
	if id == "" {
		// The hedge was ACCEPTED (err == nil) but the broker returned no id to track it
		// by. The lots are still credited: the engine's model books a placed taker as
		// done, and dropping the credit would desync the cap position from the hedged
		// book. Its fill events will look foreign (no account) and be ignored — correct,
		// the placement credit already carries the inventory; only the price keeps the
		// cross estimate. If the order silently never existed, reconcile flags the
		// divergence exactly like any taker that dies unfilled. Retrying here instead
		// would risk DOUBLING the hedge — the order most likely does exist. Until a
		// clean reconcile pass CONFIRMS the position, it is unverified: no new clips.
		e.logf("CRITICAL: taker %s (buy=%v x%d) returned an EMPTY order id — hedge credited blind at the touch; its fills cannot be tracked; new clips wait for a clean reconcile", symbol, buy, lots)
		e.recovery.unverified = true
		if e.sink != nil {
			e.sink.Fill(symbol, buy, lots, e.crossPrice(symbol, buy))
		}
		return true
	}
	reused := e.own[id] != nil
	if reused {
		// A reused id means the broker's responses can no longer be trusted, but the
		// hedge itself was accepted. Replace the stale account with the taker's: the
		// taker fill path is passive (amend-only, never hedges), so events misattributed
		// to the OLD order can no longer trigger a blind re-hedge. Fill attribution for
		// this id is ambiguous from here on, so the position is unverified until a clean
		// reconcile pass confirms it — no new clips before that.
		e.logf("CRITICAL: broker REUSED order id %s for a taker %s (buy=%v x%d) — replacing its fill account; new clips wait for a clean reconcile", id, symbol, buy, lots)
		e.recovery.unverified = true
	}
	acct := &ordAcct{maker: false, sym: symbol, isBuy: buy, placed: lots, final: -1, placedAt: e.now} // taker: never re-hedged as stray
	e.own[id] = acct
	// The placement credit below is an assumption until the broker's data confirms it —
	// track the order until its fill events (or a terminal status) do. A REUSED id is
	// NOT tracked: fills and status polls for it answer about an ambiguous order, and
	// mis-confirming is worse than waiting for reconcile (unverified handles it above).
	if !reused {
		e.pending[id] = struct{}{}
	} else {
		delete(e.pending, id)
	}
	// The engine's own model treats a placed taker as done (completion/commit never
	// wait on its fill event), so credit its lots to the sink at placement too, priced
	// at the touch it crosses. The fill event trues the price later (see OnFill);
	// if the taker dies unfilled, the confirmation watchdog catches and repairs it.
	if e.sink != nil {
		acct.price = e.crossPrice(symbol, buy)
		e.sink.Fill(symbol, buy, lots, acct.price)
		acct.credited = lots
	}
	return true
}
