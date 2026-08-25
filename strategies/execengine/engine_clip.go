// Clip lifecycle: one Decider-decided move worked from the signal to a booked lot —
// open (place both passives), work (pull/complete), settle the pair 1:1, commit to the
// Decider's lot book. Owns clip and the Decider handle. This is the engine's spine.

package execengine

import (
	"fmt"
	"sync"
	"time"
)

// OnState folds one prepared signal bar into the engine. When idle and the Decider wants
// to move it opens a dual-passive clip toward that move. When a clip is already working
// the signal only decides whether to KEEP it: the passive orders keep resting for as long
// as the strategy still wants the trade and are pulled once the signal has gone or
// reversed. Keeping them AT the touch as the book moves is handled separately by the
// re-peg in OnBook, not here.
func (e *Engine) OnState(state RowState) {
	e.advanceNow(state.Time)
	if e.recovery.halted {
		return
	}
	e.serviceImpaired()
	// Confirm (or repair) any takers the broker still owes fill data for BEFORE deciding —
	// a confirmation may have arrived as a status, and a confirmed shortfall must be
	// re-hedged before the position is trusted for a new clip.
	e.checkPendingTakers(e.now)
	if e.recovery.impaired {
		return // connection trouble: orders pulled, obligations retrying — wait, don't trade
	}
	if e.clip != nil {
		e.manageWorkingClip(state)
		return
	}
	// A reconcile pass has seen the broker and internal positions diverge and the next pass
	// has not yet confirmed or cleared it — the position view is in doubt, so no new clips
	// are opened against it (see Reconcile). unverified is the same doubt from the other
	// direction: the engine itself issued an order it cannot track (empty/reused id), so the
	// position is unconfirmed until a clean reconcile pass agrees with the broker.
	if e.recovery.suspect || e.recovery.unverified {
		return
	}
	// A placed taker past its confirm window has neither fill events nor a terminal status:
	// the hedge it represents is UNCONFIRMED, and the position a new clip would build on is
	// therefore a guess. Wait for the broker's data (checkPendingTakers keeps polling) rather
	// than stacking new exposure on top of it.
	if e.awaitingTaker(e.now) {
		return
	}
	if !e.canQuote(state.Time) {
		return
	}
	// Rate-limit / failure backoff: suppress opening new clips until the backoff clears so
	// a rejected order is never retried on the next bar (the request-storm that exhausts
	// the broker's quota).
	if state.Time.Before(e.backoffUntil) {
		return
	}
	in := e.dm.Peek(state)
	if in.Action == actionHold {
		return
	}
	e.tryOpenClip(in, state.Time)
}

// manageWorkingClip re-checks the signal for the in-flight clip. A resting maker order
// that simply has not filled yet is NOT cancelled — an order at the touch is worth waiting
// on, and cancelling it would forfeit queue priority and burn requests re-posting it. The
// clip is resolved only when the Decider no longer wants this direction: an unfilled clip
// is cancelled, a partially-filled one is taker-completed to a whole lot so no naked
// half-lot is left resting once we have stopped wanting it.
func (e *Engine) manageWorkingClip(state RowState) {
	in := e.dm.Peek(state)
	// The execution SHAPE is part of what is wanted, not just the direction: a strategy that
	// switches the same close from passive to taker (basis's force-flatten cutoff) must not have
	// its resting order sit out the full FillTimeout first. Resolving here routes it through the
	// normal disposal path — cancelled if nothing filled, taker-completed under
	// ForceCloseOnTimeout — and the next bar opens the clip again in the newly wanted shape.
	if in.Action == e.clip.dir && e.clipExecMode(in) == e.clip.mode {
		return // still wanted — leave the passives resting at the touch
	}
	e.resolveClip(state.Time)
}

// PullIfUnwanted cancels the working clip's RESTING orders the moment the Decider no longer
// wants its direction — an entry whose z decayed back below θ, or an exit whose closing side
// sank back below −ExitZ (closing now would realise worse than fair, so the strategy would
// rather hold for reversion and re-exit when the closing z recovers). Fill state does NOT
// matter: lots that already filled were hedged 1:1 as they happened and live in the
// ledger-derived position, so they simply stay; only the unfilled remainder is removed — the
// signal-driven twin of the KeepPartialOpenOnTimeout abandon path, without waiting out the
// timeout. No taker is ever issued beyond the equalizing top-up a cancel/fill race may need
// (see settleClip). It reuses the Decider's Peek, so the cancel threshold is exactly the
// decision gate itself. The basis runner and backtest call it on every book tick while a clip
// works — BEFORE feeding the engine the new book, so a no-longer-wanted order is cancelled
// rather than re-pegged to the moved touch. A U6 backtest showed pulling decayed entries
// lifts gross edge — they are adverse late fills. A configured MinRest guarantees a
// just-placed order its hang time before this pull may remove it (see EngineConfig.MinRest).
//
// NOT for strategies that book whole clips in a lot book (spread): dropping a partial
// UNCOMMITTED would desync their book from the broker — they resolve a lost signal via
// OnState/resolveClip instead, which completes a begun clip to target.
func (e *Engine) PullIfUnwanted(state RowState) {
	c := e.clip
	if c == nil {
		return
	}
	// MinRest: a just-placed (or just re-pegged) order gets its guaranteed hang time before
	// a flickering signal may pull it — see EngineConfig.MinRest. The clock is the NEWEST
	// resting placement, so a re-peg restarts the guarantee at the new price.
	if e.cfg.MinRest > 0 && state.Time.Sub(c.newestPlacement()) < e.cfg.MinRest {
		return
	}
	if e.dm.Peek(state).Action == c.dir {
		return // still wanted (entry z ≥ θ, or closing z still ≥ −ExitZ)
	}
	e.CancelClip()
}

// newestPlacement is the placement time of the clip's most recently (re-)placed resting
// order — the reference point for the MinRest guarantee. Legs without a resting order
// (single-passive LegB, or a leg already retired) don't count.
func (c *clip) newestPlacement() time.Time {
	var t time.Time
	if c.legA.id != "" && c.legA.placedAt.After(t) {
		t = c.legA.placedAt
	}
	if c.legB.id != "" && c.legB.placedAt.After(t) {
		t = c.legB.placedAt
	}
	return t
}

// resolveClip disposes of the working clip when it is no longer wanted (or an optional
// fill-timeout backstop has fired): nothing filled → abandon the clip (see abandonClip
// for what happens when the cancels catch a fill in flight); something filled →
// taker-complete the remainder so exactly one whole lot is booked.
func (e *Engine) resolveClip(ts time.Time) {
	if e.clip == nil {
		return
	}
	// Nothing filled (as far as fill events told us): abandon the clip, UNLESS it is a closing
	// clip and the strategy asked for guaranteed reductions — then cross the spread to finish.
	if e.clip.makerFilled == 0 && (!e.cfg.ForceCloseOnTimeout || !e.clip.intent.IsClose) {
		e.abandonClip(ts)
		return
	}
	// Partially filled: an opening clip may ask to keep the already-hedged partial rather than
	// taker-chase the remainder (see KeepPartialOpenOnTimeout). A closing clip always finishes —
	// once a reduction has begun, it must not leave the position in an odd, undecided state.
	if !e.clip.intent.IsClose && e.cfg.KeepPartialOpenOnTimeout {
		e.CancelClip()
		return
	}
	e.completeClip(ts)
}

// abandonClip drops an (event-wise) unfilled clip. Its cancels can still CATCH fills in
// flight — a leg that began filling before the cancel landed. Under KeepPartialOpenOnTimeout
// (basis) the catch is equalized and dropped exactly like a partial-fill timeout: the
// fill-derived ledger already carries it, so CancelClip's teardown is enough. Otherwise
// (spread semantics: once a leg begins filling, the clip completes) the catch is folded as
// the clip's first fill and the clip is completed to target and COMMITTED — the same
// convention as the re-peg fold and the partial-fill signal reversal. The old handling
// (equalize the catch but never commit) left a real, hedged pair at the broker that the
// Decider's book knew nothing about: a permanent position divergence that tripped the
// two-pass reconcile kill-switch on a perfectly routine cancel/fill race.
func (e *Engine) abandonClip(ts time.Time) {
	c := e.clip
	if !c.intent.IsClose && e.cfg.KeepPartialOpenOnTimeout {
		e.CancelClip()
		return
	}
	gapA := e.retireOrder(c.legA.id)
	gapB := e.retireOrder(c.legB.id)
	if e.recovery.halted || e.clip == nil {
		return // a retire hit a stuck order and halted — Halt already tore the clip down
	}
	if gapA == 0 && gapB == 0 {
		e.clip = nil // clean abandon: nothing rested filled
		return
	}
	// A cancel caught fills after all. Complete the pair to target (the retires above are
	// idempotent, so settleClip only issues the completion takers) and book the lot.
	e.settleClip(gapA, gapB, true)
	if !e.recovery.halted {
		e.commitClip(ts)
	}
}

// commitClip records the completed clip's move in the Decider's lot book and returns to
// idle. Called once the maker leg is fully filled (in OnFill) or the unfilled remainder
// has been taker-completed (in checkTimeout). The commit happens FIRST — the lot is real,
// its fills confirmed — so a maker retire that runs into connection trouble afterwards
// (deferred to the obligation loop) can never lose the lot from the Decider's book.
func (e *Engine) commitClip(ts time.Time) {
	c := e.clip
	if c == nil {
		return
	}
	e.dm.Commit(c.intent, ts)
	if p, ok := e.dm.(Persister); ok {
		p.SaveLots() // optional: persist the lot book only for Deciders that keep one
	}
	e.clip = nil
	if c.makerID != "" {
		// Pull any maker passive left resting after a top-up completion. Lots its cancel
		// catches in flight are real inventory beyond the booked target — match them on
		// the other leg to stay 1:1 (takerRetry queues the debt if the broker is down;
		// while halted the kill-switch already logged).
		if gap := e.retireOrder(c.makerID); gap > 0 && !e.recovery.halted {
			if c.makerID == c.legA.id {
				e.takerRetry(e.cfg.LegB, c.legB.isBid, e.hedgeLots(e.cfg.LegB, gap))
			} else {
				e.takerRetry(e.cfg.LegA, c.legA.isBid, gap)
			}
		}
	}
}

// checkTimeout fires the optional fill-timeout backstop: a clip that has sat past its
// deadline is resolved the same way a signal-loss resolves it. It is a no-op unless
// FillTimeout > 0 (the deadline is zero when the backstop is disabled).
func (e *Engine) checkTimeout(ts time.Time) {
	c := e.clip
	if c == nil || c.deadline.IsZero() || ts.Before(c.deadline) {
		return
	}
	e.resolveClip(ts)
}

// completeClip settles the clip to its full target — retiring both legs and crossing
// the unfilled remainder with takers — guaranteeing the whole lot is realized once the
// maker leg has begun to fill. On a taker failure the engine is already halted and the
// clip is left for the operator to inspect.
func (e *Engine) completeClip(ts time.Time) {
	c := e.clip
	if c == nil {
		return
	}
	e.settleClip(c.makerFilled, c.makerFilled, true)
	if e.recovery.halted {
		return
	}
	e.commitClip(ts)
}

// CancelClip pulls both of the working clip's passive orders — settling any lots the
// cancels catch in flight so the pair stays 1:1 — and returns to idle. Exported for
// callers (e.g. a shutdown path) that need to pull resting orders without tripping the
// kill-switch the way Halt does.
func (e *Engine) CancelClip() {
	if e.clip == nil {
		return
	}
	e.settleClip(e.clip.makerFilled, e.clip.makerFilled, false)
	e.clip = nil
}

// settleClip retires both of the working clip's legs, folds any lots their cancels
// caught in flight, and issues the minimal takers that land the PAIR balanced: each leg
// is topped up to `want` — the larger leg's realized count, and at least the clip's
// target when complete=true (a completing teardown crosses the unfilled remainder; an
// abandoning one only equalizes what actually filled). realA/realB are the lots already
// realized per leg BEFORE retirement — the normal invariant is both == makerFilled,
// because every processed maker fill was hedged 1:1 as it happened (the re-peg fold is
// the one caller that passes an unbalanced pair). While halted nothing is placed — the
// kill-switch is absolute — the imbalance is only logged. The clip is left in place;
// callers decide whether to commit or drop it.
func (e *Engine) settleClip(realA, realB int, complete bool) {
	c := e.clip
	if c == nil {
		return
	}
	realA += e.retireOrder(c.legA.id)
	realB += e.retireOrder(c.legB.id)
	want := max(realA, realB)
	if complete {
		want = max(want, c.target)
	}
	if realA == want && realB == want {
		return
	}
	if e.recovery.halted {
		e.logf("CRITICAL: clip legs unbalanced at halt (legA=%d legB=%d want=%d) — NOT hedged (kill-switch); fix manually", realA, realB, want)
		return
	}
	if realA < want {
		e.takerRetry(e.cfg.LegA, c.legA.isBid, want-realA)
	}
	if realB < want {
		e.takerRetry(e.cfg.LegB, c.legB.isBid, e.hedgeLots(e.cfg.LegB, want-realB))
	}
}

// tryOpenClip gates a discretionary clip open on the rate limiter, then opens it. When the
// limiter declines (quota low) it backs off until the quota window resets so the engine
// neither spins nor pushes the broker over its request budget. A clip posts two orders.
func (e *Engine) tryOpenClip(in Intent, ts time.Time) {
	lots := in.Lots // a self-sizing Decider sets the clip size; 0 falls back to the fixed OrderVol
	if lots <= 0 {
		lots = e.cfg.OrderVol
	}
	if lots <= 0 {
		return
	}
	floor := e.cfg.RejectRetryMinLots
	if floor < 1 {
		floor = 1
	}
	for {
		// Allow is checked on the PROCESSING clock (same domain as Spend — see clock.go),
		// not ts (the event clock OnState decided on). retryAt therefore comes back in the
		// processing clock's domain too: translate it into a DURATION and re-anchor that
		// onto ts before storing it in backoffUntil, which the rest of the engine compares
		// against the event clock (see OnState's backoff check) — copying retryAt across
		// verbatim would misfire the backoff by however far the two clocks have drifted.
		processingNow := e.clock.Now()
		if ok, retryAt := e.limiter.Allow(processingNow, 2); !ok {
			if retryAt.After(processingNow) {
				e.backoffUntil = ts.Add(retryAt.Sub(processingNow))
			} else {
				e.backoffUntil = ts.Add(e.placeBackoff())
			}
			return
		}
		opened, retryable := e.openClip(in, lots, ts)
		if opened || !retryable || e.cfg.RejectRetryLotStep <= 0 {
			return // openClip already logged and set backoffUntil on any failure
		}
		next := lots - e.cfg.RejectRetryLotStep
		if next < floor {
			return
		}
		e.logf("rejected clip at %d lots — retrying smaller at %d", lots, next)
		lots = next
	}
}

// canQuote reports whether both legs currently carry a valid, non-crossed and sufficiently
// fresh book to place orders against as of ts.
func (e *Engine) canQuote(ts time.Time) bool {
	if !e.legA.valid() || !e.legB.valid() {
		return false
	}
	if e.cfg.MaxStaleness > 0 &&
		(ts.Sub(e.legA.ts) > e.cfg.MaxStaleness || ts.Sub(e.legB.ts) > e.cfg.MaxStaleness) {
		return false
	}
	return true
}

// clipExecMode resolves the shape ONE clip is executed in. An Intent that names a mode wins
// over the engine-wide configuration for that clip alone (see Intent.ExecMode); the zero value
// defers to SoloMakerLeg/TakerOnly, which is what every existing strategy relies on.
//
// The result is always one of the three concrete shapes, never ExecDefault, so callers can
// compare two clips' modes directly: manageWorkingClip resolves a resting clip the moment the
// Decider wants the same direction executed differently (basis crossing into its force-flatten
// taker phase), and a mode comparison on the raw Intents would read "default" and "solo_maker"
// as a change under a solo-maker engine when nothing actually changed.
//
// Dual-passive at a hedge ratio > 1 is a misconfiguration the engine refuses at construction
// (see NewEngine); reached per-clip it cannot halt — a halt inside a force-flatten window would
// strand the position overnight, which is worse than the thing being guarded against — so the
// clip is downgraded to the solo shape, whose LegB conversion only ever runs one way, and the
// downgrade is logged loudly.
func (e *Engine) clipExecMode(in Intent) ExecMode {
	mode := in.ExecMode
	if mode == ExecDefault {
		switch {
		case e.cfg.TakerOnly:
			mode = ExecTaker
		case e.cfg.SoloMakerLeg:
			mode = ExecSoloMaker
		default:
			mode = ExecDualPassive
		}
	}
	if (mode == ExecDualPassive || mode == ExecSoloMakerLegB) && e.cfg.HedgeRatio > 1 {
		e.logf("CRITICAL: clip asked for %s execution at HedgeRatio=%d — LegB cannot rest at a ratio (see EngineConfig.HedgeRatio); executing solo-maker on LegA instead", mode, e.cfg.HedgeRatio)
		mode = ExecSoloMaker
	}
	return mode
}

// openClip attempts to open a clip at exactly `lots`: for a buy (dir>0) bid leg A and ask
// leg B, for a sell the reverse, each resting at its side's touch. On a placement error the
// clip is unwound so the engine never carries a single naked resting order. It reports
// whether the clip opened and, if not, whether the failure is eligible for tryOpenClip's
// shrink-and-retry ladder — see RejectRetryLotStep's doc for exactly which failures qualify
// (only a CLOSING clip's first placement; never an opening clip — see in.IsClose below).
func (e *Engine) openClip(in Intent, lots int, ts time.Time) (opened, retryable bool) {
	dir := in.Action
	mode := e.clipExecMode(in)
	if mode == ExecTaker {
		return e.openTakerOnlyClip(in, dir, lots, ts)
	}
	// For a buy (dir>0) we bid leg A and ask leg B; for a sell the reverse. Each order
	// rests at its own side's touch.
	legA := legOrder{isBid: dir > 0}
	legB := legOrder{isBid: dir < 0}
	legA.price = e.legA.sidePrice(legA.isBid)
	legB.price = e.legB.sidePrice(legB.isBid)
	// Leg-B solo is the mirror of the LegA case: rest the perp alone and let the existing
	// first-fill path taker-hedge LegA. Which leg is worth resting is a property of the pair
	// (the wider half-spread), not of the engine, so both mirrors exist.
	if mode == ExecSoloMakerLegB {
		if err := e.placeLeg(&legB, e.cfg.LegB, e.hedgeLots(e.cfg.LegB, lots)); err != nil {
			e.logf("place legB failed: %v", err)
			e.backoffUntil = ts.Add(e.placeBackoff())
			// Nothing placed yet, so size is renegotiable — but only for a CLOSING clip: an
			// opening clip's size is the Decider's entry sizing (room under the cap/SizeGate),
			// and shrinking it on a margin reject would silently under-open a position the
			// signal asked for a specific size of. A rejected exit has no such ambiguity —
			// getting SOME of it off is strictly better than none.
			return false, in.IsClose && !MaybeDelivered(err)
		}
		var deadline time.Time
		if e.cfg.FillTimeout > 0 {
			deadline = ts.Add(e.cfg.FillTimeout)
		}
		e.clip = &clip{dir: dir, intent: in, target: lots, legA: legA, legB: legB, mode: mode, deadline: deadline}
		return true, false
	}
	if err := e.placeLeg(&legA, e.cfg.LegA, lots); err != nil {
		e.logf("place legA failed: %v", err)
		e.backoffUntil = ts.Add(e.placeBackoff())
		// See the ExecSoloMakerLegB branch above: only a CLOSING clip's first placement is
		// ladder-eligible.
		return false, in.IsClose && !MaybeDelivered(err)
	}
	// Single-passive: LegB is never rested — it is taker-hedged when LegA fills (the first-fill
	// path retires the empty LegB id → 0 → crosses the full leg). LegB keeps its direction
	// (isBid) so that hedge takes the right side, but carries no order id.
	if mode == ExecDualPassive {
		if err := e.placeLeg(&legB, e.cfg.LegB, lots); err != nil {
			e.logf("place legB failed: %v", err)
			// legA may have filled in this window — its retire gap is a naked leg; match it.
			if gap := e.retireOrder(legA.id); gap > 0 {
				e.takerRetry(e.cfg.LegB, legB.isBid, e.hedgeLots(e.cfg.LegB, gap))
			}
			e.backoffUntil = ts.Add(e.placeBackoff())
			// legA already rested (and may have caught a race fill before its cancel): the
			// ladder's "retry the whole pair smaller" would attempt MORE on top of whatever
			// gap already got hedged, potentially overshooting the Decider's intended size
			// (e.g. past a close's flat into a reverse). Not retryable — old backoff only.
			return false, false
		}
	}
	// Only arm the timeout backstop when FillTimeout > 0; otherwise the clip rests until
	// it fills or the signal turns (a zero deadline disables checkTimeout).
	var deadline time.Time
	if e.cfg.FillTimeout > 0 {
		deadline = ts.Add(e.cfg.FillTimeout)
	}
	e.clip = &clip{dir: dir, intent: in, target: lots, legA: legA, legB: legB, mode: mode, deadline: deadline}
	return true, false
}

// openTakerOnlyClip crosses both legs as market orders the instant a clip opens and
// commits the Decider's intent immediately — there is no resting order, so the clip never
// becomes "working" and OnFill never sees it. buyA mirrors legA.isBid in the dual-passive
// path (bid leg A / ask leg B for a buy, the reverse for a sell); the taker path credits
// the FillSink at placement (the engine's model treats a placed taker as done), so the
// position the Decider caps against and the position it commits agree the instant this
// returns.
//
// Both legs' FIRST attempt races the wire concurrently (placeTakerRPC does no engine-state
// mutation, so it is safe off the engine's single goroutine) instead of paying leg A's full
// RPC round-trip before leg B is even sent — the naked-exposure window between the two legs
// is what taker-only mode exists to close. A failed first attempt falls through to the
// existing sequential retry loop (takerRetryFrom) for that leg alone; retries stay rare
// enough that racing them too isn't worth the extra state-machine complexity.
//
// Reports whether the clip opened; this is NEVER eligible for tryOpenClip's own shrink-and-
// retry ladder (always returns opened=true, retryable=false, kill-switch aside) — unlike the
// maker path, a leg's shortfall here is chased by takerRetryFrom's own shrink-and-accumulate
// (engine_hedge.go, RejectRetryLotStep-gated), which reaches the SAME full target more
// completely (installments within this one call, not a smaller clip punted to the next Peek).
// A leg whose sibling already succeeded is never a sizing decision — it must hedge exactly
// what the sibling did, installments included, via takerRetryFrom. The ONE case that must NOT
// shrink is neither leg moving on an OPENING clip: no fill happened on either side, so the
// size is still the Decider's entry decision, not a hedge to chase — that goes through
// takerRetryPlain instead (see the switch below).
func (e *Engine) openTakerOnlyClip(in Intent, dir int, lots int, ts time.Time) (opened, retryable bool) {
	if e.recovery.halted {
		// tryPlaceTaker's other callers reach the kill-switch check through takerRetry;
		// this path races placeTakerRPC directly for the first attempt, bypassing it — so
		// the kill switch needs its own check here.
		e.logf("CRITICAL: taker-only open %s/%s (x%d) suppressed while halted — NOT hedged (kill-switch); hedge manually", e.cfg.LegA, e.cfg.LegB, lots)
		return true, false
	}
	buyA := dir > 0
	buyB := !buyA
	// Both sizes are known before either order is sent, so a hedge ratio needs no division
	// here either — LegB simply crosses R× the clip (identity at ratio 1).
	lotsB := e.hedgeLots(e.cfg.LegB, lots)
	var idA, idB string
	var errA, errB error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		idA, errA = e.placeTakerRPC(e.cfg.LegA, buyA, lots)
	}()
	go func() {
		defer wg.Done()
		idB, errB = e.placeTakerRPC(e.cfg.LegB, buyB, lotsB)
	}()
	wg.Wait()

	okA := e.commitTakerPlacement(e.cfg.LegA, buyA, lots, idA, errA)
	okB := e.commitTakerPlacement(e.cfg.LegB, buyB, lotsB, idB, errB)

	switch {
	case !okA && !okB && !in.IsClose:
		// Neither leg moved on an OPENING clip: no fill has happened on either side, so this
		// is still the Decider's entry-sizing decision, not a hedge to chase — takerRetryFrom
		// would shrink it (RejectRetryLotStep>0), exactly what tryOpenClip's own ladder
		// refuses to do for !IsClose. takerRetryPlain retries each leg at its own un-shrunk
		// size instead.
		e.takerRetryPlain(e.cfg.LegA, buyA, lots, 1)
		e.takerRetryPlain(e.cfg.LegB, buyB, lotsB, 1)
	default:
		// Either leg already succeeded (a REAL placement, whether this clip opens or closes),
		// or this is a closing clip where neither did — closing has no "still deciding the
		// size" ambiguity (getting some of it off beats none), so lots/lotsB are always safe
		// to chase via takerRetryFrom's shrink-and-accumulate (engine_hedge.go).
		if !okA {
			e.takerRetryFrom(e.cfg.LegA, buyA, lots, 1)
		}
		if !okB {
			e.takerRetryFrom(e.cfg.LegB, buyB, lotsB, 1)
		}
	}

	// Commit the intent as ACTUALLY worked (lots — the resolved size after OrderVol fallback
	// or a shrink-retry rung), not the raw request: legB's own count is a hedge-ratio
	// conversion of it, not an independent size, so lots (LegA units, what everything the
	// engine counts is denominated in) is the one true "what happened" figure.
	acted := in
	acted.Lots = lots
	e.dm.Commit(acted, ts)
	if p, ok := e.dm.(Persister); ok {
		p.SaveLots()
	}
	return true, false
}

// placeLeg posts lo's side (a bid or an ask) for lots at lo.price and records the returned
// order id on lo. It is the single spot both the initial open and a re-peg go through.
func (e *Engine) placeLeg(lo *legOrder, sym string, lots int) error {
	var id string
	var err error
	if lo.isBid {
		id, err = e.maker.PlaceBid(sym, lots, lo.price)
	} else {
		id, err = e.maker.PlaceAsk(sym, lots, lo.price)
	}
	// One placeOrder RPC spent whether or not it succeeded — the broker meters requests,
	// not accepted orders. Spend books against the PROCESSING clock (read right after the
	// RPC returns), never e.now: a broker quota window is real time, and e.now can lag
	// arbitrarily far behind it while this very RPC was in flight (see clock.go).
	e.limiter.Spend(e.clock.Now(), 1)
	if err != nil {
		return err
	}
	// A nil error with an unusable id is broker corruption placeLeg cannot paper over: an
	// EMPTY id can never be cancelled or matched to its fills (and would collide with the
	// clip's "no maker yet" makerID sentinel, so a fill on it would never designate the
	// maker), and a REUSED id would clobber the existing order's fill account — its dedup
	// state, and with it every late-race-fill guard. Either way the order may actually be
	// resting at the broker: fail the placement loudly and let the caller unwind; if the
	// untracked ghost ever fills, its fill looks foreign (ignored) and reconcile flags the
	// divergence.
	if id == "" {
		e.logf("CRITICAL: broker returned an EMPTY order id placing %s x%d — treating as failed; the order may rest untracked, so new clips wait for a clean reconcile", sym, lots)
		e.recovery.unverified = true
		return fmt.Errorf("broker returned an empty order id for %s", sym)
	}
	if e.own[id] != nil {
		e.logf("CRITICAL: broker REUSED order id %s placing %s x%d — treating as failed to protect the existing order's fill account; the order may rest untracked, so new clips wait for a clean reconcile", id, sym, lots)
		e.recovery.unverified = true
		return fmt.Errorf("broker reused order id %s for %s", id, sym)
	}
	lo.id = id
	lo.placedAt = e.now // engine clock (the caller's event time) — starts the MinRest guarantee
	// maker: ledger-own for the runner; fills fold through its account (see OnFill). The
	// leg/side/limit are recorded so a cancel-ack gap can be credited without a fill event.
	e.own[id] = &ordAcct{maker: true, sym: sym, isBuy: lo.isBid, price: lo.price, placed: lots, final: -1}
	return nil
}
