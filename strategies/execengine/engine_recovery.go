// Degraded operation: impaired mode (the broker stopped answering — park obligations and
// retry at a backing-off pace), halt, and reconciliation against the broker's real
// position. Owns impaired/halted/suspect and drains the deferred retire/hedge queues.

package execengine

import (
	"fmt"
)

// enterImpaired switches the engine into connection-trouble mode: no new clips, all
// resting orders pulled (at the next handler entry — never mid-teardown, so a failure
// inside settleClip cannot recursively re-settle the same clip), and every outstanding
// obligation retried at a backing-off pace until the broker answers. It NEVER halts:
// impaired is the state for unanswered questions, and unanswered questions are waited
// out, not given up on. Idempotent; a halted engine stays halted (the kill-switch is the
// operator's, and it wins).
func (e *Engine) enterImpaired(reason string) {
	if e.recovery.halted || e.recovery.impaired {
		return
	}
	e.recovery.impaired = true
	e.recovery.retryGap = e.placeBackoff()
	e.recovery.nextRetryAt = e.now.Add(e.recovery.retryGap)
	e.warn("%s — pulling all orders, suspending new clips; retrying outstanding obligations until the broker answers", reason)
}

// deferRetire queues an order whose retirement the broker would not confirm (cancel
// failed and no terminal status). The obligation loop keeps asking; until it is answered
// the order counts zero executed lots — WAITING, never guessing — and any lots the
// eventual answer reveals are pair-hedged then.
func (e *Engine) deferRetire(orderID string, acct *ordAcct) {
	if acct.deferred {
		return
	}
	acct.deferred = true
	e.recovery.retireQ = append(e.recovery.retireQ, orderID)
	e.enterImpaired(fmt.Sprintf("order %s: cancel failed and no terminal status yet", orderID))
}

// deferHedge queues a taker hedge that could not be placed. The book OWES these lots; the
// obligation loop keeps trying until the broker accepts them. The debt is NOT credited to
// the sink until actually placed — credits belong to real orders only.
func (e *Engine) deferHedge(symbol string, buy bool, lots int) {
	e.recovery.debts = append(e.recovery.debts, hedgeDebt{sym: symbol, buy: buy, lots: lots})
	e.enterImpaired(fmt.Sprintf("hedge %s (buy=%v x%d) could not be placed", symbol, buy, lots))
}

// serviceImpaired runs the impaired-mode housekeeping at a handler entry (a safe point:
// no clip teardown is mid-flight). It pulls a still-working clip, then retries the
// outstanding obligations at the current backoff pace. When every obligation has been
// confirmed by the broker the engine recovers on its own — through the unverified gate,
// so one clean reconcile pass re-confirms the position before any new clip opens.
func (e *Engine) serviceImpaired() {
	if !e.recovery.impaired || e.recovery.halted {
		return
	}
	if e.clip != nil {
		// "Pull all orders": dispose of the working clip exactly the way a lost signal
		// does — unfilled → cancel, partially filled → complete/keep per the strategy's
		// own config. Failures inside land in the queues below.
		e.resolveClip(e.now)
	}
	if e.now.Before(e.recovery.nextRetryAt) {
		return
	}
	progressed := false

	if len(e.recovery.retireQ) > 0 {
		remaining := e.recovery.retireQ[:0]
		for _, id := range e.recovery.retireQ {
			if e.tryDeferredRetire(id) {
				progressed = true
			} else {
				remaining = append(remaining, id)
			}
		}
		e.recovery.retireQ = remaining
	}
	if len(e.recovery.debts) > 0 {
		remaining := e.recovery.debts[:0]
		for _, d := range e.recovery.debts {
			if e.tryPlaceTaker(d.sym, d.buy, d.lots) {
				progressed = true
			} else {
				remaining = append(remaining, d)
			}
		}
		e.recovery.debts = remaining
	}

	if len(e.recovery.retireQ) == 0 && len(e.recovery.debts) == 0 && e.clip == nil {
		e.recovery.impaired = false
		e.recovery.unverified = true // one clean reconcile pass must confirm the position before trading
		e.recovery.retryGap = 0
		e.logf("RECOVERED: every deferred cancel and hedge is confirmed — waiting for a clean reconcile before opening clips")
		return
	}
	// Back off while the broker keeps not answering; any progress resets the pace.
	if progressed {
		e.recovery.retryGap = e.placeBackoff()
	} else if e.recovery.retryGap *= 2; e.recovery.retryGap > impairedRetryMax {
		e.recovery.retryGap = impairedRetryMax
	}
	e.recovery.nextRetryAt = e.now.Add(e.recovery.retryGap)
}

// tryDeferredRetire re-asks the broker about one queued retirement: one cancel attempt,
// then one status read. No answer → false, stays queued (the engine waits — forever if
// need be). A terminal answer settles the account exactly like a live retire, and any
// executed lots the engine had not acted on are pair-hedged now (the same last-resort
// cross a stray fill gets), keeping the book 1:1.
func (e *Engine) tryDeferredRetire(orderID string) bool {
	acct := e.own[orderID]
	if acct == nil {
		return true // untracked — nothing to confirm
	}
	if acct.final < 0 {
		executed, err := e.maker.Cancel(orderID)
		if err != nil {
			var terminal bool
			executed, terminal, err = e.maker.Status(orderID)
			if err != nil || !terminal {
				return false // still no data — keep waiting
			}
		}
		e.finishRetire(orderID, acct, executed)
	}
	acct.deferred = false
	if gap := acct.final - acct.folded; gap > 0 {
		acct.folded = acct.final
		e.logf("deferred retire %s resolved: %d executed lots surfaced — pair-hedging them", orderID, gap)
		e.hedgeStrayMakerFill(orderID, acct.sym, acct.isBuy, gap)
	} else {
		e.logf("deferred retire %s resolved: terminal, nothing unaccounted", orderID)
	}
	return true
}

// Impaired reports whether the engine is in connection-trouble mode: orders pulled, new
// clips suspended, obligations retrying until the broker answers.
func (e *Engine) Impaired() bool { return e.recovery.impaired }

// Suspect reports whether trading is suspended pending position confirmation: a reconcile
// divergence (suspect) or an unconfirmable broker response (unverified). It clears by
// itself on a clean reconcile pass.
func (e *Engine) Suspect() bool { return e.recovery.suspect || e.recovery.unverified }

// StateLabel names the engine's current lifecycle state for the runners' status blocks:
// "halted" (kill-switch), "impaired" (connection trouble: orders pulled, obligations
// retrying, waiting for the broker), "working" (a clip's passives in flight) or "suspect"
// (position awaiting reconcile confirmation: no new clips until broker and book agree),
// in that precedence. "" means none of these — the engine is simply ready, and the caller
// substitutes its own strategy-level state (warmup, closed session, idle).
func (e *Engine) StateLabel() string {
	switch {
	case e.recovery.halted:
		return "halted"
	case e.recovery.impaired:
		return "impaired"
	case e.clip != nil:
		return "working"
	case e.Suspect():
		return "suspect"
	}
	return ""
}

// Halt trips the kill-switch: it pulls any resting passive orders and stops the engine
// from opening new clips. The current position is frozen (not auto-flattened) so an
// operator can investigate and close it deliberately. Idempotent.
func (e *Engine) Halt(reason string) {
	if e.recovery.halted {
		return
	}
	e.recovery.halted = true
	e.critical("%s (position=%d)", reason, e.Position())
	e.CancelClip()
}

// Halted reports whether the kill-switch is tripped.
func (e *Engine) Halted() bool { return e.recovery.halted }

// Reconcile compares the broker's actual per-leg positions (in contracts, signed) against
// what the strategy believes it holds — the guard that catches missed fills and outside
// interference. A long position P means +P on leg A and −P on leg B.
//
// A single divergence is NOT immediately fatal: hedges and forced taker closes fill
// asynchronously, and the engine goes idle the moment a taker completion is PLACED, not
// filled, so the broker can be briefly ahead of the fill stream (the runner's last-fill
// grace cannot see an in-flight taker that had no preceding fill). The first divergent
// pass therefore only suspends new clips and waits; a divergence still present on the
// NEXT pass is real and trips the kill-switch. Callers should only reconcile while idle
// (no clip working) and settled (no fill for a few seconds).
func (e *Engine) Reconcile(legAActual, legBActual int) {
	// A working clip's legs are legitimately in flight — comparing mid-clip would read a
	// half-realized pair as a divergence and false-suspect a healthy clip. Impaired is the
	// same: deferred cancels and unpaid hedge debts are EXPECTED divergence, so comparing
	// mid-outage would only mis-blame the position. And a halted book is frozen for the
	// operator, with nothing to resume. The runners already gate on working/halted; the
	// engine refuses all three so no caller can misuse it.
	if e.recovery.halted || e.clip != nil || e.recovery.impaired {
		return
	}
	// The broker reports each leg in its OWN contracts: a position of P LegA lots is −P on
	// LegB at the usual 1:1, and −P×R when the legs carry different notional (see
	// EngineConfig.HedgeRatio).
	pos := e.Position()
	wantB := -e.hedgeLots(e.cfg.LegB, pos)
	if legAActual == pos && legBActual == wantB {
		if e.recovery.suspect {
			e.logf("reconcile: broker and internal positions agree again (pos=%d) — resuming", pos)
			e.recovery.suspect = false
			e.recovery.mismatchLogged = false
		}
		// A clean pass is the data confirmation the unverified state was waiting for: whatever
		// untrackable order the broker's empty/reused id left behind, the account's actual
		// positions now agree with the book.
		if e.recovery.unverified {
			e.logf("reconcile: broker and internal positions agree (pos=%d) — unverified broker responses confirmed harmless, resuming", pos)
			e.recovery.unverified = false
		}
		return
	}
	if !e.recovery.suspect {
		e.recovery.suspect = true
		e.logf("reconcile WARNING: legA have=%d want=%d, legB have=%d want=%d — possibly an in-flight fill; new clips suspended until the next reconcile confirms",
			legAActual, pos, legBActual, wantB)
		return
	}
	// The divergence survived a second pass: it is real. The old engine tripped the
	// kill-switch here, which froze the bot until an operator restarted it. Autonomous
	// mode SUSPENDS instead: no new clips for as long as the positions disagree (exactly
	// the halt's protection), while reconcile keeps comparing every interval — if the
	// divergence heals (a delayed fill lands, an outside transfer is reversed, the broker
	// view catches up), trading resumes by itself. No auto-trading repair is attempted:
	// the position is doubted, and trading on a doubted position is guessing.
	if !e.recovery.mismatchLogged {
		e.recovery.mismatchLogged = true
		e.critical("POSITION MISMATCH persisted across two reconciles: legA have=%d want=%d, legB have=%d want=%d — new clips suspended until broker and internal positions agree again (position frozen, not auto-repaired; investigate if this does not clear)",
			legAActual, pos, legBActual, wantB)
	}
}
