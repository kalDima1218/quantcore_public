// Order registry: what the engine itself placed. Retiring an order (cancel + settle its
// executed count) and the per-order dedup that keeps a re-delivered fill event from
// minting inventory twice — the fix for the 2026-07-15 churn storm. Owns own and retireQ.

package execengine

// OnOrderStatus reacts to a terminal order-state update. If a working clip's passive
// order dies WITHOUT filling (rejected, cancelled or expired by the exchange), the clip
// is broken, so it is pulled and the engine re-evaluates on the next bar rather than
// waiting out the fill timeout. Fills are handled by OnFill, so dead=false is ignored.
//
// A terminal status for an order the engine ITSELF already retired is ignored: it is the
// broker's ack of our own cancel echoing back through the order stream. The common case is
// the counterpart leg — its id stays in the clip after the first fill retires it, so its
// CANCELED event would otherwise tear down the still-working clip moments after every
// partial first fill (pulling the resting maker and skipping the commit). Only an order
// that died while the engine still considered it live (final unknown) breaks the clip.
func (e *Engine) OnOrderStatus(orderID string, dead bool) {
	if !dead {
		return
	}
	// An own TAKER reported terminal-dead is the broker saying an assumed-done hedge may not
	// have happened. Don't guess which way — read the authoritative executed count and settle
	// the account: fully filled → confirmed; short → un-credit and re-hedge the shortfall.
	if acct := e.own[orderID]; acct != nil && !acct.maker {
		if acct.final < 0 && acct.seen < acct.placed {
			e.confirmDeadTaker(orderID, acct)
		}
		return
	}
	// A terminal status for a DEFERRED retire is the order-status stream answering the very
	// question the unary RPCs would not: the order is done. Try to settle the obligation NOW
	// (one cancel/status round) instead of waiting out the retry backoff — cross-stream
	// redundancy is exactly what makes an outage survivable. Failure keeps it queued.
	if acct := e.own[orderID]; acct != nil && acct.maker && acct.deferred && !e.recovery.halted {
		e.tryDeferredRetire(orderID)
		return
	}
	if e.clip == nil {
		return
	}
	if orderID != e.clip.legA.id && orderID != e.clip.legB.id {
		return
	}
	if acct := e.own[orderID]; acct != nil && acct.final >= 0 {
		return // our own retire's cancel-ack — the clip is intact
	}
	e.CancelClip()
}

// Own reports whether orderID is one this engine placed this process (a maker leg or a taker
// hedge). The live runner uses it to fold only genuinely-own fills into the P&L ledger,
// ignoring other strategies' fills on a shared account and this strategy's own fills from a
// prior session that the account trade stream replays on subscribe.
func (e *Engine) Own(orderID string) bool { _, ok := e.own[orderID]; return ok }

// retireOrder cancels orderID and learns the authoritative total it executed, marking
// the order terminal. It returns the GAP — executed lots the engine has not yet acted
// on via fill events — which the caller must fold into its pair accounting (top-up,
// settle or hedge). Idempotent: a second retire returns 0; unknown ids return 0.
//
// A failed cancel does NOT mean nothing executed — cancelling an already-filled order
// is itself an error at the broker — so on error the truth is read from Status (after
// one cancel retry for transient rpc blips). Only when the order can neither be
// cancelled nor observed terminal does the engine halt: a resting order that cannot be
// pulled is the stuck-leg condition.
func (e *Engine) retireOrder(orderID string) int {
	acct := e.own[orderID]
	if acct == nil {
		return 0
	}
	if acct.deferred {
		return 0 // already queued for confirmation — the obligation loop owns it now
	}
	if acct.final < 0 {
		executed, err := e.maker.Cancel(orderID)
		if err != nil {
			executed, err = e.maker.Cancel(orderID) // one retry: transient rpc blips are common
		}
		if err != nil {
			// The cancel would not confirm, so the truth must come from Status: cancelling
			// an already-filled order IS an error at the broker, and assuming executed=0
			// there loses real inventory.
			var terminal bool
			executed, terminal, err = e.maker.Status(orderID)
			if err != nil || !terminal {
				// No answer (or a PENDING_CANCEL still settling). The old engine halted here;
				// an unanswered question is not a reason to give up — queue the retirement
				// and WAIT: the obligation loop re-asks until the broker answers, however
				// long the outage lasts, and pair-hedges whatever the answer reveals.
				e.logf("order %s: cancel failed and status gave no terminal answer (err=%v terminal=%v) — deferring; will keep asking", orderID, err, terminal)
				e.deferRetire(orderID, acct)
				return 0
			}
		}
		e.finishRetire(orderID, acct, executed)
	}
	gap := acct.final - acct.folded
	if gap <= 0 {
		return 0
	}
	acct.folded = acct.final
	return gap
}

// finishRetire folds a broker-confirmed terminal executed count into a retired order's
// account: the placed-size clamp, the downward-contradiction check, and the sink credit
// for lots learned only from the ack. Shared by the synchronous retire and the deferred
// obligation loop.
func (e *Engine) finishRetire(orderID string, acct *ordAcct, executed int) {
	// The same placed-size ceiling OnFill enforces: a terminal executed count beyond the
	// order's placement size is a corrupt ack, and folding it would top up / hedge lots
	// that cannot exist at the broker.
	if acct.placed > 0 && executed > acct.placed {
		e.critical("%s terminal executed count %d exceeds its placed size %d — clamping (corrupt ack?)", orderID, executed, acct.placed)
		executed = acct.placed
	}
	acct.final = executed
	// The broker's terminal count can also contradict its own fill stream DOWNWARD: fewer
	// lots executed than the events the engine already acted on (and hedged). There is no
	// safe auto-repair — un-hedging on the say-so of a self-contradicting broker could
	// just as well strand a naked leg — so the book keeps the acted-on hedges and the
	// contradiction is only surfaced loudly; reconcile confirms which side was right.
	if acct.final < acct.folded {
		e.critical("%s terminal executed count %d is BELOW the %d lots already acted on — the broker contradicts its own fill stream; the book may be over-hedged by %d (reconcile will confirm)", orderID, acct.final, acct.folded, acct.folded-acct.final)
	}
	// Lots learned ONLY from the cancel-ack (no fill event yet): credit them to the sink
	// NOW, at the order's resting limit (exact for a passive), so the position the Decider
	// caps against moves with the broker instead of waiting on the fill stream. A stalled
	// stream otherwise leaves the cap checking a stale position while clips keep opening —
	// the 2026-07-16 over-cap incident. The late fill event then amends, never re-adds
	// (see OnFill's credited guard).
	if e.sink != nil && acct.final > acct.credited {
		e.sink.Fill(acct.sym, acct.isBuy, acct.final-acct.credited, acct.price)
		acct.credited = acct.final
	}
}
