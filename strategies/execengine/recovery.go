// recovery holds the engine's degraded-operation state: the kill-switch, connection-
// trouble mode, reconcile-divergence tracking, and the queues of obligations retried
// while impaired. It is a plain state grouping, not a black-box component — every method
// that ACTS on this state also touches the clip, ledger or hedge state (enterImpaired is
// called from failure sites across all three; serviceImpaired drains the clip and both
// queues; Reconcile reads the clip and calls into hedge for the expected leg-B lots), so
// those transitions stay on Engine (engine_recovery.go) and read/write through
// e.recovery.* rather than flat fields. This type exists so ownership of the ten fields
// below is explicit and grep-able, not to hide them behind an interface the rest of the
// engine cannot see through — see CLAUDE.md's Engine-decomposition note for why.
package execengine

import "time"

type recovery struct {
	halted     bool // kill-switch: no new clips; resting passives pulled; position frozen
	suspect    bool // one reconcile pass diverged: new clips suspended until a clean pass clears it or a second divergence halts
	unverified bool // a broker response could not be tied to trackable state; cleared by one clean reconcile pass
	impaired   bool // connection-trouble mode: orders pulled, new clips suspended, obligations retried until the broker answers

	// takerDeadStreak counts consecutive own takers confirmed dead short of their placed
	// size. After takerDeadLimit the shortfall goes to the hedge-debt queue instead of
	// re-hedging at full speed.
	takerDeadStreak int

	// retireQ holds order ids whose retirement could not be confirmed (cancel failed and no
	// terminal status): each is retried every obligation cycle until the broker answers.
	retireQ []string

	// debts are taker hedges the engine OWES the book but could not place: retried every
	// obligation cycle until placed.
	debts []hedgeDebt

	nextRetryAt time.Time     // next obligation-retry time (impaired pacing)
	retryGap    time.Duration // current obligation-retry gap: placeBackoff() doubling to impairedRetryMax while nothing resolves

	// mismatchLogged: the persistent-reconcile-mismatch CRITICAL line has been emitted for
	// the current divergence episode (one loud line, not one per pass); cleared when broker
	// and internal positions agree again.
	mismatchLogged bool
}
