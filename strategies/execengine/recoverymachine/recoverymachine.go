// Package recoverymachine holds the engine's degraded-operation state: the kill-switch,
// connection-trouble mode, reconcile-divergence tracking, and the queues of obligations
// retried while impaired.
//
// This is a real, compiler-enforced component: mutation only ever happens through its own
// methods. The obligation-retry LOOP itself (iterating the retire/hedge queues, calling
// back into order placement and cancellation) stays on the engine — it is called-into,
// never calling back — since that loop needs machinery (the ledger, the broker adapter,
// the clip) this package deliberately knows nothing about. See
// strategies/execengine/CLAUDE.md's Engine-decomposition note for why some transitions
// cannot honestly live in a single component.
package recoverymachine

import (
	"fmt"
	"time"

	"QuantCore/modlog"
)

// mlog shares execengine's own log (logs/execengine.log) — the same stream Engine's own
// critical/warn/logf write to, so an operator sees these lines in context.
var mlog = modlog.For("execengine")

// HedgeDebt is one taker hedge the engine owes the book but has not managed to place.
type HedgeDebt struct {
	Sym  string
	Buy  bool
	Lots int
}

// Machine holds the engine's degraded-operation state.
type Machine struct {
	logTag string // stamped on every log line — see New

	halted     bool // kill-switch: no new clips; resting passives pulled; position frozen
	suspect    bool // one reconcile pass diverged: new clips suspended until a clean pass clears it or a second divergence halts
	unverified bool // a broker response could not be tied to trackable state; cleared by one clean reconcile pass
	impaired   bool // connection-trouble mode: orders pulled, new clips suspended, obligations retried until the broker answers

	// takerDeadStreak counts consecutive own takers confirmed dead short of their placed
	// size. After a caller-defined limit the shortfall goes to the hedge-debt queue instead
	// of re-hedging at full speed.
	takerDeadStreak int

	// retireQ holds order ids whose retirement could not be confirmed (cancel failed and no
	// terminal status): each is retried every obligation cycle until the broker answers.
	retireQ []string

	// debts are taker hedges the engine OWES the book but could not place: retried every
	// obligation cycle until placed.
	debts []HedgeDebt

	nextRetryAt time.Time     // next obligation-retry time (impaired pacing)
	retryGap    time.Duration // current obligation-retry gap: doubles to a caller-supplied ceiling while nothing resolves

	// mismatchLogged: the persistent-reconcile-mismatch CRITICAL line has been emitted for
	// the current divergence episode (one loud line, not one per pass); cleared when broker
	// and internal positions agree again.
	mismatchLogged bool
}

// New builds a Machine. logTag (e.g. "[basis_ema]") is stamped on every log line, matching
// EngineConfig.LogTag — pass "" for untagged.
func New(logTag string) *Machine {
	return &Machine{logTag: logTag}
}

func (m *Machine) logf(format string, args ...any) {
	mlog.Printf("[execengine]"+m.logTag+" "+format, args...)
}

func (m *Machine) warn(format string, args ...any) {
	mlog.Warn("[execengine]"+m.logTag+" "+format, args...)
}

func (m *Machine) critical(format string, args ...any) {
	mlog.Critical("[execengine]"+m.logTag+" "+format, args...)
}

// Halted reports whether the kill-switch is tripped.
func (m *Machine) Halted() bool { return m.halted }

// Impaired reports whether the engine is in connection-trouble mode.
func (m *Machine) Impaired() bool { return m.impaired }

// Suspect reports whether trading is suspended pending position confirmation: a reconcile
// divergence or an unconfirmable broker response. It clears by itself on a clean reconcile
// pass (ResumeFromSuspect / ClearUnverified).
func (m *Machine) Suspect() bool { return m.suspect || m.unverified }

// TakerDeadStreak reports the current count of consecutive own takers confirmed dead short.
func (m *Machine) TakerDeadStreak() int { return m.takerDeadStreak }

// NextRetryAt reports when the obligation loop should next retry queued work.
func (m *Machine) NextRetryAt() time.Time { return m.nextRetryAt }

// RetireQueue returns the current retire obligation queue, for the caller's own retry loop
// to iterate — see SetRetireQueue to replace it afterward.
func (m *Machine) RetireQueue() []string { return m.retireQ }

// Debts returns the current hedge-debt queue, mirroring RetireQueue.
func (m *Machine) Debts() []HedgeDebt { return m.debts }

// SetRetireQueue replaces the retire obligation queue — the caller's obligation loop keeps
// only what it could not resolve this pass.
func (m *Machine) SetRetireQueue(q []string) { m.retireQ = q }

// SetDebts replaces the hedge-debt queue, mirroring SetRetireQueue.
func (m *Machine) SetDebts(d []HedgeDebt) { m.debts = d }

// Halt trips the kill-switch: no new clips, and (the caller's responsibility) resting
// orders pulled. The position is left frozen for an operator to investigate deliberately.
// Idempotent; reports whether it just transitioned (false if already halted).
func (m *Machine) Halt(reason string, position int) bool {
	if m.halted {
		return false
	}
	m.halted = true
	m.critical("%s (position=%d)", reason, position)
	return true
}

// EnterImpaired switches into connection-trouble mode: no new clips, all resting orders
// pulled (the caller's responsibility, at its own safe point), and every outstanding
// obligation retried at a backing-off pace until the broker answers. It NEVER halts.
// Idempotent; a halted machine stays halted. backoff is the base retry gap the caller
// wants a fresh impaired episode to start at.
func (m *Machine) EnterImpaired(now time.Time, backoff time.Duration, reason string) {
	if m.halted || m.impaired {
		return
	}
	m.impaired = true
	m.retryGap = backoff
	m.nextRetryAt = now.Add(m.retryGap)
	m.warn("%s — pulling all orders, suspending new clips; retrying outstanding obligations until the broker answers", reason)
}

// DeferHedge queues a taker hedge that could not be placed and enters impaired mode. The
// debt is NOT credited anywhere here — credits belong to real orders only, applied by the
// caller once actually placed.
func (m *Machine) DeferHedge(sym string, buy bool, lots int, now time.Time, backoff time.Duration) {
	m.debts = append(m.debts, HedgeDebt{Sym: sym, Buy: buy, Lots: lots})
	m.EnterImpaired(now, backoff, fmt.Sprintf("hedge %s (buy=%v x%d) could not be placed", sym, buy, lots))
}

// QueueRetire queues orderID for retry and enters impaired mode. The caller is responsible
// for its own dedup against re-queuing the same order twice (see execengine's ordAcct.deferred).
func (m *Machine) QueueRetire(orderID string, now time.Time, backoff time.Duration) {
	m.retireQ = append(m.retireQ, orderID)
	m.EnterImpaired(now, backoff, fmt.Sprintf("order %s: cancel failed and no terminal status yet", orderID))
}

// AdvancePace re-paces the next obligation retry after one impaired-mode cycle: on progress
// the gap resets to baseBackoff; on a stall it doubles, capped at maxGap.
func (m *Machine) AdvancePace(now time.Time, progressed bool, baseBackoff, maxGap time.Duration) {
	if progressed {
		m.retryGap = baseBackoff
	} else if m.retryGap *= 2; m.retryGap > maxGap {
		m.retryGap = maxGap
	}
	m.nextRetryAt = now.Add(m.retryGap)
}

// ClearImpaired marks every obligation confirmed: impaired mode ends, but one clean
// reconcile pass must still confirm the position before new clips resume.
func (m *Machine) ClearImpaired() {
	m.impaired = false
	m.unverified = true // one clean reconcile pass must confirm the position before trading
	m.retryGap = 0
	m.logf("RECOVERED: every deferred cancel and hedge is confirmed — waiting for a clean reconcile before opening clips")
}

// MarkUnverified flags that a broker response could not be tied to trackable state (an
// empty or reused order id) — a clean reconcile pass must confirm the position before new
// clips resume.
func (m *Machine) MarkUnverified() { m.unverified = true }

// ResetDeadStreak clears the consecutive-dead-taker counter (a taker confirmed fully
// executed, or the streak's shortfall was already handed to the debt queue).
func (m *Machine) ResetDeadStreak() { m.takerDeadStreak = 0 }

// IncrementDeadStreak counts one more taker confirmed dead short of its placed size and
// returns the new streak length.
func (m *Machine) IncrementDeadStreak() int {
	m.takerDeadStreak++
	return m.takerDeadStreak
}

// ResumeFromSuspect clears a healed reconcile divergence (broker and internal positions
// agree again after at least one suspect pass). A no-op if not currently suspect.
func (m *Machine) ResumeFromSuspect(pos int) {
	if m.suspect {
		m.logf("reconcile: broker and internal positions agree again (pos=%d) — resuming", pos)
		m.suspect = false
		m.mismatchLogged = false
	}
}

// ClearUnverified confirms an unconfirmable broker response was actually harmless (broker
// and internal positions agree on a clean reconcile pass). A no-op if not unverified.
func (m *Machine) ClearUnverified(pos int) {
	if m.unverified {
		m.logf("reconcile: broker and internal positions agree (pos=%d) — unverified broker responses confirmed harmless, resuming", pos)
		m.unverified = false
	}
}

// MarkSuspect flags a reconcile divergence — new clips suspended pending confirmation.
// Reports whether this is the FIRST divergent pass (false if already suspect, meaning this
// is at least the second consecutive divergence — the caller should treat it as real via
// LogPersistentMismatch).
func (m *Machine) MarkSuspect(legAActual, wantA, legBActual, wantB int) bool {
	if m.suspect {
		return false
	}
	m.suspect = true
	m.warn("reconcile: legA have=%d want=%d, legB have=%d want=%d — possibly an in-flight fill; new clips suspended until the next reconcile confirms",
		legAActual, wantA, legBActual, wantB)
	return true
}

// LogPersistentMismatch logs the loud CRITICAL for a divergence that survived a second
// reconcile pass — once per episode (cleared by the next ResumeFromSuspect).
func (m *Machine) LogPersistentMismatch(legAActual, wantA, legBActual, wantB int) {
	if m.mismatchLogged {
		return
	}
	m.mismatchLogged = true
	m.critical("POSITION MISMATCH persisted across two reconciles: legA have=%d want=%d, legB have=%d want=%d — new clips suspended until broker and internal positions agree again (position frozen, not auto-repaired; investigate if this does not clear)",
		legAActual, wantA, legBActual, wantB)
}
