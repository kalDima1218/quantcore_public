package execengine

// Тесты восстановления: сверка позиции с брокером (Reconcile), suspend/resume,
// impaired-режим и отложенные операции. Зеркалит engine_recovery.go.

import (
	"errors"
	"testing"
	"time"
)

// A persistent leg mismatch SUSPENDS trading (the halt's protection) but never halts:
// reconcile keeps comparing every pass, and the moment broker and book agree again the
// engine resumes by itself — a delayed fill or a reversed outside transfer needs no
// operator restart.
func TestReconcileMismatchSuspendsAndAutoResumes(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	e.OnFill(openHour, m.id(testLegA), testLegA, true, 2, 100) // position +2

	e.Reconcile(2, -2) // broker agrees
	if e.Halted() || e.Suspect() {
		t.Fatal("matching positions must not suspend")
	}
	e.Reconcile(2, 0) // leg B diverges — could be an in-flight fill, so first pass only warns
	if e.Halted() {
		t.Fatal("a single divergence must not halt (an async taker fill may still be in flight)")
	}
	e.Reconcile(2, 0) // still diverged on the next pass — a real desync
	if e.Halted() {
		t.Fatal("the engine must never halt itself on a mismatch — it suspends and keeps checking")
	}
	if !e.Suspect() {
		t.Fatal("a persistent mismatch must suspend new clips")
	}
	e.OnState(buyState(openHour.Add(time.Second)))
	if e.Working() {
		t.Fatal("no clips may open on a doubted position")
	}

	// The divergence heals (the delayed legB fill finally lands at the broker): the next
	// pass agrees and trading resumes without any operator involvement.
	e.Reconcile(2, -2)
	if e.Suspect() {
		t.Fatal("an agreeing pass must clear the suspension")
	}
	e.OnState(buyState(openHour.Add(2 * time.Second)))
	if !e.Working() {
		t.Fatal("trading must resume once broker and book agree")
	}
}

// TestReconcileTransientMismatchSuspendsThenClears pins the two-pass reconcile rule: the
// first divergent pass must suspend NEW clips (the position view is in doubt) without
// halting, and a clean pass — the in-flight fill has landed and both views agree again —
// must clear the suspension so trading resumes.
func TestReconcileTransientMismatchSuspendsThenClears(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)

	e.Reconcile(2, -2) // broker briefly ahead of the fill stream (engine flat)
	if e.Halted() {
		t.Fatal("a single divergence must not halt")
	}
	e.OnState(buyState(openHour))
	if e.Working() {
		t.Fatal("no new clips may open while a reconcile divergence is unconfirmed")
	}
	e.Reconcile(0, 0) // next pass agrees — transient resolved
	if e.Halted() {
		t.Fatal("a resolved divergence must not halt")
	}
	e.OnState(buyState(openHour.Add(time.Second)))
	if !e.Working() {
		t.Fatal("trading must resume after a clean reconcile pass")
	}
}

// A divergence that CHANGES shape between the two passes is still a persistent divergence:
// the second divergent pass confirms it is real. The engine stays SUSPENDED (no new clips,
// position frozen) but never halts — reconcile keeps checking and resumes on agreement.
func TestReconcileShiftingDivergenceStaysSuspended(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)

	e.Reconcile(2, -2) // engine flat, broker says ±2
	if e.Halted() || !e.Suspect() {
		t.Fatal("first divergence only suspends")
	}
	e.Reconcile(1, 0) // still wrong, differently
	if e.Halted() {
		t.Fatal("the engine must never halt itself on a mismatch")
	}
	if !e.Suspect() {
		t.Fatal("the confirmed divergence must keep trading suspended")
	}
	e.OnState(buyState(openHour))
	if e.Working() {
		t.Fatal("no clips may open while the position is doubted")
	}
	e.Reconcile(0, 0) // the account is squared outside — data agrees again
	if e.Suspect() {
		t.Fatal("an agreeing pass must lift the suspension")
	}
}

// A halt is permanent: an agreeing reconcile pass afterwards must not resurrect trading.
func TestReconcileAgreementAfterHaltStaysHalted(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.Halt("test")

	e.Reconcile(0, 0) // broker agrees with the (flat) position

	if !e.Halted() {
		t.Fatal("an agreeing pass must never clear a halt")
	}
	e.OnState(buyState(openHour))
	if e.Working() {
		t.Fatal("a halted engine must not trade after an agreeing reconcile")
	}
}

// Reconciling while a clip works must be a no-op: mid-clip the legs are legitimately in
// flight, and two divergent passes would otherwise halt a perfectly healthy clip.
func TestReconcileSkippedWhileClipWorking(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	if !e.Working() {
		t.Fatal("clip did not open")
	}

	e.Reconcile(9, -3) // wildly diverged — but a clip is working
	e.Reconcile(9, -3) // twice: would halt if the pass were folded

	if e.Halted() {
		t.Fatal("reconcile must be skipped while a clip works")
	}
	if !e.Working() {
		t.Fatal("the working clip must be untouched")
	}

	// And no suspect flag may linger from the skipped passes: after the clip completes,
	// the engine quotes again immediately.
	e.OnFill(openHour, m.id(testLegA), testLegA, true, 2, 100) // completes and commits
	e.OnState(buyState(openHour.Add(time.Second)))
	if !e.Working() {
		t.Fatal("a skipped mid-clip reconcile must not suspend future clips")
	}
}

// Reconciling a halted engine is a no-op: the book is frozen for the operator, and the
// pass must neither log a bogus "resuming" nor re-halt.
func TestReconcileNoOpWhileHalted(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.Halt("test")

	e.Reconcile(5, 0)
	e.Reconcile(5, 0)

	if !e.Halted() {
		t.Fatal("halted stays halted")
	}
	e.OnState(buyState(openHour))
	if e.Working() {
		t.Fatal("no trading after a halt regardless of reconcile calls")
	}
}

// Diverge → agree → diverge: the second divergence starts a FRESH two-pass cycle (suspend,
// don't halt) — the agreement in between proved the first one transient.
func TestReconcileDivergeAgreeDivergeOnlySuspends(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)

	e.Reconcile(2, -2) // diverged (engine flat) → suspect
	e.Reconcile(0, 0)  // agrees → cleared
	e.Reconcile(2, -2) // diverged again → a fresh first pass, not the fatal second

	if e.Halted() {
		t.Fatal("a divergence separated by an agreeing pass must not halt")
	}
	e.OnState(buyState(openHour))
	if e.Working() {
		t.Fatal("the fresh divergence must still suspend new clips")
	}
}

// Three consecutive takers confirmed dead short of their size mean the broker keeps
// accepting and killing hedges. The engine must NOT halt (autonomy): the shortfall
// becomes a hedge debt paid at the impaired backoff pace, and once a hedge finally
// sticks the engine recovers by itself through a clean reconcile.
func TestConsecutiveDeadTakersGoImpairedThenRecover(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	tak := completeBuyClip(t, e, m, tk)

	for i := 0; i < takerDeadLimit; i++ {
		if e.Halted() || e.Impaired() {
			t.Fatalf("dead taker %d/%d: too early for impaired/halt", i, takerDeadLimit)
		}
		m.status = map[string]int{tak: 0}
		e.OnOrderStatus(tak, true)
		tak = tk.lastID
	}
	if e.Halted() {
		t.Fatal("the engine must never halt itself — dead-taker streaks go impaired")
	}
	if !e.Impaired() {
		t.Fatalf("%d consecutive dead takers must enter impaired mode", takerDeadLimit)
	}
	if got := tk.lots["sell "+testLegB]; got != 6 {
		t.Fatalf("immediate re-hedges before the streak limit: sold %d on legB, want 6", got)
	}

	// The obligation loop pays the debt once its backoff elapses; the broker accepts and
	// (this time) does not kill it — the engine recovers into the unverified wait.
	e.OnTick(openHour.Add(10 * time.Second))
	if e.Impaired() {
		t.Fatal("a paid hedge debt must clear impaired mode")
	}
	if got := tk.lots["sell "+testLegB]; got != 8 {
		t.Fatalf("the debt must be re-placed: sold %d on legB, want 8", got)
	}
	e.Reconcile(2, -2)
	e.OnState(buyState(openHour.Add(11 * time.Second)))
	if got := m.count("bid "); got != 2 {
		t.Fatalf("after recovery + clean reconcile a new clip must open, got %d bid placements", got)
	}
}

// An empty taker order id cannot be tracked or confirmed: the position is unverified, so
// new clips wait until a clean reconcile pass confirms broker and internal positions agree.
func TestEmptyTakerIDWaitsForCleanReconcile(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	tk.forceIDs = []string{""} // the hedge's placement answers with no id
	completeBuyClipNoID(t, e, m)

	next := openHour.Add(2 * time.Second)
	before := m.count("bid ")
	e.OnState(buyState(next))
	if got := m.count("bid "); got != before {
		t.Fatal("no new clip may open while the position is unverified")
	}

	e.Reconcile(2, -2) // the broker confirms the blind-credited hedge really exists
	e.OnState(buyState(next.Add(time.Second)))
	if got := m.count("bid "); got != before+1 {
		t.Fatal("a clean reconcile pass must clear the unverified state")
	}
}

// An empty MAKER order id may leave an untracked order resting at the broker: the same
// unverified wait applies until reconcile confirms the book.
func TestEmptyMakerIDWaitsForCleanReconcile(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	m.forceID = map[string]string{testLegA: ""}
	e.OnState(buyState(openHour)) // placement "succeeds" with an empty id → treated as failed
	if e.Working() {
		t.Fatal("an empty-id placement must not leave a working clip")
	}

	// Even past the failure backoff, no new clip: the ghost order is unconfirmed.
	later := openHour.Add(time.Minute)
	e.OnState(buyState(later))
	if got := m.count("bid "); got != 1 {
		t.Fatalf("unverified position must suppress new clips, got %d bid placements", got)
	}

	e.Reconcile(0, 0) // the broker confirms nothing rests
	e.OnState(buyState(later.Add(time.Second)))
	if got := m.count("bid "); got != 2 {
		t.Fatalf("a clean reconcile must resume clip opens, got %d bid placements", got)
	}
}

// A terminal order-status for a DEFERRED retire resolves the obligation immediately: the
// status stream answered while the unary RPCs were down — cross-stream redundancy, no
// waiting out the retry backoff.
func TestDeadStatusResolvesDeferredRetireImmediately(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	bidA := m.id(testLegA)

	m.cancelErr = map[string]error{bidA: errors.New("rpc down")} // no status entry → Status errors too
	e.OnState(holdState(openHour.Add(time.Second)))              // abandon → legA retire deferred
	if !e.Impaired() {
		t.Fatal("the unanswered cancel must be deferred into impaired mode")
	}

	// The order-status stream (an independent connection) delivers the terminal state, and
	// by now Status answers too: the order died having executed 1 lot.
	m.status = map[string]int{bidA: 1}
	e.OnOrderStatus(bidA, true)

	if got := tk.lots["sell "+testLegB]; got != 1 {
		t.Fatalf("the surfaced lot must be pair-hedged at once, sold %d on legB want 1 (%v)", got, tk.calls)
	}
	e.OnTick(openHour.Add(3 * time.Second)) // the obligation loop confirms the queue is clear
	if e.Impaired() {
		t.Fatal("the resolved obligation must clear impaired mode")
	}
}

// TestHedgeRatioReconcileExpectsScaledLegB: the broker reports each leg in its OWN
// contracts, so a position of P LegA lots is −P×R on LegB. Comparing against −P would read
// a correctly hedged book as a divergence and suspend trading forever.
func TestHedgeRatioReconcileExpectsScaledLegB(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newSoloRatioTestEngine(m, tk, 2, 10)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	e.OnFill(openHour, m.id(testLegA), testLegA, true, 2, 100)
	if e.Position() != 2 {
		t.Fatalf("setup: expected a +2 position, got %d", e.Position())
	}

	e.Reconcile(2, -20)

	if e.Suspect() {
		t.Fatal("legA=+2 / legB=−20 IS the balanced book at ratio 10 — reconcile must agree")
	}
}

// TestHedgeRatioReconcileFlagsUnscaledLegB: the mirror — a LegB position of −P (1:1) against
// a ratio-10 book is a real R-fold under-hedge and must be caught, not accepted.
func TestHedgeRatioReconcileFlagsUnscaledLegB(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newSoloRatioTestEngine(m, tk, 2, 10)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	e.OnFill(openHour, m.id(testLegA), testLegA, true, 2, 100)

	e.Reconcile(2, -2)

	if !e.Suspect() {
		t.Fatal("an unscaled LegB position must be flagged as a divergence")
	}
}
