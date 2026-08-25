package execengine

// Тесты котирования: следование за тачем (re-peg), троттлинг перестановок,
// подавление на протухшей и перевёрнутой книге. Зеркалит engine_quote.go.

import (
	"errors"
	"testing"
	"time"
)

// TestMinRestDelaysRepeg: an out-quoted leg is not chased until its current order has
// rested MinRest; the next book tick after the guarantee re-pegs it, and the re-place
// restarts the guarantee (the pull right after a re-peg is refused again).
func TestMinRestDelaysRepeg(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newEngineWith(EngineConfig{LegA: testLegA, LegB: testLegB, OrderVol: 2, HedgeRetries: 2, MinRest: 3 * time.Second}, m, tk, newTestDecider(2))
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	bidA := m.id(testLegA)

	// Out-quoted 1s in: inside the guarantee — the order must stay where it is.
	e.OnBook(testLegA, openHour.Add(time.Second), 101, 102)
	if m.count("cancel "+bidA) != 0 {
		t.Fatalf("re-peg inside MinRest must be skipped, got %v", m.calls)
	}

	// Same out-quote after the guarantee: now it chases the touch.
	e.OnBook(testLegA, openHour.Add(3*time.Second), 101, 102)
	if m.count("cancel "+bidA) != 1 {
		t.Fatalf("after MinRest the out-quoted leg must be re-pegged, got %v", m.calls)
	}

	// The re-place restarted the guarantee: a decayed signal right after may not pull it.
	e.PullIfUnwanted(holdState(openHour.Add(4 * time.Second)))
	if !e.Working() {
		t.Fatal("a just re-pegged order must get a fresh MinRest before the pull")
	}
	e.PullIfUnwanted(holdState(openHour.Add(6 * time.Second)))
	if e.Working() {
		t.Fatal("the re-pegged order's fresh MinRest has been served — pull must go through")
	}
}

func TestRepegFollowsTouchWhenOutQuoted(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour)) // bids leg A @ 100, asks leg B @ 51
	oldLegAID := m.id(testLegA)

	// Someone out-bids us on the leg A: best bid rises 100 → 100.5.
	e.OnBook(testLegA, openHour.Add(time.Second), 100.5, 101)

	if m.count("cancel "+oldLegAID) != 1 {
		t.Fatalf("the stale leg A bid should be cancelled, got %v", m.calls)
	}
	if e.clip.legA.price != 100.5 {
		t.Fatalf("leg A should be re-pegged to 100.5, got %.2f", e.clip.legA.price)
	}
	// Leg B was untouched, so its order id must be unchanged.
	if e.clip.legB.id != m.id(testLegB) {
		t.Fatal("leg B must not be re-pegged when only leg A moved")
	}
	if e.Position() != 0 {
		t.Fatalf("re-pegging must not change position, got %d", e.Position())
	}
}

func TestRepegSkippedWhenStillAtTouch(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	calls := len(m.calls)

	// Best bid unchanged (we are still the touch) and a lower second level — no re-peg.
	e.OnBook(testLegA, openHour.Add(time.Second), 100, 101)
	if len(m.calls) != calls {
		t.Fatalf("no re-peg while still at the touch, got %v", m.calls)
	}
}

func TestRepegThrottledPerLeg(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	e.cfg.RepegThrottle = time.Second
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))

	// First out-bid re-pegs to 100.5.
	e.OnBook(testLegA, openHour.Add(100*time.Millisecond), 100.5, 101)
	firstID := e.clip.legA.id
	// A second out-bid 200ms later is inside the 1s throttle — must be ignored.
	e.OnBook(testLegA, openHour.Add(300*time.Millisecond), 101, 102)
	if e.clip.legA.id != firstID || e.clip.legA.price != 100.5 {
		t.Fatalf("re-peg within the throttle window must be skipped, price=%.2f", e.clip.legA.price)
	}
	// Past the throttle window it re-pegs again.
	e.OnBook(testLegA, openHour.Add(2*time.Second), 101, 102)
	if e.clip.legA.price != 101 {
		t.Fatalf("re-peg after the throttle window should move to 101, got %.2f", e.clip.legA.price)
	}
}

func TestNoRepegAfterFirstFill(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	// Partial fill on leg A: makerID is now set.
	e.OnFill(openHour, m.id(testLegA), testLegA, true, 1, 100)
	calls := len(m.calls)

	// A better leg B ask appears, but re-peg is off once a leg has begun filling.
	e.OnBook(testLegB, openHour.Add(time.Second), 50, 49)
	if len(m.calls) != calls {
		t.Fatalf("no re-peg after the first fill, got %v", m.calls)
	}
}

func TestStaleBookSuppressesQuoting(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := newTestDecider(2)
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 2,
		FillTimeout: time.Minute, HedgeRetries: 2, MaxStaleness: time.Second,
	}, m, tk, dm)
	seedBooks(e, openHour) // books stamped at openHour

	e.OnState(buyState(openHour.Add(2 * time.Second))) // 2s later — stale
	if e.Working() {
		t.Fatal("must not quote against a stale book")
	}
}

// --- re-peg folds race fills ---

// A re-pegged order that filled during its cancel is a de-facto maker first-fill: fold
// it and complete the clip (spread semantics — once a leg begins filling, the clip
// completes); blindly re-posting the FULL clip size on top of it doubles the position.
func TestRepegFoldsRaceFillInsteadOfReposting(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 4)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour)) // buy clip: bid legA @100, ask legB @51
	bidA := m.id(testLegA)
	placesBefore := m.count("bid " + testLegA)

	m.executed = map[string]int{bidA: 4}                        // the bid fills entirely while the re-peg cancels it
	e.OnBook(testLegA, openHour.Add(time.Second), 100.5, 101.5) // better bid appears → re-peg fires

	if got := m.count("bid " + testLegA); got != placesBefore {
		t.Fatalf("must NOT re-post after the old order filled; bid places %d→%d", placesBefore, got)
	}
	// the clip completed: legB's remainder was taker-crossed for the full 4
	if got := tk.lots["sell "+testLegB]; got != 4 {
		t.Fatalf("legB completion lots=%d want 4 (calls %v)", got, tk.calls)
	}
	if e.Working() {
		t.Fatal("clip must be committed and cleared")
	}
	if e.Position() == 0 {
		t.Fatal("committed clip must move the decider position")
	}
}

// --- re-peg: deferred retry on a stuck order ---

// A stuck order discovered during a re-peg (cancel and status both fail) defers the
// retire and goes impaired; the re-peg path must then stop — never re-place on top of an
// order that may still rest — and the clip is pulled at the next handler entry.
func TestRepegStuckOrderDefersWithoutReplacing(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	legAID := m.id(testLegA)
	m.cancelErr = map[string]error{legAID: errors.New("rpc down")} // cancel fails; Status has no entry → error too

	// A better bid appears → leg A is out-quoted → re-peg tries to retire it and cannot
	// confirm the cancel.
	e.OnBook(testLegA, openHour.Add(time.Second), 100.5, 101)

	if e.Halted() {
		t.Fatal("the engine must never halt itself during a re-peg")
	}
	if !e.Impaired() {
		t.Fatal("an unconfirmable re-peg cancel must enter impaired mode")
	}
	if got := m.count("bid " + testLegA); got != 1 {
		t.Fatalf("nothing may be re-placed on top of an unconfirmed cancel, got %d bids (%v)", got, m.calls)
	}
	// The next handler entry pulls the clip ("убрать все ордера"): legB is cancelled, the
	// deferred legA keeps confirming in the obligation loop.
	e.OnTick(openHour.Add(2 * time.Second))
	if e.Working() {
		t.Fatal("impaired mode must pull the working clip")
	}
	if got := m.count("cancel " + m.id(testLegB)); got == 0 {
		t.Fatalf("the healthy leg must be cancelled on the impaired teardown (%v)", m.calls)
	}
}

// --- re-peg corners ---

// The ask side re-pegs too: a lower competing ask out-quotes the resting legB ask, which
// must follow the touch down (the main suite only exercises the bid side).
func TestRepegAskSideFollowsLowerAsk(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour)) // ask legB @ 51
	oldB := m.id(testLegB)

	e.OnBook(testLegB, openHour.Add(time.Second), 50, 50.5) // better ask appears

	if m.count("cancel "+oldB) != 1 {
		t.Fatalf("the out-quoted legB ask must be cancelled, got %v", m.calls)
	}
	if e.clip.legB.price != 50.5 {
		t.Fatalf("legB must re-peg to 50.5, got %.2f", e.clip.legB.price)
	}
	if e.clip.legA.id != m.id(testLegA) {
		t.Fatal("legA must be untouched by a legB re-peg")
	}
}

// A re-peg cancel that catches a PARTIAL race fill (2 of target 4) folds it as the maker's
// first fill and completes the clip: both legs land at target via minimal takers, and no
// replacement passive is posted on top of the fold.
func TestRepegPartialRaceFillCompletesClip(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 4)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	bidA := m.id(testLegA)
	bids := m.count("bid " + testLegA)

	m.executed = map[string]int{bidA: 2} // 2 of 4 fill during the re-peg's cancel
	e.OnBook(testLegA, openHour.Add(time.Second), 100.5, 101.5)

	if m.count("bid "+testLegA) != bids {
		t.Fatalf("no re-post on top of a folded race fill, got %v", m.calls)
	}
	if got := tk.lots["buy "+testLegA]; got != 2 {
		t.Fatalf("legA must be completed to target (4−2), got %d (%v)", got, tk.lots)
	}
	if got := tk.lots["sell "+testLegB]; got != 4 {
		t.Fatalf("legB must be completed to target (4−0), got %d (%v)", got, tk.lots)
	}
	if e.Working() || e.Position() != 4 {
		t.Fatalf("clip must be committed at target, working=%v pos=%d", e.Working(), e.Position())
	}
}

// A re-peg fold where the OTHER leg's teardown cancel also catches fills: each leg is
// topped to target once — no per-fill blind hedging, no doubling.
func TestRepegFoldWithBothLegsCaughtFills(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 4)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	bidA, askB := m.id(testLegA), m.id(testLegB)

	m.executed = map[string]int{bidA: 3, askB: 1} // both cancels catch in-flight fills
	e.OnBook(testLegA, openHour.Add(time.Second), 100.5, 101.5)

	if got := tk.lots["buy "+testLegA]; got != 1 {
		t.Fatalf("legA topped to target by 1 (4−3), got %d (%v)", got, tk.lots)
	}
	if got := tk.lots["sell "+testLegB]; got != 3 {
		t.Fatalf("legB topped to target by 3 (4−1), got %d (%v)", got, tk.lots)
	}
	if e.Working() || e.Halted() {
		t.Fatalf("clip must be committed cleanly, working=%v halted=%v", e.Working(), e.Halted())
	}
}

// When the limiter has no headroom the re-peg is skipped BEFORE the cancel is issued —
// a tight quota may never strand a leg mid re-peg (cancelled but not re-placed).
func TestRepegLimiterDenialSkipsBeforeCancel(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	lim := &stubLimiter{}
	e.SetLimiter(lim)
	lim.ok = true // allow the open
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	bidA := m.id(testLegA)

	lim.ok = false // quota exhausted before the re-peg
	e.OnBook(testLegA, openHour.Add(time.Second), 100.5, 101)

	if m.count("cancel "+bidA) != 0 {
		t.Fatalf("a denied re-peg must not cancel the resting order, got %v", m.calls)
	}
	if !e.Working() || e.clip.legA.price != 100 {
		t.Fatalf("the resting order must stay put, working=%v price=%.2f", e.Working(), e.clip.legA.price)
	}
}

// DisableRepeg (basis post-and-wait): an out-quoted leg stays exactly where it was posted.
func TestDisableRepegLeavesOutQuotedOrderResting(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := newTestDecider(2)
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 2, HedgeRetries: 2, DisableRepeg: true,
	}, m, tk, dm)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	calls := len(m.calls)

	e.OnBook(testLegA, openHour.Add(time.Second), 100.5, 101) // out-quoted

	if len(m.calls) != calls {
		t.Fatalf("DisableRepeg must leave the book untouched, got %v", m.calls)
	}
	if e.clip.legA.price != 100 {
		t.Fatalf("the order must keep its original price, got %.2f", e.clip.legA.price)
	}
}

// A crossed book (bid > ask — a broken or auction snapshot) is invalid: no clip may open
// against it, and no re-peg may chase it.
func TestCrossedBookSuppressesQuotingAndRepeg(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	e.OnBook(testLegA, openHour, 102, 101) // crossed
	e.OnBook(testLegB, openHour, 50, 51)

	e.OnState(buyState(openHour))
	if e.Working() || len(m.calls) != 0 {
		t.Fatalf("no clip may open against a crossed book, got %v", m.calls)
	}

	// Open against a sane book, then a crossed legA snapshot arrives mid-clip: no re-peg.
	e.OnBook(testLegA, openHour, 100, 101)
	e.OnState(buyState(openHour))
	calls := len(m.calls)
	e.OnBook(testLegA, openHour.Add(time.Second), 103, 102) // crossed again, "better" bid
	if len(m.calls) != calls {
		t.Fatalf("a crossed snapshot must not trigger a re-peg, got %v", m.calls)
	}
}

// --- OnBook edge cases: unknown symbols, throttle boundary, out-of-order and stale books ---

// A book update for an unknown symbol is ignored: it must neither satisfy canQuote nor
// disturb a working clip's re-peg bookkeeping.
func TestOnBookUnknownSymbolIgnored(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)

	e.OnBook("XX@YY", openHour, 100, 101) // valid prices, wrong symbol
	e.OnBook(testLegA, openHour, 100, 101)
	e.OnState(buyState(openHour))
	if e.Working() {
		t.Fatal("an unknown symbol's book must not make the missing leg quotable")
	}

	e.OnBook(testLegB, openHour, 50, 51)
	e.OnState(buyState(openHour))
	if !e.Working() {
		t.Fatal("clip did not open once both real legs had books")
	}
	places := m.n
	e.OnBook("XX@YY", openHour.Add(time.Second), 200, 201) // out-quotes nobody real
	if m.n != places {
		t.Fatalf("an unknown symbol must never trigger a re-peg, got calls %v", m.calls)
	}
}

// The re-peg throttle is a minimum GAP: a re-peg exactly at the throttle boundary (gap ==
// throttle) is allowed; only a strictly smaller gap is skipped.
func TestRepegAllowedExactlyAtThrottleBoundary(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour)) // bid legA @100

	t1 := openHour.Add(time.Second)
	e.OnBook(testLegA, t1, 100.5, 101.5) // out-quoted → re-peg #1
	if got := m.count("bid " + testLegA); got != 2 {
		t.Fatalf("first re-peg must fire, got %d bids (%v)", got, m.calls)
	}

	e.OnBook(testLegA, t1.Add(499*time.Millisecond), 101, 101.5) // inside the 500ms throttle → skipped
	if got := m.count("bid " + testLegA); got != 2 {
		t.Fatalf("a re-peg inside the throttle must be skipped, got %d bids (%v)", got, m.calls)
	}

	e.OnBook(testLegA, t1.Add(500*time.Millisecond), 101, 101.5) // exactly at the boundary → allowed
	if got := m.count("bid " + testLegA); got != 3 {
		t.Fatalf("a re-peg exactly at the throttle gap must fire, got %d bids (%v)", got, m.calls)
	}
}

// A book update older than the stored touch (stream reconnects re-deliver snapshots out
// of order) must be dropped: folding it would regress the touch to prices that no longer
// exist and quote/re-peg against them.
func TestOutOfOrderBookUpdateIgnored(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour) // legA 100/101 at openHour

	// A replayed snapshot from a second earlier carries a very different (stale) touch.
	e.OnBook(testLegA, openHour.Add(-time.Second), 200, 201)

	e.OnState(buyState(openHour))
	if got := m.count("bid " + testLegA + " 2 @ 100.00"); got != 1 {
		t.Fatalf("the clip must quote the NEWEST touch (100), not the replayed stale one (%v)", m.calls)
	}
}

// A dead market-data feed (books past MaxStaleness) pulls the resting orders — quotes
// priced off a dead book are blind — and trading resumes by itself once fresh books
// arrive. No halt, no operator: pull, wait for data, come back.
func TestStaleBooksPullOrdersAndResumeWithData(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := newTestDecider(2)
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 2, HedgeRetries: 2,
		MaxStaleness: 5 * time.Second, PullOnStaleBook: true,
	}, m, tk, dm)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	if !e.Working() {
		t.Fatal("clip did not open")
	}

	// The feed dies: only the 1s wall ticks keep arriving. Past MaxStaleness the resting
	// orders must be pulled — and nothing worse: no halt, no impaired, just waiting.
	e.OnTick(openHour.Add(6 * time.Second))
	if e.Working() {
		t.Fatal("stale books must pull the resting orders")
	}
	if e.Halted() || e.Impaired() {
		t.Fatalf("a dead feed is a waiting state, not a failure: halted=%v impaired=%v", e.Halted(), e.Impaired())
	}
	// Still no data → no new quotes (canQuote's staleness gate).
	e.OnState(buyState(openHour.Add(7 * time.Second)))
	if e.Working() {
		t.Fatal("no orders may be priced off a dead book")
	}

	// The feed returns: fresh books on both legs → the engine quotes again by itself.
	fresh := openHour.Add(10 * time.Second)
	seedBooks(e, fresh)
	e.OnState(buyState(fresh))
	if !e.Working() {
		t.Fatal("trading must resume by itself once fresh books arrive")
	}
}
