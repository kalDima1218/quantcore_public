package execengine

// Тесты жизненного цикла клипа: открытие двух пассивов, таймаут, keep-partial,
// counterpart-ahead, abandon/catch, снятие нежеланного клипа (PullIfUnwanted),
// разбор при остановке. Зеркалит engine_clip.go.

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// countAttempts reports how many of attempts (fakeTaker's "sym:lots" log) target sym.
func countAttempts(attempts []string, sym string) int {
	n := 0
	for _, a := range attempts {
		if strings.HasPrefix(a, sym+":") {
			n++
		}
	}
	return n
}

// --- opening a clip: dual-passive placement at touch, the sell side, and solo-maker mode (placement, then hedge-on-fill) ---

func TestOpenClipPlacesDualPassivesAtTouch(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)

	e.OnState(buyState(openHour))

	if !e.Working() {
		t.Fatal("expected a working clip after a buy signal")
	}
	want := map[string]bool{
		"bid SI 2 @ 100.00": true, // buy leg A at its bid
		"ask UF 2 @ 51.00":  true, // sell leg B at its ask
	}
	for _, c := range m.calls {
		delete(want, c)
	}
	if len(want) != 0 {
		t.Fatalf("missing passive placements %v; got calls %v", want, m.calls)
	}
	if e.Position() != 0 {
		t.Fatalf("position must stay 0 until a fill lands, got %d", e.Position())
	}
}

func TestSellOpensOppositeSides(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)

	e.OnState(sellState(openHour))

	want := map[string]bool{
		"ask SI 2 @ 101.00": true, // sell leg A at its ask
		"bid UF 2 @ 50.00":  true, // buy leg B at its bid
	}
	for _, c := range m.calls {
		delete(want, c)
	}
	if len(want) != 0 {
		t.Fatalf("missing sell-side placements %v; got %v", want, m.calls)
	}

	legAID := m.id(testLegA)
	e.OnFill(openHour, legAID, testLegA, false, 2, 101) // sold leg A
	if tk.lots["buy "+testLegB] != 2 {
		t.Fatalf("expected taker buy of leg B x2, got %v", tk.lots)
	}
	if e.Position() != -2 {
		t.Fatalf("position should be -2 after a short, got %d", e.Position())
	}
}

// TestSoloMakerLegPlacesOnlyLegA: in single-passive mode a decided move posts ONLY the LegA
// passive. LegB is never rested in the book — it will be taker-hedged once LegA fills.
func TestSoloMakerLegPlacesOnlyLegA(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newSoloTestEngine(m, tk, 2)
	seedBooks(e, openHour)

	e.OnState(buyState(openHour)) // long: bid leg A (SI); leg B (UF) would be an ask

	if !e.Working() {
		t.Fatal("expected a working clip after a buy signal")
	}
	if got := m.count("bid " + testLegA); got != 1 {
		t.Fatalf("LegA should be posted once as a maker bid, got %d (calls %v)", got, m.calls)
	}
	if n := m.count("bid "+testLegB) + m.count("ask "+testLegB); n != 0 {
		t.Fatalf("LegB must NOT be posted in solo mode, got %d placements (calls %v)", n, m.calls)
	}
	if e.Position() != 0 {
		t.Fatalf("position must stay 0 until a fill lands, got %d", e.Position())
	}
}

// TestSoloMakerLegHedgesLegBOnFill: once the sole LegA passive fills, the engine crosses the
// LegB book with a taker for the same lots — a balanced pair, no naked leg — the same
// completion path dual-passive runs when LegA happens to win the fill race.
func TestSoloMakerLegHedgesLegBOnFill(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newSoloTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	legAID := m.id(testLegA)

	e.OnFill(openHour, legAID, testLegA, true, 2, 100) // the sole maker leg fills fully

	if tk.lots["sell "+testLegB] != 2 {
		t.Fatalf("LegB should be taker-hedged with a 2-lot sell (a long-LegA clip is short LegB), got taker calls %v", tk.calls)
	}
	if e.Working() {
		t.Fatal("clip should have committed after the sole maker leg filled fully")
	}
	if e.Position() != 2 {
		t.Fatalf("committed pair should be +2, got %d", e.Position())
	}
}

// TestTakerOnlyPlacesNoMakerLegs: in taker-only mode a decided move never rests a passive
// order on either leg — both legs cross the book as market orders immediately.
func TestTakerOnlyPlacesNoMakerLegs(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTakerOnlyTestEngine(m, tk, 2)
	seedBooks(e, openHour)

	e.OnState(buyState(openHour))

	if len(m.calls) != 0 {
		t.Fatalf("taker-only must never place a maker order, got calls %v", m.calls)
	}
	if e.Working() {
		t.Fatal("taker-only clip completes synchronously — no working clip should remain")
	}
}

// TestTakerOnlyExecutesBothLegsAsTaker: both legs cross as takers on the correct sides for
// a buy (bid leg A / ask leg B) and a sell (the mirror), and the Decider's intent is
// committed immediately — position moves without waiting for a fill event.
func TestTakerOnlyExecutesBothLegsAsTaker(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTakerOnlyTestEngine(m, tk, 2)
	seedBooks(e, openHour)

	e.OnState(buyState(openHour))

	if tk.lots["buy "+testLegA] != 2 {
		t.Fatalf("expected taker buy of leg A x2, got %v", tk.lots)
	}
	if tk.lots["sell "+testLegB] != 2 {
		t.Fatalf("expected taker sell of leg B x2, got %v", tk.lots)
	}
	if e.Position() != 2 {
		t.Fatalf("position should commit to +2 immediately in taker-only mode, got %d", e.Position())
	}
}

func TestTakerOnlySellCrossesOppositeSides(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTakerOnlyTestEngine(m, tk, 2)
	seedBooks(e, openHour)

	e.OnState(sellState(openHour))

	if tk.lots["sell "+testLegA] != 2 {
		t.Fatalf("expected taker sell of leg A x2, got %v", tk.lots)
	}
	if tk.lots["buy "+testLegB] != 2 {
		t.Fatalf("expected taker buy of leg B x2, got %v", tk.lots)
	}
	if e.Position() != -2 {
		t.Fatalf("position should commit to -2 immediately in taker-only mode, got %d", e.Position())
	}
}

// TestTakerOnlyOnTakerFailureQueuesDebt verifies that when the taker placement itself
// fails (both HedgeRetries attempts fail for both legs), the Decider's Commit still fires
// (position is committed immediately), and the failed hedges become debts queued for later
// retry while the engine enters impaired mode.
func TestTakerOnlyOnTakerFailureQueuesDebt(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{fail: true} // taker will fail all attempts
	e := newTakerOnlyTestEngine(m, tk, 2)
	seedBooks(e, openHour)

	e.OnState(buyState(openHour))

	// Both takerRetry attempts fail (HedgeRetries=2), so both legs' hedges become debts
	// and the engine enters impaired mode.
	if !e.Impaired() {
		t.Fatal("failed taker-only hedges must queue debts and enter impaired mode")
	}
	if e.Halted() {
		t.Fatal("the engine must not halt on impaired entry")
	}
	if e.Working() {
		t.Fatal("taker-only clip must not stay working — it completed synchronously")
	}

	// The critical invariant: Commit fired UNCONDITIONALLY despite both legs' taker failures.
	// The position reflects the intent that was committed immediately.
	if e.Position() != 2 {
		t.Fatalf("Commit must fire regardless of taker placement success, position should be +2, got %d", e.Position())
	}

	// No takers landed (both failed to place).
	if tk.lots["buy "+testLegA] != 0 {
		t.Fatalf("legA taker must not have landed (all attempts failed), got %d (%v)", tk.lots["buy "+testLegA], tk.lots)
	}
	if tk.lots["sell "+testLegB] != 0 {
		t.Fatalf("legB taker must not have landed (all attempts failed), got %d (%v)", tk.lots["sell "+testLegB], tk.lots)
	}
}

// TestTakerOnlyOnOneLegFailureKeepsOtherLegLanded verifies the asymmetric case: legA's
// taker lands for real (a naked position on the exchange) while legB's taker fails every
// attempt. legA must NOT be retried or duplicated to compensate — it already landed — and
// only legB's lots become a hedge debt while the engine goes impaired.
func TestTakerOnlyOnOneLegFailureKeepsOtherLegLanded(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	tk.failSym = map[string]bool{testLegB: true} // legA taker succeeds, legB taker fails
	e := newTakerOnlyTestEngine(m, tk, 2)
	seedBooks(e, openHour)

	e.OnState(buyState(openHour))

	// legA landed for real — a genuine naked position on the exchange.
	if tk.lots["buy "+testLegA] != 2 {
		t.Fatalf("legA taker must have landed (it never fails), got %d (%v)", tk.lots["buy "+testLegA], tk.lots)
	}
	// legB never landed — all attempts failed.
	if tk.lots["sell "+testLegB] != 0 {
		t.Fatalf("legB taker must not have landed (all attempts failed), got %d (%v)", tk.lots["sell "+testLegB], tk.lots)
	}
	if !e.Impaired() {
		t.Fatal("legB's failed hedge must queue a debt and enter impaired mode")
	}
	if e.Halted() {
		t.Fatal("the engine must not halt on impaired entry")
	}
	// Commit still fires unconditionally: the Decider's intent is +2 regardless of which
	// leg's taker placement failed.
	if e.Position() != 2 {
		t.Fatalf("Commit must fire regardless of taker placement success, position should be +2, got %d", e.Position())
	}

	// legB's taker recovers: the obligation loop pays the owed debt without re-touching legA.
	tk.failSym = nil
	e.OnTick(openHour.Add(5 * time.Second))
	if e.Impaired() {
		t.Fatal("a paid debt must clear impaired mode")
	}
	if got := tk.lots["sell "+testLegB]; got != 2 {
		t.Fatalf("the owed legB lots must be paid on recovery, got %d sells (%v)", got, tk.calls)
	}
	if got := tk.lots["buy "+testLegA]; got != 2 {
		t.Fatalf("legA must not be re-hedged or duplicated once its taker already landed, got %d buys (%v)", got, tk.calls)
	}
}

// TestTakerOnlyDispatchesBothLegsConcurrently proves the two legs' first taker attempt
// races the wire instead of serializing: barrierTaker's Buy/Sell only return once BOTH
// have been called, so a sequential dispatch (leg A's call awaited before leg B's starts)
// deadlocks against it and this test times out.
func TestTakerOnlyDispatchesBothLegsConcurrently(t *testing.T) {
	m, tk := &fakeMaker{}, newBarrierTaker()
	e := newTakerOnlyTestEngine(m, tk, 2)
	seedBooks(e, openHour)

	done := make(chan struct{})
	go func() {
		e.OnState(buyState(openHour))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("OnState did not return — the two legs' first taker attempt must be dispatched concurrently, not sequentially (a sequential dispatch deadlocks against barrierTaker)")
	}
	if e.Position() != 2 {
		t.Fatalf("position should commit to +2 once both legs land, got %d", e.Position())
	}
}

// --- clip sizing: Intent.Lots vs OrderVol, including the zero-volume no-op ---

func TestClipSizeComesFromIntentLots(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: 1, Lots: 3}} // wants a 3-lot clip
	e := newEngineWith(EngineConfig{LegA: testLegA, LegB: testLegB, OrderVol: 10, HedgeRetries: 2}, m, tk, dm)
	seedBooks(e, openHour)

	e.OnState(RowState{Time: openHour})

	// The clip must be sized to Intent.Lots (3), NOT the fixed OrderVol (10).
	if m.count("bid SI 3 @") != 1 || m.count("ask UF 3 @") != 1 {
		t.Fatalf("clip must use Intent.Lots=3, got calls %v", m.calls)
	}
}

func TestIntentLotsZeroFallsBackToOrderVol(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: 1}} // Lots 0 → engine uses OrderVol
	e := newEngineWith(EngineConfig{LegA: testLegA, LegB: testLegB, OrderVol: 2, HedgeRetries: 2}, m, tk, dm)
	seedBooks(e, openHour)

	e.OnState(RowState{Time: openHour})

	if m.count("bid SI 2 @") != 1 {
		t.Fatalf("Lots=0 must fall back to OrderVol=2, got calls %v", m.calls)
	}
}

// With OrderVol and Intent.Lots both zero there is nothing to size a clip with: nothing
// may be placed (and nothing consumed from the limiter path via a placement attempt).
func TestZeroVolumePlacesNothing(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: 1, Lots: 0}}
	e := newEngineWith(EngineConfig{LegA: testLegA, LegB: testLegB, OrderVol: 0, HedgeRetries: 1}, m, tk, dm)
	seedBooks(e, openHour)

	e.OnState(RowState{Time: openHour})

	if e.Working() || m.n != 0 {
		t.Fatalf("a zero-sized clip must not place, working=%v calls=%v", e.Working(), m.calls)
	}
}

// --- fill-timeout: the deadline boundary, no-fill/partial-fill outcomes, a cancel-race fold, and the disabled backstop ---

func TestTimeoutWithNoFillCancelsAndKeepsFlat(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	legAID, legBID := m.id(testLegA), m.id(testLegB)

	e.OnTick(openHour.Add(3 * time.Minute)) // past the 2-minute fill timeout

	if e.Working() {
		t.Fatal("clip should be cancelled after the fill timeout")
	}
	if m.count("cancel "+legAID) != 1 || m.count("cancel "+legBID) != 1 {
		t.Fatalf("both passives should be cancelled, got %v", m.calls)
	}
	if len(tk.calls) != 0 {
		t.Fatalf("no taker orders on a no-fill timeout, got %v", tk.calls)
	}
	if e.Position() != 0 {
		t.Fatalf("position must stay flat, got %d", e.Position())
	}
}

func TestPartialFillThenTimeoutTakerCompletes(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	legAID := m.id(testLegA)

	e.OnFill(openHour, legAID, testLegA, true, 1, 100) // only 1 of 2 fills
	if !e.Working() {
		t.Fatal("clip should still be working after a partial fill")
	}
	e.OnTick(openHour.Add(3 * time.Minute)) // timeout

	// The 1 already-hedged lot (taker sell leg B x1) plus the taker-completed remainder
	// (buy leg A x1 + sell leg B x1) leave the whole lot realized.
	if tk.lots["sell "+testLegB] != 2 {
		t.Fatalf("expected leg B sold x2 total, got %v", tk.lots)
	}
	if tk.lots["buy "+testLegA] != 1 {
		t.Fatalf("expected leg A remainder bought x1, got %v", tk.lots)
	}
	if e.Working() {
		t.Fatal("clip should be committed after taker completion")
	}
	if e.Position() != 2 {
		t.Fatalf("position should be +2 (one whole lot), got %d", e.Position())
	}
}

// The fill-timeout must fire exactly AT the deadline (ts == deadline), not one tick later.
func TestTimeoutFiresExactlyAtDeadline(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2) // FillTimeout 2m
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))

	e.OnTick(openHour.Add(2*time.Minute - time.Nanosecond))
	if !e.Working() {
		t.Fatal("the clip must still be working just before the deadline")
	}
	e.OnTick(openHour.Add(2 * time.Minute))
	if e.Working() {
		t.Fatal("the timeout must fire exactly at the deadline")
	}
}

// Timeout completion whose maker cancel catches an extra in-flight fill: the fold happens
// once, the pair lands at target, and the late fill event for the caught lot dedups.
func TestTimeoutCompletionFoldsMakerCancelRaceFill(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	bidA := m.id(testLegA)

	e.OnFill(openHour, bidA, testLegA, true, 1, 100) // partial: 1 of 2, hedged on legB
	m.executed = map[string]int{bidA: 2}             // the 2nd lot fills during the timeout's cancel
	e.OnTick(openHour.Add(3 * time.Minute))

	if e.Working() {
		t.Fatal("clip must be completed on timeout")
	}
	// legA: 2 passive (1 event + 1 caught) — no legA taker; legB: 1 hedge + 1 completion = 2.
	if got := tk.lots["buy "+testLegA]; got != 0 {
		t.Fatalf("legA is already full — no taker, got %d (%v)", got, tk.lots)
	}
	if got := tk.lots["sell "+testLegB]; got != 2 {
		t.Fatalf("legB must total 2 sells (hedge + completion), got %d (%v)", got, tk.lots)
	}
	takers := len(tk.calls)

	e.OnFill(openHour.Add(4*time.Minute), bidA, testLegA, true, 1, 100) // the caught lot streams in
	if len(tk.calls) != takers {
		t.Fatalf("the caught lot's late event must dedup, got %v", tk.calls)
	}
	if e.Position() != 2 {
		t.Fatalf("position must stay at the committed target, got %d", e.Position())
	}
}

// With FillTimeout == 0 the backstop is disabled: a resting clip never times out, no
// matter how far the clock advances — it is purely signal-driven.
func TestNoTimeoutWhenFillTimeoutZero(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: 1, Lots: 2}}
	e := newEngineWith(EngineConfig{LegA: testLegA, LegB: testLegB, OrderVol: 2, HedgeRetries: 1}, m, tk, dm)
	seedBooks(e, openHour)
	e.OnState(RowState{Time: openHour})
	if !e.Working() {
		t.Fatal("clip did not open")
	}

	e.OnTick(openHour.Add(1000 * time.Hour))

	if !e.Working() {
		t.Fatal("a signal-driven clip must never time out")
	}
	if m.count("cancel") != 0 || len(tk.calls) != 0 {
		t.Fatalf("nothing may be cancelled or crossed, maker=%v taker=%v", m.calls, tk.calls)
	}
}

// --- KeepPartialOpenOnTimeout: an opening clip keeps the hedged partial and folds any cancel-race fill, a closing clip still completes ---

// TestKeepPartialOpenOnTimeoutCancelsRemainder pins the C1 fix: an OPENING clip that
// partially fills and then times out must be CANCELLED — the already-hedged partial kept,
// the unfilled remainder simply dropped — NOT taker-completed. This reproduces the old
// basis Engine's checkTimeout, which cancelled a partially-filled entry instead of
// crossing the spread to finish it (a passive entry must stay passive).
//
// Position() is asserted via fixedDecider.pos crediting the fill directly rather than via
// dm.Commit: basis's real Decider derives Position() from fills recorded by the runner as
// they land (Commit is a no-op — see strategies/basis/decider.go), independent of the
// engine's Commit path. That path is deliberately never reached on this branch (CancelClip
// does not call Commit), so the test mirrors basis's actual wiring instead of relying on it.
func TestKeepPartialOpenOnTimeoutCancelsRemainder(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: 1, Lots: 2}} // opening long, target 2
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 2,
		FillTimeout: time.Minute, HedgeRetries: 2, KeepPartialOpenOnTimeout: true,
	}, m, tk, dm)
	seedBooks(e, openHour)
	e.OnState(RowState{Time: openHour}) // opens (bid leg A / ask leg B)
	legAID := m.id(testLegA)

	dm.pos++                                           // the runner would credit this fill to the ledger directly
	e.OnFill(openHour, legAID, testLegA, true, 1, 100) // only 1 of 2 lots fills

	if !e.Working() {
		t.Fatal("clip should still be working after a partial fill")
	}

	e.OnTick(openHour.Add(2 * time.Minute)) // fires the fill-timeout

	if e.Working() {
		t.Fatal("clip should be resolved (cancelled) after the partial-open timeout")
	}
	if e.Position() != 1 {
		t.Fatalf("KeepPartialOpenOnTimeout must keep the hedged partial, got position %d", e.Position())
	}
	// No taker chase for the unfilled remainder: leg B's hedge count must stay at the single
	// fill it already covered, and leg A must see no taker buy at all.
	if tk.lots["sell "+testLegB] != 1 {
		t.Fatalf("timeout must not taker-chase the remainder (extra leg B sell), got %v", tk.lots)
	}
	if tk.lots["buy "+testLegA] != 0 {
		t.Fatalf("timeout must not taker-cross leg A for the unfilled remainder, got %v", tk.lots)
	}
	if m.count("cancel "+legAID) == 0 {
		t.Fatalf("the resting leg A order must be cancelled on timeout, got calls %v", m.calls)
	}
}

// TestKeepPartialOpenOnTimeoutClosingStillCompletes confirms KeepPartialOpenOnTimeout is
// scoped to OPENING clips only: a partially-filled CLOSING clip must still taker-complete
// on timeout exactly as without the flag — once a reduction has begun, it must finish,
// never leaving the position in an odd, undecided state.
func TestKeepPartialOpenOnTimeoutClosingStillCompletes(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: -1, IsClose: true, Lots: 2}}
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 2,
		FillTimeout: time.Minute, HedgeRetries: 2, KeepPartialOpenOnTimeout: true,
	}, m, tk, dm)
	seedBooks(e, openHour)
	e.OnState(RowState{Time: openHour}) // opens a closing clip (ask leg A / bid leg B)
	legAID := m.id(testLegA)

	e.OnFill(openHour, legAID, testLegA, false, 1, 100) // leg A (ask) sells 1 of 2
	if !e.Working() {
		t.Fatal("clip should still be working after a partial fill")
	}

	e.OnTick(openHour.Add(2 * time.Minute)) // fires the fill-timeout

	// The 1 already-hedged lot (taker buy leg B x1) plus the taker-completed remainder
	// (sell leg A x1 + buy leg B x1) leave the whole close realized.
	if tk.lots["buy "+testLegB] != 2 {
		t.Fatalf("expected leg B bought x2 total, got %v", tk.lots)
	}
	if tk.lots["sell "+testLegA] != 1 {
		t.Fatalf("expected leg A remainder sold x1, got %v", tk.lots)
	}
	if e.Working() {
		t.Fatal("clip should be resolved (completed) after the partial-close timeout")
	}
}

// KeepPartialOpenOnTimeout whose cancel catches an extra in-flight fill: the extra lot is
// real inventory and must be equalized with ONE taker on the other leg, keeping 1:1.
func TestKeepPartialTimeoutFoldsCancelRaceFill(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: 1, Lots: 3}}
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 3,
		FillTimeout: time.Minute, HedgeRetries: 2, KeepPartialOpenOnTimeout: true,
	}, m, tk, dm)
	seedBooks(e, openHour)
	e.OnState(RowState{Time: openHour})
	bidA := m.id(testLegA)

	e.OnFill(openHour, bidA, testLegA, true, 1, 100) // 1 of 3 fills, hedged
	m.executed = map[string]int{bidA: 2}             // one more lot caught by the timeout's cancel
	e.OnTick(openHour.Add(2 * time.Minute))

	if e.Working() || e.Halted() {
		t.Fatalf("clip must be dropped cleanly, working=%v halted=%v", e.Working(), e.Halted())
	}
	// legA: 2 passive; legB: 1 hedge + 1 equalizer = 2. No chase to target 3.
	if got := tk.lots["sell "+testLegB]; got != 2 {
		t.Fatalf("legB must be equalized to 2, got %d (%v)", got, tk.lots)
	}
	if got := tk.lots["buy "+testLegA]; got != 0 {
		t.Fatalf("no legA taker under keep-partial, got %d (%v)", got, tk.lots)
	}
}

// --- counterpart-ahead: the clip resolves immediately, never left working ---

// The counterpart ran ahead of the maker's first fill: the clip is resolved immediately —
// both legs completed to target with takers and the clip committed. It must NOT be left
// working: the maker's passive still rests for its FULL remainder at the broker, so keeping
// it (the old taker-top-up handling) double-realized the top-up lots when the passive later
// filled — a target-4 clip landing 6:6 (see TestCounterpartAheadNeverOvershootsTarget).
func TestCounterpartAheadCompletesClipAtTarget(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 4)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	m.executed = map[string]int{m.id(testLegB): 3} // leg B ahead
	legAID := m.id(testLegA)

	e.OnFill(openHour, legAID, testLegA, true, 2, 100) // maker leg A only fills 2

	if got := tk.lots["buy "+testLegA]; got != 2 {
		t.Fatalf("expected leg A completed to target with a 2-lot taker (4−2), got %d (%v)", got, tk.lots)
	}
	if got := tk.lots["sell "+testLegB]; got != 1 {
		t.Fatalf("expected leg B completed to target with a 1-lot taker (4−3), got %d (%v)", got, tk.lots)
	}
	if e.Halted() || e.Working() {
		t.Fatalf("clip must be committed and cleared, halted=%v working=%v", e.Halted(), e.Working())
	}
	if e.Position() != 4 {
		t.Fatalf("committed clip must book the full target, got %d", e.Position())
	}
	if m.count("cancel "+legAID) != 1 {
		t.Fatalf("the maker's resting remainder must be pulled at resolution, got %v", m.calls)
	}
}

// The overshoot regression the immediate-resolution fix exists for: counterpart ahead
// (3 > 1) on a target-4 clip. Under the old top-up handling the maker's passive kept
// resting for its full 3-lot remainder; when it later filled in one print the clip landed
// 6:6 — past the cap and desynced from the Decider. Now the clip is resolved at the first
// fill: exactly target realized per leg, the maker's remainder cancelled, and the late
// fill events of the cancel race dedup instead of adding lots.
func TestCounterpartAheadNeverOvershootsTarget(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 4)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour)) // buy clip: bid legA, ask legB
	bidA, askB := m.id(testLegA), m.id(testLegB)

	m.executed = map[string]int{askB: 3, bidA: 1} // legB raced ahead; legA's cancel-ack agrees with its 1 event lot
	e.OnFill(openHour, bidA, testLegA, true, 1, 100)

	if e.Working() {
		t.Fatal("counterpart-ahead clip must be resolved immediately, not left working")
	}
	// legA: 1 passive + 3 taker = 4; legB: 3 passive + 1 taker = 4.
	if got := tk.lots["buy "+testLegA]; got != 3 {
		t.Fatalf("legA must be taker-completed by 3 (4−1), got %d (%v)", got, tk.lots)
	}
	if got := tk.lots["sell "+testLegB]; got != 1 {
		t.Fatalf("legB must be taker-completed by 1 (4−3), got %d (%v)", got, tk.lots)
	}
	if e.Position() != 4 {
		t.Fatalf("realized position must be exactly target 4, got %d", e.Position())
	}
	takers := len(tk.calls)

	// The counterpart's cancel-race lots stream in late: already accounted via the
	// cancel-ack, so nothing may move (the maker's 1 lot already streamed — only the
	// counterpart's 3 are still owed by the stream).
	e.OnFill(openHour, askB, testLegB, false, 3, 51)
	if len(tk.calls) != takers {
		t.Fatalf("late race fills must dedup, got extra takers: %v", tk.calls)
	}
	if e.Position() != 4 {
		t.Fatalf("late race fills must not move the position, got %d", e.Position())
	}
}

// Counterpart-ahead on an OPENING clip with KeepPartialOpenOnTimeout (basis semantics):
// the legs are equalized at the larger side with one minimal taker — no chase to target —
// and the clip is dropped WITHOUT committing (the ledger, not Commit, carries the partial).
func TestCounterpartAheadKeepPartialEqualizesAndDrops(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: 1, Lots: 4}}
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 4,
		HedgeRetries: 2, KeepPartialOpenOnTimeout: true,
	}, m, tk, dm)
	seedBooks(e, openHour)
	e.OnState(RowState{Time: openHour}) // opening long: bid legA, ask legB
	bidA, askB := m.id(testLegA), m.id(testLegB)

	m.executed = map[string]int{askB: 3, bidA: 1}
	e.OnFill(openHour, bidA, testLegA, true, 1, 100)

	if e.Working() || e.Halted() {
		t.Fatalf("clip must be dropped cleanly, working=%v halted=%v", e.Working(), e.Halted())
	}
	// legA: 1 passive + 2 taker = 3 = legB's 3 passive. No leg is chased to target 4.
	if got := tk.lots["buy "+testLegA]; got != 2 {
		t.Fatalf("legA must be equalized up to 3 with a 2-lot taker, got %d (%v)", got, tk.lots)
	}
	if got := tk.lots["sell "+testLegB]; got != 0 {
		t.Fatalf("no legB taker: it is already the larger side, got %d (%v)", got, tk.lots)
	}
	if dm.pos != 0 {
		t.Fatalf("an equalized partial must NOT be committed, got pos %d", dm.pos)
	}
}

// Counterpart-ahead on a CLOSING clip must complete to target even under
// KeepPartialOpenOnTimeout — a reduction, once begun, always finishes.
func TestCounterpartAheadClosingCompletesDespiteKeepPartial(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: -1, IsClose: true, Lots: 4}}
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 4,
		HedgeRetries: 2, KeepPartialOpenOnTimeout: true,
	}, m, tk, dm)
	seedBooks(e, openHour)
	e.OnState(RowState{Time: openHour}) // closing clip: ask legA, bid legB
	askA, bidB := m.id(testLegA), m.id(testLegB)

	m.executed = map[string]int{bidB: 3, askA: 1}
	e.OnFill(openHour, askA, testLegA, false, 1, 101)

	if e.Working() || e.Halted() {
		t.Fatalf("closing clip must be completed, working=%v halted=%v", e.Working(), e.Halted())
	}
	if got := tk.lots["sell "+testLegA]; got != 3 {
		t.Fatalf("legA must be completed to target with a 3-lot taker, got %d (%v)", got, tk.lots)
	}
	if got := tk.lots["buy "+testLegB]; got != 1 {
		t.Fatalf("legB must be completed to target with a 1-lot taker, got %d (%v)", got, tk.lots)
	}
	if dm.pos != -4 {
		t.Fatalf("the completed close must commit the full target, got pos %d", dm.pos)
	}
}

// --- signal changes on a resting clip: revert, persist, and a reversal with a partial fill ---

func TestSignalRevertCancelsUnfilledClip(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	legAID, legBID := m.id(testLegA), m.id(testLegB)

	// Signal no longer wants the trade: the resting (unfilled) clip is cancelled.
	e.OnState(holdState(openHour.Add(time.Second)))

	if e.Working() {
		t.Fatal("an unwanted unfilled clip should be cancelled")
	}
	if m.count("cancel "+legAID) != 1 || m.count("cancel "+legBID) != 1 {
		t.Fatalf("both passives should be cancelled, got %v", m.calls)
	}
	if e.Position() != 0 {
		t.Fatalf("position must stay flat, got %d", e.Position())
	}
}

func TestSignalPersistsKeepsClipResting(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	placements := len(m.calls)

	// The same signal on the next bar: the clip must keep resting, not restack or cancel.
	e.OnState(buyState(openHour.Add(time.Second)))

	if !e.Working() {
		t.Fatal("a still-wanted clip must keep resting")
	}
	if len(m.calls) != placements {
		t.Fatalf("no new orders/cancels while the signal persists, got %v", m.calls)
	}
}

// A partially-filled clip whose signal REVERSES resolves exactly like a timeout: the
// remainder is taker-completed to a whole lot, never left as a naked half-lot.
func TestSignalReversalWithPartialFillCompletes(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	bidA := m.id(testLegA)

	e.OnFill(openHour, bidA, testLegA, true, 1, 100) // 1 of 2 fills
	e.OnState(sellState(openHour.Add(time.Second)))  // the decider now wants the OTHER direction

	if e.Working() {
		t.Fatal("a reversed partially-filled clip must be resolved")
	}
	if got := tk.lots["sell "+testLegB]; got != 2 {
		t.Fatalf("legB must total 2 (hedge + completion), got %d (%v)", got, tk.lots)
	}
	if got := tk.lots["buy "+testLegA]; got != 1 {
		t.Fatalf("legA remainder must be taker-completed x1, got %d (%v)", got, tk.lots)
	}
	if e.Position() != 2 {
		t.Fatalf("the whole lot must be committed, got %d", e.Position())
	}
}

// --- ForceCloseOnTimeout: an unfilled close is taker-crossed with the flag, cancelled without it ---

func TestForceCloseOnTimeoutTakerClosesUnfilledClose(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: -1, IsClose: true, Lots: 2}}
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 2,
		FillTimeout: time.Minute, HedgeRetries: 2, ForceCloseOnTimeout: true,
	}, m, tk, dm)
	seedBooks(e, openHour)
	e.OnState(RowState{Time: openHour}) // opens a closing clip (ask leg A / bid leg B)

	// Nothing fills; the fill-timeout fires. A closing clip must be finished with a taker cross.
	e.OnTick(openHour.Add(2 * time.Minute))

	if tk.lots["sell "+testLegA] != 2 || tk.lots["buy "+testLegB] != 2 {
		t.Fatalf("ForceCloseOnTimeout must taker-complete the unfilled close, got %v", tk.lots)
	}
	if e.Working() {
		t.Fatal("clip should be resolved after the forced close")
	}
}

func TestUnfilledCloseCancelsWithoutForceClose(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: -1, IsClose: true, Lots: 2}}
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 2,
		FillTimeout: time.Minute, HedgeRetries: 2, // ForceCloseOnTimeout false
	}, m, tk, dm)
	seedBooks(e, openHour)
	e.OnState(RowState{Time: openHour})

	e.OnTick(openHour.Add(2 * time.Minute))

	if len(tk.calls) != 0 {
		t.Fatalf("without ForceCloseOnTimeout an unfilled clip must not taker-cross, got %v", tk.calls)
	}
	if e.Working() {
		t.Fatal("an unfilled clip must be cancelled on timeout")
	}
}

// --- PullIfUnwanted: pulling a decayed entry or close, keeping a wanted one, MinRest, and after a halt ---

// TestPullEntryCancelsDecayedUnfilledEntry pins cancel-entry-on-decay: an unfilled opening clip
// whose entry signal has reverted (Peek now wants nothing) is pulled — both passives cancelled,
// back to flat and idle — instead of resting to fill/timeout.
func TestPullEntryCancelsDecayedUnfilledEntry(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour)) // open an unfilled long entry clip
	legAID, legBID := m.id(testLegA), m.id(testLegB)
	if !e.Working() {
		t.Fatal("expected a working clip after the buy signal")
	}

	e.PullIfUnwanted(holdState(openHour)) // entry z decayed: nothing wanted

	if e.Working() {
		t.Fatal("a decayed unfilled entry should have been pulled")
	}
	if m.count("cancel "+legAID) != 1 || m.count("cancel "+legBID) != 1 {
		t.Fatalf("both passives should be cancelled, got %v", m.calls)
	}
	if e.Position() != 0 {
		t.Fatalf("position must stay flat, got %d", e.Position())
	}
}

// TestPullEntryKeepsWantedEntry: while the entry still holds (Peek wants this direction),
// PullIfUnwanted leaves the resting clip alone — no cancel, keep queue priority.
func TestPullEntryKeepsWantedEntry(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	legAID := m.id(testLegA)

	e.PullIfUnwanted(buyState(openHour)) // still wanted

	if !e.Working() {
		t.Fatal("a still-wanted entry must keep resting")
	}
	if m.count("cancel "+legAID) != 0 {
		t.Fatalf("a still-wanted entry must not be cancelled, got %v", m.calls)
	}
}

// TestPullDropsPartiallyFilledEntryRemainder: fill state makes no difference to the pull —
// a decayed entry's UNFILLED REMAINDER is cancelled; the already-filled (and already
// 1:1-hedged) lots simply stay. No takers beyond the fill-time hedge are issued.
func TestPullDropsPartiallyFilledEntryRemainder(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	legAID := m.id(testLegA)
	e.OnFill(openHour, legAID, testLegA, true, 1, 100) // partial: 1 of 2 fills, hedged on legB

	if !e.Working() {
		t.Fatal("clip should still work after a partial fill")
	}
	hedges := len(tk.calls)               // the partial's 1:1 hedge, issued at fill time
	e.PullIfUnwanted(holdState(openHour)) // signal decays

	if e.Working() {
		t.Fatal("a decayed clip's unfilled remainder must be pulled, partial or not")
	}
	if m.count("cancel "+legAID) != 1 {
		t.Fatalf("the resting remainder must be cancelled, got %v", m.calls)
	}
	if len(tk.calls) != hedges {
		t.Fatalf("pulling the remainder must not issue new takers, got %v", tk.calls)
	}
}

// TestMinRestDelaysPull: with MinRest configured, a freshly placed clip survives a decayed
// signal until it has rested its guaranteed time — then the pull goes through.
func TestMinRestDelaysPull(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newEngineWith(EngineConfig{LegA: testLegA, LegB: testLegB, OrderVol: 2, HedgeRetries: 2, MinRest: 3 * time.Second}, m, tk, newTestDecider(20, 2))
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	if !e.Working() {
		t.Fatal("expected a working clip")
	}

	e.PullIfUnwanted(holdState(openHour.Add(time.Second))) // decayed, but only 1s rested
	if !e.Working() {
		t.Fatal("MinRest must keep the order resting: pulled after 1s of a 3s guarantee")
	}
	if m.count("cancel") != 0 {
		t.Fatalf("no cancels may be issued inside MinRest, got %v", m.calls)
	}

	e.PullIfUnwanted(holdState(openHour.Add(3 * time.Second))) // guarantee served
	if e.Working() {
		t.Fatal("after MinRest a decayed clip must be pulled")
	}
}

// PullIfUnwanted pulls an UNFILLED closing clip whose signal has decayed (closing would now
// realise worse than fair — the strategy would rather hold for reversion), symmetric with the
// decayed-entry pull. Without it, the fill-timeout's ForceCloseOnTimeout would force-cross a
// close the Decider no longer wants. The position itself is untouched — it stays held and
// re-exits when the closing z recovers.
func TestPullPullsDecayedUnfilledClosingClip(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: -1, IsClose: true, Lots: 2}}
	e := newEngineWith(EngineConfig{LegA: testLegA, LegB: testLegB, OrderVol: 2, HedgeRetries: 2}, m, tk, dm)
	seedBooks(e, openHour)
	e.OnState(RowState{Time: openHour}) // closing clip opens

	dm.intent = Intent{Action: actionHold} // exit signal decays: closing would be worse than fair
	e.PullIfUnwanted(RowState{Time: openHour.Add(time.Second)})

	if e.Working() {
		t.Fatal("a decayed UNFILLED closing clip must be pulled, not left to the force-cross timeout")
	}
	if len(tk.calls) != 0 {
		t.Fatalf("pulling an unfilled close must never taker-cross, got %v", tk.calls)
	}
}

// A partially-filled closing clip whose signal decays: the unfilled remainder is pulled —
// never taker-crossed — and the already-realised partial reduction stays. The pair remains
// 1:1 (the partial was hedged at fill time); the position is simply smaller and re-exits
// when the closing z recovers.
func TestPullDropsPartiallyFilledClosingClipRemainder(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: -1, IsClose: true, Lots: 2}}
	e := newEngineWith(EngineConfig{LegA: testLegA, LegB: testLegB, OrderVol: 2, HedgeRetries: 2}, m, tk, dm)
	seedBooks(e, openHour)
	e.OnState(RowState{Time: openHour}) // closing clip opens
	askA := m.id(testLegA)
	e.OnFill(openHour, askA, testLegA, false, 1, 100) // partial: 1 of 2 fills, hedged on legB
	hedges := len(tk.calls)

	dm.intent = Intent{Action: actionHold} // signal decays after the partial
	e.PullIfUnwanted(RowState{Time: openHour.Add(time.Second)})

	if e.Working() {
		t.Fatal("a decayed closing clip's unfilled remainder must be pulled")
	}
	if len(tk.calls) != hedges {
		t.Fatalf("the remainder must be cancelled, never taker-crossed, got %v", tk.calls)
	}
}

// PullIfUnwanted after a halt: the clip is already gone; the call must be a no-op
// (no panic, no orders) — the runner keeps calling it on every book tick.
func TestPullEntryAfterHaltNoOp(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	e.Halt("test")
	calls := len(m.calls)

	e.PullIfUnwanted(holdState(openHour.Add(time.Second)))

	if len(m.calls) != calls || len(tk.calls) != 0 {
		t.Fatalf("a halted engine's PullEntry must be a no-op, maker=%v taker=%v", m.calls, tk.calls)
	}
}

// --- CancelClip / teardown: settleClip balances the pair once, CancelClip is idempotent, and a halt places nothing ---

// Teardown with in-flight fills on both legs must balance the pair with ONE minimal
// taker, not hedge each late fill blindly: legA caught 3 lots, legB 1 → one 2-lot
// taker on legB's side, and the late fill events are all no-ops afterwards.
func TestTeardownBalancesLegsWithOneTaker(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 4)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour)) // buy clip: bid legA, ask legB
	bidA, askB := m.id(testLegA), m.id(testLegB)

	m.executed = map[string]int{bidA: 3, askB: 1} // both cancels catch in-flight fills
	e.CancelClip()

	if got := tk.lots["sell "+testLegB]; got != 2 {
		t.Fatalf("want one 2-lot taker on legB side, got lots=%d calls=%v", got, tk.calls)
	}
	if len(tk.calls) != 1 {
		t.Fatalf("want exactly one taker, got %v", tk.calls)
	}

	// the caught lots stream in afterwards — all deduped
	e.OnFill(openHour, bidA, testLegA, true, 3, 100)
	e.OnFill(openHour, askB, testLegB, false, 1, 51)
	if len(tk.calls) != 1 {
		t.Fatalf("late fills after settle must be no-ops, got %v", tk.calls)
	}
}

// While halted the kill-switch is absolute: teardown discovers an imbalance but places
// nothing, only logs.
func TestHaltedTeardownPlacesNothing(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 4)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	bidA := m.id(testLegA)

	m.executed = map[string]int{bidA: 3}
	e.Halt("test halt") // Halt calls CancelClip internally
	if len(tk.calls) != 0 {
		t.Fatalf("halted teardown must place nothing, got %v", tk.calls)
	}
}

// CancelClip is idempotent: a second call (shutdown racing a signal revert) must not
// re-cancel or re-settle anything.
func TestDoubleCancelClipIdempotent(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	legAID, legBID := m.id(testLegA), m.id(testLegB)

	e.CancelClip()
	e.CancelClip()

	if m.count("cancel "+legAID) != 1 || m.count("cancel "+legBID) != 1 {
		t.Fatalf("each leg must be cancelled exactly once, got %v", m.calls)
	}
}

// --- OnOrderStatus: the engine's own cancel-ack must not misfire, an exchange-initiated kill must always cancel the clip ---

// The broker's order stream echoes back a CANCELED status for every order the engine
// itself cancels. After the first partial fill retires the counterpart leg, that echo
// still carries a clip leg's id — it must NOT tear down the still-working clip (pulling
// the resting maker remainder and skipping the commit was exactly that bug).
func TestCounterpartCancelAckStatusKeepsClipWorking(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 4)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	legAID, legBID := m.id(testLegA), m.id(testLegB)

	// Partial first fill: leg A becomes the maker, leg B is retired (cancelled).
	e.OnFill(openHour, legAID, testLegA, true, 1, 100)
	if m.count("cancel "+legBID) != 1 {
		t.Fatalf("expected the counterpart %s to be cancelled on first fill, got %v", legBID, m.calls)
	}

	// The broker's cancel-ack for OUR OWN retire arrives on the order stream.
	e.OnOrderStatus(legBID, true)

	if !e.Working() {
		t.Fatal("our own cancel's ack must not tear down the working clip")
	}
	if m.count("cancel "+legAID) != 0 {
		t.Fatalf("the resting maker %s must not be pulled by the cancel-ack echo, got %v", legAID, m.calls)
	}

	// The clip keeps working post-and-wait and completes normally.
	e.OnFill(openHour, legAID, testLegA, true, 3, 100)
	if e.Working() || e.Position() != 4 {
		t.Fatalf("clip must still complete to target 4, working=%v pos=%d", e.Working(), e.Position())
	}
}

// Regression guard for the case OnOrderStatus exists for: a clip leg that dies while the
// engine still considers it live (exchange reject/cancel/expire, final unknown) must still
// pull the clip.
func TestExchangeDeadLegStillCancelsClip(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 4)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	legAID, legBID := m.id(testLegA), m.id(testLegB)

	e.OnOrderStatus(legAID, true) // the exchange killed leg A: the engine never cancelled it

	if e.Working() {
		t.Fatal("an exchange-dead leg must cancel the clip")
	}
	if m.count("cancel "+legBID) != 1 {
		t.Fatalf("the surviving leg %s must be pulled with the clip, got %v", legBID, m.calls)
	}
}

// The exchange kills the maker's REMAINDER after a partial fill (expire/cancel from the
// venue): the clip is pulled, the already-hedged partial stays, the pair stays 1:1 and the
// engine goes idle without a halt.
func TestExchangeDeadMakerAfterPartialFillStaysBalanced(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	bidA := m.id(testLegA)

	e.OnFill(openHour, bidA, testLegA, true, 1, 100) // partial, hedged 1:1
	m.executed = map[string]int{bidA: 1}             // the venue's kill confirms 1 lot executed
	e.OnOrderStatus(bidA, true)                      // exchange kills the remainder

	if e.Working() {
		t.Fatal("an exchange-dead maker must pull the clip")
	}
	if e.Halted() {
		t.Fatal("a balanced teardown must not halt")
	}
	// The single hedge from the partial is all the taker traffic there may be.
	if got := tk.lots["sell "+testLegB]; got != 1 || len(tk.calls) != 1 {
		t.Fatalf("the pair must stay at the hedged 1:1 partial, got %v", tk.lots)
	}
}

// --- impaired mode: an unconfirmable cancel defers the retire instead of halting ---

// A stuck maker order discovered while committing a completed clip must not crash the
// commit path or lose the lot: the commit lands FIRST (the fills are confirmed), the
// unconfirmable retire is deferred, and the engine waits impaired instead of halting.
func TestCommitStuckMakerDefersAndKeepsTheLot(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: 1, Lots: 2}}
	e := newEngineWith(EngineConfig{LegA: testLegA, LegB: testLegB, OrderVol: 2, HedgeRetries: 2}, m, tk, dm)
	seedBooks(e, openHour)
	e.OnState(RowState{Time: openHour})
	legAID, legBID := m.id(testLegA), m.id(testLegB)

	// The counterpart runs ahead (2 filled at its cancel) of the maker's first fill (1):
	// the maker is topped up to 2 = target, so the clip completes and commitClip retires
	// the still-resting maker — whose cancel and status both fail.
	m.executed = map[string]int{legBID: 2}
	m.cancelErr = map[string]error{legAID: errors.New("rpc down")}
	e.OnFill(openHour, legAID, testLegA, true, 1, 100)

	if e.Halted() {
		t.Fatal("the engine must never halt itself at commit")
	}
	if !e.Impaired() {
		t.Fatal("an unconfirmable maker retire at commit must enter impaired mode")
	}
	if e.Working() {
		t.Fatal("the committed clip must not stay working")
	}
	if dm.pos != 2 {
		t.Fatalf("the completed lot's fills are confirmed — it must be committed, got pos %d", dm.pos)
	}

	// The broker answers at last: the maker executed exactly the 1 lot already folded —
	// nothing new surfaces, the obligation clears, the engine recovers.
	m.cancelErr = nil
	m.executed[legAID] = 1
	e.OnTick(openHour.Add(5 * time.Second))
	if e.Impaired() {
		t.Fatal("a confirmed retire must clear impaired mode")
	}
}

// An abandon whose first retire hits an unconfirmable cancel (cancel and status both
// fail) defers that retire and goes impaired: the clip is dropped without a commit, no
// takers fire on guessed numbers, and the obligation loop owns the unanswered order.
func TestAbandonStuckOrderDefersWithoutCommit(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	bidA := m.id(testLegA)
	m.cancelErr = map[string]error{bidA: errors.New("rpc down")} // no status entry → Status errors too

	e.OnState(holdState(openHour.Add(time.Second)))

	if e.Halted() {
		t.Fatal("the engine must never halt itself during an abandon")
	}
	if !e.Impaired() {
		t.Fatal("an unconfirmable cancel during the abandon must enter impaired mode")
	}
	if e.Working() {
		t.Fatal("the clip must be dropped — its orders are pulled or deferred")
	}
	if e.Position() != 0 {
		t.Fatalf("nothing may be committed on a deferred abandon, got %d", e.Position())
	}
	if len(tk.calls) != 0 {
		t.Fatalf("no takers may fire while the executed count is unknown, got %v", tk.calls)
	}
}

// --- placement failures: no naked resting order may survive a failed open ---

// legA's placement fails outright: nothing may rest, and the failure backoff must
// suppress the retry on the very next bar (the request storm) until it expires.
func TestPlaceLegAFailureBacksOffThenRetries(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	m.placeErr = map[string]error{testLegA: errors.New("reject")}

	e.OnState(buyState(openHour))
	if e.Working() || len(m.ids) != 0 {
		t.Fatalf("no order may rest after a legA placement failure, ids=%v", m.ids)
	}

	m.placeErr = nil // broker recovers, but the backoff (default 2s) is still running
	e.OnState(buyState(openHour.Add(time.Second)))
	if e.Working() {
		t.Fatal("the backoff must suppress the retry on the next bar")
	}
	e.OnState(buyState(openHour.Add(3 * time.Second)))
	if !e.Working() {
		t.Fatal("the engine must quote again once the backoff expires")
	}
}

// legB's placement fails after legA was placed: legA is retired, and if its cancel caught
// nothing the engine is flat with no taker — a clean unwind.
func TestPlaceLegBFailureUnwindsUnfilledLegA(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	m.placeErr = map[string]error{testLegB: errors.New("reject")}

	e.OnState(buyState(openHour))

	if e.Working() || e.Halted() {
		t.Fatalf("failed open must leave the engine idle, working=%v halted=%v", e.Working(), e.Halted())
	}
	if m.count("cancel "+m.id(testLegA)) != 1 {
		t.Fatalf("the naked legA order must be cancelled, got %v", m.calls)
	}
	if len(tk.calls) != 0 {
		t.Fatalf("nothing filled → no taker, got %v", tk.calls)
	}
}

// legB's placement fails AND legA's cancel catches a race fill: the gap is real naked
// inventory and must be taker-matched on legB — never left for reconcile to halt on.
func TestPlaceLegBFailureHedgesLegARaceFill(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	m.placeErr = map[string]error{testLegB: errors.New("reject")}
	m.executed = map[string]int{"b1": 2} // legA (first placed order) fills before its cancel

	e.OnState(buyState(openHour))

	if got := tk.lots["sell "+testLegB]; got != 2 {
		t.Fatalf("the caught legA fill must be matched on legB (sell x2), got %d (%v)", got, tk.lots)
	}
	if e.Working() || e.Halted() {
		t.Fatalf("hedged unwind must leave the engine idle, working=%v halted=%v", e.Working(), e.Halted())
	}
}

// --- reject-retry ladder: shrink a clip's FIRST placement attempt on a definitive broker reject ---

// A definitive rejection (broker answered: no) at the full size retries smaller, within the
// same tryOpenClip call, until a size the broker accepts.
func TestRejectRetryLadderShrinksToFillableSize(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	// ExecSoloMaker: only legA rests as a maker, isolating its placement/retry sequence from
	// legB's (a separate, DualPassive-only concern already covered below).
	dm := &fixedDecider{intent: Intent{Action: 1, IsClose: true, Lots: 10, ExecMode: ExecSoloMaker}}
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 10, HedgeRetries: 2,
		RejectRetryLotStep: 2, RejectRetryMinLots: 2,
	}, m, tk, dm)
	seedBooks(e, openHour)
	m.placeErr = map[string]error{testLegA: status.Error(codes.InvalidArgument, "insufficient funds")}
	m.placeErrMaxLots = map[string]int{testLegA: 6} // broker accepts ≤6

	e.OnState(RowState{Time: openHour})

	if want := []int{10, 8, 6}; !slices.Equal(m.attempts, want) {
		t.Fatalf("attempts = %v, want %v", m.attempts, want)
	}
	if !e.Working() {
		t.Fatal("the clip must open at the smaller, accepted size")
	}
	if e.clip.target != 6 {
		t.Fatalf("clip target = %d, want 6 (the size that was actually accepted)", e.clip.target)
	}
}

// An OPENING clip (!Intent.IsClose) never gets the ladder, even with a definitive reject at
// a size the broker would have accepted smaller: its size is the Decider's entry sizing
// (room under the cap/SizeGate), and silently under-opening it would diverge from what the
// signal asked for. Only a rejected CLOSE is retried smaller — see RejectRetryLotStep's doc.
func TestRejectRetryLadderNeverAppliesToOpeningClips(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: 1, Lots: 10, ExecMode: ExecSoloMaker}} // IsClose: false
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 10, HedgeRetries: 2,
		RejectRetryLotStep: 2, RejectRetryMinLots: 2,
	}, m, tk, dm)
	seedBooks(e, openHour)
	m.placeErr = map[string]error{testLegA: status.Error(codes.InvalidArgument, "insufficient funds")}
	m.placeErrMaxLots = map[string]int{testLegA: 6} // would have been fillable smaller — must not matter

	e.OnState(RowState{Time: openHour})

	if want := []int{10}; !slices.Equal(m.attempts, want) {
		t.Fatalf("attempts = %v, want %v — an opening clip must never shrink-retry", m.attempts, want)
	}
	if e.Working() {
		t.Fatal("nothing may rest after a failed entry attempt")
	}
}

// Every rung fails, even at the floor: the ladder gives up exactly like a single failed
// attempt always did — logged and backed off, nothing left resting.
func TestRejectRetryLadderGivesUpBelowFloor(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: 1, IsClose: true, Lots: 10}}
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 10, HedgeRetries: 2,
		RejectRetryLotStep: 3, RejectRetryMinLots: 4,
	}, m, tk, dm)
	seedBooks(e, openHour)
	m.placeErr = map[string]error{testLegA: status.Error(codes.InvalidArgument, "insufficient funds")}

	e.OnState(RowState{Time: openHour})

	if want := []int{10, 7, 4}; !slices.Equal(m.attempts, want) {
		t.Fatalf("attempts = %v, want %v (next rung 1 < floor 4, ladder must stop)", m.attempts, want)
	}
	if e.Working() {
		t.Fatal("nothing may rest when every rung was rejected")
	}
}

// A transport/ambiguous error (maybeDelivered == true) is NOT a definitive rejection — the
// ladder must not fire, exactly the pre-existing single-attempt-then-backoff behaviour.
func TestRejectRetryLadderSkipsAmbiguousTransportError(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: 1, IsClose: true, Lots: 10}}
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 10, HedgeRetries: 2,
		RejectRetryLotStep: 2, RejectRetryMinLots: 2,
	}, m, tk, dm)
	seedBooks(e, openHour)
	m.placeErr = map[string]error{testLegA: status.Error(codes.Unavailable, "temporarily unavailable")}
	m.placeErrMaxLots = map[string]int{testLegA: 6} // would have been fillable smaller — must not matter

	e.OnState(RowState{Time: openHour})

	if want := []int{10}; !slices.Equal(m.attempts, want) {
		t.Fatalf("attempts = %v, want %v — an ambiguous error must not trigger the ladder", m.attempts, want)
	}
	if e.Working() {
		t.Fatal("nothing may rest after a failed attempt")
	}
}

// RejectRetryLotStep unset (0) is the default: the ladder stays off and a definitive
// rejection behaves exactly as before this feature existed, even if a smaller size would
// have been accepted.
func TestRejectRetryLadderDisabledByDefault(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: 1, IsClose: true, Lots: 10}}
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 10, HedgeRetries: 2,
	}, m, tk, dm)
	seedBooks(e, openHour)
	m.placeErr = map[string]error{testLegA: status.Error(codes.InvalidArgument, "insufficient funds")}
	m.placeErrMaxLots = map[string]int{testLegA: 6}

	e.OnState(RowState{Time: openHour})

	if want := []int{10}; !slices.Equal(m.attempts, want) {
		t.Fatalf("attempts = %v, want %v — RejectRetryLotStep=0 must disable the ladder", m.attempts, want)
	}
	if e.Working() {
		t.Fatal("nothing may rest after a failed attempt with the ladder off")
	}
}

// The rate limiter is re-consulted before EACH rung, not just the first: a reject storm
// cannot silently spend past the shared placeOrder budget by looping past what Allow denies.
func TestRejectRetryLadderRechecksQuotaMidLadder(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: 1, IsClose: true, Lots: 10}}
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 10, HedgeRetries: 2,
		RejectRetryLotStep: 2, RejectRetryMinLots: 2,
	}, m, tk, dm)
	lim := &stubLimiter{ok: true, allowN: 1, retryAt: openHour.Add(10 * time.Second)}
	e.SetLimiter(lim)
	seedBooks(e, openHour)
	m.placeErr = map[string]error{testLegA: status.Error(codes.InvalidArgument, "insufficient funds")}

	e.OnState(RowState{Time: openHour})

	if want := []int{10}; !slices.Equal(m.attempts, want) {
		t.Fatalf("attempts = %v, want %v — the 2nd rung's Allow was denied, so it must never place", m.attempts, want)
	}
	if e.Working() {
		t.Fatal("nothing may rest once the quota denies the next rung")
	}
}

// A dual-passive clip whose legB is rejected AFTER legA already rested is NEVER retried
// smaller: legA may have caught a race fill before its cancel (see openClip), and retrying
// the pair at a reduced size on top of that would risk overshooting the Decider's intended
// size. Only a clip's very first placement attempt is ladder-eligible.
func TestRejectRetryLadderDoesNotShrinkLegBAfterLegAPlaced(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: 1, Lots: 10, ExecMode: ExecDualPassive}}
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 10, HedgeRetries: 2,
		RejectRetryLotStep: 2, RejectRetryMinLots: 2,
	}, m, tk, dm)
	seedBooks(e, openHour)
	m.placeErr = map[string]error{testLegB: status.Error(codes.InvalidArgument, "insufficient funds")}
	m.placeErrMaxLots = map[string]int{testLegB: 6} // would have been fillable smaller — must not matter

	e.OnState(RowState{Time: openHour})

	if legBAttempts := len(m.attempts) - 1; legBAttempts != 1 { // attempts[0] is legA's single successful placement
		t.Fatalf("legB attempts = %d, want 1 — legA already placed, so no shrink-retry", legBAttempts)
	}
	if e.Working() {
		t.Fatal("failed open must leave the engine idle")
	}
	if m.count("cancel "+m.id(testLegA)) != 1 {
		t.Fatalf("the naked legA order must still be cancelled, got %v", m.calls)
	}
}

// --- reject-retry ladder in taker-only mode: only when BOTH legs cleanly reject ---

// Both legs of a taker-only CLOSING clip come back with a definitive reject — neither
// landed, a genuine clean slate — so the ladder retries the whole pair smaller, exactly like
// the maker path's first-attempt case.
// When both legs of a closing taker-only clip cleanly reject at the full size, each leg
// independently chases its OWN full target through takerRetryFrom's shrink-and-accumulate
// (engine_hedge.go) — the clip closes COMPLETELY (10, not a reduced 6) within this one call,
// which is strictly more complete than the old (removed) clip-level carve-out that shrank the
// whole pair and left the remainder for the next Peek.
func TestRejectRetryLadderClosesTakerOnlyClipInFullWhenBothLegsRejectCleanly(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: 1, IsClose: true, Lots: 10}}
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 10, HedgeRetries: 2, TakerOnly: true,
		RejectRetryLotStep: 2, RejectRetryMinLots: 2,
	}, m, tk, dm)
	seedBooks(e, openHour)
	rej := status.Error(codes.InvalidArgument, "insufficient funds")
	tk.failErrSym = map[string]error{testLegA: rej, testLegB: rej}
	tk.failMaxLots = map[string]int{testLegA: 6, testLegB: 6} // broker accepts ≤6 on both

	e.OnState(RowState{Time: openHour})

	if tk.lots["buy "+testLegA] != 10 {
		t.Fatalf("legA must chase its full 10-lot target via installments, got %v", tk.lots)
	}
	if tk.lots["sell "+testLegB] != 10 {
		t.Fatalf("legB must chase its full 10-lot target via installments, got %v", tk.lots)
	}
	if e.Position() != 10 {
		t.Fatalf("position should commit to the full 10, got %d", e.Position())
	}
	// Each leg: 1 initial concurrent attempt (10, rejected) + shrinkTakerChase's 10,8,6,4
	// (10/8 rejected, 6/4 accepted) = 5 attempts × 2 legs.
	if want := 10; len(tk.attempts) != want {
		t.Fatalf("attempts = %v (%d), want %d entries (5 per leg)", tk.attempts, len(tk.attempts), want)
	}
	if e.Impaired() {
		t.Fatal("a fully-chased close must not leave the engine impaired")
	}
}

// legA lands for real while legB is rejected: the pair is no longer a clean slate, so legB
// must NOT shrink — it falls through to the existing takerRetryFrom/hedge-debt path at its
// full, un-shrunk size (legA already committed that size, and legB must match it exactly).
// When legA lands and legB's hedge is definitively rejected at the full size but the
// broker accepts smaller ones, takerRetryFrom's shrink-and-accumulate (engine_hedge.go)
// must chase legB up to the SAME 10-lot total legA already moved — no debt, no impaired
// mode, since the obligation is fully paid, just via more than one order.
func TestRejectRetryLadderRecoversTakerOnlyWhenOneLegLands(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: 1, IsClose: true, Lots: 10}}
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 10, HedgeRetries: 1, TakerOnly: true,
		RejectRetryLotStep: 2, RejectRetryMinLots: 2,
	}, m, tk, dm)
	seedBooks(e, openHour)
	tk.failErrSym = map[string]error{testLegB: status.Error(codes.InvalidArgument, "insufficient funds")}
	tk.failMaxLots = map[string]int{testLegB: 6} // broker accepts legB orders up to 6 lots

	e.OnState(RowState{Time: openHour})

	if tk.lots["buy "+testLegA] != 10 {
		t.Fatalf("legA must land at its full, un-shrunk size, got %v", tk.lots)
	}
	if tk.lots["sell "+testLegB] != 10 {
		t.Fatalf("legB must chase the same 10-lot total legA moved, got %v", tk.lots)
	}
	if e.Position() != 10 {
		t.Fatalf("position should be +10, got %d", e.Position())
	}
	if e.Impaired() {
		t.Fatal("a fully-chased hedge must not go impaired")
	}
}

// When legB is rejected at every size down to the floor (a genuinely stuck broker, not a
// per-order-size limit), the shrink ladder cannot recover: it must exhaust every rung and
// still fall through to the unchanged debt/impaired fallback, exactly as before the ladder
// existed — trying harder must never mean trying forever.
func TestRejectRetryLadderDebtsTakerOnlyWhenFloorStillRejects(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: 1, IsClose: true, Lots: 10}}
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 10, HedgeRetries: 1, TakerOnly: true,
		RejectRetryLotStep: 2, RejectRetryMinLots: 2,
	}, m, tk, dm)
	seedBooks(e, openHour)
	tk.failErrSym = map[string]error{testLegB: status.Error(codes.InvalidArgument, "insufficient funds")}

	e.OnState(RowState{Time: openHour})

	if tk.lots["buy "+testLegA] != 10 {
		t.Fatalf("legA must land at its full, un-shrunk size, got %v", tk.lots)
	}
	// The ladder must have tried every rung (10, 8, 6, 4, 2) on top of the initial
	// concurrent attempt, and only THEN given up: 6 legB attempts total.
	if got := countAttempts(tk.attempts, testLegB); got != 6 {
		t.Fatalf("legB attempts = %d, want 6 (initial + 5 shrink rungs down to the floor)", got)
	}
	if e.Position() != 10 {
		t.Fatalf("Commit must fire regardless, position should be +10, got %d", e.Position())
	}
	if !e.Impaired() {
		t.Fatal("legB's exhausted ladder must still queue a hedge debt and enter impaired mode")
	}
}

// Both legs fail, but with an AMBIGUOUS (transport) error, not a definitive reject: the
// clean-slate carve-out must not fire — an order may already rest at the broker, so shrinking
// and resending risks a duplicate. Falls through to the existing per-leg retry/debt path.
func TestRejectRetryLadderDoesNotShrinkTakerOnlyOnAmbiguousError(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: 1, IsClose: true, Lots: 10}}
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 10, HedgeRetries: 1, TakerOnly: true,
		RejectRetryLotStep: 2, RejectRetryMinLots: 2,
	}, m, tk, dm)
	seedBooks(e, openHour)
	amb := status.Error(codes.Unavailable, "temporarily unavailable")
	tk.failErrSym = map[string]error{testLegA: amb, testLegB: amb}

	e.OnState(RowState{Time: openHour})

	for _, a := range tk.attempts {
		if a != testLegA+":10" && a != testLegB+":10" {
			t.Fatalf("attempts = %v — an ambiguous error must never shrink the size", tk.attempts)
		}
	}
	if !e.Impaired() {
		t.Fatal("exhausted retries on both legs must queue hedge debts and enter impaired mode")
	}
}

// An OPENING taker-only clip (!IsClose) never shrinks even when both legs cleanly reject at
// a size the broker would have accepted smaller — same entry-sizing invariant as the maker
// path (see TestRejectRetryLadderNeverAppliesToOpeningClips).
func TestRejectRetryLadderNeverAppliesToOpeningTakerOnlyClips(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: 1, Lots: 10}} // IsClose: false
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 10, HedgeRetries: 1, TakerOnly: true,
		RejectRetryLotStep: 2, RejectRetryMinLots: 2,
	}, m, tk, dm)
	seedBooks(e, openHour)
	rej := status.Error(codes.InvalidArgument, "insufficient funds")
	tk.failErrSym = map[string]error{testLegA: rej, testLegB: rej}
	tk.failMaxLots = map[string]int{testLegA: 6, testLegB: 6} // would have been fillable smaller — must not matter

	e.OnState(RowState{Time: openHour})

	for _, a := range tk.attempts {
		if a != testLegA+":10" && a != testLegB+":10" {
			t.Fatalf("attempts = %v — an opening clip must never shrink-retry", tk.attempts)
		}
	}
	if !e.Impaired() {
		t.Fatal("exhausted retries on both legs must queue hedge debts and enter impaired mode")
	}
}

// --- abandon: the clip drops via a signal revert or a timeout, with or without a caught fill ---

// The desync this round fixes: a signal revert abandons an (event-wise) unfilled clip, but
// the cancel catches a fill in flight. The old handling equalized the catch (pair 1:1 at the
// broker) but never committed — the Decider's book stayed flat, so the broker and internal
// positions diverged FOREVER and the two-pass reconcile tripped the kill-switch on a routine
// race. Under spread semantics (once a leg begins filling, the clip completes) the catch now
// folds as the first fill: both legs completed to target, the lot committed.
func TestAbandonCatchCompletesAndCommits(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour)) // buy clip: bid legA, ask legB
	bidA := m.id(testLegA)

	m.executed = map[string]int{bidA: 1}            // legA catches 1 lot during the abandon's cancel
	e.OnState(holdState(openHour.Add(time.Second))) // signal gone → abandon

	if e.Working() || e.Halted() {
		t.Fatalf("clip must be resolved cleanly, working=%v halted=%v", e.Working(), e.Halted())
	}
	// legA: 1 caught + 1 taker = 2; legB: 2 taker = 2.
	if got := tk.lots["buy "+testLegA]; got != 1 {
		t.Fatalf("legA must be completed to target with a 1-lot taker, got %d (%v)", got, tk.lots)
	}
	if got := tk.lots["sell "+testLegB]; got != 2 {
		t.Fatalf("legB must be completed to target with a 2-lot taker, got %d (%v)", got, tk.lots)
	}
	if e.Position() != 2 {
		t.Fatalf("the caught clip must be COMMITTED (the broker really holds it), got position %d", e.Position())
	}

	// The whole point: broker and internal positions now agree, so reconcile stays green.
	e.Reconcile(2, -2)
	e.Reconcile(2, -2)
	if e.Halted() {
		t.Fatal("a booked catch must not trip the reconcile kill-switch")
	}

	// The caught lot's fill event streams in late: already accounted via the cancel-ack.
	takers := len(tk.calls)
	e.OnFill(openHour.Add(2*time.Second), bidA, testLegA, true, 1, 100)
	if len(tk.calls) != takers || e.Position() != 2 {
		t.Fatalf("the late caught-lot event must dedup, takers=%v pos=%d", tk.calls, e.Position())
	}
}

// The same catch discovered by the fill-timeout backstop (not a signal revert) books the
// clip the same way — the discovery path must not change the outcome.
func TestAbandonCatchOnTimeoutCompletesToo(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2) // FillTimeout 2m
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	askB := m.id(testLegB)

	m.executed = map[string]int{askB: 2}    // legB catches its FULL size during the timeout's cancel
	e.OnTick(openHour.Add(3 * time.Minute)) // timeout fires the abandon

	if e.Working() || e.Halted() {
		t.Fatalf("clip must be resolved cleanly, working=%v halted=%v", e.Working(), e.Halted())
	}
	// legB: 2 caught (no taker); legA: 2 taker.
	if got := tk.lots["buy "+testLegA]; got != 2 {
		t.Fatalf("legA must be completed to target x2, got %d (%v)", got, tk.lots)
	}
	if got := tk.lots["sell "+testLegB]; got != 0 {
		t.Fatalf("legB is already full — no taker, got %d (%v)", got, tk.lots)
	}
	if e.Position() != 2 {
		t.Fatalf("the caught clip must be committed, got position %d", e.Position())
	}
}

// Catches on BOTH legs' cancels complete to target once each — no per-catch blind hedging.
func TestAbandonCatchBothLegsCompletesOnce(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 3)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	bidA, askB := m.id(testLegA), m.id(testLegB)

	m.executed = map[string]int{bidA: 1, askB: 2}
	e.OnState(holdState(openHour.Add(time.Second)))

	// legA: 1 caught + 2 taker = 3; legB: 2 caught + 1 taker = 3.
	if got := tk.lots["buy "+testLegA]; got != 2 {
		t.Fatalf("legA topped to target by 2, got %d (%v)", got, tk.lots)
	}
	if got := tk.lots["sell "+testLegB]; got != 1 {
		t.Fatalf("legB topped to target by 1, got %d (%v)", got, tk.lots)
	}
	if e.Position() != 3 || e.Halted() {
		t.Fatalf("clip must commit at target, pos=%d halted=%v", e.Position(), e.Halted())
	}

	// Both catches stream in late — all deduped.
	takers := len(tk.calls)
	e.OnFill(openHour.Add(2*time.Second), bidA, testLegA, true, 1, 100)
	e.OnFill(openHour.Add(2*time.Second), askB, testLegB, false, 2, 51)
	if len(tk.calls) != takers {
		t.Fatalf("late caught-lot events must dedup, got %v", tk.calls)
	}
}

// Under KeepPartialOpenOnTimeout (basis) an abandoned OPENING clip keeps the old semantics:
// the catch is equalized with one minimal taker and the clip is dropped WITHOUT a commit —
// basis's position is fill-derived (the ledger/sink), so nothing desyncs.
func TestAbandonCatchKeepPartialEqualizesWithoutCommit(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: 1, Lots: 2}}
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 2,
		FillTimeout: time.Minute, HedgeRetries: 2, KeepPartialOpenOnTimeout: true,
	}, m, tk, dm)
	sink := &recordSink{}
	e.SetFillSink(sink)
	seedBooks(e, openHour)
	e.OnState(RowState{Time: openHour})
	bidA := m.id(testLegA)

	m.executed = map[string]int{bidA: 1}
	dm.intent = Intent{Action: actionHold}               // signal decays
	e.OnState(RowState{Time: openHour.Add(time.Second)}) // abandon via signal loss

	if e.Working() || e.Halted() {
		t.Fatalf("clip must be dropped cleanly, working=%v halted=%v", e.Working(), e.Halted())
	}
	// legA: 1 caught; legB: 1 equalizer. NO chase to target 2, NO commit.
	if got := tk.lots["sell "+testLegB]; got != 1 {
		t.Fatalf("the catch must be equalized x1, got %d (%v)", got, tk.lots)
	}
	if got := tk.lots["buy "+testLegA]; got != 0 {
		t.Fatalf("keep-partial must not taker-chase legA, got %d (%v)", got, tk.lots)
	}
	if dm.pos != 0 {
		t.Fatalf("keep-partial must NOT commit (the ledger carries the partial), got pos %d", dm.pos)
	}
	if sink.netA != 1 || sink.netB != -1 {
		t.Fatalf("the sink must carry the equalized pair, netA=%d netB=%d", sink.netA, sink.netB)
	}
}

// The clean abandon (no catch) stays exactly as before: both passives cancelled, no takers,
// no commit — the completion path must never fire without a catch.
func TestAbandonWithoutCatchStaysFlat(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	legAID, legBID := m.id(testLegA), m.id(testLegB)

	e.OnState(holdState(openHour.Add(time.Second)))

	if e.Working() || e.Position() != 0 || len(tk.calls) != 0 {
		t.Fatalf("clean abandon must stay flat, working=%v pos=%d takers=%v", e.Working(), e.Position(), tk.calls)
	}
	if m.count("cancel "+legAID) != 1 || m.count("cancel "+legBID) != 1 {
		t.Fatalf("both passives must be cancelled exactly once, got %v", m.calls)
	}
}

// --- downward ack contradiction: fewer lots acked than already folded, the acted-on book stands ---

// The engine folded (and hedged) 3 stream lots; the teardown's cancel-ack then claims only
// 2 ever executed. There is no safe auto-repair — un-hedging on a self-contradicting
// broker's say-so could strand a naked leg — so the acted-on book stands: no reverse taker,
// no halt, the completion still lands at target, and the sink keeps every acted-on lot.
func TestAckBelowFoldedKeepsActedOnBook(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 4)
	sink := &recordSink{}
	e.SetFillSink(sink)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	bidA := m.id(testLegA)

	e.OnFill(openHour, bidA, testLegA, true, 3, 100) // 3 stream lots folded and hedged
	if got := tk.lots["sell "+testLegB]; got != 3 {
		t.Fatalf("the partial must be hedged x3 first, got %d (%v)", got, tk.lots)
	}

	m.executed = map[string]int{bidA: 2}            // the ack contradicts the stream downward
	e.OnState(holdState(openHour.Add(time.Second))) // signal revert → resolve (partial → complete)

	if e.Halted() {
		t.Fatal("a downward ack contradiction must not halt — reconcile owns the verdict")
	}
	if e.Working() {
		t.Fatal("the clip must be resolved")
	}
	// Completion to target 4: legA 3 folded + 1 taker; legB 3 hedges + 1 completion taker.
	if got := tk.lots["buy "+testLegA]; got != 1 {
		t.Fatalf("legA must be completed to target with 1 taker, got %d (%v)", got, tk.lots)
	}
	if got := tk.lots["sell "+testLegB]; got != 4 {
		t.Fatalf("legB must total 4 sells, got %d (%v)", got, tk.lots)
	}
	if sink.netA != 4 || sink.netB != -4 {
		t.Fatalf("the sink must keep every acted-on lot, netA=%d netB=%d (%v)", sink.netA, sink.netB, sink.events)
	}
}

// --- account-truth hedging: a garbled fill event cannot skip or misdirect a hedge ---

// A CLIP leg's fill with a garbled symbol still runs the normal clip path (the id binds it
// to the leg): maker designation, counterpart retire and the completion hedge all work.
func TestClipFillGarbledSymbolStillPairs(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	bidA := m.id(testLegA)
	m.executed = map[string]int{bidA: 2}

	e.OnFill(openHour, bidA, testLegA+"@MISX", false /* garbled side too */, 2, 100)

	if got := tk.lots["sell "+testLegB]; got != 2 {
		t.Fatalf("the maker fill must hedge legB x2 despite the garbled event, got %v", tk.lots)
	}
	if e.Working() || e.Position() != 2 || e.Halted() {
		t.Fatalf("clip must commit normally, working=%v pos=%d halted=%v", e.Working(), e.Position(), e.Halted())
	}
}

// TestHedgeRatioScalesSettleTopUp: when a clip is completed to target, the LegB top-up is
// sized in LegB contracts — settle counts in LegA lots (`want`), and only the order it
// issues is converted. A 1:1 top-up here would silently leave the pair R-fold naked on the
// very path that exists to guarantee balance.
func TestHedgeRatioScalesSettleTopUp(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newSoloRatioTestEngine(m, tk, 4, 10)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	legAID := m.id(testLegA)

	e.OnFill(openHour, legAID, testLegA, true, 1, 100) // 1 of 4 lots fills → hedged 1×10
	e.OnState(holdState(openHour.Add(time.Second)))    // signal gone → complete the clip to target

	if got := tk.lots["buy "+testLegA]; got != 3 {
		t.Fatalf("LegA must be topped up by the 3 unfilled lots, got %d (%v)", got, tk.lots)
	}
	if got := tk.lots["sell "+testLegB]; got != 40 {
		t.Fatalf("LegB must end at 4 lots × ratio 10 = 40 contracts, got %d (%v)", got, tk.lots)
	}
}

// TestHedgeRatioScalesCancelCaughtFill: a cancel that catches lots in flight is EQUALIZED at
// the ratio — the last thing standing between a caught fill and a naked leg. Run under
// KeepPartialOpenOnTimeout (basis's setting), where an abandoned clip keeps what filled
// instead of being completed to target, so the assertion isolates the equalizing taker.
func TestHedgeRatioScalesCancelCaughtFill(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 4,
		FillTimeout: 2 * time.Minute, HedgeRetries: 2,
		SoloMakerLeg: true, HedgeRatio: 10, KeepPartialOpenOnTimeout: true,
	}, m, tk, newTestDecider(20, 4))
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	legAID := m.id(testLegA)
	m.executed = map[string]int{legAID: 2} // the cancel will reveal 2 filled lots

	e.OnState(holdState(openHour.Add(time.Second))) // nothing filled per the stream → abandon

	if got := tk.lots["buy "+testLegA]; got != 0 {
		t.Fatalf("an abandoned clip must not chase the unfilled remainder, got a %d-lot LegA taker", got)
	}
	if got := tk.lots["sell "+testLegB]; got != 20 {
		t.Fatalf("2 lots caught by the cancel must be hedged with 20 LegB contracts, got %d (%v)", got, tk.lots)
	}
}

// --- Intent.ExecMode: one clip's execution shape overriding the engine-wide mode ---

// TestIntentExecTakerCrossesBothLegsUnderMakerConfig: a clip carrying ExecTaker crosses both
// legs immediately even though the engine is configured dual-passive — the per-clip mode wins.
func TestIntentExecTakerCrossesBothLegsUnderMakerConfig(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: -1, IsClose: true, Lots: 2, ExecMode: ExecTaker}}
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 2,
		FillTimeout: time.Minute, HedgeRetries: 2,
	}, m, tk, dm)
	seedBooks(e, openHour)

	e.OnState(RowState{Time: openHour})

	if len(m.calls) != 0 {
		t.Fatalf("ExecTaker must rest no passive order, got maker calls %v", m.calls)
	}
	if tk.lots["sell "+testLegA] != 2 || tk.lots["buy "+testLegB] != 2 {
		t.Fatalf("ExecTaker must cross both legs, got %v", tk.lots)
	}
	if e.Working() {
		t.Fatal("a taker clip completes synchronously — no working clip should remain")
	}
}

// TestIntentExecSoloMakerRestsLegAUnderTakerOnly: the case force-flatten needs — a clip asking
// for solo-maker rests LegA passively even though the engine is configured taker-only.
func TestIntentExecSoloMakerRestsLegAUnderTakerOnly(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: -1, IsClose: true, Lots: 2, ExecMode: ExecSoloMaker}}
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 2,
		FillTimeout: time.Minute, HedgeRetries: 2, TakerOnly: true,
	}, m, tk, dm)
	seedBooks(e, openHour)

	e.OnState(RowState{Time: openHour})

	if m.count("ask "+testLegA) != 1 {
		t.Fatalf("ExecSoloMaker must rest a LegA ask for a sell, got maker calls %v", m.calls)
	}
	if len(tk.calls) != 0 {
		t.Fatalf("ExecSoloMaker must not cross anything before the passive fills, got %v", tk.calls)
	}
	if !e.Working() {
		t.Fatal("a passive clip must be working while its order rests")
	}
}

// TestIntentExecDualPassiveRestsBothLegsUnderTakerOnly pins the other maker shape: both legs
// rest, again overriding a taker-only engine.
func TestIntentExecDualPassiveRestsBothLegsUnderTakerOnly(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: -1, IsClose: true, Lots: 2, ExecMode: ExecDualPassive}}
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 2,
		FillTimeout: time.Minute, HedgeRetries: 2, TakerOnly: true,
	}, m, tk, dm)
	seedBooks(e, openHour)

	e.OnState(RowState{Time: openHour})

	if m.count("ask "+testLegA) != 1 || m.count("bid "+testLegB) != 1 {
		t.Fatalf("ExecDualPassive must rest BOTH legs, got maker calls %v", m.calls)
	}
	if len(tk.calls) != 0 {
		t.Fatalf("ExecDualPassive must not cross anything at open, got %v", tk.calls)
	}
}

// TestIntentExecDefaultKeepsConfiguredMode: the zero value changes nothing — a taker-only
// engine still crosses both legs, which is what every existing config relies on.
func TestIntentExecDefaultKeepsConfiguredMode(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: -1, IsClose: true, Lots: 2}} // ExecDefault
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 2,
		FillTimeout: time.Minute, HedgeRetries: 2, TakerOnly: true,
	}, m, tk, dm)
	seedBooks(e, openHour)

	e.OnState(RowState{Time: openHour})

	if len(m.calls) != 0 {
		t.Fatalf("ExecDefault under taker-only must rest nothing, got %v", m.calls)
	}
	if tk.lots["sell "+testLegA] != 2 {
		t.Fatalf("ExecDefault under taker-only must cross LegA, got %v", tk.lots)
	}
}

// TestWorkingClipResolvesWhenExecModeChanges is the cutoff behaviour: a passive close is
// resting when the strategy switches the SAME direction to ExecTaker (the force-flatten
// taker phase begins). The clip must not sit out its FillTimeout — it is resolved at once,
// and ForceCloseOnTimeout finishes the reduction with a taker cross.
func TestWorkingClipResolvesWhenExecModeChanges(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: -1, IsClose: true, Lots: 2, ExecMode: ExecSoloMaker}}
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 2,
		FillTimeout: time.Hour, HedgeRetries: 2, TakerOnly: true, ForceCloseOnTimeout: true,
	}, m, tk, dm)
	seedBooks(e, openHour)
	e.OnState(RowState{Time: openHour})
	if !e.Working() {
		t.Fatal("setup: the passive close should be resting")
	}

	dm.intent.ExecMode = ExecTaker // cutoff crossed: same direction, different execution
	e.OnState(RowState{Time: openHour.Add(time.Second)})

	if e.Working() {
		t.Fatal("a resting clip whose execution mode no longer matches must be resolved, not left to its FillTimeout")
	}
	if tk.lots["sell "+testLegA] != 2 || tk.lots["buy "+testLegB] != 2 {
		t.Fatalf("the reduction must be taker-completed at the cutoff, got %v", tk.lots)
	}
}

// TestIntentExecDualPassiveWithHedgeRatioFallsBackToSolo: a ratio pair cannot rest LegB (the
// conversion would have to run both ways — see EngineConfig.HedgeRatio), so a clip asking for
// dual-passive is downgraded to solo-maker rather than resting a leg it cannot convert back.
func TestIntentExecDualPassiveWithHedgeRatioFallsBackToSolo(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: -1, IsClose: true, Lots: 2, ExecMode: ExecDualPassive}}
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 2,
		FillTimeout: time.Minute, HedgeRetries: 2, TakerOnly: true, HedgeRatio: 10,
	}, m, tk, dm)
	seedBooks(e, openHour)

	e.OnState(RowState{Time: openHour})

	if m.count("ask "+testLegA) != 1 {
		t.Fatalf("LegA must still rest, got maker calls %v", m.calls)
	}
	if m.count("bid "+testLegB) != 0 {
		t.Fatalf("LegB must NOT rest at a hedge ratio > 1, got maker calls %v", m.calls)
	}
}

// TestIntentExecSoloMakerLegBRestsPerpOnly: the mirror of ExecSoloMaker — LegB (the perp) is
// the sole passive and LegA is taker-hedged on its fill. Which leg is worth resting is a
// property of the pair's spreads, not of the engine: LegA's half-spread is narrower than
// LegB's, so the wider leg is the one a passive can actually earn on.
func TestIntentExecSoloMakerLegBRestsPerpOnly(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: -1, IsClose: true, Lots: 2, ExecMode: ExecSoloMakerLegB}}
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 2,
		FillTimeout: time.Minute, HedgeRetries: 2, TakerOnly: true,
	}, m, tk, dm)
	seedBooks(e, openHour)

	e.OnState(RowState{Time: openHour})

	if got := m.count("bid " + testLegB); got != 1 {
		t.Fatalf("LegB should rest as the sole passive (a sell clip bids the perp), got %d (calls %v)", got, m.calls)
	}
	if n := m.count("bid "+testLegA) + m.count("ask "+testLegA); n != 0 {
		t.Fatalf("LegA must NOT rest in leg-B solo mode, got %d placements (calls %v)", n, m.calls)
	}
	if !e.Working() {
		t.Fatal("a passive clip must be working while its order rests")
	}
}

// TestExecSoloMakerLegBHedgesLegAOnFill: the sole LegB passive filling crosses LegA with a
// taker for the same lots — the pair stays balanced, mirroring the LegA-solo path.
func TestExecSoloMakerLegBHedgesLegAOnFill(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: -1, IsClose: true, Lots: 2, ExecMode: ExecSoloMakerLegB}}
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 2,
		FillTimeout: time.Minute, HedgeRetries: 2, TakerOnly: true,
	}, m, tk, dm)
	seedBooks(e, openHour)
	e.OnState(RowState{Time: openHour})
	legBID := m.id(testLegB)

	e.OnFill(openHour, legBID, testLegB, true, 2, 50) // the sole maker leg fills fully

	if tk.lots["sell "+testLegA] != 2 {
		t.Fatalf("LegA should be taker-hedged with a 2-lot sell, got taker calls %v", tk.calls)
	}
	if e.Working() {
		t.Fatal("clip should have committed after the sole maker leg filled fully")
	}
}

// TestExecSoloMakerLegBRepegsOnlyTheRestingLeg pins the re-peg gate to the CLIP's shape rather
// than the engine's configuration: with LegB resting, a moved LegA touch must not re-place
// anything (there is no LegA order), while a moved LegB touch re-pegs the passive.
func TestExecSoloMakerLegBRepegsOnlyTheRestingLeg(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: -1, IsClose: true, Lots: 2, ExecMode: ExecSoloMakerLegB}}
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 2,
		FillTimeout: time.Minute, HedgeRetries: 2, TakerOnly: true,
	}, m, tk, dm)
	seedBooks(e, openHour)
	e.OnState(RowState{Time: openHour})
	placed := len(m.calls)

	e.OnBook(testLegA, openHour.Add(time.Second), 102, 103) // LegA touch moves — nothing rests there
	if len(m.calls) != placed {
		t.Fatalf("a moved LegA touch must not touch orders in leg-B solo mode, got %v", m.calls[placed:])
	}

	e.OnBook(testLegB, openHour.Add(2*time.Second), 51, 52) // our bid at 50 is now behind
	if m.count("bid "+testLegB) != 2 {
		t.Fatalf("the resting LegB passive must be re-pegged to the new touch, got %v", m.calls)
	}
}

// TestExecSoloMakerLegBWithHedgeRatioFallsBackToLegA: at a ratio, LegB contracts cannot be
// converted back to whole LegA lots (see EngineConfig.HedgeRatio), so resting LegB is refused
// and the clip falls back to the LegA-solo shape, whose conversion runs the safe way.
func TestExecSoloMakerLegBWithHedgeRatioFallsBackToLegA(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: -1, IsClose: true, Lots: 2, ExecMode: ExecSoloMakerLegB}}
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 2,
		FillTimeout: time.Minute, HedgeRetries: 2, TakerOnly: true, HedgeRatio: 10,
	}, m, tk, dm)
	seedBooks(e, openHour)

	e.OnState(RowState{Time: openHour})

	if m.count("ask "+testLegA) != 1 {
		t.Fatalf("LegA must rest instead at a hedge ratio, got %v", m.calls)
	}
	if n := m.count("bid "+testLegB) + m.count("ask "+testLegB); n != 0 {
		t.Fatalf("LegB must NOT rest at a hedge ratio, got %v", m.calls)
	}
}
