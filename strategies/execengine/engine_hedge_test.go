package execengine

// Тесты хеджа: taker по первому филлу, добор при перевыполнении, хедж чужого
// (stray) филла, долг при отказе, поведение с мёртвым тейкером.
// Зеркалит engine_hedge.go.

import (
	"errors"
	"testing"
	"time"
)

// --- first fill hedges the other leg: direction, counterpart races, and the no-taker case when both legs land passively ---

func TestFirstFillHedgesAndCommits(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))

	legAID, legBID := m.id(testLegA), m.id(testLegB)
	// Leg A fills fully first (we bought leg A).
	e.OnFill(openHour, legAID, testLegA, true, 2, 100)

	if tk.lots["sell "+testLegB] != 2 {
		t.Fatalf("expected taker sell of leg B x2, got %v", tk.lots)
	}
	if m.count("cancel "+legBID) != 1 {
		t.Fatalf("expected the leg B passive %s to be cancelled, got %v", legBID, m.calls)
	}
	if e.Working() {
		t.Fatal("clip should be closed after a full maker fill")
	}
	if e.Position() != 2 {
		t.Fatalf("position should be +2 after committing the lot, got %d", e.Position())
	}
}

func TestLegBFillsFirstHedgesLegA(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))

	legAID, legBID := m.id(testLegA), m.id(testLegB)
	// The leg B passive (a sell/ask) fills first.
	e.OnFill(openHour, legBID, testLegB, false, 2, 51)

	if tk.lots["buy "+testLegA] != 2 {
		t.Fatalf("expected taker buy of leg A x2, got %v", tk.lots)
	}
	if m.count("cancel "+legAID) != 1 {
		t.Fatalf("expected the leg A passive %s to be cancelled, got %v", legAID, m.calls)
	}
	if e.Position() != 2 {
		t.Fatalf("position should be +2, got %d", e.Position())
	}
}

// The counterpart passive filled before its cancel: the engine must hedge only the GAP
// (makerFilled − executed), not the full maker fill, so the legs land 1:1 and no halt fires.
func TestCounterpartRaceNetHedges(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 4)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour)) // long: bid leg A, ask leg B
	legBID := m.id(testLegB)
	m.executed = map[string]int{legBID: 2} // leg B passively filled 2 before its cancel
	legAID := m.id(testLegA)

	e.OnFill(openHour, legAID, testLegA, true, 4, 100) // maker leg A fills 4

	if got := tk.lots["sell "+testLegB]; got != 2 {
		t.Fatalf("expected net hedge sell leg B x2 (4−2), got %d (%v)", got, tk.lots)
	}
	if e.Halted() {
		t.Fatal("a net-hedged race must not halt")
	}
	if e.Position() != 4 {
		t.Fatalf("clip must still complete to target 4, got %d", e.Position())
	}
}

// Both legs filled passively: the gap is zero, so NO taker fires — both legs are already
// hedged at maker prices.
func TestBothPassiveNeedsNoTaker(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 4)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	m.executed = map[string]int{m.id(testLegB): 4}
	legAID := m.id(testLegA)

	e.OnFill(openHour, legAID, testLegA, true, 4, 100)

	if got := tk.lots["sell "+testLegB]; got != 0 {
		t.Fatalf("both-passive fill needs no hedge, got sell leg B x%d", got)
	}
	if e.Halted() || e.Position() != 4 {
		t.Fatalf("expected committed 1:1 at 4 with no halt, pos=%d halted=%v", e.Position(), e.Halted())
	}
}

// --- stray fills outside the live clip: late arrivals and races must be hedged, taker/foreign echoes and a halt must not misfire one ---

// TestLateFillAfterCancelIsHedged pins the stray-fill hedge: a passive that fills in the
// race window after its clip was cancelled (the signal reverted, the cancel had not landed
// yet) is real inventory at the broker, so the engine must taker-hedge the other leg
// instead of ignoring the fill and leaving a naked leg for reconcile to halt on.
func TestLateFillAfterCancelIsHedged(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour)) // bid leg A / ask leg B
	legAID := m.id(testLegA)

	e.OnState(holdState(openHour.Add(time.Second))) // signal revert cancels the clip
	if e.Working() {
		t.Fatal("the unwanted clip should be cancelled")
	}
	// The leg A bid had already filled at the exchange; its fill arrives after the drop.
	e.OnFill(openHour.Add(2*time.Second), legAID, testLegA, true, 2, 100)

	if tk.lots["sell "+testLegB] != 2 {
		t.Fatalf("a late fill on a cancelled passive must be hedged (sell leg B x2), got %v", tk.lots)
	}
	if e.Halted() {
		t.Fatal("a hedged late fill must not halt the engine")
	}
}

// TestNonMakerRaceFillIsHedged pins the in-clip race branch: when the non-maker leg fills
// after the maker was designated (its cancel raced the fill), the increment must be hedged
// on the other leg — not left naked until reconciliation halts on it.
func TestNonMakerRaceFillIsHedged(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour)) // bid leg A / ask leg B
	legAID, legBID := m.id(testLegA), m.id(testLegB)

	e.OnFill(openHour, legAID, testLegA, true, 1, 100) // partial maker fill: legA is the maker
	// The leg B ask (already cancelled) fills in the race window while the clip still works.
	e.OnFill(openHour, legBID, testLegB, false, 2, 51)

	if tk.lots["buy "+testLegA] != 2 {
		t.Fatalf("the raced non-maker fill must be hedged (buy leg A x2), got %v", tk.lots)
	}
	if e.Halted() {
		t.Fatal("a hedged race fill must not halt the engine")
	}
}

// TestTakerAndForeignFillsAreNotRehedged guards the stray-fill hedge's filter: an own
// TAKER fill is itself a hedge and a foreign order id is not ours — neither may trigger
// another taker cross (that would pyramid the position on every hedge fill).
func TestTakerAndForeignFillsAreNotRehedged(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	e.OnFill(openHour, m.id(testLegA), testLegA, true, 2, 100) // maker fill → taker hedge placed
	hedgeID := tk.lastID
	calls := len(tk.calls)

	e.OnFill(openHour, hedgeID, testLegB, false, 2, 51) // the hedge's own fill arrives
	e.OnFill(openHour, "foreign", testLegA, true, 5, 100)

	if len(tk.calls) != calls {
		t.Fatalf("taker/foreign fills must not place more taker orders, got %v", tk.calls)
	}
}

// TestNoStrayHedgeWhileHalted keeps the kill-switch absolute: a late passive fill arriving
// after a halt is logged for the operator but must not place orders.
func TestNoStrayHedgeWhileHalted(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	legAID := m.id(testLegA)
	e.Halt("test")

	e.OnFill(openHour, legAID, testLegA, true, 2, 100)

	if len(tk.calls) != 0 {
		t.Fatalf("no orders may be placed while halted, got %v", tk.calls)
	}
}

// --- excess and duplicate fills beyond what was acked or placed: hedge exactly the possible gap, never more ---

// A fill exceeding the order's terminal executed count is real unaccounted inventory
// (the broker contradicting its own cancel-ack): pair-hedge the EXCESS only, loudly.
func TestFillBeyondTerminalCountHedgesExcessOnly(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 4)
	seedBooks(e, openHour)

	id, _ := m.PlaceBid(testLegA, 4, 100)
	e.own[id] = &ordAcct{maker: true, final: -1}
	m.executed = map[string]int{id: 2}
	if gap := e.retireOrder(id); gap != 2 {
		t.Fatalf("gap=%d want 2", gap)
	}

	e.OnFill(openHour, id, testLegA, true, 2, 100) // the 2 acked lots stream in → no-op
	if len(tk.calls) != 0 {
		t.Fatalf("acked lots must dedup, got %v", tk.calls)
	}
	e.OnFill(openHour, id, testLegA, true, 1, 100) // 1 lot BEYOND the ack → hedge exactly 1
	if got := tk.lots["sell "+testLegB]; got != 1 {
		t.Fatalf("excess hedge lots=%d want 1 (calls %v)", got, tk.calls)
	}
	if len(tk.calls) != 1 {
		t.Fatalf("exactly one excess hedge, got %v", tk.calls)
	}
}

// A duplicate on a LIVE (never-retired) maker order previously had NO guard at all: it
// re-ran the clip fill path — extra hedge, extra credit, makerFilled inflated past target.
// The placed-size ceiling caps the total at the placement size: the overlap beyond it is
// dropped and the clip still lands exactly 1:1 at target.
func TestLiveMakerFillBeyondPlacedClamped(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 4)
	sink := &recordSink{}
	e.SetFillSink(sink)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	bidA := m.id(testLegA)
	m.executed = map[string]int{bidA: 4}

	e.OnFill(openHour, bidA, testLegA, true, 3, 100) // genuine partial → counterpart retired, top-up 3
	e.OnFill(openHour, bidA, testLegA, true, 3, 100) // 3 reported, only 1 can still exist → act on 1

	// legB: 3-lot top-up after the first fill + 1-lot hedge for the clamped increment = 4.
	if got := tk.lots["sell "+testLegB]; got != 4 {
		t.Fatalf("legB must be hedged to exactly target 4, got %d (%v)", got, tk.lots)
	}
	if got := tk.lots["buy "+testLegA]; got != 0 {
		t.Fatalf("no legA taker may appear, got %d (%v)", got, tk.lots)
	}
	if e.Working() || e.Position() != 4 {
		t.Fatalf("clip must commit at exactly target, working=%v pos=%d", e.Working(), e.Position())
	}
	if sink.netA != 4 || sink.netB != -4 {
		t.Fatalf("sink must carry exactly target per leg, netA=%d netB=%d (%v)", sink.netA, sink.netB, sink.events)
	}
}

// The clamp must NOT swallow the genuine beyond-terminal contradiction: lots past the
// cancel-ack's count but still within the placement size are real unaccounted inventory
// and keep being hedged (the broker contradicting its own ack, not a duplicate).
func TestExcessWithinPlacedStillHedged(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 4)
	seedBooks(e, openHour)

	id, _ := m.PlaceBid(testLegA, 4, 100)
	e.own[id] = &ordAcct{maker: true, sym: testLegA, isBuy: true, price: 100, placed: 4, final: -1}
	m.executed = map[string]int{id: 2}
	if gap := e.retireOrder(id); gap != 2 {
		t.Fatalf("gap=%d want 2", gap)
	}

	e.OnFill(openHour, id, testLegA, true, 2, 100) // the acked lots → dedup
	e.OnFill(openHour, id, testLegA, true, 1, 100) // beyond the ack, within placed → hedge 1
	if got := tk.lots["sell "+testLegB]; got != 1 {
		t.Fatalf("the within-placed excess must be hedged, got %d (%v)", got, tk.lots)
	}

	e.OnFill(openHour, id, testLegA, true, 2, 100) // 2 more reported, only 1 can exist → hedge 1
	if got := tk.lots["sell "+testLegB]; got != 2 {
		t.Fatalf("only the possible lot may be hedged, got %d (%v)", got, tk.lots)
	}
}

// A beyond-terminal print re-delivered in a loop (the old seen-rollback re-hedged it on
// EVERY delivery, unboundedly): the total hedged beyond the ack is now capped at
// placed − final, so the loop stops minting once the order's physical size is exhausted.
func TestRedeliveredExcessHedgeBoundedByPlaced(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 4)
	seedBooks(e, openHour)

	id, _ := m.PlaceBid(testLegA, 4, 100)
	e.own[id] = &ordAcct{maker: true, sym: testLegA, isBuy: true, price: 100, placed: 4, final: -1}
	m.executed = map[string]int{id: 2}
	e.retireOrder(id) // terminal count 2

	e.OnFill(openHour, id, testLegA, true, 2, 100) // the acked lots → dedup
	for i := 0; i < 5; i++ {
		e.OnFill(openHour, id, testLegA, true, 1, 100) // the same beyond-ack print, over and over
	}

	// Only placed − final = 2 lots can physically exist beyond the ack: hedge exactly 2.
	if got := tk.lots["sell "+testLegB]; got != 2 {
		t.Fatalf("beyond-terminal hedging must cap at placed−final=2, got %d (%v)", got, tk.lots)
	}
	if e.Halted() {
		t.Fatal("dropping the impossible re-deliveries must not halt")
	}
}

// The retired counterpart fills BEYOND its terminal count while the clip is still working.
// The excess is hedged as it arrives, the clip keeps working and commits at target, and the
// physical pair stays 1:1 throughout — one lot larger than the Decider's book, which is
// exactly what reconcile exists to flag (the broker lied; an operator must arbitrate).
func TestBeyondAckExcessMidClipKeepsPairsBalanced(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	sink := &recordSink{}
	e.SetFillSink(sink)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	bidA, askB := m.id(testLegA), m.id(testLegB)

	e.OnFill(openHour, bidA, testLegA, true, 1, 100) // maker legA; counterpart retired with ack 0; hedge x1
	if sink.netA != 1 || sink.netB != -1 {
		t.Fatalf("after the partial the pair must be 1:1, netA=%d netB=%d", sink.netA, sink.netB)
	}

	// The counterpart fills 1 lot BEYOND its cancel-ack (terminal count 0) mid-clip.
	e.OnFill(openHour, askB, testLegB, false, 1, 51)
	if sink.netA != 2 || sink.netB != -2 {
		t.Fatalf("the excess must be hedged and credited at once, netA=%d netB=%d (%v)", sink.netA, sink.netB, sink.events)
	}
	if !e.Working() {
		t.Fatal("the excess hedge must not disturb the working clip")
	}

	// The maker finishes its passive size; the clip commits at target 2.
	e.OnFill(openHour, bidA, testLegA, true, 1, 100)
	if e.Working() || e.Halted() || e.Position() != 2 {
		t.Fatalf("clip must commit at target, working=%v halted=%v pos=%d", e.Working(), e.Halted(), e.Position())
	}
	// Physical book: 3:3 (target 2 + 1 excess pair) — balanced, but one pair past the book.
	if sink.netA != 3 || sink.netB != -3 {
		t.Fatalf("the physical pair must stay balanced at 3:3, netA=%d netB=%d", sink.netA, sink.netB)
	}
	// Reconcile sees the extra pair against the Decider's 2 and (correctly) escalates:
	// trading stays suspended for as long as the views disagree — but no self-halt, so a
	// later agreement (the operator squares the account, or the broker corrects itself)
	// resumes trading without a restart.
	e.Reconcile(3, -3)
	if e.Halted() {
		t.Fatal("the first divergent pass must only suspend")
	}
	e.Reconcile(3, -3)
	if e.Halted() {
		t.Fatal("the engine must never halt itself on the persistent extra pair")
	}
	if !e.Suspect() {
		t.Fatal("the persistent broker-side extra pair must keep trading suspended")
	}
}

// --- account-truth hedging: a garbled fill event cannot skip or misdirect a hedge ---

// A stray fill on a live own passive arrives with the broker's OTHER symbol format and a
// garbled side flag. Pre-fix hedgeStrayMakerFill matched the event's symbol against the
// config legs, recognized neither, and silently returned — a naked leg. The order's account
// knows the true leg and side; the hedge must land on the other leg, opposite side.
func TestStrayFillGarbledSymbolStillHedgesViaAccount(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)

	id, _ := m.PlaceBid(testLegA, 2, 100)
	e.own[id] = &ordAcct{maker: true, sym: testLegA, isBuy: true, price: 100, placed: 2, final: -1}

	// No owning clip; the event's symbol/side are garbage relative to the config legs.
	e.OnFill(openHour, id, testLegA+"@MISX", false, 2, 100)

	if got := tk.lots["sell "+testLegB]; got != 2 {
		t.Fatalf("the stray hedge must use the account's leg/side (sell %s x2), got %v", testLegB, tk.lots)
	}
	if len(tk.calls) != 1 {
		t.Fatalf("exactly one hedge, got %v", tk.calls)
	}
}

// The beyond-terminal excess hedge is protected the same way: the event carrying the
// excess uses a different symbol format, and the hedge must still land correctly.
func TestExcessHedgeGarbledSymbolStillHedgesViaAccount(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 4)
	seedBooks(e, openHour)

	id, _ := m.PlaceAsk(testLegB, 4, 51)
	e.own[id] = &ordAcct{maker: true, sym: testLegB, isBuy: false, price: 51, placed: 4, final: -1}
	m.executed = map[string]int{id: 1}
	e.retireOrder(id) // terminal count 1

	e.OnFill(openHour, id, testLegB+"@RTSX", true, 3, 51) // 1 acked + 2 genuine excess, garbled event

	if got := tk.lots["buy "+testLegA]; got != 2 {
		t.Fatalf("the excess hedge must use the account's leg/side (buy %s x2), got %v", testLegA, tk.lots)
	}
}

// --- when the hedge or top-up itself cannot be placed: the shortfall becomes a debt, not a lost leg ---

// A hedge the broker will not accept is CONNECTION trouble, not a reason to give up: the
// maker fills are real (they are committed), the owed hedge becomes a debt, and the engine
// goes impaired — no new clips — until the debt is paid and a clean reconcile confirms.
func TestHedgeFailureGoesImpairedAndPaysTheDebt(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{fail: true}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	legAID := m.id(testLegA)

	e.OnFill(openHour, legAID, testLegA, true, 2, 100)

	if e.Halted() {
		t.Fatal("the engine must never halt itself on a failed hedge")
	}
	if !e.Impaired() {
		t.Fatal("a hedge the broker refuses must enter impaired mode")
	}
	if e.Working() {
		t.Fatal("the completed clip must not stay working")
	}
	if e.Position() != 2 {
		t.Fatalf("the maker's confirmed fills are real — the lot must be committed, got %d", e.Position())
	}
	// No new clips while the hedge is owed.
	e.OnState(buyState(openHour.Add(time.Second)))
	if e.Working() {
		t.Fatal("impaired engine must not open new clips")
	}

	// The broker comes back: the debt is paid, the engine recovers, and one clean
	// reconcile pass later it trades again.
	tk.fail = false
	e.OnTick(openHour.Add(5 * time.Second))
	if e.Impaired() {
		t.Fatal("a paid debt must clear impaired mode")
	}
	if got := tk.lots["sell "+testLegB]; got != 2 {
		t.Fatalf("the owed hedge must be placed once the broker answers, got %d (%v)", got, tk.lots)
	}
	e.Reconcile(2, -2)
	e.OnState(buyState(openHour.Add(6 * time.Second)))
	if !e.Working() {
		t.Fatal("recovered engine must open clips again after the clean reconcile")
	}
}

// A top-up failure MID-teardown (settleClip's first top-up cannot be placed) queues that
// leg's lots as a hedge debt without disturbing the other leg: each leg owes its own lots
// toward 1:1, the healthy one is placed now, the owed one paid when the broker recovers.
func TestFailedTopUpBecomesDebtWithoutBlockingTheOtherLeg(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: 1, Lots: 2}}
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 2,
		FillTimeout: time.Minute, HedgeRetries: 2,
	}, m, tk, dm)
	sink := &recordSink{}
	e.SetFillSink(sink)
	seedBooks(e, openHour)
	e.OnState(RowState{Time: openHour}) // opening long clip: bid legA / ask legB

	e.OnFill(openHour, m.id(testLegA), testLegA, true, 1, 100) // 1 of 2 fills; hedge on legB succeeds
	if tk.lots["sell "+testLegB] != 1 {
		t.Fatalf("expected the partial's hedge first, got %v", tk.lots)
	}

	// legA's taker side dies (e.g. margin) before the fill-timeout completes the clip:
	// completeClip's settleClip needs top-ups on BOTH legs (target 2 > filled 1). The legA
	// top-up fails → its lots become a DEBT (impaired mode); the legB top-up is
	// independent and must still be placed — each leg owes its own lots toward 1:1.
	tk.failSym = map[string]bool{testLegA: true}
	e.OnTick(openHour.Add(2 * time.Minute))

	if e.Halted() {
		t.Fatal("the engine must never halt itself on a failed top-up")
	}
	if !e.Impaired() {
		t.Fatal("an unplaceable top-up must enter impaired mode as a debt")
	}
	if got := tk.lots["sell "+testLegB]; got != 2 {
		t.Fatalf("the healthy legB top-up must still be placed, got %d sells (%v)", got, tk.calls)
	}
	if got := tk.lots["buy "+testLegA]; got != 0 {
		t.Fatalf("the failing legA top-up must not have landed yet, got %d buys", got)
	}

	// legA's taker side recovers: the obligation loop pays the owed lot.
	tk.failSym = nil
	e.OnTick(openHour.Add(2*time.Minute + 5*time.Second))
	if e.Impaired() {
		t.Fatal("a paid debt must clear impaired mode")
	}
	if got := tk.lots["buy "+testLegA]; got != 1 {
		t.Fatalf("the owed legA lot must be paid on recovery, got %d buys (%v)", got, tk.calls)
	}
}

// legB's placement fails, legA caught a fill AND the matching taker fails too: the naked
// gap becomes a hedge DEBT — the engine goes impaired and pays it the moment the broker
// accepts orders again, instead of halting and waiting for an operator.
func TestPlaceLegBFailureThenHedgeFailureQueuesTheDebt(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{fail: true}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	m.placeErr = map[string]error{testLegB: errors.New("reject")}
	m.executed = map[string]int{"b1": 2}

	e.OnState(buyState(openHour))

	if e.Halted() {
		t.Fatal("the engine must never halt itself on an unplaceable hedge")
	}
	if !e.Impaired() {
		t.Fatal("the naked gap must be queued as a debt in impaired mode")
	}
	// The taker side recovers: the owed hedge is paid and the engine comes back.
	tk.fail = false
	e.OnTick(openHour.Add(5 * time.Second))
	if e.Impaired() {
		t.Fatal("a paid debt must clear impaired mode")
	}
	if got := tk.lots["sell "+testLegB]; got != 2 {
		t.Fatalf("the caught legA fill must be matched on legB once the broker answers, got %d (%v)", got, tk.lots)
	}
}

// --- a transient taker blip must retry and land, not immediately become a debt ---

// The taker fails once and succeeds on the retry (HedgeRetries=2): the hedge lands,
// exactly once, with no halt.
func TestTakerTransientFailureRetriesWithoutHalt(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{failN: 1}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))

	e.OnFill(openHour, m.id(testLegA), testLegA, true, 2, 100)

	if e.Halted() {
		t.Fatal("a transient taker failure within HedgeRetries must not halt")
	}
	if got := tk.lots["sell "+testLegB]; got != 2 {
		t.Fatalf("the hedge must land exactly once after the retry, got %d (%v)", got, tk.lots)
	}
	if e.Position() != 2 {
		t.Fatalf("the clip must commit normally, got %d", e.Position())
	}
}

// --- a placed taker is an assumed hedge until the broker's own data confirms it: dead and unconfirmed takers ---

// A taker the exchange kills unfilled is not a hedge, however sure the engine's model was
// at placement: the dead status triggers a Status read (data, not assumption), the phantom
// placement credit is reversed, and the shortfall is re-hedged with a fresh taker.
func TestDeadTakerConfirmedUnfilledIsUncreditedAndRehedged(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	sink := &recordSink{}
	e.SetFillSink(sink)
	tak := completeBuyClip(t, e, m, tk)

	if sink.netB != -2 {
		t.Fatalf("taker credited at placement: netB=%d want -2", sink.netB)
	}
	// The exchange rejects the taker; Status confirms 0 lots executed.
	m.status = map[string]int{tak: 0}
	e.OnOrderStatus(tak, true)

	if e.Halted() {
		t.Fatal("a single dead taker must be repaired, not halted on")
	}
	if got := tk.lots["sell "+testLegB]; got != 4 {
		t.Fatalf("shortfall must be re-hedged: sold %d lots on legB, want 4 (2 dead + 2 fresh)", got)
	}
	if sink.netB != -2 {
		t.Fatalf("un-credit + fresh hedge must net to the hedged book: netB=%d want -2 (%v)", sink.netB, sink.events)
	}
	if acct := e.own[tak]; acct.final != 0 {
		t.Fatalf("the dead taker's account must record the confirmed terminal count, final=%d", acct.final)
	}
}

// A dead taker that PARTIALLY filled reverses and re-hedges only the confirmed shortfall.
func TestDeadTakerPartialShortfallRehedgedExactly(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	sink := &recordSink{}
	e.SetFillSink(sink)
	tak := completeBuyClip(t, e, m, tk)

	m.status = map[string]int{tak: 1} // 1 of 2 lots executed before the kill
	e.OnOrderStatus(tak, true)

	if got := tk.lots["sell "+testLegB]; got != 3 {
		t.Fatalf("only the 1-lot shortfall must be re-hedged: sold %d on legB, want 3", got)
	}
	if sink.netB != -2 {
		t.Fatalf("netB=%d want -2 after partial un-credit and re-hedge (%v)", sink.netB, sink.events)
	}
}

// A taker with neither fill events nor a terminal status past the confirm window is an
// UNCONFIRMED hedge: the engine must stop opening new clips and wait for the broker's
// data — resuming only once the fill stream (here) finally covers the placed size.
func TestOverdueUnconfirmedTakerBlocksNewClips(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	tak := completeBuyClip(t, e, m, tk)

	// Past the confirm window with no taker fill events; Status has no answer either
	// (fakeMaker errors on unknown ids), so the engine must keep waiting.
	late := openHour.Add(11 * time.Second)
	e.OnTick(late)
	before := m.count("bid ")
	e.OnState(buyState(late))
	if got := m.count("bid "); got != before {
		t.Fatalf("no new clip may open on an unconfirmed hedge: bid placements %d → %d", before, got)
	}

	// The fill stream catches up: the taker's lots are confirmed by data — trading resumes.
	e.OnFill(late.Add(time.Second), tak, testLegB, false, 2, 50)
	e.OnState(buyState(late.Add(2 * time.Second)))
	if got := m.count("bid "); got != before+1 {
		t.Fatalf("a confirmed taker must unblock new clips: bid placements %d → %d", before, got)
	}
	if e.Halted() {
		t.Fatal("waiting for confirmation must not halt")
	}
}

// Inside the confirm window the fill stream gets its normal latency: no status polls, no
// clip suppression — the watchdog only acts once the data is overdue.
func TestFreshPendingTakerDoesNotBlockOrPoll(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	completeBuyClip(t, e, m, tk)

	soon := openHour.Add(2 * time.Second)
	e.OnTick(soon)
	before := m.count("bid ")
	e.OnState(buyState(soon))
	if got := m.count("bid "); got != before+1 {
		t.Fatal("a just-placed taker must not suppress the next clip")
	}
	if got := m.count("status "); got != 0 {
		t.Fatalf("no status polls inside the confirm window, got %d", got)
	}
}

// --- reject-retry ladder applied to a taker HEDGE: shrink-and-accumulate toward the owed total ---

// A maker fill's hedge is DEFINITIVELY rejected at its full size, but the broker accepts a
// smaller one: takerRetryFrom must chase the OWED total (what legA already moved) across
// multiple installments, unlike tryOpenClip's ladder this applies with no Intent.IsClose
// check at all — the amount is fixed by a fill that already happened, not a size still
// being decided, so entry vs. exit does not enter into it.
func TestHedgeShrinkChaseAccumulatesToOwedTotal(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 10,
		FillTimeout: 2 * time.Minute, HedgeRetries: 2,
		RejectRetryLotStep: 2, RejectRetryMinLots: 2,
	}, m, tk, newTestDecider(10))
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	legAID := m.id(testLegA)
	tk.failErrSym = map[string]error{testLegB: NewDefinitiveReject(errors.New("insufficient funds"))}
	tk.failMaxLots = map[string]int{testLegB: 6} // broker accepts legB orders up to 6 lots

	e.OnFill(openHour, legAID, testLegA, true, 10, 100)

	if got := tk.lots["sell "+testLegB]; got != 10 {
		t.Fatalf("the hedge must reach the full owed 10 lots via installments, got %d (%v)", got, tk.lots)
	}
	if e.Impaired() {
		t.Fatal("a fully-chased hedge must not go impaired")
	}
	if e.Position() != 10 {
		t.Fatalf("position must be +10, got %d", e.Position())
	}
	wantTail := []string{testLegB + ":10", testLegB + ":8", testLegB + ":6", testLegB + ":4"}
	if got := tk.attempts; len(got) != len(wantTail) {
		t.Fatalf("attempts = %v, want %v", got, wantTail)
	} else {
		for i := range wantTail {
			if got[i] != wantTail[i] {
				t.Fatalf("attempts = %v, want %v", got, wantTail)
			}
		}
	}
}

// A hedge shrunk down to the floor that STILL cannot place at all (genuinely stuck, not a
// per-order-size limit) must exhaust the ladder and then fall through to the unchanged
// debt/impaired fallback — the ladder trying harder must never mean trying forever.
func TestHedgeShrinkChaseExhaustsToDebtWhenFloorStillRejects(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 10,
		FillTimeout: 2 * time.Minute, HedgeRetries: 1,
		RejectRetryLotStep: 2, RejectRetryMinLots: 2,
	}, m, tk, newTestDecider(10))
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	legAID := m.id(testLegA)
	tk.failErrSym = map[string]error{testLegB: NewDefinitiveReject(errors.New("insufficient funds"))}

	e.OnFill(openHour, legAID, testLegA, true, 10, 100)

	if !e.Impaired() {
		t.Fatal("an unrecoverable hedge must still queue a debt and enter impaired mode")
	}
	if got := tk.lots["sell "+testLegB]; got != 0 {
		t.Fatalf("nothing should have placed, got %v", tk.lots)
	}
	if got := len(tk.attempts); got != 6 { // shrink ladder: 10,8,6,4,2 (all rejected), then the unchanged HedgeRetries=1 loop retries the original 10 once more before deferring
		t.Fatalf("attempts = %d, want 6, calls %v", got, tk.attempts)
	}
}

// An AMBIGUOUS taker failure (maybeDelivered==true — a transport blip, not a business
// reject) must not shrink at all: the order may already rest at the broker, so guessing
// smaller on top of that uncertainty would risk a double placement. The unchanged
// HedgeRetries loop, at the ORIGINAL size, must run instead.
func TestHedgeShrinkChaseSkipsAmbiguousError(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{failN: 1} // one transient failure, then succeeds
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 10,
		FillTimeout: 2 * time.Minute, HedgeRetries: 2,
		RejectRetryLotStep: 2, RejectRetryMinLots: 2,
	}, m, tk, newTestDecider(10))
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	legAID := m.id(testLegA)

	e.OnFill(openHour, legAID, testLegA, true, 10, 100)

	if got := tk.lots["sell "+testLegB]; got != 10 {
		t.Fatalf("the hedge must land at its ORIGINAL size after the transient retry, got %v", tk.lots)
	}
	if got := len(tk.attempts); got != 2 { // one failed attempt at full size, one successful — no shrinking
		t.Fatalf("attempts = %d, want 2 (no shrinking on an ambiguous error), calls %v", got, tk.attempts)
	}
	if e.Impaired() {
		t.Fatal("a transient failure within HedgeRetries must not go impaired")
	}
}

// RejectRetryLotStep=0 (default) must leave takerRetryFrom byte-identical to its pre-ladder
// behaviour: a definitive reject just retries the same size HedgeRetries times, then defers.
func TestHedgeShrinkChaseDisabledByDefault(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 10) // RejectRetryLotStep unset
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	legAID := m.id(testLegA)
	tk.failErrSym = map[string]error{testLegB: NewDefinitiveReject(errors.New("insufficient funds"))}

	e.OnFill(openHour, legAID, testLegA, true, 10, 100)

	if got := tk.lots["sell "+testLegB]; got != 0 {
		t.Fatalf("nothing should have placed, got %v", tk.lots)
	}
	if got := len(tk.attempts); got != 2 { // HedgeRetries=2, no shrinking
		t.Fatalf("attempts = %d, want 2 (HedgeRetries, no shrink ladder), calls %v", got, tk.attempts)
	}
	if !e.Impaired() {
		t.Fatal("exhausted attempts must still queue a debt")
	}
}

// --- HedgeRatio: asymmetric-notional pairs (one LegA contract hedged by R of LegB) ---

// TestHedgeRatioScalesLegBTakerOnMakerFill: the sole LegA passive fills n contracts, so the
// LegB taker must cross n×R — the pair is delta-flat only at the ratio, and hedging 1:1
// would leave an R-fold naked leg.
func TestHedgeRatioScalesLegBTakerOnMakerFill(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newSoloRatioTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	legAID := m.id(testLegA)

	e.OnFill(openHour, legAID, testLegA, true, 2, 100) // the sole maker leg fills fully

	if tk.lots["sell "+testLegB] != 20 {
		t.Fatalf("a 2-lot LegA fill at ratio 10 must be hedged with a 20-contract LegB sell, got %v", tk.lots)
	}
	if e.Position() != 2 {
		t.Fatalf("position is counted in LegA contracts and must be +2, got %d", e.Position())
	}
}

// TestHedgeRatioLeavesLegAMakerUnscaled: the ratio sizes the LegB hedge only. The LegA
// passive is the clip's own target and must be posted unscaled — scaling it too would
// multiply the whole clip instead of hedging it.
func TestHedgeRatioLeavesLegAMakerUnscaled(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newSoloRatioTestEngine(m, tk, 3)
	seedBooks(e, openHour)

	e.OnState(buyState(openHour))

	want := "bid " + testLegA + " 3 @ 100.00"
	if len(m.calls) != 1 || m.calls[0] != want {
		t.Fatalf("LegA passive must be posted at the clip size unscaled (%q), got %v", want, m.calls)
	}
}

// TestHedgeRatioScalesPartialMakerFills: each partial LegA fill is hedged at the ratio as it
// happens, so the book is never naked between partials.
func TestHedgeRatioScalesPartialMakerFills(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newSoloRatioTestEngine(m, tk, 4)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	legAID := m.id(testLegA)

	e.OnFill(openHour, legAID, testLegA, true, 1, 100)
	if tk.lots["sell "+testLegB] != 10 {
		t.Fatalf("first partial (1 lot) must hedge 10 contracts, got %v", tk.lots)
	}
	e.OnFill(openHour.Add(time.Second), legAID, testLegA, true, 3, 100)
	if tk.lots["sell "+testLegB] != 40 {
		t.Fatalf("4 filled LegA lots must total a 40-contract LegB hedge, got %v", tk.lots)
	}
	if e.Working() {
		t.Fatal("clip should commit once the sole maker leg filled its target")
	}
}

// TestHedgeRatioWithoutSoloMakerRefusesToTrade: a ratio in dual-passive mode is a
// MISCONFIGURATION — the engine cannot convert a LegB fill back into whole LegA lots, and
// quietly pairing 1:1 would be a silent R-fold under-hedge (the failure mode that invalidated
// a whole day of index-pair measurements). Refuse to trade instead.
func TestHedgeRatioWithoutSoloMakerRefusesToTrade(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 2,
		FillTimeout: 2 * time.Minute, HedgeRetries: 2,
		HedgeRatio: 10, // no SoloMakerLeg
	}, m, tk, newTestDecider(2))
	seedBooks(e, openHour)

	e.OnState(buyState(openHour))

	if !e.Halted() {
		t.Fatal("a hedge ratio without SoloMakerLeg must halt the engine, not degrade to 1:1")
	}
	if len(m.calls) != 0 || len(tk.calls) != 0 {
		t.Fatalf("a halted engine must place nothing, got maker %v taker %v", m.calls, tk.calls)
	}
}

// TestHedgeRatioRefusesToUnscaleAStrayLegBFill: the LegB→LegA direction has no whole-lot
// answer at R>1 (7 LegB contracts are not a LegA lot), and rounding either way would leave a
// permanent position divergence — the two-pass reconcile suspend. Solo never rests a LegB
// passive so this cannot arise in practice; if it ever does, the engine logs and places
// NOTHING, the same "waiting, never guessing" rule the deferred-retire path follows.
func TestHedgeRatioRefusesToUnscaleAStrayLegBFill(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newSoloRatioTestEngine(m, tk, 2)
	seedBooks(e, openHour)

	e.hedgeStrayMakerFill("ghost", testLegB, true, 7)

	if len(tk.calls) != 0 {
		t.Fatalf("an unconvertible LegB stray must not be hedged by guesswork, got %v", tk.calls)
	}
}

// TestHedgeRatioScalesTakerOnlyLegB: taker-only needs the ratio too — both legs cross at
// once, so LegB must be sized at R× the LegA clip. Like solo, this mode never divides (both
// sizes are known before either order is sent), so it is a second legitimate home for a
// ratio; unlike solo, there is no passive leg at all.
func TestHedgeRatioScalesTakerOnlyLegB(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 2,
		FillTimeout: 2 * time.Minute, HedgeRetries: 2,
		TakerOnly: true, HedgeRatio: 10,
	}, m, tk, newTestDecider(2))
	seedBooks(e, openHour)

	e.OnState(buyState(openHour))

	if got := tk.lots["buy "+testLegA]; got != 2 {
		t.Fatalf("LegA must cross at the clip size unscaled, got %d (%v)", got, tk.lots)
	}
	if got := tk.lots["sell "+testLegB]; got != 20 {
		t.Fatalf("LegB must cross at 2 lots × ratio 10 = 20 contracts, got %d (%v)", got, tk.lots)
	}
	if e.Halted() {
		t.Fatal("taker-only is a supported home for a hedge ratio — it must not halt")
	}
}
