package execengine

// Тесты учёта ордеров: пустые и переиспользованные брокером order id, дедуп
// повторно доставленных событий, Own, OnOrderStatus, снятие ордера с учёта
// (retire). Зеркалит engine_orders.go.

import (
	"errors"
	"testing"
	"time"

	"QuantCore/strategies/execengine/orderregistry"
)

// --- untrustable broker responses: empty and reused order ids ---

// The broker acks a placement with an EMPTY order id. Registering it would poison the own
// map (own[""]) — every foreign empty-id fill would then read as ours and be hedged — and an
// empty leg id collides with the clip's `makerID == ""` "no maker yet" sentinel. The open
// must fail cleanly: no clip, no own[""], and empty-id fills stay foreign no-ops.
func TestEmptyMakerIDFirstLegAbortsOpen(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	m.forceID = map[string]string{testLegA: ""}

	e.OnState(buyState(openHour))

	if e.Working() || e.Halted() {
		t.Fatalf("an empty-id placement must abort the open, working=%v halted=%v", e.Working(), e.Halted())
	}
	if e.Own("") {
		t.Fatal("an empty order id must never be registered as Own")
	}
	// A (foreign) empty-id fill arrives — a shared account's stray or a garbled event. It
	// must be ignored, never hedged as our own.
	e.OnFill(openHour, "", testLegA, true, 5, 100)
	if len(tk.calls) != 0 {
		t.Fatalf("an empty-id fill must stay a foreign no-op, got %v", tk.calls)
	}
}

// The empty id arrives on the SECOND leg: legA is already resting and must be unwound
// (with its race-fill gap hedged) exactly like any legB placement failure.
func TestEmptyMakerIDSecondLegUnwindsFirst(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	m.forceID = map[string]string{testLegB: ""}
	m.executed = map[string]int{"b1": 1} // legA catches 1 lot before its unwind cancel

	e.OnState(buyState(openHour))

	if e.Working() || e.Halted() {
		t.Fatalf("the failed open must leave the engine idle, working=%v halted=%v", e.Working(), e.Halted())
	}
	if m.count("cancel b1") != 1 {
		t.Fatalf("the naked legA order must be cancelled, got %v", m.calls)
	}
	if got := tk.lots["sell "+testLegB]; got != 1 {
		t.Fatalf("legA's caught lot must be matched on legB, got %d (%v)", got, tk.lots)
	}
	if e.Own("") {
		t.Fatal("an empty order id must never be registered as Own")
	}
}

// The broker REUSES legA's id for legB. Accepting it would clobber legA's fill account —
// its dedup state and every late-race-fill guard. The open must fail and legA's account
// must survive intact.
func TestReusedMakerIDAbortsWithoutClobberingAccount(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	m.forceID = map[string]string{testLegB: "b1"} // legB will echo legA's id

	e.OnState(buyState(openHour))

	if e.Working() || e.Halted() {
		t.Fatalf("a reused-id placement must abort the open, working=%v halted=%v", e.Working(), e.Halted())
	}
	acct := e.own["b1"]
	if acct == nil || acct.Sym != testLegA || !acct.IsBuy {
		t.Fatalf("legA's fill account must survive the collision intact, got %+v", acct)
	}
	if m.count("cancel b1") != 1 {
		t.Fatalf("the naked legA order must be unwound, got %v", m.calls)
	}
}

// A taker hedge whose placement is ACCEPTED but acked with an empty id: the hedge is real
// at the broker, so its lots must still be credited to the sink exactly once (the cap
// position may not lose them), while the untrackable id is never registered and later
// empty-id fills change nothing.
func TestEmptyTakerIDCreditsHedgeOnceAndIgnoresGhostFills(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 1)
	sink := &recordSink{}
	e.SetFillSink(sink)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	bidA := m.id(testLegA)
	tk.forceIDs = []string{""} // the hedge's placement acks with an empty id

	e.OnFill(openHour, bidA, testLegA, true, 1, 100) // maker fills → hedge legB

	if e.Halted() {
		t.Fatal("an empty taker id must not halt — the hedge was accepted")
	}
	if got := tk.lots["sell "+testLegB]; got != 1 {
		t.Fatalf("the hedge must be placed exactly once, got %d (%v)", got, tk.lots)
	}
	if sink.netA != 1 || sink.netB != -1 {
		t.Fatalf("the blind hedge must still be credited once, netA=%d netB=%d (%v)", sink.netA, sink.netB, sink.events)
	}
	if e.Own("") {
		t.Fatal("an empty order id must never be registered as Own")
	}
	if e.Working() || e.Position() != 1 {
		t.Fatalf("the clip must commit normally, working=%v pos=%d", e.Working(), e.Position())
	}

	// The untracked hedge's fill arrives with an empty order id — foreign, ignored, and the
	// credited inventory must not move again.
	e.OnFill(openHour, "", testLegB, false, 1, 50)
	e.OnFill(openHour, "", testLegB, false, 1, 50)
	if sink.netB != -1 || len(tk.calls) != 1 {
		t.Fatalf("empty-id fills must not re-credit or re-hedge, netB=%d takers=%v", sink.netB, tk.calls)
	}
}

// The broker reuses a RETIRED maker's id for a new taker hedge. The stale maker account is
// replaced by the taker's, so fills reported under that id run the taker path (amend-only)
// and can never trigger a blind beyond-terminal re-hedge.
func TestReusedTakerIDNeverRehedges(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 1)
	sink := &recordSink{}
	e.SetFillSink(sink)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	bidA, askB := m.id(testLegA), m.id(testLegB)

	tk.forceIDs = []string{askB}                     // the hedge's id collides with the retired counterpart's
	e.OnFill(openHour, bidA, testLegA, true, 1, 100) // maker fills → counterpart retired → hedge placed as "askB"

	if e.Halted() || e.Working() || e.Position() != 1 {
		t.Fatalf("the clip must commit normally, halted=%v working=%v pos=%d", e.Halted(), e.Working(), e.Position())
	}
	takers := len(tk.calls)

	// A fill reported under the collided id: it must run the TAKER path (price-true only) —
	// under the old maker account it would have been "beyond-terminal excess" and re-hedged.
	e.OnFill(openHour, askB, testLegB, false, 1, 50)
	if len(tk.calls) != takers {
		t.Fatalf("a collided-id fill must never re-hedge, got %v", tk.calls)
	}
	if sink.netA != 1 || sink.netB != -1 {
		t.Fatalf("inventory must stay at the committed pair, netA=%d netB=%d (%v)", sink.netA, sink.netB, sink.events)
	}
}

// --- placed-size ceiling: duplicates and corrupt acks cannot mint inventory ---

// A fully-filled maker's fill event is re-delivered after the clip committed (a stream
// reconnect replay that slipped the runner's trade-id dedup). Pre-clamp the duplicate
// overflowed past the terminal count into the excess-hedge path: 4 phantom lots hedged and
// credited. It must be dropped outright.
func TestDuplicateMakerFillAfterCommitDropped(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 4)
	sink := &recordSink{}
	e.SetFillSink(sink)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	bidA := m.id(testLegA)

	m.executed = map[string]int{bidA: 4}             // commit's retire ack agrees with the stream
	e.OnFill(openHour, bidA, testLegA, true, 4, 100) // full fill → hedge 4, commit
	if e.Working() || e.Position() != 4 {
		t.Fatalf("clip must be committed at target, working=%v pos=%d", e.Working(), e.Position())
	}
	takers, netA := len(tk.calls), sink.netA

	e.OnFill(openHour, bidA, testLegA, true, 4, 100) // the same 4 lots again

	if len(tk.calls) != takers {
		t.Fatalf("a re-delivered fill must not hedge, got extra takers: %v", tk.calls)
	}
	if sink.netA != netA {
		t.Fatalf("a re-delivered fill must not credit the sink, netA=%d want %d (%v)", sink.netA, netA, sink.events)
	}
	if e.Position() != 4 || e.Halted() {
		t.Fatalf("position must stay at target, pos=%d halted=%v", e.Position(), e.Halted())
	}
}

// A duplicate delivered after a teardown's cancel-ack: the genuine lots dedup against the
// terminal count (already covered elsewhere); the REPEAT of those lots pushes seen past
// final and pre-clamp was hedged as "beyond-terminal excess". It is impossible inventory
// (the order only ever held `placed` lots) and must be dropped.
func TestDuplicateFillAfterTeardownDropped(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	bidA := m.id(testLegA)

	m.executed = map[string]int{bidA: 2} // the cancel catches both lots in flight
	e.CancelClip()                       // teardown folds the gap and equalizes legB with one 2-lot taker
	if got := tk.lots["sell "+testLegB]; got != 2 {
		t.Fatalf("teardown must equalize legB by 2, got %d (%v)", got, tk.lots)
	}
	takers := len(tk.calls)

	e.OnFill(openHour, bidA, testLegA, true, 2, 100) // the acked lots stream in → dedup (no-op)
	e.OnFill(openHour, bidA, testLegA, true, 2, 100) // and are re-delivered → impossible, drop

	if len(tk.calls) != takers {
		t.Fatalf("a re-delivered post-teardown fill must not hedge, got %v", tk.calls)
	}
	if e.Halted() {
		t.Fatal("dropping a duplicate must not halt")
	}
}

// A corrupt cancel-ack claiming more executed lots than the order was placed for must be
// clamped at the placement size: pre-clamp the teardown equalized the other leg up to the
// impossible count (9 taker lots on a 4-lot clip).
func TestCancelAckBeyondPlacedClamped(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 4)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	bidA := m.id(testLegA)

	m.executed = map[string]int{bidA: 9} // ack: 9 executed on a 4-lot order
	e.CancelClip()

	if got := tk.lots["sell "+testLegB]; got != 4 {
		t.Fatalf("the equalizing taker must be clamped to the placed size 4, got %d (%v)", got, tk.lots)
	}
	if got := tk.lots["buy "+testLegA]; got != 0 {
		t.Fatalf("no legA taker may appear, got %d (%v)", got, tk.lots)
	}
}

// The Status fallback (cancel kept erroring) is clamped the same way: a terminal executed
// count beyond the placement size folds as the placement size.
func TestStatusFallbackBeyondPlacedClamped(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	bidA := m.id(testLegA)

	m.cancelErr = map[string]error{bidA: errors.New("already filled")}
	m.status = map[string]int{bidA: 7} // terminal, but 7 executed on a 2-lot order
	e.CancelClip()

	if e.Halted() {
		t.Fatal("a readable terminal status must not halt")
	}
	if got := tk.lots["sell "+testLegB]; got != 2 {
		t.Fatalf("the equalizing taker must be clamped to the placed size 2, got %d (%v)", got, tk.lots)
	}
}

// The live storm's core: both passives fill in the race window. The maker leg's fill
// arrives, the counterpart's cancel-ack reports executed=4 (pair complete, no taker
// needed) — and THEN the counterpart's 4 lots arrive as fill events. Before the fix
// each of those events was blindly taker-hedged (the "paid alignment"); they must be
// recognized as already accounted: ZERO taker orders.
func TestCounterpartFillEventsAfterCancelAckAreNoOps(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 4)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	bidA, askB := m.id(testLegA), m.id(testLegB)
	if bidA == "" || askB == "" {
		t.Fatal("clip did not open")
	}

	m.executed = map[string]int{askB: 4}             // counterpart fills entirely during the cancel race
	e.OnFill(openHour, bidA, testLegA, true, 4, 100) // maker fills → counterpart retired, ack says 4 → pair complete

	if len(tk.calls) != 0 {
		t.Fatalf("no taker needed at commit: pair completed passively, got %v", tk.calls)
	}
	if e.Working() {
		t.Fatal("clip must be committed")
	}

	// The counterpart's fills now arrive from the stream — the exact events that were
	// double-hedged in production. They are already in the cancel-ack count: no-ops.
	e.OnFill(openHour, askB, testLegB, false, 1, 51)
	e.OnFill(openHour, askB, testLegB, false, 3, 51)

	if len(tk.calls) != 0 {
		t.Fatalf("late counterpart fills must be deduped, got takers: %v", tk.calls)
	}
}

// --- cross-clip contamination: a finished clip's ghosts must not touch the next clip ---

// A late fill from a PREVIOUS clip's leg arriving while a NEW clip is working must dedup
// against the old order's account — never be mistaken for the new clip's first fill.
func TestPrevClipLateFillDoesNotDisturbNewClip(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	oldA := m.id(testLegA)

	m.executed = map[string]int{oldA: 1}            // clip 1's legA catches a fill at teardown
	e.OnState(holdState(openHour.Add(time.Second))) // signal gone → the catch folds as a first fill and the clip completes
	if got := tk.lots["sell "+testLegB]; got != 2 {
		t.Fatalf("the abandon catch must complete legB to target, got %d (%v)", got, tk.lots)
	}
	if got := tk.lots["buy "+testLegA]; got != 1 {
		t.Fatalf("the abandon catch must complete legA to target, got %d (%v)", got, tk.lots)
	}

	e.OnState(buyState(openHour.Add(2 * time.Second))) // clip 2 opens
	if !e.Working() {
		t.Fatal("clip 2 must open")
	}
	takers := len(tk.calls)

	e.OnFill(openHour.Add(3*time.Second), oldA, testLegA, true, 1, 100) // clip 1's lot streams in late

	if e.clip.makerID != "" {
		t.Fatal("a previous clip's fill must never designate the new clip's maker")
	}
	if len(tk.calls) != takers {
		t.Fatalf("the old lot must dedup, got new takers: %v", tk.calls)
	}
}

// A dead-status echo for a PREVIOUS clip's order id must not tear down the new clip.
func TestPrevClipStatusEchoIgnoredDuringNewClip(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	oldA := m.id(testLegA)
	e.OnState(holdState(openHour.Add(time.Second))) // clip 1 cancelled

	e.OnState(buyState(openHour.Add(2 * time.Second))) // clip 2 opens
	e.OnOrderStatus(oldA, true)                        // clip 1's cancel-ack echoes back

	if !e.Working() {
		t.Fatal("an old clip's status echo must not tear down the new clip")
	}
}

// --- irrelevant statuses: alive and foreign never touch the working clip ---

// dead=false statuses and foreign order ids never touch the working clip.
func TestAliveAndForeignStatusesIgnored(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))

	e.OnOrderStatus(m.id(testLegA), false) // NEW/PARTIALLY_FILLED etc. — alive
	e.OnOrderStatus("foreign", true)       // some other strategy's dead order

	if !e.Working() {
		t.Fatal("alive/foreign statuses must not cancel the clip")
	}
}

// --- cross-channel ordering: a terminal status racing ahead of its fill events ---

// The order-state stream reports the maker DEAD (venue cancel) BEFORE any of its fill
// events arrive (the two streams have no cross-ordering guarantee). The teardown learns the
// executed count from the cancel-ack/status truth, equalizes once, and the late fill events
// then dedup — the mirror image of the fill-first ordering the older suites pin.
func TestStatusDeadBeforeFillEventsEqualizesThenDedups(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	bidA := m.id(testLegA)

	// The venue killed the order after it filled 1 lot; our cancel errors ("already done")
	// and Status supplies the terminal truth.
	m.cancelErr = map[string]error{bidA: errors.New("order already done")}
	m.status = map[string]int{bidA: 1}
	e.OnOrderStatus(bidA, true) // the dead status arrives FIRST

	if e.Working() || e.Halted() {
		t.Fatalf("the dead leg must pull the clip cleanly, working=%v halted=%v", e.Working(), e.Halted())
	}
	if got := tk.lots["sell "+testLegB]; got != 1 {
		t.Fatalf("the executed lot must be equalized once, got %d (%v)", got, tk.lots)
	}
	takers := len(tk.calls)

	e.OnFill(openHour.Add(time.Second), bidA, testLegA, true, 1, 100) // its fill event arrives late
	if len(tk.calls) != takers {
		t.Fatalf("the late fill must dedup against the status truth, got %v", tk.calls)
	}
}

// --- Own: which order ids the engine claims, including after retirement ---

// TestOwnTracksPlacedMakerAndTakerIDs pins the own-order set the live runner gates its P&L
// ledger on: every order the engine places this process (both maker legs and the taker hedge)
// is Own; an id it never placed is not. This is the mechanism that keeps a shared-account or
// replayed foreign fill out of the position.
func TestOwnTracksPlacedMakerAndTakerIDs(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))

	legAID, legBID := m.id(testLegA), m.id(testLegB)
	if !e.Own(legAID) || !e.Own(legBID) {
		t.Fatalf("both placed maker legs must be Own (legA=%q legB=%q)", legAID, legBID)
	}
	if e.Own("foreign") {
		t.Fatal("an order id the engine never placed must not be Own")
	}
	// A maker fill fires the taker hedge; the hedge's order id must be recorded too, so its own
	// fill is later recognised as own by the live runner.
	e.OnFill(openHour, legAID, testLegA, true, 2, 100)
	if tk.lastID == "" || !e.Own(tk.lastID) {
		t.Fatalf("the taker-hedge order id %q must be Own", tk.lastID)
	}
}

// Own must keep an id AFTER its order is retired — the live runner gates its ledger fold
// on Own, and a race-fill can arrive long after the cancel.
func TestOwnSurvivesRetirement(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	bidA, askB := m.id(testLegA), m.id(testLegB)

	e.OnState(holdState(openHour.Add(time.Second))) // teardown retires both

	if !e.Own(bidA) || !e.Own(askB) {
		t.Fatal("retired orders must stay Own for late-fill gating")
	}
}

// --- retireOrder: synchronous cancel accounting ---

// retireOrder must survive a failing cancel by reading Status: cancelling an
// already-fully-filled order errors at the broker, and assuming executed=0 there is
// exactly the 2026-07-15 churn-storm bug.
func TestRetireOrderStatusFallbackOnCancelError(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 4)
	id, _ := m.PlaceBid(testLegA, 4, 100)
	e.own[id] = &orderregistry.OrdAcct{Maker: true, Final: -1}

	m.cancelErr = map[string]error{id: errors.New("order already done")}
	m.status = map[string]int{id: 4} // broker: 4 lots executed, terminal

	if gap := e.retireOrder(id); gap != 4 {
		t.Fatalf("gap=%d want 4 (from Status fallback)", gap)
	}
	if gap := e.retireOrder(id); gap != 0 {
		t.Fatalf("second retire gap=%d want 0 (idempotent)", gap)
	}
	if e.Halted() {
		t.Fatal("must not halt when Status supplies the truth")
	}
}

// When the order can neither be cancelled nor observed terminal, the retire is DEFERRED —
// never silently assumed executed=0, never a self-halt: the engine goes impaired and the
// obligation loop keeps asking the broker until it answers.
func TestRetireOrderDefersWhenTruthUnreachable(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 4)
	id, _ := m.PlaceBid(testLegA, 4, 100)
	e.own[id] = &orderregistry.OrdAcct{Maker: true, Final: -1}

	m.cancelErr = map[string]error{id: errors.New("rpc down")}
	// no m.status entry → Status errors too

	if gap := e.retireOrder(id); gap != 0 {
		t.Fatalf("gap=%d want 0 while the truth is unknown (waiting, not guessing)", gap)
	}
	if e.Halted() {
		t.Fatal("the engine must never halt itself on an unanswered cancel")
	}
	if !e.Impaired() {
		t.Fatal("an unconfirmable retire must enter impaired mode")
	}
	// A repeat retire while deferred must not spam the broker — the obligation loop owns it.
	cancels := m.count("cancel " + id)
	if gap := e.retireOrder(id); gap != 0 || m.count("cancel "+id) != cancels {
		t.Fatal("a deferred order must not be re-cancelled synchronously")
	}
}

// A clean cancel that reports in-flight fills returns them as the gap exactly once,
// minus whatever fill events already folded.
func TestRetireOrderReturnsUnfoldedGapOnce(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 4)
	id, _ := m.PlaceBid(testLegA, 4, 100)
	acct := &orderregistry.OrdAcct{Maker: true, Final: -1}
	e.own[id] = acct

	acct.Folded = 1 // one lot already processed via a live fill event
	m.executed = map[string]int{id: 3}

	if gap := e.retireOrder(id); gap != 2 {
		t.Fatalf("gap=%d want 2 (3 executed − 1 already folded)", gap)
	}
	if acct.Final != 3 || acct.Folded != 3 {
		t.Fatalf("acct=%+v want final=3 folded=3", acct)
	}
}

// --- retireOrder: a transient cancel blip retries clean, without falling back to Status ---

// Cancel fails once with a transient rpc error and succeeds on retireOrder's single retry:
// the gap folds normally — no Status fallback, no halt.
func TestCancelTransientFailureRetriesWithoutStatus(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 4)
	id, _ := m.PlaceBid(testLegA, 4, 100)
	e.own[id] = &orderregistry.OrdAcct{Maker: true, Sym: testLegA, IsBuy: true, Price: 100, Final: -1}
	m.cancelErrOnce = map[string]error{id: errors.New("rpc blip")}
	m.executed = map[string]int{id: 3}

	if gap := e.retireOrder(id); gap != 3 {
		t.Fatalf("gap=%d want 3 from the retried cancel", gap)
	}
	if e.Halted() {
		t.Fatal("a transient cancel blip must not halt")
	}
	if m.count("status "+id) != 0 {
		t.Fatalf("Status must not be consulted when the cancel retry succeeds, got %v", m.calls)
	}
	if m.count("cancel "+id) != 2 {
		t.Fatalf("want exactly 2 cancel attempts, got %v", m.calls)
	}
}

// --- retireOrder: cancel accounting waits for the truth instead of halting ---

// A cancel that fails while the order settles through PENDING_CANCEL must WAIT for the
// terminal status instead of halting: the retire is deferred (impaired mode), the
// obligation loop keeps asking, and once the broker confirms the cancel clean the engine
// recovers by itself.
func TestRetireWaitsOutPendingCancelInsteadOfHalting(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	bidA := m.id(testLegA)

	m.cancelErr = map[string]error{bidA: errors.New("cancel rejected: order in cancellation")}
	m.statusLiveN = map[string]int{bidA: 2} // two non-terminal reads before the cancel settles
	m.status = map[string]int{bidA: 0}      // then terminal, nothing executed

	e.OnState(holdState(openHour.Add(time.Second))) // signal gone → clip torn down
	if e.Halted() {
		t.Fatal("a cancel settling through PENDING_CANCEL must never halt")
	}
	if e.Working() {
		t.Fatal("the clip must be gone (orders pulled) while the cancel confirms")
	}
	if !e.Impaired() {
		t.Fatal("an unconfirmed cancel must enter impaired mode and keep asking")
	}

	// The obligation loop keeps asking at its backoff pace: live, live, then terminal.
	e.OnTick(openHour.Add(4 * time.Second))  // probe 2: still PENDING_CANCEL
	e.OnTick(openHour.Add(10 * time.Second)) // probe 3: terminal — resolved
	if e.Impaired() {
		t.Fatalf("the confirmed cancel must clear impaired mode (%v)", m.calls)
	}
	if got := m.count("status " + bidA); got != 3 {
		t.Fatalf("expected 3 status reads (2 live + 1 terminal), got %d", got)
	}

	// Recovery flows through the unverified gate: one clean reconcile, then trading.
	e.OnState(buyState(openHour.Add(11 * time.Second)))
	if got := m.count("bid "); got != 1 {
		t.Fatal("no new clip before the clean reconcile confirms the position")
	}
	e.Reconcile(0, 0)
	e.OnState(buyState(openHour.Add(12 * time.Second)))
	if got := m.count("bid "); got != 2 {
		t.Fatalf("recovered engine must trade again after the clean reconcile, got %d bids", got)
	}
}

// When the order can neither be cancelled nor observed terminal, the engine does NOT
// halt: it stays impaired — orders pulled, no new clips — and keeps asking the broker
// forever. Waiting is the correct terminal behaviour for an unanswered question.
func TestUnanswerableRetireWaitsForeverWithoutHalting(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	bidA := m.id(testLegA)

	m.cancelErr = map[string]error{bidA: errors.New("rpc down")} // no status entry → Status errors every time

	e.OnState(holdState(openHour.Add(time.Second)))
	if e.Halted() {
		t.Fatal("the engine must never halt itself on an unanswered cancel")
	}
	if !e.Impaired() {
		t.Fatal("an unanswerable retire must enter impaired mode")
	}

	// Hours pass; the broker never answers. The engine keeps asking (backing off to the
	// ceiling) and never opens a clip — but never gives up either.
	asked := m.count("status " + bidA)
	for i := 1; i <= 240; i++ {
		e.OnTick(openHour.Add(time.Duration(i) * 30 * time.Second))
	}
	if !e.Impaired() || e.Halted() {
		t.Fatalf("still-unanswered retire: impaired=%v halted=%v, want impaired forever", e.Impaired(), e.Halted())
	}
	if got := m.count("status " + bidA); got <= asked {
		t.Fatal("the obligation loop must keep re-asking the broker")
	}
	e.OnState(buyState(openHour.Add(2 * time.Hour)))
	if got := m.count("bid "); got != 1 {
		t.Fatalf("no new clips while impaired, got %d bid placements", got)
	}

	// The broker finally answers: the order died with 1 lot executed. The engine settles
	// it — pair-hedging the surfaced lot so the BROKER book stays 1:1 — and recovers from
	// impaired. The surfaced pair is unknown to this Commit-driven Decider, so reconcile
	// reports a real divergence: the engine SUSPENDS (suspect) and keeps comparing — but
	// it never halts. Autonomy means the freeze is reversible the moment data agrees.
	m.cancelErr = nil
	m.executed = map[string]int{bidA: 1}
	e.OnTick(openHour.Add(3 * time.Hour))
	if e.Impaired() {
		t.Fatal("an answered retire must clear impaired mode")
	}
	if got := tk.lots["sell "+testLegB]; got != 1 {
		t.Fatalf("the surfaced executed lot must be pair-hedged, sold %d on legB want 1", got)
	}
	e.Reconcile(1, -1)
	e.Reconcile(1, -1) // persists into the second pass: a REAL divergence
	if e.Halted() {
		t.Fatal("a persistent reconcile mismatch must suspend, never halt")
	}
	if !e.Suspect() {
		t.Fatal("the surfaced-pair divergence must leave the engine suspect")
	}
	e.OnState(buyState(openHour.Add(3*time.Hour + time.Second)))
	if got := m.count("bid "); got != 1 {
		t.Fatalf("no clips may open on a doubted position, got %d bid placements", got)
	}
}
