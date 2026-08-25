package execengine

// Тесты FillSink: что и когда движок начисляет во внешний учёт (кредит тейкера
// при размещении, аменд по факту цены, отсутствие двойного начисления).
// Точка входа со стороны движка — SetFillSink в engine.go.

import (
	"fmt"
	"testing"
	"time"
)

// The 2026-07-16 over-cap regression: a clip's fills sit undelivered on a stalled stream,
// the clip is torn down on a signal loss, and the cancel-acks reveal both legs executed.
// The position the Decider caps against must move AT THE TEARDOWN (via the sink), not when
// the stream eventually catches up — checking the cap against a stream-fed position let
// clips keep opening 8 lots past it — and the late events must then only amend the price,
// never re-add the lots.
func TestSinkCreditsCancelAckGapAtTeardown(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 4)
	sink := &recordSink{}
	e.SetFillSink(sink)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour)) // buy clip: bid legA @100, ask legB @51
	bidA, askB := m.id(testLegA), m.id(testLegB)

	// The stream is stalled: no fill events arrive. The signal turns; the teardown's
	// cancel-acks report both legs fully executed.
	m.executed = map[string]int{bidA: 4, askB: 4}
	e.OnState(holdState(openHour.Add(time.Second)))

	if sink.netA != 4 || sink.netB != -4 {
		t.Fatalf("teardown must credit the acked lots immediately, got netA=%d netB=%d (%v)", sink.netA, sink.netB, sink.events)
	}

	// The stream catches up: the same lots arrive as fill events. The position must not move.
	e.OnFill(openHour.Add(time.Minute), bidA, testLegA, true, 4, 100)
	e.OnFill(openHour.Add(time.Minute), askB, testLegB, false, 4, 51)
	if sink.netA != 4 || sink.netB != -4 {
		t.Fatalf("late fill events must not re-add credited lots, got netA=%d netB=%d (%v)", sink.netA, sink.netB, sink.events)
	}
}

// A placed taker is credited at placement (the engine's own model treats it as done),
// priced at the touch it crosses; its fill event later only trues the price via Amend.
func TestSinkCreditsTakerAtPlacementAndAmendsPrice(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	sink := &recordSink{}
	e.SetFillSink(sink)
	seedBooks(e, openHour) // legB bid=50 / ask=51
	e.OnState(buyState(openHour))
	bidA := m.id(testLegA)

	// legA (maker) partially fills via the stream → the engine hedges legB with a taker.
	e.OnFill(openHour, bidA, testLegA, true, 1, 100)
	if sink.netA != 1 || sink.netB != -1 {
		t.Fatalf("maker fill + taker hedge must both be credited at once, got netA=%d netB=%d (%v)", sink.netA, sink.netB, sink.events)
	}

	// The taker's own fill event arrives at a worse price than the touch it was estimated
	// at: inventory must not move again; only the price difference is amended.
	e.OnFill(openHour, tk.lastID, testLegB, false, 1, 49.5)
	if sink.netB != -1 {
		t.Fatalf("a taker's fill event must not re-add its lots, got netB=%d (%v)", sink.netB, sink.events)
	}
	want := fmt.Sprintf("amend %s buy=%v x%d %.2f->%.2f", testLegB, false, 1, 50.0, 49.5)
	if got := sink.events[len(sink.events)-1]; got != want {
		t.Fatalf("want %q as the last sink event, got %v", want, sink.events)
	}
}

// A duplicated taker fill event (empty trade id slipping the runner's dedup, or a hostile
// replay) must never mint inventory: the placement credit is the taker's SOLE inventory
// source, so events only amend the price on the overlap and are dropped beyond it.
func TestDuplicateTakerFillEventNeverCreditsNewLots(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	sink := &recordSink{}
	e.SetFillSink(sink)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))

	e.OnFill(openHour, m.id(testLegA), testLegA, true, 1, 100) // maker fill → taker hedge on legB
	takerID := tk.lastID
	if sink.netB != -1 {
		t.Fatalf("placement credit expected, netB=%d", sink.netB)
	}

	e.OnFill(openHour, takerID, testLegB, false, 1, 50) // the real fill event: amortizes the credit
	e.OnFill(openHour, takerID, testLegB, false, 1, 50) // duplicate: must be dropped
	e.OnFill(openHour, takerID, testLegB, false, 1, 50) // and again
	if sink.netB != -1 {
		t.Fatalf("duplicate taker events minted inventory, netB=%d (%v)", sink.netB, sink.events)
	}
}

// Every beyond-terminal excess is hedged, so every beyond-terminal excess must also be
// credited — the sink mirrors the hedge decision one-for-one, or the ledger position and
// the hedged book drift apart on the second excess (the first used to consume the credit
// headroom).
func TestRepeatedExcessCreditMirrorsHedge(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 4)
	sink := &recordSink{}
	e.SetFillSink(sink)
	seedBooks(e, openHour)

	id, _ := m.PlaceBid(testLegA, 4, 100)
	e.own[id] = &ordAcct{maker: true, sym: testLegA, isBuy: true, price: 100, final: -1}
	m.executed = map[string]int{id: 2}
	e.retireOrder(id) // terminal count 2, credited 2

	e.OnFill(openHour, id, testLegA, true, 2, 100) // the acked lots stream in → amend-only, no hedge
	if sink.netA != 2 || len(tk.calls) != 0 {
		t.Fatalf("acked lots must not re-credit or hedge, netA=%d takers=%v", sink.netA, tk.calls)
	}
	e.OnFill(openHour, id, testLegA, true, 1, 100) // 1st lot beyond the ack → hedge 1 + credit 1
	e.OnFill(openHour, id, testLegA, true, 1, 100) // 2nd lot beyond → hedge 1 + credit 1 again
	if hedged := tk.lots["sell "+testLegB]; hedged != 2 {
		t.Fatalf("want 2 excess lots hedged, got %d (%v)", hedged, tk.calls)
	}
	if sink.netA != 4 {
		t.Fatalf("credit must mirror the hedge one-for-one, netA=%d want 4 (%v)", sink.netA, sink.events)
	}
}

// A taker fill event at exactly the estimated cross price must produce NO amend: the
// credit was already exact and a zero-delta amend is ledger noise.
func TestSinkNoAmendWhenFillMatchesEstimate(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	sink := &recordSink{}
	e.SetFillSink(sink)
	seedBooks(e, openHour) // legB bid=50 → a sell taker is estimated at 50
	e.OnState(buyState(openHour))

	e.OnFill(openHour, m.id(testLegA), testLegA, true, 1, 100) // hedge placed, credited @50
	events := len(sink.events)

	e.OnFill(openHour, tk.lastID, testLegB, false, 1, 50) // fills at exactly the estimate

	if len(sink.events) != events {
		t.Fatalf("a fill at the estimated price must not amend, got %v", sink.events)
	}
}

// ForceCloseOnTimeout's forced takers are credited to the sink AT PLACEMENT, priced at the
// touch each one crosses — the position the cap checks moves with the engine's decision,
// not with the (possibly stalled) fill stream.
func TestSinkCreditsForcedCloseTakersAtPlacement(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	dm := &fixedDecider{intent: Intent{Action: -1, IsClose: true, Lots: 2}}
	e := newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: 2,
		FillTimeout: time.Minute, HedgeRetries: 2, ForceCloseOnTimeout: true,
	}, m, tk, dm)
	sink := &recordSink{}
	e.SetFillSink(sink)
	seedBooks(e, openHour) // legA bid=100 (sell crosses at 100), legB ask=51 (buy crosses at 51)
	e.OnState(RowState{Time: openHour})

	e.OnTick(openHour.Add(2 * time.Minute)) // nothing filled → forced taker close

	if sink.netA != -2 || sink.netB != 2 {
		t.Fatalf("forced takers must be credited at placement, got netA=%d netB=%d (%v)", sink.netA, sink.netB, sink.events)
	}
	wantA := fmt.Sprintf("fill %s buy=%v x%d @%.2f", testLegA, false, 2, 100.0)
	wantB := fmt.Sprintf("fill %s buy=%v x%d @%.2f", testLegB, true, 2, 51.0)
	seen := map[string]bool{}
	for _, ev := range sink.events {
		seen[ev] = true
	}
	if !seen[wantA] || !seen[wantB] {
		t.Fatalf("credits must be priced at the crossed touch, want %q and %q in %v", wantA, wantB, sink.events)
	}
}

// The counterpart-ahead resolution keeps the sink in lock-step too: every realized lot is
// credited exactly once whether it came from the maker's fill event, the counterpart's
// cancel-ack, or the completion takers.
func TestSinkCreditsCounterpartAheadResolutionOnce(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 4)
	sink := &recordSink{}
	e.SetFillSink(sink)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	bidA, askB := m.id(testLegA), m.id(testLegB)

	m.executed = map[string]int{askB: 3, bidA: 1}
	e.OnFill(openHour, bidA, testLegA, true, 1, 100)

	// legA: 1 event + 3 taker = +4; legB: 3 cancel-ack + 1 taker = −4.
	if sink.netA != 4 || sink.netB != -4 {
		t.Fatalf("resolution must credit exactly target per leg, got netA=%d netB=%d (%v)", sink.netA, sink.netB, sink.events)
	}

	// The counterpart's acked lots stream in late: price-true only, no inventory.
	e.OnFill(openHour.Add(time.Second), askB, testLegB, false, 3, 51)
	if sink.netA != 4 || sink.netB != -4 {
		t.Fatalf("late acked lots must not re-credit, got netA=%d netB=%d (%v)", sink.netA, sink.netB, sink.events)
	}
}
