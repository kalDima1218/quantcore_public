package quotebook

import (
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 5, 8, 0, 0, 0, time.UTC)

func TestUpdateAppliesFreshTouch(t *testing.T) {
	b := New("A", "B")
	if ok := b.Update("A", t0, 100, 101); !ok {
		t.Fatal("a fresh update must apply")
	}
	got := b.TouchA()
	if got.Bid != 100 || got.Ask != 101 || !got.OK || !got.TS.Equal(t0) {
		t.Fatalf("TouchA = %+v, want bid=100 ask=101 ok=true ts=%v", got, t0)
	}
}

func TestUpdateRejectsOutOfOrderSnapshot(t *testing.T) {
	b := New("A", "B")
	b.Update("A", t0, 100, 101)
	if ok := b.Update("A", t0.Add(-time.Second), 90, 91); ok {
		t.Fatal("an update strictly older than the stored touch must be rejected")
	}
	if got := b.TouchA(); got.Bid != 100 {
		t.Fatalf("stale update must not regress the touch, got bid=%v", got.Bid)
	}
}

func TestUpdateAcceptsEqualTimestamp(t *testing.T) {
	b := New("A", "B")
	b.Update("A", t0, 100, 101)
	if ok := b.Update("A", t0, 102, 103); !ok {
		t.Fatal("an update at the SAME timestamp must apply — intra-timestamp updates arrive in order")
	}
	if got := b.TouchA(); got.Bid != 102 {
		t.Fatalf("equal-timestamp update must apply, got bid=%v", got.Bid)
	}
}

func TestUpdateIgnoresUnrecognizedSymbol(t *testing.T) {
	b := New("A", "B")
	if ok := b.Update("C", t0, 100, 101); ok {
		t.Fatal("an unrecognized symbol must be a no-op")
	}
}

func TestUpdateTracksLegBIndependently(t *testing.T) {
	b := New("A", "B")
	b.Update("A", t0, 100, 101)
	b.Update("B", t0, 50, 51)
	if got := b.TouchA(); got.Bid != 100 {
		t.Fatalf("legA=%v, want 100", got.Bid)
	}
	if got := b.TouchB(); got.Bid != 50 {
		t.Fatalf("legB=%v, want 50", got.Bid)
	}
}

func TestCanQuoteRequiresBothLegsValid(t *testing.T) {
	b := New("A", "B")
	if b.CanQuote(t0, 0) {
		t.Fatal("neither leg has a touch yet — must not be quotable")
	}
	b.Update("A", t0, 100, 101)
	if b.CanQuote(t0, 0) {
		t.Fatal("only legA has a touch — must not be quotable")
	}
	b.Update("B", t0, 50, 51)
	if !b.CanQuote(t0, 0) {
		t.Fatal("both legs valid, maxStaleness disabled — must be quotable")
	}
}

func TestCanQuoteRejectsCrossedOrZeroPricedTouch(t *testing.T) {
	b := New("A", "B")
	b.Update("A", t0, 101, 100) // crossed: ask < bid
	b.Update("B", t0, 50, 51)
	if b.CanQuote(t0, 0) {
		t.Fatal("a crossed touch must not be quotable")
	}
}

func TestCanQuoteEnforcesMaxStaleness(t *testing.T) {
	b := New("A", "B")
	b.Update("A", t0, 100, 101)
	b.Update("B", t0, 50, 51)
	fresh := t0.Add(time.Second)
	if !b.CanQuote(fresh, 2*time.Second) {
		t.Fatal("within maxStaleness — must be quotable")
	}
	stale := t0.Add(3 * time.Second)
	if b.CanQuote(stale, 2*time.Second) {
		t.Fatal("beyond maxStaleness — must not be quotable")
	}
}

func TestCanQuoteMaxStalenessZeroDisablesCheck(t *testing.T) {
	b := New("A", "B")
	b.Update("A", t0, 100, 101)
	b.Update("B", t0, 50, 51)
	if !b.CanQuote(t0.Add(time.Hour), 0) {
		t.Fatal("maxStaleness<=0 must disable the freshness check (a backtest feed's quiet gaps are not outages)")
	}
}

func TestStaleReportsEachLegIndependently(t *testing.T) {
	b := New("A", "B")
	b.Update("A", t0, 100, 101)
	// legB never updated: OK=false, always stale regardless of "how old".
	staleA, staleB := b.Stale(t0, time.Second)
	if staleA {
		t.Fatal("legA just updated — must not be stale")
	}
	if !staleB {
		t.Fatal("legB never updated — must be stale")
	}
	staleA, _ = b.Stale(t0.Add(2*time.Second), time.Second)
	if !staleA {
		t.Fatal("legA now beyond maxStaleness — must be stale")
	}
}

func TestCrossPriceBuysAtAskSellsAtBid(t *testing.T) {
	b := New("A", "B")
	b.Update("A", t0, 100, 101)
	b.Update("B", t0, 50, 51)
	if got := b.CrossPrice("A", true); got != 101 {
		t.Fatalf("a buy on A must cross the ask: got %v want 101", got)
	}
	if got := b.CrossPrice("A", false); got != 100 {
		t.Fatalf("a sell on A must cross the bid: got %v want 100", got)
	}
	if got := b.CrossPrice("B", true); got != 51 {
		t.Fatalf("a buy on B must cross the ask: got %v want 51", got)
	}
}

func TestCrossPriceZeroBeforeAnyTouch(t *testing.T) {
	b := New("A", "B")
	if got := b.CrossPrice("A", true); got != 0 {
		t.Fatalf("no touch yet — CrossPrice must be 0, got %v", got)
	}
}

func TestBackoffUntilDefaultsToZeroAndIsSettable(t *testing.T) {
	b := New("A", "B")
	if !b.BackoffUntil().IsZero() {
		t.Fatal("a fresh Book must have no backoff")
	}
	until := t0.Add(5 * time.Second)
	b.SetBackoff(until)
	if !b.BackoffUntil().Equal(until) {
		t.Fatalf("BackoffUntil = %v, want %v", b.BackoffUntil(), until)
	}
}

func TestTouchValidRejectsZeroOrCrossedPrices(t *testing.T) {
	cases := []struct {
		name string
		t    Touch
		want bool
	}{
		{"zero value", Touch{}, false},
		{"ok but zero bid", Touch{OK: true, Bid: 0, Ask: 101}, false},
		{"ok but zero ask", Touch{OK: true, Bid: 100, Ask: 0}, false},
		{"crossed", Touch{OK: true, Bid: 101, Ask: 100}, false},
		{"valid", Touch{OK: true, Bid: 100, Ask: 101}, true},
		{"valid equal bid ask", Touch{OK: true, Bid: 100, Ask: 100}, true},
	}
	for _, c := range cases {
		if got := c.t.Valid(); got != c.want {
			t.Errorf("%s: Valid() = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestTouchSidePrice(t *testing.T) {
	tc := Touch{Bid: 100, Ask: 101}
	if got := tc.SidePrice(true); got != 100 {
		t.Fatalf("SidePrice(bid) = %v, want 100", got)
	}
	if got := tc.SidePrice(false); got != 101 {
		t.Fatalf("SidePrice(ask) = %v, want 101", got)
	}
}
