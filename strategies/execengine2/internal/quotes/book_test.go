package quotes_test

import (
	"testing"
	"time"

	"QuantCore/strategies/execengine2"
	"QuantCore/strategies/execengine2/internal/quotes"
)

func TestReadyNeedsTwoPrices(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 26, 10, 0, 0, 0, time.UTC)
	b := quotes.New("A", "B")
	b.Update("A", now, 99, 100)
	if b.Ready(now, time.Second) {
		t.Fatal("book became ready without leg B")
	}
	b.Update("B", now, 199, 200)
	if !b.Ready(now, time.Second) {
		t.Fatal("fresh pair was not ready")
	}
	b.Update("A", now.Add(time.Millisecond), 101, 100)
	if b.Ready(now.Add(time.Millisecond), time.Second) {
		t.Fatal("crossed touch was accepted")
	}
}

func TestAgeAndOpenWait(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 26, 10, 0, 0, 0, time.UTC)
	b := quotes.New("A", "B")
	b.Update("A", now, 99, 100)
	b.Update("B", now, 199, 200)
	b.BlockOpen(now.Add(2 * time.Second))
	if b.Ready(now.Add(time.Second), time.Minute) {
		t.Fatal("open backoff was ignored")
	}
	if !b.Ready(now.Add(3*time.Second), time.Minute) {
		t.Fatal("book remained suppressed after backoff")
	}
	staleA, staleB := b.TooOld(now.Add(3*time.Second), time.Second)
	if !staleA || !staleB {
		t.Fatalf("stale = (%v, %v), want both true", staleA, staleB)
	}
	if got := b.Prices(execengine2.LegA).Bid; got != 99 {
		t.Fatalf("leg A bid = %v, want 99", got)
	}
}
