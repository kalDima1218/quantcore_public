package budget_test

import (
	"sync"
	"sync/atomic"
	"testing"

	"QuantCore/strategies/execengine2"
	"QuantCore/strategies/execengine2/budget"
)

func TestTakeKeepsReserve(t *testing.T) {
	t.Parallel()
	b, err := budget.New(10, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !b.Take(7, execengine2.LimitNormal) {
		t.Fatal("expected discretionary reservation to fit exactly above reserve")
	}
	if b.Take(1, execengine2.LimitNormal) {
		t.Fatal("discretionary placement consumed the hedge reserve")
	}
	if !b.Take(4, execengine2.LimitMust) {
		t.Fatal("mandatory placement was blocked")
	}
	if got := b.Remaining(); got != -1 {
		t.Fatalf("remaining = %d, want -1", got)
	}
}

func TestTakeIsAtomic(t *testing.T) {
	t.Parallel()
	b, err := budget.New(100, 0)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	var admitted atomic.Int64
	for range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if b.Take(1, execengine2.LimitNormal) {
				admitted.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := admitted.Load(); got != 100 {
		t.Fatalf("admitted = %d, want 100", got)
	}
	if got := b.Remaining(); got != 0 {
		t.Fatalf("remaining = %d, want 0", got)
	}
}

func TestOldBrokerValueCannotRaiseLimit(t *testing.T) {
	t.Parallel()
	b, err := budget.New(20, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !b.Take(3, execengine2.LimitNormal) {
		t.Fatal("take failed")
	}
	b.SetIfLower(20) // stale snapshot from before the take
	if got := b.Remaining(); got != 17 {
		t.Fatalf("remaining = %d, want 17", got)
	}
	b.SetIfLower(12)
	if got := b.Remaining(); got != 12 {
		t.Fatalf("remaining after conservative clamp = %d, want 12", got)
	}
}

func TestResetAndTake(t *testing.T) {
	t.Parallel()
	b, err := budget.New(1, 0)
	if err != nil {
		t.Fatal(err)
	}
	b.Reset(2)
	if !b.Take(1, execengine2.LimitNormal) {
		t.Fatal("take from reset window failed")
	}
	if got := b.Remaining(); got != 1 {
		t.Fatalf("remaining = %d, want 1", got)
	}
}
