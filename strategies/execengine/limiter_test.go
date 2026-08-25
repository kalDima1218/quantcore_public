package execengine

import (
	"sync"
	"testing"
	"time"
)

// TestQuotaLimiterLegacyPermissiveUntilSet — старый конструктор: без данных
// брокера пускает всё (полагается на failure-backoff движка).
func TestQuotaLimiterLegacyPermissiveUntilSet(t *testing.T) {
	q := NewQuotaLimiter(20)
	now := time.Now()
	if ok, _ := q.Allow(now, 2); !ok {
		t.Fatal("legacy без Set обязан пускать (permissive)")
	}
	if s := q.String(); s != "n/a" {
		t.Fatalf("String()=%q, want n/a до Set", s)
	}
	q.Set(23, now.Add(time.Minute)) // 23 >= 2+20 -> пускать
	if ok, _ := q.Allow(now, 2); !ok {
		t.Fatal("23 >= 2+20 -> должен пускать")
	}
	q.Spend(now, 2) // 23 -> 21
	if ok, _ := q.Allow(now, 2); ok {
		t.Fatal("21 < 2+20 -> должен отказать (резерв margin)")
	}
}

// TestQuotaLimiterBootstrapGatesWithoutRefresh — КЛЮЧЕВОЙ фикс: fail-safe лимитер
// БЕЗ единого Set сам гейтит опены, резервируя margin под хеджи (раньше был
// fail-open — пускал всё и давал хеджу голодать).
func TestQuotaLimiterBootstrapGatesWithoutRefresh(t *testing.T) {
	q := NewQuotaLimiterBudget(20, 200, time.Minute)
	now := time.Now()
	// Первый Allow бутстрапит бюджет = 200. Тратим опенами (по 2) до резерва.
	opens := 0
	for {
		ok, _ := q.Allow(now, 2)
		if !ok {
			break
		}
		q.Spend(now, 2)
		opens++
		if opens > 1000 {
			t.Fatal("бутстрап-лимитер НЕ гейтит опены — fail-open (баг не починен)")
		}
	}
	// Опены остановились, оставив >= margin под хеджи.
	rem, ok := q.Remaining()
	if !ok {
		t.Fatal("bootstrap Remaining() обязан быть meaningful")
	}
	if rem < 20 {
		t.Fatalf("остаток %d < margin 20 — резерв под хедж не удержан", rem)
	}
	// Хеджи (Spend без Allow) всё ещё проходят по зарезервированному бюджету.
	q.Spend(now, 20)
	if r, _ := q.Remaining(); r != rem-20 {
		t.Fatalf("Spend хеджа не списался: %d -> %d", rem, r)
	}
}

// TestQuotaLimiterBootstrapSelfResetsWindow — самосброс бюджета по окну (живёт
// без рефрешера и не умирает в remaining=0 навсегда).
func TestQuotaLimiterBootstrapSelfResetsWindow(t *testing.T) {
	q := NewQuotaLimiterBudget(20, 200, time.Minute)
	now := time.Now()
	// Выжечь бюджет.
	for {
		ok, _ := q.Allow(now, 2)
		if !ok {
			break
		}
		q.Spend(now, 2)
	}
	if ok, _ := q.Allow(now, 2); ok {
		t.Fatal("после выжигания опены должны быть заблокированы")
	}
	// Следующее окно -> бюджет восстановлен.
	later := now.Add(61 * time.Second)
	if ok, _ := q.Allow(later, 2); !ok {
		t.Fatal("после сброса окна опены обязаны возобновиться (самосброс)")
	}
}

// TestQuotaLimiterSetOverridesBootstrap — реальные данные брокера авторитетнее
// бутстрапа.
func TestQuotaLimiterSetOverridesBootstrap(t *testing.T) {
	q := NewQuotaLimiterBudget(20, 200, time.Minute)
	now := time.Now()
	q.Allow(now, 2) // бутстрап -> 200
	q.Set(25, now.Add(30*time.Second))
	if r, _ := q.Remaining(); r != 25 {
		t.Fatalf("Set должен переопределить бутстрап: remaining=%d, want 25", r)
	}
	if ok, _ := q.Allow(now, 2); !ok {
		t.Fatal("25 >= 2+20 -> пускать")
	}
	q.Spend(now, 4) // 25 -> 21
	q.Spend(now, 2) // 21 -> 19
	if ok, _ := q.Allow(now, 2); ok {
		t.Fatal("19 < 2+20 -> отказать")
	}
}

// --- limiter corners ---

// A limiter denial with a known reset time must back the engine off until exactly that
// reset — not forever, not until the generic failure backoff.
func TestLimiterDenialBacksOffUntilReset(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	lim := &stubLimiter{ok: false, retryAt: openHour.Add(10 * time.Second)}
	e.SetLimiter(lim)
	clk := &fakeClock{t: openHour} // Allow's clock domain — see clock.go; kept in step with ts below
	e.SetClock(clk)
	seedBooks(e, openHour)

	e.OnState(buyState(openHour))
	if e.Working() {
		t.Fatal("a denied open must not place orders")
	}

	lim.ok = true // quota window resets
	clk.t = openHour.Add(5 * time.Second)
	e.OnState(buyState(openHour.Add(5 * time.Second)))
	if e.Working() {
		t.Fatal("the engine must stay backed off until the limiter's reset time")
	}
	clk.t = openHour.Add(11 * time.Second)
	e.OnState(buyState(openHour.Add(11 * time.Second)))
	if !e.Working() {
		t.Fatal("the engine must quote again after the reset")
	}
}

// QuotaLimiter must keep `margin` ops in reserve: a burst that would eat into the reserve
// is denied even though raw budget remains.
func TestQuotaLimiterKeepsMarginInReserve(t *testing.T) {
	q := NewQuotaLimiter(2)
	reset := openHour.Add(time.Minute)
	q.Set(4, reset)

	if ok, _ := q.Allow(openHour, 2); !ok {
		t.Fatal("4 remaining with margin 2 must allow a 2-op burst")
	}
	q.Spend(openHour, 2) // the granted burst is actually placed
	ok, retryAt := q.Allow(openHour, 1)
	if ok {
		t.Fatal("2 remaining with margin 2 must deny: the reserve is for hedges/cancels only")
	}
	if !retryAt.Equal(reset) {
		t.Fatalf("the denial must report the window reset, got %v want %v", retryAt, reset)
	}
}

// Allow must be check-only: a granted op that is never placed keeps its budget. Only
// Spend — the actual placement RPC — decrements, and Remaining tracks the local view.
func TestQuotaLimiterAllowChecksAndSpendDecrements(t *testing.T) {
	q := NewQuotaLimiter(2)
	q.Set(10, openHour.Add(time.Minute))

	q.Allow(openHour, 2) // granted but never placed
	if rem, known := q.Remaining(); !known || rem != 10 {
		t.Fatalf("Allow alone must not decrement, got remaining=%d known=%v", rem, known)
	}
	q.Spend(openHour, 3) // e.g. two maker legs plus an ungated taker hedge
	if rem, _ := q.Remaining(); rem != 7 {
		t.Fatalf("Spend(3) must land remaining at 7, got %d", rem)
	}
}

// The engine must book EVERY placement RPC on the limiter: the clip's two maker legs,
// and the ungated taker top-up a maker fill triggers — including failed attempts.
func TestEngineSpendsQuotaOnMakersAndTakers(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	lim := &stubLimiter{ok: true}
	e.SetLimiter(lim)
	seedBooks(e, openHour)

	e.OnState(buyState(openHour)) // opens a clip: two maker placements
	if lim.spent != 2 {
		t.Fatalf("two maker legs must spend 2, got %d", lim.spent)
	}

	tk.failN = 1                                               // first hedge attempt fails, the retry lands
	e.OnFill(openHour, m.id(testLegA), testLegA, true, 1, 100) // partial maker fill → taker top-up
	if lim.spent != 4 {
		t.Fatalf("a failed and a successful taker attempt must spend 2 more (total 4), got %d", lim.spent)
	}
}

// Before the first Set the limiter has no data and must allow (the engine's failure
// backoff is the safety net) rather than deadlock the strategy at startup.
func TestQuotaLimiterUnknownBudgetAllows(t *testing.T) {
	q := NewQuotaLimiter(20)
	if ok, _ := q.Allow(openHour, 2); !ok {
		t.Fatal("an unknown budget must allow")
	}
}

// The refresher goroutine Sets while the engine goroutine Allows: the limiter must be
// race-free under the race detector.
func TestQuotaLimiterConcurrentSetAndAllow(t *testing.T) {
	q := NewQuotaLimiter(2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			q.Set(200, openHour.Add(time.Duration(i)*time.Millisecond))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			q.Allow(openHour, 2)
			q.Spend(openHour, 1)
			q.Remaining()
		}
	}()
	wg.Wait()
}

// A broker-reported remaining budget at or below the margin (or negative after an
// accounting glitch) must deny discretionary opens — the reserve is for hedges/cancels.
func TestQuotaLimiterLowAndNegativeBudgetsDeny(t *testing.T) {
	q := NewQuotaLimiter(5)
	q.Set(5, openHour.Add(time.Minute)) // exactly the margin left
	if ok, _ := q.Allow(openHour, 1); ok {
		t.Fatal("a budget equal to the margin must deny")
	}
	q.Set(-3, openHour.Add(time.Minute)) // corrupt/negative budget
	if ok, _ := q.Allow(openHour, 1); ok {
		t.Fatal("a negative budget must deny")
	}
}

// Spend must roll the bootstrap window over BEFORE debiting it, at the exact boundary:
// a spend one nanosecond before resetAt still owes the OLD window, a spend at or after
// resetAt belongs to a FRESH one. Allow already gets this right (via refreshWindow);
// Spend used to skip refreshWindow entirely and always debit whatever remaining held,
// stale or not.
func TestQuotaLimiterSpendRollsOverWindowAtBoundary(t *testing.T) {
	const margin, budget, spentInWindow1 = 20, 200, 150
	window := time.Minute

	cases := []struct {
		name string
		at   time.Time
		want int // remaining immediately after Spend(at, 3)
	}{
		// Still window 1: its already-spent-down remaining (50) takes the debit.
		{"just before reset: still the old window", openHour.Add(window).Add(-time.Nanosecond), budget - spentInWindow1 - 3},
		// Window 2 has begun: a fresh budget takes the debit, window 1's spend is behind it.
		{"exactly at reset: already the new window", openHour.Add(window), budget - 3},
		{"just after reset: the new window", openHour.Add(window).Add(time.Nanosecond), budget - 3},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q := NewQuotaLimiterBudget(margin, budget, window)
			q.Allow(openHour, 1)              // bootstrap window 1: remaining=200, resetAt=openHour+1m
			q.Spend(openHour, spentInWindow1) // pay it down so old-vs-new-window is observable below
			q.Spend(c.at, 3)
			if rem, _ := q.Remaining(); rem != c.want {
				t.Fatalf("remaining=%d, want %d (window must roll over before the debit)", rem, c.want)
			}
		})
	}
}

// The taker hedge is UNGATED: it Spends without a preceding Allow. If it happens to be
// the very first RPC after a window boundary — nothing polled Allow yet to roll the
// window over — its debit must still land against the FRESH window, not the stale one
// (which a later Allow would then silently reset, erasing the hedge's spend from the
// budget the engine believes it has).
func TestQuotaLimiterUngatedHedgeSpendAsFirstRPCOfNewWindow(t *testing.T) {
	const margin, budget = 20, 200
	window := time.Minute
	q := NewQuotaLimiterBudget(margin, budget, window)

	q.Allow(openHour, 1) // bootstrap window 1
	q.Spend(openHour, budget-margin-5)
	if rem, _ := q.Remaining(); rem != margin+5 {
		t.Fatalf("setup: remaining=%d, want %d", rem, margin+5)
	}

	// Window 1 elapses with no further Allow — a hedge fires straight into window 2.
	afterReset := openHour.Add(window).Add(time.Nanosecond)
	q.Spend(afterReset, 1)
	if rem, _ := q.Remaining(); rem != budget-1 {
		t.Fatalf("hedge spend lost the window-2 rollover: remaining=%d, want %d", rem, budget-1)
	}

	// A subsequent Allow in the same (already-rolled) window must NOT roll over again
	// and must NOT forget the hedge's spend.
	if ok, _ := q.Allow(afterReset.Add(time.Second), margin+1); !ok {
		t.Fatal("budget-1 with margin left over must still allow")
	}
	if rem, _ := q.Remaining(); rem != budget-1 {
		t.Fatalf("a later Allow must not re-roll the window it already rolled: remaining=%d, want %d", rem, budget-1)
	}
}

// Legacy limiter (no self-managed window, windowLimit==0) has nothing to roll over —
// Spend must keep debiting the broker-reported remaining exactly as before.
func TestQuotaLimiterLegacySpendUnaffectedByRollover(t *testing.T) {
	q := NewQuotaLimiter(5)
	q.Set(10, openHour.Add(time.Minute))
	q.Spend(openHour.Add(time.Hour), 3) // far past any "window" — legacy has none
	if rem, _ := q.Remaining(); rem != 7 {
		t.Fatalf("remaining=%d, want 7 (legacy Spend must not gain a rollover side effect)", rem)
	}
}

// CONTRACT (not a bug): an authoritative broker Set always wins over any Spend that
// landed while the usage-metrics RPC was in flight — Set's remaining is a snapshot from
// request time and knows nothing about spends since. This pins the accepted staleness
// window (bounded by quotaRPCTimeout, and self-healed by the next 5s poll) so a future
// change to the merge behaviour is a deliberate decision, not a silent regression.
func TestQuotaLimiterSetOverwritesConcurrentSpend(t *testing.T) {
	q := NewQuotaLimiterBudget(20, 200, time.Minute)
	q.Set(50, openHour.Add(time.Minute)) // broker snapshot taken before the in-flight spend

	q.Spend(openHour, 10) // lands while that RPC is still in flight: 50 -> 40

	q.Set(50, openHour.Add(time.Minute)) // the in-flight RPC's response finally arrives
	if rem, _ := q.Remaining(); rem != 50 {
		t.Fatalf("remaining=%d, want 50 (Set is last-write-wins by design; the 10-op spend is absorbed once the next poll runs)", rem)
	}
}
