package execengine

import (
	"strconv"
	"sync"
	"time"
)

// noLimit is the default permissive limiter (unit tests): every burst is allowed. The
// engine's failure backoff remains the safety net against request storms.
type noLimit struct{}

func (noLimit) Allow(time.Time, int) (bool, time.Time) { return true, time.Time{} }
func (noLimit) Spend(time.Time, int)                   {}

const (
	// DefaultPlaceOrderBudget is Finam's documented per-minute placeOrder quota. It is
	// the FAIL-SAFE bootstrap budget a QuotaLimiter self-manages until (and if ever) the
	// usage-metrics refresher reports the broker's real remaining count — so opens are
	// gated to keep the hedge reserve even when the refresher has not polled yet, is
	// misconfigured, or has died. Conservative by construction (≤ the real limit): a
	// wrong-low guess only throttles opens, it never starves a mandatory hedge.
	DefaultPlaceOrderBudget = 200
	// DefaultQuotaWindow is the broker's quota window (200 requests per MINUTE).
	DefaultQuotaWindow = time.Minute
)

// QuotaLimiter tracks the broker's remaining order-placement budget for the current
// window and keeps `margin` ops in reserve so a burst of clip opens can never consume the
// quota a mandatory taker hedge needs. A background refresher calls Set with fresh numbers
// from the usage-metrics RPC; between refreshes the limiter keeps its own running count:
// the engine calls Spend for EVERY placement RPC it actually issues — gated clip opens and
// re-pegs, but also the ungated taker hedges/top-ups and failed attempts, which a broker
// poll alone would only reveal seconds later.
//
// FAIL-SAFE: when constructed with a window budget (NewQuotaLimiterBudget), the limiter is
// self-sufficient — it bootstraps from that budget and re-bootstraps every window, so it
// NEVER blind-permits opens (the old fail-OPEN default that let a fast burst starve a
// hedge into a naked leg). A real Set only refines the estimate; if the refresher never
// runs or dies, the self-managed budget still reserves the hedge headroom. Constructed with
// NewQuotaLimiter (no budget) it keeps the legacy behaviour: permissive until the first Set.
//
// It is concurrency-safe: Set runs on the refresher goroutine, Allow/Spend on the engine
// goroutine.
type QuotaLimiter struct {
	mu          sync.Mutex
	margin      int
	windowLimit int           // >0 → fail-safe self-managed budget; 0 → legacy (permissive until Set)
	window      time.Duration // self-reset period for the bootstrap budget
	remaining   int
	resetAt     time.Time
	known       bool // a REAL broker Set has arrived (authoritative); false → bootstrap/legacy
}

// NewQuotaLimiter builds a legacy QuotaLimiter (permissive until the first broker Set)
// that keeps margin ops in reserve once quota data arrives. Prefer NewQuotaLimiterBudget
// for live trading so opens are gated even before/without a usage-metrics refresher.
func NewQuotaLimiter(margin int) *QuotaLimiter { return &QuotaLimiter{margin: margin} }

// NewQuotaLimiterBudget builds a FAIL-SAFE QuotaLimiter that self-manages windowLimit ops
// per window (re-bootstrapping every window) until/while the broker refresher feeds real
// numbers via Set — so a mandatory hedge is never starved by opens even if the refresher
// is absent or dies. margin ops stay in reserve for hedges/cancels/refresh-lag.
func NewQuotaLimiterBudget(margin, windowLimit int, window time.Duration) *QuotaLimiter {
	if window <= 0 {
		window = DefaultQuotaWindow
	}
	return &QuotaLimiter{margin: margin, windowLimit: windowLimit, window: window}
}

// Set installs the latest known remaining budget and window reset from the broker. It is
// authoritative: it overrides the self-managed bootstrap estimate with real data.
func (q *QuotaLimiter) Set(remaining int, resetAt time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.remaining, q.resetAt, q.known = remaining, resetAt, true
}

// refreshWindow re-bootstraps the self-managed budget when its window has elapsed (fail-safe
// mode only). It also self-heals a dead refresher: once the broker-reported resetAt passes
// with no fresh Set, the window is assumed rolled over and the budget restored. Caller holds mu.
func (q *QuotaLimiter) refreshWindow(now time.Time) {
	if q.windowLimit <= 0 {
		return // legacy limiter: no self-managed budget
	}
	if q.resetAt.IsZero() || !now.Before(q.resetAt) {
		q.remaining = q.windowLimit
		q.resetAt = now.Add(q.window)
	}
}

// Allow reports whether ops discretionary placements fit the remaining budget with the
// margin still in reserve. It only checks — the decrement happens in Spend, at the moment
// an RPC is actually issued — so a granted-but-never-placed op is not lost from the budget.
func (q *QuotaLimiter) Allow(now time.Time, ops int) (bool, time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.refreshWindow(now) // fail-safe: lazily restore the budget when the window rolls over
	if !q.known && q.windowLimit <= 0 {
		return true, time.Time{} // legacy, no data yet — rely on the engine's failure backoff
	}
	if q.remaining >= ops+q.margin {
		return true, q.resetAt
	}
	return false, q.resetAt
}

// Spend books ops placement RPCs the engine actually issued against the budget. Failed
// attempts count too — the broker metered the request whether or not the order stuck.
// It rolls the bootstrap window over (same as Allow) BEFORE debiting: Spend is also the
// ungated taker-hedge path, which never calls Allow first, so a hedge landing as the
// first RPC after a window boundary must still debit the FRESH window — otherwise a
// later Allow's own rollover would silently reset remaining and forget this spend.
func (q *QuotaLimiter) Spend(now time.Time, ops int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.refreshWindow(now)              // fail-safe: lazily restore the budget when the window rolls over
	if q.known || q.windowLimit > 0 { // track in both real and bootstrap modes
		q.remaining -= ops
	}
}

// Remaining reports the current local view of the remaining budget (the last broker
// refresh, or the self-managed bootstrap, minus everything spent since) and whether the
// number is meaningful yet (real data OR a bootstrap budget).
func (q *QuotaLimiter) Remaining() (int, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.remaining, q.known || q.windowLimit > 0
}

// String renders the local remaining-budget view for status blocks: the number, or "n/a"
// until the first broker refresh has arrived (legacy limiter with no bootstrap).
func (q *QuotaLimiter) String() string {
	rem, ok := q.Remaining()
	if !ok {
		return "n/a"
	}
	return strconv.Itoa(rem)
}
