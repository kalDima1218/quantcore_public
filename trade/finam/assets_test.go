package finam

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// scriptedFetch is one entry in a fetch script fed to runMonitor.
type scriptedFetch struct {
	sessions []session
	err      error
}

// runMonitor drives monitorMarketOpen over a scripted fetch sequence (the last
// entry repeats), waits until the loop has called fetch at least len(script)
// times, then stops it and returns every value pushed to onUpdate. The safety net
// is fixed at 1 hour (far outside every test's window) so only session-boundary
// crossings drive re-fetches — each script's sessions must therefore end quickly
// (tens of milliseconds) to make the next scripted fetch happen inside the test's
// timeout instead of waiting on a boundary an hour away.
func runMonitor(t *testing.T, script []scriptedFetch) []bool {
	t.Helper()

	fetched := make(chan struct{}, len(script)+8)
	var n int
	fetch := func() ([]session, error) {
		s := script[min(n, len(script)-1)]
		n++
		select {
		case fetched <- struct{}{}:
		default:
		}
		return s.sessions, s.err
	}

	var mu sync.Mutex
	var got []bool

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		monitorMarketOpen(ctx, fetch, "LEGA@RTSX", time.Hour,
			func(time.Duration) time.Duration { return time.Hour },
			func() time.Duration { return 0 },
			func(open bool) {
				mu.Lock()
				got = append(got, open)
				mu.Unlock()
			})
	}()

	for i := range script {
		select {
		case <-fetched:
		case <-time.After(5 * time.Second):
			t.Fatalf("fetch %d never ran", i+1)
		}
	}
	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	return append([]bool(nil), got...)
}

var errSchedule = errors.New("failed to get schedule: rpc error: code = DeadlineExceeded")

// Characterisation: answered fetches must keep reaching the caller, so the fix for
// the error path cannot degenerate into "never report anything".
func TestMonitorMarketOpenReportsAnsweredFetches(t *testing.T) {
	now := time.Now()
	shortOpen := []session{{sessionType: "CORE_TRADING", start: now.Add(-time.Hour), end: now.Add(20 * time.Millisecond)}}
	longClosed := []session{{sessionType: "CLOSED", start: now.Add(-time.Hour), end: now.Add(time.Hour)}}

	got := runMonitor(t, []scriptedFetch{{sessions: shortOpen}, {sessions: longClosed}})

	if len(got) < 2 || !got[0] || got[1] {
		t.Fatalf("updates = %v, want to start with [true false]", got)
	}
}

// A Schedule call that times out means the broker did not answer — it is NOT the
// exchange reporting itself shut. Folding that into "closed" is what stopped the
// listener from writing a single row on 2026-07-30: the streams kept delivering,
// the gate said closed, and the data was dropped silently.
func TestMonitorMarketOpenKeepsLastViewWhenScheduleFails(t *testing.T) {
	now := time.Now()
	shortWindow := []session{{sessionType: "CORE_TRADING", start: now.Add(-time.Hour), end: now.Add(20 * time.Millisecond)}}

	got := runMonitor(t, []scriptedFetch{{sessions: shortWindow}, {err: errSchedule}, {err: errSchedule}})

	if len(got) != 1 || !got[0] {
		t.Fatalf("updates = %v, want exactly [true]: a failed fetch must not be reported as a closed market", got)
	}
}

// The core promise of the redesign: crossing a session boundary that is already
// inside the cached window must NOT make a network call.
func TestMonitorMarketOpenRecomputesLocallyAcrossBoundary(t *testing.T) {
	now := time.Now()
	sessions := []session{
		{sessionType: "CORE_TRADING", start: now.Add(-time.Hour), end: now.Add(30 * time.Millisecond)},
		{sessionType: "CLOSED", start: now.Add(30 * time.Millisecond), end: now.Add(time.Hour)},
	}

	var fetchCalls int32
	fetch := func() ([]session, error) {
		atomic.AddInt32(&fetchCalls, 1)
		return sessions, nil
	}

	var mu sync.Mutex
	var got []bool
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		monitorMarketOpen(ctx, fetch, "LEGA@RTSX", time.Hour,
			func(time.Duration) time.Duration { return time.Hour }, // safety net must not fire
			func() time.Duration { return 0 },
			func(open bool) {
				mu.Lock()
				got = append(got, open)
				mu.Unlock()
			})
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("got %d updates after 5s, want at least 2 (open, then closed after the boundary)", n)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	if len(got) < 2 || !got[0] || got[1] {
		t.Fatalf("updates = %v, want to start with [true false] (open, then closed across the boundary)", got)
	}
	if n := atomic.LoadInt32(&fetchCalls); n != 1 {
		t.Fatalf("fetch called %d times, want exactly 1: the boundary crossing must be resolved from the cached window, not a new fetch", n)
	}
}

// Once now walks past every session in the cached window, a fresh fetch is
// required even though the safety net is far away.
func TestMonitorMarketOpenRefetchesWhenWindowExhausted(t *testing.T) {
	now := time.Now()
	first := []session{{sessionType: "CORE_TRADING", start: now.Add(-time.Hour), end: now.Add(30 * time.Millisecond)}}
	second := []session{{sessionType: "CORE_TRADING", start: now.Add(-time.Hour), end: now.Add(time.Hour)}}

	var calls int32
	fetch := func() ([]session, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			return first, nil
		}
		return second, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		monitorMarketOpen(ctx, fetch, "LEGA@RTSX", time.Hour,
			func(time.Duration) time.Duration { return time.Hour }, // safety net must not be what triggers this
			func() time.Duration { return 0 },
			func(bool) {})
	}()

	deadline := time.Now().Add(5 * time.Second)
	for atomic.LoadInt32(&calls) < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("fetch called %d times after 5s, want a second fetch once the first window's only session ends", atomic.LoadInt32(&calls))
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
}

// The brokersim-degenerate case: a window that never exhausts (a single session
// repeated forever) must still refetch once the safety net elapses.
func TestMonitorMarketOpenRefetchesOnSafetyNetEvenWithinWindow(t *testing.T) {
	now := time.Now()
	sessions := []session{{sessionType: "CORE_TRADING", start: now.Add(-time.Hour), end: now.Add(time.Hour)}}

	var calls int32
	fetch := func() ([]session, error) {
		atomic.AddInt32(&calls, 1)
		return sessions, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		monitorMarketOpen(ctx, fetch, "LEGA@RTSX", time.Hour,
			func(time.Duration) time.Duration { return 30 * time.Millisecond }, // tiny safety net
			func() time.Duration { return 0 },
			func(bool) {})
	}()

	deadline := time.Now().Add(5 * time.Second)
	for atomic.LoadInt32(&calls) < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("fetch called %d times after 5s, want a second fetch once the safety net elapses", atomic.LoadInt32(&calls))
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
}

// When the cached window carries no forward information at all — every
// session in it has already ended, e.g. a symbol past contract expiry — the
// loop must not sit on the (far away) safety net: it retries on the short
// cadence instead. Regression: nextWake used to fall through to
// w.safetyDeadline whenever nextBoundary reported ok=false, which could pin
// the gate for up to 2h instead of retrying quickly.
func TestMonitorMarketOpenRetriesQuicklyWhenWindowExhausted(t *testing.T) {
	now := time.Now()
	// Every session already ended: nextBoundary(sessions, now) reports ok=false
	// forever, so only the exhausted-window retry path (never a boundary
	// crossing) can explain a second fetch.
	sessions := []session{{sessionType: "CORE_TRADING", start: now.Add(-time.Hour), end: now.Add(-time.Minute)}}

	var calls int32
	fetch := func() ([]session, error) {
		atomic.AddInt32(&calls, 1)
		return sessions, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		monitorMarketOpen(ctx, fetch, "LEGA@RTSX", time.Hour,
			func(time.Duration) time.Duration { return time.Hour }, // safety net alone must never explain a quick 2nd fetch
			func() time.Duration { return 0 },                      // exercises nextWake's busy-loop clamp too
			func(bool) {})
	}()

	deadline := time.Now().Add(300 * time.Millisecond)
	for atomic.LoadInt32(&calls) < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("fetch called %d times after 300ms, want a second fetch quickly: an exhausted window must retry on the short cadence, not wait on the 1h safety net", atomic.LoadInt32(&calls))
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
}

// window's doc comment promises the safety-net deadline is drawn fresh on
// every fetch, not fixed once at the first fetch. A regression that computes
// it once and never updates it again would hot-loop in production: once that
// single deadline elapses, every subsequent wake sees now >= safetyDeadline
// and refetches immediately, forever. Prove the SECOND safetyNetFn call's
// return value is actually the new deadline: make the first call short and
// every later call long, then check exactly 2 fetches happen (not 3+).
func TestMonitorMarketOpenSafetyNetIsReArmedOnEveryFetch(t *testing.T) {
	now := time.Now()
	// A session end far beyond any plausible safety deadline (30ms, then 1h):
	// nextWake's min(boundary, safetyDeadline) must resolve to the safety
	// deadline on every fetch, so the quiet period after fetch #2 is actually
	// caused by the re-armed 1h value — not by the boundary happening to be
	// close by (which would let a stale, never-updated deadline pass unnoticed).
	sessions := []session{{sessionType: "CORE_TRADING", start: now.Add(-time.Hour), end: now.Add(24 * time.Hour)}}

	var calls int32
	var safetyCalls int32
	fetch := func() ([]session, error) {
		atomic.AddInt32(&calls, 1)
		return sessions, nil
	}
	safetyNetFn := func(time.Duration) time.Duration {
		if atomic.AddInt32(&safetyCalls, 1) == 1 {
			return 30 * time.Millisecond
		}
		return time.Hour
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		monitorMarketOpen(ctx, fetch, "LEGA@RTSX", time.Hour,
			safetyNetFn,
			func() time.Duration { return 0 },
			func(bool) {})
	}()

	deadline := time.Now().Add(time.Second)
	for atomic.LoadInt32(&calls) < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("fetch called %d times after 1s, want exactly 2 once the first (30ms) safety net elapses", atomic.LoadInt32(&calls))
		}
		time.Sleep(time.Millisecond)
	}

	// If the second safetyNetFn call's 1h return value were being ignored (the
	// deadline not re-armed), the loop would see now >= the stale first
	// deadline on every subsequent wake and refetch again immediately,
	// hot-looping toward a 3rd, 4th, ... fetch well within this window.
	time.Sleep(200 * time.Millisecond)
	if n := atomic.LoadInt32(&calls); n != 2 {
		t.Fatalf("fetch called %d times, want exactly 2: the second safetyNetFn call's 1h deadline must actually be armed, not ignored", n)
	}

	cancel()
	<-done
}

// Regression for the bug the 2026-08-12 redesign fixed: once the safety-net
// deadline elapses, a background refresh that keeps failing must NOT stop a
// session-boundary crossing that's already inside the cached window from being
// applied locally. Before the fix, a failing refresh at/after the safety-net
// deadline fell into a blocking retry loop, and a boundary crossing due later
// (even one already sitting in the cache) was silently skipped until some fetch
// eventually succeeded.
func TestMonitorMarketOpenAppliesBoundaryDespiteFailedRefresh(t *testing.T) {
	now := time.Now()
	sessions := []session{
		{sessionType: "CORE_TRADING", start: now.Add(-time.Hour), end: now.Add(80 * time.Millisecond)},
		{sessionType: "CLOSED", start: now.Add(80 * time.Millisecond), end: now.Add(time.Hour)},
	}

	var calls int32
	fetch := func() ([]session, error) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			return sessions, nil
		}
		return nil, errSchedule
	}

	var mu sync.Mutex
	var got []bool
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		monitorMarketOpen(ctx, fetch, "LEGA@RTSX", time.Hour,
			func(time.Duration) time.Duration { return 30 * time.Millisecond }, // safety net elapses well before the +80ms boundary
			func() time.Duration { return 0 },
			func(open bool) {
				mu.Lock()
				got = append(got, open)
				mu.Unlock()
			})
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := len(got)
		var last bool
		if n > 0 {
			last = got[n-1]
		}
		mu.Unlock()
		if n > 0 && !last {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("got %v after 5s, want the CLOSED transition (false) to eventually appear despite every refresh after the bootstrap fetch failing", got)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done

	// >=2 (bootstrap success + at least one failed background attempt) is the only
	// floor the code actually guarantees: the tick that first observes now past the
	// safety-net deadline always attempts a fetch. A higher floor would assume a
	// retry cadence tight enough to land a 2nd retry before the 80ms boundary, which
	// a starved goroutine (GC pause, loaded CI box) isn't guaranteed to hit.
	if n := atomic.LoadInt32(&calls); n < 2 {
		t.Fatalf("fetch called %d times, want at least 2: the bootstrap success plus a failed background attempt before the boundary — otherwise this isn't exercising the failing-refresh path", n)
	}
}

func TestNextPollDelay(t *testing.T) {
	cases := []struct {
		name     string
		interval time.Duration
		jitter   time.Duration
		want     time.Duration
	}{
		{"positive jitter adds", 60 * time.Second, 10 * time.Second, 70 * time.Second},
		{"negative jitter subtracts", 60 * time.Second, -10 * time.Second, 50 * time.Second},
		{"jitter driving delay to zero clamps to interval", 60 * time.Second, -60 * time.Second, 60 * time.Second},
		{"jitter driving delay negative clamps to interval", 60 * time.Second, -90 * time.Second, 60 * time.Second},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := nextPollDelay(c.interval, c.jitter); got != c.want {
				t.Fatalf("nextPollDelay(%v, %v) = %v, want %v", c.interval, c.jitter, got, c.want)
			}
		})
	}
}

func TestRandJitterStaysInRange(t *testing.T) {
	const interval = 60 * time.Second
	max := time.Duration(float64(interval) * 0.20)
	for i := 0; i < 1000; i++ {
		j := randJitter(interval)
		if j < -max || j > max {
			t.Fatalf("randJitter(%v) = %v, want within [%v, %v]", interval, j, -max, max)
		}
	}
}

func TestIsTradingType(t *testing.T) {
	cases := []struct {
		sessionType string
		want        bool
	}{
		{"EARLY_TRADING", true},
		{"CORE_TRADING", true},
		{"LATE_TRADING", true},
		{"CLOSED", false},
		{"OPENING_AUCTION", false},
		{"CLEARING", false},
		{"", false},
	}
	for _, c := range cases {
		if got := isTradingType(c.sessionType); got != c.want {
			t.Errorf("isTradingType(%q) = %v, want %v", c.sessionType, got, c.want)
		}
	}
}

func TestClassify(t *testing.T) {
	now := time.Now()
	sessions := []session{
		{sessionType: "CLOSED", start: now.Add(-2 * time.Hour), end: now.Add(-time.Hour)},
		{sessionType: "CORE_TRADING", start: now.Add(-time.Hour), end: now.Add(time.Hour)},
	}

	if !classify(sessions, now) {
		t.Fatalf("classify at now (inside CORE_TRADING) = false, want true")
	}
	if classify(sessions, now.Add(-90*time.Minute)) {
		t.Fatalf("classify inside CLOSED session = true, want false")
	}
	if classify(sessions, now.Add(5*time.Hour)) {
		t.Fatalf("classify outside every known session = true, want false (no coverage found)")
	}
}

func TestNextBoundary(t *testing.T) {
	now := time.Now()
	sessions := []session{
		{sessionType: "CORE_TRADING", start: now.Add(-time.Hour), end: now.Add(time.Hour)},
		{sessionType: "CLOSED", start: now.Add(time.Hour), end: now.Add(2 * time.Hour)},
	}

	boundary, ok := nextBoundary(sessions, now)
	if !ok || !boundary.Equal(now.Add(time.Hour)) {
		t.Fatalf("nextBoundary = (%v, %v), want (%v, true)", boundary, ok, now.Add(time.Hour))
	}

	if _, ok := nextBoundary(sessions, now.Add(3*time.Hour)); ok {
		t.Fatalf("nextBoundary past every known boundary should report ok=false (window exhausted)")
	}
}

func TestSafetyNetStaysInRange(t *testing.T) {
	const interval = time.Hour
	for i := 0; i < 1000; i++ {
		d := safetyNet(interval)
		if d < interval || d >= 2*interval {
			t.Fatalf("safetyNet(%v) = %v, want within [%v, %v)", interval, d, interval, 2*interval)
		}
	}
}
