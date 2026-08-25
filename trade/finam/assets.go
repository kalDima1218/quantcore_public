package finam

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/assets"
)

// jitterFraction is how far a failed-fetch retry (scheduleRetryInterval, via
// randJitter — see MonitorMarketOpen's wiring) may drift from that base
// interval in either direction, as a fraction of it. A fixed retry period
// would let every symbol's retry loop stay in phase forever (they all start
// at process boot), so a broad Schedule outage would retry many symbols
// within milliseconds of each other instead of spreading the retries out.
const jitterFraction = 0.20

// nextPollDelay is the pure core of the reschedule decision: interval plus
// jitter, clamped back to interval if that would be non-positive (a
// misbehaving injected jitter must never produce a busy-loop or a negative
// timer duration).
func nextPollDelay(interval, jitter time.Duration) time.Duration {
	d := interval + jitter
	if d <= 0 {
		return interval
	}
	return d
}

// randJitter returns a value uniform in [-jitterFraction*interval, +jitterFraction*interval].
func randJitter(interval time.Duration) time.Duration {
	span := time.Duration(float64(interval) * jitterFraction)
	if span <= 0 {
		return 0
	}
	return time.Duration(rand.Int63n(int64(2*span)+1)) - span
}

// session is one Schedule interval, reduced to the fields the timeline-walk logic
// in monitorMarketOpen needs — decoupled from the proto types so the walk logic
// doesn't reach into assets.ScheduleResponse_Sessions directly.
type session struct {
	sessionType string
	start, end  time.Time
}

// isTradingType is MarketOpen's open/closed mapping, extracted so MarketOpen and
// the new timeline-walk classifier (classify, below) share one definition and can
// never drift apart.
func isTradingType(sessionType string) bool {
	return sessionType == "EARLY_TRADING" || sessionType == "CORE_TRADING" || sessionType == "LATE_TRADING"
}

// classify reports whether now falls inside a tradeable session in sessions. A
// successful fetch where no session covers now is NOT the same as "unknown" — the
// broker answered and the empirically-verified real Schedule response tiles ~3
// days with zero gaps, so this can only return false here for a positively known
// reason (a CLOSED/CLEARING/OPENING_AUCTION session, or truly no data — the caller
// treats both the same way MarketOpen always has).
func classify(sessions []session, now time.Time) bool {
	for _, s := range sessions {
		if (now.After(s.start) || now.Equal(s.start)) && now.Before(s.end) {
			return isTradingType(s.sessionType)
		}
	}
	return false
}

// nextBoundary returns the earliest Start or End time in sessions strictly after
// now, and whether one was found. ok=false means now has walked past every known
// session's end — the cached window is exhausted from this point on.
func nextBoundary(sessions []session, now time.Time) (time.Time, bool) {
	var best time.Time
	found := false
	consider := func(t time.Time) {
		if t.After(now) && (!found || t.Before(best)) {
			best, found = t, true
		}
	}
	for _, s := range sessions {
		consider(s.start)
		consider(s.end)
	}
	return best, found
}

// safetyNet draws a fresh uniform random duration in [interval, 2*interval) every
// call — not fixed once at poller startup — so that many pollers starting together
// at process boot (one per unique symbol, per sessionTracker) drift apart instead
// of staying phase-locked on their safety-net refetch forever. Same rationale as
// randJitter, applied to the new safety-net cadence instead of the old recurring
// tick.
func safetyNet(interval time.Duration) time.Duration {
	if interval <= 0 {
		return 0
	}
	return interval + time.Duration(rand.Int63n(int64(interval)))
}

func GetMarketMode(client *Client, symbol string) (string, error) {
	conn, ctx, cancel, err := client.dial(context.Background())
	if err != nil {
		return "", fmt.Errorf("failed to create grpc connection: %w", err)
	}
	defer cancel()

	assetsClient := assets.NewAssetsServiceClient(conn)

	scheduleResp, err := assetsClient.Schedule(ctx, &assets.ScheduleRequest{
		Symbol: symbol,
	})
	if err != nil {
		return "", fmt.Errorf("failed to get schedule: %w", err)
	}

	now := time.Now().UTC()

	for _, session := range scheduleResp.Sessions {
		if session.Interval == nil {
			continue
		}

		var startTime, endTime time.Time

		if session.Interval.StartTime != nil {
			startTime = session.Interval.StartTime.AsTime().UTC()
		}

		if session.Interval.EndTime != nil {
			endTime = session.Interval.EndTime.AsTime().UTC()
		}

		if !startTime.IsZero() && !endTime.IsZero() {
			if (now.After(startTime) || now.Equal(startTime)) && now.Before(endTime) {
				return session.Type, nil
			}
		}
	}

	return "", nil
}

// getSchedule fetches every session Schedule currently knows for symbol. Entries
// missing an Interval or a Start/End time are dropped — GetMarketMode's linear scan
// already treats such an entry as "never matches", so no caller can observe the
// difference.
func getSchedule(client *Client, symbol string) ([]session, error) {
	conn, ctx, cancel, err := client.dial(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to create grpc connection: %w", err)
	}
	defer cancel()

	assetsClient := assets.NewAssetsServiceClient(conn)

	scheduleResp, err := assetsClient.Schedule(ctx, &assets.ScheduleRequest{
		Symbol: symbol,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get schedule: %w", err)
	}

	var out []session
	for _, s := range scheduleResp.Sessions {
		if s.Interval == nil || s.Interval.StartTime == nil || s.Interval.EndTime == nil {
			continue
		}
		out = append(out, session{
			sessionType: s.Type,
			start:       s.Interval.StartTime.AsTime().UTC(),
			end:         s.Interval.EndTime.AsTime().UTC(),
		})
	}
	return out, nil
}

// MarketOpen reports whether symbol is inside a trading session right now. The error
// is returned rather than folded into the bool so a caller can tell "closed" apart
// from "don't know": a failed Schedule call means the broker did not answer, which is
// not the same as the exchange being shut.
func MarketOpen(client *Client, symbol string) (bool, error) {
	sessionType, err := GetMarketMode(client, symbol)
	if err != nil {
		return false, err
	}

	return isTradingType(sessionType), nil
}

// IsMarketOpen is the fail-closed shorthand: an unanswered Schedule call reads as
// "closed". Only safe where not acting is the conservative choice (the spread
// runner's initial probe); anything that RECORDS data wants MonitorMarketOpen, which
// keeps its last view instead.
func IsMarketOpen(client *Client, symbol string) bool {
	open, err := MarketOpen(client, symbol)
	if err != nil {
		mlog.Printf("[%s] Error getting trading session: %v", symbol, err)
		return false
	}

	return open
}

// scheduleRetryInterval is the base cadence for retrying a FAILED fetch — the old
// "poll every ~1 minute" period, now scoped specifically to the failure path,
// since the successful-fetch cadence is driven by known session boundaries and the
// safety net instead (see interval's new meaning below).
const scheduleRetryInterval = time.Minute

// MonitorMarketOpen reports the market-open state of symbol to onUpdate immediately
// and then on every subsequent session-boundary transition, computed locally from a
// cached Schedule fetch with zero network calls — plus, at most, once more per
// background refresh tick where a boundary is still ahead (harmless: consumers
// store into an atomic/map, so a same-value re-report is a no-op). The cache is
// refreshed in the background, independent of the local walk, whenever the fetched
// window is exhausted or interval's safety-net elapses — whichever comes first. Run
// it in a goroutine; callers decide how to store the result (atomic, mutex-guarded
// map, ...).
//
// interval is no longer a poll period: it is the base of the safety-net range
// [interval, 2*interval) (see safetyNet). See
// docs/superpowers/specs/2026-08-03-event-driven-schedule-poll-design.md for why a
// single fetch's session list stays trustworthy for days, not just minutes — Schedule's
// real response tiles ~3 calendar days ahead with zero gaps (verified empirically), and
// docs/superpowers/specs/2026-08-12-schedule-poll-safety-net-unblock-design.md for why
// the local walk and the background refresh are independent of each other.
func MonitorMarketOpen(ctx context.Context, client *Client, symbol string, interval time.Duration, onUpdate func(open bool)) {
	monitorMarketOpen(ctx, func() ([]session, error) { return getSchedule(client, symbol) }, symbol, interval,
		safetyNet,
		func() time.Duration { return nextPollDelay(scheduleRetryInterval, randJitter(scheduleRetryInterval)) },
		onUpdate)
}

// monitorMarketOpen is the timeline-walk loop with the broker call injected, so the
// failure-retry path and the local-recompute path can both be exercised without a
// live connection.
//
// Two independent events drive the loop, and neither can block the other:
//   - a session-boundary crossing inside the cached window, resolved purely locally
//     from `sessions` via classify — this runs on every tick where nextBoundary
//     still finds something ahead, regardless of whether a background refresh is
//     also due right now or has been failing;
//   - a background refresh attempt, due at nextRefreshAt (the safety-net deadline
//     after a success, a short retryDelay() backoff after a failure). Each attempt
//     is single-shot via attemptFetch: on failure it logs and reschedules, it never
//     loops in place waiting for success — so a Schedule outage can never stall the
//     local walk.
//
// When the cached window has no forward information at all (nextBoundary reports
// ok=false — e.g. a symbol past contract expiry, or Schedule down long enough that
// the ~3-day cache ran dry), the loop keeps retrying quickly but does NOT fold that
// into "closed": an unanswered Schedule call means the broker didn't answer, not
// that the exchange shut. Folding it into "closed" is what silently stopped the
// listener from writing a single row on 2026-07-30 (see
// TestMonitorMarketOpenKeepsLastViewWhenScheduleFails) — this branch keeps
// reporting whatever the last resolved state was until a fetch actually succeeds.
func monitorMarketOpen(ctx context.Context, fetch func() ([]session, error), symbol string, interval time.Duration, safetyNetFn func(time.Duration) time.Duration, retryDelay func() time.Duration, onUpdate func(open bool)) {
	if interval <= 0 {
		// A non-positive interval would flow straight into safetyNetFn, which for
		// safetyNet(interval<=0) returns 0 — pinning the refresh deadline at "now"
		// and refetching in a tight loop forever if a caller ever passes one. Both
		// current callers pass a positive const; this is a defensive floor only.
		interval = scheduleRetryInterval
	}

	var sessions []session

	// attemptFetch makes exactly ONE fetch attempt. On success it adopts the new
	// session list and reports the freshly computed state; on failure it logs and
	// returns false, leaving `sessions` untouched. It never retries internally —
	// callers decide the retry cadence via nextRefreshAt, so a Schedule outage never
	// stalls the caller's ability to keep walking the cached timeline.
	attemptFetch := func() bool {
		fetched, err := fetch()
		if err != nil {
			// Deliberately no onUpdate: an unanswered Schedule call says nothing about
			// the session, so the caller keeps whatever it last knew. Reporting false
			// here is what silently switched the listener's writers off for a full
			// poll interval on every transient RPC failure.
			mlog.Printf("[%s] Error getting schedule (keeping last known state): %v", symbol, err)
			return false
		}
		sessions = fetched
		onUpdate(classify(sessions, time.Now()))
		return true
	}

	// Bootstrap: there is no cached window yet to fall back to, so block-retry on
	// the failure cadence until the first fetch succeeds or ctx is cancelled.
	for !attemptFetch() {
		select {
		case <-time.After(retryDelay()):
		case <-ctx.Done():
			return
		}
	}

	nextRefreshAt := time.Now().Add(safetyNetFn(interval))

	// nextWake picks the earlier of "next known boundary" and "next refresh attempt
	// due". When the window carries no forward information at all (ok=false), it
	// ignores nextRefreshAt entirely and retries on the short cadence instead of
	// possibly sitting on a safety-net deadline that's still far away — see
	// TestMonitorMarketOpenRetriesQuicklyWhenWindowExhausted.
	nextWake := func() time.Duration {
		now := time.Now()
		boundary, ok := nextBoundary(sessions, now)
		if !ok {
			if d := retryDelay(); d > 0 {
				return d
			}
			return time.Millisecond
		}
		wake := nextRefreshAt
		if boundary.Before(wake) {
			wake = boundary
		}
		if !wake.After(now) {
			return time.Millisecond
		}
		return time.Until(wake)
	}

	timer := time.NewTimer(nextWake())
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			now := time.Now()
			_, ok := nextBoundary(sessions, now)
			if ok {
				// The cached window still covers a stretch ahead of now — resolve the
				// current state purely locally, independent of whether a refresh is
				// also due or has been failing.
				onUpdate(classify(sessions, now))
			}
			// A refresh is due either because the window has genuinely run out of
			// forward information (always refresh then) or because the periodic
			// safety-net/backoff deadline arrived.
			if !ok || !now.Before(nextRefreshAt) {
				if attemptFetch() {
					nextRefreshAt = time.Now().Add(safetyNetFn(interval))
				} else {
					nextRefreshAt = time.Now().Add(retryDelay())
				}
			}
			timer.Reset(nextWake())
		case <-ctx.Done():
			return
		}
	}
}

func GetAsset(client *Client, symbol string) (*assets.GetAssetResponse, error) {
	conn, ctx, cancel, err := client.dial(context.Background())
	if err != nil {
		return nil, err
	}
	defer cancel()

	assetsClient := assets.NewAssetsServiceClient(conn)

	assetResp, err := assetsClient.GetAsset(ctx, &assets.GetAssetRequest{
		Symbol:    symbol,
		AccountId: client.GetConfig().AccountID,
	})
	if err != nil {
		return nil, err
	}

	return assetResp, nil
}

func GetOptionsChain(client *Client, symbol string) ([]*assets.Option, error) {
	conn, ctx, cancel, err := client.dial(context.Background())
	if err != nil {
		return nil, err
	}
	defer cancel()

	assetsClient := assets.NewAssetsServiceClient(conn)

	assetResp, err := assetsClient.OptionsChain(ctx, &assets.OptionsChainRequest{
		UnderlyingSymbol: symbol,
	})
	if err != nil {
		return nil, err
	}

	return assetResp.Options, nil
}

// GetAllAssets returns the broker's instrument directory, following the cursor
// pagination of AssetsService.AllAssets until the server stops handing out a next
// cursor. onlyActive drops instruments that are not currently tradeable. The page
// budget is bounded so a server that keeps echoing a cursor cannot spin forever —
// each page costs one call against the 200/min AssetsService.allAssets quota.
func GetAllAssets(ctx context.Context, client *Client, onlyActive bool) ([]*assets.Asset, error) {
	const maxPages = 100

	var (
		out    []*assets.Asset
		cursor int64
	)
	for page := 0; page < maxPages; page++ {
		conn, rctx, cancel, err := client.dial(ctx)
		if err != nil {
			return out, err
		}

		resp, err := assets.NewAssetsServiceClient(conn).AllAssets(rctx, &assets.AllAssetsRequest{
			Cursor:     cursor,
			OnlyActive: onlyActive,
		})
		// Released per iteration rather than deferred: the response is fully materialized
		// by now, and a deferred cancel would pile up one live context per page.
		cancel()
		if err != nil {
			return out, err
		}

		out = append(out, resp.GetAssets()...)

		// A zero next cursor (or an empty page) is the server saying "that was the last one".
		if resp.GetNextCursor() == 0 || len(resp.GetAssets()) == 0 {
			return out, nil
		}
		cursor = resp.GetNextCursor()
	}

	return out, fmt.Errorf("AllAssets: page budget %d exhausted, cursor still %d", maxPages, cursor)
}

func GetExchanges(client *Client) ([]*assets.Exchange, error) {
	conn, ctx, cancel, err := client.dial(context.Background())
	if err != nil {
		return nil, err
	}
	defer cancel()

	assetsClient := assets.NewAssetsServiceClient(conn)

	exchangesResp, err := assetsClient.Exchanges(ctx, &assets.ExchangesRequest{})
	if err != nil {
		return nil, err
	}

	return exchangesResp.Exchanges, nil
}
