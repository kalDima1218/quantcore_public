// quote groups the engine's continuously-updated book state: the latest touch (bid/ask/ts)
// per leg, independent of whether a clip is open, and the rate-limit/failure backoff that
// suppresses opening a NEW clip. Like recovery/ledger/hedge (see their files), this is a
// state grouping, not a black-box component: canQuote and openClip's price calc read
// legA/legB to size and price a clip, so those transitions stay on Engine
// (engine_clip.go, engine_quote.go) and access e.quote.* directly.
package execengine

import "time"

type quote struct {
	legA, legB   touch
	backoffUntil time.Time // suppress opening new clips until this time (rate-limit / failure backoff)
}
