package execengine

import "time"

// Clock reads the current time for quota bookkeeping against the rate limiter
// (Allow/Spend). A broker's placeOrder quota window is a real-time concept — it resets on
// the broker's own clock regardless of whether market data is flowing — so it must never
// be measured against e.now (the engine's data-driven, event-sourced clock; see engine.go).
// A stalled feed or a blocked placement RPC freezes e.now but must NOT freeze quota
// accounting along with it.
type Clock interface {
	Now() time.Time
}

// realClock is the production Clock: an ordinary wall-clock read.
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }
