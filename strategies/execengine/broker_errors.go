package execengine

import "errors"

// placementOutcome distinguishes the two ways a broker adapter can mark an error
// DEFINITIVE. Both classify identically for MaybeDelivered today — the split exists so a
// future, genuinely authoritative non-placement source (see NewConfirmedNotPlaced) is
// distinguishable from an ordinary business rejection, without execengine needing to know
// what either adapter's API actually looked like.
type placementOutcome int

const (
	outcomeDefinitiveReject placementOutcome = iota
	outcomeConfirmedNotPlaced
)

// outcomeMarker tags a placement/cancel error with a broker-neutral classification. Unwrap
// preserves errors.Is/errors.As through it, so a caller can still inspect the original
// broker error alongside the marker.
type outcomeMarker struct {
	err     error
	outcome placementOutcome
}

func (m *outcomeMarker) Error() string { return m.err.Error() }
func (m *outcomeMarker) Unwrap() error { return m.err }

// NewDefinitiveReject wraps err to mark it DEFINITIVE: the broker answered and nothing
// happened — safe for the reject-retry ladder to shrink-and-retry on. A broker adapter
// calls this for a genuine business rejection (invalid args, permission, insufficient
// funds) — never for a transport failure, and never for an inconclusive resolution (see
// NewConfirmedNotPlaced).
func NewDefinitiveReject(err error) error {
	if err == nil {
		return nil
	}
	return &outcomeMarker{err: err, outcome: outcomeDefinitiveReject}
}

// NewConfirmedNotPlaced wraps err to mark it DEFINITIVE via a genuinely authoritative
// non-placement source — stronger than "absent from a broker's list of currently active
// orders", which proves nothing about an order that already reached a terminal state (see
// finambroker's placeResolved, which does NOT call this: Finam's GetOrders excludes
// terminal orders, so absence there stays ambiguous). Reserved for a future adapter/API
// that can actually prove non-placement.
func NewConfirmedNotPlaced(err error) error {
	if err == nil {
		return nil
	}
	return &outcomeMarker{err: err, outcome: outcomeConfirmedNotPlaced}
}

// MaybeDelivered reports whether a placement/cancel error leaves the broker's action on an
// order UNKNOWN: the request may have reached the broker even though nothing said so
// definitively. Broker-neutral by construction — it knows nothing about gRPC, HTTP, or any
// other transport; a broker adapter marks its OWN errors via NewDefinitiveReject /
// NewConfirmedNotPlaced (whatever transport-specific vocabulary that classification needs
// lives entirely in the adapter package), and anything unmarked defaults to ambiguous
// (never assume success, never assume failure — including a nil error, which callers must
// treat as delivered before ever reaching this classifier). This is the single classifier
// both the reject-retry ladder (engine_clip.go/engine_hedge.go: shrink-and-retry only on a
// DEFINITIVE reject) and a broker adapter's own resolution logic key off — they must never
// disagree about what "ambiguous" means for the same error.
func MaybeDelivered(err error) bool {
	if err == nil {
		return false
	}
	var m *outcomeMarker
	if errors.As(err, &m) {
		return false // both outcomes are definitive
	}
	return true // unmarked: unknown shape — default conservative
}
