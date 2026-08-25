package execengine

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MaybeDelivered reports whether a placement/cancel error leaves the broker's action on
// an order UNKNOWN: the request may have reached the broker even though the response
// never made it back (transport-level failure). A definitive business rejection (invalid
// args, permission, insufficient funds — the broker answered and nothing happened) is
// NOT ambiguous. This is the single classifier both the reject-retry ladder
// (engine_clip.go/engine_hedge.go: shrink-and-retry only on a DEFINITIVE reject) and the
// broker adapter's ghost-order resolution (which broker package wires it in) key off —
// the two must never disagree about what "ambiguous" means for the same error.
func MaybeDelivered(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled,
		codes.Unknown, codes.Internal, codes.DataLoss, codes.Aborted:
		return true
	}
	return false
}
