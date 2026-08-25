package finambroker

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ambiguous reports whether a placement/cancel error from the Finam gRPC API leaves the
// order's fate UNKNOWN: the request may have reached the broker even though the response
// never made it back. The gRPC-code vocabulary this decision is made on lives ENTIRELY in
// this file — execengine.MaybeDelivered is broker-neutral and knows nothing about it (see
// strategies/execengine/broker_errors.go); placeResolved marks a definitive result via
// execengine.NewDefinitiveReject before it ever crosses that boundary.
//
// Definitive is the NARROW, explicit list below: a genuine business rejection the broker
// evaluated and answered. Every other code — known-but-not-listed, or one this package has
// never seen — defaults to ambiguous. An unrecognized code carries no evidence the broker
// actually evaluated and rejected the order, so treating it as definitive would let the
// reject-retry ladder shrink-and-retry (or, before placeResolved's own same-cid retry,
// the taker retry loop mint a fresh placement) on top of an order that may already rest at
// the broker — guessing wrong here risks doubling a position; guessing wrong the other way
// only costs an extra resolution probe.
func ambiguous(err error) bool {
	switch status.Code(err) {
	case codes.InvalidArgument, codes.FailedPrecondition, codes.PermissionDenied,
		codes.NotFound, codes.ResourceExhausted, codes.Unauthenticated, codes.OutOfRange:
		return false // the broker evaluated the order and answered: rejected, nothing rests
	}
	// Includes, among others: Unavailable, DeadlineExceeded, Canceled, Unknown, Internal,
	// DataLoss, Aborted, Unimplemented, and AlreadyExists (the client_order_id collided —
	// it does NOT prove the broker rejected OUR order; the attempt that first minted this
	// id may have succeeded).
	return true
}
