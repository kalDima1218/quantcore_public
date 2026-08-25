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
func ambiguous(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled,
		codes.Unknown, codes.Internal, codes.DataLoss, codes.Aborted,
		// AlreadyExists means the client_order_id collided — it does NOT prove the broker
		// rejected our order; the attempt that first minted this id may have succeeded.
		// Treating it as definitive would let the reject-retry ladder shrink-and-retry on
		// top of an order that may already exist.
		codes.AlreadyExists:
		return true
	}
	return false
}
