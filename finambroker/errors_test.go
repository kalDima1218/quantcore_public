package finambroker

import (
	"errors"
	"testing"

	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/orders"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The gRPC-code classifier: only errors that leave the order's fate unknown may trigger
// ghost-order resolution / the reject-retry ladder; a genuine broker verdict must not.
func TestAmbiguousClassification(t *testing.T) {
	for _, c := range []codes.Code{codes.Unavailable, codes.DeadlineExceeded, codes.Canceled, codes.Unknown, codes.Internal, codes.DataLoss, codes.Aborted} {
		if !ambiguous(status.Error(c, "x")) {
			t.Fatalf("%v must count as possibly delivered", c)
		}
	}
	for _, c := range []codes.Code{codes.InvalidArgument, codes.FailedPrecondition, codes.PermissionDenied, codes.NotFound, codes.ResourceExhausted, codes.Unauthenticated, codes.OutOfRange} {
		if ambiguous(status.Error(c, "x")) {
			t.Fatalf("%v is a definitive broker answer — no resolution", c)
		}
	}
	// Plain (non-gRPC) errors map to codes.Unknown: fate unknown → resolve.
	if !ambiguous(errors.New("dial tcp: broken pipe")) {
		t.Fatal("a non-gRPC transport error leaves the fate unknown")
	}
}

// AlreadyExists means the client_order_id collided — it does NOT prove the broker rejected
// OUR order; the attempt that first minted this id may have succeeded. Classifying it as
// definitive would let the reject-retry ladder shrink-and-retry on top of an order that may
// already exist, doubling the position.
func TestAmbiguousClassifiesAlreadyExistsAsAmbiguousNotDefinitive(t *testing.T) {
	if !ambiguous(status.Error(codes.AlreadyExists, "duplicate client_order_id")) {
		t.Fatal("AlreadyExists must be ambiguous — the original attempt owning this id may have succeeded")
	}
}

// End-to-end: placeResolved must resolve (probe the order list) on AlreadyExists rather
// than immediately reporting a definitive rejection.
func TestPlaceResolvedResolvesOnAlreadyExists(t *testing.T) {
	p := testPlacer(
		func(string) (*orders.OrderState, error) {
			return nil, status.Error(codes.AlreadyExists, "duplicate client_order_id")
		},
		func(cid string) (string, bool, error) { return "existing1", true, nil },
	)
	id, err := p.placeResolved(kindLimitBid, "SI", 2, 100)
	if err != nil || id != "existing1" {
		t.Fatalf("AlreadyExists must resolve via the order list, got id=%q err=%v", id, err)
	}
}
