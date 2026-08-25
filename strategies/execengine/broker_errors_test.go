package execengine

import (
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The transport-error classifier: only errors that leave the order's fate unknown may
// trigger the reject-retry ladder / ghost-order resolution; broker verdicts must not.
func TestMaybeDeliveredClassification(t *testing.T) {
	for _, c := range []codes.Code{codes.Unavailable, codes.DeadlineExceeded, codes.Canceled, codes.Unknown, codes.Internal, codes.DataLoss, codes.Aborted} {
		if !MaybeDelivered(status.Error(c, "x")) {
			t.Fatalf("%v must count as possibly delivered", c)
		}
	}
	for _, c := range []codes.Code{codes.InvalidArgument, codes.FailedPrecondition, codes.PermissionDenied, codes.NotFound, codes.ResourceExhausted, codes.Unauthenticated, codes.AlreadyExists, codes.OutOfRange} {
		if MaybeDelivered(status.Error(c, "x")) {
			t.Fatalf("%v is a definitive broker answer — no resolution", c)
		}
	}
	// Plain (non-gRPC) errors map to codes.Unknown: fate unknown → resolve.
	if !MaybeDelivered(errors.New("dial tcp: broken pipe")) {
		t.Fatal("a non-gRPC transport error leaves the fate unknown")
	}
}
