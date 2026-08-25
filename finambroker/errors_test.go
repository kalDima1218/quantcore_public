package finambroker

import (
	"errors"
	"fmt"
	"testing"

	v1 "github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1"
	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/orders"
	"google.golang.org/genproto/googleapis/type/decimal"
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
	// An UNRECOGNIZED gRPC code — not in either explicit list above — must default to
	// ambiguous, not definitive: a code this package has never seen carries no evidence the
	// broker evaluated and rejected the order, so guessing "definitive" would let the
	// reject-retry ladder shrink-and-retry (or the taker retry loop mint a new placement) on
	// top of an order that may already rest at the broker. Definitive is the narrow,
	// explicit allowlist above; everything else — known-but-unlisted or genuinely unknown —
	// defaults to ambiguous.
	if !ambiguous(status.Error(codes.Unimplemented, "x")) {
		t.Fatal("an unrecognized gRPC code must default to ambiguous, not definitive")
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

func testOrderState(symbol string, side v1.Side, lots int) *orders.OrderState {
	return &orders.OrderState{
		OrderId: "found1",
		Order:   &orders.Order{Symbol: symbol, Side: side},
		InitialQuantity: &decimal.Decimal{
			Value: fmt.Sprintf("%d", lots),
		},
	}
}

// A client_order_id collision only proves the id matches — matchesIntent must also verify
// symbol/side/lots agree with what THIS call intended to place before it is safe to adopt
// the found order under our id (see placer.find's doc: trust nothing, treat a mismatch
// exactly like "not found").
func TestMatchesIntent(t *testing.T) {
	cases := []struct {
		name     string
		symbol   string
		side     v1.Side
		lots     int
		wantSym  string
		wantBuy  bool
		wantLots int
		want     bool
	}{
		{"exact match buy", "SI", v1.Side_SIDE_BUY, 2, "SI", true, 2, true},
		{"exact match sell", "UF", v1.Side_SIDE_SELL, 3, "UF", false, 3, true},
		{"symbol mismatch", "SI", v1.Side_SIDE_BUY, 2, "UF", true, 2, false},
		{"side mismatch", "SI", v1.Side_SIDE_BUY, 2, "SI", false, 2, false},
		{"lots mismatch", "SI", v1.Side_SIDE_BUY, 2, "SI", true, 5, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			st := testOrderState(c.symbol, c.side, c.lots)
			if got := matchesIntent(st, c.wantSym, c.wantBuy, c.wantLots); got != c.want {
				t.Fatalf("matchesIntent(%q, buy=%v, %d) = %v, want %v", c.wantSym, c.wantBuy, c.wantLots, got, c.want)
			}
		})
	}
}
