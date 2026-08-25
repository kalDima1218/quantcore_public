package execengine

import (
	"errors"
	"testing"
)

// MaybeDelivered is broker-neutral: it classifies by MARKER, never by transport-specific
// error shape (see finambroker's own gRPC-code classifier for that). Only errors that leave
// the order's fate unknown may trigger the reject-retry ladder / ghost-order resolution.
func TestMaybeDeliveredClassification(t *testing.T) {
	if MaybeDelivered(nil) {
		t.Fatal("nil (success) must never count as possibly delivered")
	}

	unmarked := errors.New("dial tcp: broken pipe")
	if !MaybeDelivered(unmarked) {
		t.Fatal("an unmarked error must default to ambiguous — never assume success or failure")
	}

	reject := NewDefinitiveReject(errors.New("insufficient funds"))
	if MaybeDelivered(reject) {
		t.Fatal("a definitive reject must not count as possibly delivered")
	}

	confirmed := NewConfirmedNotPlaced(errors.New("authoritatively absent"))
	if MaybeDelivered(confirmed) {
		t.Fatal("a confirmed-not-placed outcome must not count as possibly delivered")
	}
}

// Both constructors must preserve the original error through Unwrap, so a caller can still
// inspect the underlying broker error (e.g. for logging or errors.Is against a sentinel)
// alongside the classification.
func TestOutcomeMarkersUnwrapToOriginalError(t *testing.T) {
	original := errors.New("insufficient funds")
	wrapped := NewDefinitiveReject(original)
	if !errors.Is(wrapped, original) {
		t.Fatal("NewDefinitiveReject must preserve the original error through Unwrap")
	}

	original2 := errors.New("absent")
	wrapped2 := NewConfirmedNotPlaced(original2)
	if !errors.Is(wrapped2, original2) {
		t.Fatal("NewConfirmedNotPlaced must preserve the original error through Unwrap")
	}
}

// Wrapping nil must stay nil — a broker adapter that mistakenly wraps a successful (nil)
// result must not manufacture a non-nil error out of it.
func TestOutcomeMarkersNilInNilOut(t *testing.T) {
	if err := NewDefinitiveReject(nil); err != nil {
		t.Fatalf("NewDefinitiveReject(nil) = %v, want nil", err)
	}
	if err := NewConfirmedNotPlaced(nil); err != nil {
		t.Fatalf("NewConfirmedNotPlaced(nil) = %v, want nil", err)
	}
}
