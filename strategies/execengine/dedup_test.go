package execengine

import "testing"

// The natural Go zero value — `var d TradeDedup`, not the composite literal every current
// call site happens to use — must be immediately usable. A bare defined-map-type zero
// value is nil and panics on the first write; TradeDedup must not have that footgun.
func TestTradeDedupZeroValueIsUsable(t *testing.T) {
	var d TradeDedup
	if d.Seen("t1") {
		t.Fatal("an unseen id on a zero-value TradeDedup must report false")
	}
	if !d.Seen("t1") {
		t.Fatal("a repeated id must report true (already seen)")
	}
}

// The composite-literal form every current call site uses must keep working unchanged.
func TestTradeDedupCompositeLiteralSeen(t *testing.T) {
	d := TradeDedup{}
	if d.Seen("t1") {
		t.Fatal("an unseen id must report false")
	}
	if !d.Seen("t1") {
		t.Fatal("a repeated id must report true")
	}
	if d.Seen("") {
		t.Fatal("an empty id must never be treated as a duplicate")
	}
}
