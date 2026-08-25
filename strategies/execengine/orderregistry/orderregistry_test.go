package orderregistry

import "testing"

func TestConfirmedByTerminalCount(t *testing.T) {
	a := &OrdAcct{Final: 0}
	if !a.Confirmed() {
		t.Fatal("a non-negative Final (settled, even at 0) must be confirmed")
	}
}

func TestConfirmedByFillStreamCoverage(t *testing.T) {
	a := &OrdAcct{Final: -1, Placed: 3, Seen: 3}
	if !a.Confirmed() {
		t.Fatal("Seen >= Placed must be confirmed even without a terminal Final")
	}
}

func TestNotConfirmedWhilePending(t *testing.T) {
	a := &OrdAcct{Final: -1, Placed: 3, Seen: 1}
	if a.Confirmed() {
		t.Fatal("Final<0 and Seen<Placed must not be confirmed")
	}
}

func TestLedgerIsAPlainMap(t *testing.T) {
	l := Ledger{}
	l["ord1"] = &OrdAcct{Placed: 2}
	if got, ok := l["ord1"]; !ok || got.Placed != 2 {
		t.Fatalf("Ledger must support plain map indexing, got %+v ok=%v", got, ok)
	}
	delete(l, "ord1")
	if _, ok := l["ord1"]; ok {
		t.Fatal("delete must work like an ordinary map")
	}
}
