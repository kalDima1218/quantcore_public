package hedges_test

import (
	"errors"
	"testing"
	"time"

	"QuantCore/strategies/execengine2"
	"QuantCore/strategies/execengine2/internal/hedges"
)

func hedgeRequest(lots int) execengine2.OrderRequest {
	return execengine2.OrderRequest{
		Symbol: "B", Side: execengine2.SideSell, Kind: execengine2.OrderMarket,
		Role: execengine2.RoleHedge, Leg: execengine2.LegB, Lots: lots, TradeID: 7,
	}
}

func TestMarketCheckFindsMissingLots(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 26, 10, 0, 0, 0, time.UTC)
	var m hedges.List
	if err := m.AddMarket("t1", hedgeRequest(5), now); err != nil {
		t.Fatal(err)
	}
	if got := m.Checks(now.Add(time.Second), 2*time.Second, time.Second); len(got) != 0 {
		t.Fatalf("early probes = %v", got)
	}
	probes := m.Checks(now.Add(2*time.Second), 2*time.Second, time.Second)
	if len(probes) != 1 || probes[0].OrderID != "t1" {
		t.Fatalf("probes = %+v", probes)
	}
	result := m.SetStatus("t1", execengine2.OrderStatus{Filled: 3, Done: true})
	if !result.Known || result.Done || result.Missing.Lots != 2 ||
		result.Missing.Role != execengine2.RoleFix {
		t.Fatalf("status result = %+v", result)
	}
}

func TestAllReturnsCopy(t *testing.T) {
	t.Parallel()
	var m hedges.List
	id := m.Add(hedgeRequest(2), errors.New("transport"))
	debts := m.All()
	if len(debts) != 1 || debts[0].ID != id || debts[0].Tries != 1 {
		t.Fatalf("debts = %+v", debts)
	}
	debts[0].Request.Lots = 99
	if got := m.All()[0].Request.Lots; got != 2 {
		t.Fatalf("caller mutated debt lots to %d", got)
	}
	m.Fail(id, errors.New("again"))
	if got := m.All()[0].Tries; got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
	if !m.Done(id) || m.HasWork() {
		t.Fatal("resolved debt remained outstanding")
	}
}

func TestFillEndsMarketCheck(t *testing.T) {
	t.Parallel()
	var m hedges.List
	if err := m.AddMarket("t1", hedgeRequest(2), time.Time{}); err != nil {
		t.Fatal(err)
	}
	if m.SeeFill("t1", 1) {
		t.Fatal("partial report confirmed taker")
	}
	if !m.SeeFill("t1", 2) || m.MarketCount() != 0 {
		t.Fatal("full report did not confirm taker")
	}
}
