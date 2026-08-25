package finam

import (
	"testing"

	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/accounts"
	"google.golang.org/genproto/googleapis/type/decimal"
	"google.golang.org/genproto/googleapis/type/money"
)

func TestFindPosition(t *testing.T) {
	acct := &accounts.GetAccountResponse{
		Positions: []*accounts.Position{
			{
				Symbol:        "SBER@MISX",
				Quantity:      &decimal.Decimal{Value: "-3"},
				AveragePrice:  &decimal.Decimal{Value: "270.5"},
				UnrealizedPnl: &decimal.Decimal{Value: "12.25"},
			},
			{
				Symbol:   "GAZP@MISX",
				Quantity: &decimal.Decimal{Value: "not-a-number"},
			},
		},
	}

	t.Run("held position is parsed", func(t *testing.T) {
		pos, ok := findPosition(acct, "SBER@MISX")
		if !ok {
			t.Fatal("expected position to be found")
		}
		if pos.Quantity != -3 || pos.AveragePrice != 270.5 || pos.UnrealizedPnl != 12.25 {
			t.Fatalf("unexpected parse: %+v", pos)
		}
	})

	t.Run("unparseable decimal yields zero", func(t *testing.T) {
		pos, ok := findPosition(acct, "GAZP@MISX")
		if !ok {
			t.Fatal("expected position to be found")
		}
		if pos.Quantity != 0 {
			t.Fatalf("want 0 for bad decimal, got %v", pos.Quantity)
		}
	})

	t.Run("symbol not held", func(t *testing.T) {
		if _, ok := findPosition(acct, "LKOH@MISX"); ok {
			t.Fatal("did not expect to find an unheld symbol")
		}
	})

	t.Run("nil account", func(t *testing.T) {
		if _, ok := findPosition(nil, "SBER@MISX"); ok {
			t.Fatal("did not expect to find a position in a nil account")
		}
	})
}

func TestAccountMargin(t *testing.T) {
	t.Run("forts portfolio is parsed", func(t *testing.T) {
		acct := &accounts.GetAccountResponse{
			Portfolio: &accounts.GetAccountResponse_PortfolioForts{
				PortfolioForts: &accounts.FORTS{
					AvailableCash: &decimal.Decimal{Value: "150000.5"},
					MoneyReserved: &decimal.Decimal{Value: "42000"},
				},
			},
		}
		m, ok := accountMargin(acct)
		if !ok {
			t.Fatal("expected a FORTS margin snapshot")
		}
		if m.AvailableCash != 150000.5 || m.MoneyReserved != 42000 || m.Source != "forts" {
			t.Fatalf("unexpected parse: %+v", m)
		}
	})

	t.Run("mc portfolio is parsed", func(t *testing.T) {
		acct := &accounts.GetAccountResponse{
			Portfolio: &accounts.GetAccountResponse_PortfolioMc{
				PortfolioMc: &accounts.MC{
					AvailableCash: &decimal.Decimal{Value: "87000.25"},
					InitialMargin: &decimal.Decimal{Value: "31000"},
				},
			},
		}
		m, ok := accountMargin(acct)
		if !ok {
			t.Fatal("expected an MC margin snapshot")
		}
		if m.AvailableCash != 87000.25 || m.MoneyReserved != 31000 || m.Source != "mc" {
			t.Fatalf("unexpected parse: %+v", m)
		}
	})

	t.Run("rub cash fallback sums rub only", func(t *testing.T) {
		acct := &accounts.GetAccountResponse{
			Cash: []*money.Money{
				{CurrencyCode: "RUB", Units: 30000, Nanos: 500_000_000},
				{CurrencyCode: "USD", Units: 999},
				{CurrencyCode: "RUB", Units: 1000},
			},
		}
		m, ok := accountMargin(acct)
		if !ok {
			t.Fatal("expected a cash-based margin snapshot")
		}
		if m.AvailableCash != 31000.5 || m.MoneyReserved != 0 || m.Source != "cash" {
			t.Fatalf("unexpected parse: %+v", m)
		}
	})

	t.Run("mct portfolio carries no numbers, no cash", func(t *testing.T) {
		acct := &accounts.GetAccountResponse{
			Portfolio: &accounts.GetAccountResponse_PortfolioMct{PortfolioMct: &accounts.MCT{}},
		}
		if _, ok := accountMargin(acct); ok {
			t.Fatal("did not expect margin from an MCT portfolio with no cash")
		}
	})

	t.Run("nothing usable", func(t *testing.T) {
		if _, ok := accountMargin(&accounts.GetAccountResponse{}); ok {
			t.Fatal("did not expect margin from an empty account")
		}
		if _, ok := accountMargin(nil); ok {
			t.Fatal("did not expect margin from a nil account")
		}
	})
}

func TestPortfolioKind(t *testing.T) {
	for _, tc := range []struct {
		want string
		acct *accounts.GetAccountResponse
	}{
		{"FORTS", &accounts.GetAccountResponse{Portfolio: &accounts.GetAccountResponse_PortfolioForts{PortfolioForts: &accounts.FORTS{}}}},
		{"MC", &accounts.GetAccountResponse{Portfolio: &accounts.GetAccountResponse_PortfolioMc{PortfolioMc: &accounts.MC{}}}},
		{"MCT", &accounts.GetAccountResponse{Portfolio: &accounts.GetAccountResponse_PortfolioMct{PortfolioMct: &accounts.MCT{}}}},
		{"none", &accounts.GetAccountResponse{}},
		{"none", nil},
	} {
		if got := PortfolioKind(tc.acct); got != tc.want {
			t.Fatalf("PortfolioKind want %q, got %q", tc.want, got)
		}
	}
}
