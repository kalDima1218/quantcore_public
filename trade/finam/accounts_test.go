package finam

import (
	"testing"
	"time"

	v1 "github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1"
	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/accounts"
	"google.golang.org/genproto/googleapis/type/decimal"
	"google.golang.org/genproto/googleapis/type/money"
	"google.golang.org/protobuf/types/known/timestamppb"
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

func TestParseTrades(t *testing.T) {
	// Только этот метод отдаёт биржевую метку времени сделки: логи бота пишут время
	// логгера, и с ним сверка «цена филла против книги в тот момент» невалидна.
	resp := &accounts.TradesResponse{Trades: []*v1.AccountTrade{
		{
			TradeId: "t1", OrderId: "o1", Symbol: "LEGA@RTSX",
			Price: &decimal.Decimal{Value: "78835"}, Size: &decimal.Decimal{Value: "4"},
			Side:      v1.Side_SIDE_BUY,
			Timestamp: timestamppb.New(time.Date(2026, 7, 27, 4, 40, 50, 228159000, time.UTC)),
		},
		{
			TradeId: "t2", OrderId: "o2", Symbol: "LEGB@RTSX",
			Price: &decimal.Decimal{Value: "77.91"}, Size: &decimal.Decimal{Value: "3"},
			Side:      v1.Side_SIDE_SELL,
			Timestamp: timestamppb.New(time.Date(2026, 7, 27, 4, 40, 50, 314357000, time.UTC)),
		},
		{TradeId: "t3", Symbol: "LEGA@RTSX"}, // без цены, размера и метки
	}}

	got := parseTrades(resp)
	if len(got) != 3 {
		t.Fatalf("trade count = %d, want 3", len(got))
	}
	if !got[0].Buy || got[0].Price != 78835 || got[0].Size != 4 || got[0].OrderID != "o1" {
		t.Fatalf("buy trade parsed wrong: %+v", got[0])
	}
	if want := time.Date(2026, 7, 27, 4, 40, 50, 228159000, time.UTC); !got[0].Time.Equal(want) {
		t.Fatalf("exchange timestamp = %v, want %v", got[0].Time, want)
	}
	if got[1].Buy || got[1].Price != 77.91 || got[1].Symbol != "LEGB@RTSX" {
		t.Fatalf("sell trade parsed wrong: %+v", got[1])
	}
	if got[2].Price != 0 || !got[2].Time.IsZero() {
		t.Fatalf("empty trade should degrade to zeros, got %+v", got[2])
	}
	if parseTrades(nil) != nil {
		t.Fatal("nil response should yield no trades")
	}
}

func TestParseTransactions(t *testing.T) {
	// Фондирование вечного фьючерса приходит отдельной транзакцией — по логам сделок его
	// не увидеть, а именно оно решает, во сколько обходится перенос позиции через ночь.
	resp := &accounts.TransactionsResponse{Transactions: []*accounts.Transaction{
		{
			Id: "tx1", Symbol: "LEGB@RTSX", TransactionName: "Фондирование",
			Change:    &money.Money{CurrencyCode: "RUB", Units: -1234, Nanos: -500000000},
			Timestamp: timestamppb.New(time.Date(2026, 7, 16, 21, 5, 0, 0, time.UTC)),
		},
		{
			Id: "tx2", Symbol: "LEGA@RTSX", TransactionName: "Вариационная маржа",
			Change:    &money.Money{CurrencyCode: "RUB", Units: 2000},
			Timestamp: timestamppb.New(time.Date(2026, 7, 16, 21, 5, 0, 0, time.UTC)),
		},
		{Id: "tx3"}, // без денег и метки
	}}

	got := parseTransactions(resp)
	if len(got) != 3 {
		t.Fatalf("transaction count = %d, want 3", len(got))
	}
	if got[0].Change != -1234.5 || got[0].Symbol != "LEGB@RTSX" || got[0].Name != "Фондирование" {
		t.Fatalf("funding row parsed wrong: %+v", got[0])
	}
	if want := time.Date(2026, 7, 16, 21, 5, 0, 0, time.UTC); !got[0].Time.Equal(want) {
		t.Fatalf("timestamp = %v, want %v", got[0].Time, want)
	}
	if got[1].Change != 2000 {
		t.Fatalf("whole-unit money parsed wrong: %+v", got[1])
	}
	if got[2].Change != 0 || !got[2].Time.IsZero() {
		t.Fatalf("empty transaction should degrade to zeros: %+v", got[2])
	}
	if parseTransactions(nil) != nil {
		t.Fatal("nil response should yield no transactions")
	}
}
