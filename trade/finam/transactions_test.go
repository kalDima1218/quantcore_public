package finam

import (
	"testing"
	"time"

	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/accounts"
	"google.golang.org/genproto/googleapis/type/money"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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
