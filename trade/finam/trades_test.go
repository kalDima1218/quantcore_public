package finam

import (
	"testing"
	"time"

	v1 "github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1"
	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/accounts"
	"google.golang.org/genproto/googleapis/type/decimal"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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
