package finam

import (
	"context"
	"time"

	v1 "github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1"
	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/accounts"
	"google.golang.org/genproto/googleapis/type/interval"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Trade is one executed fill on the account, carrying the EXCHANGE's own timestamp.
// That timestamp is the point: the bot's trades.log stamps fills with logger time,
// whose lag behind the exchange runs to seconds, so any "fill price against the book
// at that instant" measurement — slip, markout, adverse selection — has to be built
// on trades pulled from here, not on the log.
type Trade struct {
	TradeID string
	OrderID string
	Symbol  string
	Time    time.Time // exchange timestamp, UTC
	Price   float64
	Size    float64
	Buy     bool
}

// GetTrades returns the account's own executed trades in [from, to]. limit caps the
// number returned (0 leaves the broker's own default).
func GetTrades(client *Client, from, to time.Time, limit int32) ([]Trade, error) {
	conn, ctx, cancel, err := client.dial(context.Background())
	if err != nil {
		return nil, err
	}
	defer cancel()

	accountsClient := accounts.NewAccountsServiceClient(conn)

	resp, err := accountsClient.Trades(ctx, &accounts.TradesRequest{
		AccountId: client.GetConfig().AccountID,
		Limit:     limit,
		Interval: &interval.Interval{
			StartTime: timestamppb.New(from),
			EndTime:   timestamppb.New(to),
		},
	})
	if err != nil {
		return nil, err
	}
	return parseTrades(resp), nil
}

// parseTrades converts the broker's decimal/timestamp wire types into plain Go values,
// degrading a missing field to its zero rather than dropping the trade.
func parseTrades(resp *accounts.TradesResponse) []Trade {
	if resp == nil {
		return nil
	}
	out := make([]Trade, 0, len(resp.Trades))
	for _, t := range resp.Trades {
		if t == nil {
			continue
		}
		tr := Trade{
			TradeID: t.TradeId,
			OrderID: t.OrderId,
			Symbol:  t.Symbol,
			Price:   decimalToFloat(t.Price),
			Size:    decimalToFloat(t.Size),
			Buy:     t.Side == v1.Side_SIDE_BUY,
		}
		if t.Timestamp != nil {
			tr.Time = t.Timestamp.AsTime().UTC()
		}
		out = append(out, tr)
	}
	return out
}
