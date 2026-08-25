package finam

import (
	"context"
	"time"

	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/accounts"
	"google.golang.org/genproto/googleapis/type/interval"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Transaction is one money movement on the account: funding on a perpetual, variation margin,
// commission, transfer. The funding rows are the point — a perpetual leg carried overnight is
// charged (or paid) a swap rate that no trade log shows and the backtest does not model at all,
// so the true cost of holding through the night can only be read from here.
type Transaction struct {
	ID     string
	Symbol string
	Name   string    // broker's own label, e.g. "Фондирование"
	Time   time.Time // UTC
	Change float64   // ₽ (signed: negative = charged)
}

// GetTransactions returns the account's money movements in [from, to], limit capping the count
// (0 leaves the broker's default).
func GetTransactions(client *Client, from, to time.Time, limit int32) ([]Transaction, error) {
	conn, ctx, cancel, err := client.dial(context.Background())
	if err != nil {
		return nil, err
	}
	defer cancel()

	accountsClient := accounts.NewAccountsServiceClient(conn)

	resp, err := accountsClient.Transactions(ctx, &accounts.TransactionsRequest{
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
	return parseTransactions(resp), nil
}

// parseTransactions flattens the broker's money type (units + nanos) into ₽, degrading a
// missing field to zero rather than dropping the row.
func parseTransactions(resp *accounts.TransactionsResponse) []Transaction {
	if resp == nil {
		return nil
	}
	out := make([]Transaction, 0, len(resp.Transactions))
	for _, t := range resp.Transactions {
		if t == nil {
			continue
		}
		tr := Transaction{ID: t.Id, Symbol: t.Symbol, Name: t.TransactionName}
		if t.Change != nil {
			tr.Change = float64(t.Change.Units) + float64(t.Change.Nanos)/1e9
		}
		if t.Timestamp != nil {
			tr.Time = t.Timestamp.AsTime().UTC()
		}
		out = append(out, tr)
	}
	return out
}
