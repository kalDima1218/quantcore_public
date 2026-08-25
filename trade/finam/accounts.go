package finam

import (
	"context"
	"strconv"
	"time"

	v1 "github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1"
	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/accounts"
	"google.golang.org/genproto/googleapis/type/decimal"
	"google.golang.org/genproto/googleapis/type/interval"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const accountStreamBufferSize = 64

func GetAccount(client *Client) (*accounts.GetAccountResponse, error) {
	conn, ctx, cancel, err := client.dial(context.Background())
	if err != nil {
		return nil, err
	}
	defer cancel()

	accountsClient := accounts.NewAccountsServiceClient(conn)

	account, err := accountsClient.GetAccount(ctx, &accounts.GetAccountRequest{
		AccountId: client.GetConfig().AccountID,
	})
	if err != nil {
		return nil, err
	}

	return account, nil
}

// Position is the account's holding in a single symbol, with the decimal fields
// parsed to float64 for direct use by trading logic.
type Position struct {
	Symbol        string
	Quantity      float64
	AveragePrice  float64
	UnrealizedPnl float64
}

// GetPosition returns the current position in symbol for the configured account.
// The second result is false when the account holds no position in that symbol.
func GetPosition(client *Client, symbol string) (Position, bool, error) {
	account, err := GetAccount(client)
	if err != nil {
		return Position{}, false, err
	}
	pos, ok := findPosition(account, symbol)
	return pos, ok, nil
}

// findPosition extracts symbol's position from an account snapshot, parsing the
// decimal fields to float64. It is the pure core of GetPosition, split out so the
// parsing is testable without a live client.
func findPosition(account *accounts.GetAccountResponse, symbol string) (Position, bool) {
	if account == nil {
		return Position{}, false
	}
	for _, p := range account.Positions {
		if p.Symbol != symbol {
			continue
		}
		return Position{
			Symbol:        p.Symbol,
			Quantity:      decimalToFloat(p.Quantity),
			AveragePrice:  decimalToFloat(p.AveragePrice),
			UnrealizedPnl: decimalToFloat(p.UnrealizedPnl),
		}, true
	}

	return Position{}, false
}

// Margin is the account's margin snapshot, in ₽: the own funds still available for
// trading and the margin currently reserved under the open positions. Which response
// fields carry those numbers depends on the account's portfolio type (see accountMargin);
// Source names the one that was actually used, for logging.
type Margin struct {
	AvailableCash float64 // own funds available for trading (includes margin funds)
	MoneyReserved float64 // margin reserved under open positions (0 when the source carries no such figure)
	Source        string  // where the numbers came from: "forts", "mc" or "cash"
}

// GetMargin returns the configured account's margin numbers. The second result is false
// when the account response carries no known portfolio and no RUB cash to read them from.
func GetMargin(client *Client) (Margin, bool, error) {
	account, err := GetAccount(client)
	if err != nil {
		return Margin{}, false, err
	}
	m, ok := accountMargin(account)
	return m, ok, nil
}

// accountMargin extracts the margin numbers from an account snapshot, parsing the decimals
// to float64. The portfolio is a oneof whose variant depends on the account type, so it
// tries them in order of fidelity: FORTS (available_cash + money_reserved), then MC
// (available_cash + initial_margin — the unified-account shape futures land in), then the
// top-level RUB cash as a last resort (own funds NOT including margin funds, so it reads
// conservatively low — fine for a free-cash floor; reserved is unknown there and reads 0).
// It is the pure core of GetMargin, split out so the parsing is testable without a live
// client.
func accountMargin(account *accounts.GetAccountResponse) (Margin, bool) {
	if f := account.GetPortfolioForts(); f != nil {
		return Margin{
			AvailableCash: decimalToFloat(f.AvailableCash),
			MoneyReserved: decimalToFloat(f.MoneyReserved),
			Source:        "forts",
		}, true
	}
	if mc := account.GetPortfolioMc(); mc != nil {
		return Margin{
			AvailableCash: decimalToFloat(mc.AvailableCash),
			MoneyReserved: decimalToFloat(mc.InitialMargin),
			Source:        "mc",
		}, true
	}
	total, any := 0.0, false
	for _, c := range account.GetCash() {
		if c.GetCurrencyCode() != "RUB" {
			continue
		}
		total += float64(c.GetUnits()) + float64(c.GetNanos())/1e9
		any = true
	}
	if any {
		return Margin{AvailableCash: total, Source: "cash"}, true
	}
	return Margin{}, false
}

// PortfolioKind names the portfolio variant an account snapshot carries ("MC", "MCT",
// "FORTS" or "none") — diagnostics for when accountMargin finds nothing usable.
func PortfolioKind(account *accounts.GetAccountResponse) string {
	switch account.GetPortfolio().(type) {
	case *accounts.GetAccountResponse_PortfolioForts:
		return "FORTS"
	case *accounts.GetAccountResponse_PortfolioMc:
		return "MC"
	case *accounts.GetAccountResponse_PortfolioMct:
		return "MCT"
	default:
		return "none"
	}
}

// SubscribeAccount streams realtime account snapshots (equity, positions, cash)
// for the configured account, reconnecting on any stream error. NOTE: the streamed
// positions include THEORETICAL positions implied by resting active orders, not
// only filled inventory — for exact fill-based inventory use SubscribeTrades.
func SubscribeAccount(client *Client) (<-chan *accounts.GetAccountResponse, error) {
	out := make(chan *accounts.GetAccountResponse, accountStreamBufferSize)
	go func() {
		defer close(out)
		reconnectLoop("account", func() error { return runAccountStream(client, out) })
	}()
	return out, nil
}

func runAccountStream(client *Client, out chan<- *accounts.GetAccountResponse) error {
	conn, err := client.GetConn(context.Background())
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "Authorization", client.GetJWT())

	accountsClient := accounts.NewAccountsServiceClient(conn)
	stream, err := accountsClient.SubscribeAccount(ctx, &accounts.GetAccountRequest{
		AccountId: client.GetConfig().AccountID,
	})
	if err != nil {
		return err
	}

	for {
		resp, err := stream.Recv()
		if err != nil {
			return err
		}

		select {
		case out <- resp:
		default:
			mlog.Printf("[account] Warning: channel full, dropping update")
		}
	}
}

// decimalToFloat parses a Finam decimal into a float64, yielding 0 for a nil or
// unparseable value.
func decimalToFloat(d *decimal.Decimal) float64 {
	if d == nil {
		return 0
	}
	v, err := strconv.ParseFloat(d.Value, 64)
	if err != nil {
		return 0
	}
	return v
}

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
