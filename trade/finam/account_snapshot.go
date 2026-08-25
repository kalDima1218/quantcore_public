package finam

import (
	"context"
	"strconv"

	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/accounts"
	"google.golang.org/genproto/googleapis/type/decimal"
)

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
