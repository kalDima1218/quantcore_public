package finam

import (
	"context"
	"fmt"
	"math"
	"strconv"

	v1 "github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1"
	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/orders"
	"google.golang.org/genproto/googleapis/type/decimal"
)

// ParseDecimal parses the string payload of an API decimal value (sizes, prices,
// executed quantities), returning 0 for empty or malformed input. Shared by every
// consumer of the API's decimal fields.
func ParseDecimal(s string) float64 {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return v
}

// ExecutedLots returns an order state's executed quantity rounded to whole lots
// (0 when the field is absent).
func ExecutedLots(st *orders.OrderState) int {
	return int(math.Round(ParseDecimal(st.GetExecutedQuantity().GetValue())))
}

func PlaceOrder(client *Client, order *orders.Order) (*orders.OrderState, error) {
	conn, ctx, cancel, err := client.dial(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to gRPC: %w", err)
	}
	defer cancel()

	ordersClient := orders.NewOrdersServiceClient(conn)

	orderState, err := ordersClient.PlaceOrder(ctx, order)
	if err != nil {
		return nil, fmt.Errorf("failed to place order: %w", err)
	}

	mlog.Printf("Order placed successfully: OrderID=%s, Status=%s", orderState.OrderId, orderState.Status.String())

	return orderState, nil
}

// CancelOrder cancels the order identified by orderID and returns its resulting state.
func CancelOrder(client *Client, orderID string) (*orders.OrderState, error) {
	conn, ctx, cancel, err := client.dial(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to gRPC: %w", err)
	}
	defer cancel()

	ordersClient := orders.NewOrdersServiceClient(conn)

	orderState, err := ordersClient.CancelOrder(ctx, &orders.CancelOrderRequest{
		AccountId: client.GetConfig().AccountID,
		OrderId:   orderID,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to cancel order %s: %w", orderID, err)
	}

	mlog.Printf("Order canceled: OrderID=%s, Status=%s", orderState.OrderId, orderState.Status.String())

	return orderState, nil
}

// GetOrder returns the current state of the order identified by orderID.
func GetOrder(client *Client, orderID string) (*orders.OrderState, error) {
	conn, ctx, cancel, err := client.dial(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to gRPC: %w", err)
	}
	defer cancel()

	ordersClient := orders.NewOrdersServiceClient(conn)

	orderState, err := ordersClient.GetOrder(ctx, &orders.GetOrderRequest{
		AccountId: client.GetConfig().AccountID,
		OrderId:   orderID,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get order %s: %w", orderID, err)
	}

	return orderState, nil
}

// GetOrders returns the account's ACTIVE orders — per Finam's API docs (Migration Guide),
// this is NOT a full order history: a terminal order (filled, cancelled, rejected) can drop
// off this list once it settles. Absence from the response proves "not currently active",
// never "never existed" or "never filled" — see FindOrderByClientID.
func GetOrders(client *Client) (*orders.OrdersResponse, error) {
	conn, ctx, cancel, err := client.dial(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to gRPC: %w", err)
	}
	defer cancel()

	ordersClient := orders.NewOrdersServiceClient(conn)

	resp, err := ordersClient.GetOrders(ctx, &orders.OrdersRequest{
		AccountId: client.GetConfig().AccountID,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get orders: %w", err)
	}

	return resp, nil
}

// FindOrderByClientID scans the account's ACTIVE order list (see GetOrders) for the order
// carrying the given client_order_id (the API echoes it back inside OrderState.Order). It
// is the recovery probe for the lost-placement-response race: a place RPC that died in
// transport may still have delivered its order, and the client id — chosen BEFORE the RPC —
// is the only handle that survives the lost response. found=false with a nil error means
// only "not currently active" — it does NOT distinguish "never placed" from "placed, then
// already filled or cancelled before this probe ran" (GetOrders excludes terminal orders).
// Callers that need to shrink-and-retry on a confirmed-absent order need a stronger source
// than this one.
func FindOrderByClientID(client *Client, clientOrderID string) (*orders.OrderState, bool, error) {
	if clientOrderID == "" {
		return nil, false, fmt.Errorf("empty client order id")
	}
	resp, err := GetOrders(client)
	if err != nil {
		return nil, false, err
	}
	for _, st := range resp.GetOrders() {
		if st.GetOrder().GetClientOrderId() == clientOrderID {
			return st, true, nil
		}
	}
	return nil, false, nil
}

// sideOf maps buy to the API's order side enum.
func sideOf(buy bool) v1.Side {
	if buy {
		return v1.Side_SIDE_BUY
	}
	return v1.Side_SIDE_SELL
}

// placeMarketOrder places a market order for ticker.Vol lots. clientOrderID tags the
// order with a caller-chosen client_order_id (unique, max 20 chars — see the API docs);
// "" lets the broker auto-generate one, forfeiting lost-response recovery via
// FindOrderByClientID.
func placeMarketOrder(client *Client, ticker Ticker, buy bool, clientOrderID string) (*orders.OrderState, error) {
	return PlaceOrder(client, &orders.Order{
		AccountId: client.GetConfig().AccountID,
		Symbol:    ticker.Symbol,
		Quantity: &decimal.Decimal{
			Value: strconv.FormatInt(int64(ticker.Vol), 10),
		},
		Side:          sideOf(buy),
		Type:          orders.OrderType_ORDER_TYPE_MARKET,
		TimeInForce:   orders.TimeInForce_TIME_IN_FORCE_DAY,
		ClientOrderId: clientOrderID,
	})
}

// placeLimitOrder places a post-only (maker) limit order at price.
// TIME_IN_FORCE_GOOD_TILL_CROSSING prevents the order from crossing the spread
// and executing as a taker: it either rests in the book or is rejected.
// See placeMarketOrder for the clientOrderID semantics.
func placeLimitOrder(client *Client, ticker Ticker, buy bool, price float64, clientOrderID string) (*orders.OrderState, error) {
	return PlaceOrder(client, &orders.Order{
		AccountId: client.GetConfig().AccountID,
		Symbol:    ticker.Symbol,
		Quantity: &decimal.Decimal{
			Value: strconv.FormatInt(int64(ticker.Vol), 10),
		},
		LimitPrice: &decimal.Decimal{
			Value: strconv.FormatFloat(price, 'f', -1, 64),
		},
		Side:          sideOf(buy),
		Type:          orders.OrderType_ORDER_TYPE_LIMIT,
		TimeInForce:   orders.TimeInForce_TIME_IN_FORCE_GOOD_TILL_CROSSING,
		ClientOrderId: clientOrderID,
	})
}

func PlaceMarketOrderBuy(client *Client, ticker Ticker, clientOrderID string) (*orders.OrderState, error) {
	return placeMarketOrder(client, ticker, true, clientOrderID)
}

func PlaceMarketOrderSell(client *Client, ticker Ticker, clientOrderID string) (*orders.OrderState, error) {
	return placeMarketOrder(client, ticker, false, clientOrderID)
}

func PlaceLimitOrderBuy(client *Client, ticker Ticker, price float64, clientOrderID string) (*orders.OrderState, error) {
	return placeLimitOrder(client, ticker, true, price, clientOrderID)
}

func PlaceLimitOrderSell(client *Client, ticker Ticker, price float64, clientOrderID string) (*orders.OrderState, error) {
	return placeLimitOrder(client, ticker, false, price, clientOrderID)
}
