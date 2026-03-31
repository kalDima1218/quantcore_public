package finam

import (
	"context"
	"fmt"
	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1"
	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/orders"
	"google.golang.org/genproto/googleapis/type/decimal"
	"google.golang.org/grpc/metadata"
	"log"
	"strconv"
)

func PlaceOrder(client *Client, order *orders.Order) (*orders.OrderState, error) {
	conn, err := client.GetConn(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to gRPC: %w", err)
	}

	log.Printf("Authentication successful")

	ctx := metadata.AppendToOutgoingContext(
		context.Background(),
		"Authorization",
		client.GetJWT(),
	)

	ordersClient := orders.NewOrdersServiceClient(conn)

	orderState, err := ordersClient.PlaceOrder(ctx, order)
	if err != nil {
		return nil, fmt.Errorf("failed to place order: %w", err)
	}

	log.Printf("Order placed successfully: OrderID=%s, Status=%s", orderState.OrderId, orderState.Status.String())

	return orderState, nil
}

func PlaceMarketOrderBuy(client *Client, ticker Ticker) (*orders.OrderState, error) {
	order := &orders.Order{
		AccountId: client.GetConfig().AccountID,
		Symbol:    ticker.Symbol,
		Quantity: &decimal.Decimal{
			Value: strconv.FormatInt(int64(ticker.Vol), 10),
		},
		Side:        v1.Side_SIDE_BUY,
		Type:        orders.OrderType_ORDER_TYPE_MARKET,
		TimeInForce: orders.TimeInForce_TIME_IN_FORCE_DAY,
	}

	orderState, err := PlaceOrder(client, order)
	return orderState, err
}

func PlaceMarketOrderSell(client *Client, ticker Ticker) (*orders.OrderState, error) {
	order := &orders.Order{
		AccountId: client.GetConfig().AccountID,
		Symbol:    ticker.Symbol,
		Quantity: &decimal.Decimal{
			Value: strconv.FormatInt(int64(ticker.Vol), 10),
		},
		Side:        v1.Side_SIDE_SELL,
		Type:        orders.OrderType_ORDER_TYPE_MARKET,
		TimeInForce: orders.TimeInForce_TIME_IN_FORCE_DAY,
	}

	orderState, err := PlaceOrder(client, order)
	return orderState, err
}
