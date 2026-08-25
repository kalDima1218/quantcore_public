package finam

import (
	"context"
	"time"

	v1 "github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1"
	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/orders"
	"google.golang.org/grpc/metadata"
)

const orderStreamBufferSize = 256

// SubscribeOrders streams order-state snapshots for the configured account,
// reconnecting on any stream error. Provided for callers
// that need only order status (not fills).
func SubscribeOrders(client *Client) (<-chan *orders.OrderState, error) {
	out := make(chan *orders.OrderState, orderStreamBufferSize)
	go func() {
		defer close(out)
		reconnectLoop("orders", func() error { return runOrderStream(client, out) })
	}()
	return out, nil
}

// SubscribeTrades streams the account's own executed trades (fills), reconnecting
// on any stream error. This is the cleanest fill-based inventory source: a plain
// server stream of AccountTrade with no order-status noise.
func SubscribeTrades(client *Client) (<-chan *v1.AccountTrade, error) {
	out := make(chan *v1.AccountTrade, orderStreamBufferSize)
	go func() {
		defer close(out)
		reconnectLoop("trades", func() error { return runTradeStream(client, out) })
	}()
	return out, nil
}

// reconnectLoop runs the streaming function forever, pausing reconnectDelay after
// each error before re-establishing the stream.
func reconnectLoop(label string, run func() error) {
	for {
		if err := run(); err != nil {
			mlog.Printf("[%s] stream error: %v, reconnecting in %v...", label, err, reconnectDelay)
			time.Sleep(reconnectDelay)
		}
	}
}

func runTradeStream(client *Client, out chan<- *v1.AccountTrade) error {
	conn, err := client.GetStreamConn(context.Background())
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "Authorization", client.GetJWT())

	ordersClient := orders.NewOrdersServiceClient(conn)
	stream, err := ordersClient.SubscribeTrades(ctx, &orders.SubscribeTradesRequest{
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

		for _, t := range resp.Trades {
			// Blocking send, deliberately: fills are EVENTS, not state — dropping one
			// permanently desyncs the strategy's position ledger from the broker (an
			// unhedged leg and a reconcile halt at best, trading against a phantom
			// position at worst). The stream is low-rate outside the subscribe-time
			// replay burst, and the consumer loop always drains, so briefly parking
			// the reader here is the lesser evil. The high-rate book streams keep
			// their keep-newest drop policy instead — book snapshots are state.
			out <- t
		}
	}
}

func runOrderStream(client *Client, out chan<- *orders.OrderState) error {
	conn, err := client.GetStreamConn(context.Background())
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "Authorization", client.GetJWT())

	ordersClient := orders.NewOrdersServiceClient(conn)
	stream, err := ordersClient.SubscribeOrders(ctx, &orders.SubscribeOrdersRequest{
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

		for _, o := range resp.Orders {
			// Blocking send: order states are events too — a dropped terminal state
			// (rejected/cancelled/expired) would leave a clip resting on a dead order
			// until the fill-timeout backstop. Same rationale as the trades stream.
			out <- o
		}
	}
}
