package finam

import (
	"context"

	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/accounts"
	"google.golang.org/grpc/metadata"
)

const accountStreamBufferSize = 64

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
			mlog.Warn("[account] channel full, dropping update")
		}
	}
}
