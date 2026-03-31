package finam

import (
	"context"
	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/accounts"
	"google.golang.org/grpc/metadata"
)

func GetAccount(client *Client) (*accounts.GetAccountResponse, error) {
	conn, err := client.GetConn(context.Background())
	if err != nil {
		return nil, err
	}

	accountsClient := accounts.NewAccountsServiceClient(conn)

	ctx := metadata.AppendToOutgoingContext(
		context.Background(),
		"Authorization",
		client.GetJWT(),
	)

	account, err := accountsClient.GetAccount(
		ctx,
		&accounts.GetAccountRequest{
			AccountId: client.GetConfig().AccountID,
		},
	)
	if err != nil {
		return nil, err
	}

	return account, nil

}
