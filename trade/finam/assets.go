package finam

import (
	"context"
	"fmt"
	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/assets"
	"google.golang.org/grpc/metadata"
	"log"
	"time"
)

func GetMarketMode(client *Client, symbol string) (string, error) {
	conn, err := client.GetConn(context.Background())
	if err != nil {
		return "", fmt.Errorf("failed to create grpc connection: %w", err)
	}

	authCtx := metadata.AppendToOutgoingContext(
		context.Background(),
		"Authorization",
		client.GetJWT(),
	)

	assetsClient := assets.NewAssetsServiceClient(conn)

	scheduleResp, err := assetsClient.Schedule(authCtx, &assets.ScheduleRequest{
		Symbol: symbol,
	})
	if err != nil {
		return "", fmt.Errorf("failed to get schedule: %w", err)
	}

	now := time.Now().UTC()

	for _, session := range scheduleResp.Sessions {
		if session.Interval == nil {
			continue
		}

		var startTime, endTime time.Time

		if session.Interval.StartTime != nil {
			startTime = session.Interval.StartTime.AsTime().UTC()
		}

		if session.Interval.EndTime != nil {
			endTime = session.Interval.EndTime.AsTime().UTC()
		}

		if !startTime.IsZero() && !endTime.IsZero() {
			if (now.After(startTime) || now.Equal(startTime)) && now.Before(endTime) {
				return session.Type, nil
			}
		}
	}

	return "", nil
}

func IsMarketOpen(client *Client, symbol string) bool {
	sessionType, err := GetMarketMode(client, symbol)
	if err != nil {
		log.Printf("[%s] Error getting trading session: %v", symbol, err)
		return false
	}

	return sessionType == "EARLY_TRADING" || sessionType == "CORE_TRADING" || sessionType == "LATE_TRADING"
}

func GetAssets(client *Client) ([]*assets.Asset, error) {
	conn, err := client.GetConn(context.Background())
	if err != nil {
		return nil, err
	}

	assetsClient := assets.NewAssetsServiceClient(conn)

	ctx := metadata.AppendToOutgoingContext(
		context.Background(),
		"Authorization",
		client.GetJWT(),
	)

	assetsResp, err := assetsClient.Assets(
		ctx,
		&assets.AssetsRequest{},
	)
	if err != nil {
		return nil, err
	}

	return assetsResp.Assets, nil
}

func GetAsset(client *Client, symbol string) (*assets.GetAssetResponse, error) {
	conn, err := client.GetConn(context.Background())
	if err != nil {
		return nil, err
	}

	assetsClient := assets.NewAssetsServiceClient(conn)

	ctx := metadata.AppendToOutgoingContext(
		context.Background(),
		"Authorization",
		client.GetJWT(),
	)

	assetResp, err := assetsClient.GetAsset(
		ctx,
		&assets.GetAssetRequest{
			Symbol: symbol,
		},
	)
	if err != nil {
		return nil, err
	}

	return assetResp, nil
}

func GetOptionsChain(client *Client, symbol string) ([]*assets.Option, error) {
	conn, err := client.GetConn(context.Background())
	if err != nil {
		return nil, err
	}

	assetsClient := assets.NewAssetsServiceClient(conn)

	ctx := metadata.AppendToOutgoingContext(
		context.Background(),
		"Authorization",
		client.GetJWT(),
	)

	assetResp, err := assetsClient.OptionsChain(
		ctx,
		&assets.OptionsChainRequest{
			UnderlyingSymbol: symbol,
		},
	)
	if err != nil {
		return nil, err
	}

	return assetResp.Options, nil
}

func GetExchanges(client *Client) ([]*assets.Exchange, error) {
	conn, err := client.GetConn(context.Background())
	if err != nil {
		return nil, err
	}

	assetsClient := assets.NewAssetsServiceClient(conn)

	ctx := metadata.AppendToOutgoingContext(
		context.Background(),
		"Authorization",
		client.GetJWT(),
	)

	exchangesResp, err := assetsClient.Exchanges(
		ctx,
		&assets.ExchangesRequest{},
	)
	if err != nil {
		return nil, err
	}

	return exchangesResp.Exchanges, nil
}
