package finam

import (
	"context"
	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/metrics"
	"google.golang.org/grpc/metadata"
)

func GetUsageMetrics(client *Client) ([]*usage_metrics_service.GetUsageMetricsResponse_QuotaUsage, error) {
	conn, err := client.GetConn(context.Background())
	if err != nil {
		return nil, err
	}

	metricsClient := usage_metrics_service.NewUsageMetricsServiceClient(conn)

	ctx := metadata.AppendToOutgoingContext(
		context.Background(),
		"Authorization",
		client.GetJWT(),
	)

	metrics, err := metricsClient.GetUsageMetrics(
		ctx,
		&usage_metrics_service.GetUsageMetricsRequest{},
	)
	if err != nil {
		return nil, err
	}

	return metrics.Quotas, nil
}
