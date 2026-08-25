package finam

import (
	"context"

	usage_metrics_service "github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/metrics"
)

func GetUsageMetrics(ctx context.Context, client *Client) ([]*usage_metrics_service.GetUsageMetricsResponse_QuotaUsage, error) {
	conn, ctx, cancel, err := client.dial(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()

	metricsClient := usage_metrics_service.NewUsageMetricsServiceClient(conn)

	metrics, err := metricsClient.GetUsageMetrics(ctx, &usage_metrics_service.GetUsageMetricsRequest{})
	if err != nil {
		return nil, err
	}

	return metrics.Quotas, nil
}
