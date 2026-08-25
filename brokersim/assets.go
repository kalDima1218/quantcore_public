package brokersim

import (
	"context"
	"math"
	"strings"
	"time"

	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/assets"
	usage_metrics "github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/metrics"
	"google.golang.org/genproto/googleapis/type/date"
	"google.golang.org/genproto/googleapis/type/interval"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func symbolNotFound(symbol string) error {
	return status.Errorf(codes.NotFound, "unknown symbol %s", symbol)
}

// assetsService реализует используемое ботом подмножество
// tradeapi.v1.assets.AssetsService: Schedule (IsMarketOpen), GetAsset,
// Exchanges, Clock, OptionsChain.
type assetsService struct {
	assets.UnimplementedAssetsServiceServer
	s *Sim
}

// Schedule отдаёт одну сессию текущего типа сима (default CORE_TRADING) на
// ±12 часов вокруг «сейчас». Тип меняется на лету через control-plane
// (POST /v1/session) — так симулируется закрытие рынка: клиентский
// IsMarketOpen видит не-торговый тип, и раннеры снимают котировки.
func (svc *assetsService) Schedule(ctx context.Context, req *assets.ScheduleRequest) (*assets.ScheduleResponse, error) {
	if err := svc.s.gateReadOnly("Schedule"); err != nil {
		return nil, err
	}
	s := svc.s
	if _, _, err := s.checkAuth(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	sessionType := s.sessionType
	now := s.now()
	s.mu.Unlock()
	return &assets.ScheduleResponse{
		Symbol: req.GetSymbol(),
		Sessions: []*assets.ScheduleResponse_Sessions{{
			Type: sessionType,
			Interval: &interval.Interval{
				StartTime: timestamppb.New(now.Add(-12 * time.Hour)),
				EndTime:   timestamppb.New(now.Add(12 * time.Hour)),
			},
		}},
	}, nil
}

// GetAsset собирает карточку инструмента из SymbolConfig; тикер и MIC
// выводятся из символа вида "TICKER@MIC".
func (svc *assetsService) GetAsset(ctx context.Context, req *assets.GetAssetRequest) (*assets.GetAssetResponse, error) {
	if err := svc.s.gateReadOnly("GetAsset"); err != nil {
		return nil, err
	}
	s := svc.s
	if _, _, err := s.checkAuth(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.symbolLocked(req.GetSymbol())
	if !ok {
		return nil, symbolNotFound(req.GetSymbol())
	}
	cfg := st.cfg
	ticker, mic := req.GetSymbol(), ""
	if i := strings.IndexByte(ticker, '@'); i >= 0 {
		ticker, mic = ticker[:i], ticker[i+1:]
	}
	lot := cfg.LotSize
	if lot == 0 {
		lot = 1
	}
	decimals := cfg.Decimals
	minStep := int64(1)
	if cfg.MinStep > 0 {
		minStep = int64(math.Round(cfg.MinStep * math.Pow10(int(decimals))))
	}
	resp := &assets.GetAssetResponse{
		Board:    mic,
		Id:       ticker,
		Ticker:   ticker,
		Mic:      mic,
		Type:     "FUTURES",
		Name:     ticker + " (brokersim)",
		Decimals: decimals,
		MinStep:  minStep,
		LotSize:  dec(lot),
	}
	if cfg.ExpirationDate != "" {
		if t, err := time.Parse("2006-01-02", cfg.ExpirationDate); err == nil {
			resp.ExpirationDate = &date.Date{Year: int32(t.Year()), Month: int32(t.Month()), Day: int32(t.Day())}
		}
	}
	return resp, nil
}

func (svc *assetsService) Exchanges(ctx context.Context, req *assets.ExchangesRequest) (*assets.ExchangesResponse, error) {
	if err := svc.s.gateReadOnly("Exchanges"); err != nil {
		return nil, err
	}
	if _, _, err := svc.s.checkAuth(ctx); err != nil {
		return nil, err
	}
	return &assets.ExchangesResponse{Exchanges: []*assets.Exchange{
		{Mic: "RTSX", Name: "Moscow Exchange Derivatives (brokersim)"},
		{Mic: "MISX", Name: "Moscow Exchange Securities (brokersim)"},
	}}, nil
}

func (svc *assetsService) Clock(ctx context.Context, req *assets.ClockRequest) (*assets.ClockResponse, error) {
	if err := svc.s.gateReadOnly("Clock"); err != nil {
		return nil, err
	}
	if _, _, err := svc.s.checkAuth(ctx); err != nil {
		return nil, err
	}
	return &assets.ClockResponse{Timestamp: timestamppb.New(svc.s.now())}, nil
}

func (svc *assetsService) OptionsChain(ctx context.Context, req *assets.OptionsChainRequest) (*assets.OptionsChainResponse, error) {
	if err := svc.s.gateReadOnly("OptionsChain"); err != nil {
		return nil, err
	}
	if _, _, err := svc.s.checkAuth(ctx); err != nil {
		return nil, err
	}
	return &assets.OptionsChainResponse{Symbol: req.GetUnderlyingSymbol()}, nil
}

// metricsService реализует UsageMetricsService: квота постановки ордеров с
// именем "OrdersService.placeOrder" — ровно тем, которое опрашивает
// finambroker.RefreshQuota.
type metricsService struct {
	usage_metrics.UnimplementedUsageMetricsServiceServer
	s *Sim
}

func (svc *metricsService) GetUsageMetrics(ctx context.Context, req *usage_metrics.GetUsageMetricsRequest) (*usage_metrics.GetUsageMetricsResponse, error) {
	if err := svc.s.gateReadOnly("GetUsageMetrics"); err != nil {
		return nil, err
	}
	s := svc.s
	if _, _, err := s.checkAuth(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if now.Sub(s.quotaStart) >= s.cfg.placeQuotaWindow() {
		s.quotaStart = now
		s.quotaUsed = 0
	}
	limit := int64(s.cfg.placeQuotaLimit())
	remaining := limit - int64(s.quotaUsed)
	if remaining < 0 {
		remaining = 0
	}
	return &usage_metrics.GetUsageMetricsResponse{Quotas: []*usage_metrics.GetUsageMetricsResponse_QuotaUsage{{
		Name:      "OrdersService.placeOrder",
		Limit:     limit,
		Remaining: remaining,
		ResetTime: timestamppb.New(s.quotaStart.Add(s.cfg.placeQuotaWindow())),
	}}}, nil
}
