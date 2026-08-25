package brokersim

import (
	"fmt"
	"net"

	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/accounts"
	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/assets"
	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/auth"
	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/marketdata"
	usage_metrics "github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/metrics"
	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/orders"
	"google.golang.org/grpc"
)

// Server — запущенный локальный брокер: gRPC-фасад tradeapi.v1 плюс
// (опционально) HTTP control-plane для сценариев инцидентов.
type Server struct {
	Sim *Sim

	grpcSrv  *grpc.Server
	grpcLis  net.Listener
	httpStop func() error
}

// Start поднимает сим: gRPC на grpcAddr (например "127.0.0.1:50051"; порт 0 —
// любой свободный), control-plane на controlAddr ("" — выключен). Трафик —
// plaintext: сим предназначен для loopback-отладки, клиент направляется на него
// явным QUANTCORE_FINAM_ADDR.
func Start(cfg Config, grpcAddr, controlAddr string) (*Server, error) {
	sim := NewSim(cfg)

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		sim.Close()
		return nil, fmt.Errorf("brokersim: listen %s: %w", grpcAddr, err)
	}

	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(sim.unaryAuthInterceptor),
		grpc.ChainStreamInterceptor(sim.streamAuthInterceptor),
	)
	auth.RegisterAuthServiceServer(srv, &authService{s: sim})
	orders.RegisterOrdersServiceServer(srv, &ordersService{s: sim})
	marketdata.RegisterMarketDataServiceServer(srv, &mdService{s: sim})
	accounts.RegisterAccountsServiceServer(srv, &accountsService{s: sim})
	assets.RegisterAssetsServiceServer(srv, &assetsService{s: sim})
	usage_metrics.RegisterUsageMetricsServiceServer(srv, &metricsService{s: sim})

	s := &Server{Sim: sim, grpcSrv: srv, grpcLis: lis}

	if controlAddr != "" {
		stop, err := sim.startControl(controlAddr)
		if err != nil {
			srv.Stop()
			_ = lis.Close()
			sim.Close()
			return nil, err
		}
		s.httpStop = stop
	}

	go func() { _ = srv.Serve(lis) }()
	return s, nil
}

// Addr — фактический адрес gRPC-листенера (для QUANTCORE_FINAM_ADDR).
func (s *Server) Addr() string { return s.grpcLis.Addr().String() }

// Close гасит gRPC, control-plane и фоновые горутины сима.
func (s *Server) Close() {
	s.grpcSrv.Stop()
	if s.httpStop != nil {
		_ = s.httpStop()
	}
	s.Sim.Close()
}
