package brokersim

import (
	"context"
	"math"
	"time"

	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/accounts"
	"google.golang.org/genproto/googleapis/type/money"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// accountsService реализует tradeapi.v1.accounts.AccountsService.
type accountsService struct {
	accounts.UnimplementedAccountsServiceServer
	s *Sim
}

// accountSnapshotLocked собирает GetAccountResponse счёта: позиции с
// unrealized PnL по последней цене, кэш и вариант портфеля по portfolioKind
// ("forts" default | "mc" | "cash" | "none" — для отладки всех веток
// accountMargin клиента). Вызывать под mu.
func (s *Sim) accountSnapshotLocked(acc *account) *accounts.GetAccountResponse {
	var unrealized, reserved float64
	poss := make([]*accounts.Position, 0, len(acc.positions))
	for sym, p := range acc.positions {
		if p.qty == 0 {
			continue
		}
		last := p.avg
		if st := s.symbols[sym]; st != nil && st.last > 0 {
			last = st.last
		}
		u := (last - p.avg) * p.qty
		unrealized += u
		if st := s.symbols[sym]; st != nil {
			reserved += abs(p.qty) * st.cfg.MarginPerLot
		}
		poss = append(poss, &accounts.Position{
			Symbol:        sym,
			Quantity:      dec(p.qty),
			AveragePrice:  dec(p.avg),
			CurrentPrice:  dec(last),
			UnrealizedPnl: dec(u),
		})
	}
	equity := acc.cash + unrealized
	units := int64(math.Trunc(acc.cash))
	nanos := int32(math.Round((acc.cash - math.Trunc(acc.cash)) * 1e9))
	resp := &accounts.GetAccountResponse{
		AccountId:        acc.id,
		Type:             "UNION",
		Status:           "ACCOUNT_ACTIVE",
		Equity:           dec(equity),
		UnrealizedProfit: dec(unrealized),
		Positions:        poss,
		Cash:             []*money.Money{{CurrencyCode: "RUB", Units: units, Nanos: nanos}},
	}
	switch acc.portfolioKind {
	case "forts":
		resp.Portfolio = &accounts.GetAccountResponse_PortfolioForts{PortfolioForts: &accounts.FORTS{
			AvailableCash: dec(acc.cash - reserved),
			MoneyReserved: dec(reserved),
		}}
	case "mc":
		resp.Portfolio = &accounts.GetAccountResponse_PortfolioMc{PortfolioMc: &accounts.MC{
			AvailableCash: dec(acc.cash - reserved),
			InitialMargin: dec(reserved),
		}}
	case "cash":
		// портфель не проставляется — клиент падает в ветку top-level RUB cash
	case "none":
		resp.Cash = nil // и портфеля, и кэша нет — GetMargin обязан вернуть ok=false
	}
	return resp
}

func (svc *accountsService) GetAccount(ctx context.Context, req *accounts.GetAccountRequest) (*accounts.GetAccountResponse, error) {
	if err := svc.s.gateReadOnly("GetAccount"); err != nil {
		return nil, err
	}
	s := svc.s
	acc, _, err := s.checkAccount(ctx, req.GetAccountId())
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.accountSnapshotLocked(acc), nil
}

// SubscribeAccount — стрим снапшотов счёта: снапшот при подписке и после
// каждого изменения (фил, реализованный PnL).
func (svc *accountsService) SubscribeAccount(req *accounts.GetAccountRequest, stream grpc.ServerStreamingServer[accounts.GetAccountResponse]) error {
	s := svc.s
	if err := s.gateReadOnly("SubscribeAccount"); err != nil {
		return err
	}
	acc, tokenExp, err := s.checkAccount(stream.Context(), req.GetAccountId())
	if err != nil {
		return err
	}
	sub := &eventSub[struct{}]{accountID: acc.id, ch: make(chan struct{}, 1)}
	sub.tokenExp.set(tokenExp)

	s.mu.Lock()
	s.accountSubs[sub] = struct{}{}
	snapshot := s.accountSnapshotLocked(acc)
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.accountSubs, sub)
		s.mu.Unlock()
	}()

	if err := streamSend(s, stream, "SubscribeAccount", snapshot); err != nil {
		return err
	}
	tokenCheck := time.NewTicker(time.Second)
	defer tokenCheck.Stop()
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-s.stop:
			return status.Error(codes.Unavailable, "brokersim shutting down")
		case <-tokenCheck.C:
			if sub.tokenExp.expired(s.now()) {
				return status.Error(codes.Unauthenticated, "token expired")
			}
			if err := s.faults.gateKill("SubscribeAccount"); err != nil {
				return err
			}
		case <-sub.ch:
			s.mu.Lock()
			snap := s.accountSnapshotLocked(acc)
			s.mu.Unlock()
			if err := streamSend(s, stream, "SubscribeAccount", snap); err != nil {
				return err
			}
		}
	}
}
