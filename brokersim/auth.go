package brokersim

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// authService реализует tradeapi.v1.auth.AuthService: обмен секрета на JWT с TTL
// (как боевые ~15 минут) и TokenDetails с настоящим expires_at — на нём держится
// планировщик рефреша клиента.
type authService struct {
	auth.UnimplementedAuthServiceServer
	s *Sim
}

func (a *authService) Auth(ctx context.Context, req *auth.AuthRequest) (*auth.AuthResponse, error) {
	if err := a.s.gateReadOnly("Auth"); err != nil {
		return nil, err
	}
	a.s.mu.Lock()
	defer a.s.mu.Unlock()
	acc, ok := a.s.secrets[req.GetSecret()]
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "invalid secret")
	}
	now := a.s.now()
	a.s.tokenSeq++
	t := &token{
		accountID: acc.id,
		createdAt: now,
		expiresAt: now.Add(a.s.cfg.tokenTTL()),
	}
	// Токен структурно похож на JWT (header.payload.signature) и, как боевой,
	// валидируется БЕЗ серверного состояния — по полям payload (см. parseToken).
	// Таблица tokens нужна только TokenDetails (created_at) и ExpireTokens.
	payload := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(
		`{"acc":%q,"seq":%d,"iat":%d,"exp":%d}`, acc.id, a.s.tokenSeq, now.UnixNano(), t.expiresAt.Unix())))
	t.value = simTokenPrefix + payload + simTokenSuffix
	a.s.tokens[t.value] = t
	return &auth.AuthResponse{Token: t.value}, nil
}

const (
	simTokenPrefix = "eyJhbGciOiJTSU0ifQ." // {"alg":"SIM"}
	simTokenSuffix = ".sim"
)

// simTokenClaims — payload сим-токена.
type simTokenClaims struct {
	Acc string `json:"acc"`
	Seq uint64 `json:"seq"`
	Iat int64  `json:"iat"` // unix-nanos выпуска (сверяется с revokeBefore)
	Exp int64  `json:"exp"` // unix-seconds истечения
}

// parseToken разбирает сим-токен без обращения к серверному состоянию — как
// боевой gateway валидирует JWT по подписи. Благодаря этому рестарт сима
// (инцидент «разрыв соединения с брокером») не превращается в массовый
// Unauthenticated: токены, выданные прошлым процессом, остаются валидными.
func parseToken(raw string) (simTokenClaims, bool) {
	if !strings.HasPrefix(raw, simTokenPrefix) || !strings.HasSuffix(raw, simTokenSuffix) {
		return simTokenClaims{}, false
	}
	payload := raw[len(simTokenPrefix) : len(raw)-len(simTokenSuffix)]
	b, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return simTokenClaims{}, false
	}
	var c simTokenClaims
	if json.Unmarshal(b, &c) != nil || c.Acc == "" || c.Exp == 0 {
		return simTokenClaims{}, false
	}
	return c, true
}

func (a *authService) TokenDetails(ctx context.Context, req *auth.TokenDetailsRequest) (*auth.TokenDetailsResponse, error) {
	if err := a.s.gateReadOnly("TokenDetails"); err != nil {
		return nil, err
	}
	a.s.mu.Lock()
	defer a.s.mu.Unlock()
	if t, ok := a.s.tokens[req.GetToken()]; ok {
		return &auth.TokenDetailsResponse{
			CreatedAt:  timestamppb.New(t.createdAt),
			ExpiresAt:  timestamppb.New(t.expiresAt),
			AccountIds: []string{t.accountID},
		}, nil
	}
	// Токен прошлого процесса сима (рестарт «шлюза»): детали из claims.
	if c, ok := parseToken(req.GetToken()); ok {
		return &auth.TokenDetailsResponse{
			CreatedAt:  timestamppb.New(time.Unix(0, c.Iat)),
			ExpiresAt:  timestamppb.New(time.Unix(c.Exp, 0)),
			AccountIds: []string{c.Acc},
		}, nil
	}
	return nil, status.Error(codes.Unauthenticated, "unknown token")
}

// ---- проверка токена на остальных сервисах ----

// bearerToken достаёт JWT из метаданных запроса. Клиент QuantCore шлёт голый
// токен в "Authorization"; префикс "Bearer " тоже принимается.
func bearerToken(ctx context.Context) string {
	md, _ := metadata.FromIncomingContext(ctx)
	vals := md.Get("authorization")
	if len(vals) == 0 {
		return ""
	}
	return strings.TrimPrefix(vals[0], "Bearer ")
}

// checkAuth валидирует токен запроса stateless'ом (по claims, как боевой
// gateway по подписи JWT) и возвращает счёт токена и его expiry. Токены,
// выданные до revokeBefore (ExpireTokens), отклоняются.
func (s *Sim) checkAuth(ctx context.Context) (accountID string, expiresAt time.Time, err error) {
	raw := bearerToken(ctx)
	if raw == "" {
		return "", time.Time{}, status.Error(codes.Unauthenticated, "missing authorization token")
	}
	c, ok := parseToken(raw)
	if !ok {
		return "", time.Time{}, status.Error(codes.Unauthenticated, "invalid token")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	exp := time.Unix(c.Exp, 0)
	if t, tracked := s.tokens[raw]; tracked {
		exp = t.expiresAt // ExpireTokens мог сдвинуть expiry выданного токена
	}
	if c.Iat <= s.revokeBefore {
		return "", time.Time{}, status.Error(codes.Unauthenticated, "token revoked")
	}
	if s.now().After(exp) {
		return "", time.Time{}, status.Error(codes.Unauthenticated, "token expired")
	}
	return c.Acc, exp, nil
}

// checkAccount дополнительно сверяет account_id запроса со счётом токена.
func (s *Sim) checkAccount(ctx context.Context, accountID string) (*account, time.Time, error) {
	tokenAcc, exp, err := s.checkAuth(ctx)
	if err != nil {
		return nil, time.Time{}, err
	}
	if accountID == "" {
		accountID = tokenAcc
	}
	if accountID != tokenAcc {
		return nil, time.Time{}, status.Errorf(codes.PermissionDenied, "account %s is not available to this token", accountID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	acc, ok := s.accounts[accountID]
	if !ok {
		return nil, time.Time{}, status.Errorf(codes.NotFound, "account %s not found", accountID)
	}
	return acc, exp, nil
}

// ExpireTokens мгновенно протухает ВСЕ выданные к этому моменту токены —
// инцидент «JWT умер раньше рефреша». Ставится revocation-watermark (закрывает
// и токены прошлых процессов), плюс сдвигается expiry отслеживаемых токенов.
// Активные стримы, открытые на этих токенах, оборвутся Unauthenticated при
// следующей проверке (как боевой gateway).
func (s *Sim) ExpireTokens() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	past := s.now().Add(-time.Second)
	s.revokeBefore = s.now().UnixNano()
	for _, t := range s.tokens {
		if t.expiresAt.After(past) {
			t.expiresAt = past
			n++
		}
	}
	for sub := range s.orderSubs {
		sub.tokenExp.set(past)
	}
	for sub := range s.tradeSubs {
		sub.tokenExp.set(past)
	}
	for sub := range s.accountSubs {
		sub.tokenExp.set(past)
	}
	for sub := range s.bookSubs {
		sub.tokenExp.set(past)
	}
	for sub := range s.tapeSubs {
		sub.tokenExp.set(past)
	}
	return n
}

// authSkipped — методы, доступные без токена (сам AuthService).
func authSkipped(fullMethod string) bool {
	return strings.HasPrefix(fullMethod, "/grpc.tradeapi.v1.auth.AuthService/")
}

// unaryAuthInterceptor заворачивает унарные RPC в проверку токена.
func (s *Sim) unaryAuthInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if !authSkipped(info.FullMethod) {
		if _, _, err := s.checkAuth(ctx); err != nil {
			return nil, err
		}
	}
	return handler(ctx, req)
}

// streamAuthInterceptor — то же для стримов (проверка на установлении).
func (s *Sim) streamAuthInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if !authSkipped(info.FullMethod) {
		if _, _, err := s.checkAuth(ss.Context()); err != nil {
			return err
		}
	}
	return handler(srv, ss)
}
