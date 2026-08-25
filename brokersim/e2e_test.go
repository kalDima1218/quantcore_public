package brokersim_test

// e2e-тесты сима: НАСТОЯЩИЙ клиент trade/finam (и placer из execengine)
// направляется на brokersim через QUANTCORE_FINAM_ADDR — проверяется вся
// цепочка grpcclient → trade/finam → сим, включая сценарии инцидентов
// (потерянный ответ постановки, реплей сделок при реконнекте, кросс книги).

import (
	"sync"
	"testing"
	"time"

	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/auth"
	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/orders"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"QuantCore/brokersim"
	"QuantCore/strategies/execengine"
	"QuantCore/trade/finam"
)

const (
	testSecret  = "test-secret"
	testAccount = "1900001"
	testSymbol  = "LEGA@RTSX"
)

// startSim поднимает сим и настоящий finam.Client, направленный на него.
func startSim(t *testing.T, cfg brokersim.Config) (*brokersim.Server, *finam.Client) {
	t.Helper()
	if len(cfg.Accounts) == 0 {
		cfg.Accounts = []brokersim.AccountConfig{{Secret: testSecret, AccountID: testAccount, InitialCash: 1_000_000}}
	}
	if len(cfg.Symbols) == 0 {
		cfg.Symbols = []brokersim.SymbolConfig{{Symbol: testSymbol, MinStep: 1}}
	}
	srv, err := brokersim.Start(cfg, "127.0.0.1:0", "")
	if err != nil {
		t.Fatalf("start sim: %v", err)
	}
	t.Cleanup(srv.Close)

	t.Setenv(finam.EnvAddr, srv.Addr())
	client, err := finam.NewClient(finam.Config{Secret: testSecret, AccountID: testAccount})
	if err != nil {
		t.Fatalf("finam.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return srv, client
}

// waitFor поллит cond до deadline.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func setBook(t *testing.T, srv *brokersim.Server, bid, bidVol, ask, askVol float64) {
	t.Helper()
	var bids, asks []brokersim.Level
	if bidVol > 0 {
		bids = append(bids, brokersim.Level{Price: bid, Size: bidVol})
	}
	if askVol > 0 {
		asks = append(asks, brokersim.Level{Price: ask, Size: askVol})
	}
	if err := srv.Sim.SetBook(testSymbol, bids, asks); err != nil {
		t.Fatalf("SetBook: %v", err)
	}
}

// TestAuthAccountAndSchedule: авторизация через настоящий NewClient, срез счёта
// (FORTS-портфель), маржа и сессионное расписание с переключением на CLOSED.
func TestAuthAccountAndSchedule(t *testing.T) {
	srv, client := startSim(t, brokersim.Config{})

	m, ok, err := finam.GetMargin(client)
	if err != nil || !ok {
		t.Fatalf("GetMargin: ok=%v err=%v", ok, err)
	}
	if m.Source != "forts" || m.AvailableCash != 1_000_000 {
		t.Fatalf("margin = %+v, want forts/1000000", m)
	}

	if _, ok, err := finam.GetPosition(client, testSymbol); err != nil || ok {
		t.Fatalf("GetPosition on flat account: ok=%v err=%v", ok, err)
	}

	if !finam.IsMarketOpen(client, testSymbol) {
		t.Fatal("market should be open (CORE_TRADING) by default")
	}
	srv.Sim.SetSession("CLOSED")
	if finam.IsMarketOpen(client, testSymbol) {
		t.Fatal("market should be closed after SetSession(CLOSED)")
	}
}

// TestPlaceRestFillStreams: лимитник встаёт (NEW в стриме ордеров), форс-фил
// доставляет сделку в стрим сделок и FILLED в стрим ордеров; позиция и
// GetOrder сходятся.
func TestPlaceRestFillStreams(t *testing.T) {
	srv, client := startSim(t, brokersim.Config{})
	setBook(t, srv, 99, 10, 101, 10)

	orderCh, err := finam.SubscribeOrders(client)
	if err != nil {
		t.Fatal(err)
	}
	tradeCh, err := finam.SubscribeTrades(client)
	if err != nil {
		t.Fatal(err)
	}

	st, err := finam.PlaceLimitOrderBuy(client, finam.Ticker{Symbol: testSymbol, Vol: 2}, 100, "cid-rest-1")
	if err != nil {
		t.Fatalf("PlaceLimitOrderBuy: %v", err)
	}
	if st.GetOrderId() == "" {
		t.Fatal("empty order id")
	}
	if got := st.GetOrder().GetClientOrderId(); got != "cid-rest-1" {
		t.Fatalf("client_order_id echo = %q, want cid-rest-1", got)
	}

	// NEW долетает в стрим ордеров.
	waitFor(t, "NEW on order stream", func() bool {
		select {
		case o := <-orderCh:
			return o.GetOrderId() == st.GetOrderId()
		default:
			return false
		}
	})

	if err := srv.Sim.FillOrder(st.GetOrderId(), 0, 0); err != nil {
		t.Fatalf("FillOrder: %v", err)
	}

	waitFor(t, "fill on trade stream", func() bool {
		select {
		case tr := <-tradeCh:
			return tr.GetOrderId() == st.GetOrderId() && finam.ParseDecimal(tr.GetSize().GetValue()) == 2
		default:
			return false
		}
	})

	got, err := finam.GetOrder(client, st.GetOrderId())
	if err != nil {
		t.Fatal(err)
	}
	if finam.ExecutedLots(got) != 2 {
		t.Fatalf("executed = %d, want 2", finam.ExecutedLots(got))
	}
	pos, ok, err := finam.GetPosition(client, testSymbol)
	if err != nil || !ok || pos.Quantity != 2 {
		t.Fatalf("position = %+v ok=%v err=%v, want qty 2", pos, ok, err)
	}
}

// TestPostOnlyRejectedByExchange: GTX-лимитник, кроссящий спред, принимается
// (NEW) и асинхронно снимается биржей — терминальный статус приходит стримом,
// ровно как ждёт движок (IsDeadStatus).
func TestPostOnlyRejectedByExchange(t *testing.T) {
	srv, client := startSim(t, brokersim.Config{})
	setBook(t, srv, 99, 10, 101, 10)

	st, err := finam.PlaceLimitOrderBuy(client, finam.Ticker{Symbol: testSymbol, Vol: 1}, 102, "cid-cross-1")
	if err != nil {
		t.Fatalf("place: %v", err)
	}
	waitFor(t, "exchange reject", func() bool {
		got, err := finam.GetOrder(client, st.GetOrderId())
		return err == nil && execengine.IsDeadStatus(got.GetStatus()) && finam.ExecutedLots(got) == 0
	})
}

// TestMarketOrderPartialLiquidity: маркет-ордер на 5 лотов при 4 лотах в книге —
// частичные филлы и снятие недобора биржей (CANCELED, executed=4). Инцидент
// «подтверждённо-мёртвый хедж с недобором».
func TestMarketOrderPartialLiquidity(t *testing.T) {
	srv, client := startSim(t, brokersim.Config{})
	if err := srv.Sim.SetBook(testSymbol,
		[]brokersim.Level{{Price: 99, Size: 10}},
		[]brokersim.Level{{Price: 101, Size: 2}, {Price: 102, Size: 2}}); err != nil {
		t.Fatal(err)
	}

	st, err := finam.PlaceMarketOrderBuy(client, finam.Ticker{Symbol: testSymbol, Vol: 5}, "cid-mkt-1")
	if err != nil {
		t.Fatalf("market buy: %v", err)
	}
	waitFor(t, "partial fill + exchange kill", func() bool {
		got, err := finam.GetOrder(client, st.GetOrderId())
		return err == nil && execengine.IsDeadStatus(got.GetStatus()) && finam.ExecutedLots(got) == 4
	})
	pos, ok, err := finam.GetPosition(client, testSymbol)
	if err != nil || !ok || pos.Quantity != 4 {
		t.Fatalf("position = %+v ok=%v err=%v, want qty 4", pos, ok, err)
	}
}

// TestLostPlaceResponseRecovery — главный инцидент: ответ PlaceOrder теряется в
// транспорте ПОСЛЕ постановки. Настоящий placer из execengine обязан найти ордер
// по client_order_id через GetOrders и «усыновить» его.
func TestLostPlaceResponseRecovery(t *testing.T) {
	srv, client := startSim(t, brokersim.Config{})
	setBook(t, srv, 99, 10, 101, 10)

	if _, err := srv.Sim.AddFault(brokersim.Fault{Method: "PlaceOrder", Action: "drop_after_apply"}); err != nil {
		t.Fatal(err)
	}

	maker := execengine.NewFinamMaker(client)
	orderID, err := maker.PlaceBid(testSymbol, 1, 100)
	if err != nil {
		t.Fatalf("PlaceBid should have adopted the ghost order, got error: %v", err)
	}
	got, err := finam.GetOrder(client, orderID)
	if err != nil {
		t.Fatalf("adopted order not found: %v", err)
	}
	if got.GetStatus().String() != "ORDER_STATUS_NEW" {
		t.Fatalf("adopted order status = %s, want NEW", got.GetStatus())
	}
}

// TestCleanRejectIsNotRetried: деловой отказ (InvalidArgument) не считается
// потерянным ответом — placer отдаёт ошибку сразу, ничего не «усыновляя».
func TestCleanRejectIsNotRetried(t *testing.T) {
	srv, client := startSim(t, brokersim.Config{})
	setBook(t, srv, 99, 10, 101, 10)

	if _, err := srv.Sim.AddFault(brokersim.Fault{
		Method: "PlaceOrder", Action: "error", Code: codes.InvalidArgument, Message: "bad params",
	}); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	maker := execengine.NewFinamMaker(client)
	if _, err := maker.PlaceBid(testSymbol, 1, 100); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("want InvalidArgument, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("clean reject took %v — placer must not probe GetOrders on business errors", elapsed)
	}
}

// TestCancelLifecycle: отмена стоящего ордера возвращает CANCELED и executed=0;
// повторная отмена — FailedPrecondition; отмена неизвестного — NotFound.
func TestCancelLifecycle(t *testing.T) {
	srv, client := startSim(t, brokersim.Config{})
	setBook(t, srv, 99, 10, 101, 10)

	st, err := finam.PlaceLimitOrderSell(client, finam.Ticker{Symbol: testSymbol, Vol: 1}, 102, "cid-cxl-1")
	if err != nil {
		t.Fatal(err)
	}
	canceled, err := finam.CancelOrder(client, st.GetOrderId())
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if canceled.GetStatus().String() != "ORDER_STATUS_CANCELED" || finam.ExecutedLots(canceled) != 0 {
		t.Fatalf("cancel result: status=%s executed=%d", canceled.GetStatus(), finam.ExecutedLots(canceled))
	}
	if _, err := finam.CancelOrder(client, st.GetOrderId()); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("double cancel: want FailedPrecondition, got %v", err)
	}
	if _, err := finam.CancelOrder(client, "999999999"); status.Code(err) != codes.NotFound {
		t.Fatalf("cancel unknown: want NotFound, got %v", err)
	}
}

// TestDuplicateClientOrderID: повторный client_order_id в пределах дня — отказ.
func TestDuplicateClientOrderID(t *testing.T) {
	srv, client := startSim(t, brokersim.Config{})
	setBook(t, srv, 99, 10, 101, 10)

	tk := finam.Ticker{Symbol: testSymbol, Vol: 1}
	if _, err := finam.PlaceLimitOrderBuy(client, tk, 100, "cid-dup"); err != nil {
		t.Fatal(err)
	}
	if _, err := finam.PlaceLimitOrderBuy(client, tk, 100, "cid-dup"); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("want AlreadyExists, got %v", err)
	}
}

// TestTradeReplayOnReconnect: реплей сегодняшних сделок при (пере)подписке.
// Убийство стрима сбоем заставляет клиентскую обёртку переподключиться — та же
// сделка доставляется снова, и её обязан отсеять TradeDedup.
func TestTradeReplayOnReconnect(t *testing.T) {
	srv, client := startSim(t, brokersim.Config{})
	setBook(t, srv, 99, 10, 101, 10)

	st, err := finam.PlaceLimitOrderBuy(client, finam.Ticker{Symbol: testSymbol, Vol: 1}, 100, "cid-rep-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Sim.FillOrder(st.GetOrderId(), 0, 0); err != nil {
		t.Fatal(err)
	}

	// Подписка ПОСЛЕ сделки: реплей обязан её доставить.
	tradeCh, err := finam.SubscribeTrades(client)
	if err != nil {
		t.Fatal(err)
	}
	var firstID string
	waitFor(t, "replay burst", func() bool {
		select {
		case tr := <-tradeCh:
			firstID = tr.GetTradeId()
			return tr.GetOrderId() == st.GetOrderId()
		default:
			return false
		}
	})

	// Обрыв стрима: обёртка переподключается (1s) и реплей приходит повторно.
	if _, err := srv.Sim.AddFault(brokersim.Fault{Method: "SubscribeTrades", Action: "kill_stream"}); err != nil {
		t.Fatal(err)
	}
	dedup := execengine.TradeDedup{}
	dedup.Seen(firstID)
	waitFor(t, "replayed duplicate after reconnect", func() bool {
		select {
		case tr := <-tradeCh:
			if !dedup.Seen(tr.GetTradeId()) {
				t.Fatalf("reconnect replay delivered unseen trade %s — expected the same trade id %s", tr.GetTradeId(), firstID)
			}
			return true
		default:
			return false
		}
	})
}

// TestBookSnapshotDeltasAndCross: подписка на стакан через настоящий клиент —
// снапшот, дельты, и порча книги кроссирующим бидом: клиент обязан НЕ эмитить
// кроссированный срез.
func TestBookSnapshotDeltasAndCross(t *testing.T) {
	srv, client := startSim(t, brokersim.Config{})
	setBook(t, srv, 99, 10, 101, 10)

	ch, err := finam.SubscribeFullOrderBook(client, finam.Ticker{Symbol: testSymbol, Vol: 1})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "initial snapshot", func() bool {
		select {
		case b := <-ch:
			return len(b.Bids) == 1 && b.Bids[0].Price == 99 && len(b.Asks) == 1 && b.Asks[0].Price == 101
		default:
			return false
		}
	})

	setBook(t, srv, 100, 5, 102, 7)
	waitFor(t, "delta application", func() bool {
		select {
		case b := <-ch:
			return len(b.Bids) == 1 && b.Bids[0].Price == 100 && b.Asks[0].Price == 102
		default:
			return false
		}
	})

	// Кросс: клиент молчит (не эмитит кроссированный срез).
	if err := srv.Sim.CrossBook(testSymbol); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	for {
		select {
		case b := <-ch:
			if len(b.Bids) > 0 && len(b.Asks) > 0 && b.Bids[0].Price > b.Asks[0].Price {
				t.Fatalf("client emitted a CROSSED book: bid %v > ask %v", b.Bids[0].Price, b.Asks[0].Price)
			}
			continue
		default:
		}
		break
	}
}

// TestQuota: GetUsageMetrics отдаёт квоту placeOrder, постановки её расходуют,
// исчерпание — чистый ResourceExhausted (не транспортная неоднозначность).
func TestQuota(t *testing.T) {
	srv, client := startSim(t, brokersim.Config{})
	setBook(t, srv, 99, 10, 101, 10)

	quotas, err := finam.GetUsageMetrics(t.Context(), client)
	if err != nil {
		t.Fatal(err)
	}
	if len(quotas) == 0 || quotas[0].GetName() != "OrdersService.placeOrder" || quotas[0].GetRemaining() != 200 {
		t.Fatalf("quotas = %+v", quotas)
	}

	if _, err := finam.PlaceLimitOrderBuy(client, finam.Ticker{Symbol: testSymbol, Vol: 1}, 100, "cid-q-1"); err != nil {
		t.Fatal(err)
	}
	quotas, err = finam.GetUsageMetrics(t.Context(), client)
	if err != nil {
		t.Fatal(err)
	}
	if quotas[0].GetRemaining() != 199 {
		t.Fatalf("remaining = %d, want 199", quotas[0].GetRemaining())
	}

	srv.Sim.SetQuotaRemaining(0)
	if _, err := finam.PlaceLimitOrderBuy(client, finam.Ticker{Symbol: testSymbol, Vol: 1}, 100, "cid-q-2"); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("want ResourceExhausted, got %v", err)
	}
}

// TestGetOrdersActiveOnly: терминальные ордера по умолчанию НЕ в списке (как
// документирован боевой API) — слепая зона fill-and-vanish воспроизводится.
func TestGetOrdersActiveOnly(t *testing.T) {
	srv, client := startSim(t, brokersim.Config{})
	setBook(t, srv, 99, 10, 101, 10)

	resting, err := finam.PlaceLimitOrderBuy(client, finam.Ticker{Symbol: testSymbol, Vol: 1}, 100, "cid-act-1")
	if err != nil {
		t.Fatal(err)
	}
	filled, err := finam.PlaceLimitOrderBuy(client, finam.Ticker{Symbol: testSymbol, Vol: 1}, 98, "cid-act-2")
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Sim.FillOrder(filled.GetOrderId(), 0, 0); err != nil {
		t.Fatal(err)
	}

	if _, found, err := finam.FindOrderByClientID(client, "cid-act-1"); err != nil || !found {
		t.Fatalf("resting order must be discoverable by client id: found=%v err=%v", found, err)
	}
	if _, found, err := finam.FindOrderByClientID(client, "cid-act-2"); err != nil || found {
		t.Fatalf("filled order must have vanished from the active list: found=%v err=%v", found, err)
	}
	_ = resting
}

// TestErrorFaultDefaultCode: у действия "error" дефолтный код — InvalidArgument
// (чистый деловой отказ, ВНЕ maybeDelivered-класса): placer обязан отдать
// ошибку сразу, без проб GetOrders.
func TestErrorFaultDefaultCode(t *testing.T) {
	srv, client := startSim(t, brokersim.Config{})
	setBook(t, srv, 99, 10, 101, 10)

	if _, err := srv.Sim.AddFault(brokersim.Fault{Method: "PlaceOrder", Action: "error"}); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	maker := execengine.NewFinamMaker(client)
	if _, err := maker.PlaceBid(testSymbol, 1, 100); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("want default InvalidArgument, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("default error fault took %v — must be classified as a clean reject, not probed", elapsed)
	}
}

// TestCancelAsyncWithInterveningFill: частичный фил в окне асинхронной отмены
// не теряет её — остаток снимается (CANCELED, executed<qty).
func TestCancelAsyncWithInterveningFill(t *testing.T) {
	srv, client := startSim(t, brokersim.Config{
		CancelAsync: true,
		ExecLatency: brokersim.Duration(200 * time.Millisecond),
	})
	setBook(t, srv, 99, 10, 101, 10)

	st, err := finam.PlaceLimitOrderBuy(client, finam.Ticker{Symbol: testSymbol, Vol: 2}, 100, "cid-async-1")
	if err != nil {
		t.Fatal(err)
	}
	pending, err := finam.CancelOrder(client, st.GetOrderId())
	if err != nil {
		t.Fatal(err)
	}
	if pending.GetStatus() != orders.OrderStatus_ORDER_STATUS_PENDING_CANCEL {
		t.Fatalf("async cancel response = %s, want PENDING_CANCEL", pending.GetStatus())
	}
	// Частичный фил внутри окна отмены.
	if err := srv.Sim.FillOrder(st.GetOrderId(), 1, 100); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "remainder canceled", func() bool {
		got, err := finam.GetOrder(client, st.GetOrderId())
		return err == nil &&
			got.GetStatus() == orders.OrderStatus_ORDER_STATUS_CANCELED &&
			finam.ExecutedLots(got) == 1
	})
}

// TestConfigureAutoMarketConcurrent: конкурентные включения/выключения
// авторынка (гонка st.auto и дубли генераторов — под -race).
func TestConfigureAutoMarketConcurrent(t *testing.T) {
	srv, _ := startSim(t, brokersim.Config{})
	cfg := brokersim.AutoMarketConfig{
		Enabled: true, Mid: 100, Spread: 2, Step: 1, Levels: 3, LevelVol: 10,
		Interval: brokersim.Duration(5 * time.Millisecond), TradeProb: 0.5,
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				c := cfg
				c.Enabled = i%2 == 0 || j%3 != 0
				_ = srv.Sim.ConfigureAutoMarket(testSymbol, c)
			}
		}(i)
	}
	wg.Wait()
	off := cfg
	off.Enabled = false
	_ = srv.Sim.ConfigureAutoMarket(testSymbol, off)
	// Server.Close в t.Cleanup дождётся генераторов — зависание здесь = дефект.
}

// TestAuthSemantics: прямыми SDK-стабами — неверный секрет отклоняется
// Unauthenticated, RPC без токена отклоняется Unauthenticated, TokenDetails
// валидного токена несёт expires_at ~ TTL и account_ids.
func TestAuthSemantics(t *testing.T) {
	srv, err := brokersim.Start(brokersim.Config{
		Accounts: []brokersim.AccountConfig{{Secret: testSecret, AccountID: testAccount}},
	}, "127.0.0.1:0", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Close)

	conn, err := grpc.NewClient(srv.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ctx := t.Context()

	authClient := auth.NewAuthServiceClient(conn)
	if _, err := authClient.Auth(ctx, &auth.AuthRequest{Secret: "wrong"}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("bad secret: want Unauthenticated, got %v", err)
	}

	ordersClient := orders.NewOrdersServiceClient(conn)
	if _, err := ordersClient.GetOrders(ctx, &orders.OrdersRequest{AccountId: testAccount}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("no token: want Unauthenticated, got %v", err)
	}

	resp, err := authClient.Auth(ctx, &auth.AuthRequest{Secret: testSecret})
	if err != nil {
		t.Fatal(err)
	}
	det, err := authClient.TokenDetails(ctx, &auth.TokenDetailsRequest{Token: resp.GetToken()})
	if err != nil {
		t.Fatal(err)
	}
	ttl := det.GetExpiresAt().AsTime().Sub(det.GetCreatedAt().AsTime())
	if ttl < 14*time.Minute || ttl > 16*time.Minute {
		t.Fatalf("token ttl = %v, want ~15m", ttl)
	}
	if len(det.GetAccountIds()) != 1 || det.GetAccountIds()[0] != testAccount {
		t.Fatalf("account_ids = %v", det.GetAccountIds())
	}

	// Протухание токенов: RPC с живым токеном начинает отваливаться Unauthenticated.
	md := metadata.AppendToOutgoingContext(ctx, "Authorization", resp.GetToken())
	if _, err := ordersClient.GetOrders(md, &orders.OrdersRequest{AccountId: testAccount}); err != nil {
		t.Fatalf("authorized GetOrders: %v", err)
	}
	srv.Sim.ExpireTokens()
	if _, err := ordersClient.GetOrders(md, &orders.OrdersRequest{AccountId: testAccount}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expired token: want Unauthenticated, got %v", err)
	}
}

// TestPlaceCancelCountersAreMonotonic: счётчики размещений/отмен считают ВСЕ принятые
// вызовы за жизнь сима. Это наземная правда для churn-инварианта харнесса бота —
// внутренний quotaUsed для этого не годится, он обнуляется на границе окна квоты.
func TestPlaceCancelCountersAreMonotonic(t *testing.T) {
	srv, client := startSim(t, brokersim.Config{})
	setBook(t, srv, 99, 10, 101, 10)

	if got := srv.Sim.PlaceCount(); got != 0 {
		t.Fatalf("PlaceCount на старте = %d, ожидалось 0", got)
	}
	if got := srv.Sim.CancelCount(); got != 0 {
		t.Fatalf("CancelCount на старте = %d, ожидалось 0", got)
	}

	tk := finam.Ticker{Symbol: testSymbol, Vol: 1}
	st1, err := finam.PlaceLimitOrderBuy(client, tk, 98, "cid-cnt-1")
	if err != nil {
		t.Fatal(err)
	}
	st2, err := finam.PlaceLimitOrderBuy(client, tk, 97, "cid-cnt-2")
	if err != nil {
		t.Fatal(err)
	}
	if got := srv.Sim.PlaceCount(); got != 2 {
		t.Fatalf("PlaceCount после двух размещений = %d, ожидалось 2", got)
	}

	if _, err := finam.CancelOrder(client, st1.GetOrderId()); err != nil {
		t.Fatalf("cancel 1: %v", err)
	}
	if _, err := finam.CancelOrder(client, st2.GetOrderId()); err != nil {
		t.Fatalf("cancel 2: %v", err)
	}
	if got := srv.Sim.CancelCount(); got != 2 {
		t.Fatalf("CancelCount после двух отмен = %d, ожидалось 2", got)
	}
	if got := srv.Sim.PlaceCount(); got != 2 {
		t.Fatalf("PlaceCount не должен меняться от отмен, стало %d", got)
	}

	// Отказавшая попытка ТОЖЕ считается: счётчик меряет нагрузку на брокера, а не
	// успехи. Иначе шторм ретраев по отвергнутым запросам был бы невидим — а именно
	// его churn-инвариант харнесса и ловит (боевой реджект «(10003) Session is not
	// available» 25.07 в 06:59).
	if _, err := finam.CancelOrder(client, "999999999"); err == nil {
		t.Fatal("cancel неизвестного ордера обязан был отказать")
	}
	if got := srv.Sim.CancelCount(); got != 3 {
		t.Fatalf("отказавшая отмена обязана считаться как нагрузка, стало %d, ожидалось 3", got)
	}
}
