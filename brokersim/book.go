package brokersim

import (
	"math/rand"
	"sort"
	"time"

	v1 "github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1"
	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/marketdata"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// symbolState — публичный стакан и лента символа. Стакан экзогенный: его задаёт
// control-plane или авторынок; заявки клиентов исполняются ОБ него (маркет-ордера
// съедают уровни, публичные сделки задевают лимитники), но собственные лимитники
// в публичные уровни не подмешиваются — так книга остаётся полностью управляемой
// из сценария инцидента.
type symbolState struct {
	cfg  SymbolConfig
	bids map[float64]float64
	asks map[float64]float64
	last float64 // цена последней сделки (для /fill без цены и unrealized PnL)

	tape []*marketdata.Trade // лента публичных сделок дня (для реплея подписчикам)

	auto        AutoMarketConfig
	autoMid     float64
	autoRunning bool // генератор-горутина жива (гейт от дублей; под s.mu)
	rng         *rand.Rand
}

func newSymbolState(cfg SymbolConfig) *symbolState {
	st := &symbolState{
		cfg:  cfg,
		bids: make(map[float64]float64),
		asks: make(map[float64]float64),
	}
	if cfg.AutoMarket != nil {
		st.auto = *cfg.AutoMarket
		st.autoMid = st.auto.Mid
	}
	st.rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	return st
}

func (st *symbolState) bestBid() (float64, bool) { return bestPrice(st.bids, true) }
func (st *symbolState) bestAsk() (float64, bool) { return bestPrice(st.asks, false) }

func bestPrice(book map[float64]float64, highest bool) (float64, bool) {
	var best float64
	found := false
	for p, v := range book {
		if v <= 0 {
			continue
		}
		if !found || (highest && p > best) || (!highest && p < best) {
			best, found = p, true
		}
	}
	return best, found
}

// Level — уровень стакана для control-plane.
type Level struct {
	Price float64 `json:"price"`
	Size  float64 `json:"size"`
}

// row строит одну дельту стакана. REMOVE-строки тоже несут size ("0") на нужной
// стороне oneof: клиент определяет сторону по наличию buy_size/sell_size и без
// него молча игнорирует REMOVE.
func bookRow(price, size float64, bid bool, action marketdata.StreamOrderBook_Row_Action, at time.Time) *marketdata.StreamOrderBook_Row {
	r := &marketdata.StreamOrderBook_Row{
		Price:     dec(price),
		Action:    action,
		Timestamp: timestamppb.New(at),
	}
	if bid {
		r.Side = &marketdata.StreamOrderBook_Row_BuySize{BuySize: dec(size)}
	} else {
		r.Side = &marketdata.StreamOrderBook_Row_SellSize{SellSize: dec(size)}
	}
	return r
}

// snapshotRows — полный срез книги как ADD-строки (первое сообщение подписки).
func (st *symbolState) snapshotRows(at time.Time) []*marketdata.StreamOrderBook_Row {
	rows := make([]*marketdata.StreamOrderBook_Row, 0, len(st.bids)+len(st.asks))
	for _, p := range sortedPrices(st.bids, true) {
		rows = append(rows, bookRow(p, st.bids[p], true, marketdata.StreamOrderBook_Row_ACTION_ADD, at))
	}
	for _, p := range sortedPrices(st.asks, false) {
		rows = append(rows, bookRow(p, st.asks[p], false, marketdata.StreamOrderBook_Row_ACTION_ADD, at))
	}
	return rows
}

func sortedPrices(book map[float64]float64, descending bool) []float64 {
	ps := make([]float64, 0, len(book))
	for p, v := range book {
		if v > 0 {
			ps = append(ps, p)
		}
	}
	sort.Float64s(ps)
	if descending {
		for i, j := 0, len(ps)-1; i < j; i, j = i+1, j-1 {
			ps[i], ps[j] = ps[j], ps[i]
		}
	}
	return ps
}

// setBook заменяет книгу целиком и возвращает дельты (REMOVE исчезнувших уровней
// + ADD/UPDATE новых) для рассылки подписчикам.
func (st *symbolState) setBook(bids, asks []Level, at time.Time) []*marketdata.StreamOrderBook_Row {
	var rows []*marketdata.StreamOrderBook_Row
	rows = append(rows, diffSide(st.bids, bids, true, at)...)
	rows = append(rows, diffSide(st.asks, asks, false, at)...)
	return rows
}

// diffSide приводит старую сторону книги к новой, эмитя дельты по ходу.
func diffSide(old map[float64]float64, levels []Level, bid bool, at time.Time) []*marketdata.StreamOrderBook_Row {
	var rows []*marketdata.StreamOrderBook_Row
	next := make(map[float64]float64, len(levels))
	for _, l := range levels {
		if l.Size > 0 {
			next[l.Price] = l.Size
		}
	}
	for p := range old {
		if _, keep := next[p]; !keep {
			rows = append(rows, bookRow(p, 0, bid, marketdata.StreamOrderBook_Row_ACTION_REMOVE, at))
			delete(old, p)
		}
	}
	for p, sz := range next {
		if old[p] != sz {
			action := marketdata.StreamOrderBook_Row_ACTION_ADD
			if _, had := old[p]; had {
				action = marketdata.StreamOrderBook_Row_ACTION_UPDATE
			}
			old[p] = sz
			rows = append(rows, bookRow(p, sz, bid, action, at))
		}
	}
	return rows
}

// consumeBounded снимает lots лотов с одной стороны книги (для покупателя — с
// асков, для продавца — с бидов), возвращая исполнения по уровням и дельты книги.
// limit>0 ограничивает цену уровня (лимитный тейкер), 0 — маркет без ограничения.
func (st *symbolState) consumeBounded(buy bool, lots, limit float64, at time.Time) (fills []Level, rows []*marketdata.StreamOrderBook_Row) {
	book := st.asks
	if !buy {
		book = st.bids
	}
	prices := sortedPrices(book, !buy) // покупатель ест аски от дешёвых, продавец — биды от дорогих
	remaining := lots
	for _, p := range prices {
		if remaining <= 0 {
			break
		}
		if limit > 0 && ((buy && p > limit) || (!buy && p < limit)) {
			break // уровень хуже лимитной цены
		}
		take := book[p]
		if take > remaining {
			take = remaining
		}
		fills = append(fills, Level{Price: p, Size: take})
		left := book[p] - take
		if left <= 0 {
			delete(book, p)
			rows = append(rows, bookRow(p, 0, !buy, marketdata.StreamOrderBook_Row_ACTION_REMOVE, at))
		} else {
			book[p] = left
			rows = append(rows, bookRow(p, left, !buy, marketdata.StreamOrderBook_Row_ACTION_UPDATE, at))
		}
		remaining -= take
		st.last = p
	}
	return fills, rows
}

// crossRows возвращает дельту, вбрасывающую в книгу «зависший» бид выше лучшего
// аска — классическая порча инкрементального стакана пропущенным REMOVE.
// Клиент обязан распознать кросс, не эмитить его стратегии и переподключиться.
func (st *symbolState) crossRows(at time.Time) []*marketdata.StreamOrderBook_Row {
	ask, ok := st.bestAsk()
	if !ok {
		ask = st.last
		if ask == 0 {
			ask = 100
		}
	}
	step := st.cfg.MinStep
	if step == 0 {
		step = 1
	}
	p := ask + step
	st.bids[p] = 1
	return []*marketdata.StreamOrderBook_Row{bookRow(p, 1, true, marketdata.StreamOrderBook_Row_ACTION_ADD, at)}
}

// appendTape добавляет публичную сделку в ленту дня.
func (st *symbolState) appendTape(t *marketdata.Trade) {
	st.tape = append(st.tape, t)
}

// publicTrade строит запись публичной ленты.
func publicTrade(id string, price, size float64, side v1.Side, at time.Time) *marketdata.Trade {
	return &marketdata.Trade{
		TradeId:   id,
		Timestamp: timestamppb.New(at),
		Price:     dec(price),
		Size:      dec(size),
		Side:      side,
	}
}

// autoTick — один шаг авторынка: случайное блуждание мида, пересборка стакана,
// иногда публичная сделка на споте. Возвращает дельты книги и напечатанные сделки.
func (st *symbolState) autoTick(at time.Time, tradeID func() string) (rows []*marketdata.StreamOrderBook_Row, prints []*marketdata.Trade) {
	a := &st.auto
	if a.Walk > 0 {
		st.autoMid += (st.rng.Float64()*2 - 1) * a.Walk
	}
	step := a.Step
	if step <= 0 {
		step = 1
	}
	levels := a.Levels
	if levels <= 0 {
		levels = 5
	}
	vol := a.LevelVol
	if vol <= 0 {
		vol = 100
	}
	half := a.Spread / 2
	if half <= 0 {
		half = step / 2
	}
	quant := func(p float64) float64 {
		ms := st.cfg.MinStep
		if ms <= 0 {
			return p
		}
		return float64(int64(p/ms+0.5)) * ms
	}
	var bids, asks []Level
	for i := 0; i < levels; i++ {
		bids = append(bids, Level{Price: quant(st.autoMid - half - float64(i)*step), Size: vol})
		asks = append(asks, Level{Price: quant(st.autoMid + half + float64(i)*step), Size: vol})
	}
	rows = st.setBook(bids, asks, at)

	if a.TradeProb > 0 && st.rng.Float64() < a.TradeProb {
		side := v1.Side_SIDE_BUY
		price := asks[0].Price
		if st.rng.Float64() < 0.5 {
			side = v1.Side_SIDE_SELL
			price = bids[0].Price
		}
		size := float64(1 + st.rng.Intn(10))
		tr := publicTrade(tradeID(), price, size, side, at)
		st.appendTape(tr)
		st.last = price
		prints = append(prints, tr)
	}
	return rows, prints
}

// startAutoMarket запускает горутину-генератор рынка символа, если включён и
// ещё не запущен. Конфиг читается и autoRunning выставляется под s.mu: гонка
// с конкурентным ConfigureAutoMarket и дубли генераторов исключены; wg.Add
// гейтится на s.stop, чтобы не наперегонки с wg.Wait в Close.
func (s *Sim) startAutoMarket(st *symbolState) {
	s.mu.Lock()
	if !st.auto.Enabled || st.autoRunning {
		s.mu.Unlock()
		return
	}
	interval := time.Duration(st.auto.Interval)
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	select {
	case <-s.stop:
		s.mu.Unlock()
		return
	default:
	}
	st.autoRunning = true
	s.wg.Add(1)
	s.mu.Unlock()
	go func() {
		defer s.wg.Done()
		defer func() {
			s.mu.Lock()
			st.autoRunning = false
			s.mu.Unlock()
		}()
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-t.C:
				s.mu.Lock()
				if !st.auto.Enabled {
					s.mu.Unlock()
					return
				}
				at := s.now()
				rows, prints := st.autoTick(at, func() string { return "T" + itoa(s.nextSeq()) })
				s.broadcastBookLocked(st.cfg.Symbol, rows)
				for _, tr := range prints {
					s.broadcastTapeLocked(st.cfg.Symbol, tr)
					s.matchPublicPrintLocked(st, tr)
				}
				s.mu.Unlock()
			}
		}
	}()
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}
