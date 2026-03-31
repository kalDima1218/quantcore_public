package main

import (
	"QuantCore/trade/finam"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	loggingFreq             = 1 * time.Second
	fullOrderBookTimeFormat = "2006-01-02 15:04:05+00:00"
	maxBufferAgeSec         = 60
	staleDataThreshold      = 35 * time.Second
)

func isDataFresh(t time.Time) bool {
	return time.Since(t) <= staleDataThreshold
}

func cleanSymbolName(symbol string) string {
	if idx := strings.Index(symbol, "@"); idx != -1 {
		return symbol[:idx]
	}
	return symbol
}

type MultiSymbolCSVWriter struct {
	writer        *csv.Writer
	file          *os.File
	lastWriteTime time.Time
	symbols       []string
	mu            sync.Mutex
}

func NewMultiSymbolCSVWriter(filename string, symbols []string) (*MultiSymbolCSVWriter, error) {
	fileExists := false
	if _, err := os.Stat(filename); err == nil {
		fileExists = true
	}

	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	writer := csv.NewWriter(file)

	if !fileExists {
		header := []string{"time"}
		for _, symbol := range symbols {
			cleanSymbol := cleanSymbolName(symbol)
			header = append(header, fmt.Sprintf("%s_bid", cleanSymbol))
			header = append(header, fmt.Sprintf("%s_ask", cleanSymbol))
		}
		if err := writer.Write(header); err != nil {
			err := file.Close()
			if err != nil {
				return nil, err
			}
			return nil, err
		}

		writer.Flush()
	}

	return &MultiSymbolCSVWriter{
		writer:  writer,
		file:    file,
		symbols: symbols,
	}, nil
}

func (mcw *MultiSymbolCSVWriter) WriteMultiple(timestamp time.Time, dataMap map[string]finam.OrderBookData) error {
	mcw.mu.Lock()
	defer mcw.mu.Unlock()

	timestampUTC := timestamp.UTC()
	if !mcw.lastWriteTime.IsZero() && timestampUTC.Truncate(time.Second).Equal(mcw.lastWriteTime.Truncate(time.Second)) {
		return nil
	}

	record := []string{timestampUTC.Format("2006-01-02 15:04:05.000000-07:00")}
	for _, symbol := range mcw.symbols {
		data, exists := dataMap[symbol]
		if !exists {
			return nil
		}
		record = append(record, fmt.Sprintf("%.4f", data.BestBid))
		record = append(record, fmt.Sprintf("%.4f", data.BestAsk))
	}

	if err := mcw.writer.Write(record); err != nil {
		return err
	}

	mcw.writer.Flush()
	mcw.lastWriteTime = timestampUTC

	return mcw.writer.Error()
}

func (mcw *MultiSymbolCSVWriter) Close() error {
	mcw.mu.Lock()
	defer mcw.mu.Unlock()
	mcw.writer.Flush()
	return mcw.file.Close()
}

func SubscribeOrderBookLogging(config finam.Config, tickers []finam.Ticker, filename string) error {
	symbols := make([]string, len(tickers))
	for i, ticker := range tickers {
		symbols[i] = ticker.Symbol
	}

	csvWriter, err := NewMultiSymbolCSVWriter(filename, symbols)
	if err != nil {
		return fmt.Errorf("error creating multi-symbol CSV writer: %w", err)
	}
	defer func() {
		if err := csvWriter.Close(); err != nil {
			log.Printf("Error closing CSV writer: %v", err)
		}
	}()

	latestData := make(map[string]finam.OrderBookData)
	var dataMu sync.RWMutex

	marketOpenStatus := make(map[string]bool)
	var statusMu sync.RWMutex

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, ticker := range tickers {
		ticker := ticker

		go func() {
			client, err := finam.NewClient(config)
			if err != nil {
				log.Printf("[%s] Error creating client: %v", ticker.Symbol, err)
				return
			}

			dataChan, err := finam.SubscribeOrderBook(client, ticker)
			if err != nil {
				log.Printf("[%s] Error subscribing to orderbook: %v", ticker.Symbol, err)
				return
			}

			go func() {
				sessionTicker := time.NewTicker(1 * time.Minute)
				defer sessionTicker.Stop()

				updateSession := func() {
					statusMu.Lock()
					marketOpenStatus[ticker.Symbol] = finam.IsMarketOpen(client, ticker.Symbol)
					statusMu.Unlock()
				}

				updateSession()

				for {
					select {
					case <-sessionTicker.C:
						updateSession()
					case <-ctx.Done():
						return
					}
				}
			}()

			for data := range dataChan {
				dataMu.Lock()
				latestData[ticker.Symbol] = data
				dataMu.Unlock()
			}
		}()
	}

	writeTicker := time.NewTicker(loggingFreq)
	defer writeTicker.Stop()

	for {
		select {
		case <-writeTicker.C:
			timestamp := time.Now().UTC()

			if !isWithinWorkingHours(timestamp) {
				continue
			}

			dataMu.RLock()
			statusMu.RLock()

			dataToWrite := make(map[string]finam.OrderBookData)
			for symbol, data := range latestData {
				if marketOpenStatus[symbol] && isDataFresh(data.Timestamp) {
					dataToWrite[symbol] = data
				}
			}

			statusMu.RUnlock()
			dataMu.RUnlock()

			if len(dataToWrite) > 0 {
				if err := csvWriter.WriteMultiple(timestamp, dataToWrite); err != nil {
					log.Printf("Error writing to CSV: %v", err)
				}
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type FullOrderBookCSVWriter struct {
	writer           *csv.Writer
	file             *os.File
	depth            int
	symbols          []string
	mu               sync.Mutex
	buffer           map[int64]map[string]finam.FullOrderBookData
	lastSnapshot     map[string]finam.FullOrderBookData
	lastSnapshotTime map[string]time.Time
	lastFlushedSec   int64
	stopCh           chan struct{}
	flushDone        chan struct{}
}

func NewFullOrderBookCSVWriter(filename string, depth int, symbols []string) (*FullOrderBookCSVWriter, error) {
	fileExists := false
	if _, err := os.Stat(filename); err == nil {
		fileExists = true
	}

	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	writer := csv.NewWriter(file)

	if !fileExists {
		header := []string{"time"}
		for _, sym := range symbols {
			prefix := cleanSymbolName(sym)
			for i := 1; i <= depth; i++ {
				header = append(header, fmt.Sprintf("%s_bid_%d_price", prefix, i), fmt.Sprintf("%s_bid_%d_vol", prefix, i))
			}
			for i := 1; i <= depth; i++ {
				header = append(header, fmt.Sprintf("%s_ask_%d_price", prefix, i), fmt.Sprintf("%s_ask_%d_vol", prefix, i))
			}
		}
		if err := writer.Write(header); err != nil {
			_ = file.Close()
			return nil, err
		}
		writer.Flush()
	}

	fw := &FullOrderBookCSVWriter{
		writer:           writer,
		file:             file,
		depth:            depth,
		symbols:          symbols,
		buffer:           make(map[int64]map[string]finam.FullOrderBookData),
		lastSnapshot:     make(map[string]finam.FullOrderBookData),
		lastSnapshotTime: make(map[string]time.Time),
		stopCh:           make(chan struct{}),
		flushDone:        make(chan struct{}),
	}

	go fw.flusher()

	return fw, nil
}

func (fw *FullOrderBookCSVWriter) flusher() {
	defer close(fw.flushDone)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case t := <-ticker.C:
			cutoff := t.UTC().Truncate(time.Second).Unix()
			fw.flushBefore(cutoff)
		case <-fw.stopCh:
			fw.flushAll()
			return
		}
	}
}

func (fw *FullOrderBookCSVWriter) flushBefore(cutoffUnix int64) {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	if fw.lastFlushedSec == 0 && len(fw.buffer) == 0 {
		return
	}

	start := fw.lastFlushedSec
	if start == 0 {
		for sec := range fw.buffer {
			if sec < cutoffUnix && (start == 0 || sec < start) {
				start = sec
			}
		}
		if start == 0 {
			return
		}
		start--
	}

	for sec := start + 1; sec < cutoffUnix; sec++ {
		tickerData := fw.buffer[sec]
		fw.writeRow(sec, tickerData)
		fw.lastFlushedSec = sec
		delete(fw.buffer, sec)
	}
}

func (fw *FullOrderBookCSVWriter) flushAll() {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	var seconds []int64
	for sec := range fw.buffer {
		seconds = append(seconds, sec)
	}
	sort.Slice(seconds, func(i, j int) bool { return seconds[i] < seconds[j] })

	for _, sec := range seconds {
		fw.writeRow(sec, fw.buffer[sec])
		if sec > fw.lastFlushedSec {
			fw.lastFlushedSec = sec
		}
		delete(fw.buffer, sec)
	}
}

func (fw *FullOrderBookCSVWriter) writeRow(unixSec int64, tickerData map[string]finam.FullOrderBookData) {
	merged := make(map[string]finam.FullOrderBookData, len(fw.symbols))
	for _, sym := range fw.symbols {
		if d, ok := tickerData[sym]; ok {
			merged[sym] = d
		} else if d, ok := fw.lastSnapshot[sym]; ok {
			if t, ok := fw.lastSnapshotTime[sym]; ok && isDataFresh(t) {
				merged[sym] = d
			} else {
				return
			}
		} else {
			return
		}
	}

	t := time.Unix(unixSec, 0).UTC()
	record := []string{t.Format(fullOrderBookTimeFormat)}

	for _, sym := range fw.symbols {
		data := merged[sym]
		for i := 0; i < fw.depth; i++ {
			if i < len(data.Bids) {
				record = append(record,
					fmt.Sprintf("%.4f", data.Bids[i].Price),
					fmt.Sprintf("%.4f", data.Bids[i].Volume),
				)
			} else {
				record = append(record, "", "")
			}
		}
		for i := 0; i < fw.depth; i++ {
			if i < len(data.Asks) {
				record = append(record,
					fmt.Sprintf("%.4f", data.Asks[i].Price),
					fmt.Sprintf("%.4f", data.Asks[i].Volume),
				)
			} else {
				record = append(record, "", "")
			}
		}
	}

	if err := fw.writer.Write(record); err != nil {
		log.Printf("Error writing full orderbook row for %v: %v", t, err)
		return
	}
	fw.writer.Flush()
}

func (fw *FullOrderBookCSVWriter) Write(data finam.FullOrderBookData) {
	sec := data.Timestamp.UTC().Truncate(time.Second).Unix()
	now := time.Now().UTC().Unix()
	if now-sec > maxBufferAgeSec {
		return
	}

	fw.mu.Lock()
	defer fw.mu.Unlock()

	if _, ok := fw.buffer[sec]; !ok {
		fw.buffer[sec] = make(map[string]finam.FullOrderBookData)
	}
	fw.buffer[sec][data.Symbol] = data
	fw.lastSnapshot[data.Symbol] = data
	fw.lastSnapshotTime[data.Symbol] = time.Now()
}

func (fw *FullOrderBookCSVWriter) Stop() error {
	close(fw.stopCh)
	<-fw.flushDone
	return fw.file.Close()
}

func subscribeFullOrderBookLogging(wg *sync.WaitGroup, config finam.Config, ticker finam.Ticker, csvWriter *FullOrderBookCSVWriter) {
	defer wg.Done()

	client, err := finam.NewClient(config)
	if err != nil {
		log.Printf("[%s] Error creating client: %v", ticker.Symbol, err)
		return
	}

	dataChan, err := finam.SubscribeFullOrderBook(client, ticker)
	if err != nil {
		log.Printf("[%s] Error subscribing to full orderbook: %v", ticker.Symbol, err)
		return
	}

	var isMarketOpenFlag bool
	var sessionMu sync.RWMutex

	tickerCtx, tickerCancel := context.WithCancel(context.Background())
	defer tickerCancel()

	go func() {
		sessionTicker := time.NewTicker(1 * time.Minute)
		defer sessionTicker.Stop()

		updateSession := func() {
			sessionMu.Lock()
			isMarketOpenFlag = finam.IsMarketOpen(client, ticker.Symbol)
			sessionMu.Unlock()
		}

		updateSession()

		for {
			select {
			case <-sessionTicker.C:
				updateSession()
			case <-tickerCtx.Done():
				return
			}
		}
	}()

	for data := range dataChan {
		sessionMu.RLock()
		canWrite := isMarketOpenFlag
		sessionMu.RUnlock()

		if canWrite && isWithinWorkingHours(data.Timestamp) {
			csvWriter.Write(data)
		}
	}
}

func SubscribeFullOrderBookLogging(config finam.Config, tickers []finam.Ticker, depth int, filename string) error {
	symbols := make([]string, len(tickers))
	for i, t := range tickers {
		symbols[i] = t.Symbol
	}

	csvWriter, err := NewFullOrderBookCSVWriter(filename, depth, symbols)
	if err != nil {
		return fmt.Errorf("error creating full orderbook CSV writer: %w", err)
	}
	defer func() {
		if err := csvWriter.Stop(); err != nil {
			log.Printf("Error closing full orderbook CSV writer: %v", err)
		}
	}()

	var wg sync.WaitGroup

	for _, ticker := range tickers {
		wg.Add(1)
		go subscribeFullOrderBookLogging(&wg, config, ticker, csvWriter)
	}

	wg.Wait()
	return nil
}

type SingleListenerConfig struct {
	Symbols []struct {
		Symbol string `json:"symbol"`
		Vol    int    `json:"vol"`
	} `json:"symbols"`
	OutputFile     string `json:"output_file"`
	FullOutputFile string `json:"full_output_file"`
	Depth          int    `json:"depth"`
}

func loadListenerConfig() ([]SingleListenerConfig, error) {
	data, err := os.ReadFile("listener_config.json")
	if err != nil {
		return nil, fmt.Errorf("failed to read listener_config.json: %w", err)
	}
	var cfg []SingleListenerConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse listener_config.json: %w", err)
	}
	return cfg, nil
}

func runListenFullOrderBook() {
	config, err := finam.ReadConfig("config.json")
	if err != nil {
		log.Fatalf("Failed to read config: %v", err)
	}

	listenerConfig, err := loadListenerConfig()
	if err != nil {
		log.Fatalf("%v", err)
	}

	var wg sync.WaitGroup

	for _, cfg := range listenerConfig {
		cfg := cfg

		depth := cfg.Depth
		if depth <= 0 {
			depth = 20
		}

		outputFile := cfg.FullOutputFile
		if outputFile == "" {
			log.Printf("runListenFullOrderBook: no full output file in config, skipping")
			continue
		}

		symbols := make([]finam.Ticker, len(cfg.Symbols))
		for i, s := range cfg.Symbols {
			symbols[i] = *finam.NewTicker(s.Symbol, s.Vol)
		}

		wg.Add(1)
		go func(syms []finam.Ticker, d int, file string) {
			defer wg.Done()
			if err := SubscribeFullOrderBookLogging(*config, syms, d, file); err != nil {
				log.Printf("Error in full orderbook logging: %v", err)
			}
		}(symbols, depth, outputFile)
	}

	wg.Wait()
}

func runListen() {
	config, err := finam.ReadConfig("config.json")
	if err != nil {
		log.Fatalf("Failed to read config: %v", err)
	}

	listenerConfig, err := loadListenerConfig()
	if err != nil {
		log.Fatalf("%v", err)
	}

	var wg sync.WaitGroup

	for _, cfg := range listenerConfig {
		cfg := cfg

		if cfg.OutputFile == "" {
			log.Printf("runListen: no output file in config, skipping")
			continue
		}

		symbols := make([]finam.Ticker, len(cfg.Symbols))
		for i, s := range cfg.Symbols {
			symbols[i] = *finam.NewTicker(s.Symbol, s.Vol)
		}

		wg.Add(1)
		go func(syms []finam.Ticker, file string) {
			defer wg.Done()
			if err := SubscribeOrderBookLogging(*config, syms, file); err != nil {
				log.Printf("Error in multi-symbol logging: %v", err)
			}
		}(symbols, cfg.OutputFile)
	}

	wg.Wait()
}

func runListener() {
	config, err := finam.ReadConfig("config.json")
	if err != nil {
		log.Fatalf("Failed to read config: %v", err)
	}

	listenerConfig, err := loadListenerConfig()
	if err != nil {
		log.Fatalf("%v", err)
	}

	var wg sync.WaitGroup

	for _, cfg := range listenerConfig {
		cfg := cfg

		depth := cfg.Depth
		if depth <= 0 {
			log.Printf("runListener: non-positive depth in config, skipping")
			continue
		}

		outputFile := cfg.OutputFile
		if outputFile == "" {
			log.Printf("runListener: no output file in config, skipping")
			continue
		}

		fullOutputFile := cfg.FullOutputFile
		if fullOutputFile == "" {
			log.Printf("runListener: no full output file in config, skipping")
			continue
		}

		symbols := make([]finam.Ticker, len(cfg.Symbols))
		for i, s := range cfg.Symbols {
			symbols[i] = *finam.NewTicker(s.Symbol, s.Vol)
		}

		wg.Add(2)

		go func(syms []finam.Ticker, file string) {
			defer wg.Done()
			if err := SubscribeOrderBookLogging(*config, syms, file); err != nil {
				log.Printf("Error in orderbook logging: %v", err)
			}
		}(symbols, outputFile)

		go func(syms []finam.Ticker, d int, file string) {
			defer wg.Done()
			if err := SubscribeFullOrderBookLogging(*config, syms, d, file); err != nil {
				log.Printf("Error in full orderbook logging: %v", err)
			}
		}(symbols, depth, fullOutputFile)
	}

	wg.Wait()
}
