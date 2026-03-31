package finam

import (
	"context"
	"fmt"
	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/marketdata"
	"google.golang.org/grpc/metadata"
	"log"
	"sort"
	"strconv"
	"time"
)

const (
	orderBookChannelBufferSize = 1000
	isLogging                  = false
)

type PriceLevel struct {
	Price  float64
	Volume float64
}

type FullOrderBookData struct {
	Timestamp time.Time
	Symbol    string
	Bids      []PriceLevel
	Asks      []PriceLevel
}

type Ticker struct {
	Symbol string
	Vol    int
}

type OrderBookData struct {
	Timestamp time.Time
	Symbol    string
	BestBid   float64
	BestAsk   float64
}

func NewTicker(symbol string, vol int) *Ticker {
	return &Ticker{Symbol: symbol, Vol: vol}
}

func calculateExecutionPrice(priceLevels []PriceLevel, targetVolume int) (float64, bool) {
	if len(priceLevels) == 0 || targetVolume <= 0 {
		return 0, false
	}

	var (
		accumulatedVolume float64
		executionPrice    float64
	)

	targetVol := float64(targetVolume)

	for _, level := range priceLevels {
		remainingVolume := targetVol - accumulatedVolume
		if remainingVolume <= 0 {
			break
		}

		volumeToTake := level.Volume
		if volumeToTake > remainingVolume {
			volumeToTake = remainingVolume
		}

		accumulatedVolume += volumeToTake
		executionPrice += level.Price * volumeToTake
	}

	if accumulatedVolume > 0 {
		return executionPrice / accumulatedVolume, true
	}

	return 0, false
}

func subscribeOrderBook(client *Client, ticker Ticker, dataChan chan<- OrderBookData) error {
	conn, err := client.GetConn(context.Background())
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx = metadata.AppendToOutgoingContext(
		ctx,
		"Authorization",
		client.GetJWT(),
	)

	mdClient := marketdata.NewMarketDataServiceClient(conn)

	stream, err := mdClient.SubscribeOrderBook(ctx, &marketdata.SubscribeOrderBookRequest{
		Symbol: ticker.Symbol,
	})
	if err != nil {
		return err
	}

	askBook := make(map[float64]float64)
	bidBook := make(map[float64]float64)

	respChan := make(chan *marketdata.SubscribeOrderBookResponse, 1)
	errChan := make(chan error, 1)

	go func() {
		for {
			resp, err := stream.Recv()
			if err != nil {
				select {
				case errChan <- err:
				case <-ctx.Done():
				}
				return
			}
			select {
			case respChan <- resp:
			case <-ctx.Done():
				return
			}
		}
	}()

	heartbeatTimeout := 30 * time.Second
	timer := time.NewTimer(heartbeatTimeout)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-timer.C:
			log.Printf("[%s] Heartbeat timeout - no data received for %v", ticker.Symbol, heartbeatTimeout)
			return fmt.Errorf("heartbeat timeout: no data received for %v", heartbeatTimeout)

		case err := <-errChan:
			return err

		case resp := <-respChan:
			timer.Stop()
			timer.Reset(heartbeatTimeout)

			timestamp := time.Now().UTC()

			for _, orderBook := range resp.OrderBook {
				for _, row := range orderBook.Rows {
					if row.Price == nil {
						continue
					}

					priceFloat, err := strconv.ParseFloat(row.Price.Value, 64)
					if err != nil {
						log.Printf("[%s] Error converting price to float64: %v", ticker.Symbol, err)
						continue
					}

					if buySize := row.GetBuySize(); buySize != nil {
						switch row.Action {
						case marketdata.StreamOrderBook_Row_ACTION_REMOVE:
							delete(bidBook, priceFloat)
						case marketdata.StreamOrderBook_Row_ACTION_ADD, marketdata.StreamOrderBook_Row_ACTION_UPDATE:
							volume, err := strconv.ParseFloat(buySize.Value, 64)
							if err != nil {
								log.Printf("[%s] Error converting buy size to float64: %v", ticker.Symbol, err)
								continue
							}

							if volume <= 0 {
								delete(bidBook, priceFloat)
							} else {
								bidBook[priceFloat] = volume
							}
						}
					}

					if sellSize := row.GetSellSize(); sellSize != nil {
						switch row.Action {
						case marketdata.StreamOrderBook_Row_ACTION_REMOVE:
							delete(askBook, priceFloat)
						case marketdata.StreamOrderBook_Row_ACTION_ADD, marketdata.StreamOrderBook_Row_ACTION_UPDATE:
							volume, err := strconv.ParseFloat(sellSize.Value, 64)
							if err != nil {
								log.Printf("[%s] Error converting sell size to float64: %v", ticker.Symbol, err)
								continue
							}

							if volume <= 0 {
								delete(askBook, priceFloat)
							} else {
								askBook[priceFloat] = volume
							}
						}
					}
				}

				var (
					asks []PriceLevel
					bids []PriceLevel
				)

				for price, volume := range askBook {
					if volume > 0 {
						asks = append(asks, PriceLevel{Price: price, Volume: volume})
					}
				}

				for price, volume := range bidBook {
					if volume > 0 {
						bids = append(bids, PriceLevel{Price: price, Volume: volume})
					}
				}

				sort.Slice(asks, func(i, j int) bool {
					return asks[i].Price < asks[j].Price
				})

				sort.Slice(bids, func(i, j int) bool {
					return bids[i].Price > bids[j].Price
				})

				bestAsk, hasAsk := calculateExecutionPrice(asks, ticker.Vol)
				bestBid, hasBid := calculateExecutionPrice(bids, ticker.Vol)

				if hasBid && hasAsk {
					data := OrderBookData{
						Timestamp: timestamp,
						Symbol:    orderBook.Symbol,
						BestBid:   bestBid,
						BestAsk:   bestAsk,
					}

					select {
					case dataChan <- data:
						if isLogging {
							fmt.Printf("[%s] %s | Best Bid: %.4f | Best Ask: %.4f | Spread: %.4f | Vol: %d\n",
								timestamp.Format("15:04:05"), orderBook.Symbol, bestBid, bestAsk, bestAsk-bestBid, ticker.Vol)
						}
					default:
						log.Printf("[%s] Warning: data channel full, dropping update", ticker.Symbol)
					}
				}
			}
		}
	}
}

func SubscribeOrderBook(client *Client, ticker Ticker) (<-chan OrderBookData, error) {
	dataChan := make(chan OrderBookData, orderBookChannelBufferSize)

	go func() {
		defer close(dataChan)

		for {
			err := subscribeOrderBook(client, ticker, dataChan)
			if err != nil {
				log.Printf("[%s] Error: %v, reconnecting in 1 second...", ticker.Symbol, err)
				time.Sleep(1 * time.Second)
			}
		}
	}()

	return dataChan, nil
}

func subscribeFullOrderBook(client *Client, ticker Ticker, dataChan chan<- FullOrderBookData) error {
	conn, err := client.GetConn(context.Background())
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx = metadata.AppendToOutgoingContext(
		ctx,
		"Authorization",
		client.GetJWT(),
	)

	mdClient := marketdata.NewMarketDataServiceClient(conn)

	stream, err := mdClient.SubscribeOrderBook(ctx, &marketdata.SubscribeOrderBookRequest{
		Symbol: ticker.Symbol,
	})
	if err != nil {
		return err
	}

	askBook := make(map[float64]float64)
	bidBook := make(map[float64]float64)

	respChan := make(chan *marketdata.SubscribeOrderBookResponse, 1)
	errChan := make(chan error, 1)

	go func() {
		for {
			resp, err := stream.Recv()
			if err != nil {
				select {
				case errChan <- err:
				case <-ctx.Done():
				}
				return
			}
			select {
			case respChan <- resp:
			case <-ctx.Done():
				return
			}
		}
	}()

	heartbeatTimeout := 30 * time.Second
	timer := time.NewTimer(heartbeatTimeout)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-timer.C:
			log.Printf("[%s] Heartbeat timeout - no data received for %v", ticker.Symbol, heartbeatTimeout)
			return fmt.Errorf("heartbeat timeout: no data received for %v", heartbeatTimeout)

		case err := <-errChan:
			return err

		case resp := <-respChan:
			timer.Stop()
			timer.Reset(heartbeatTimeout)

			timestamp := time.Now().UTC()

			for _, orderBook := range resp.OrderBook {
				for _, row := range orderBook.Rows {
					if row.Price == nil {
						continue
					}

					priceFloat, err := strconv.ParseFloat(row.Price.Value, 64)
					if err != nil {
						log.Printf("[%s] Error converting price to float64: %v", ticker.Symbol, err)
						continue
					}

					if buySize := row.GetBuySize(); buySize != nil {
						switch row.Action {
						case marketdata.StreamOrderBook_Row_ACTION_REMOVE:
							delete(bidBook, priceFloat)
						case marketdata.StreamOrderBook_Row_ACTION_ADD, marketdata.StreamOrderBook_Row_ACTION_UPDATE:
							volume, err := strconv.ParseFloat(buySize.Value, 64)
							if err != nil {
								log.Printf("[%s] Error converting buy size to float64: %v", ticker.Symbol, err)
								continue
							}

							if volume <= 0 {
								delete(bidBook, priceFloat)
							} else {
								bidBook[priceFloat] = volume
							}
						}
					}

					if sellSize := row.GetSellSize(); sellSize != nil {
						switch row.Action {
						case marketdata.StreamOrderBook_Row_ACTION_REMOVE:
							delete(askBook, priceFloat)
						case marketdata.StreamOrderBook_Row_ACTION_ADD, marketdata.StreamOrderBook_Row_ACTION_UPDATE:
							volume, err := strconv.ParseFloat(sellSize.Value, 64)
							if err != nil {
								log.Printf("[%s] Error converting sell size to float64: %v", ticker.Symbol, err)
								continue
							}

							if volume <= 0 {
								delete(askBook, priceFloat)
							} else {
								askBook[priceFloat] = volume
							}
						}
					}
				}

				var (
					asks []PriceLevel
					bids []PriceLevel
				)

				for price, volume := range askBook {
					if volume > 0 {
						asks = append(asks, PriceLevel{Price: price, Volume: volume})
					}
				}

				for price, volume := range bidBook {
					if volume > 0 {
						bids = append(bids, PriceLevel{Price: price, Volume: volume})
					}
				}

				sort.Slice(asks, func(i, j int) bool {
					return asks[i].Price < asks[j].Price
				})

				sort.Slice(bids, func(i, j int) bool {
					return bids[i].Price > bids[j].Price
				})

				if len(bids) > 0 && len(asks) > 0 {
					data := FullOrderBookData{
						Timestamp: timestamp,
						Symbol:    orderBook.Symbol,
						Bids:      bids,
						Asks:      asks,
					}

					select {
					case dataChan <- data:
					default:
						log.Printf("[%s] Warning: full orderbook channel full, dropping update", ticker.Symbol)
					}
				}
			}
		}
	}
}

func SubscribeFullOrderBook(client *Client, ticker Ticker) (<-chan FullOrderBookData, error) {
	dataChan := make(chan FullOrderBookData, orderBookChannelBufferSize)

	go func() {
		defer close(dataChan)

		for {
			err := subscribeFullOrderBook(client, ticker, dataChan)
			if err != nil {
				log.Printf("[%s] Error: %v, reconnecting in 1 second...", ticker.Symbol, err)
				time.Sleep(1 * time.Second)
			}
		}
	}()

	return dataChan, nil
}
