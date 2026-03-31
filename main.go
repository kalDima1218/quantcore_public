package main

import (
	"QuantCore/trade/finam"
	"flag"
	"log"
	"time"
)

func main() {
	isOptimize := flag.Bool("optimize", false, "Run parameter optimization")
	isBacktest := flag.Bool("backtest", false, "Run backtest")
	isListen := flag.Bool("listen", false, "Run listen")
	isListenFull := flag.Bool("listen-full", false, "Run full orderbook listen")
	isListener := flag.Bool("listener", false, "Run both orderbook and full orderbook listen")
	isDebug := flag.Bool("debug", false, "Run debug")
	flag.Parse()

	if *isOptimize {
		runOptimize()
	} else if *isBacktest {
		runBacktest()
	} else if *isListen {
		runListen()
	} else if *isListenFull {
		runListenFullOrderBook()
	} else if *isListener {
		runListener()
	} else if *isDebug {
		config, err := finam.ReadConfig("config.json")
		if err != nil {
			return
		}
		client, _ := finam.NewClient(*config)
		account, _ := finam.GetAccount(client)
		println(account.GetPortfolioMc().AvailableCash.Value)
		println(account.Equity.Value)
	} else {
		log.Fatal("no flag")
	}
}

func ParseTime(s string) time.Time {
	t, err := time.Parse("2006-01-02 15:04:05", s)
	if err != nil {
		log.Fatalf("Failed to parse time: %v", err)
	}

	return t.UTC()
}

func isWithinWorkingHours(t time.Time) bool {
	moscowLocation := time.FixedZone("Moscow", 3600*3)
	moscowTime := t.In(moscowLocation)

	hour := moscowTime.Hour()
	minute := moscowTime.Minute()

	if hour >= 9 && hour < 14 {
		return true
	}

	if (hour > 14 || (hour == 14 && minute >= 5)) &&
		(hour < 18 || (hour == 18 && minute < 50)) {
		return true
	}

	if (hour > 19 || (hour == 19 && minute >= 5)) &&
		(hour < 23 || (hour == 23 && minute < 50)) {
		return true
	}

	return false
}
