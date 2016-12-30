package main

import (
	"kdbutils"
	l4g "github.com/alecthomas/log4go"
	"time"
	"kdbutils/tbls"
//	"reflect"
//	"go/types"
)

func main() {
	l4g.LoadConfiguration("etc/log.xml")
	var host string
	var port int
	host = "139.196.77.165"
	port = 5034
	kdb := kdbutils.MewKdb(host, port)
	kdb.Connect()
	//  sym := []string{} // default is all
	sym := []string{"000001", "601818"}
	kdb.Subscribe("ohlcv", sym)
	kdb.Subscribe("Transaction", sym)

	ch := make(chan interface{}, 1000)
	table2type := make(map[string] interface{})

	table2type["Market"] = &tbls.Market{}
	table2type["Transaction"] = &tbls.Transaction{}
	table2type["Order"] = &tbls.Order{}
	table2type["OrderQueue"] = &tbls.OrderQueue{}
	table2type["ohlcv"] = &tbls.Ohlcv{}

	go kdb.SubscribedData2Channel(ch, table2type)
	var data interface{}


	go func() {
		for {
			data = <-ch
			switch data.(type) {
			case *tbls.Ohlcv:
				ohlcv := data.(*tbls.Ohlcv)
				l4g.Debug("sym: %v, time: %v, open: %v, high: %v, low: %v, close: %v",
					ohlcv.Sym, ohlcv.Minute, ohlcv.Open, ohlcv.High, ohlcv.Low, ohlcv.Close)
			case *tbls.Market:
				market := data.(*tbls.Market)
				l4g.Debug("askprice1: %v, askvol1: %v, bidprice1: %v, bidvol1: %v",
					market.NAskPrice1, market.NAskVol1, market.NBidPrice1, market.NBidVol1)
			case *tbls.Order:
				order := data.(*tbls.Order)
				l4g.Debug("askprice1: %v, askvol1: %v, bidprice1: %v, bidvol1: %v",
					order.Sym, order.)
			}
		}
	}()

	for {
		time.Sleep(1000 * time.Second)
	}
}
