package main

import (
	"github.com/llmofang/kdbutils"
	"github.com/llmofang/kdbutils/tbls"
	l4g "github.com/alecthomas/log4go"
	"time"
)

func main() {
	l4g.LoadConfiguration("etc/log.xml")
	var host string
	var port int
	host = "139.196.77.165"
	port = 5034
	kdb := kdbutils.MewKdb(host, port)

	kdb.Connect()
	test_query_table(kdb)
	test_subscribe(kdb)
	time.Sleep(10 * time.Second)


	//kdb.Disconnect()
	//kdb.Connect()
	// test for query table


	for {
		time.Sleep(100 * time.Second)
	}
}

func test_subscribe(kdb *kdbutils.Kdb)  {

	//  sym := []string{} // default is all
	sym := []string{"000001", "601818"}
	kdb.Subscribe("ohlcv", sym)
	kdb.Subscribe("Transaction", sym)

	ch := make(chan interface{}, 1000)
	table2struct := make(map[string] kdbutils.Factory_New)

	table2struct["Market"] = func() interface{}{ return new(tbls.Market) }

	table2struct["Transaction"] = func() interface{}{ return new(tbls.Transaction) }
	table2struct["Order"] = func() interface{}{ return new(tbls.Order) }
	table2struct["OrderQueue"] = func() interface{}{ return new(tbls.OrderQueue) }
	table2struct["ohlcv"] = func() interface{}{ return new(tbls.Ohlcv) }

	go kdb.SubscribedData2Channel(ch, table2struct)

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
			}
		}
	}()
}

func test_query_table(kdb *kdbutils.Kdb) {

	query := "0!select [-10] from ohlcv"
	if table_data, err := kdb.QueryTable(query, func() interface{} {
		return new(tbls.Ohlcv)
	}); err == nil {
		for i := 0; i < len(table_data); i++ {
			ohlcv := table_data[i].(*tbls.Ohlcv)
			l4g.Debug("sym: %v, min: %v, open: %v, high: %v, low: %v, close: %v", ohlcv.Sym, ohlcv.Minute, ohlcv.Open, ohlcv.High, ohlcv.Low, ohlcv.Close)

		}
	}
}
