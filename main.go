package main

import (
	"kdbutils/kdbutils"
	l4g "github.com/alecthomas/log4go"
	"time"
	"kdbutils/kdbutils/tbls"
	"reflect"
)

func main() {
	l4g.LoadConfiguration("etc/log.xml")
	var host string
	var port int
	host = "139.196.77.165"
	port = 5034
	kdb := kdbutils.MewKdb(host, port)
	kdb.Connect()
	sym := []string{"000001"}
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

			t := reflect.TypeOf(data)
			if t.Kind() == reflect.Ptr {
				t = t.Elem()
			}
			name := t.Name()
			l4g.Debug("Received %s data", name)
		}
	}()

	for {
		time.Sleep(1000 * time.Second)
	}
}
