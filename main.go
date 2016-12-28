package main

import (
	"kdbutils/kdbutils"
	l4g "github.com/alecthomas/log4go"
)

func main() {
	l4g.LoadConfiguration("etc/log.xml")
	var host string
	var port int
	host = "139.196.77.165"
	port = 5034
	kdb := kdbutils.MewKdb(host, port)
	kdb.Connect()
	sym := []string{}
	table := "ohlcv"
	kdb.Subscribe(table, sym)
	var ch chan<- interface{}
	kdb.GetSubscribedData(ch)
	//var data interface{}
	//data <- ch
}
