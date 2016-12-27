package main

import "kdbutils/kdbutils"

func main() {
	host := "139.196.77.165 "
	port := 5034
	kdb := kdbutils.MewKdb(host, port)
	kdb.Connect()
	sym :=[]string {"000001", "601818"}
	table := "ohlcv"
	kdb.Subscribe(table, sym)
	var ch chan <- interface{}
	go kdb.GetSubscribedData(ch)
	//var data interface{}
	//data <- ch
}
