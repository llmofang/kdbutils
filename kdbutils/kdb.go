package kdbutils

import (
	kdbgo "github.com/sv/kdbgo"
	logger "github.com/alecthomas/log4go"
	"fmt"
	"strings"
	"kdbutils/kdbutils/tbls"
)

type Kdb struct {
	host string
	post int
	con *kdbgo.KDBConn
}

type kdb_PubData struct {
	table_name string

}

func MewKdb(host string, port int) *Kdb {
	kdb := &Kdb{host, port, nil}
	return kdb
}

func (this *Kdb) Connect() error {
	logger.Info("connecting to kdb, host: %v, port:%v", this.host, this.post)
	var err error
	if this.con, err = kdbgo.DialKDB(this.host, this.post, "kdbgo:kdbgo"); err == nil {
		logger.Info("Connected to kdb successful")
	} else {
		logger.Error("Failed to connect to kdb, error: %s", err.Error())
		return err
	}
	return nil
}

func (this *Kdb) Subscribe(table string, sym []string)  {
	if this.con == nil {
		this.Connect()
	}
	logger.Info("Subscribing Kdb, table: %s, sym: %v", table, sym)
	symbol := Symbol_array2string(sym)
	err := this.con.AsyncCall(".u.sub", &kdbgo.K{-kdbgo.KS, kdbgo.NONE, table}, &kdbgo.K{-kdbgo.KS, kdbgo.NONE, symbol})
	if err != nil {
		logger.Error("Failed to subscibe, table: %s, sym; %s", table, sym)
	}
}

func (this *Kdb) GetSubscribedData(channel chan<-interface{})  {
	for {
		// ignore type print output
		res, _, err := this.con.ReadMessage()
		if err != nil {
			logger.Error("Error on processing KDB message, error: ", err.Error())
			return
		}
		len := res.Len()
		if len != 3 {
			logger.Error("Message is not pub data, length: %i", len)
			continue
		}

		data_list := res.Data.([]*kdbgo.K)

		// TODO 如何是从函数参数中取得表名并且转换成相应的数据类型
		table_name :=data_list[1]
		//var data interface{}
		//switch table_name {
		//case "Market":
		//	data = &tbls.Market{}
		//case "Order":
		//	data = &tbls.Order{}
		//case "Transaction":
		//	data = &tbls.Transaction{}
		//case "OrderQueue":
		//	data = &tbls.OrderQueue{}
		//default:
		//	continue
		//}
		fmt.Println("table_name: %v", table_name)
		data := &tbls.ohlcv{}
		table := data_list[2].Data.(kdbgo.Table)

		for i := 0; i < int(table.Data[0].Len()); i++ {
			err := kdbgo.UnmarshalDict(table.Index(i), data)
			if err != nil {
				fmt.Println("Failed to unmrshall dict ", err)
				continue
			}
			//fmt.Println(kline_data)
			channel <- data
		}
	}

}

func Symbol_array2string(sym []string) string {
	return strings.Join(sym, "`")
}
