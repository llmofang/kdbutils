package kdbutils

import (
	kdbgo "github.com/sv/kdbgo"
	logger "github.com/alecthomas/log4go"
	"fmt"
	//"strings"
)

type Kdb struct {
	host string
	post int
	con *kdbgo.KDBConn
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

func (this *Kdb) Disconnect() error {
	logger.Info("disconnecting to kdb, host: %v, port:%v", this.host, this.post)
	err := this.con.Close()
	if err == nil {
		logger.Info("disconnected to kdb successful")
	} else {
		logger.Error("disconnected to kdb error, err: %s", err.Error())
	}
	return err
}

func (this *Kdb) Subscribe(table string, sym []string)  {
	if this.con == nil {
		this.Connect()
	}
	sym_num := len(sym)
	logger.Info("Subscribing Kdb, table: %s, sym: %v, sym_num: %v", table, sym, sym_num)
	var err error
	if sym_num == 0 {
		err = this.con.AsyncCall(".u.sub", &kdbgo.K{-kdbgo.KS, kdbgo.NONE, table},
			&kdbgo.K{-kdbgo.KS, kdbgo.NONE, ""})
	} else {
		err = this.con.AsyncCall(".u.sub", &kdbgo.K{-kdbgo.KS, kdbgo.NONE, table},
			&kdbgo.K{kdbgo.KS, kdbgo.NONE, sym})
	}
	if err != nil {
		logger.Error("Failed to subscibe, table: %s, sym; %s", table, sym)
	}
}

func (this *Kdb) SubscribedData2Channel(channel chan<-interface{}, table2type map[string] interface{})  {
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

		// confirm function is upd
		func_name := data_list[0].Data.(string)
		if func_name != "upd" {
			logger.Error("function name is not upd, func_name: %s", func_name)
			continue
		}

		table_name := data_list[1].Data.(string)
		logger.Debug("message's table_name: %s", table_name)
		var data interface{}
		match := false
		for tab, tp := range table2type {
			if table_name == tab {
  				data = tp
				match = true
				break
			}
		}
		if !match {
			continue
		}

		table_data := data_list[2].Data.(kdbgo.Table)
		length := table_data.Data[0].Len()
		logger.Debug("message's length: %d", length)
		for i := 0; i < length; i++ {
			err := kdbgo.UnmarshalDict(table_data.Index(i), data)
			if err != nil {
				fmt.Println("Failed to unmrshall dict ", err)
				continue
			}
			channel <- data
		}
	}
}
