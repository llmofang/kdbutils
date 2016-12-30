package kdbutils

import (
	kdbgo "github.com/sv/kdbgo"
	logger "github.com/alecthomas/log4go"
	"fmt"
	"errors"
)

// ref: http://stackoverflow.com/questions/10210188/instance-new-type-golang

type Factory_New func() interface{}

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

func (this *Kdb) SubscribedData2Channel(channel chan<-interface{}, table2struct map[string]Factory_New)  {
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

		var factory Factory_New
		match := false
		for tab, fn := range table2struct {
			if table_name == tab {
  				factory = fn
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
			row := factory()
			err := kdbgo.UnmarshalDict(table_data.Index(i), row)
			if err != nil {
				fmt.Println("Failed to unmrshall dict ", err)
				continue
			}
			channel <- row
		}
	}
}

func (this *Kdb) QueryTable(query string, factory Factory_New) ([]interface{},error) {
	if this.con == nil {
		logger.Error("Kdb is not connected")
		return nil, errors.New("kdb is not connected")
	}

	if res, err := this.con.Call(query); err != nil {
		logger.Error("Kdb query error, query: %v, error: %v", query, err)
		return nil, errors.New("kdb query error")
	} else {
		// parse result
		table := res.Data.(kdbgo.Table)
		length := table.Data[0].Len()
		table_data := make([]interface{}, length, length)

		for i := 0; i < length; i++ {
			row := factory()
			if err := kdbgo.UnmarshalDict(table.Index(i), row); err != nil {
				return nil, err
			}
			table_data[i] = row
		}
		return table_data, nil
	}
}
